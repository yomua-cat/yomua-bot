//! echo 插件 —— 插件系统的最小可运行示例客户端。
//!
//! 它演示了插件与 Core 之间最常用的三类交互：
//! 1. 握手：连接 Core 注入的 UDS socket，发送 `Hello{name, version, subscribe}`，
//!    收到 `hello_ack{ok: true}` 后进入运行态；
//! 2. 事件订阅：在 Hello 中订阅若干 dotted 事件名，收到 `notify` 帧时打印；
//! 3. 插件数据（可选演示）：收到 `message.received` 事件时，调用
//!    `plugin_data.set` / `plugin_data.get` 读写自己命名空间下的数据。
//!
//! 它只依赖 `yomua_bot::infrastructure::plugin::protocol` 提供的帧编解码纯函数，
//! 不依赖 Core 的任何内部模块。
//!
//! ## 手动运行
//!
//! 1. 编译示例：`cargo build --example echo_plugin`
//! 2. 建立插件目录，并把产物拷贝为插件可执行文件：
//!    ```
//!    mkdir -p plugins/echo
//!    cp target/debug/examples/echo_plugin plugins/echo/echo_plugin
//!    ```
//! 3. 编写 `plugins/echo/plugin.toml`（权限可按需声明，`message.read` 可选；
//!    `plugin_data.*` 免权限、自动按插件名隔离，无需声明）：
//!    ```toml
//!    name = "echo"
//!    version = "0.1.0"
//!    description = "示例 echo 插件"
//!    permissions = []
//!    executable = "echo_plugin"
//!    ```
//! 4. 在 `runtime.toml` 配置插件目录（默认不启用）：
//!    ```toml
//!    plugins_dir = "plugins"
//!    ```
//! 5. 启动 Core：`cargo run`（或直接运行 `target/debug/yomua-bot`），
//!    观察插件日志与事件通知；退出 Core 时插件会收到 `shutdown` 通知并自行退出。

use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;

use yomua_bot::infrastructure::plugin::protocol::{
    decode_full_read, encode_frame, Hello, WireMessage, MAX_FRAME_LEN,
};

/// 握手超时：连接后等待 Core 回 `hello_ack` 的时限。
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);
/// 每次读 socket 的临时缓冲大小（单帧可跨多次读取累计）。
const READ_CHUNK_SIZE: usize = 4096;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. 读取 Core 注入的环境变量（由插件监督器在子进程启动时设置）。
    let socket_path = match std::env::var("YOMUA_PLUGIN_SOCKET") {
        Ok(v) => v,
        Err(_) => {
            eprintln!("错误：缺少环境变量 YOMUA_PLUGIN_SOCKET（应由 Core 的插件监督器注入）");
            std::process::exit(1);
        }
    };
    let plugin_name = match std::env::var("YOMUA_PLUGIN_NAME") {
        Ok(v) => v,
        Err(_) => {
            eprintln!("错误：缺少环境变量 YOMUA_PLUGIN_NAME（应由 Core 的插件监督器注入）");
            std::process::exit(1);
        }
    };

    // 2. 连接 Core 的 UDS socket 并发送 Hello（订阅三个事件）。
    let mut stream = UnixStream::connect(&socket_path).await?;
    println!("已连接到 Core：{socket_path}");
    let hello = WireMessage::Hello(Hello {
        name: plugin_name.clone(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        subscribe: [
            "message.received",
            "adapter.connected",
            "adapter.disconnected",
        ]
        .map(String::from)
        .to_vec(),
    });
    stream.write_all(&encode_frame(&hello)?).await?;

    // 3. 等待 hello_ack（限时 5 秒）。
    let mut buffer: Vec<u8> = Vec::new();
    let ack = tokio::time::timeout(HANDSHAKE_TIMEOUT, read_frame(&mut stream, &mut buffer)).await;
    match ack {
        Ok(Ok(Some(WireMessage::HelloAck { ok: true, .. }))) => {
            println!("握手成功，插件已上线（name={plugin_name}）");
        }
        Ok(Ok(Some(WireMessage::HelloAck { ok: false, reason }))) => {
            eprintln!(
                "握手被拒绝：{}",
                reason.unwrap_or_else(|| "未知原因".to_string())
            );
            std::process::exit(1);
        }
        Ok(Ok(Some(other))) => {
            eprintln!("握手期间收到意外消息：{other:?}");
            std::process::exit(1);
        }
        Ok(Ok(None)) => {
            eprintln!("连接在握手完成前被关闭（EOF）");
            std::process::exit(1);
        }
        Ok(Err(e)) => {
            eprintln!("握手期间读取失败：{e}");
            std::process::exit(1);
        }
        Err(_) => {
            eprintln!(
                "握手超时（{} 秒内未收到 hello_ack）",
                HANDSHAKE_TIMEOUT.as_secs()
            );
            std::process::exit(1);
        }
    }

    // 4. 主循环：读取并处理来自 Core 的帧。
    //    请求 id 自增，用于恢复线协议的响应匹配。
    let mut next_id: u64 = 1;
    loop {
        let msg = match read_frame(&mut stream, &mut buffer).await {
            Ok(Some(msg)) => msg,
            Ok(None) => {
                eprintln!("连接已关闭（EOF），插件退出");
                std::process::exit(1);
            }
            Err(e) => {
                eprintln!("帧解码错误：{e}，插件退出");
                std::process::exit(1);
            }
        };
        match msg {
            WireMessage::Notify { event, data } => {
                println!("[通知] {}: {}", event, data);
                if event == "shutdown" {
                    println!("收到 shutdown 通知，插件退出");
                    return Ok(());
                }
                // 可选演示插件数据持久化：在收到消息事件时，往自己命名空间写一条、
                // 再读回一条，展示 plugin_data API 的落库往返。
                if event == "message.received" {
                    if let Err(e) =
                        demo_plugin_data(&mut stream, &mut buffer, &mut next_id, &event, &data)
                            .await
                    {
                        eprintln!("插件数据演示失败：{e}");
                    }
                }
            }
            // Core 当前协议不支持向插件发起请求；防御性处理。
            WireMessage::Request { id, method, .. } => {
                println!("[忽略] 收到不应出现的 Request（Core 暂不支持反向调用）：id={id} method={method}");
            }
            WireMessage::Response { id, .. } => {
                println!("[忽略] 收到意外的 Response：id={id}");
            }
            WireMessage::Hello(_) | WireMessage::HelloAck { .. } => {
                println!("[忽略] 收到意外的握手消息");
            }
        }
    }
}

/// 从流中累计缓冲并取出一整帧。
///
/// 处理半包/粘包语义；帧体超长且无法完成时判协议错误（与 Core 的 transport 一致）。
async fn read_frame(
    stream: &mut UnixStream,
    buffer: &mut Vec<u8>,
) -> Result<Option<WireMessage>, String> {
    loop {
        if let Some((consumed, msg)) = decode_full_read(buffer) {
            let remaining = buffer.split_off(consumed);
            *buffer = remaining;
            return Ok(Some(msg));
        }
        // 缓冲里仍取不出完整帧：已超上限且无法继续 → 协议错误。
        if buffer.len() > MAX_FRAME_LEN as usize {
            return Err("帧体超过长度上限且无法完成".to_string());
        }
        let mut chunk = [0u8; READ_CHUNK_SIZE];
        match stream.read(&mut chunk).await {
            Ok(0) => return Ok(None), // EOF
            Ok(n) => buffer.extend_from_slice(&chunk[..n]),
            Err(e) => return Err(format!("读取 socket 失败：{e}")),
        }
    }
}

/// 向 Core 发送一个请求并等待同 id 的响应。
///
/// 等待期间到达的 Notify 照常打印（与主循环语义一致）；若收到 shutdown 通知
/// 则直接退出（与主循环一致）。
async fn call(
    stream: &mut UnixStream,
    buffer: &mut Vec<u8>,
    id: u64,
    method: &str,
    params: serde_json::Value,
) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    let frame = encode_frame(&WireMessage::Request {
        id,
        method: method.to_string(),
        params,
    })?;
    stream.write_all(&frame).await?;
    loop {
        let msg = match read_frame(stream, buffer).await? {
            Some(msg) => msg,
            None => return Err("连接已关闭（EOF）".into()),
        };
        match msg {
            WireMessage::Response {
                id: rid,
                ok,
                result,
                error,
            } => {
                // 响应 id 匹配校验：不匹配则打印并继续等待。
                if rid != id {
                    println!("[忽略] 响应 id 不匹配（期望 {id}，实际 {rid}）");
                    continue;
                }
                if ok {
                    return Ok(result.unwrap_or(serde_json::Value::Null));
                }
                let err = error.unwrap_or_else(|| "未知错误".to_string());
                return Err(format!("方法 {method} 失败：{err}").into());
            }
            WireMessage::Notify { event, data } => {
                println!("[通知] {}: {}", event, data);
                if event == "shutdown" {
                    println!("收到 shutdown 通知，插件退出");
                    std::process::exit(0);
                }
            }
            WireMessage::Request {
                id: rid, method: m, ..
            } => {
                println!(
                    "[忽略] 收到不应出现的 Request（Core 暂不支持反向调用）：id={rid} method={m}"
                );
            }
            other => {
                println!("[忽略] 调用期间收到意外的消息：{other:?}");
            }
        }
    }
}

/// 可选演示插件数据持久化：写一条 `last_event` 再读回，展示落库往返。
async fn demo_plugin_data(
    stream: &mut UnixStream,
    buffer: &mut Vec<u8>,
    next_id: &mut u64,
    event: &str,
    data: &serde_json::Value,
) -> Result<(), Box<dyn std::error::Error>> {
    // 1. plugin_data.set：写入当前事件与时间戳。
    let set_id = *next_id;
    *next_id += 1;
    let set_params = serde_json::json!({
        "key": "last_event",
        "value": {
            "event": event,
            "at": chrono::Utc::now().to_rfc3339(),
            "payload": data,
        },
    });
    let result = call(stream, buffer, set_id, "plugin_data.set", set_params).await?;
    println!("  plugin_data.set -> {result}");

    // 2. plugin_data.get：读回同一条。
    let get_id = *next_id;
    *next_id += 1;
    let get_params = serde_json::json!({ "key": "last_event" });
    let result = call(stream, buffer, get_id, "plugin_data.get", get_params).await?;
    println!("  plugin_data.get -> {result}");
    Ok(())
}
