//! yomua-ctl —— yomua-bot 控制客户端。
//!
//! 通过 Unix 域套接字向运行中的 yomua-bot 发送控制命令。
//!
//! 用法：
//! ```text
//! yomua-ctl [选项] <命令>
//!
//! 命令：
//!   status         查询运行时状态
//!   reload-config  重新加载并校验配置文件
//!   shutdown       发送优雅关停信号
//!
//! 选项：
//!   --socket <路径>  控制 socket 路径（默认：<data-dir>/control.sock）
//!   --data-dir <路径>  数据目录（默认：data）
//! ```

use std::path::PathBuf;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;
use tokio::time::timeout;

/// 控制协议错误详情。
#[derive(Debug, serde::Deserialize)]
struct ControlErrorDetail {
    code: String,
    #[allow(dead_code)]
    message: String,
}

#[derive(Debug, serde::Deserialize)]
struct Response {
    id: u64,
    ok: bool,
    #[serde(default)]
    data: Option<serde_json::Value>,
    #[serde(default)]
    error: Option<ControlErrorDetail>,
}

impl Response {
    fn print(&self) {
        if self.ok {
            if let Some(data) = &self.data {
                println!(
                    "{}",
                    serde_json::to_string_pretty(data).unwrap_or_else(|_| data.to_string())
                );
            } else {
                println!("{{\"id\": {}, \"ok\": true}}", self.id);
            }
        } else {
            let err = self.error.as_ref().map(|e| e.message.as_str()).unwrap_or("未知错误");
            eprintln!("错误[{}]: {}", self.id, err);
            std::process::exit(1);
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().skip(1).collect();

    if args.is_empty() {
        eprintln!("用法: yomua-ctl [选项] <命令>");
        eprintln!("运行 yomua-ctl --help 查看帮助");
        std::process::exit(1);
    }

    let mut socket_path: Option<PathBuf> = None;
    let mut data_dir: PathBuf = PathBuf::from("data");
    let mut cmd_arg: Option<String> = None;

    // 解析选项。
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--socket" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("错误: --socket 需要一个路径参数");
                    std::process::exit(1);
                }
                socket_path = Some(PathBuf::from(&args[i]));
            }
            "--data-dir" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("错误: --data-dir 需要一个路径参数");
                    std::process::exit(1);
                }
                data_dir = PathBuf::from(&args[i]);
            }
            "--help" | "-h" => {
                println!("用法: yomua-ctl [选项] <命令>");
                println!();
                println!("命令：");
                println!("  status         查询运行时状态");
                println!("  reload-config  重新加载并校验配置文件");
                println!("  shutdown       发送优雅关停信号");
                println!("  help           显示所有可用命令");
                println!();
                println!("选项：");
                println!("  --socket <路径>  控制 socket 路径（默认：<data-dir>/control.sock）");
                println!("  --data-dir <路径>  数据目录（默认：data）");
                std::process::exit(0);
            }
            _ => {
                if cmd_arg.is_some() {
                    eprintln!("错误: 未知选项: {}", args[i]);
                    std::process::exit(1);
                }
                cmd_arg = Some(args[i].clone());
            }
        }
        i += 1;
    }

    let cmd_arg = match cmd_arg {
        Some(arg) => arg,
        None => {
            eprintln!("错误: 未指定命令");
            std::process::exit(1);
        }
    };

    // 确定 socket 路径。
    let socket = socket_path.unwrap_or_else(|| data_dir.join("control.sock"));

    // 生成请求 ID（使用时间戳 + 随机数）。
    let id = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
        ^ (rand_u32() as u64);

    // 构建命令（JSON 格式：{id, cmd}）。
    let request_cmd = match cmd_arg.as_str() {
        "status" => "status",
        "reload-config" => "reload-config",
        "shutdown" => "shutdown",
        "help" => "help",
        _ => {
            eprintln!("错误: 未知命令: {}", cmd_arg);
            eprintln!("可用命令: status, reload-config, shutdown, help");
            std::process::exit(1);
        }
    };

    let request = serde_json::json!({
        "id": id,
        "cmd": request_cmd
    });

    // 连接 socket。
    let mut stream = UnixStream::connect(&socket).await.map_err(|e| {
        eprintln!(
            "错误: 无法连接到 control socket {}: {}",
            socket.display(),
            e
        );
        eprintln!("提示: 确认 yomua-bot 正在运行，并且 --data-dir 指向正确的数据目录");
        e
    })?;

    // 发送命令。
    let json = serde_json::to_string(&request).expect("请求序列化失败");
    stream.write_all(json.as_bytes()).await?;

    // 读取响应（5 秒超时）。
    let mut buf = Vec::with_capacity(8192);
    let read_result = timeout(Duration::from_secs(5), stream.read_buf(&mut buf)).await;

    let n = match read_result {
        Ok(Ok(n)) => n,
        Ok(Err(e)) => {
            eprintln!("错误: 读取响应失败: {}", e);
            std::process::exit(1);
        }
        Err(_) => {
            // 超时
            eprintln!("错误: 服务器响应超时（5 秒）");
            eprintln!("提示: 确认 yomua-bot 正在正常运行");
            std::process::exit(1);
        }
    };

    if n == 0 {
        eprintln!("错误: 服务器关闭了连接");
        std::process::exit(1);
    }

    buf.truncate(n);
    let resp: Response = serde_json::from_slice(&buf).map_err(|e| {
        eprintln!("错误: 无法解析服务器响应: {}", e);
        eprintln!("原始数据: {:?}", String::from_utf8_lossy(&buf));
        e
    })?;

    // 验证响应 ID 匹配。
    if resp.id != id {
        eprintln!("错误: 响应 ID 不匹配（请求: {}, 响应: {}）", id, resp.id);
        std::process::exit(1);
    }

    resp.print();
    Ok(())
}

/// 生成一个随机 u32。
fn rand_u32() -> u32 {
    use std::time::Instant;
    let instant = Instant::now();
    (instant.elapsed().as_nanos() as u32).wrapping_add(instant.elapsed().subsec_nanos())
}
