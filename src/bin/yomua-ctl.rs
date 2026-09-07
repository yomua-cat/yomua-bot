//! yomua-ctl —— yomua-bot 控制客户端。
//!
//! 通过 Unix 域套接字向运行中的 yomua-bot 发送控制命令。
//!
//! 用法：
//! ```text
//! yomua-ctl [选项] <命令>
//!
//! 命令：
//!   status                   查询运行时状态
//!   reload-config            重新加载并校验配置文件
//!   shutdown                 发送优雅关停信号
//!   help                     显示所有可用命令
//!   state-get                查询角色状态
//!                            （需要 --character；可选 --conversation）
//!   state-set                修改角色状态
//!                            （需要 --character；全局：--energy/--stress/--activity；
//!                             会话级：--energy/--stress/--mood + --conversation）
//!
//! 选项：
//!   --socket <路径>          控制 socket 路径（默认：<data-dir>/control.sock）
//!   --data-dir <路径>        数据目录（默认：data）
//!   --character <ID>         角色 ID（state-get / state-set）
//!   --conversation <ID>      会话 ID（缺省表示全局 Character State）
//!   --mood <0-100>           情绪值（Character × Conversation 范围，需 --conversation）
//!   --energy <0-100>         精力值
//!   --stress <0-100>         压力值
//!   --activity <文本>         当前活动（仅全局 Character State）
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
            let code = self
                .error
                .as_ref()
                .map(|e| e.code.as_str())
                .unwrap_or("UNKNOWN");
            let err = self
                .error
                .as_ref()
                .map(|e| e.message.as_str())
                .unwrap_or("未知错误");
            eprintln!("错误[{code}@{}]: {}", self.id, err);
            std::process::exit(1);
        }
    }
}

#[derive(Default)]
struct StateParams {
    character_id: Option<i64>,
    conversation_id: Option<i64>,
    mood: Option<f64>,
    energy: Option<f64>,
    stress: Option<f64>,
    activity: Option<String>,
}

impl StateParams {
    fn to_json(&self) -> serde_json::Map<String, serde_json::Value> {
        let mut m = serde_json::Map::new();
        if let Some(v) = self.character_id {
            m.insert("character_id".to_string(), serde_json::json!(v));
        }
        if let Some(v) = self.conversation_id {
            m.insert("conversation_id".to_string(), serde_json::json!(v));
        }
        if let Some(v) = self.mood {
            m.insert("mood".to_string(), serde_json::json!(v));
        }
        if let Some(v) = self.energy {
            m.insert("energy".to_string(), serde_json::json!(v));
        }
        if let Some(v) = self.stress {
            m.insert("stress".to_string(), serde_json::json!(v));
        }
        if let Some(v) = &self.activity {
            m.insert("activity".to_string(), serde_json::json!(v));
        }
        m
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
    let mut state = StateParams::default();

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
            "--character" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("错误: --character 需要一个角色 ID");
                    std::process::exit(1);
                }
                match args[i].parse::<i64>() {
                    Ok(v) if v > 0 => state.character_id = Some(v),
                    _ => {
                        eprintln!("错误: --character 必须是正整数");
                        std::process::exit(1);
                    }
                }
            }
            "--conversation" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("错误: --conversation 需要一个会话 ID");
                    std::process::exit(1);
                }
                match args[i].parse::<i64>() {
                    Ok(v) if v > 0 => state.conversation_id = Some(v),
                    _ => {
                        eprintln!("错误: --conversation 必须是正整数");
                        std::process::exit(1);
                    }
                }
            }
            "--mood" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("错误: --mood 需要一个数值");
                    std::process::exit(1);
                }
                match args[i].parse::<f64>() {
                    Ok(v) => state.mood = Some(v),
                    Err(_) => {
                        eprintln!("错误: --mood 必须是数字（0-100）");
                        std::process::exit(1);
                    }
                }
            }
            "--energy" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("错误: --energy 需要一个数值");
                    std::process::exit(1);
                }
                match args[i].parse::<f64>() {
                    Ok(v) => state.energy = Some(v),
                    Err(_) => {
                        eprintln!("错误: --energy 必须是数字（0-100）");
                        std::process::exit(1);
                    }
                }
            }
            "--stress" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("错误: --stress 需要一个数值");
                    std::process::exit(1);
                }
                match args[i].parse::<f64>() {
                    Ok(v) => state.stress = Some(v),
                    Err(_) => {
                        eprintln!("错误: --stress 必须是数字（0-100）");
                        std::process::exit(1);
                    }
                }
            }
            "--activity" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("错误: --activity 需要一个文本");
                    std::process::exit(1);
                }
                state.activity = Some(args[i].clone());
            }
            "--help" | "-h" => {
                println!("用法: yomua-ctl [选项] <命令>");
                println!();
                println!("命令：");
                println!("  status         查询运行时状态");
                println!("  reload-config  重新加载并校验配置文件");
                println!("  shutdown       发送优雅关停信号");
                println!("  help           显示所有可用命令");
                println!("  state-get      查询角色状态（--character [--conversation]）");
                println!("  state-set      修改角色状态（--character [--conversation] --mood/--energy/--stress/--activity）");
                println!();
                println!("选项：");
                println!(
                    "  --socket <路径>      控制 socket 路径（默认：<data-dir>/control.sock）"
                );
                println!("  --data-dir <路径>    数据目录（默认：data）");
                println!("  --character <ID>     角色 ID");
                println!("  --conversation <ID>  会话 ID（缺省表示全局状态）");
                println!("  --mood <0-100>       情绪值（需 --conversation）");
                println!("  --energy <0-100>     精力值");
                println!("  --stress <0-100>     压力值");
                println!("  --activity <文本>    当前活动（仅全局状态）");
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

    // 校验状态命令参数。
    let is_state_cmd = matches!(cmd_arg.as_str(), "state-set" | "state-get");
    if is_state_cmd && state.character_id.is_none() {
        eprintln!("错误: {cmd_arg} 需要 --character <角色ID>");
        std::process::exit(1);
    }
    if cmd_arg == "state-set"
        && state.mood.is_none()
        && state.energy.is_none()
        && state.stress.is_none()
        && state.activity.is_none()
    {
        eprintln!(
            "错误: state-set 至少需要一个状态字段（--mood / --energy / --stress / --activity）"
        );
        std::process::exit(1);
    }
    if state.mood.is_some() && state.conversation_id.is_none() {
        eprintln!("错误: mood 属于 Character × Conversation 范围，必须同时提供 --conversation");
        std::process::exit(1);
    }
    if state.activity.is_some() && state.conversation_id.is_some() {
        eprintln!("错误: activity 仅存在于全局 Character State；会话级 activity 模型未定义");
        std::process::exit(1);
    }

    // 确定 socket 路径。
    let socket = socket_path.unwrap_or_else(|| data_dir.join("control.sock"));

    // 生成请求 ID（使用时间戳 + 随机数）。
    let id = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
        ^ (rand_u32() as u64);

    // 构建命令（JSON 格式：{id, cmd, params}）。
    let request_cmd = match cmd_arg.as_str() {
        "status" => "status",
        "reload-config" => "reload-config",
        "shutdown" => "shutdown",
        "help" => "help",
        "state-get" => "state-get",
        "state-set" => "state-set",
        _ => {
            eprintln!("错误: 未知命令: {}", cmd_arg);
            eprintln!("可用命令: status, reload-config, shutdown, help, state-get, state-set");
            std::process::exit(1);
        }
    };

    let request = if is_state_cmd {
        serde_json::json!({
            "id": id,
            "cmd": request_cmd,
            "params": state.to_json(),
        })
    } else {
        serde_json::json!({ "id": id, "cmd": request_cmd })
    };

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
