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

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;

#[derive(Debug, serde::Serialize)]
#[serde(tag = "cmd")]
enum Command {
    Status {},
    #[serde(rename = "reload-config")]
    ReloadConfig {},
    Shutdown {},
    Help {},
}

#[derive(Debug, serde::Deserialize)]
struct Response {
    ok: bool,
    #[serde(default)]
    data: Option<serde_json::Value>,
    #[serde(default)]
    error: Option<String>,
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
                println!("{{\"ok\": true}}");
            }
        } else {
            eprintln!("错误: {}", self.error.as_deref().unwrap_or("未知错误"));
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

    // 构建命令。
    let cmd: Command = match cmd_arg.as_str() {
        "status" => Command::Status {},
        "reload-config" => Command::ReloadConfig {},
        "shutdown" => Command::Shutdown {},
        "help" => Command::Help {},
        _ => {
            eprintln!("错误: 未知命令: {}", cmd_arg);
            eprintln!("可用命令: status, reload-config, shutdown, help");
            std::process::exit(1);
        }
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
    let json = serde_json::to_string(&cmd).expect("命令序列化失败");
    stream.write_all(json.as_bytes()).await?;

    // 读取响应。
    let mut buf = Vec::with_capacity(8192);
    let n = stream.read_buf(&mut buf).await?;

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

    resp.print();
    Ok(())
}
