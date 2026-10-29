//! 插件清单的发现、读取与校验。
//!
//! 目录布局：`plugins/<name>/plugin.toml` + 相对插件目录的 executable。
//! 发现过程不阻塞：坏清单记日志跳过，其余插件照常发现。

use std::path::{Component, Path};

use crate::error::RuntimeError;
use crate::infrastructure::plugin::PluginManifest;

/// 插件清单文件名。
pub const MANIFEST_FILE: &str = "plugin.toml";

/// 插件名称最大字符数（缓解 macOS sun_path 104 字节限制）。
pub const MAX_NAME_CHARS: usize = 48;

/// 遍历插件目录，发现所有清单有效（读取 + 校验通过）的插件。
///
/// 坏清单（目录不完整/TOML 解析失败/校验失败）记 `tracing` 日志后跳过，不阻塞其余插件。
pub fn discover_plugins(plugins_dir: &Path) -> Vec<PluginManifest> {
    let entries = match std::fs::read_dir(plugins_dir) {
        Ok(entries) => entries,
        Err(e) => {
            tracing::warn!("无法读取插件目录 {}：{e}", plugins_dir.display());
            return Vec::new();
        }
    };

    let mut dirs: Vec<_> = entries
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().map(|t| t.is_dir()).unwrap_or(false))
        .collect();
    // 稳定排序，保证发现顺序可预期
    dirs.sort_by_key(|e| e.file_name());

    let mut manifests = Vec::new();
    for entry in dirs {
        let dir = entry.path();
        let result = load_manifest(&dir).and_then(|m| validate_manifest(&m, &dir).map(|_| m));
        match result {
            Ok(m) => manifests.push(m),
            Err(e) => tracing::warn!("跳过插件 {}：{e}", dir.display()),
        }
    }
    manifests
}

/// 从插件目录读取清单（`plugin_dir/plugin.toml`）。
///
/// 缺失 `permissions` 字段视为空权限；缺失 `config` 字段兜底为 `Null`。
pub fn load_manifest(plugin_dir: &Path) -> Result<PluginManifest, RuntimeError> {
    let path = plugin_dir.join(MANIFEST_FILE);
    let content = std::fs::read_to_string(&path)
        .map_err(|e| RuntimeError::Plugin(format!("无法读取插件清单 {}：{e}", path.display())))?;
    let raw: toml::Value = toml::from_str(&content)
        .map_err(|e| RuntimeError::Plugin(format!("插件清单 {} 解析失败：{e}", path.display())))?;

    let field_str = |key: &str| -> Result<String, RuntimeError> {
        raw.get(key)
            .and_then(|v| v.as_str())
            .map(str::to_string)
            .ok_or_else(|| {
                RuntimeError::Plugin(format!("插件清单 {} 缺少字符串字段 {key}", path.display()))
            })
    };
    let name = field_str("name")?;
    let version = field_str("version")?;
    let description = field_str("description")?;
    let executable = field_str("executable")?;

    let permissions: Vec<crate::infrastructure::plugin::PluginPermission> =
        match raw.get("permissions") {
            Some(v) => v.clone().try_into().map_err(|e| {
                RuntimeError::Plugin(format!(
                    "插件清单 {} 的 permissions 解析失败（含未知权限？）：{e}",
                    path.display()
                ))
            })?,
            None => Vec::new(),
        };

    let config: serde_json::Value = match raw.get("config") {
        None => serde_json::Value::Null,
        Some(v) => serde_json::to_value(v).map_err(|e| {
            RuntimeError::Plugin(format!(
                "插件清单 {} 的 config 转换失败：{e}",
                path.display()
            ))
        })?,
    };

    Ok(PluginManifest {
        name,
        version,
        description,
        permissions,
        executable,
        config,
    })
}

/// 校验清单是否满足运行约束。
///
/// 校验失败返回中文错误信息，调用方（发现层）负责跳过。
pub fn validate_manifest(m: &PluginManifest, plugin_dir: &Path) -> Result<(), RuntimeError> {
    // 1. 名称与目录名一致
    let dir_name = plugin_dir
        .file_name()
        .and_then(|s| s.to_str())
        .ok_or_else(|| RuntimeError::Plugin("插件目录名无法解析为 UTF-8 字符串".to_string()))?;
    if m.name != dir_name {
        return Err(RuntimeError::Plugin(format!(
            "插件清单名称 \"{}\" 与目录名 \"{}\" 不一致",
            m.name, dir_name
        )));
    }

    // 2. 名称非空且不超过 48 字符
    if m.name.is_empty() {
        return Err(RuntimeError::Plugin("插件名称不能为空".to_string()));
    }
    if m.name.chars().count() > MAX_NAME_CHARS {
        return Err(RuntimeError::Plugin(format!(
            "插件名称 \"{}\" 超过 {} 个字符",
            m.name, MAX_NAME_CHARS
        )));
    }

    // 3. version 与 description 非空
    if m.version.is_empty() {
        return Err(RuntimeError::Plugin(format!(
            "插件 \"{}\" 的 version 不能为空",
            m.name
        )));
    }
    if m.description.is_empty() {
        return Err(RuntimeError::Plugin(format!(
            "插件 \"{}\" 的 description 不能为空",
            m.name
        )));
    }

    // 4. executable 必须为相对路径：非绝对、不以 .. 开头、不含 .. 路径段
    let exe = Path::new(&m.executable);
    if exe.is_absolute() {
        return Err(RuntimeError::Plugin(format!(
            "插件 \"{}\" 的 executable \"{}\" 必须是相对路径",
            m.name, m.executable
        )));
    }
    if m.executable.starts_with("..") {
        return Err(RuntimeError::Plugin(format!(
            "插件 \"{}\" 的 executable \"{}\" 不能以 .. 开头",
            m.name, m.executable
        )));
    }
    let has_parent_dir_segment = m.executable.split(['/', '\\']).any(|seg| seg == "..");
    if has_parent_dir_segment {
        return Err(RuntimeError::Plugin(format!(
            "插件 \"{}\" 的 executable \"{}\" 不能包含 .. 路径段",
            m.name, m.executable
        )));
    }

    // 5. 解析后的相对路径不得越出插件目录
    let resolved = plugin_dir.join(exe);
    let escapes = resolved
        .components()
        .any(|c| matches!(c, Component::ParentDir));
    if escapes {
        return Err(RuntimeError::Plugin(format!(
            "插件 \"{}\" 的 executable \"{}\" 解析后越出插件目录",
            m.name, m.executable
        )));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infrastructure::plugin::PluginPermission;
    use std::fs;
    use std::path::{Path, PathBuf};
    use tempfile::tempdir;

    /// 写一个清单完整合法的插件目录，返回其路径。
    fn write_valid_plugin(root: &Path, name: &str) -> PathBuf {
        let dir = root.join(name);
        fs::create_dir_all(&dir).unwrap();
        let toml = format!(
            "name = \"{name}\"\n\
             version = \"0.1.0\"\n\
             description = \"示例插件\"\n\
             permissions = [\"message.send\", \"message.read\"]\n\
             executable = \"echo-plugin\"\n\
             [config]\n\
             foo = \"bar\"\n"
        );
        fs::write(dir.join(MANIFEST_FILE), toml).unwrap();
        dir
    }

    /// 写一个自定义内容的清单目录。
    fn write_raw_plugin(root: &Path, name: &str, toml: &str) -> PathBuf {
        let dir = root.join(name);
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join(MANIFEST_FILE), toml).unwrap();
        dir
    }

    #[test]
    fn discovers_valid_plugin() {
        let root = tempdir().unwrap();
        write_valid_plugin(root.path(), "echo");
        let manifests = discover_plugins(root.path());
        assert_eq!(manifests.len(), 1, "应发现 1 个插件");
        let m = &manifests[0];
        assert_eq!(m.name, "echo");
        assert_eq!(m.version, "0.1.0");
        assert_eq!(m.description, "示例插件");
        assert_eq!(m.executable, "echo-plugin");
        assert_eq!(
            m.permissions,
            vec![PluginPermission::MessageSend, PluginPermission::MessageRead]
        );
        assert_eq!(m.config["foo"], "bar");
    }

    #[test]
    fn empty_dir_yields_empty_vec() {
        let root = tempdir().unwrap();
        assert!(discover_plugins(root.path()).is_empty());
    }

    #[test]
    fn subdir_without_manifest_skipped() {
        let root = tempdir().unwrap();
        fs::create_dir_all(root.path().join("no-manifest")).unwrap();
        assert!(discover_plugins(root.path()).is_empty());
    }

    #[test]
    fn bad_toml_skipped_others_kept() {
        let root = tempdir().unwrap();
        write_valid_plugin(root.path(), "echo");
        write_raw_plugin(root.path(), "broken", "name = ");
        let manifests = discover_plugins(root.path());
        assert_eq!(manifests.len(), 1, "坏清单应被跳过，其余保留");
        assert_eq!(manifests[0].name, "echo");
    }

    #[test]
    fn dotted_permissions_parse_to_enum() {
        let root = tempdir().unwrap();
        let dir = write_raw_plugin(
            root.path(),
            "echo",
            "name = \"echo\"\n\
             version = \"0.1.0\"\n\
             description = \"示例插件\"\n\
             permissions = [\"llm.call\", \"memory.write\", \"character.state.read\"]\n\
             executable = \"echo-plugin\"\n",
        );
        let m = load_manifest(&dir).unwrap();
        assert_eq!(
            m.permissions,
            vec![
                PluginPermission::LlmCall,
                PluginPermission::MemoryWrite,
                PluginPermission::CharacterStateRead
            ]
        );
    }

    #[test]
    fn name_mismatch_directory_rejected() {
        let root = tempdir().unwrap();
        let dir = write_raw_plugin(
            root.path(),
            "echo",
            "name = \"other\"\n\
             version = \"0.1.0\"\n\
             description = \"示例插件\"\n\
             executable = \"echo-plugin\"\n",
        );
        let m = load_manifest(&dir).unwrap();
        let err = validate_manifest(&m, &dir).unwrap_err().to_string();
        assert!(err.contains("不一致"), "错误信息：{err}");
        assert!(
            discover_plugins(root.path()).is_empty(),
            "发现层应跳过名目不符的插件"
        );
    }

    #[test]
    fn absolute_executable_rejected() {
        let root = tempdir().unwrap();
        let dir = write_raw_plugin(
            root.path(),
            "echo",
            "name = \"echo\"\n\
             version = \"0.1.0\"\n\
             description = \"示例插件\"\n\
             executable = \"/usr/bin/echo-plugin\"\n",
        );
        let m = load_manifest(&dir).unwrap();
        let err = validate_manifest(&m, &dir).unwrap_err().to_string();
        assert!(err.contains("相对路径"), "错误信息：{err}");
    }

    #[test]
    fn parent_dir_executable_rejected() {
        // 以 .. 开头
        let root = tempdir().unwrap();
        let dir1 = write_raw_plugin(
            root.path(),
            "echo",
            "name = \"echo\"\n\
             version = \"0.1.0\"\n\
             description = \"示例插件\"\n\
             executable = \"../bin/echo-plugin\"\n",
        );
        let m1 = load_manifest(&dir1).unwrap();
        let err1 = validate_manifest(&m1, &dir1).unwrap_err().to_string();
        assert!(err1.contains(".."), "错误信息：{err1}");

        // 中间含 .. 路径段
        let dir2 = write_raw_plugin(
            root.path(),
            "echo2",
            "name = \"echo2\"\n\
             version = \"0.1.0\"\n\
             description = \"示例插件\"\n\
             executable = \"sub/../bin\"\n",
        );
        let m2 = load_manifest(&dir2).unwrap();
        let err2 = validate_manifest(&m2, &dir2).unwrap_err().to_string();
        assert!(err2.contains(".."), "错误信息：{err2}");

        // 反斜杠变体在 unix 上不构成路径分隔，但分号防呆：仅断言绝不越界
        let dir3 = write_raw_plugin(
            root.path(),
            "echo3",
            "name = \"echo3\"\n\
             version = \"0.1.0\"\n\
             description = \"示例插件\"\n\
             executable = \"..\\\\bin\"\n",
        );
        let m3 = load_manifest(&dir3).unwrap();
        let resolved = dir3.join(Path::new(&m3.executable));
        assert!(
            !resolved
                .components()
                .any(|c| matches!(c, Component::ParentDir)),
            "解析后的路径不得含 ParentDir"
        );
    }

    #[test]
    fn overlong_name_rejected() {
        let root = tempdir().unwrap();
        let long_name = "a".repeat(MAX_NAME_CHARS + 1);
        let dir = write_raw_plugin(
            root.path(),
            &long_name,
            &format!(
                "name = \"{long_name}\"\n\
                 version = \"0.1.0\"\n\
                 description = \"示例插件\"\n\
                 executable = \"echo-plugin\"\n"
            ),
        );
        let m = load_manifest(&dir).unwrap();
        let err = validate_manifest(&m, &dir).unwrap_err().to_string();
        assert!(err.contains("48"), "错误信息：{err}");
    }

    #[test]
    fn empty_version_or_description_rejected() {
        let root = tempdir().unwrap();
        let dir_v = write_raw_plugin(
            root.path(),
            "echo",
            "name = \"echo\"\n\
             version = \"\"\n\
             description = \"示例插件\"\n\
             executable = \"echo-plugin\"\n",
        );
        let m_v = load_manifest(&dir_v).unwrap();
        let err_v = validate_manifest(&m_v, &dir_v).unwrap_err().to_string();
        assert!(err_v.contains("version"), "错误信息：{err_v}");

        let dir_d = write_raw_plugin(
            root.path(),
            "echo",
            "name = \"echo\"\n\
             version = \"0.1.0\"\n\
             description = \"\"\n\
             executable = \"echo-plugin\"\n",
        );
        let m_d = load_manifest(&dir_d).unwrap();
        let err_d = validate_manifest(&m_d, &dir_d).unwrap_err().to_string();
        assert!(err_d.contains("description"), "错误信息：{err_d}");
    }

    #[test]
    fn unknown_permission_rejected_and_skipped() {
        let root = tempdir().unwrap();
        let dir = write_raw_plugin(
            root.path(),
            "echo",
            "name = \"echo\"\n\
             version = \"0.1.0\"\n\
             description = \"示例插件\"\n\
             permissions = [\"message.send\", \"bogus.perm\"]\n\
             executable = \"echo-plugin\"\n",
        );
        let err = load_manifest(&dir).unwrap_err().to_string();
        assert!(err.contains("permissions"), "错误信息：{err}");
        assert!(
            discover_plugins(root.path()).is_empty(),
            "发现层应跳过未知权限的插件"
        );
    }

    #[test]
    fn missing_config_defaults_to_null() {
        let root = tempdir().unwrap();
        let dir = write_raw_plugin(
            root.path(),
            "echo",
            "name = \"echo\"\n\
             version = \"0.1.0\"\n\
             description = \"示例插件\"\n\
             executable = \"echo-plugin\"\n",
        );
        let m = load_manifest(&dir).unwrap();
        assert_eq!(m.config, serde_json::Value::Null);
    }

    #[test]
    fn missing_permissions_defaults_to_empty() {
        let root = tempdir().unwrap();
        let dir = write_raw_plugin(
            root.path(),
            "echo",
            "name = \"echo\"\n\
             version = \"0.1.0\"\n\
             description = \"示例插件\"\n\
             executable = \"echo-plugin\"\n\
             [config]\n\
             foo = \"bar\"\n",
        );
        let m = load_manifest(&dir).unwrap();
        assert!(m.permissions.is_empty(), "缺失 permissions 应默认为空");
    }
}
