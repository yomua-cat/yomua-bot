//! 插件 API 方法 ↔ 权限映射与权限执行判定（纯函数）。
//!
//! 方法与权限为 1:1 对应关系；判定只做权限层面检查，具体 API 是否
//! 实现由上层决定（个别方法在此层就有针对性限制，见 `check_permission`）。

use crate::infrastructure::plugin::PluginPermission;

/// API 方法所需的权限（1:1 映射）。
///
/// 返回 `None` 表示方法不在授权表中（未知方法）。
pub fn permission_for_method(method: &str) -> Option<PluginPermission> {
    match method {
        "message.send" => Some(PluginPermission::MessageSend),
        "message.read" => Some(PluginPermission::MessageRead),
        "character.read" => Some(PluginPermission::CharacterRead),
        "character.state.read" => Some(PluginPermission::CharacterStateRead),
        "character.state.write" => Some(PluginPermission::CharacterStateWrite),
        "memory.read" => Some(PluginPermission::MemoryRead),
        "memory.write" => Some(PluginPermission::MemoryWrite),
        "relationship.read" => Some(PluginPermission::RelationshipRead),
        "relationship.write" => Some(PluginPermission::RelationshipWrite),
        "llm.call" => Some(PluginPermission::LlmCall),
        "scheduler.create" => Some(PluginPermission::ScheduleCreate),
        _ => None,
    }
}

/// 权限的 dotted 名称（与 serde rename 一致）。
fn permission_name(p: PluginPermission) -> &'static str {
    match p {
        PluginPermission::MessageRead => "message.read",
        PluginPermission::MessageSend => "message.send",
        PluginPermission::CharacterRead => "character.read",
        PluginPermission::CharacterStateRead => "character.state.read",
        PluginPermission::CharacterStateWrite => "character.state.write",
        PluginPermission::MemoryRead => "memory.read",
        PluginPermission::MemoryWrite => "memory.write",
        PluginPermission::RelationshipRead => "relationship.read",
        PluginPermission::RelationshipWrite => "relationship.write",
        PluginPermission::ScheduleCreate => "scheduler.create",
        PluginPermission::LlmCall => "llm.call",
    }
}

/// 检查插件对某 API 方法的调用是否被授权。
///
/// - `scheduler.create`：本期不开放，一律返回错误（即使已声明该权限）。
/// - 未知方法：返回「未知方法」错误。
/// - 未声明所需权限：返回「权限不足」错误。
///
/// 注意：`message.read` 在权限层授权即通过；具体 API 未实现由 API 层决定。
pub fn check_permission(method: &str, granted: &[PluginPermission]) -> Result<(), String> {
    if method == "scheduler.create" {
        return Err("scheduler.create 本期不开放".to_string());
    }
    let needed = permission_for_method(method).ok_or_else(|| format!("未知方法：{method}"))?;
    if granted.contains(&needed) {
        Ok(())
    } else {
        Err(format!("权限不足：需要 {} 权限", permission_name(needed)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_methods_map_to_permissions() {
        let cases = [
            ("message.send", PluginPermission::MessageSend),
            ("message.read", PluginPermission::MessageRead),
            ("character.read", PluginPermission::CharacterRead),
            ("character.state.read", PluginPermission::CharacterStateRead),
            (
                "character.state.write",
                PluginPermission::CharacterStateWrite,
            ),
            ("memory.read", PluginPermission::MemoryRead),
            ("memory.write", PluginPermission::MemoryWrite),
            ("relationship.read", PluginPermission::RelationshipRead),
            ("relationship.write", PluginPermission::RelationshipWrite),
            ("llm.call", PluginPermission::LlmCall),
            ("scheduler.create", PluginPermission::ScheduleCreate),
        ];
        assert_eq!(cases.len(), 11, "方法与权限必须 1:1 完整枚举");
        for (method, perm) in cases {
            assert_eq!(
                permission_for_method(method),
                Some(perm),
                "方法 {method} 映射错误"
            );
        }
        assert_eq!(permission_for_method("no.such.method"), None);
    }

    #[test]
    fn granted_permission_passes() {
        assert_eq!(
            check_permission("message.send", &[PluginPermission::MessageSend]),
            Ok(())
        );
        assert_eq!(
            check_permission("memory.write", &[PluginPermission::MemoryWrite]),
            Ok(())
        );
        assert_eq!(
            check_permission("llm.call", &[PluginPermission::LlmCall]),
            Ok(())
        );
    }

    #[test]
    fn missing_permission_rejected() {
        let err = check_permission("message.send", &[PluginPermission::MessageRead]).unwrap_err();
        assert!(err.contains("权限不足"), "错误信息：{err}");
        assert!(
            err.contains("message.send"),
            "错误信息应含所需权限名：{err}"
        );
    }

    #[test]
    fn unknown_method_rejected() {
        let err = check_permission("no.such.method", &[]).unwrap_err();
        assert_eq!(err, "未知方法：no.such.method");
    }

    #[test]
    fn scheduler_create_denied_even_if_granted() {
        // 映射表完整：scheduler.create ↔ ScheduleCreate
        assert_eq!(
            permission_for_method("scheduler.create"),
            Some(PluginPermission::ScheduleCreate)
        );
        // 但权限层一律拒绝，即使声明了该权限
        let err =
            check_permission("scheduler.create", &[PluginPermission::ScheduleCreate]).unwrap_err();
        assert_eq!(err, "scheduler.create 本期不开放");
        let err2 = check_permission("scheduler.create", &[]).unwrap_err();
        assert_eq!(err2, "scheduler.create 本期不开放");
    }

    #[test]
    fn message_read_passes_at_permission_layer() {
        // message.read：权限层授权即通过，具体不实现由 API 层决定
        assert_eq!(
            permission_for_method("message.read"),
            Some(PluginPermission::MessageRead)
        );
        assert_eq!(
            check_permission("message.read", &[PluginPermission::MessageRead]),
            Ok(())
        );
        let err = check_permission("message.read", &[PluginPermission::MessageSend]).unwrap_err();
        assert!(err.contains("权限不足"), "错误信息：{err}");
    }
}
