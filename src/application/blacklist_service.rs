//! 黑名单应用服务。

use std::sync::Arc;

use chrono::{Duration, Utc};

use crate::domain::blacklist::{BlacklistEntry, BlacklistType, NewBlacklistEntry};
use crate::domain::repository::BlacklistRepository;
use crate::error::{DomainError, RuntimeError};

/// 黑名单应用服务。
#[derive(Clone)]
pub struct BlacklistService {
    repo: Arc<dyn BlacklistRepository>,
}

impl BlacklistService {
    pub fn new(repo: Arc<dyn BlacklistRepository>) -> Self {
        Self { repo }
    }

    /// 列出所有黑名单条目（含已过期的）。
    pub async fn list_all(&self) -> Result<Vec<BlacklistEntry>, RuntimeError> {
        self.repo
            .find_all()
            .await
            .map_err(|e| RuntimeError::Domain(DomainError::Repository(e.to_string())))
    }

    /// 检查目标是否在有效黑名单中（未过期）。
    pub async fn is_blacklisted(
        &self,
        target: &str,
        target_type: BlacklistType,
    ) -> Result<bool, RuntimeError> {
        self.repo
            .is_blacklisted(target, target_type)
            .await
            .map_err(|e| RuntimeError::Domain(DomainError::Repository(e.to_string())))
    }

    /// 添加永久黑名单。
    pub async fn add_permanent(
        &self,
        target: String,
        target_type: BlacklistType,
        reason: Option<String>,
    ) -> Result<i64, RuntimeError> {
        // 检查是否已存在
        if let Some(existing) = self
            .repo
            .find_by_target(&target, target_type)
            .await
            .map_err(|e| RuntimeError::Domain(DomainError::Repository(e.to_string())))?
        {
            if existing.is_active() {
                return Err(RuntimeError::Domain(DomainError::InvalidState(
                    "目标已在黑名单中".to_string(),
                )));
            }
            // 已过期：更新为永久（清除 expires_at）
            self.repo
                .set_expiration(existing.id, None)
                .await
                .map_err(|e| RuntimeError::Domain(DomainError::Repository(e.to_string())))?;
            if reason.is_some() {
                self.repo
                    .update_reason(existing.id, reason)
                    .await
                    .map_err(|e| RuntimeError::Domain(DomainError::Repository(e.to_string())))?;
            }
            return Ok(existing.id);
        }

        let entry = NewBlacklistEntry {
            target,
            target_type,
            reason,
            expires_at: None,
        };

        self.repo
            .insert(&entry)
            .await
            .map_err(|e| RuntimeError::Domain(DomainError::Repository(e.to_string())))
    }

    /// 添加临时封禁。
    pub async fn ban(
        &self,
        target: String,
        target_type: BlacklistType,
        duration_secs: i64,
        reason: Option<String>,
    ) -> Result<i64, RuntimeError> {
        if duration_secs <= 0 {
            return Err(RuntimeError::Domain(DomainError::InvalidArgument(
                "封禁时长必须为正数".to_string(),
            )));
        }

        let expires_at = Utc::now() + Duration::seconds(duration_secs);

        // 检查是否已存在
        if let Some(existing) = self
            .repo
            .find_by_target(&target, target_type)
            .await
            .map_err(|e| RuntimeError::Domain(DomainError::Repository(e.to_string())))?
        {
            // 更新过期时间
            self.repo
                .set_expiration(existing.id, Some(expires_at))
                .await
                .map_err(|e| RuntimeError::Domain(DomainError::Repository(e.to_string())))?;
            if reason.is_some() {
                self.repo
                    .update_reason(existing.id, reason)
                    .await
                    .map_err(|e| RuntimeError::Domain(DomainError::Repository(e.to_string())))?;
            }
            return Ok(existing.id);
        }

        let entry = NewBlacklistEntry {
            target,
            target_type,
            reason,
            expires_at: Some(expires_at),
        };

        self.repo
            .insert(&entry)
            .await
            .map_err(|e| RuntimeError::Domain(DomainError::Repository(e.to_string())))
    }

    /// 移除黑名单条目。
    pub async fn remove(
        &self,
        target: &str,
        target_type: BlacklistType,
    ) -> Result<(), RuntimeError> {
        self.repo
            .delete_by_target(target, target_type)
            .await
            .map_err(|e| RuntimeError::Domain(DomainError::Repository(e.to_string())))
    }

    /// 解析 duration 字符串为秒数。
    /// 支持：30m, 1h, 6h, 12h, 1d, 7d
    pub fn parse_duration(duration_str: &str) -> Result<i64, RuntimeError> {
        let duration_str = duration_str.trim().to_lowercase();
        if duration_str.is_empty() {
            return Err(RuntimeError::Domain(DomainError::InvalidArgument(
                "时长不能为空".to_string(),
            )));
        }

        let (num_str, unit) = duration_str
            .strip_suffix(|c: char| c.is_alphabetic())
            .map(|s| (s, &duration_str[s.len()..]))
            .unwrap_or((&duration_str, ""));

        let num: i64 = num_str
            .parse()
            .map_err(|_| RuntimeError::Domain(DomainError::InvalidArgument(format!(
                "无效时长数值: {num_str}"
            ))))?;

        if num <= 0 {
            return Err(RuntimeError::Domain(DomainError::InvalidArgument(
                "时长必须为正数".to_string(),
            )));
        }

        let secs = match unit {
            "" | "s" => num,
            "m" => num * 60,
            "h" => num * 3600,
            "d" => num * 86400,
            _ => {
                return Err(RuntimeError::Domain(DomainError::InvalidArgument(format!(
                    "无效时长单位: {unit}"
                ))));
            }
        };

        Ok(secs)
    }
}
