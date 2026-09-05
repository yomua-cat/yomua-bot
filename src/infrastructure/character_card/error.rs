//! Character Card 导入错误类型。

/// 角色卡导入错误。
#[derive(Debug, thiserror::Error)]
pub enum CardImportError {
    /// JSON 解析失败。
    #[error("JSON 解析失败：{0}")]
    Json(String),

    /// 无法识别为角色卡。
    #[error("无法识别为角色卡")]
    NotRecognized,

    /// PNG 解析失败。
    #[error("PNG 解析失败：{0}")]
    Png(String),

    /// PNG 中未找到角色卡数据（缺少 `chara` 文本块）。
    #[error("PNG 中未找到角色卡数据：{0}")]
    NoCardData(String),
}
