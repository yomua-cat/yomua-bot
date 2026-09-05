//! PNG 角色卡元数据读取。
//!
//! SillyTavern 的 PNG 角色卡把卡 JSON 写入 PNG 的 tEXt chunk（key 为 `chara`）。
//! 本模块负责读取该 chunk 的文本内容，不涉及图像像素解码。

use crate::infrastructure::character_card::error::CardImportError;

/// PNG 文件签名（8 字节）。
const PNG_SIGNATURE: [u8; 8] = [137, 80, 78, 71, 13, 10, 26, 10];

/// 将文本写入 PNG 的 `chara` tEXt chunk 并返回 PNG 字节。
///
/// 仅用于**测试构造**最小 PNG，不用于生产解析路径。
#[cfg(test)]
pub(crate) fn build_test_png(card_json: &str) -> Vec<u8> {
    build_test_png_with_text(card_json)
}

/// 构造一张不含任何文本 chunk 的最小 PNG，仅用于测试。
#[cfg(test)]
pub(crate) fn build_plain_png() -> Vec<u8> {
    let mut buf = Vec::new();
    {
        let mut encoder = png::Encoder::new(&mut buf, 1, 1);
        encoder.set_color(png::ColorType::Grayscale);
        encoder.set_depth(png::BitDepth::Eight);
        let mut writer = encoder.write_header().expect("写入头应成功");
        writer.write_image_data(&[0u8]).expect("写入像素应成功");
    }
    buf
}

/// 构造含指定 `chara` 文本的最小 PNG（测试辅助）。
///
/// `chara` 文本以 UTF-8 原始字节写入 tEXt chunk，模拟 SillyTavern 直接把卡 JSON
/// 的 UTF-8 字节写入 tEXt chunk 的行为（而非按 Latin-1 重新编码）。
#[cfg(test)]
fn build_test_png_with_text(chara_text: &str) -> Vec<u8> {
    let mut buf = Vec::new();
    {
        let mut encoder = png::Encoder::new(&mut buf, 1, 1);
        encoder.set_color(png::ColorType::Grayscale);
        encoder.set_depth(png::BitDepth::Eight);

        let mut writer = encoder.write_header().expect("写入头应成功");

        // 不通过 `add_text_chunk`（它会把文本按 Latin-1 编码，无法承载非 ASCII），
        // 而是直接写一个携带原始 UTF-8 字节的 `chara` tEXt chunk。
        let mut chunk_data = b"chara".to_vec();
        chunk_data.push(0); // keyword 与 text 之间的分隔空字节
        chunk_data.extend_from_slice(chara_text.as_bytes());
        writer
            .write_chunk(png::chunk::tEXt, &chunk_data)
            .expect("写入 tEXt chunk 应成功");

        writer.write_image_data(&[0u8]).expect("写入像素应成功");
    }
    buf
}

/// 从 PNG 字节中读取 key 为 `chara` 的 tEXt chunk 文本。
///
/// SillyTavern 把卡 JSON 以 **UTF-8 原始字节** 写入 tEXt chunk 的 text 区，
/// 而 png crate 的 `ReaderInfo::uncompressed_latin1_text` 会按 Latin-1 重新解码
/// （每个字节 `as char`），会把 ≥0x80 的字节损坏（如 UTF-8 的「你」被映射成乱码）。
/// 因此本函数**不依赖** crate 的解码结果，而是自行遍历 PNG chunk，
/// 取回 `chara` 的原始 UTF-8 字节后再按 UTF-8 解析。
pub(crate) fn extract_chara_text(bytes: &[u8]) -> Result<String, CardImportError> {
    // 校验 PNG 签名。
    if bytes.len() < PNG_SIGNATURE.len() || bytes.strip_prefix(&PNG_SIGNATURE).is_none() {
        return Err(CardImportError::Png("不是有效的 PNG 文件".to_string()));
    }

    let mut pos = PNG_SIGNATURE.len();

    // 遍历各 chunk：4 字节大端长度 + 4 字节类型 + 数据 + 4 字节 CRC。
    while pos + 8 <= bytes.len() {
        let length = read_be_u32(&bytes[pos..pos + 4]) as usize;
        let chunk_type = &bytes[pos + 4..pos + 8];
        let data_start = pos + 8;
        let data_end = data_start + length;
        // chunk 尾部还有 4 字节 CRC。
        let next = data_end + 4;
        if data_end > bytes.len() || next > bytes.len() {
            return Err(CardImportError::Png("PNG chunk 数据不完整".to_string()));
        }

        if chunk_type == b"tEXt" {
            let data = &bytes[data_start..data_end];
            // keyword 与 text 以空字节分隔；空字节前为 keyword，之后为 text 原始字节。
            if let Some(null_pos) = data.iter().position(|&b| b == 0) {
                let keyword = &data[..null_pos];
                let text_bytes = &data[null_pos + 1..];
                if keyword == b"chara" {
                    // 按 UTF-8 解析原始文本。
                    return String::from_utf8(text_bytes.to_vec()).map_err(|e| {
                        CardImportError::Png(format!("`chara` 文本不是合法 UTF-8：{e}"))
                    });
                }
            }
        }

        pos = next;
    }

    Err(CardImportError::NoCardData(
        "未找到 `chara` 文本块".to_string(),
    ))
}

/// 读取大端 4 字节无符号整数。
fn read_be_u32(bytes: &[u8]) -> u32 {
    u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]])
}
