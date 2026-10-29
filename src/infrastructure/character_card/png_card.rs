//! PNG 角色卡文本块读取：标准 chara/ccv3 使用 UTF-8 JSON 的 Base64 编码。

use super::error::CardImportError;
use base64::Engine;

const PNG_SIGNATURE: [u8; 8] = [137, 80, 78, 71, 13, 10, 26, 10];
const MAX_CARD_BYTES: usize = 8 * 1024 * 1024;

pub(crate) fn extract_chara_text(bytes: &[u8]) -> Result<String, CardImportError> {
    if !bytes.starts_with(&PNG_SIGNATURE) {
        return Err(CardImportError::Png("不是有效的 PNG 文件".into()));
    }
    let mut pos = PNG_SIGNATURE.len();
    let mut legacy = None;
    let mut v3 = None;
    let mut ended = false;
    while pos < bytes.len() {
        let header = bytes
            .get(pos..pos + 8)
            .ok_or_else(|| CardImportError::Png("PNG chunk 头不完整".into()))?;
        let length = u32::from_be_bytes(header[..4].try_into().unwrap()) as usize;
        let end = pos
            .checked_add(12)
            .and_then(|v| v.checked_add(length))
            .filter(|v| *v <= bytes.len())
            .ok_or_else(|| CardImportError::Png("PNG chunk 数据不完整".into()))?;
        let kind = &header[4..];
        let data = &bytes[pos + 8..end - 4];
        let expected_crc = u32::from_be_bytes(bytes[end - 4..end].try_into().unwrap());
        if crc32(&bytes[pos + 4..end - 4]) != expected_crc {
            return Err(CardImportError::Png("PNG chunk 校验失败".into()));
        }
        if kind == b"tEXt" {
            if let Some(sep) = data.iter().position(|v| *v == 0) {
                let key = &data[..sep];
                if key == b"chara" || key == b"ccv3" {
                    if data.len() > MAX_CARD_BYTES {
                        return Err(CardImportError::Png("角色卡文本超过大小限制".into()));
                    }
                    let text = decode_text(&data[sep + 1..])?;
                    if key == b"ccv3" {
                        v3 = Some(text);
                    } else {
                        legacy = Some(text);
                    }
                }
            }
        }
        pos = end;
        if kind == b"IEND" {
            ended = true;
            break;
        }
    }
    if !ended {
        return Err(CardImportError::Png("PNG 缺少结束块".into()));
    }
    v3.or(legacy)
        .ok_or_else(|| CardImportError::NoCardData("未找到 chara 或 ccv3 文本块".into()))
}

fn decode_text(bytes: &[u8]) -> Result<String, CardImportError> {
    // 兼容旧版导出的原始 JSON，标准路径始终先按 Base64 解码。
    let decoded = if bytes.first().is_some_and(|v| *v == b'{') {
        bytes.to_vec()
    } else {
        base64::engine::general_purpose::STANDARD
            .decode(bytes)
            .map_err(|e| CardImportError::Png(format!("角色卡 Base64 无效：{e}")))?
    };
    String::from_utf8(decoded)
        .map_err(|e| CardImportError::Png(format!("角色卡文本不是合法 UTF-8：{e}")))
}

// PNG 使用 IEEE CRC-32；同时校验元数据避免损坏卡片被静默导入。
fn crc32(bytes: &[u8]) -> u32 {
    let mut crc = !0u32;
    for byte in bytes {
        crc ^= *byte as u32;
        for _ in 0..8 {
            crc = (crc >> 1) ^ (0xedb8_8320 & 0u32.wrapping_sub(crc & 1));
        }
    }
    !crc
}

#[cfg(test)]
pub(crate) fn build_test_png(card_json: &str) -> Vec<u8> {
    build_png_texts(&[(
        "chara",
        base64::engine::general_purpose::STANDARD
            .encode(card_json)
            .as_str(),
    )])
}

#[cfg(test)]
pub(crate) fn build_plain_png() -> Vec<u8> {
    build_png_texts(&[])
}

#[cfg(test)]
fn build_png_texts(texts: &[(&str, &str)]) -> Vec<u8> {
    let mut buf = Vec::new();
    {
        let mut encoder = png::Encoder::new(&mut buf, 1, 1);
        encoder.set_color(png::ColorType::Grayscale);
        encoder.set_depth(png::BitDepth::Eight);
        let mut writer = encoder.write_header().unwrap();
        for (key, text) in texts {
            let mut data = key.as_bytes().to_vec();
            data.push(0);
            data.extend_from_slice(text.as_bytes());
            writer.write_chunk(png::chunk::tEXt, &data).unwrap();
        }
        writer.write_image_data(&[0]).unwrap();
    }
    buf
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn standard_v3_preferred_independent_of_order() {
        // 字符串为独立的 Base64 样本，避免编码与解码互相自证。
        let v3 = "eyJzcGVjIjoiY2hhcmFfY2FyZF92MyIsInNwZWNfdmVyc2lvbiI6IjMuMCIsImRhdGEiOnsibmFtZSI6IuWwj+eQsyJ9fQ==";
        for values in [
            vec![("ccv3", v3), ("chara", "eyJuYW1lIjoiT2xkIn0=")],
            vec![("chara", "eyJuYW1lIjoiT2xkIn0="), ("ccv3", v3)],
        ] {
            let value = extract_chara_text(&build_png_texts(&values)).unwrap();
            assert!(value.contains("小琳"));
            assert!(value.contains("3.0"));
        }
    }
    #[test]
    fn corrupt_truncated_and_invalid_base64_rejected() {
        let mut png = build_test_png("{\"name\":\"Alice\"}");
        png[20] ^= 1;
        assert!(extract_chara_text(&png).is_err());
        assert!(extract_chara_text(&png[..12]).is_err());
        assert!(extract_chara_text(&build_png_texts(&[("ccv3", "%%%")])).is_err());
    }
}
