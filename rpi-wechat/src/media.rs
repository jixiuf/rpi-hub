//! Media transfer for iLink: CDN download + AES-128-ECB decrypt, and the
//! inverse (encrypt + getuploadurl + CDN PUT) for outbound images/files.
//!
//! Note: `outbound.rs` keeps its own copy of the low-level helpers (a
//! deliberate split so the tool driver does not depend on this module's
//! private state); `#![allow(dead_code)]` silences the duplicate warnings.
//!
//! Key conventions (mirroring the reference implementation):
//! - inbound images: `image_item.aeskey` is **hex** (32 chars);
//!   `image_item.media.aes_key` is **base64** — ECB, no padding removal
//!   guarantee (strip PKCS#7 only when the tail validates)
//! - inbound files: `file_item.media.aes_key` is **base64-wrapped hex**
//!   (decode base64 → hex string) and the plaintext is PKCS#7-unpadded
//! - outbound: random 16-byte hex key, encrypt with PKCS#7, upload ciphertext

use std::io::Write;
use std::path::{Path, PathBuf};

use aes::cipher::{BlockDecryptMut, BlockEncryptMut, KeyInit};
use md5::{Digest, Md5};

use rpi_hub_core::now_ms;

use crate::error::IlinkError;

type Aes128Ecb = ecb::Decryptor<aes::Aes128>;

#[allow(dead_code)]
mod _doc_note {}

// moved to ilink.rs (fetch_cdn is a method on IlinkClient)

/// Fixed CDN base for media transfer (upload + download).
pub const CDN_BASE: &str = "https://novac2c.cdn.weixin.qq.com/c2c";

/// AES-128-ECB decrypt with best-effort PKCS#7 unpadding.
pub fn aes_decrypt_ecb(ciphertext: &[u8], key_hex: &str) -> Result<Vec<u8>, IlinkError> {
    let key = decode_hex_key(key_hex)?;
    let mut cipher = Aes128Ecb::new_from_slice(&key)
        .map_err(|e| IlinkError::Protocol(format!("aes key init: {e}")))?;
    // ECB requires complete 16-byte blocks: a truncated download or an error
    // page would panic inside decrypt_block_mut and kill the bridge thread.
    if ciphertext.is_empty() || ciphertext.len() % 16 != 0 {
        return Err(IlinkError::Protocol(format!(
            "ciphertext length {} is not AES-block aligned",
            ciphertext.len()
        )));
    }
    let mut buffer = ciphertext.to_vec();
    for chunk in buffer.chunks_mut(16) {
        cipher.decrypt_block_mut(chunk.into());
    }
    Ok(unpad_pkcs7(buffer))
}

/// AES-128-ECB encrypt with PKCS#7 padding.
#[allow(dead_code)]
pub fn aes_encrypt_ecb(plaintext: &[u8], key_hex: &str) -> Result<Vec<u8>, IlinkError> {
    type Aes128EcbEnc = ecb::Encryptor<aes::Aes128>;
    if plaintext.is_empty() {
        return Err(IlinkError::Protocol("cannot encrypt empty payload".into()));
    }
    let key = decode_hex_key(key_hex)?;
    let mut cipher = Aes128EcbEnc::new_from_slice(&key)
        .map_err(|e| IlinkError::Protocol(format!("aes key init: {e}")))?;
    let padded_len = (plaintext.len() / 16 + 1) * 16;
    let mut buffer = vec![0u8; padded_len];
    buffer[..plaintext.len()].copy_from_slice(plaintext);
    pkcs7_pad(&mut buffer, plaintext.len());
    for chunk in buffer.chunks_mut(16) {
        cipher.encrypt_block_mut(chunk.into());
    }
    Ok(buffer)
}

/// Accepts 32-char hex (inbound images) — the base64-wrapped variant is
/// unwrapped by the caller.
fn decode_hex_key(key_hex: &str) -> Result<[u8; 16], IlinkError> {
    let trimmed = key_hex.trim();
    if trimmed.len() != 32 || !trimmed.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err(IlinkError::Protocol(format!(
            "AES key must be 32 hex chars, got {}",
            trimmed.len()
        )));
    }
    let mut key = [0u8; 16];
    for index in 0..16 {
        key[index] = u8::from_str_radix(&trimmed[index * 2..index * 2 + 2], 16)
            .map_err(|e| IlinkError::Protocol(format!("aes key hex: {e}")))?;
    }
    Ok(key)
}

/// Unwrap `media.aes_key` (base64) → hex string when it is not already hex.
pub fn normalize_aes_key(key: &str) -> String {
    let trimmed = key.trim();
    if trimmed.len() == 32 && trimmed.chars().all(|c| c.is_ascii_hexdigit()) {
        return trimmed.to_string();
    }
    // base64 → bytes → the bytes ARE the hex string per the reference impl.
    match crate::ilink::base64_decode(trimmed) {
        Some(bytes) => String::from_utf8_lossy(&bytes).to_string(),
        None => trimmed.to_string(),
    }
}

fn pkcs7_pad(buffer: &mut [u8], data_len: usize) {
    let pad = (buffer.len() - data_len) as u8;
    for byte in &mut buffer[data_len..] {
        *byte = pad;
    }
}

fn unpad_pkcs7(mut data: Vec<u8>) -> Vec<u8> {
    if data.is_empty() {
        return data;
    }
    let pad = data[data.len() - 1];
    if (1..=16).contains(&pad)
        && data.len() >= pad as usize
        && data[data.len() - pad as usize..].iter().all(|&b| b == pad)
    {
        data.truncate(data.len() - pad as usize);
    }
    data
}

/// Download an image: `url` + optional hex `aeskey`.
pub async fn download_image(
    client: &crate::ilink::IlinkClient,
    url: &str,
    aes_key: Option<&str>,
) -> Result<(Vec<u8>, String), IlinkError> {
    let bytes = client.fetch_cdn(url).await?;
    let bytes = match aes_key {
        Some(key) => aes_decrypt_ecb(&bytes, &normalize_aes_key(key))?,
        None => bytes,
    };
    Ok((bytes, "image/jpeg".to_string()))
}

/// Download a file: `encrypt_query_param` + base64-wrapped hex `aes_key`.
pub async fn download_file(
    client: &crate::ilink::IlinkClient,
    full_url: &str,
    aes_key_b64: Option<&str>,
) -> Result<Vec<u8>, IlinkError> {
    // Use the full_url from file_item AS-IS: encrypt_query_param is already
    // URL-encoded, and re-encoding it (% → %25) breaks the CDN (HTTP 400).
    let bytes = client.fetch_cdn(full_url).await?;
    match aes_key_b64 {
        Some(wrapped) => {
            let hex_key = crate::ilink::base64_decode(wrapped)
                .map(|b| String::from_utf8_lossy(&b).to_string())
                .unwrap_or_else(|| wrapped.to_string());
            aes_decrypt_ecb(&bytes, &hex_key)
        }
        None => Ok(bytes),
    }
}

/// Save bytes under the wechat media dir with a safe unique name.
pub fn save_to_disk(dir: &Path, file_name: &str, bytes: &[u8]) -> Result<PathBuf, IlinkError> {
    std::fs::create_dir_all(dir).map_err(|e| IlinkError::Io(format!("mkdir: {e}")))?;
    let sanitized: String = file_name
        .chars()
        .map(|c| {
            if c.is_alphanumeric() || matches!(c, '.' | '-' | '_') || is_cjk(c) {
                c
            } else {
                '_'
            }
        })
        .collect();
    let unique = format!("{}_{:08}_{}", now_ms(), md5_short(file_name), sanitized);
    let path = dir.join(unique);
    let mut file =
        std::fs::File::create(&path).map_err(|e| IlinkError::Io(format!("create: {e}")))?;
    file.write_all(bytes)
        .map_err(|e| IlinkError::Io(format!("write: {e}")))?;
    Ok(path)
}

fn is_cjk(c: char) -> bool {
    matches!(c as u32, 0x4E00..=0x9FFF)
}

fn md5_short(text: &str) -> String {
    let mut hasher = Md5::new();
    hasher.update(text.as_bytes());
    let digest = hasher.finalize();
    digest.iter().take(4).map(|b| format!("{b:02x}")).collect()
}

/// Encrypt a local file for outbound transfer (returns ciphertext + hex key).
pub fn encrypt_file_bytes(bytes: &[u8]) -> Result<(Vec<u8>, String), IlinkError> {
    use rand::RngCore;
    let mut key_bytes = [0u8; 16];
    rand::thread_rng().fill_bytes(&mut key_bytes);
    let key_hex: String = key_bytes.iter().map(|b| format!("{b:02x}")).collect();
    let ciphertext = aes_encrypt_ecb(bytes, &key_hex)?;
    Ok((ciphertext, key_hex))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn aes_round_trip() {
        let key = "00112233445566778899aabbccddeeff";
        let plain = b"hello wechat media";
        let cipher = aes_encrypt_ecb(plain, key).unwrap();
        assert_ne!(cipher, plain);
        let back = aes_decrypt_ecb(&cipher, key).unwrap();
        assert_eq!(back, plain);
    }

    #[test]
    fn decrypt_strips_valid_pkcs7_only() {
        // Manual ECB encrypt of "AAAAAAAAAAAAAAAA" + padding 16 (all 0x10).
        let key = "00000000000000000000000000000000";
        let block = vec![0x41u8; 16];
        let padded = aes_encrypt_ecb(&block, key).unwrap(); // adds full pad block
        let back = aes_decrypt_ecb(&padded, key).unwrap();
        assert_eq!(back, block);
    }

    #[test]
    fn hex_key_validation() {
        assert!(decode_hex_key(&"ab".repeat(16)).is_ok());
        assert!(decode_hex_key("short").is_err());
        assert!(decode_hex_key(&"zz".repeat(16)).is_err());
    }

    #[test]
    fn normalize_aes_key_accepts_hex_and_base64() {
        assert_eq!(normalize_aes_key(&"ab".repeat(16)), "ab".repeat(16));
        // base64 of the ASCII string "aabb..." (32 chars)
        let b64 = crate::ilink::base64_encode(b"aabb");
        assert_eq!(normalize_aes_key(&b64), "aabb");
    }

    #[test]
    fn save_to_disk_sanitizes_names() {
        let dir = std::env::temp_dir().join(format!("rpi-wx-media-{}", std::process::id()));
        let path = save_to_disk(&dir, "报告 final.pdf", b"data").unwrap();
        let name = path.file_name().unwrap().to_string_lossy().to_string();
        assert!(name.contains("报告_final.pdf"), "{name}");
        assert!(!name.contains(' '));
        let _ = std::fs::remove_dir_all(&dir);
    }
}
