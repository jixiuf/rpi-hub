//! Media transfer for iLink: CDN download + AES-128-ECB decrypt, and the
//! inverse (encrypt + getuploadurl + CDN PUT) for outbound images/files.
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
use crate::ilink::IlinkClient;

type Aes128Ecb = ecb::Decryptor<aes::Aes128>;

// (moved to ilink.rs; media.rs uses the client's fetch_cdn)

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
    encrypt_param: &str,
    aes_key_b64: Option<&str>,
) -> Result<Vec<u8>, IlinkError> {
    let url = format!(
        "{}?encrypted_query_param={}",
        crate::media::CDN_BASE,
        urlencode(encrypt_param)
    );
    let bytes = client.fetch_cdn(&url).await?;
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

fn urlencode(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(byte as char)
            }
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
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


// ===========================================================================
// Outbound media tools: send_image_to_wechat / send_file_to_wechat
// ===========================================================================

use rpi_plugin_sdk::callbacks::free_string;
use rpi_plugin_sdk::handles::ToolHandles;
use rpi_plugin_sdk::StbString;
use rpi_plugin_sdk::{FreeStringFn, StepHandle, StepResult, StbStringRef, ToolPartialCb};

fn media_type_of(is_image: bool) -> i64 {
    // getuploadurl media_type: 1=IMG, 2=VID, 3=FILE. The sendmessage item_list
    // type is a different namespace (image=2, file=4) - do not confuse them.
    if is_image { 1 } else { 3 }
}

struct OutboundDrive {
    path: Option<String>,
    is_image: bool,
    started: bool,
    outcome: Option<Result<String, String>>,
}

static DRIVES: ToolHandles<OutboundDrive> = ToolHandles::new();

fn parse_and_validate(params: &str, hub: &crate::host::Hub) -> Result<PathBuf, String> {
    let value: serde_json::Value =
        serde_json::from_str(params).map_err(|e| format!("bad params: {e}"))?;
    let path_text = value
        .get("path")
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|p| !p.is_empty())
        .ok_or("missing path")?;
    if path_text
        .chars()
        .any(|c| matches!(c, ';' | '|' | '`' | '$' | '<' | '>' | '*' | '?' | '\\'))
    {
        return Err(format!("unsafe path: {path_text}"));
    }
    let resolved = if std::path::Path::new(path_text).is_absolute() {
        PathBuf::from(path_text)
    } else {
        hub.cwd().join(path_text)
    };
    if !resolved.exists() {
        return Err(format!("file not found: {}", resolved.display()));
    }
    Ok(resolved)
}

async fn send_media(
    _hub: &std::sync::Arc<crate::host::Hub>,
    client: &IlinkClient,
    user_id: &str,
    path: &Path,
    is_image: bool,
    context_token: Option<&str>,
) -> Result<String, crate::error::IlinkError> {
    let bytes = std::fs::read(path).map_err(|e| crate::error::IlinkError::Io(format!("read: {e}")))?;
    if bytes.is_empty() {
        return Err(crate::error::IlinkError::Protocol("file is empty".into()));
    }
    let (ciphertext, key_hex) = encrypt_file_bytes(&bytes)?;
    let file_key: String = {
        use rand::RngCore;
        let mut b = [0u8; 16];
        rand::thread_rng().fill_bytes(&mut b);
        b.iter().map(|x| format!("{x:02x}")).collect()
    };
    let raw_md5 = {
        let mut hasher = Md5::new();
        hasher.update(&bytes);
        let digest = hasher.finalize();
        digest.iter().map(|b| format!("{b:02x}")).collect::<String>()
    };
    let upload_param = client
        .get_upload_url(
            user_id,
            &file_key,
            media_type_of(is_image),
            bytes.len() as u64,
            &raw_md5,
            ciphertext.len() as u64,
            &key_hex,
        )
        .await?;
    let download_param = client
        .upload_to_cdn(&upload_param, &file_key, &ciphertext)
        .await?;

    let file_name = path
        .file_name()
        .map(|n| n.to_string_lossy().to_string())
        .unwrap_or_else(|| "file".into());
    let encoded_aes_key = crate::ilink::base64_encode(key_hex.as_bytes());
    let encrypted_size = ciphertext.len() as u64;
    let raw_size = bytes.len() as u64;

    let item = if is_image {
        serde_json::json!({
            "type": 2,
            "image_item": {
                "media": {
                    "encrypt_query_param": download_param,
                    "aes_key": encoded_aes_key,
                    "encrypt_type": 1,
                },
                "mid_size": encrypted_size,
            }
        })
    } else {
        serde_json::json!({
            "type": 4,
            "file_item": {
                "media": {
                    "encrypt_query_param": download_param,
                    "aes_key": encoded_aes_key,
                    "encrypt_type": 1,
                },
                "file_name": file_name,
                "len": raw_size.to_string(),
            }
        })
    };

    match client
        .send_media_message(user_id, context_token, serde_json::json!([item]))
        .await
    {
        Ok(()) => Ok(file_name),
        Err(crate::error::IlinkError::Api { code: Some(-2), .. }) => {
            client
                .send_media_message(user_id, None, serde_json::json!([item]))
                .await?;
            Ok(file_name)
        }
        Err(other) => Err(other),
    }
}

fn tokio_lite() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
}

struct SendDrive {
    path: Option<String>,
    is_image: bool,
    started: bool,
    outcome: Option<Result<String, String>>,
}

static SEND: ToolHandles<SendDrive> = ToolHandles::new();

extern "C" fn send_image_execute(
    _call_id: StbStringRef,
    params: StbString,
    free: Option<FreeStringFn>,
) -> StepHandle {
    spawn_send_drive(params, free, true)
}

extern "C" fn send_file_execute(
    _call_id: StbStringRef,
    params: StbString,
    free: Option<FreeStringFn>,
) -> StepHandle {
    spawn_send_drive(params, free, false)
}

fn spawn_send_drive(params: StbString, _free: Option<FreeStringFn>, is_image: bool) -> StepHandle {
    let hub = match crate::host::current() {
        Some(hub) => hub,
        None => {
            return SEND.insert(SendDrive {
                path: None,
                is_image,
                started: false,
                outcome: Some(Err("rpi-wechat not initialized".into())),
            })
        }
    };
    // SAFETY: `params` ownership was adopted by this handler; reading it is
    // sound for the call's duration.
    let text = unsafe { params.to_string_lossy() };
    let parsed = match parse_and_validate(&text, &hub) {
        Ok(path) => path,
        Err(error) => {
            return SEND.insert(SendDrive {
                path: None,
                is_image,
                started: false,
                outcome: Some(Err(error)),
            })
        }
    };
    SEND.insert(SendDrive {
        path: Some(parsed.display().to_string()),
        is_image,
        started: false,
        outcome: None,
    })
}

extern "C" fn send_poll(
    handle: StepHandle,
    _partial: Option<ToolPartialCb>,
    _user_data: *mut std::ffi::c_void,
) -> StepResult {
    rpi_plugin_sdk::guard(|| {
        let token = handle as usize;
        let Some(entry) = SEND.get(handle) else {
            return StepResult::err(StbString::from_string("invalid handle".into()));
        };
        if entry.cancelled.load(std::sync::atomic::Ordering::Acquire) {
            return StepResult::err(StbString::from_string("cancelled".into()));
        }
        let outcome = {
            let mut drive = entry.lock();
            if !drive.started {
                drive.started = true;
                let Some(path) = drive.path.clone() else {
                    // No path: this drive was created for a validation failure.
                    let result = drive
                        .outcome
                        .clone()
                        .unwrap_or_else(|| Err("missing path".into()));
                    return match result {
                        Ok(text) => StepResult::done(StbString::from_string(
                            serde_json::json!({
                                "content": [{ "type": "text", "text": text }],
                                "details": {}
                            })
                            .to_string(),
                        )),
                        Err(error) => StepResult::err(StbString::from_string(error)),
                    };
                };
                let is_image = drive.is_image;
                let hub = match crate::host::current() {
                    Some(hub) => hub,
                    None => {
                        drive.outcome = Some(Err("rpi-wechat not initialized".into()));
                        return StepResult::pending(StbString::empty());
                    }
                };
                let credentials = crate::state::Credentials::load(&hub.dir);
                std::thread::spawn(move || {
                    let outcome = match credentials {
                        Some(credentials) => {
                            let runtime = tokio_lite();
                            let client =
                                match IlinkClient::new(
                                    &credentials.base_url,
                                    &credentials.token,
                                ) {
                                    Ok(client) => Ok(client),
                                    Err(error) => Err(error.to_string()),
                                };
                            match client {
                                Ok(client) => runtime
                                    .block_on(send_media(
                                        &hub,
                                        &client,
                                        &credentials.user_id,
                                        std::path::Path::new(&path),
                                        is_image,
                                        hub.context_token().as_deref(),
                                    ))
                                    .map_err(|e| e.to_string()),
                                Err(error) => Err(error),
                            }
                        }
                        None => Err("微信未登录".into()),
                    };
                    if let Some(entry) = SEND.get(token as StepHandle) {
                        entry.lock().outcome = Some(outcome);
                    }
                });
                None
            } else {
                drive.outcome.clone()
            }
        };
        match outcome {
            Some(Ok(text)) => StepResult::done(StbString::from_string(
                serde_json::json!({
                    "content": [{ "type": "text", "text": text }],
                    "details": {}
                })
                .to_string(),
            )),
            Some(Err(error)) => StepResult::err(StbString::from_string(error)),
            None => StepResult::pending(StbString::empty()),
        }
    })
    .unwrap_or_else(|_| StepResult::err(StbString::from_string("send poll panicked".into())))
}

extern "C" fn send_cancel(handle: StepHandle) {
    SEND.cancel(handle);
}

extern "C" fn send_destroy(handle: StepHandle) {
    drop(SEND.remove(handle));
}

pub fn register(
    _hub: &std::sync::Arc<crate::host::Hub>,
    api: &rpi_plugin_sdk::PluginApi,
) -> Result<(), String> {
    let Some(register_tool) = api.register_tool else {
        return Err("host has no tool registrar".into());
    };
    for (name, description, execute, is_image) in [
        (
            "send_image_to_wechat",
            "Send a local image to the WeChat-bound user (JPG/PNG; encrypted per iLink CDN rules).",
            send_image_execute as rpi_plugin_sdk::ToolExecuteFn,
            true,
        ),
        (
            "send_file_to_wechat",
            "Send a local file to the WeChat-bound user.",
            send_file_execute as rpi_plugin_sdk::ToolExecuteFn,
            false,
        ),
    ] {
        let parameters = if is_image {
            r#"{"type":"object","properties":{"path":{"type":"string","description":"Absolute or cwd-relative image path"}},"required":["path"],"additionalProperties":false}"#
        } else {
            r#"{"type":"object","properties":{"path":{"type":"string","description":"Absolute or cwd-relative file path"}},"required":["path"],"additionalProperties":false}"#
        };
        let schema = Box::new(rpi_plugin_sdk::StableToolSchema {
            name: StbString::from_string(name.to_string()),
            description: StbString::from_string(description.to_string()),
            parameters: StbString::from_string(parameters.to_string()),
        });
        let rc = register_tool(
            schema.as_ref(),
            execute,
            send_poll,
            send_cancel,
            send_destroy,
            free_string,
        );
        if rc != 0 {
            return Err(format!("tool {name} rejected by host (rc={rc})"));
        }
    }
    Ok(())
}
