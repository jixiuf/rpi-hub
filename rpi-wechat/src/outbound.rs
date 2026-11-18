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

use std::path::{Path, PathBuf};

use md5::{Digest, Md5};

use crate::ilink::IlinkClient;

// (media helpers moved to media.rs — single implementation)











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
    let (ciphertext, key_hex) = crate::media::encrypt_file_bytes(&bytes)?;
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
                            let runtime = crate::commands::tokio_lite();
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
