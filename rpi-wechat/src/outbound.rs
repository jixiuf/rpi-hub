//! Outbound media: local file → AES encrypt → getuploadurl → CDN PUT →
//! sendmessage with a media item. Exposed to the agent as
//! `send_image_to_wechat` / `send_file_to_wechat` tools.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use rpi_hub_core::now_ms;
use rpi_plugin_sdk::handles::ToolHandles;
use rpi_plugin_sdk::callbacks::free_string;
use rpi_plugin_sdk::{StbString, StepHandle, StepResult, ToolPartialCb};

use md5::{Digest, Md5};

use crate::error::IlinkError;
use crate::host;
use crate::host::Hub;
use crate::media;

/// WeChat item type for images.
const IMAGE_ITEM_TYPE: i64 = 1;
/// WeChat item type for files.
const FILE_ITEM_TYPE: i64 = 3;

pub struct OutboundDrive {
    /// Path resolved from the tool call (absolute, sanitized).
    path: String,
    /// Image (true) or file (false).
    is_image: bool,
    outcome: Option<Result<String, String>>,
}

static DRIVES: ToolHandles<OutboundDrive> = ToolHandles::new();

/// Send one local file to the bound user. Runs on the bridge runtime.
pub async fn send_file(
    hub: &Arc<Hub>,
    client: &crate::ilink::IlinkClient,
    user_id: &str,
    path: &Path,
    is_image: bool,
) -> Result<String, IlinkError> {
    let bytes = std::fs::read(path)
        .map_err(|e| IlinkError::Io(format!("read {}: {e}", path.display())))?;
    if bytes.is_empty() {
        return Err(IlinkError::Protocol("file is empty".into()));
    }
    let (ciphertext, key_hex) = media::encrypt_file_bytes(&bytes)?;
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
    let media_type = if is_image { 2 } else { 3 };
    let upload_param = client
        .get_upload_url(
            user_id,
            &file_key,
            media_type,
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

    let item = if is_image {
        serde_json::json!({
            "type": IMAGE_ITEM_TYPE,
            "image_item": {
                "media": {
                    "encrypt_query_param": download_param,
                    "aes_key": key_hex,
                    "encrypt_type": 1,
                },
                "mid_size": bytes.len().to_string(),
            }
        })
    } else {
        serde_json::json!({
            "type": FILE_ITEM_TYPE,
            "file_item": {
                "media": {
                    "encrypt_query_param": download_param,
                    "aes_key": key_hex,
                    "encrypt_type": 1,
                },
                "file_name": file_name,
                "len": bytes.len().to_string(),
            }
        })
    };
    client
        .send_media_message(user_id, None, serde_json::json!([item]))
        .await?;
    Ok(file_name)
}

// ---------------------------------------------------------------------------
// Tool lifecycle
// ---------------------------------------------------------------------------

struct SendDrive {
    path: Option<String>,
    is_image: bool,
    started: bool,
    outcome: Option<Result<String, String>>,
}

static SEND: ToolHandles<SendDrive> = ToolHandles::new();

const SEND_TIMEOUT_MS: i64 = 60_000;

fn parse_and_validate(params: &str, hub: &Hub) -> Result<(PathBuf, bool), String> {
    let value: serde_json::Value =
        serde_json::from_str(params).map_err(|e| format!("bad params: {e}"))?;
    let path_text = value
        .get("path")
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|p| !p.is_empty())
        .ok_or("缺参数 path")?;
    let is_image = value
        .get("isImage")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    // Path sandbox: no shell metacharacters (mirrors rpi-hub's rule).
    if path_text
        .chars()
        .any(|c| matches!(c, ';' | '|' | '`' | '$' | '<' | '>' | '*' | '?' | '\\'))
    {
        return Err(format!("路径含不安全字符: {path_text}"));
    }
    let resolved = if Path::new(path_text).is_absolute() {
        PathBuf::from(path_text)
    } else {
        hub.cwd().join(path_text)
    };
    if !resolved.exists() {
        return Err(format!("文件不存在: {}", resolved.display()));
    }
    Ok((resolved, is_image))
}

fn drive_send(hub: &Arc<Hub>, path: PathBuf, is_image: bool) -> Result<String, String> {
    host::log(&format!(
        "outbound: drive_send path={} is_image={}",
        path.display(),
        is_image
    ));
    let Some(user_id) = hub
        .user_id
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .clone()
    else {
        return Err("微信未登录".into());
    };
    let client = crate::ilink::IlinkClient::new(
        &hub.credentials_base_url(),
        &hub.credentials_token(),
    )
    .map_err(|e| e.to_string())?;
    host::log("outbound: client built");
    let runtime = tokio_lite();
    let result = runtime.block_on(async move {
        send_file(hub, &client, &user_id, &path, is_image)
            .await
            .map_err(|e| e.to_string())
    });
    host::log(&format!(
        "outbound: send_file result={}",
        result.as_ref().map(|n| n.as_str()).unwrap_or("ERR")
    ));
    result
}

fn tokio_lite() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
}

rpi_plugin_sdk::execute_handler! {
    fn send_execute(_call_id, params) {
        let hub = match host::current() {
            Some(hub) => hub,
            None => {
                return SEND.insert(SendDrive {
                    path: None,
                    is_image: false,
                    started: false,
                    outcome: Some(Err("rpi-wechat 未初始化".into())),
                })
            }
        };
        match parse_and_validate(&params.to_string_lossy(), &hub) {
            Ok((path, is_image)) => SEND.insert(SendDrive {
                path: Some(path.to_string_lossy().to_string()),
                is_image,
                started: false,
                outcome: None,
            }),
            Err(error) => SEND.insert(SendDrive {
                path: None,
                is_image: false,
                started: false,
                outcome: Some(Err(error)),
            }),
        }
    }
}

extern "C" fn send_poll(
    handle: StepHandle,
    _partial: Option<ToolPartialCb>,
    _user_data: *mut std::ffi::c_void,
) -> StepResult {
    rpi_plugin_sdk::guard(|| {
        let Some(entry) = SEND.get(handle) else {
            return StepResult::err(StbString::from_string("invalid handle".into()));
        };
        if entry.cancelled.load(std::sync::atomic::Ordering::Acquire) {
            return StepResult::err(StbString::from_string("cancelled".into()));
        }
        // Kick off the blocking send exactly once, on this poll's thread.
        let outcome_pending = {
            let mut drive = entry.lock();
            if !drive.started {
                drive.started = true;
                match &drive.path {
                    Some(path) => {
                        let hub = host::current();
                        let path = path.clone();
                        let is_image = drive.is_image;
                        // StepHandle is a raw pointer (not Send): carry it as
                        // usize across the thread boundary and cast back for
                        // the registry lookup (tokens are never dereferenced).
                        let token = handle as usize;
                        std::thread::spawn(move || {
                            let outcome = match hub {
                                Some(hub) => drive_send(&hub, std::path::PathBuf::from(path), is_image),
                                None => Err("rpi-wechat 未初始化".into()),
                            };
                            if let Some(entry) = SEND.get(token as StepHandle) {
                                entry.lock().outcome = Some(outcome);
                            }
                        });
                        None
                    }
                    None => Some(Err("缺参数 path".into())),
                }
            } else {
                drive.outcome.clone()
            }
        };
        match outcome_pending {
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

/// Register the two outbound media tools.
pub fn register(hub: &Arc<Hub>, api: &rpi_plugin_sdk::PluginApi) -> Result<(), String> {
    let Some(register_tool) = api.register_tool else {
        return Err("host has no tool registrar".into());
    };
    let _ = hub;
    for (name, description, is_image) in [
        (
            "send_image_to_wechat",
            "Send a local image to the WeChat-bound user (JPG/PNG; encrypted per iLink CDN rules).",
            true,
        ),
        (
            "send_file_to_wechat",
            "Send a local file to the WeChat-bound user.",
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
            send_execute,
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
