//! Plugin registration, commands, event handlers and the bridge thread.

use std::sync::Arc;
use std::sync::mpsc;
use std::time::Duration;

use rpi_plugin_sdk::{CommandHandlerFn, EventHandlerFn, EventTag, StbString, StbStringRef};

use rpi_hub_core::now_ms;

use crate::bridge::{self, BridgeContext};
use crate::host::{self, Hub};
use crate::ilink::IlinkClient;
use crate::state::{self, Credentials};

/// Commands the plugin exposes.
pub fn registry() -> Vec<(&'static str, &'static str, CommandHandlerFn)> {
    vec![
        (
            "wechat",
            "微信通道：/wechat login|start|stop|status",
            cmd_wechat,
        ),
    ]
}

fn emit(out: *mut StbString, text: &str) -> i32 {
    let payload = serde_json::json!({ "kind": "message", "text": text });
    unsafe {
        *out = StbString::from_string(payload.to_string());
    }
    0
}

fn emit_error(out: *mut StbString, error: impl std::fmt::Display) -> i32 {
    emit(out, &format!("❌ {error}"))
}

fn args_of(raw: StbStringRef) -> String {
    let text = unsafe { raw.as_str() }.to_string();
    serde_json::from_str::<serde_json::Value>(&text)
        .ok()
        .and_then(|value| {
            value
                .get("args")
                .and_then(|v| v.as_str())
                .map(str::to_string)
        })
        .unwrap_or(text)
}

fn hub() -> Result<Arc<Hub>, String> {
    host::current().ok_or_else(|| "rpi-wechat 未初始化".to_string())
}

// ---------------------------------------------------------------------------
// Bridge thread
// ---------------------------------------------------------------------------

/// Public wrapper for lib.rs auto-start.
pub fn spawn_bridge_public(hub: &Arc<Hub>, credentials: &Credentials) -> Result<(), String> {
    spawn_bridge(hub, credentials)
}

/// Spawn the bridge thread with a fresh client and channel.
fn spawn_bridge(hub: &Arc<Hub>, credentials: &Credentials) -> Result<(), String> {
    // Lease: claim (auto) — a held lease from another instance is an error.
    match hub.claim_wechat().map_err(|e| e.to_string())? {
        rpi_hub_core::store::Claim::Granted { .. } => {}
        rpi_hub_core::store::Claim::Held { holder } => {
            return Err(format!(
                "微信被 {} 持有（/use 切换后重试，或等待其 TTL 过期）",
                holder.owner_name
            ));
        }
    }

    let client = IlinkClient::new(&credentials.base_url, &credentials.token)
        .map_err(|e| e.to_string())?;
    let (sender, receiver) = mpsc::channel::<String>();
    hub.set_outbound(sender);
    *hub.user_id.lock().unwrap_or_else(|p| p.into_inner()) =
        Some(credentials.user_id.clone());

    let ctx = BridgeContext {
        client,
        user_id: credentials.user_id.clone(),
        cursor: state::load_cursor(&hub.dir).unwrap_or_default(),
        outbound: receiver,
    };
    let thread_hub = Arc::clone(hub);
    let status_hub = Arc::clone(hub);
    std::thread::Builder::new()
        .name("rpi-wechat-bridge".into())
        .spawn(move || {
            let runtime = tokio_lite();
            runtime.block_on(async move {
                let reason = bridge::run(thread_hub, ctx).await;
                host::log(&format!("bridge stopped: {reason:?}"));
            });
            status_hub.mark_stopped();
        })
        .map_err(|e| e.to_string())?;
    hub.set_status(&format!("wechat ✓ {}", hub.name()));
    Ok(())
}

/// Minimal single-threaded runtime for the bridge's async HTTP calls.
fn tokio_lite() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
}

// ---------------------------------------------------------------------------
// Commands
// ---------------------------------------------------------------------------

extern "C" fn cmd_wechat(raw: StbStringRef, out: *mut StbString, _ud: *mut std::ffi::c_void) -> i32 {
    let args = args_of(raw);
    let result = run_command(&args);
    match result {
        Ok(text) => emit(out, &text),
        Err(error) => emit_error(out, error),
    }
}

/// `/wechat login|start|stop|status` — synchronous; QR login runs bounded in
/// this handler (poll loop with a 120 s cap) so the TUI gets one answer.
fn run_command(args: &str) -> Result<String, String> {
    let hub = hub()?;
    let sub = args.trim().to_ascii_lowercase();
    let dir = hub.dir.clone();

    // Bridge already running?
    let running = hub.bridge_running();

    match sub.as_str() {
        "" | "status" => {
            let credentials = Credentials::load(&dir);
            let holder = hub.store.lease_holder("wechat").map_err(|e| e.to_string())?;
            Ok(format!(
                "实例: {}\n凭据: {}\n桥: {}\n绑定用户: {}\n微信接管: {}\n禁用标志: {}",
                hub.label(),
                credentials
                    .as_ref()
                    .map(|c| format!("user={}", c.user_id))
                    .unwrap_or_else(|| "未登录（/wechat login）".into()),
                if running { "运行中" } else { "停止" },
                hub.user_id
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .clone()
                    .unwrap_or_else(|| "-".into()),
                holder
                    .map(|h| format!("{}（generation {}）", h.owner_name, h.generation))
                    .unwrap_or_else(|| "(无)".into()),
                if state::is_disabled(&dir) { "已停用" } else { "启用" },
            ))
        }
        "login" => {
            if running {
                return Err("桥已在运行（先 /wechat stop）".into());
            }
            login_flow(&hub)
        }
        "start" => {
            if state::is_disabled(&dir) {
                state::set_disabled(&dir, false).map_err(|e| e.to_string())?;
            }
            if running {
                return Ok("桥已在运行".into());
            }
            let credentials =
                Credentials::load(&dir).ok_or("未登录：先 /wechat login 扫码绑定")?;
            spawn_bridge(&hub, &credentials)?;
            Ok(format!("微信桥已启动（{}）", hub.label()))
        }
        "stop" => {
            state::set_disabled(&dir, true).map_err(|e| e.to_string())?;
            bridge::release_and_stop(&hub);
            Ok("微信桥已停止（/wechat start 重新启动）".into())
        }
        other => Err(format!(
            "未知子命令 {other:?}；可用: login | start | stop | status"
        )),
    }
}

/// QR login: fetch the code, save a PNG, poll until confirmed (120 s cap),
/// persist credentials, and start the bridge.
fn login_flow(hub: &Arc<Hub>) -> Result<String, String> {
    let client = IlinkClient::new(crate::ilink::DEFAULT_BASE_URL, "")
        .map_err(|e| e.to_string())?;
    let login_runtime = tokio_lite();
    let (qrcode, png_base64) = login_runtime
        .block_on(client.login_qrcode())
        .map_err(|e| e.to_string())?;

    // Save the QR as a PNG next to the state dir and open it with the system
    // viewer (printing ASCII QR would wreck the TUI).
    let dir = state::wechat_dir(&hub.dir);
    rpi_hub_core::paths::ensure_private_dir(&dir).map_err(|e| e.to_string())?;
    let png_path = dir.join("login-qr.png");
    let bytes = crate::qr::decode_base64(&png_base64)
        .ok_or("二维码内容不是合法 base64")?;
    std::fs::write(&png_path, &bytes).map_err(|e| e.to_string())?;
    let _ = std::process::Command::new("open")
        .arg(&png_path)
        .spawn();

    let mut answer = format!(
        "二维码已保存: {}\n（扫码后在微信里确认；等待确认中…最长 120 秒）",
        png_path.display()
    );
    let deadline = now_ms() + 120_000;
    loop {
        if hub.shutdown.requested() || now_ms() >= deadline {
            return Err("二维码超时未确认".into());
        }
        let status = login_runtime
            .block_on(client.qrcode_status(&qrcode))
            .map_err(|e| e.to_string())?;
        match status.status.as_str() {
            "confirmed" => {
                let credentials = Credentials {
                    token: status.bot_token.ok_or("confirmed 缺少 bot_token")?,
                    base_url: status
                        .base_url
                        .unwrap_or_else(|| crate::ilink::DEFAULT_BASE_URL.to_string()),
                    user_id: status.user_id.ok_or("confirmed 缺少 user_id")?,
                    saved_at: now_ms(),
                };
                credentials.save(&hub.dir).map_err(|e| e.to_string())?;
                spawn_bridge(hub, &credentials)?;
                answer.push_str(&format!(
                    "\n✓ 已绑定 user={} 并启动桥（{}）",
                    credentials.user_id,
                    hub.label()
                ));
                return Ok(answer);
            }
            "expired" => return Err("二维码已过期，重新 /wechat login".into()),
            _ => std::thread::sleep(Duration::from_secs(2)),
        }
    }
}

// ---------------------------------------------------------------------------
// Event handlers
// ---------------------------------------------------------------------------

pub fn event_registry() -> Vec<(EventTag, EventHandlerFn)> {
    vec![
        (EventTag::AgentStart, on_agent_start),
        (EventTag::AgentSettled, on_agent_settled),
        (EventTag::MessageEnd, on_message_end),
    ]
}

rpi_plugin_sdk::event_handler! {
    fn on_agent_start(_event, _user_data) {
        if let Some(hub) = host::current() {
            hub.busy.store(true, std::sync::atomic::Ordering::Release);
        }
        0
    }
}

rpi_plugin_sdk::event_handler! {
    fn on_agent_settled(_event, _user_data) {
        if let Some(hub) = host::current() {
            hub.busy.store(false, std::sync::atomic::Ordering::Release);
        }
        0
    }
}

/// Assistant text closed this turn → queue chunks for the bridge thread.
rpi_plugin_sdk::event_handler! {
    fn on_message_end(event, _user_data) {
        let Some(raw) = event.message_text() else {
            return 0;
        };
        let Some(text) = assistant_text(&raw) else {
            return 0;
        };
        if let Some(hub) = host::current() {
            hub.queue_reply(&text);
        }
        0
    }
}

/// Extract visible assistant text (thinking/toolCalls/empty frames skipped).
fn assistant_text(raw: &str) -> Option<String> {
    let value: serde_json::Value = serde_json::from_str(raw).ok()?;
    if value.get("role").and_then(|v| v.as_str()) != Some("assistant") {
        return None;
    }
    let content = value.get("content")?;
    let mut out = String::new();
    match content {
        serde_json::Value::String(text) => out.push_str(text),
        serde_json::Value::Array(blocks) => {
            for block in blocks {
                if block.get("type").and_then(|v| v.as_str()) == Some("text") {
                    if let Some(text) = block.get("text").and_then(|v| v.as_str()) {
                        if !out.is_empty() {
                            out.push('\n');
                        }
                        out.push_str(text);
                    }
                }
            }
        }
        _ => {}
    }
    let trimmed = out.trim();
    (!trimmed.is_empty()).then(|| trimmed.to_string())
}

#[cfg(test)]
mod tests {
    use super::assistant_text;

    #[test]
    fn extracts_visible_assistant_text_only() {
        let raw = r#"{"role":"assistant","content":[{"type":"thinking","thinking":"hm"},{"type":"text","text":"答案"}]}"#;
        assert_eq!(assistant_text(raw).as_deref(), Some("答案"));
    }

    #[test]
    fn ignores_tool_frames_and_user_messages() {
        assert!(assistant_text(
            r#"{"role":"assistant","content":[{"type":"toolCall","name":"read"}]}"#
        )
        .is_none());
        assert!(assistant_text(r#"{"role":"user","content":"hi"}"#).is_none());
    }
}
