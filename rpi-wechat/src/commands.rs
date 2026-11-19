//! Plugin registration, commands, event handlers and the bridge thread.

use std::sync::mpsc;
use std::sync::Arc;
use std::time::Duration;

use rpi_plugin_sdk::{CommandHandlerFn, EventHandlerFn, EventTag, StbString, StbStringRef};

use rpi_hub_core::now_ms;

use crate::bridge::{self, BridgeContext};
use crate::host::{self, Hub};
use crate::ilink::IlinkClient;
use crate::state::{self, Credentials};

/// Commands the plugin exposes.
pub fn registry() -> Vec<(&'static str, &'static str, CommandHandlerFn)> {
    vec![(
        "wechat",
        "微信通道：/wechat login|start|stop|status",
        cmd_wechat,
    )]
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
    // Sync the store-resolved display name BEFORE the claim: the store may
    // have renamed us at registration (`-<pid>` suffix on name collision),
    // and the claim/resume writes carry `node_name` — without this the lease
    // row's owner_name is overwritten with the RAW config name (the same
    // base name as the colliding instance), corrupting every holder display.
    if let Some(view) = hub.store.get_instance_alive(&hub.node_id).ok().flatten() {
        hub.lease_owner.set_node_name(&view.name);
        hub.set_name(&view.name);
    }
    // Lease: claim (auto) — a held lease from another instance is an error.
    match hub.claim_wechat().map_err(|e| e.to_string())? {
        rpi_hub_core::store::Claim::Granted { .. } => {}
        rpi_hub_core::store::Claim::Held { holder } => {
            // Another instance answers WeChat right now. The STATUS line must
            // say so (🗨️ + holder) — the user's TUI has no other source for
            // this fact, and the old code left a stale `wx ✓ <me>` from a
            // previous run next to the hub plugin's `🗨️ <holder>`, which read
            // as a contradiction. The bridge does NOT start here.
            hub.set_status(&rpi_hub_core::status::status_remote(&holder.full_name()));
            return Err(format!(
                "微信被 {} 持有（/use 切换后重试，或等待其 TTL 过期）",
                holder.owner_name
            ));
        }
    }

    let client =
        IlinkClient::new(&credentials.base_url, &credentials.token).map_err(|e| e.to_string())?;
    hub.set_credentials(&credentials.base_url, &credentials.token);
    let (sender, receiver) = mpsc::channel::<String>();
    hub.set_outbound(sender);
    hub.set_degraded(false);
    hub.clear_bridge_stop();
    hub.set_lease_alive(true);
    *hub.user_id.lock().unwrap_or_else(|p| p.into_inner()) = Some(credentials.user_id.clone());

    // OwnerLoop first: it owns renew/footer from now on. The bridge is
    // started afterwards and reads `lease_alive` (already true), so there is
    // no window where the bridge runs without an owner.
    let owner = crate::owner::spawn(Arc::clone(hub));
    hub.set_owner_thread(owner);

    let ctx = BridgeContext {
        client,
        user_id: credentials.user_id.clone(),
        cursor: state::load_cursor(&hub.dir).unwrap_or_default(),
        outbound: receiver,
    };
    let thread_hub = Arc::clone(hub);
    let status_hub = Arc::clone(hub);
    let stop_hub = Arc::clone(hub);
    std::thread::Builder::new()
        .name("rpi-wechat-bridge".into())
        .spawn(move || {
            let runtime = std::sync::Arc::new(tokio_lite());
            runtime.block_on(async move {
                let reason = bridge::run(thread_hub, ctx).await;
                host::log(&format!("bridge stopped: {reason:?}"));
                stop_hub.set_bridge_stop(reason);
            });
            status_hub.mark_stopped();
        })
        .map_err(|e| e.to_string())?;
    Ok(())
}

/// Minimal single-threaded runtime for the bridge's async HTTP calls.
pub(crate) fn tokio_lite() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
}

// ---------------------------------------------------------------------------
// Commands
// ---------------------------------------------------------------------------

extern "C" fn cmd_wechat(
    raw: StbStringRef,
    out: *mut StbString,
    _ud: *mut std::ffi::c_void,
) -> i32 {
    let args = args_of(raw);
    host::log(&format!("cmd /wechat {args:?}"));
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
            let holder = hub
                .lease_owner
                .holder("wechat")
                .map_err(|e| e.to_string())?;
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
                if state::is_disabled(&dir) {
                    "已停用"
                } else {
                    "启用"
                },
            ))
        }
        "login" => {
            if running {
                return Err("桥已在运行（先 /wechat stop）".into());
            }
            hub.set_session_expired(false);
            login_flow(&hub)
        }
        "start" => {
            if state::is_disabled(&dir) {
                state::set_disabled(&dir, false).map_err(|e| e.to_string())?;
            }
            hub.set_session_expired(false);
            if running {
                return Ok("桥已在运行".into());
            }
            let credentials = Credentials::load(&dir).ok_or("未登录：先 /wechat login 扫码绑定")?;
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

/// QR login: fetch the code and render it immediately (synchronous, fast);
/// confirmation polling runs on a background thread and auto-starts the
/// bridge, reporting the result as a transcript notice.
fn login_flow(hub: &Arc<Hub>) -> Result<String, String> {
    let client = IlinkClient::new(crate::ilink::DEFAULT_BASE_URL, "").map_err(|e| e.to_string())?;
    let login_runtime = tokio_lite();
    let (qrcode, login_url) = login_runtime
        .block_on(client.login_qrcode())
        .map_err(|e| e.to_string())?;
    let qr_ascii = crate::qr::render_terminal(&login_url).map_err(|e| e.to_string())?;

    // Poll on a detached thread; on success persist credentials + start.
    let thread_hub = Arc::clone(hub);
    let thread_client = client.clone();
    let thread_qrcode = qrcode.clone();
    std::thread::Builder::new()
        .name("rpi-wechat-login".into())
        .spawn(move || {
            let runtime = tokio_lite();
            let deadline = std::time::Instant::now() + Duration::from_secs(180);
            loop {
                if thread_hub.shutdown.requested() || std::time::Instant::now() >= deadline {
                    let _ = thread_hub.notice("❌ 二维码超时未确认，重新 /wechat login");
                    return;
                }
                let status = match runtime.block_on(thread_client.qrcode_status(&thread_qrcode)) {
                    Ok(status) => status,
                    Err(error) => {
                        thread_hub.record_error(format!("wechat login poll: {error}"));
                        std::thread::sleep(Duration::from_secs(2));
                        continue;
                    }
                };
                match status.status.as_str() {
                    "confirmed" => {
                        let credentials = Credentials {
                            token: match status.bot_token.clone() {
                                Some(token) => token,
                                None => {
                                    thread_hub.record_error("wechat login: missing bot_token");
                                    return;
                                }
                            },
                            base_url: status
                                .base_url
                                .clone()
                                .unwrap_or_else(|| crate::ilink::DEFAULT_BASE_URL.to_string()),
                            user_id: match status.user_id.clone() {
                                Some(user) => user,
                                None => {
                                    thread_hub.record_error("wechat login: missing user_id");
                                    return;
                                }
                            },
                            saved_at: now_ms(),
                        };
                        if let Err(error) = credentials.save(&thread_hub.dir) {
                            thread_hub.record_error(format!("wechat login: save failed: {error}"));
                            return;
                        }
                        match spawn_bridge(&thread_hub, &credentials) {
                            Ok(()) => {
                                let _ = thread_hub.notice(&format!(
                                    "✓ 微信已绑定 user={}，桥已启动（{}）",
                                    credentials.user_id,
                                    thread_hub.label()
                                ));
                            }
                            Err(error) => {
                                let _ = thread_hub.notice(&format!(
                                    "✓ 已绑定 user={}，但桥启动失败: {error}",
                                    credentials.user_id
                                ));
                            }
                        }
                        return;
                    }
                    "expired" => {
                        let _ = thread_hub.notice("❌ 二维码已过期，重新 /wechat login");
                        return;
                    }
                    _ => std::thread::sleep(Duration::from_secs(2)),
                }
            }
        })
        .map_err(|e| e.to_string())?;

    Ok(format!(
        "请用微信扫码登录（或打开链接）：\n\n{qr_ascii}\n{login_url}\n\n（等待扫码确认，最长 180 秒；结果会以通知出现）"
    ))
}

pub fn event_registry() -> Vec<(EventTag, EventHandlerFn)> {
    vec![
        (EventTag::AgentStart, on_agent_start),
        // AgentEnd fires on BOTH the completed and the C-c-aborted path (the
        // aborted run emits it before AgentSettled, which rides the later
        // RunEnd dispatch). Without this handler the busy flag lingered for
        // seconds after an interrupt and /instances showed 忙.
        (EventTag::AgentEnd, on_agent_end),
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
    fn on_agent_end(_event, _user_data) {
        if let Some(hub) = host::current() {
            hub.busy.store(false, std::sync::atomic::Ordering::Release);
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

// Assistant text closed this turn -> queue chunks for the bridge thread.
rpi_plugin_sdk::event_handler! {
    fn on_message_end(event, _user_data) {
        let Some(raw) = event.message_text() else {
            return 0;
        };
        let Some(text) = assistant_text(&raw) else {
            return 0;
        };
        if let Some(hub) = host::current() {
            host::log(&format!("message_end: queueing reply ({} chars)", text.len()));
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

/// A lightweight watcher: takes over the channel when it is genuinely
/// unattended, and keeps the wechat footer truthful otherwise.
///
/// Anti-oscillation rules (the takeover-oscillation bug lived here):
/// - it NEVER claims an unexpired lease held by a live instance (claiming
///   `expires_at <= now` while the owner is mid-40s-poll is exactly how the
///   generation flapping started — the owner loop is the only writer, and a
///   healthy owner renews well before expiry);
/// - it takes over ONLY when (a) there is no lease row, (b) the row expired
///   AND its `updated_at` shows no recent renewal (a renewing-but-lagging
///   owner keeps touching the row even while its expires_at lags one tick),
///   or (c) the owner's presence row is gone (dead-owner takeover);
/// - it does not restart after a session expiry: the credentials are dead,
///   so retries would just churn the lease. `/wechat start|login` clears
///   the latch.
/// Skipped entirely when the user explicitly disabled the bridge.
pub fn spawn_watcher(hub: &Arc<Hub>, credentials: Credentials) {
    let thread_hub = Arc::clone(hub);
    let dir = hub.dir.clone();
    std::thread::Builder::new()
        .name("rpi-wechat-watcher".into())
        .spawn(move || {
            // 200ms fact poll (see owner.rs FACT_POLL: cross-dylib
            // Condvars are impossible — each plugin static-links its own
            // copy of rpi-hub-core globals). An assign envelope to THIS
            // instance becomes a running bridge — and a 💬 footer — within
            // ~0.5s of the /use hand-off; a fenced remote bridge flips the
            // footer to 🗨️ the moment its bridge thread exits.
            loop {
                if thread_hub.shutdown.requested() || state::is_disabled(&dir) {
                    return;
                }
                // Bridge already running in this process? The OwnerLoop
                // owns the footer while we hold; just yield briefly.
                if thread_hub.bridge_running() {
                    std::thread::sleep(Duration::from_millis(200));
                    continue;
                }
                if thread_hub.session_expired() {
                    // Dead credentials: footer shows stopped, no claim churn.
                    thread_hub.set_status(&rpi_hub_core::status::status_stopped("登录过期"));
                    std::thread::sleep(Duration::from_millis(200));
                    continue;
                }
                let now = rpi_hub_core::now_ms();
                match thread_hub.lease_owner.holder("wechat") {
                    Ok(Some(holder)) => {
                        let owner_present = thread_hub
                            .store
                            .get_instance_alive(&holder.owner_node)
                            .ok()
                            .flatten()
                            .is_some();
                        let ours = holder.owner_node == thread_hub.node_id;
                        let recently_renewed = now - holder.updated_at < 30_000;
                        let unattended =
                            !owner_present || (holder.expires_at <= now && !recently_renewed);
                        if ours && unattended && holder.expires_at <= now {
                            // Our own stale row (owner loop died): take it
                            // back via the normal claim path.
                            start_from_watcher(&thread_hub, &credentials);
                        } else if !ours && unattended {
                            host::log(&format!(
                                "watcher: wechat unattended (owner {} gone/expired) — taking over",
                                holder.full_name()
                            ));
                            start_from_watcher(&thread_hub, &credentials);
                        } else if !ours {
                            // Held elsewhere and alive: footer from facts.
                            thread_hub.set_status(&rpi_hub_core::status::status_remote(
                                &holder.full_name(),
                            ));
                        } else {
                            // Ours but the bridge is not running and the lease
                            // is healthy: start the bridge to match the lease.
                            start_from_watcher(&thread_hub, &credentials);
                        }
                    }
                    Ok(None) => {
                        // No lease row: the channel is free.
                        start_from_watcher(&thread_hub, &credentials);
                    }
                    Err(error) => {
                        host::log(&format!("watcher: lease read failed: {error}"));
                    }
                }
                std::thread::sleep(Duration::from_millis(200));
            }
        })
        .ok();
}

/// Claim + start the bridge from the watcher; log failures and surface the
/// remote-holder footer on a race loss.
fn start_from_watcher(hub: &Arc<Hub>, credentials: &Credentials) {
    match spawn_bridge_public(hub, credentials) {
        Ok(()) => host::log("watcher: took over the wechat channel"),
        Err(error) => {
            host::log(&format!("watcher: bridge start failed: {error}"));
            // The claim inside spawn_bridge may have raced a holder; show it.
            if let Ok(Some(holder)) = hub.lease_owner.holder("wechat") {
                if holder.owner_node != hub.node_id {
                    hub.set_status(&rpi_hub_core::status::status_remote(&holder.full_name()));
                }
            }
        }
    }
}
