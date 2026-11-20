//! Plugin registration, commands, event handlers and the bridge thread.

use std::sync::mpsc;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use rpi_plugin_sdk::{CommandHandlerFn, EventHandlerFn, EventTag, StbString, StbStringRef};

use rpi_mesh_core::now_ms;

use crate::wechat::bridge::{self, BridgeContext};
use crate::wechat::channel::Channel;
use crate::wechat::ilink::IlinkClient;
use crate::wechat::state::{self, Credentials};

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

fn mesh() -> Result<Arc<Channel>, String> {
    crate::wechat::channel::current().ok_or_else(|| "rpi-wechat 未初始化".to_string())
}

// ---------------------------------------------------------------------------
// Bridge thread
// ---------------------------------------------------------------------------

/// Public wrapper for the mesh bootstrap (lib.rs).
pub fn spawn_bridge_public(mesh: &Arc<Channel>, credentials: &Credentials) -> Result<(), String> {
    spawn_bridge(mesh, credentials)
}

/// Plugin-load bootstrap for the wechat channel (called from lib.rs):
/// auto-start the bridge when credentials exist and the channel is enabled,
/// then arm the watcher for takeover.
pub fn bootstrap(mesh: &Arc<Channel>) {
    let dir = mesh.dir();
    if crate::wechat::state::is_disabled(&dir) {
        return;
    }
    // Credential sync: a fresh instance pulls the bound credentials from the
    // shared store (published by whichever instance logged in), so ANY mesh
    // instance can serve wechat without re-scanning a QR on that machine.
    sync_credentials_from_store(mesh, &dir);
    let Some(credentials) = crate::wechat::state::Credentials::load(&dir) else {
        return;
    };
    // Publish the capability fact: presence rows carry it, so /use can
    // verify the target BEFORE handing the channel over.
    mesh.mesh
        .set_channel_capability("wechat", serde_json::json!({ "userId": credentials.user_id }));
    match spawn_bridge_public(mesh, &credentials) {
        Ok(()) => crate::wechat::channel::log("bridge auto-started"),
        Err(error) => crate::wechat::channel::log(&format!(
            "bridge auto-start deferred: {error}; watcher will take over"
        )),
    }
    if !mesh.session_expired() {
        spawn_watcher(Arc::clone(mesh), credentials);
    }
}

/// Pull credentials published to the shared store into the local file.
/// Publishes local credentials the other direction if the store has none
/// (or an older user binding) — whichever instance logged in shares with
/// all of them. No conflict resolution beyond user_id: single-user system.
fn sync_credentials_from_store(mesh: &Arc<Channel>, dir: &Path) {
    let store = mesh.store();
    let published = store
        .channel_state("wechat:credentials")
        .and_then(|state| state.get("credentials").cloned());
    let local = crate::wechat::state::Credentials::load(dir);
    match (published, local) {
        (Some(published), None) => {
            match serde_json::from_value::<Credentials>(published) {
                Ok(credentials) => {
                    if credentials.save(dir).is_ok() {
                        crate::wechat::channel::log(&format!(
                            "wechat: credentials pulled from the store (user={})",
                            credentials.user_id
                        ));
                    }
                }
                Err(error) => crate::wechat::channel::log(&format!(
                    "wechat: published credentials unparseable: {error}"
                )),
            }
        }
        (None, Some(local)) => {
            // This instance holds the only copy: publish it.
            let state = serde_json::json!({ "credentials": serde_json::to_value(&local).unwrap_or_default() });
            store.set_channel_state("wechat:credentials", &state);
            crate::wechat::channel::log("wechat: local credentials published to the store");
        }
        (Some(published), Some(local)) => {
            // Both exist: they should be the same user binding. If the store
            // has a different (presumably newer login), adopt the store's.
            let same_user = published
                .get("credentials")
                .and_then(|c| c.get("userId"))
                .and_then(|u| u.as_str())
                .is_some_and(|u| u == local.user_id);
            if !same_user {
                if let Ok(credentials) = serde_json::from_value::<Credentials>(published) {
                    let old_user = local.user_id.clone();
                    if credentials.save(dir).is_ok() {
                        crate::wechat::channel::log(&format!(
                            "wechat: credentials re-synced from the store ({} → {})",
                            old_user, credentials.user_id
                        ));
                    }
                }
            }
        }
        (None, None) => {}
    }
}

/// Unified shutdown (worker lifecycle): stop owner loop, stop bridge, release
/// the lease. Idempotent.
pub fn shutdown_channel(mesh: &Arc<Channel>) {
    crate::wechat::channel::log("wechat channel shutdown");
    mesh.stop_owner();
    mesh.stop_thread();
    crate::wechat::bridge::release_and_stop(mesh);
}

/// Spawn the bridge thread with a fresh client and channel.
fn spawn_bridge(mesh: &Arc<Channel>, credentials: &Credentials) -> Result<(), String> {
    // Sync the store-resolved display name BEFORE the claim: the store may
    // have renamed us at registration (`-<pid>` suffix on name collision),
    // and the claim/resume writes carry `node_name` — without this the lease
    // row's owner_name is overwritten with the RAW config name (the same
    // base name as the colliding instance), corrupting every holder display.
    if let Some(view) = mesh.store().get_instance_alive(&mesh.node_id()).ok().flatten() {
        mesh.lease_owner.set_node_name(&view.name);
        mesh.set_name(&view.name);
    }
    // Lease: claim (auto) — a held lease from another instance is an error.
    match mesh.claim_wechat().map_err(|e| e.to_string())? {
        rpi_mesh_core::store::Claim::Granted { .. } => {}
        rpi_mesh_core::store::Claim::Held { holder } => {
            // Another instance answers WeChat right now. The STATUS line must
            // say so (🗨️ + holder) — the user's TUI has no other source for
            // this fact, and the old code left a stale `wx ✓ <me>` from a
            // previous run next to the mesh plugin's `🗨️ <holder>`, which read
            // as a contradiction. The bridge does NOT start here.
            mesh.set_status(&rpi_mesh_core::status::status_remote(&holder.full_name()));
            return Err(format!(
                "微信被 {} 持有（/use 切换后重试，或等待其 TTL 过期）",
                holder.owner_name
            ));
        }
    }

    let client =
        IlinkClient::new(&credentials.base_url, &credentials.token).map_err(|e| e.to_string())?;
    mesh.set_credentials(&credentials.base_url, &credentials.token);
    let (sender, receiver) = mpsc::channel::<String>();
    mesh.set_outbound(sender);
    mesh.set_degraded(false);
    mesh.clear_bridge_stop();
    mesh.set_lease_alive(true);
    *mesh.user_id.lock().unwrap_or_else(|p| p.into_inner()) = Some(credentials.user_id.clone());

    // OwnerLoop first: it owns renew/footer from now on. The bridge is
    // started afterwards and reads `lease_alive` (already true), so there is
    // no window where the bridge runs without an owner.
    let owner = crate::wechat::owner::spawn(Arc::clone(mesh));
    mesh.set_owner_thread(owner);

    let ctx = BridgeContext {
        client,
        user_id: credentials.user_id.clone(),
        cursor: state::load_cursor(&mesh.dir()).unwrap_or_default(),
        outbound: receiver,
    };
    let thread_mesh = Arc::clone(&mesh);
    let status_mesh = Arc::clone(mesh);
    let stop_mesh = Arc::clone(mesh);
    std::thread::Builder::new()
        .name("rpi-wechat-bridge".into())
        .spawn(move || {
            let runtime = std::sync::Arc::new(tokio_lite());
            runtime.block_on(async move {
                let reason = bridge::run(thread_mesh, ctx).await;
                crate::wechat::channel::log(&format!("bridge stopped: {reason:?}"));
                stop_mesh.set_bridge_stop(reason);
            });
            status_mesh.mark_stopped();
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
    crate::wechat::channel::log(&format!("cmd /wechat {args:?}"));
    let result = run_command(&args);
    match result {
        Ok(text) => emit(out, &text),
        Err(error) => emit_error(out, error),
    }
}

/// `/wechat login|start|stop|status` — synchronous; QR login runs bounded in
/// this handler (poll loop with a 120 s cap) so the TUI gets one answer.
fn run_command(args: &str) -> Result<String, String> {
    let mesh = mesh()?;
    let sub = args.trim().to_ascii_lowercase();
    let dir = mesh.dir();

    // Bridge already running?
    let running = mesh.bridge_running();

    match sub.as_str() {
        "" | "status" => {
            let credentials = Credentials::load(&dir);
            let holder = mesh
                .lease_owner
                .holder("wechat")
                .map_err(|e| e.to_string())?;
            Ok(format!(
                "实例: {}\n凭据: {}\n桥: {}\n绑定用户: {}\n微信接管: {}\n禁用标志: {}",
                mesh.label(),
                credentials
                    .as_ref()
                    .map(|c| format!("user={}", c.user_id))
                    .unwrap_or_else(|| "未登录（/wechat login）".into()),
                if running { "运行中" } else { "停止" },
                mesh.user_id
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
            mesh.set_session_expired(false);
            login_flow(&mesh)
        }
        "start" => {
            if state::is_disabled(&dir) {
                state::set_disabled(&dir, false).map_err(|e| e.to_string())?;
            }
            mesh.set_session_expired(false);
            if running {
                return Ok("桥已在运行".into());
            }
            let credentials = Credentials::load(&dir).ok_or("未登录：先 /wechat login 扫码绑定")?;
            spawn_bridge(&mesh, &credentials)?;
            Ok(format!("微信桥已启动（{}）", mesh.label()))
        }
        "stop" => {
            state::set_disabled(&dir, true).map_err(|e| e.to_string())?;
            bridge::release_and_stop(&mesh);
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
fn login_flow(mesh: &Arc<Channel>) -> Result<String, String> {
    let client = IlinkClient::new(crate::wechat::ilink::DEFAULT_BASE_URL, "").map_err(|e| e.to_string())?;
    let login_runtime = tokio_lite();
    let (qrcode, login_url) = login_runtime
        .block_on(client.login_qrcode())
        .map_err(|e| e.to_string())?;
    let qr_ascii = crate::wechat::qr::render_terminal(&login_url).map_err(|e| e.to_string())?;

    // Poll on a detached thread; on success persist credentials + start.
    let thread_mesh = Arc::clone(&mesh);
    let thread_client = client.clone();
    let thread_qrcode = qrcode.clone();
    std::thread::Builder::new()
        .name("rpi-wechat-login".into())
        .spawn(move || {
            let runtime = tokio_lite();
            let deadline = std::time::Instant::now() + Duration::from_secs(180);
            loop {
                if thread_mesh.shutdown().requested() || std::time::Instant::now() >= deadline {
                    let _ = thread_mesh.notice("❌ 二维码超时未确认，重新 /wechat login");
                    return;
                }
                let status = match runtime.block_on(thread_client.qrcode_status(&thread_qrcode)) {
                    Ok(status) => status,
                    Err(error) => {
                        thread_mesh.record_error(format!("wechat login poll: {error}"));
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
                                    thread_mesh.record_error("wechat login: missing bot_token");
                                    return;
                                }
                            },
                            base_url: status
                                .base_url
                                .clone()
                                .unwrap_or_else(|| crate::wechat::ilink::DEFAULT_BASE_URL.to_string()),
                            user_id: match status.user_id.clone() {
                                Some(user) => user,
                                None => {
                                    thread_mesh.record_error("wechat login: missing user_id");
                                    return;
                                }
                            },
                            saved_at: now_ms(),
                        };
                        if let Err(error) = credentials.save(&thread_mesh.dir()) {
                            thread_mesh.record_error(format!("wechat login: save failed: {error}"));
                            return;
                        }
                        match spawn_bridge(&thread_mesh, &credentials) {
                            Ok(()) => {
                                let _ = thread_mesh.notice(&format!(
                                    "✓ 微信已绑定 user={}，桥已启动（{}）",
                                    credentials.user_id,
                                    thread_mesh.label()
                                ));
                            }
                            Err(error) => {
                                let _ = thread_mesh.notice(&format!(
                                    "✓ 已绑定 user={}，但桥启动失败: {error}",
                                    credentials.user_id
                                ));
                            }
                        }
                        return;
                    }
                    "expired" => {
                        let _ = thread_mesh.notice("❌ 二维码已过期，重新 /wechat login");
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

/// In the unified plugin, busy tracking lives in `crate::events` (one set of
/// handlers per process). The channel only needs `MessageEnd` to queue
/// assistant replies for outbound delivery.
pub fn event_registry() -> Vec<(EventTag, EventHandlerFn)> {
    vec![(EventTag::MessageEnd, on_message_end)]
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
        if let Some(mesh) = crate::wechat::channel::current() {
            crate::wechat::channel::log(&format!("message_end: queueing reply ({} chars)", text.len()));
            mesh.queue_reply(&text);
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
pub fn spawn_watcher(mesh: Arc<Channel>, credentials: Credentials) {
    let thread_mesh = Arc::clone(&mesh);
    let dir = thread_mesh.dir().to_path_buf();
    std::thread::Builder::new()
        .name("rpi-wechat-watcher".into())
        .spawn(move || {
            // 200ms fact poll (see owner.rs FACT_POLL: cross-dylib
            // Condvars are impossible — each plugin static-links its own
            // copy of rpi-mesh-core globals). An assign envelope to THIS
            // instance becomes a running bridge — and a 💬 footer — within
            // ~0.5s of the /use hand-off; a fenced remote bridge flips the
            // footer to 🗨️ the moment its bridge thread exits.
            loop {
                if thread_mesh.shutdown().requested() || state::is_disabled(&dir) {
                    return;
                }
                // Bridge already running in this process? The OwnerLoop
                // owns the footer while we hold; just yield briefly.
                if thread_mesh.bridge_running() {
                    std::thread::sleep(Duration::from_millis(200));
                    continue;
                }
                if thread_mesh.session_expired() {
                    // Dead credentials: footer shows stopped, no claim churn.
                    thread_mesh.set_status(&rpi_mesh_core::status::status_stopped("登录过期"));
                    std::thread::sleep(Duration::from_millis(200));
                    continue;
                }
                let now = rpi_mesh_core::now_ms();
                match thread_mesh.lease_owner.holder("wechat") {
                    Ok(Some(holder)) => {
                        let owner_present = thread_mesh
                            .store()
                            .get_instance_alive(&holder.owner_node)
                            .ok()
                            .flatten()
                            .is_some();
                        let ours = holder.owner_node == thread_mesh.node_id();
                        let recently_renewed = now - holder.updated_at < 30_000;
                        let unattended =
                            !owner_present || (holder.expires_at <= now && !recently_renewed);
                        if ours && unattended && holder.expires_at <= now {
                            // Our own stale row (owner loop died): take it
                            // back via the normal claim path.
                            start_from_watcher(&thread_mesh, &credentials);
                        } else if !ours && unattended {
                            crate::wechat::channel::log(&format!(
                                "watcher: wechat unattended (owner {} gone/expired) — taking over",
                                holder.full_name()
                            ));
                            start_from_watcher(&thread_mesh, &credentials);
                        } else if !ours {
                            // Held elsewhere and alive: footer from facts.
                            thread_mesh.set_status(&rpi_mesh_core::status::status_remote(
                                &holder.full_name(),
                            ));
                        } else {
                            // Ours but the bridge is not running and the lease
                            // is healthy: start the bridge to match the lease.
                            start_from_watcher(&thread_mesh, &credentials);
                        }
                    }
                    Ok(None) => {
                        // No lease row: the channel is free.
                        start_from_watcher(&thread_mesh, &credentials);
                    }
                    Err(error) => {
                        crate::wechat::channel::log(&format!("watcher: lease read failed: {error}"));
                    }
                }
                std::thread::sleep(Duration::from_millis(200));
            }
        })
        .ok();
}

/// Claim + start the bridge from the watcher; log failures and surface the
/// remote-holder footer on a race loss.
fn start_from_watcher(mesh: &Arc<Channel>, credentials: &Credentials) {
    match spawn_bridge_public(mesh, credentials) {
        Ok(()) => crate::wechat::channel::log("watcher: took over the wechat channel"),
        Err(error) => {
            crate::wechat::channel::log(&format!("watcher: bridge start failed: {error}"));
            // The claim inside spawn_bridge may have raced a holder; show it.
            if let Ok(Some(holder)) = mesh.lease_owner.holder("wechat") {
                if holder.owner_node != mesh.node_id() {
                    mesh.set_status(&rpi_mesh_core::status::status_remote(&holder.full_name()));
                }
            }
        }
    }
}
