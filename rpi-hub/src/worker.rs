//! The hub worker: one thread per plugin load.
//!
//! It owns everything periodic — presence heartbeat, mailbox dispatch, lease
//! renewal, housekeeping — so event handlers and TUI command handlers stay
//! non-blocking (the host runs both synchronously).

use std::sync::Arc;
use std::time::{Duration, Instant};

use rpi_hub_core::now_ms;
use rpi_hub_core::protocol::{Envelope, EnvelopeKind};
use rpi_hub_core::store::Heartbeat;
use rpi_plugin_sdk::RuntimeActionId;

use crate::host::{self, Hub, ALIVE_MS};
use crate::intent;
const HEARTBEAT: Duration = Duration::from_secs(3);
const DISPATCH: Duration = Duration::from_millis(300);
const CONTEXT: Duration = Duration::from_secs(15);
const PRUNE: Duration = Duration::from_secs(60);

/// Longest a `hub_ask` waits for the target's reply.
const ASK_TIMEOUT_MS: i64 = 10 * 60 * 1000;

pub fn run(hub: Arc<Hub>) {
    if let Err(error) = register(&hub) {
        hub.record_error(format!("cannot register instance: {error}"));
        return;
    }
    // The hub slot identifies THIS instance and the relay mode. Local mode
    // shows just the full name (instance count carries no actionable info);
    // relay/client append the mode, since cross-machine behavior changes.
    // 💻 marks the hub (compute node); 📡 marks cross-machine mode — a relay
    // serves other nodes, a client reaches a remote one. Local mode shows
    // just 💻 + identity (no cross-machine fact to announce).
    let mode_glyph = match hub.config.remote.mode {
        rpi_hub_core::config::RemoteMode::Relay => "📡️",
        rpi_hub_core::config::RemoteMode::Client => "💻",
        rpi_hub_core::config::RemoteMode::Local => "🏠",
    };
    hub.set_status(&format!("{} {mode_glyph}", hub.label()));

    let mut next_beat = Instant::now();
    let mut next_dispatch = Instant::now();
    let mut next_context = Instant::now() + CONTEXT;
    let mut next_prune = Instant::now() + PRUNE;

    loop {
        if hub.shutdown().requested() {
            break;
        }
        let now = Instant::now();
        if now >= next_context {
            refresh_context(&hub);
            next_context = now + CONTEXT;
        }
        if now >= next_beat {
            if !heartbeat(&hub) {
                // The epoch is stale: a `/reload` replaced this plugin load.
                host::log("worker stopping: presence epoch superseded");
                break;
            }
            next_beat = now + HEARTBEAT;
        }
        if now >= next_dispatch {
            dispatch(&hub);
            next_dispatch = now + DISPATCH;
        }
        if now >= next_prune {
            housekeeping(&hub);
            next_prune = now + PRUNE;
        }
        hub.shutdown().wait(DISPATCH);
    }

    let _ = hub.store.unregister_instance(&hub.node_id, &hub.epoch);
    hub.set_status("");
}

fn register(hub: &Arc<Hub>) -> Result<(), String> {
    // The register phase already announced us; keep the retry loop for the
    // case where that attempt hit a busy database.
    if hub.registered() {
        return Ok(());
    }
    loop {
        if hub.shutdown().requested() {
            return Err("shutdown requested".into());
        }
        match hub.register_presence() {
            Ok(()) => return Ok(()),
            Err(error) => {
                if hub.shutdown().requested() {
                    return Err(error.to_string());
                }
                host::log(&format!("register retry: {error}"));
                hub.shutdown().wait(Duration::from_secs(1));
            }
        }
    }
}

fn refresh_context(hub: &Arc<Hub>) {
    let Ok(result) = hub.action(RuntimeActionId::GetExecutionContext, serde_json::json!({})) else {
        return;
    };
    let mut snapshot = hub.session_snapshot();
    if let Some(session) = result.get("sessionId").and_then(|v| v.as_str()) {
        snapshot.session_id = Some(session.to_string());
    }
    if let Some(model) = result.get("model").and_then(|v| v.as_str()) {
        snapshot.model = Some(model.to_string());
    }
    if let Some(cwd) = result.get("cwd").and_then(|v| v.as_str()) {
        snapshot.cwd = Some(cwd.to_string());
    }
    hub.update_sessions(snapshot);
}

fn heartbeat(hub: &Arc<Hub>) -> bool {
    let now = now_ms();
    let snapshot = hub.session_snapshot();
    let beat = Heartbeat {
        busy: hub.busy(),
        session_id: snapshot.session_id,
        title: None,
        model: snapshot.model,
    };
    match hub.store.heartbeat(&hub.node_id, &hub.epoch, now, &beat) {
        Ok(alive) => alive,
        Err(error) => {
            host::log(&format!("heartbeat failed: {error}"));
            true
        }
    }
}

fn dispatch(hub: &Arc<Hub>) {
    let now = now_ms();
    let envelopes = match hub.store.inbox(&hub.node_id, now, hub.inbox_batch()) {
        Ok(envelopes) => envelopes,
        Err(error) => {
            hub.record_error(format!("mailbox read failed: {error}"));
            return;
        }
    };
    for envelope in envelopes {
        // A `reload` intent must be answered *before* it tears this load down.
        if envelope.kind == EnvelopeKind::Cmd
            && envelope
                .command
                .as_ref()
                .is_some_and(|command| command.name == "reload")
        {
            let _ = hub.store.ack_envelope(&envelope.id, &hub.node_id, now_ms());
            reply(hub, &envelope, true, "reloading…".to_string());
            host::log("reloading on request");
            let _ = hub.action(RuntimeActionId::Reload, serde_json::json!({}));
            hub.request_shutdown();
            return;
        }

        match envelope.kind {
            EnvelopeKind::Text => handle_text(hub, &envelope),
            EnvelopeKind::Cmd => handle_command(hub, &envelope),
            EnvelopeKind::Reply => handle_reply(hub, &envelope),
            EnvelopeKind::Notice => {
                let text = envelope.text.clone().unwrap_or_default();
                let _ = hub.notice(&format!("{}: {text}", envelope.from_name));
            }
            EnvelopeKind::Assign => handle_assign(hub, &envelope, true),
            EnvelopeKind::Unassign => handle_assign(hub, &envelope, false),
        }
        if let Err(error) = hub.store.ack_envelope(&envelope.id, &hub.node_id, now_ms()) {
            host::log(&format!("ack failed: {error}"));
        }
    }
}

fn handle_text(hub: &Arc<Hub>, envelope: &Envelope) {
    let text = envelope.text.clone().unwrap_or_default();
    // Make the incoming /msg visible in the target transcript: steer messages
    // ride the agent queue (and may wait behind the active turn), which read
    // as "nothing happened" in the target window. A display-only notice gives
    // immediate, model-free visibility.
    let _ = hub.notice(&format!("📨 {}: {}", envelope.from_name, text));
    if let Err(error) = hub.send_user_message(&text, Some("steer")) {
        hub.record_error(format!("cannot deliver message: {error}"));
        reply(hub, envelope, false, format!("投递失败: {error}"));
        return;
    }
    if let Some(correlation) = &envelope.correlation {
        let pending = hub.pending.lock().unwrap_or_else(|p| p.into_inner()).len();
        // Bound the pending list: stale asks must not grow without limit.
        if pending > 16 {
            hub.pending
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .remove(0);
        }
        hub.pending
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .push(host::PendingAsk {
                correlation: correlation.clone(),
                to_node: envelope.from_node.clone(),
                to_name: envelope.from_name.clone(),
                created_at: now_ms(),
                deadline: now_ms() + ASK_TIMEOUT_MS,
            });
    }
}

fn handle_command(hub: &Arc<Hub>, envelope: &Envelope) {
    let Some(command) = envelope.command.clone() else {
        reply(hub, envelope, false, "空指令".into());
        return;
    };
    match intent::execute(hub, &command) {
        Ok(text) => reply(hub, envelope, true, text),
        Err(error) => reply(hub, envelope, false, error),
    }
}

/// `assign`/`unassign` envelopes from the LeaseAdmin path (`/use`, phone
/// `/use N`, hub_switch): display the hand-off as a transcript notice.
/// (No cross-plugin wake-up here: the two dylibs static-link their own copy
/// of any process-global, so a signal from this crate would never reach the
/// wechat plugin's threads. The OwnerLoop/watcher poll the lease row every
/// 200ms instead — sub-second, simple, honest.)
fn handle_assign(hub: &Arc<Hub>, envelope: &Envelope, assigned: bool) {
    // The envelope text already carries the user-facing sentence ("wechat
    // 已交给你…" / "…已移交 <新持有者>…"); show it verbatim instead of
    // recomposing from fields (the old code displayed the SENDER —
    // "lease-admin" — as the new holder).
    let text = envelope.text.clone().unwrap_or_default();
    let text = if text.is_empty() {
        let capability = envelope.capability.clone().unwrap_or_default();
        if assigned {
            format!("✅ {capability} 接管已交给本实例")
        } else {
            format!("↗️ {capability} 已移交新持有者")
        }
    } else {
        text
    };
    let _ = hub.notice(&text);
}

fn handle_reply(hub: &Arc<Hub>, envelope: &Envelope) {
    let Some(correlation) = envelope.correlation.clone() else {
        return;
    };
    let ok = envelope.ok.unwrap_or(true);
    let text = envelope.text.clone().unwrap_or_default();
    let outcome = host::ReplyOutcome {
        ok,
        text: text.clone(),
        at: now_ms(),
    };
    if hub.answer_reply(&correlation, outcome) {
        return;
    }
    // Nobody is waiting (the asker may have restarted): surface it instead of
    // dropping the result on the floor.
    let _ = hub.notice(&format!(
        "来自 {} 的结果:\n{}",
        envelope.from_name,
        if text.is_empty() { "(空)" } else { &text }
    ));
}

fn reply(hub: &Arc<Hub>, envelope: &Envelope, ok: bool, text: String) {
    let Some(correlation) = envelope.correlation.clone() else {
        // Uncorrelated command: report through the local transcript.
        let _ = hub.notice(&format!("{} {}", if ok { "✓" } else { "✗" }, text));
        return;
    };
    let reply = Envelope::reply(
        &hub.node_id,
        &hub.name(),
        &envelope.from_node,
        correlation,
        text,
        ok,
        now_ms(),
    );
    if let Err(error) = hub.store.send_envelope(&reply) {
        host::log(&format!("reply delivery failed: {error}"));
    }
}

fn housekeeping(hub: &Arc<Hub>) {
    // Same-host dead pids go first: a C-c'd process must stop gating the
    // dead-owner lease takeover for the rest of the heartbeat window.
    if let Ok(dropped) = hub.store.reap_dead_local_instances() {
        if dropped > 0 {
            host::log(&format!("reaped {dropped} dead local presence rows"));
        }
    }
    match hub.store.prune(now_ms(), ALIVE_MS) {
        Ok(report) => {
            if report.instances + report.envelopes + report.seen > 0 {
                host::log(&format!(
                    "pruned {} instances, {} envelopes, {} seen ids",
                    report.instances, report.envelopes, report.seen
                ));
            }
        }
        Err(error) => host::log(&format!("prune failed: {error}")),
    }
}

// ---------------------------------------------------------------------------
// Lifecycle hooks
// ---------------------------------------------------------------------------

/// Stop accepting work. Must not block or join: the host calls it from a
/// blocking thread while our FFI calls may still be in flight.
pub extern "C" fn begin_shutdown(_user_data: *mut std::ffi::c_void) {
    if let Some(hub) = host::current() {
        hub.request_shutdown();
    }
}

/// Join the worker. Returning nonzero makes the host keep the library mapped
/// forever, so only do that when the worker really could not be stopped.
pub extern "C" fn finish_shutdown(_user_data: *mut std::ffi::c_void) -> i32 {
    let Some(hub) = host::current() else {
        return 0;
    };
    hub.request_shutdown();
    hub.stop_worker();
    let _ = hub.store.unregister_instance(&hub.node_id, &hub.epoch);
    hub.stop_backends();
    let _ = hub.action(
        RuntimeActionId::SetStatus,
        serde_json::json!({ "key": "hub", "value": "" }),
    );
    host::clear_current();
    0
}
