//! The mesh worker: one thread per plugin load.
//!
//! It owns everything periodic — presence heartbeat, mailbox dispatch, lease
//! renewal, housekeeping — so event handlers and TUI command handlers stay
//! non-blocking (the host runs both synchronously).

use std::sync::Arc;
use std::time::{Duration, Instant};

use rpi_mesh_core::api::MeshStore;
use rpi_mesh_core::now_ms;
use rpi_mesh_core::protocol::{Envelope, EnvelopeKind};
use rpi_mesh_core::store::Heartbeat;
use rpi_plugin_sdk::RuntimeActionId;

use crate::mesh::{self, Mesh, ALIVE_MS};
use crate::intent;
const HEARTBEAT: Duration = Duration::from_secs(3);
const DISPATCH: Duration = Duration::from_millis(300);
const CONTEXT: Duration = Duration::from_secs(15);
const PRUNE: Duration = Duration::from_secs(60);

pub fn run(mesh: Arc<Mesh>) {
    if let Err(error) = register(&mesh) {
        mesh.record_error(format!("cannot register instance: {error}"));
        return;
    }
    // The mesh slot identifies THIS instance and the relay mode. Local mode
    // shows just the full name (instance count carries no actionable info);
    // relay/client append the mode, since cross-machine behavior changes.
    // 📡 marks cross-machine participation (host serving guests / guest
    // reaching a host); 🏠 is pure local — no cross-machine fact to announce.
    let mode_glyph = if mesh.config.remote.role.is_some() {
        "📡️"
    } else {
        "🏠"
    };
    mesh.set_status(&format!("{} {mode_glyph}", mesh.label()));

    let mut next_beat = Instant::now();
    let mut next_dispatch = Instant::now();
    let mut next_context = Instant::now() + CONTEXT;
    let mut next_prune = Instant::now() + PRUNE;

    loop {
        if mesh.shutdown().requested() {
            break;
        }
        let now = Instant::now();
        if now >= next_context {
            refresh_context(&mesh);
            next_context = now + CONTEXT;
        }
        if now >= next_beat {
            if !heartbeat(&mesh) {
                // The epoch is stale: a `/reload` replaced this plugin load.
                mesh::log("worker stopping: presence epoch superseded");
                break;
            }
            next_beat = now + HEARTBEAT;
        }
        if now >= next_dispatch {
            dispatch(&mesh);
            next_dispatch = now + DISPATCH;
        }
        if now >= next_prune {
            housekeeping(&mesh);
            next_prune = now + PRUNE;
        }
        mesh.shutdown().wait(DISPATCH);
    }

    let _ = Arc::clone(&mesh.store).unregister_instance(&mesh.node_id, &mesh.epoch);
    mesh.set_status("");
}

fn register(mesh: &Arc<Mesh>) -> Result<(), String> {
    // The register phase already announced us; keep the retry loop for the
    // case where that attempt hit a busy database.
    if mesh.registered() {
        return Ok(());
    }
    loop {
        if mesh.shutdown().requested() {
            return Err("shutdown requested".into());
        }
        match mesh.register_presence() {
            Ok(()) => return Ok(()),
            Err(error) => {
                if mesh.shutdown().requested() {
                    return Err(error.to_string());
                }
                mesh::log(&format!("register retry: {error}"));
                mesh.shutdown().wait(Duration::from_secs(1));
            }
        }
    }
}

fn refresh_context(mesh: &Arc<Mesh>) {
    let Ok(result) = mesh.action(RuntimeActionId::GetExecutionContext, serde_json::json!({})) else {
        return;
    };
    let mut snapshot = mesh.session_snapshot();
    if let Some(session) = result.get("sessionId").and_then(|v| v.as_str()) {
        snapshot.session_id = Some(session.to_string());
    }
    if let Some(model) = result.get("model").and_then(|v| v.as_str()) {
        snapshot.model = Some(model.to_string());
    }
    if let Some(cwd) = result.get("cwd").and_then(|v| v.as_str()) {
        snapshot.cwd = Some(cwd.to_string());
    }
    mesh.update_sessions(snapshot);
}

fn heartbeat(mesh: &Arc<Mesh>) -> bool {
    let now = now_ms();
    let snapshot = mesh.session_snapshot();
    let beat = Heartbeat {
        busy: mesh.busy(),
        session_id: snapshot.session_id,
        title: None,
        model: snapshot.model,
    };
    match mesh.store_dyn().heartbeat(&mesh.node_id, &mesh.epoch, now, &beat) {
        Ok(alive) => alive,
        Err(error) => {
            mesh::log(&format!("heartbeat failed: {error}"));
            true
        }
    }
}

fn dispatch(mesh: &Arc<Mesh>) {
    let now = now_ms();
    let envelopes = match mesh.store_dyn().inbox(&mesh.node_id, now, mesh.inbox_batch()) {
        Ok(envelopes) => envelopes,
        Err(error) => {
            mesh.record_error(format!("mailbox read failed: {error}"));
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
            let _ = mesh.store.ack_envelope(&envelope.id, &mesh.node_id, now_ms());
            reply(mesh, &envelope, true, "reloading…".to_string());
            mesh::log("reloading on request");
            let _ = mesh.action(RuntimeActionId::Reload, serde_json::json!({}));
            mesh.request_shutdown();
            return;
        }

        match envelope.kind {
            EnvelopeKind::Text => handle_text(mesh, &envelope),
            EnvelopeKind::Cmd => handle_command(mesh, &envelope),
            EnvelopeKind::Reply => handle_reply(mesh, &envelope),
            EnvelopeKind::Notice => {
                let text = envelope.text.clone().unwrap_or_default();
                let _ = mesh.notice(&format!("{}: {text}", envelope.from_name));
            }
            EnvelopeKind::Assign => handle_assign(mesh, &envelope, true),
            EnvelopeKind::Unassign => handle_assign(mesh, &envelope, false),
        }
        if let Err(error) = mesh.store.ack_envelope(&envelope.id, &mesh.node_id, now_ms()) {
            mesh::log(&format!("ack failed: {error}"));
        }
    }
}

fn handle_text(mesh: &Arc<Mesh>, envelope: &Envelope) {
    let text = envelope.text.clone().unwrap_or_default();
    // Make the incoming /msg visible in the target transcript: steer messages
    // ride the agent queue (and may wait behind the active turn), which read
    // as "nothing happened" in the target window. A display-only notice gives
    // immediate, model-free visibility.
    let _ = mesh.notice(&format!("📨 {}: {}", envelope.from_name, text));

    // Correlated asks use the host's SYNCHRONOUS run outcome instead of the
    // pending-queue + MessageEnd guessing: `SendUserMessage` blocks until the
    // run settles and returns the final text. This kills both the silent-
    // loss bug (an ask whose injection never starts a run used to pend for
    // 10 minutes then die) and the FIFO misattribution bug (a reply landing
    // on the OLDEST pending ask instead of its own).
    //
    // This runs on the worker thread; a long ask occupies it, but the worker
    // is the only mailbox drainer anyway — dispatch() is sequential by
    // design, and a concurrent /msg to this instance just queues behind.
    if envelope.correlation.is_some() {
        let _ = mesh.notice("（ask 任务已开始，回复会自动传回发起方）");
        match mesh.send_user_message_outcome(&text, Some("steer")) {
            Ok(outcome) => {
                let status = outcome.get("status").and_then(|v| v.as_str()).unwrap_or("");
                match status {
                    "completed" => {
                        let text = outcome
                            .get("text")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string();
                        reply(mesh, envelope, true, text);
                    }
                    "aborted" | "error" => {
                        let reason = outcome
                            .get("stopReason")
                            .and_then(|v| v.as_str())
                            .or_else(|| outcome.get("status").and_then(|v| v.as_str()))
                            .unwrap_or("unknown")
                            .to_string();
                        reply(mesh, envelope, false, format!("目标回合异常结束: {reason}"));
                    }
                    // Queued behind an active run / appended without a run:
                    // the host gives no completion signal we could wait on.
                    // Fail fast with the truth instead of hanging silently.
                    other => {
                        let detail = if other.is_empty() {
                            String::new()
                        } else {
                            format!("（宿主状态: {other}）")
                        };
                        reply(
                            mesh,
                            envelope,
                            false,
                            format!("目标正忙，消息已排队而非立即执行{}；请稍后重发", detail),
                        );
                    }
                }
            }
            Err(error) => {
                reply(mesh, envelope, false, format!("投递失败: {error}"));
            }
        }
        return;
    }

    // Plain /msg: fire-and-forget, never blocks the mailbox on a run.
    if let Err(error) = mesh.send_user_message(&text, Some("steer")) {
        mesh.record_error(format!("cannot deliver message: {error}"));
        reply(mesh, envelope, false, format!("投递失败: {error}"));
    }
}

fn handle_command(mesh: &Arc<Mesh>, envelope: &Envelope) {
    let Some(command) = envelope.command.clone() else {
        reply(mesh, envelope, false, "空指令".into());
        return;
    };
    match intent::execute(mesh, &command) {
        Ok(text) => reply(mesh, envelope, true, text),
        Err(error) => reply(mesh, envelope, false, error),
    }
}

/// `assign`/`unassign` envelopes from the LeaseAdmin path (`/use`, phone
/// `/use N`, mesh_switch): display the hand-off as a transcript notice.
/// (No cross-plugin wake-up here: the two dylibs static-link their own copy
/// of any process-global, so a signal from this crate would never reach the
/// wechat plugin's threads. The OwnerLoop/watcher poll the lease row every
/// 200ms instead — sub-second, simple, honest.)
fn handle_assign(mesh: &Arc<Mesh>, envelope: &Envelope, assigned: bool) {
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
    let _ = mesh.notice(&text);
}

fn handle_reply(mesh: &Arc<Mesh>, envelope: &Envelope) {
    let Some(correlation) = envelope.correlation.clone() else {
        return;
    };
    let ok = envelope.ok.unwrap_or(true);
    let text = envelope.text.clone().unwrap_or_default();
    let outcome = mesh::ReplyOutcome {
        ok,
        text: text.clone(),
        at: now_ms(),
    };
    if mesh.answer_reply(&correlation, outcome) {
        return;
    }
    // Nobody is waiting (the asker may have restarted): surface it instead of
    // dropping the result on the floor.
    let _ = mesh.notice(&format!(
        "来自 {} 的结果:\n{}",
        envelope.from_name,
        if text.is_empty() { "(空)" } else { &text }
    ));
}

fn reply(mesh: &Arc<Mesh>, envelope: &Envelope, ok: bool, text: String) {
    let Some(correlation) = envelope.correlation.clone() else {
        // Uncorrelated command: report through the local transcript.
        let _ = mesh.notice(&format!("{} {}", if ok { "✓" } else { "✗" }, text));
        return;
    };
    let reply = Envelope::reply(
        &mesh.node_id,
        &mesh.name(),
        &envelope.from_node,
        correlation,
        text,
        ok,
        now_ms(),
    );
    if let Err(error) = mesh.store.send_envelope(&reply) {
        mesh::log(&format!("reply delivery failed: {error}"));
    }
}

fn housekeeping(mesh: &Arc<Mesh>) {
    // Same-host dead pids go first: a C-c'd process must stop gating the
    // dead-owner lease takeover for the rest of the heartbeat window.
    if let Ok(dropped) = mesh.store.reap_dead_local_instances() {
        if dropped > 0 {
            mesh::log(&format!("reaped {dropped} dead local presence rows"));
        }
    }
    match mesh.store_dyn().prune(now_ms(), ALIVE_MS) {
        Ok(report) => {
            if report.instances + report.envelopes + report.seen > 0 {
                mesh::log(&format!(
                    "pruned {} instances, {} envelopes, {} seen ids",
                    report.instances, report.envelopes, report.seen
                ));
            }
        }
        Err(error) => mesh::log(&format!("prune failed: {error}")),
    }
}

// ---------------------------------------------------------------------------
// Lifecycle hooks
// ---------------------------------------------------------------------------

/// Stop accepting work. Must not block or join: the host calls it from a
/// blocking thread while our FFI calls may still be in flight.
pub extern "C" fn begin_shutdown(_user_data: *mut std::ffi::c_void) {
    if let Some(mesh) = mesh::current() {
        mesh.request_shutdown();
    }
}

/// Join the worker. Returning nonzero makes the host keep the library mapped
/// forever, so only do that when the worker really could not be stopped.
pub extern "C" fn finish_shutdown(_user_data: *mut std::ffi::c_void) -> i32 {
    let Some(mesh) = mesh::current() else {
        return 0;
    };
    mesh.request_shutdown();
    mesh.stop_worker();
    let _ = Arc::clone(&mesh.store).unregister_instance(&mesh.node_id, &mesh.epoch);
    mesh.stop_backends();
    let _ = mesh.action(
        RuntimeActionId::SetStatus,
        serde_json::json!({ "key": "mesh", "value": "" }),
    );
    mesh::clear_current();
    0
}
