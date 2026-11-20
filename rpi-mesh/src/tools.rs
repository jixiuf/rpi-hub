//! Tools exposed to the model.
//!
//! Two shapes share the ABI lifecycle:
//! - **sync** (`mesh_instances`, `mesh_send`, `mesh_switch`): the outcome is known
//!   in `execute`, `poll` reports it once.
//! - **wait** (`mesh_ask`): `execute` sends the envelope and returns a
//!   handle holding a correlation id; `poll` checks the reply mailbox without
//!   blocking, until the deadline.

use std::sync::Arc;

use rpi_mesh_core::protocol::Envelope;
use rpi_mesh_core::store::LeaseHolder;
use rpi_mesh_core::now_ms;
use rpi_plugin_sdk::callbacks::free_string;
use rpi_plugin_sdk::handles::ToolHandles;
use rpi_plugin_sdk::{
    PluginApi, StableToolSchema, StepHandle, StepResult, StbString, ToolPartialCb,
};

use crate::commands::{forward_start, forward_stop, spawn_ctx_from};
use crate::mesh::{self, Mesh, ALIVE_MS};

const DEFAULT_ASK_TIMEOUT_S: u64 = 300;
const MAX_ASK_TIMEOUT_S: u64 = 900;
const POLL_INTERVAL_MS: i64 = 250;

// ---------------------------------------------------------------------------
// Drives
// ---------------------------------------------------------------------------

struct SyncDrive {
    outcome: Option<Result<String, String>>,
}

struct WaitDrive {
    correlation: String,
    deadline: i64,
    /// Last time `poll` looked; throttles SQLite/reply-map reads.
    last_check: i64,
}

static SYNC: ToolHandles<SyncDrive> = ToolHandles::new();
static WAIT: ToolHandles<WaitDrive> = ToolHandles::new();

fn result_json(text: &str) -> StbString {
    StbString::from_string(
        serde_json::json!({
            "content": [{ "type": "text", "text": text }],
            "details": {}
        })
        .to_string(),
    )
}

fn error_result(message: String) -> StepResult {
    StepResult::err(StbString::from_string(message))
}

// ---------------------------------------------------------------------------
// Sync lifecycle
// ---------------------------------------------------------------------------

extern "C" fn sync_poll(
    handle: StepHandle,
    _partial: Option<ToolPartialCb>,
    _user_data: *mut std::ffi::c_void,
) -> StepResult {
    rpi_plugin_sdk::guard(|| {
        let Some(entry) = SYNC.get(handle) else {
            return error_result("invalid or destroyed mesh handle".into());
        };
        if entry.cancelled.load(std::sync::atomic::Ordering::Acquire) {
            return error_result("cancelled".into());
        }
        let outcome = { entry.lock().outcome.take() };
        match outcome {
            Some(Ok(text)) => StepResult::done(result_json(&text)),
            Some(Err(error)) => error_result(error),
            None => StepResult::pending(StbString::empty()),
        }
    })
    .unwrap_or_else(|_| error_result("rpi-mesh sync poll panicked".into()))
}

extern "C" fn sync_cancel(handle: StepHandle) {
    SYNC.cancel(handle);
}

extern "C" fn sync_destroy(handle: StepHandle) {
    drop(SYNC.remove(handle));
}

// ---------------------------------------------------------------------------
// Wait lifecycle
// ---------------------------------------------------------------------------

extern "C" fn wait_poll(
    handle: StepHandle,
    _partial: Option<ToolPartialCb>,
    _user_data: *mut std::ffi::c_void,
) -> StepResult {
    rpi_plugin_sdk::guard(|| {
        let Some(entry) = WAIT.get(handle) else {
            return error_result("invalid or destroyed wait handle".into());
        };
        if entry.cancelled.load(std::sync::atomic::Ordering::Acquire) {
            return error_result("cancelled".into());
        }
        let now = now_ms();
        let (correlation, deadline, check) = {
            let mut drive = entry.lock();
            let check = now - drive.last_check >= POLL_INTERVAL_MS;
            if check {
                drive.last_check = now;
            }
            (drive.correlation.clone(), drive.deadline, check)
        };

        if check {
            let mesh = match mesh::current() {
                Some(mesh) => mesh,
                None => return error_result("rpi-mesh is not loaded".into()),
            };
            let outcome = mesh.take_reply(&correlation);
            if let Some(reply) = outcome {
                WAIT.remove(handle);
                return if reply.ok {
                    StepResult::done(result_json(&reply.text))
                } else {
                    error_result(reply.text)
                };
            }
        }

        if now >= deadline {
            WAIT.remove(handle);
            if let Some(mesh) = mesh::current() {
                mesh.forget_reply(&correlation);
            }
            return error_result("等待目标实例回复超时".into());
        }
        StepResult::pending(StbString::empty())
    })
    .unwrap_or_else(|_| error_result("rpi-mesh wait poll panicked".into()))
}

extern "C" fn wait_cancel(handle: StepHandle) {
    WAIT.cancel(handle);
}

extern "C" fn wait_destroy(handle: StepHandle) {
    drop(WAIT.remove(handle));
}

// ---------------------------------------------------------------------------
// Registration
// ---------------------------------------------------------------------------

struct ToolDef {
    name: &'static str,
    description: &'static str,
    parameters: &'static str,
    execute: rpi_plugin_sdk::ToolExecuteFn,
    poll: rpi_plugin_sdk::ToolPollFn,
    cancel: rpi_plugin_sdk::ToolCancelFn,
    destroy: rpi_plugin_sdk::ToolDestroyFn,
}

pub fn register(mesh: &Arc<Mesh>, api: &PluginApi) -> Result<(), String> {
    let Some(register_tool) = api.register_tool else {
        return Err("host has no tool registrar".into());
    };
    let _ = mesh;
    for def in definitions() {
        let schema = Box::new(StableToolSchema {
            name: StbString::from_string(def.name.to_string()),
            description: StbString::from_string(def.description.to_string()),
            parameters: StbString::from_string(def.parameters.to_string()),
        });
        let rc = register_tool(
            schema.as_ref(),
            def.execute,
            def.poll,
            def.cancel,
            def.destroy,
            free_string,
        );
        if rc != 0 {
            return Err(format!("tool {} rejected by host (rc={rc})", def.name));
        }
    }
    Ok(())
}

fn definitions() -> Vec<ToolDef> {
    vec![
        ToolDef {
            name: "mesh_instances",
            description: "List running rpi instances (this machine and remote), which one owns the WeChat channel, and each instance's session/busy state.",
            parameters: r#"{"type":"object","properties":{},"additionalProperties":false}"#,
            execute: instances_execute,
            poll: sync_poll,
            cancel: sync_cancel,
            destroy: sync_destroy,
        },
        ToolDef {
            name: "mesh_send",
            description: "Send a message into another rpi instance's agent lane. Use it to hand work to an instance that runs in a different network (office vs server) and let it act with its own environment.",
            parameters: r#"{"type":"object","properties":{"target":{"type":"string","description":"Instance name, name@host, or node id prefix"},"text":{"type":"string","description":"Message for the target agent"}},"required":["target","text"],"additionalProperties":false}"#,
            execute: send_execute,
            poll: sync_poll,
            cancel: sync_cancel,
            destroy: sync_destroy,
        },
        ToolDef {
            name: "mesh_ask",
            description: "Delegate a task to another rpi instance and wait for its answer. The target works in its own session, network and filesystem, then returns the assistant's final text.",
            parameters: r#"{"type":"object","properties":{"target":{"type":"string"},"task":{"type":"string"},"timeoutSeconds":{"type":"integer","minimum":10,"maximum":900}},"required":["target","task"],"additionalProperties":false}"#,
            execute: ask_execute,
            poll: wait_poll,
            cancel: wait_cancel,
            destroy: wait_destroy,
        },
        ToolDef {
            name: "mesh_switch",
            description: "Move ownership of a channel (default wechat) to another instance. Only the owner talks to WeChat, so this is how the mobile connection follows you between machines.",
            parameters: r#"{"type":"object","properties":{"target":{"type":"string"},"capability":{"type":"string","description":"channel id, default wechat"}},"required":["target"],"additionalProperties":false}"#,
            execute: switch_execute,
            poll: sync_poll,
            cancel: sync_cancel,
            destroy: sync_destroy,
        },
        ToolDef {
            name: "mesh_start",
            description: "Start a new rpi instance. Without 'executor' it starts on THIS machine in tmux; with 'executor' (any running instance) the start runs on THAT instance's machine instead.",
            parameters: r#"{"type":"object","properties":{"dir":{"type":"string","description":"Working directory for the new instance (default: executor's cwd)"},"executor":{"type":"string","description":"Instance that performs the start; its machine hosts the new instance"}},"additionalProperties":false}"#,
            execute: start_execute,
            poll: sync_poll,
            cancel: sync_cancel,
            destroy: sync_destroy,
        },
        ToolDef {
            name: "mesh_stop",
            description: "Stop a running rpi instance. Without 'executor' the target must be on THIS machine; with 'executor' the stop runs on THAT instance's machine (for remote targets).",
            parameters: r#"{"type":"object","properties":{"target":{"type":"string","description":"Instance to stop (name or /instances row number)"},"executor":{"type":"string","description":"Instance that performs the stop; its machine must host the target"}},"required":["target"],"additionalProperties":false}"#,
            execute: stop_execute,
            poll: sync_poll,
            cancel: sync_cancel,
            destroy: sync_destroy,
        },
    ]
}

// ---------------------------------------------------------------------------
// Tool bodies
// ---------------------------------------------------------------------------

fn require_mesh() -> Result<Arc<Mesh>, String> {
    mesh::current().ok_or_else(|| "rpi-mesh is not initialized".to_string())
}

fn parse_params(params: &str) -> serde_json::Value {
    serde_json::from_str(params).unwrap_or(serde_json::Value::Null)
}

fn param_str<'a>(params: &'a serde_json::Value, key: &str) -> Result<&'a str, String> {
    params
        .get(key)
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .ok_or_else(|| format!("缺参数 {key}"))
}

rpi_plugin_sdk::execute_handler! {
    fn instances_execute(_call_id, params) {
        let outcome = run_instances(&params.to_string_lossy());
        SYNC.insert(SyncDrive { outcome: Some(outcome) })
    }
}

fn run_instances(_params: &str) -> Result<String, String> {
    let mesh = require_mesh()?;
    mesh.format_instances(now_ms())
}

rpi_plugin_sdk::execute_handler! {
    fn send_execute(_call_id, params) {
        let outcome = run_send(&params.to_string_lossy());
        SYNC.insert(SyncDrive { outcome: Some(outcome) })
    }
}

fn run_send(params: &str) -> Result<String, String> {
    let mesh = require_mesh()?;
    let params = parse_params(params);
    let target = mesh.resolve_target(param_str(&params, "target")?)?;
    let text = param_str(&params, "text")?;
    let envelope =
        Envelope::text(&mesh.node_id, &mesh.name(), &target.node_id, text, now_ms())
            .map_err(|e| e.to_string())?;
    mesh.deliver(envelope)?;
    Ok(format!("已发送给 {}", target.label()))
}


rpi_plugin_sdk::execute_handler! {
    fn ask_execute(_call_id, params) {
        match run_ask(&params.to_string_lossy()) {
            Ok((correlation, timeout_ms)) => WAIT.insert(WaitDrive {
                correlation,
                deadline: now_ms() + timeout_ms,
                last_check: 0,
            }),
            Err(error) => SYNC.insert(SyncDrive { outcome: Some(Err(error)) }),
        }
    }
}

fn run_ask(params: &str) -> Result<(String, i64), String> {
    let mesh = require_mesh()?;
    let params = parse_params(params);
    let target = mesh.resolve_target(param_str(&params, "target")?)?;
    let task = param_str(&params, "task")?;
    let timeout = params
        .get("timeoutSeconds")
        .and_then(|v| v.as_u64())
        .unwrap_or(DEFAULT_ASK_TIMEOUT_S)
        .clamp(10, MAX_ASK_TIMEOUT_S);

    let correlation = rpi_mesh_core::protocol::new_id();
    let envelope = Envelope::text(&mesh.node_id, &mesh.name(), &target.node_id, task, now_ms())
        .map_err(|e| e.to_string())?
        .with_correlation(&correlation);
    mesh.expect_reply(&correlation);
    mesh.deliver(envelope)?;
    Ok((correlation, (timeout as i64) * 1000))
}


rpi_plugin_sdk::execute_handler! {
    fn switch_execute(_call_id, params) {
        let outcome = run_switch(&params.to_string_lossy());
        SYNC.insert(SyncDrive { outcome: Some(outcome) })
    }
}

rpi_plugin_sdk::execute_handler! {
    fn start_execute(_call_id, params) {
        let outcome = run_start(&params.to_string_lossy());
        SYNC.insert(SyncDrive { outcome: Some(outcome) })
    }
}

rpi_plugin_sdk::execute_handler! {
    fn stop_execute(_call_id, params) {
        let outcome = run_stop(&params.to_string_lossy());
        SYNC.insert(SyncDrive { outcome: Some(outcome) })
    }
}

fn run_switch(params: &str) -> Result<String, String> {
    let mesh = require_mesh()?;
    let params = parse_params(params);
    let target = mesh.resolve_target(param_str(&params, "target")?)?;
    let capability = params
        .get("capability")
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .unwrap_or("wechat");
    let admin = rpi_mesh_core::lease::LeaseAdmin::new(
        mesh.store_dyn(),
        mesh.config.lease.ttl_ms(),
    );
    let generation = admin.assign_to(capability, &target, now_ms())?;
    Ok(describe_transfer(capability, &target.label(), generation))
}

fn run_start(params: &str) -> Result<String, String> {
    let mesh = require_mesh()?;
    let params = parse_params(params);
    let dir = params.get("dir").and_then(|v| v.as_str()).map(str::trim);
    match params.get("executor").and_then(|v| v.as_str()).map(str::trim) {
        Some(executor_query) if !executor_query.is_empty() => {
            // Cross-machine: forward to the executor instance.
            let executor = mesh.resolve_target(executor_query)?;
            let cwd = dir.unwrap_or("").to_string();
            crate::commands::forward_start(&mesh, &executor, &cwd)
        }
        _ => {
            let spawn_ctx = spawn_ctx_from(&mesh);
            rpi_mesh_core::instance_start::start(&spawn_ctx, dir)
        }
    }
}

fn run_stop(params: &str) -> Result<String, String> {
    let mesh = require_mesh()?;
    let params = parse_params(params);
    let target_query = param_str(&params, "target")?;
    match params.get("executor").and_then(|v| v.as_str()).map(str::trim) {
        Some(executor_query) if !executor_query.is_empty() => {
            // Cross-machine: resolve the target HERE, forward its unique label.
            let executor = mesh.resolve_target(executor_query)?;
            let target = mesh.resolve_target(target_query)?;
            crate::commands::forward_stop(&mesh, &executor, &target.label())
        }
        _ => {
            let spawn_ctx = spawn_ctx_from(&mesh);
            rpi_mesh_core::instance_start::stop(&spawn_ctx, target_query, rpi_mesh_core::now_ms())
        }
    }
}

fn describe_transfer(capability: &str, label: &str, generation: i64) -> String {
    format!("{capability} 接管已切换到 {label}（generation {generation}）")
}

/// Format the lease table for `/mesh who`.
pub fn format_leases(mesh: &Mesh) -> Result<String, String> {
    let leases = mesh.lease_observer.all().map_err(|e| e.to_string())?;
    if leases.is_empty() {
        return Ok("没有任何接管租约".to_string());
    }
    Ok(leases
        .into_iter()
        .map(|(capability, holder): (String, LeaseHolder)| {
            format!(
                "{capability}: {}（generation {}，剩余 {}s）",
                holder.owner_name,
                holder.generation,
                ((holder.expires_at - now_ms()).max(0)) / 1000
            )
        })
        .collect::<Vec<_>>()
        .join("\n"))
}

/// Exposed for the command layer's `/instances` reuse.
pub fn instances_text(mesh: &Mesh) -> Result<String, String> {
    mesh.format_instances(now_ms())
}

#[allow(dead_code)]
fn alive_ms() -> i64 {
    ALIVE_MS
}
