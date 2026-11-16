//! Tools exposed to the model.
//!
//! Two shapes share the ABI lifecycle:
//! - **sync** (`hub_instances`, `hub_send`, `hub_switch`): the outcome is known
//!   in `execute`, `poll` reports it once.
//! - **wait** (`hub_cmd`, `hub_ask`): `execute` sends the envelope and returns a
//!   handle holding a correlation id; `poll` checks the reply mailbox without
//!   blocking, until the deadline.

use std::sync::Arc;

use rpi_hub_core::protocol::{CommandIntent, Envelope};
use rpi_hub_core::store::LeaseHolder;
use rpi_hub_core::now_ms;
use rpi_plugin_sdk::callbacks::free_string;
use rpi_plugin_sdk::handles::ToolHandles;
use rpi_plugin_sdk::{
    PluginApi, StableToolSchema, StepHandle, StepResult, StbString, ToolPartialCb,
};

use crate::host::{self, Hub, ALIVE_MS};
use crate::intent;

const DEFAULT_ASK_TIMEOUT_S: u64 = 300;
const MAX_ASK_TIMEOUT_S: u64 = 900;
const COMMAND_TIMEOUT_MS: i64 = 60_000;
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
            return error_result("invalid or destroyed hub handle".into());
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
    .unwrap_or_else(|_| error_result("rpi-hub sync poll panicked".into()))
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
            let hub = match host::current() {
                Some(hub) => hub,
                None => return error_result("rpi-hub is not loaded".into()),
            };
            let outcome = hub.take_reply(&correlation);
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
            if let Some(hub) = host::current() {
                hub.forget_reply(&correlation);
            }
            return error_result("等待目标实例回复超时".into());
        }
        StepResult::pending(StbString::empty())
    })
    .unwrap_or_else(|_| error_result("rpi-hub wait poll panicked".into()))
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

pub fn register(hub: &Arc<Hub>, api: &PluginApi) -> Result<(), String> {
    let Some(register_tool) = api.register_tool else {
        return Err("host has no tool registrar".into());
    };
    let _ = hub;
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
            name: "hub_instances",
            description: "List running rpi instances (this machine and remote), which one owns the WeChat channel, and each instance's session/busy state.",
            parameters: r#"{"type":"object","properties":{},"additionalProperties":false}"#,
            execute: instances_execute,
            poll: sync_poll,
            cancel: sync_cancel,
            destroy: sync_destroy,
        },
        ToolDef {
            name: "hub_send",
            description: "Send a message into another rpi instance's agent lane. Use it to hand work to an instance that runs in a different network (office vs server) and let it act with its own environment.",
            parameters: r#"{"type":"object","properties":{"target":{"type":"string","description":"Instance name, name@host, or node id prefix"},"text":{"type":"string","description":"Message for the target agent"}},"required":["target","text"],"additionalProperties":false}"#,
            execute: send_execute,
            poll: sync_poll,
            cancel: sync_cancel,
            destroy: sync_destroy,
        },
        ToolDef {
            name: "hub_cmd",
            description: "Run an administrative command on another rpi instance (new, fork, compact, reload, model, thinking, name, stop, status) and return its result.",
            parameters: r#"{"type":"object","properties":{"target":{"type":"string"},"command":{"type":"string","description":"e.g. '/new', '/reload', 'model sonnet'"}},"required":["target","command"],"additionalProperties":false}"#,
            execute: cmd_execute,
            poll: wait_poll,
            cancel: wait_cancel,
            destroy: wait_destroy,
        },
        ToolDef {
            name: "hub_ask",
            description: "Delegate a task to another rpi instance and wait for its answer. The target works in its own session, network and filesystem, then returns the assistant's final text.",
            parameters: r#"{"type":"object","properties":{"target":{"type":"string"},"task":{"type":"string"},"timeoutSeconds":{"type":"integer","minimum":10,"maximum":900}},"required":["target","task"],"additionalProperties":false}"#,
            execute: ask_execute,
            poll: wait_poll,
            cancel: wait_cancel,
            destroy: wait_destroy,
        },
        ToolDef {
            name: "hub_run",
            description: "Run a one-shot rpi task on an SSH-reachable machine that has no running instance (configured under remoteHosts). Use it when the work must happen in that machine's network. Prefer hub_ask when an instance there is already running: it keeps that conversation's context.",
            parameters: r#"{"type":"object","properties":{"host":{"type":"string","description":"Key under remoteHosts in the hub config"},"task":{"type":"string"},"timeoutSeconds":{"type":"integer","minimum":10,"maximum":3600}},"required":["host","task"],"additionalProperties":false}"#,
            execute: run_execute,
            poll: wait_poll,
            cancel: wait_cancel,
            destroy: wait_destroy,
        },
        ToolDef {
            name: "hub_switch",
            description: "Move ownership of a channel (default wechat) to another instance. Only the owner talks to WeChat, so this is how the mobile connection follows you between machines.",
            parameters: r#"{"type":"object","properties":{"target":{"type":"string"},"capability":{"type":"string","description":"channel id, default wechat"}},"required":["target"],"additionalProperties":false}"#,
            execute: switch_execute,
            poll: sync_poll,
            cancel: sync_cancel,
            destroy: sync_destroy,
        },
    ]
}

// ---------------------------------------------------------------------------
// Tool bodies
// ---------------------------------------------------------------------------

fn require_hub() -> Result<Arc<Hub>, String> {
    host::current().ok_or_else(|| "rpi-hub is not initialized".to_string())
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
    let hub = require_hub()?;
    hub.format_instances(now_ms())
}

rpi_plugin_sdk::execute_handler! {
    fn send_execute(_call_id, params) {
        let outcome = run_send(&params.to_string_lossy());
        SYNC.insert(SyncDrive { outcome: Some(outcome) })
    }
}

fn run_send(params: &str) -> Result<String, String> {
    let hub = require_hub()?;
    let params = parse_params(params);
    let target = hub.resolve_target(param_str(&params, "target")?)?;
    let text = param_str(&params, "text")?;
    let envelope =
        Envelope::text(&hub.node_id, &hub.name(), &target.node_id, text, now_ms())
            .map_err(|e| e.to_string())?;
    hub.deliver(envelope)?;
    Ok(format!("已发送给 {}", target.label()))
}

rpi_plugin_sdk::execute_handler! {
    fn cmd_execute(_call_id, params) {
        match run_cmd(&params.to_string_lossy()) {
            Ok(CmdOutcome::Local(text)) => SYNC.insert(SyncDrive { outcome: Some(Ok(text)) }),
            Ok(CmdOutcome::Remote { correlation }) => WAIT.insert(WaitDrive {
                correlation,
                deadline: now_ms() + COMMAND_TIMEOUT_MS,
                last_check: 0,
            }),
            Err(error) => SYNC.insert(SyncDrive { outcome: Some(Err(error)) }),
        }
    }
}

enum CmdOutcome {
    Local(String),
    Remote { correlation: String },
}

fn run_cmd(params: &str) -> Result<CmdOutcome, String> {
    let hub = require_hub()?;
    let params = parse_params(params);
    let target = hub.resolve_target(param_str(&params, "target")?)?;
    let intent = CommandIntent::parse(param_str(&params, "command")?).map_err(|e| e.to_string())?;

    if target.node_id == hub.node_id {
        let text = intent::execute(&hub, &intent)?;
        return Ok(CmdOutcome::Local(text));
    }

    let correlation = rpi_hub_core::protocol::new_id();
    let envelope = Envelope::command(&hub.node_id, &hub.name(), &target.node_id, intent, now_ms())
        .with_correlation(&correlation);
    hub.expect_reply(&correlation);
    hub.deliver(envelope)?;
    Ok(CmdOutcome::Remote { correlation })
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
    let hub = require_hub()?;
    let params = parse_params(params);
    let target = hub.resolve_target(param_str(&params, "target")?)?;
    let task = param_str(&params, "task")?;
    let timeout = params
        .get("timeoutSeconds")
        .and_then(|v| v.as_u64())
        .unwrap_or(DEFAULT_ASK_TIMEOUT_S)
        .clamp(10, MAX_ASK_TIMEOUT_S);

    let correlation = rpi_hub_core::protocol::new_id();
    let envelope = Envelope::text(&hub.node_id, &hub.name(), &target.node_id, task, now_ms())
        .map_err(|e| e.to_string())?
        .with_correlation(&correlation);
    hub.expect_reply(&correlation);
    hub.deliver(envelope)?;
    Ok((correlation, (timeout as i64) * 1000))
}

rpi_plugin_sdk::execute_handler! {
    fn run_execute(_call_id, params) {
        match run_remote(&params.to_string_lossy()) {
            Ok((correlation, timeout_ms)) => WAIT.insert(WaitDrive {
                correlation,
                deadline: now_ms() + timeout_ms,
                last_check: 0,
            }),
            Err(error) => SYNC.insert(SyncDrive { outcome: Some(Err(error)) }),
        }
    }
}

fn run_remote(params: &str) -> Result<(String, i64), String> {
    let hub = require_hub()?;
    let params = parse_params(params);
    let host_name = param_str(&params, "host")?.to_string();
    let task = param_str(&params, "task")?.to_string();
    let host = hub
        .config
        .remote
        .hosts
        .get(&host_name)
        .cloned()
        .ok_or_else(|| {
            let known: Vec<&str> = hub
                .config
                .remote
                .hosts
                .keys()
                .map(String::as_str)
                .collect();
            if known.is_empty() {
                "no remoteHosts configured; add one under remote.hosts in the hub config"
                    .to_string()
            } else {
                format!("unknown host {host_name:?}; configured: {}", known.join(", "))
            }
        })?;
    let timeout_s = params
        .get("timeoutSeconds")
        .and_then(|v| v.as_u64())
        .unwrap_or(DEFAULT_ASK_TIMEOUT_S)
        .clamp(10, 3600);
    let timeout = std::time::Duration::from_secs(timeout_s);

    let correlation = rpi_hub_core::protocol::new_id();
    hub.expect_reply(&correlation);
    crate::remote::spawn(
        hub.clone(),
        host_name,
        host,
        task,
        timeout,
        Some(correlation.clone()),
    );
    // Give the wait drive a little room past the run deadline.
    Ok((correlation, (timeout_s as i64) * 1000 + 10_000))
}

rpi_plugin_sdk::execute_handler! {
    fn switch_execute(_call_id, params) {
        let outcome = run_switch(&params.to_string_lossy());
        SYNC.insert(SyncDrive { outcome: Some(outcome) })
    }
}

fn run_switch(params: &str) -> Result<String, String> {
    let hub = require_hub()?;
    let params = parse_params(params);
    let target = hub.resolve_target(param_str(&params, "target")?)?;
    let capability = params
        .get("capability")
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .unwrap_or("wechat");
    let generation = hub
        .store
        .assign_lease(
            capability,
            &target.node_id,
            &target.name,
            now_ms(),
            hub.config.lease.ttl_ms(),
        )
        .map_err(|e| e.to_string())?;
    Ok(describe_transfer(capability, &target.label(), generation))
}

fn describe_transfer(capability: &str, label: &str, generation: i64) -> String {
    format!("{capability} 接管已切换到 {label}（generation {generation}）")
}

/// Format the lease table for `/hub who`.
pub fn format_leases(hub: &Hub) -> Result<String, String> {
    let leases = hub.store.list_leases().map_err(|e| e.to_string())?;
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
pub fn instances_text(hub: &Hub) -> Result<String, String> {
    hub.format_instances(now_ms())
}

#[allow(dead_code)]
fn alive_ms() -> i64 {
    ALIVE_MS
}
