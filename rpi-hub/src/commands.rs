//! Slash commands.
//!
//! The host runs a command handler **synchronously on the TUI thread**, so every
//! handler here is a handful of SQLite statements. Anything that needs to wait
//! for another instance reports "sent" and lets the worker post the result as a
//! transcript notice when the reply arrives.

use std::ffi::c_void;

use rpi_hub_core::now_ms;
use rpi_hub_core::protocol::{CommandIntent, Envelope};
use rpi_plugin_sdk::{CommandHandlerFn, StbString, StbStringRef};

use crate::host::{self, Hub};
use crate::{intent, tools};

pub fn registry() -> Vec<(&'static str, &'static str, CommandHandlerFn)> {
    vec![
        ("instances", "列出所有 rpi 实例与微信接管者", cmd_instances),
        ("use", "把微信接管切换到实例：/use <实例|编号>", cmd_use),
        ("msg", "给实例发消息：/msg <实例> <内容>", cmd_msg),
        ("cmd", "给实例发指令：/cmd <实例> <指令>", cmd_cmd),
        ("ask", "让实例调研并回结果：/ask <实例> <任务>", cmd_ask),
        ("run", "在远端主机跑一次性任务：/run <主机> <任务>", cmd_run),
        ("hub", "hub 诊断：/hub status|who|name <名>|leave", cmd_hub),
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

/// A command result that is either plain text (wrapped as a message) or an
/// already-shaped UI payload (selector/editor) written verbatim.
enum Reply {
    Text(String),
    Ui(serde_json::Value),
}

fn finish(out: *mut StbString, result: Result<Reply, String>) -> i32 {
    match result {
        Ok(Reply::Text(text)) => emit(out, &text),
        Ok(Reply::Ui(value)) => {
            unsafe {
                *out = StbString::from_string(value.to_string());
            }
            0
        }
        Err(error) => emit_error(out, error),
    }
}

/// Selector items for the live instance list (value = display name, the same
/// token `/use`/`/msg` accept).
fn selector_items(hub: &Hub) -> Result<Vec<serde_json::Value>, String> {
    let instances = hub.instances(now_ms())?;
    if instances.is_empty() {
        return Err("没有登记的实例".to_string());
    }
    let holder = hub
        .store
        .lease_holder("wechat")
        .map_err(|e| e.to_string())?;
    Ok(instances
        .iter()
        .enumerate()
        .map(|(index, instance)| {
            let mut label = format!("{}. {}", index + 1, instance.label());
            if instance.node_id == hub.node_id {
                label.push_str("（当前）");
            }
            if holder
                .as_ref()
                .is_some_and(|h| h.owner_node == instance.node_id)
            {
                label.push_str("（微信）");
            }
            if instance.busy {
                label.push_str(" 忙");
            }
            serde_json::json!({
                "value": instance.label(),
                "label": label,
                "description": instance.cwd,
            })
        })
        .collect())
}

/// `/msg` `/cmd` `/ask` argument shape: `<target> <free text>`, where the text
/// may be empty (then the UI asks for it in an editor).
#[derive(Debug, Default, Clone)]
struct MsgFlow {
    target: Option<String>,
    text: Option<String>,
    draft: String,
}

impl MsgFlow {
    fn parse(args: &str) -> Self {
        let (target, rest) = split_target(args);
        if target.is_empty() {
            return Self {
                draft: args.trim().to_string(),
                ..Default::default()
            };
        }
        Self {
            target: Some(target.to_string()),
            text: (!rest.trim().is_empty()).then(|| rest.trim().to_string()),
            draft: rest.trim().to_string(),
        }
    }
}

/// Build the selector payload for a command handler.
fn selector_payload(items: &[serde_json::Value]) -> Reply {
    Reply::Ui(serde_json::json!({
        "kind": "selector",
        "title": "选择实例",
        "items": items,
    }))
}

/// Editor payload (free text asked after a target was picked).
fn editor_payload(title: String, initial: &str) -> Reply {
    Reply::Ui(serde_json::json!({
        "kind": "editor",
        "title": title,
        "initialText": initial,
    }))
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

fn hub() -> Result<std::sync::Arc<Hub>, String> {
    host::current().ok_or_else(|| "rpi-hub 未初始化".to_string())
}

/// Split `"home /reload now"` into `("home", "/reload now")`.
fn split_target(args: &str) -> (&str, &str) {
    let trimmed = args.trim();
    match trimmed.split_once(char::is_whitespace) {
        Some((target, rest)) => (target, rest.trim()),
        None => (trimmed, ""),
    }
}

extern "C" fn cmd_instances(raw: StbStringRef, out: *mut StbString, _ud: *mut c_void) -> i32 {
    let _ = raw;
    match hub().and_then(|hub| tools::instances_text(&hub)) {
        Ok(text) => emit(out, &text),
        Err(error) => emit_error(out, error),
    }
}

extern "C" fn cmd_use(raw: StbStringRef, out: *mut StbString, _ud: *mut c_void) -> i32 {
    let args = args_of(raw);
    let result = (|| -> Result<Reply, String> {
        let hub = hub()?;
        // The TUI re-invokes the command with {"action":"select","value":…}
        // (or {"action":"cancel"}) after the user picks from the selector.
        if let Ok(value) = serde_json::from_str::<serde_json::Value>(&args) {
            match value.get("action").and_then(|v| v.as_str()) {
                Some("cancel") => return Ok(Reply::Text("已取消".to_string())),
                Some("select") => {
                    let picked = value
                        .get("value")
                        .and_then(|v| v.as_str())
                        .ok_or("selection missing value")?;
                    let target = hub.resolve_target(picked)?;
                    return hub.switch_lease_to(&target).map(Reply::Text);
                }
                _ => {}
            }
        }
        let query = args.trim();
        if query.is_empty() {
            let items = selector_items(&hub)?;
            return Ok(selector_payload(&items));
        }
        let target = hub.resolve_target(query)?;
        hub.switch_lease_to(&target).map(Reply::Text)
    })();
    finish(out, result)
}

extern "C" fn cmd_msg(raw: StbStringRef, out: *mut StbString, _ud: *mut c_void) -> i32 {
    let args = args_of(raw);
    let result = (|| -> Result<Reply, String> {
        let hub = hub()?;
        let mut flow = MsgFlow::parse(&args);
        // Same select/cancel re-entry contract as /use.
        if let Ok(value) = serde_json::from_str::<serde_json::Value>(&args) {
            match value.get("action").and_then(|v| v.as_str()) {
                Some("cancel") => return Ok(Reply::Text("已取消".to_string())),
                Some("select") => {
                    flow.target = value
                        .get("value")
                        .and_then(|v| v.as_str())
                        .map(str::to_string);
                }
                _ => {}
            }
        }
        let Some(target_query) = flow.target.clone() else {
            let items = selector_items(&hub)?;
            return Ok(selector_payload(&items));
        };
        let Some(text) = flow.text.clone() else {
            // Target chosen: ask for the message body in an editor pre-filled
            // with the rest of the command line, if any.
            return Ok(editor_payload(
                format!("发给 {target_query} 的消息"),
                &flow.draft,
            ));
        };
        let target = hub.resolve_target(&target_query)?;
        let envelope =
            Envelope::text(&hub.node_id, &hub.name(), &target.node_id, text, now_ms())
                .map_err(|e| e.to_string())?;
        hub.deliver(envelope)?;
        Ok(Reply::Text(format!("已发送给 {}", target.label())))
    })();
    finish(out, result)
}

extern "C" fn cmd_cmd(raw: StbStringRef, out: *mut StbString, _ud: *mut c_void) -> i32 {
    let args = args_of(raw);
    let result = (|| -> Result<Reply, String> {
        let hub = hub()?;
        let mut flow = MsgFlow::parse(&args);
        if let Ok(value) = serde_json::from_str::<serde_json::Value>(&args) {
            match value.get("action").and_then(|v| v.as_str()) {
                Some("cancel") => return Ok(Reply::Text("已取消".to_string())),
                Some("select") => {
                    flow.target = value
                        .get("value")
                        .and_then(|v| v.as_str())
                        .map(str::to_string);
                }
                _ => {}
            }
        }
        let Some(target_query) = flow.target.clone() else {
            let items = selector_items(&hub)?;
            return Ok(selector_payload(&items));
        };
        let Some(command) = flow.text.clone().filter(|t| !t.trim().is_empty()) else {
            return Ok(editor_payload(
                format!("发给 {target_query} 的指令（如 reload，不用带实例名）"),
                &flow.draft,
            ));
        };
        let target = hub.resolve_target(&target_query)?;
        let intent = CommandIntent::parse(&command).map_err(|e| e.to_string())?;
        if target.node_id == hub.node_id {
            return intent::execute(&hub, &intent).map(Reply::Text);
        }
        // 带 correlation：目标执行完会把结果作为 Reply 投回本实例，worker
        // 收到后以通知形式展示（否则结果只会出现在目标自己的会话里）。
        let correlation = rpi_hub_core::protocol::new_id();
        let envelope = Envelope::command(&hub.node_id, &hub.name(), &target.node_id, intent, now_ms())
            .with_correlation(&correlation);
        hub.deliver(envelope)?;
        Ok(Reply::Text(format!(
            "已发送给 {}；结果会以通知形式返回",
            target.label()
        )))
    })();
    finish(out, result)
}

extern "C" fn cmd_ask(raw: StbStringRef, out: *mut StbString, _ud: *mut c_void) -> i32 {
    let args = args_of(raw);
    let result = (|| -> Result<Reply, String> {
        let hub = hub()?;
        let mut flow = MsgFlow::parse(&args);
        if let Ok(value) = serde_json::from_str::<serde_json::Value>(&args) {
            match value.get("action").and_then(|v| v.as_str()) {
                Some("cancel") => return Ok(Reply::Text("已取消".to_string())),
                Some("select") => {
                    flow.target = value
                        .get("value")
                        .and_then(|v| v.as_str())
                        .map(str::to_string);
                }
                _ => {}
            }
        }
        let Some(target_query) = flow.target.clone() else {
            let items = selector_items(&hub)?;
            return Ok(selector_payload(&items));
        };
        let Some(task) = flow.text.clone().filter(|t| !t.trim().is_empty()) else {
            return Ok(editor_payload(
                format!("委派给 {target_query} 的任务"),
                &flow.draft,
            ));
        };
        let target = hub.resolve_target(&target_query)?;
        // correlation 让目标实例在回合结束时把结果投回本实例（否则结果只会
        // 出现在目标自己的会话里）。
        let correlation = rpi_hub_core::protocol::new_id();
        let envelope = Envelope::text(&hub.node_id, &hub.name(), &target.node_id, task, now_ms())
            .map_err(|e| e.to_string())?
            .with_correlation(&correlation);
        hub.deliver(envelope)?;
        Ok(Reply::Text(format!(
            "任务已交给 {}；结果会以通知形式返回",
            target.label()
        )))
    })();
    finish(out, result)
}

extern "C" fn cmd_run(raw: StbStringRef, out: *mut StbString, _ud: *mut c_void) -> i32 {
    let args = args_of(raw);
    let result = (|| -> Result<Reply, String> {
        let hub = hub()?;
        let (host_name, task) = split_target(&args);
        if host_name.is_empty() || task.is_empty() {
            return Err("用法: /run <主机> <任务>（主机来自 remoteHosts）".to_string());
        }
        let host = hub
            .config
            .remote
            .hosts
            .get(host_name)
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
                    "未配置 remoteHosts；在 hub 配置 remote.hosts 里添加".to_string()
                } else {
                    format!("未知主机 {host_name:?}；已配置: {}", known.join(", "))
                }
            })?;
        crate::remote::spawn(
            hub.clone(),
            host_name.to_string(),
            host,
            task.to_string(),
            std::time::Duration::from_secs(600),
            None,
        );
        Ok(Reply::Text(format!(
            "已在 {host_name} 上启动任务；结果会以通知形式返回"
        )))
    })();
    finish(out, result)
}

extern "C" fn cmd_hub(raw: StbStringRef, out: *mut StbString, _ud: *mut c_void) -> i32 {
    let args = args_of(raw);
    let result = (|| -> Result<Reply, String> {
        let hub = hub()?;
        let (sub, rest) = split_target(&args);
        match sub {
            "" | "status" => {
                // Build fingerprint: proves which build a running session loaded.
                const BUILD: &str = concat!(env!("CARGO_PKG_VERSION"), "@", env!("BUILD_STAMP"));
                let stats = hub.store.stats(now_ms()).map_err(|e| e.to_string())?;
                let holder = hub
                    .store
                    .lease_holder("wechat")
                    .map_err(|e| e.to_string())?;
                Ok(format!(
                    "构建: {BUILD}\n实例: {}\n节点: {}\n模式: {:?}\n实例数: {}\n待投递: {}/{} 条\n微信接管: {}",
                    hub.label(),
                    hub.node_id,
                    hub.config.remote.mode,
                    stats.instances,
                    stats.envelopes_pending,
                    stats.envelopes_total,
                    holder
                        .map(|h| format!("{}（generation {}）", h.owner_name, h.generation))
                        .unwrap_or_else(|| "(无)".to_string())
                ))
            }
            "who" => tools::format_leases(&hub),
            "name" => {
                if rest.is_empty() {
                    return Ok(Reply::Text(format!("当前实例名: {}", hub.name())));
                }
                let name = hub.rename(rest)?;
                Ok(format!("实例名已设为 {name}"))
            }
            "leave" => {
                hub.request_shutdown();
                Ok("本实例已退出 hub（/reload 或重启后重新加入）".to_string())
            }
            other => Err(format!(
                "未知子命令 {other:?}；可用: status | who | name <名> | leave"
            )),
        }
        .map(Reply::Text)
    })();
    finish(out, result)
}
