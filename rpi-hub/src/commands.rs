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
    let result = (|| -> Result<String, String> {
        let hub = hub()?;
        let query = args.trim();
        if query.is_empty() {
            let holder = hub
                .store
                .lease_holder("wechat")
                .map_err(|e| e.to_string())?;
            let current = match holder {
                Some(holder) => format!("当前微信接管: {}", holder.owner_name),
                None => "当前微信接管: (无)".to_string(),
            };
            return Ok(format!("{current}\n用法: /use <实例名|编号>"));
        }
        let target = hub.resolve_target(query)?;
        let generation = hub
            .store
            .assign_lease(
                "wechat",
                &target.node_id,
                &target.name,
                now_ms(),
                hub.config.lease.ttl_ms(),
            )
            .map_err(|e| e.to_string())?;
        Ok(format!(
            "微信接管已切换到 {}{}（generation {generation}）",
            target.label(),
            if target.node_id == hub.node_id {
                "（本实例）"
            } else {
                ""
            }
        ))
    })();
    match result {
        Ok(text) => emit(out, &text),
        Err(error) => emit_error(out, error),
    }
}

extern "C" fn cmd_msg(raw: StbStringRef, out: *mut StbString, _ud: *mut c_void) -> i32 {
    let args = args_of(raw);
    let result = (|| -> Result<String, String> {
        let hub = hub()?;
        let (target, text) = split_target(&args);
        if target.is_empty() || text.is_empty() {
            return Err("用法: /msg <实例> <内容>".to_string());
        }
        let target = hub.resolve_target(target)?;
        let envelope =
            Envelope::text(&hub.node_id, &hub.name(), &target.node_id, text, now_ms())
                .map_err(|e| e.to_string())?;
        hub.deliver(envelope)?;
        Ok(format!("已发送给 {}", target.label()))
    })();
    match result {
        Ok(text) => emit(out, &text),
        Err(error) => emit_error(out, error),
    }
}

extern "C" fn cmd_cmd(raw: StbStringRef, out: *mut StbString, _ud: *mut c_void) -> i32 {
    let args = args_of(raw);
    let result = (|| -> Result<String, String> {
        let hub = hub()?;
        let (target, command) = split_target(&args);
        if target.is_empty() || command.is_empty() {
            return Err("用法: /cmd <实例> <指令>（如 /cmd home /reload）".to_string());
        }
        let target = hub.resolve_target(target)?;
        let intent = CommandIntent::parse(command).map_err(|e| e.to_string())?;
        if target.node_id == hub.node_id {
            return intent::execute(&hub, &intent);
        }
        let envelope =
            Envelope::command(&hub.node_id, &hub.name(), &target.node_id, intent, now_ms());
        hub.deliver(envelope)?;
        Ok(format!(
            "已发送给 {}；结果会以通知形式返回",
            target.label()
        ))
    })();
    match result {
        Ok(text) => emit(out, &text),
        Err(error) => emit_error(out, error),
    }
}

extern "C" fn cmd_ask(raw: StbStringRef, out: *mut StbString, _ud: *mut c_void) -> i32 {
    let args = args_of(raw);
    let result = (|| -> Result<String, String> {
        let hub = hub()?;
        let (target, task) = split_target(&args);
        if target.is_empty() || task.is_empty() {
            return Err("用法: /ask <实例> <任务>".to_string());
        }
        let target = hub.resolve_target(target)?;
        let envelope = Envelope::text(&hub.node_id, &hub.name(), &target.node_id, task, now_ms())
            .map_err(|e| e.to_string())?;
        hub.deliver(envelope)?;
        Ok(format!(
            "任务已交给 {}；结果会以通知形式返回",
            target.label()
        ))
    })();
    match result {
        Ok(text) => emit(out, &text),
        Err(error) => emit_error(out, error),
    }
}

extern "C" fn cmd_run(raw: StbStringRef, out: *mut StbString, _ud: *mut c_void) -> i32 {
    let args = args_of(raw);
    let result = (|| -> Result<String, String> {
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
        Ok(format!("已在 {host_name} 上启动任务；结果会以通知形式返回"))
    })();
    match result {
        Ok(text) => emit(out, &text),
        Err(error) => emit_error(out, error),
    }
}

extern "C" fn cmd_hub(raw: StbStringRef, out: *mut StbString, _ud: *mut c_void) -> i32 {
    let args = args_of(raw);
    let result = (|| -> Result<String, String> {
        let hub = hub()?;
        let (sub, rest) = split_target(&args);
        match sub {
            "" | "status" => {
                let stats = hub.store.stats(now_ms()).map_err(|e| e.to_string())?;
                let holder = hub
                    .store
                    .lease_holder("wechat")
                    .map_err(|e| e.to_string())?;
                Ok(format!(
                    "实例: {}\n节点: {}\n模式: {:?}\n实例数: {}\n待投递: {}/{} 条\n微信接管: {}",
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
                    return Ok(format!("当前实例名: {}", hub.name()));
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
    })();
    match result {
        Ok(text) => emit(out, &text),
        Err(error) => emit_error(out, error),
    }
}
