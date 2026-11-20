//! Slash commands.
//!
//! The host runs a command handler **synchronously on the TUI thread**, so every
//! handler here is a handful of SQLite statements. Anything that needs to wait
//! for another instance reports "sent" and lets the worker post the result as a
//! transcript notice when the reply arrives.

use std::ffi::c_void;
use std::path::Path;
use std::process::Command;

use std::sync::Arc;

use rpi_mesh_core::now_ms;
use rpi_mesh_core::api::MeshStore;
use rpi_mesh_core::store::InstanceView;
use rpi_mesh_core::protocol::Envelope;
use rpi_plugin_sdk::{CommandHandlerFn, StbString, StbStringRef};

use crate::mesh::{self, Mesh};
use crate::tools;

pub fn registry() -> Vec<(&'static str, &'static str, CommandHandlerFn)> {
    vec![
        ("instances", "列出所有 rpi 实例与微信接管者", cmd_instances),
        ("use", "把微信接管切换到实例：/use <实例|编号>", cmd_use),
        ("msg", "给实例发消息：/msg <实例> <内容>", cmd_msg),
        ("ask", "让实例调研并回结果：/ask <实例> <任务>", cmd_ask),
        (
            "mesh",
            "组网与管理：/mesh relay [端口]（本机作中继）/ client <ip:端口> [token]（连中继）/ local / status（模式与地址）/ token / who",
            cmd_mesh,
        ),
        ("start", "启动新实例：/start [目录]（本机）/ /start <实例>:<目录>（跨机）", cmd_start),
        ("stop", "停止实例：/stop <编号|名字>（本机）/ /stop <执行实例>:<目标>（跨机）", cmd_stop),
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
fn selector_items(mesh: &Mesh) -> Result<Vec<serde_json::Value>, String> {
    let instances = mesh.instances(now_ms())?;
    if instances.is_empty() {
        return Err("没有登记的实例".to_string());
    }
    let holder = mesh
        .lease_observer
        .holder("wechat")
        .map_err(|e| e.to_string())?;
    Ok(instances
        .iter()
        .enumerate()
        .map(|(index, instance)| {
            let mut label = format!("{}. {}", index + 1, instance.label());
            if instance.node_id == mesh.node_id {
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
    let value: serde_json::Value = match serde_json::from_str(&text) {
        Ok(value) => value,
        Err(_) => return text,
    };
    // Normal invocation: {"args": "<text>"} — the host passes the tail as a
    // JSON string. Use it verbatim.
    if let Some(args) = value.get("args").and_then(|v| v.as_str()) {
        return args.to_string();
    }
    // Editor re-entry (TUI's extension editor): {"action":"edit","text":
    // "<what the user typed>"} — the text IS the argument tail. Without this
    // branch the whole JSON object was treated as the tail, and split_target
    // parsed the first JSON token as a target — the editor-submitted command
    // never executed.
    if value.get("action").and_then(|v| v.as_str()) == Some("edit") {
        return value
            .get("text")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
    }
    text
}

fn mesh() -> Result<std::sync::Arc<Mesh>, String> {
    mesh::current().ok_or_else(|| "rpi-mesh 未初始化".to_string())
}

/// Split `"home /reload now"` into `("home", "/reload now")`.
fn split_target(args: &str) -> (&str, &str) {
    let trimmed = args.trim();
    match trimmed.split_once(char::is_whitespace) {
        Some((target, rest)) => (target, rest.trim()),
        None => (trimmed, ""),
    }
}

extern "C" fn cmd_start(raw: StbStringRef, out: *mut StbString, _ud: *mut c_void) -> i32 {
    let args = args_of(raw);
    let result = mesh().and_then(|mesh| {
        // Cross-machine syntax: "/start <instance>:<cwd>" — the target
        // instance identifies the machine; a `spawn` cmd envelope makes THAT
        // machine run its local /start and mail the result back.
        if let Some((target_query, cwd)) = args.split_once(':') {
            let target_query = target_query.trim();
            if !target_query.is_empty() {
                let target = mesh.resolve_target(target_query)?;
                if target.node_id != mesh.node_id {
                    return forward_start(&mesh, &target, cwd.trim());
                }
                return Err("start 的目标实例是自己——去掉实例前缀即为本地启动".into());
            }
        }
        let arg = args.trim();
        let arg_opt = (!arg.is_empty()).then_some(arg);
        let spawn_ctx = spawn_ctx_from(&mesh);
        rpi_mesh_core::instance_start::start(&spawn_ctx, arg_opt)
    });
    match result {
        Ok(text) => emit(out, &text),
        Err(error) => emit_error(out, error),
    }
}

extern "C" fn cmd_stop(raw: StbStringRef, out: *mut StbString, _ud: *mut c_void) -> i32 {
    let args = args_of(raw);
    let query = args.trim();
    if query.is_empty() {
        emit_error(out, "用法: /stop <编号|名字>（见 /instances）或 /stop <执行实例>:<目标实例> 跨机停止");
        return 0;
    }
    let result = mesh().and_then(|mesh| {
        // Cross-machine syntax: "/stop <executor>:<target>" — the executor
        // instance identifies the machine that runs its local /stop against
        // the target.
        if let Some((executor_query, target_query)) = query.split_once(':') {
            let executor_query = executor_query.trim();
            let target_query = target_query.trim();
            if !executor_query.is_empty() && !target_query.is_empty() {
                let executor = mesh.resolve_target(executor_query)?;
                if executor.node_id != mesh.node_id {
                    // Resolve the target HERE (this machine's list order);
                    // forward its unique label — row numbers would mean a
                    // different row on the executor's own list.
                    let target = mesh.resolve_target(target_query)?;
                    return forward_stop(&mesh, &executor, &target.label());
                }
                let spawn_ctx = spawn_ctx_from(&mesh);
                return rpi_mesh_core::instance_start::stop(
                    &spawn_ctx,
                    target_query,
                    rpi_mesh_core::now_ms(),
                );
            }
        }
        let spawn_ctx = spawn_ctx_from(&mesh);
        rpi_mesh_core::instance_start::stop(&spawn_ctx, query, rpi_mesh_core::now_ms())
    });
    match result {
        Ok(text) => emit(out, &text),
        Err(error) => emit_error(out, error),
    }
}

/// Forward a cross-machine /start: `spawn <cwd>` cmd envelope to the target
/// instance; its worker executes the local /start and mails the result back
/// via the reply envelope (sync wait, same as /ask).
pub(crate) fn forward_start(mesh: &Arc<Mesh>, target: &InstanceView, cwd: &str) -> Result<String, String> {
    let args_text = format!("spawn {}", cwd);
    forward_intent(mesh, target, &args_text, &format!("/start on {}", target.label()))
}

/// Forward a cross-machine /stop: `terminate <query>` cmd envelope.
pub(crate) fn forward_stop(mesh: &Arc<Mesh>, target: &InstanceView, target_query: &str) -> Result<String, String> {
    let args_text = format!("terminate {target_query}");
    forward_intent(mesh, target, &args_text, &format!("/stop on {}", target.label()))
}

/// Send a `cmd` intent to `target` and wait synchronously for its reply.
fn forward_intent(
    mesh: &Arc<Mesh>,
    target: &InstanceView,
    intent_args: &str,
    label: &str,
) -> Result<String, String> {
    use rpi_mesh_core::protocol::CommandIntent;
    let intent = CommandIntent {
        name: intent_args.split_whitespace().next().unwrap_or("").to_string(),
        args: intent_args.split_whitespace().skip(1).map(str::to_string).collect(),
    };
    let correlation = rpi_mesh_core::protocol::new_id();
    let envelope = rpi_mesh_core::protocol::Envelope::command(
        &mesh.node_id,
        &mesh.name(),
        &target.node_id,
        intent,
        rpi_mesh_core::now_ms(),
    )
    .with_correlation(&correlation);
    mesh.expect_reply(&correlation);
    mesh.deliver(envelope)?;
    // Reuse the ask-wait machinery: block on the reply map poll.
    let deadline = rpi_mesh_core::now_ms() + 30_000;
    loop {
        if let Some(outcome) = mesh.take_reply(&correlation) {
            return if outcome.ok {
                Ok(format!("✅ {label}：{}", outcome.text))
            } else {
                Err(format!("远端执行失败：{}", outcome.text))
            };
        }
        if rpi_mesh_core::now_ms() >= deadline {
            mesh.forget_reply(&correlation);
            return Err(format!("远端 {label} 超时（目标实例未响应）"));
        }
        std::thread::sleep(std::time::Duration::from_millis(200));
    }
}

/// Hot-switch the networking backend: stop the old relay thread / client,
/// start the new one per `config`, and point the switchable store at the new
/// implementation. The config was already persisted by the caller.
fn switch_backends(mesh: &Arc<Mesh>, config: &rpi_mesh_core::config::MeshConfig) {
    let old = mesh.backend.lock().expect("backend mutex").take();
    if let Some(old) = old {
        drop(old); // Drop runs shutdown: relay thread stops, connections close.
    }
    match crate::backends::Backends::start(mesh.dir(), config) {
        Ok(new) => {
            // Reroute the OLD proxy to the new implementation: every thread
            // holding Arc<Mesh> reads this field per call, and SwitchableStore's
            // inner Mutex is the switch. The new Backends' own proxy is
            // discarded — the old one keeps serving.
            mesh.store.switch(Arc::clone(&new.store));
            *mesh.backend.lock().expect("backend mutex") = Some(new);
            mesh::log("mesh: backends switched");
        }
        Err(error) => {
            mesh::log(&format!("mesh: backend switch failed: {error} — retry via /reload"));
            // Restore the old backends so the instance is not left dead.
            match crate::backends::Backends::start(mesh.dir(), &{
                let mut restored = config.clone();
                restored.remote.role = None;
                restored
            }) {
                Ok(fallback) => {
                    *mesh.backend.lock().expect("backend mutex") = Some(fallback);
                }
                Err(e) => mesh::log(&format!("mesh: fallback to local also failed: {e}")),
            }
        }
    }
}

/// Best-effort outward-facing IPv4 (first address of `hostname -I`).
fn local_ip() -> Option<String> {
    let output = Command::new("hostname").args(["-I"]).output().ok()?;
    let first = String::from_utf8_lossy(&output.stdout)
        .split_whitespace()
        .next()?
        .to_string();
    (!first.is_empty()).then_some(first)
}

/// Read the relay token, generating the file if absent (mirrors the relay
/// listener's generate-on-first-use rule so /mesh relay can show it).
fn ensure_relay_token(dir: &Path) -> Result<String, String> {
    let path = dir.join("relay-token");
    if let Ok(text) = std::fs::read_to_string(&path) {
        let token = text.trim().to_string();
        if !token.is_empty() {
            return Ok(token);
        }
    }
    let token: String = {
        let a = uuid::Uuid::new_v4().simple().to_string();
        let b = uuid::Uuid::new_v4().simple().to_string();
        format!("{}{}", &a[..16], &b[..16])
    };
    write_relay_token(&path, &token)?;
    Ok(token)
}

/// Persist a relay token to the 0600 token file (clients type it in).
fn write_relay_token(path: &Path, token: &str) -> Result<(), String> {
    let token = token.trim();
    if token.len() != 32 || !token.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err(format!(
            "token 格式不对：应为 32 位十六进制（在 relay 机器上 /mesh token 获取）"
        ));
    }
    std::fs::write(path, format!("{token}\n"))
        .map_err(|e| format!("cannot write {}: {e}", path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if let Ok(metadata) = std::fs::metadata(path) {
            let mut permissions = metadata.permissions();
            permissions.set_mode(0o600);
            let _ = std::fs::set_permissions(path, permissions);
        }
    }
    Ok(())
}

/// Assemble the core SpawnContext from the mesh Mesh. Also used by the
/// `spawn`/`terminate` intents (cross-machine /start forward lands here).
pub(crate) fn spawn_ctx_from(
    mesh: &Mesh,
) -> rpi_mesh_core::instance_start::SpawnContext<'_> {
    let cwd = mesh
        .store
        .get_instance(&mesh.node_id)
        .ok()
        .flatten()
        .map(|v| v.cwd)
        .unwrap_or_default();
    let store_dyn = mesh.store_dyn();
    rpi_mesh_core::instance_start::SpawnContext {
        store: store_dyn,
        node_id: &mesh.node_id,
        host: &mesh.host,
        cwd,
        spawn: &mesh.config.instance_spawn,
    }
}

extern "C" fn cmd_instances(raw: StbStringRef, out: *mut StbString, _ud: *mut c_void) -> i32 {
    let _ = raw;
    match mesh().and_then(|mesh| tools::instances_text(&mesh)) {
        Ok(text) => emit(out, &text),
        Err(error) => emit_error(out, error),
    }
}

extern "C" fn cmd_use(raw: StbStringRef, out: *mut StbString, _ud: *mut c_void) -> i32 {
    let args = args_of(raw);
    let result = (|| -> Result<Reply, String> {
        let mesh = mesh()?;
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
                    let target = mesh.resolve_target(picked)?;
                    return mesh.switch_lease_to(&target).map(Reply::Text);
                }
                _ => {}
            }
        }
        let query = args.trim();
        if query.is_empty() {
            let items = selector_items(&mesh)?;
            return Ok(selector_payload(&items));
        }
        let target = mesh.resolve_target(query)?;
        mesh.switch_lease_to(&target).map(Reply::Text)
    })();
    finish(out, result)
}

extern "C" fn cmd_msg(raw: StbStringRef, out: *mut StbString, _ud: *mut c_void) -> i32 {
    let args = args_of(raw);
    let result = (|| -> Result<Reply, String> {
        let mesh = mesh()?;
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
            let items = selector_items(&mesh)?;
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
        let target = mesh.resolve_target(&target_query)?;
        let envelope =
            Envelope::text(&mesh.node_id, &mesh.name(), &target.node_id, text, now_ms())
                .map_err(|e| e.to_string())?;
        mesh.deliver(envelope)?;
        Ok(Reply::Text(format!("已发送给 {}", target.label())))
    })();
    finish(out, result)
}

extern "C" fn cmd_ask(raw: StbStringRef, out: *mut StbString, _ud: *mut c_void) -> i32 {
    let args = args_of(raw);
    let result = (|| -> Result<Reply, String> {
        let mesh = mesh()?;
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
            let items = selector_items(&mesh)?;
            return Ok(selector_payload(&items));
        };
        let Some(task) = flow.text.clone().filter(|t| !t.trim().is_empty()) else {
            return Ok(editor_payload(
                format!("委派给 {target_query} 的任务"),
                &flow.draft,
            ));
        };
        let target = mesh.resolve_target(&target_query)?;
        // correlation 让目标实例在回合结束时把结果投回本实例（否则结果只会
        // 出现在目标自己的会话里）。
        let correlation = rpi_mesh_core::protocol::new_id();
        let envelope = Envelope::text(&mesh.node_id, &mesh.name(), &target.node_id, task, now_ms())
            .map_err(|e| e.to_string())?
            .with_correlation(&correlation);
        mesh.deliver(envelope)?;
        Ok(Reply::Text(format!(
            "任务已交给 {}；结果会以通知形式返回",
            target.label()
        )))
    })();
    finish(out, result)
}

extern "C" fn cmd_mesh(raw: StbStringRef, out: *mut StbString, _ud: *mut c_void) -> i32 {
    let args = args_of(raw);
    let result = (|| -> Result<Reply, String> {
        let mesh = mesh()?;
        let (sub, rest) = split_target(&args);
        match sub {
            "" | "status" => {
                // Build fingerprint: proves which build a running session loaded.
                const BUILD: &str = concat!(env!("CARGO_PKG_VERSION"), "@", env!("BUILD_STAMP"));
                let stats = mesh.store.stats(now_ms()).map_err(|e| e.to_string())?;
                let holder = mesh
                    .lease_observer
                    .holder("wechat")
                    .map_err(|e| e.to_string())?;
                let remote_line = match mesh.config.remote.role {
                    Some(rpi_mesh_core::config::RemoteRole::Host) => format!(
                        "host，监听 {}",
                        mesh.config.remote.relay.listen.as_deref().unwrap_or("127.0.0.1:9527")
                    ),
                    Some(rpi_mesh_core::config::RemoteRole::Guest) => format!(
                        "guest，连接 {}",
                        mesh.config.remote.relay.connect.as_deref().unwrap_or("(未设置)")
                    ),
                    None => "local 单机".to_string(),
                };
                let token_path = mesh.dir().join("relay-token");
                let token_state = match std::fs::read_to_string(&token_path) {
                    Ok(text) if !text.trim().is_empty() => {
                        format!("已就位（{}…）", &text.trim()[..8])
                    }
                    _ => "未设置".to_string(),
                };
                Ok(format!(
                    "构建: {BUILD}\n实例: {}\n节点: {}\n模式: {remote_line}\ntoken: {token_state}\n实例数: {}\n待投递: {}/{} 条\n微信接管: {}",
                    mesh.label(),
                    mesh.node_id,
                    stats.instances,
                    stats.envelopes_pending,
                    stats.envelopes_total,
                    holder
                        .map(|h| format!("{}（generation {}）", h.owner_name, h.generation))
                        .unwrap_or_else(|| "(无)".to_string())
                ))
            }
            "who" => tools::format_leases(&mesh),
            "name" => {
                if rest.is_empty() {
                    return Ok(Reply::Text(format!("当前实例名: {}", mesh.name())));
                }
                let name = mesh.rename(rest)?;
                Ok(format!("实例名已设为 {name}"))
            }
            "leave" => {
                mesh.request_shutdown();
                Ok("本实例已退出 mesh（/reload 或重启后重新加入）".to_string())
            }
            "relay" => {
                // /mesh relay [port] — become the relay host, persist, guide.
                let port = rest.trim().parse::<u16>().unwrap_or(9527);
                let listen = format!("0.0.0.0:{port}");
                let mut config = mesh.config.clone();
                config.remote.role = Some(rpi_mesh_core::config::RemoteRole::Host);
                config.remote.relay.listen = Some(listen.clone());
                config.remote.relay.connect = None;
                config.remote.relay.http = true;
                rpi_mesh_core::config::write(mesh.dir(), &config)
                    .map_err(|e| format!("写入配置失败: {e}"))?;
                let token = ensure_relay_token(mesh.dir())?;
                let ip = local_ip().unwrap_or_else(|| mesh.host.clone());
                switch_backends(&mesh, &config);
                Ok(format!(
                    "✅ 已转为 host（监听 {listen}，立即生效）\n对外 IP: {ip}\n\n其它机器加入:\n  1. 把 relay-token 复制到那台机器的 ~/.rpi/agent/mesh/relay-token\n     token: {token}\n  2. 那台机器执行 /mesh client {ip}:{port}"
                ))
            }
            "client" => {
                // /mesh client <ip:port> [token] — join a relay, persist.
                // The token comes from the relay's /mesh relay output; typing
                // it here writes relay-token directly (no file copying).
                let parts: Vec<&str> = rest.trim().split_whitespace().collect();
                let Some(endpoint) = parts.first() else {
                    return Err("用法: /mesh client <ip:端口> [relay的token]".into());
                };
                let mut config = mesh.config.clone();
                config.remote.role = Some(rpi_mesh_core::config::RemoteRole::Guest);
                config.remote.relay.connect = Some(endpoint.to_string());
                config.remote.relay.listen = None;
                config.remote.relay.http = true;
                rpi_mesh_core::config::write(mesh.dir(), &config)
                    .map_err(|e| format!("写入配置失败: {e}"))?;
                let token_path = mesh.dir().join("relay-token");
                let token_hint = match parts.get(1) {
                    Some(token) => {
                        write_relay_token(&token_path, token)?;
                        "relay-token 已写入（来自你输入的 token）".to_string()
                    }
                    None if token_path.exists() => "relay-token 已就位".to_string(),
                    None => format!(
                        "⚠ 尚未设置 token：在 relay 机器上执行 /mesh token 获取，然后\n   /mesh token <token> 写入本机"
                    ),
                };
                switch_backends(&mesh, &config);
                Ok(format!(
                    "✅ 已转为 guest（连接 {endpoint}，立即生效）\n{token_hint}"
                ))
            }
            "local" => {
                let mut config = mesh.config.clone();
                config.remote.role = None;
                config.remote.relay.listen = None;
                config.remote.relay.connect = None;
                rpi_mesh_core::config::write(mesh.dir(), &config)
                    .map_err(|e| format!("写入配置失败: {e}"))?;
                switch_backends(&mesh, &config);
                Ok("✅ 已转为 local 单机模式（立即生效）".to_string())
            }
            "token" => {
                // /mesh token — show; /mesh token <value> — set (clients).
                let value = rest.trim();
                if value.is_empty() {
                    let token = ensure_relay_token(mesh.dir())?;
                    Ok(format!("relay-token: {token}"))
                } else {
                    let path = mesh.dir().join("relay-token");
                    write_relay_token(&path, value)?;
                    Ok("relay-token 已设置（/reload 生效）".to_string())
                }
            }
            other => Err(format!(
                "未知子命令 {other:?}；可用: status | relay [端口] | client <ip:端口> | local | token | who | name <名> | leave"
            )),
        }
        .map(Reply::Text)
    })();
    finish(out, result)
}
