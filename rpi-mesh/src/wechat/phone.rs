//! WeChat phone commands: a precise-whitelist parser over inbound text so
//! `/instances` and `/use <编号|名字>` work from the phone (typing long text
//! is painful; numbered hand-off is the whole point). Everything outside the
//! whitelist — including any other `/foo` like `/etc/hosts` — stays a normal
//! message for the agent.
//!
//! Single source for the list: `rpi-mesh-core::render` (same ordering the TUI
//! shows), so `/use 3` anchors to the list the user just saw. The lease
//! hand-off goes through `LeaseAdmin` (the user-command path of the PR-2
//! capability split) and works across machines: the store is the shared
//! mesh.db (or the relay RPC), so the target may be a remote instance.

use std::sync::Arc;

use rpi_mesh_core::api::MeshStore;
use rpi_mesh_core::lease::LeaseAdmin;
use rpi_mesh_core::render::{render_instances_text, RenderContext};

use crate::wechat::channel::Channel;

/// The parsed command. `Unknown` carries no marker — the caller falls through
/// to the normal agent-injection path.
pub enum WxCommand {
    /// `/instances` — the numbered list.
    Instances,
    /// `/use` with an argument (number or name). Bare `/use` yields
    /// `Use(None)` (render the list + hint).
    Use(Option<String>),
    /// `/new` — start a new session on THIS instance (the bound one).
    New,
    /// `/status` — this instance's session facts (model/session/cwd/lease).
    Status,
    /// `/help` — the command list.
    Help,
    /// `/start [cwd]` — launch a new instance on this machine (cwd omitted =
    /// the bound instance's cwd).
    Start(Option<String>),
    /// `/stop <编号|名字>` — stop a local instance's tmux session.
    Stop(String),
}

/// Parse a trimmed inbound text. Exact-match whitelist:
/// - `/instances`
/// - `/use` (bare) or `/use <arg>` (single space, arg non-empty, not itself a
///   command-looking token)
pub fn parse(text: &str) -> Option<WxCommand> {
    let text = text.trim();
    if text == "/instances" {
        return Some(WxCommand::Instances);
    }
    if text == "/use" {
        return Some(WxCommand::Use(None));
    }
    if text == "/new" {
        return Some(WxCommand::New);
    }
    if text == "/status" {
        return Some(WxCommand::Status);
    }
    if text == "/help" {
        return Some(WxCommand::Help);
    }
    if text == "/start" {
        return Some(WxCommand::Start(None));
    }
    // /start's argument is a PATH — absolute paths (leading '/') are valid
    // and common ("/start /tmp"); the command-like guard only applies to
    // instance-name arguments (/use, /msg, /ask).
    if let Some(arg) = text.strip_prefix("/start ") {
        let arg = arg.trim();
        if !arg.is_empty() {
            return Some(WxCommand::Start(Some(arg.to_string())));
        }
    }
    if let Some(arg) = text.strip_prefix("/stop ") {
        let arg = arg.trim();
        if !arg.is_empty() {
            return Some(WxCommand::Stop(arg.to_string()));
        }
    }
    if let Some(arg) = text.strip_prefix("/use ") {
        let arg = arg.trim();
        if !arg.is_empty() && !arg.starts_with('/') {
            return Some(WxCommand::Use(Some(arg.to_string())));
        }
    }
    None
}

/// `/instances` text: numbered rows (viewer = this instance) + lease footer.
pub fn instances_text(mesh: &Arc<Channel>, now: i64) -> Result<String, String> {
    let instances = mesh
        .store()
        .list_instances_alive(now, crate::wechat::channel::ALIVE_MS)
        .map_err(|e| e.to_string())?;
    let holder = mesh
        .lease_owner
        .holder("wechat")
        .map_err(|e| e.to_string())?
        .filter(|h| h.expires_at > now);
    let holder_node = holder.as_ref().map(|h| h.owner_node.clone());
    let viewer = RenderContext {
        viewer_node_id: &mesh.node_id(),
        wechat_holder_node: holder_node.as_deref(),
        remote: mesh.mesh.config.remote.role.is_some(),
    };
    let holder_triple = holder.map(|h| (h.full_name(), h.generation, h.expires_at));
    Ok(render_instances_text(
        &instances,
        &viewer,
        now,
        holder_triple,
    ))
}

/// Wait bound for the target's `/status` reply after a cross-instance hand-off.
const STATUS_REPLY_WAIT_MS: u64 = 5_000;

/// `/use <arg>`: resolve by NUMBER (anchored to `instances_text` order) or by
/// name/label/prefix (same semantics as the TUI's resolve_target), then
/// assign through `LeaseAdmin`. Returns the confirmation text followed by the
/// target's /status: local targets render from the heartbeat row; remote
/// targets get a `cmd status` envelope (their worker replies with the text,
/// which this bridge thread polls for, bounded) — so the phone always shows
/// what it just handed the channel to.
pub fn use_command(mesh: &Arc<Channel>, arg: &str) -> Result<String, String> {
    let now = rpi_mesh_core::now_ms();
    let instances = mesh
        .store()
        .list_instances_alive(now, crate::wechat::channel::ALIVE_MS)
        .map_err(|e| e.to_string())?;
    if instances.is_empty() {
        return Err("没有登记的实例".to_string());
    }
    let target = resolve_by_list(&instances, arg)?;
    // Self-target with a healthy lease of our own: nothing to do. Otherwise
    // fall through and assign — "/use self" is how the operator takes the
    // channel back when another instance holds it.
    if target.node_id == mesh.node_id() {
        let mine = mesh
            .lease_owner
            .holder("wechat")
            .ok()
            .flatten()
            .is_some_and(|h| h.owner_node == mesh.node_id() && h.expires_at > now);
        if mine {
            return Ok(format!(
                "微信接管已在本实例（{}），无需切换\n\n{}",
                target.label(),
                status_text(mesh, now)
            ));
        }
    }
    let admin = LeaseAdmin::new(
        Arc::clone(&mesh.store()) as Arc<dyn MeshStore>,
        mesh.config().lease.ttl_ms(),
    );
    let generation = admin.assign_to("wechat", &target, now)?;
    let confirmation = format!(
        "微信接管已切换到 {}（generation {generation}）",
        target.label()
    );
    if target.node_id == mesh.node_id() {
        return Ok(format!("{confirmation}\n\n{}", status_text(mesh, now)));
    }
    match remote_status(mesh, &target, now) {
        Some(status) => Ok(format!("{confirmation}\n\n{status}")),
        None => Ok(format!(
            "{confirmation}\n（{}/status 暂未响应，稍后可用 /instances 查看）",
            target.label()
        )),
    }
}

/// Ask a REMOTE instance for its status: a correlated `cmd status` envelope;
/// the target's worker executes its intent and replies. Polls this node's
/// inbox for the correlated reply and acks it. `None` on timeout.
fn remote_status(
    mesh: &Arc<Channel>,
    target: &rpi_mesh_core::store::InstanceView,
    now: i64,
) -> Option<String> {
    let correlation = rpi_mesh_core::protocol::new_id();
    let intent = rpi_mesh_core::protocol::CommandIntent::new("status");
    let envelope = rpi_mesh_core::protocol::Envelope::command(
        &mesh.node_id(),
        &mesh.name(),
        &target.node_id,
        intent,
        now,
    )
    .with_correlation(&correlation);
    mesh.store().send_envelope(&envelope).ok()?;

    let deadline = now + STATUS_REPLY_WAIT_MS as i64;
    loop {
        std::thread::sleep(std::time::Duration::from_millis(200));
        let now_i = rpi_mesh_core::now_ms();
        if now_i >= deadline {
            return None;
        }
        let inbox = mesh.store().inbox(&mesh.node_id(), now_i, 32).ok()?;
        for envelope in inbox {
            if envelope.kind == rpi_mesh_core::protocol::EnvelopeKind::Reply
                && envelope.correlation.as_deref() == Some(correlation.as_str())
            {
                let _ = mesh.store().ack_envelope(&envelope.id, &mesh.node_id(), now_i);
                let text = envelope.text.unwrap_or_default();
                return Some(text);
            }
        }
    }
}

/// Number first (`/use 3` → the 3rd row of the rendered list), then the TUI's
/// name/label/prefix semantics against the SAME list (so both anchors agree).
fn resolve_by_list(
    instances: &[rpi_mesh_core::store::InstanceView],
    arg: &str,
) -> Result<rpi_mesh_core::store::InstanceView, String> {
    if let Ok(number) = arg.parse::<usize>() {
        if number == 0 {
            return Err("编号从 1 开始（见 /instances）".to_string());
        }
        return instances.get(number - 1).cloned().ok_or_else(|| {
            format!(
                "没有编号为 {number} 的实例；当前列表见 /instances（共 {} 行）",
                instances.len()
            )
        });
    }
    // Name/label/prefix: same rules as the TUI (full label → name → base name).
    let query = arg.trim();
    if let Some(found) = instances
        .iter()
        .find(|i| i.label() == query || i.name == query)
    {
        return Ok(found.clone());
    }
    let matches: Vec<&rpi_mesh_core::store::InstanceView> = instances
        .iter()
        .filter(|i| i.name.starts_with(query))
        .collect();
    match matches.len() {
        1 => Ok(matches[0].clone()),
        0 => Err(format!("没有匹配 {query:?} 的实例；当前列表见 /instances")),
        _ => Err(format!(
            "{query:?} 匹配到 {} 个实例，请用完整名 name@host 或编号",
            matches.len()
        )),
    }
}

/// `/status`: this instance's facts — identity, model, session, cwd, busy,
/// and where the wechat lease currently sits. All from the heartbeat-fed
/// store + GetExecutionContext snapshot (no model turn spent).
pub fn status_text(mesh: &Arc<Channel>, now: i64) -> String {
    // Facts come from the heartbeat-fed instances row (3s cadence): the same
    // data /instances shows, without spending a model turn.
    let instance = mesh.store().get_instance(&mesh.node_id()).ok().flatten();
    let holder = mesh.lease_owner.holder("wechat").ok().flatten();
    let mine = holder.as_ref().is_some_and(|h| h.owner_node == mesh.node_id());
    let mut lines = vec![format!("实例: {}", mesh.label())];
    if let Some(model) = instance.as_ref().and_then(|i| i.model.as_deref()) {
        lines.push(format!("模型: {model}"));
    }
    if let Some(session) = instance.as_ref().and_then(|i| i.session_id.as_deref()) {
        let short: String = session.chars().take(8).collect();
        lines.push(format!("会话: {short}…"));
    }
    if let Some(view) = instance.as_ref() {
        if !view.cwd.is_empty() {
            lines.push(format!("目录: {}", view.cwd));
        }
        lines.push(format!(
            "状态: {}",
            if view.busy {
                "忙（生成中）"
            } else {
                "空闲"
            }
        ));
        // Session age is the cheap task identifier until a session-title
        // action exists in the host ABI (see roadmap).
        let up_minutes = (now.saturating_sub(view.started_at)) / 60_000;
        let age = if up_minutes >= 60 {
            format!("{}h{}m", up_minutes / 60, up_minutes % 60)
        } else {
            format!("{up_minutes}m")
        };
        lines.push(format!("启动: {age}前"));
        lines.push(format!("心跳: {}s 前", (now - view.last_seen) / 1000));
    }
    match holder {
        Some(h) if mine => lines.push(format!("微信: 本实例持有（generation {}）", h.generation)),
        Some(h) => lines.push(format!("微信: {} 持有", h.full_name())),
        None => lines.push("微信: 未接管".to_string()),
    }
    lines.join("\n")
}

const HELP: &str = "微信命令（直接发送，不经过 AI）:\n\
  /instances  实例列表（带编号）\n\
  /use <编号|名字>  切换微信接管\n\
  /use        查看列表+提示\n\
  /new        本实例开新会话\n\
  /status     本实例状态\n\
  /help       本帮助\n\
  /start [目录]  本机启动新实例（缺省=本实例目录）\n\
  /stop <编号|名字>  停止本机实例\n\
  （跨机启动/停止在 TUI：/start <实例>:<目录>）\n\
其它文本原样发给当前实例的 AI。";

/// Assemble the core SpawnContext for the wechat plugin. `cwd_override`
/// wins; otherwise the bound instance's cwd (the phone's "start here").
fn start_ctx<'a>(
    mesh: &'a Arc<Channel>,
    cwd_override: Option<&'a str>,
    store_dyn: &'a Arc<dyn MeshStore>,
) -> rpi_mesh_core::instance_start::SpawnContext<'a> {
    let cwd = match cwd_override.filter(|s| !s.trim().is_empty()) {
        Some(arg) => arg.to_string(),
        None => mesh
            .store()
            .get_instance(&mesh.node_id())
            .ok()
            .flatten()
            .map(|v| v.cwd)
            .unwrap_or_default(),
    };
    rpi_mesh_core::instance_start::SpawnContext {
        store: store_dyn.clone(),
        node_id: &mesh.node_id(),
        host: &mesh.mesh.host,
        cwd,
        spawn: &mesh.config().instance_spawn,
    }
}

/// Execute a parsed command on the mesh and return the reply text.
pub fn execute(mesh: &Arc<Channel>, command: WxCommand) -> Result<String, String> {
    let now = rpi_mesh_core::now_ms();
    match command {
        WxCommand::Instances => instances_text(mesh, now),
        WxCommand::Use(None) => {
            let mut text = instances_text(mesh, now)?;
            text.push_str("\n\n回复 /use <编号> 切换微信接管");
            Ok(text)
        }
        WxCommand::Use(Some(arg)) => use_command(mesh, &arg),
        WxCommand::New => {
            let result = mesh.action(
                rpi_plugin_sdk::RuntimeActionId::NewSession,
                serde_json::json!({}),
            )?;
            let session = result
                .get("sessionId")
                .and_then(|v| v.as_str())
                .unwrap_or("(新)");
            Ok(format!("已开启新会话 {session}"))
        }
        WxCommand::Status => Ok(status_text(mesh, now)),
        WxCommand::Help => Ok(HELP.to_string()),
        WxCommand::Start(cwd) => {
            // Bare number = an /instances row number: start a new instance on
            // that instance's machine (its own cwd).
            if let Some(text) = cwd.as_deref() {
                if let Ok(number) = text.trim().parse::<usize>() {
                    let now = rpi_mesh_core::now_ms();
                    let instances = mesh
                        .mesh
                        .store
                        .list_instances_alive(now, crate::mesh::ALIVE_MS)
                        .map_err(|e| e.to_string())?;
                    let target = instances
                        .get(number.wrapping_sub(1))
                        .ok_or_else(|| format!("没有编号为 {number} 的实例"))
                        .map_err(|e| e.to_string())?;
                    if target.node_id != mesh.node_id() {
                        return crate::commands::forward_start(
                            &mesh.mesh,
                            target,
                            &target.cwd,
                        );
                    }
                    let store_dyn = Arc::clone(&mesh.store()) as Arc<dyn MeshStore>;
                    let spawn_ctx = start_ctx(mesh, Some(target.cwd.as_str()), &store_dyn);
                    return rpi_mesh_core::instance_start::start(
                        &spawn_ctx,
                        Some(target.cwd.as_str()),
                    );
                }
            }
            // Cross-machine form: "<instance>:<dir>" — forward to that
            // instance's machine (same as the TUI's /start forwarding).
            if let Some(cwd) = cwd.as_ref().filter(|c| c.contains(':')) {
                if let Some((executor_query, dir)) = cwd.split_once(':') {
                    let executor_query = executor_query.trim();
                    let dir = dir.trim();
                    if !executor_query.is_empty() && !dir.is_empty() {
                        let executor = mesh.mesh.resolve_target(executor_query)?;
                        if executor.node_id != mesh.node_id() {
                            return crate::commands::forward_start(
                                &mesh.mesh,
                                &executor,
                                dir,
                            );
                        }
                        // Executor is self: local start with the dir.
                        let store_dyn = mesh.mesh.store_dyn();
                        let spawn_ctx = start_ctx(mesh, Some(dir), &store_dyn);
                        return rpi_mesh_core::instance_start::start(
                            &spawn_ctx,
                            Some(dir),
                        );
                    }
                }
            }
            let store_dyn = Arc::clone(&mesh.store()) as Arc<dyn MeshStore>;
            let spawn_ctx = start_ctx(mesh, cwd.as_deref(), &store_dyn);
            rpi_mesh_core::instance_start::start(&spawn_ctx, cwd.as_deref())
        }
        WxCommand::Stop(query) => {
            // Auto-routing (same as the TUI /stop): same machine → local
            // stop; remote → forward to a live instance on that machine.            let now = rpi_mesh_core::now_ms();
            let instances = mesh
                .mesh
                .store
                .list_instances_alive(now, crate::mesh::ALIVE_MS)
                .map_err(|e| e.to_string())?;
            let target = crate::commands::resolve_target_by_list(
                &instances,
                &query,
                &mesh.node_id(),
            )
            .map_err(|e| e.to_string())?;
            if target.node_id == mesh.node_id() {
                return Err("不能用 /stop 停掉当前实例（先 /use 切走再停）".to_string());
            }
            if target.host != mesh.mesh.host {
                let executor = instances
                    .iter()
                    .find(|i| i.host == target.host && i.node_id != target.node_id)
                    .unwrap_or(&target);
                return crate::commands::forward_stop(&mesh.mesh, executor, &target.label());
            }
            let store_dyn = Arc::clone(&mesh.store()) as Arc<dyn MeshStore>;
            let spawn_ctx = start_ctx(mesh, None, &store_dyn);
            rpi_mesh_core::instance_start::stop(&spawn_ctx, &query, now)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_matches_the_exact_whitelist() {
        assert!(matches!(parse("/instances"), Some(WxCommand::Instances)));
        assert!(matches!(
            parse("  /instances  "),
            Some(WxCommand::Instances)
        ));
        assert!(matches!(parse("/use"), Some(WxCommand::Use(None))));
        assert!(matches!(parse("/new"), Some(WxCommand::New)));
        assert!(matches!(parse("/status"), Some(WxCommand::Status)));
        assert!(matches!(parse("/help"), Some(WxCommand::Help)));
        assert!(matches!(
            parse("/use 3"),
            Some(WxCommand::Use(Some(ref a))) if a == "3"
        ));
        // Case-sensitive: /INSTANCES is a message, not a command.
        assert!(parse("/INSTANCES").is_none());
        // Non-whitelist slash paths stay messages.
        assert!(parse("/etc/hosts").is_none());
        assert!(parse("/usr/bin/env").is_none());
        // `/use` with a command-like argument is not a command. ("/use " with
        // trailing whitespace trims to bare `/use` = render the list + hint.)
        assert!(parse("/use /instances").is_none());
        // /start's argument is a PATH: absolute paths are commands.
        assert!(matches!(
            parse("/start /tmp"),
            Some(WxCommand::Start(Some(ref a))) if a == "/tmp"
        ));
        assert!(matches!(
            parse("/start ~/projects"),
            Some(WxCommand::Start(Some(ref a))) if a == "~/projects"
        ));
        assert!(matches!(parse("/start"), Some(WxCommand::Start(None))));
        // Ordinary text and code snippets are untouched.
        assert!(parse("hello /instances world").is_none());
        assert!(parse("look at /use 3 in the docs").is_none());
    }

    fn view(name: &str, node: &str, cwd: &str) -> rpi_mesh_core::store::InstanceView {
        rpi_mesh_core::store::InstanceView {
            node_id: node.into(),
            name: name.into(),
            host: "h1".into(),
            pid: 1,
            cwd: cwd.into(),
            channels: serde_json::json!({}),
            role: "normal".into(),
            session_id: None,
            title: None,
            model: None,
            busy: false,
            started_at: 0,
            last_seen: 0,
        }
    }

    #[test]
    fn resolve_by_list_number_and_name_and_errors() {
        let instances = vec![
            view("alpha", "node-a", "/a"),
            view("beta", "node-b", "/b"),
            view("gamma", "node-c", "/c"),
        ];
        // Numbers are 1-based over the list order.
        assert_eq!(resolve_by_list(&instances, "2").unwrap().name, "beta");
        // Zero and overflow.
        assert!(resolve_by_list(&instances, "0").is_err());
        assert!(resolve_by_list(&instances, "9").is_err());
        // Names: exact, label, unique prefix.
        assert_eq!(resolve_by_list(&instances, "gamma").unwrap().name, "gamma");
        assert_eq!(
            resolve_by_list(&instances, "gamma@h1").unwrap().name,
            "gamma"
        );
        assert_eq!(resolve_by_list(&instances, "gam").unwrap().name, "gamma");
        // Ambiguous prefix errors, but an EXACT name beats the ambiguity.
        let with_dup: Vec<_> = instances
            .into_iter()
            .chain(std::iter::once(view("gamma2", "node-d", "/d")))
            .collect();
        assert_eq!(resolve_by_list(&with_dup, "gamma").unwrap().name, "gamma");
        assert!(resolve_by_list(&with_dup, "gamm").is_err());
        assert!(resolve_by_list(&with_dup, "nope").is_err());
    }
}
