//! `/start` and `/stop`: launch and stop rpi instances on the LOCAL machine.
//!
//! Shared by the hub plugin (TUI commands) and the wechat plugin (phone
//! commands) — both talk to the same `HubStore`, which is why this lives in
//! core rather than either plugin.
//!
//! Design:
//! - cwd omitted ⇒ the calling instance's cwd;
//! - MULTIPLE instances per cwd allowed (tmux session auto-increments; rpi
//!   appends `-<pid>` to colliding instance names);
//! - mode from `InstanceSpawnConfig` (`tmux` default, `none` disables);
//! - success confirmed by polling the store for a new presence row;
//! - cwd/session-name cross a shell line: same whitelist as remote.rs.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::api::HubStore;
use crate::config::SpawnMode;
use crate::now_ms;
use crate::store::InstanceView;

/// How long to wait for the new instance's presence row after spawn.
const STARTUP_WAIT: Duration = Duration::from_secs(10);

/// Character whitelist for a user-supplied cwd crossing a shell command line
/// (same standard as remote.rs). No `$ ; | & \`` < > * ?` — no substitution,
/// no redirection, no globbing.
fn is_safe_cwd(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 1024
        && value.chars().all(|c| {
            c.is_ascii_alphanumeric() || matches!(c, '/' | '.' | '_' | '-' | '~' | ' ' | '+' | '=')
        })
}

/// Base name for the tmux session: the cwd's last component, stripped to
/// tmux-safe characters. `/tmp/` → `tmp`; a path with no usable component →
/// `instance`.
fn session_base(cwd: &Path) -> String {
    let name = cwd
        .file_name()
        .map(|n| n.to_string_lossy().to_string())
        .unwrap_or_default();
    let sanitized: String = name
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.'))
        .take(48)
        .collect();
    if sanitized.is_empty() {
        "instance".to_string()
    } else {
        sanitized
    }
}

fn session_exists(tmux: &str, name: &str) -> bool {
    Command::new(tmux)
        .args(["has-session", "-t", name])
        .output()
        .map(|out| out.status.success())
        .unwrap_or(false)
}

/// First free tmux session name: `<prefix>-<base>`, `<prefix>-<base>-2`, …
fn next_session_name(prefix: &str, tmux: &str, base: &str) -> String {
    let candidate = format!("{prefix}-{base}");
    if !session_exists(tmux, &candidate) {
        return candidate;
    }
    for n in 2..=99 {
        let candidate = format!("{prefix}-{base}-{n}");
        if !session_exists(tmux, &candidate) {
            return candidate;
        }
    }
    format!("{prefix}-{base}-{}", std::process::id())
}

/// Where the new instance lands.
enum Placement {
    /// A new window inside the host process's existing tmux session.
    Window(String),
    /// A dedicated detached session.
    Session(String),
}

/// The current tmux session name, from `$TMUX` (absent outside tmux).
fn current_session(tmux: &str) -> Result<String, String> {
    if std::env::var("TMUX").map(|v| v.is_empty()).unwrap_or(true) {
        return Err("不在 tmux 中（$TMUX 缺失）".to_string());
    }
    let output = Command::new(tmux)
        .args(["display-message", "-p", "-F", "#{session_name}"])
        .output()
        .map_err(|error| format!("cannot run {tmux}: {error}"))?;
    let name = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if name.is_empty() {
        return Err("session name 为空".to_string());
    }
    Ok(name)
}

/// A new window in an EXISTING session. Window name = the basename (the
/// running rpi does not rename windows, verified with allow-rename on).
fn spawn_window(
    tmux: &str,
    command: &[String],
    session: &str,
    base: &str,
    cwd: &Path,
) -> Result<(), String> {
    let status = Command::new(tmux)
        .args([
            "new-window",
            "-d",
            "-t",
            session,
            "-n",
            base,
            "-c",
            &cwd.to_string_lossy(),
        ])
        .args(command)
        .status()
        .map_err(|error| format!("cannot run {tmux}: {error}"))?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("{tmux} new-window exited with {status}"))
    }
}

fn spawn_tmux(tmux: &str, command: &[String], session: &str, cwd: &Path) -> Result<(), String> {
    let status = Command::new(tmux)
        .args([
            "new-session",
            "-d",
            "-s",
            session,
            "-c",
            &cwd.to_string_lossy(),
        ])
        .args(command)
        .status()
        .map_err(|error| format!("cannot run {tmux}: {error}"))?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("{tmux} new-session exited with {status}"))
    }
}

/// What the spawn/stop functions need from the calling plugin. Both plugins
/// assemble this from their own Hub — no cross-plugin type dependency.
pub struct SpawnContext<'a> {
    pub store: &'a Arc<dyn HubStore>,
    /// The calling instance's node_id (self-identification).
    pub node_id: &'a str,
    /// The calling instance's host (stop is local-machine only).
    pub host: &'a str,
    /// The calling instance's cwd (the default for `/start`).
    pub cwd: String,
    pub spawn: &'a crate::config::InstanceSpawnConfig,
}

fn which_tmux(tmux: &str) -> bool {
    Command::new(tmux)
        .arg("-V")
        .output()
        .map(|out| out.status.success())
        .unwrap_or(false)
}

/// Expand a leading `~` (we exec tmux directly; no shell).
fn expand_home(arg: &str) -> String {
    if arg == "~" {
        return std::env::var("HOME").unwrap_or_else(|_| arg.to_string());
    }
    if let Some(rest) = arg.strip_prefix("~/") {
        if let Ok(home) = std::env::var("HOME") {
            return format!("{home}/{rest}");
        }
    }
    arg.to_string()
}

/// `/start [cwd]`: launch a new instance and confirm via the presence table.
pub fn start(ctx: &SpawnContext<'_>, cwd_arg: Option<&str>) -> Result<String, String> {
    if ctx.spawn.mode == SpawnMode::None {
        return Err("本机配置禁用了 /start（instanceSpawn.mode = none）".to_string());
    }
    let cwd: PathBuf = match cwd_arg.map(str::trim).filter(|s| !s.is_empty()) {
        Some(arg) => {
            if !is_safe_cwd(arg) {
                return Err(format!("目录含不支持的字符: {arg:?}"));
            }
            let expanded = expand_home(arg);
            let path = PathBuf::from(&expanded);
            if !path.is_dir() {
                return Err(format!("目录不存在: {expanded}"));
            }
            path
        }
        None => PathBuf::from(&ctx.cwd),
    };
    let tmux = &ctx.spawn.tmux_command;
    if !which_tmux(tmux) {
        return Err(format!("tmux 不可用（{tmux} -V 失败）：/start 需要 tmux"));
    }

    let argv = ctx.spawn.command.argv();
    if argv.is_empty() || argv[0].trim().is_empty() {
        return Err("instanceSpawn.command 不能为空".to_string());
    }
    let base = session_base(&cwd);
    let before = rows(ctx)?;
    // Placement: a WINDOW in the current tmux session when this rpi runs
    // inside tmux ($TMUX set), a dedicated detached session otherwise.
    // That is the DEFAULT behavior — a fresh machine needs no config. The
    // only config knob left is `none` (disable /start); `tmux` and
    // `currentSession` are accepted legacy spellings of the same default.
    let placement = if std::env::var_os("TMUX").is_some() {
        current_session(tmux).map(Placement::Window)
    } else {
        Ok(Placement::Session(next_session_name(
            &ctx.spawn.session_prefix,
            tmux,
            &base,
        )))
    };
    let placement = placement.map_err(|e| format!("tmux 会话探测失败: {e}"))?;
    match &placement {
        Placement::Window(session) => spawn_window(tmux, &argv, session, &base, &cwd)?,
        Placement::Session(session) => spawn_tmux(tmux, &argv, session, &cwd)?,
    }
    let placement_label = match &placement {
        Placement::Window(session) => {
            format!("窗口 {session}（session {session}）→ tmux attach -t {session}")
        }
        Placement::Session(session) => {
            format!("tmux 会话 {session} → tmux attach -t {session}")
        }
    };
    let placement_desc = placement_label.clone();

    let deadline = Instant::now() + STARTUP_WAIT;
    while Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(300));
        let after = rows(ctx)?;
        if let Some(new_rows) = added_rows(&before, &after) {
            return Ok(format!(
                "✅ 已启动新实例 {}（{placement_desc}）",
                new_rows.join("、")
            ));
        }
    }
    Ok(format!(
        "已启动（{placement_desc}），但 10s 内未见新实例注册——attach 检查"
    ))
}

/// `/stop <编号|名字>`: kill the target's tmux session (the plugin lifecycle
/// releases the wechat lease and unregisters presence on exit).
pub fn stop(ctx: &SpawnContext<'_>, query: &str, now: i64) -> Result<String, String> {
    let instances = ctx
        .store
        .list_instances_alive(now, 30_000)
        .map_err(|e| e.to_string())?;
    let target = resolve_by_list(&instances, query)?;
    if target.node_id == ctx.node_id {
        return Err("不能用 /stop 停掉当前实例（先 /use 切走再停）".to_string());
    }
    if target.host != ctx.host {
        return Err(format!(
            "{} 在远端机器 {}，/stop 只能停本机实例",
            target.label(),
            target.host
        ));
    }
    // Primary: find the instance's tmux pane BY PID (presence row pid —
    // authoritative; window names are user-editable and not an ownership
    // record). Fallback: the instance runs but owns no tmux pane (started
    // outside tmux / detached from it) → SIGTERM the process directly.
    let placement = tmux_target_for(&ctx.spawn.tmux_command, target.pid)?;
    match placement {
        Some((session, window, pane_id)) => {
            // Always address the EXACT pane: "{session}:{window}.{pane_id}".
            // A bare "{session}:" means "the session's CURRENT window" — the
            // active one, which is usually the CALLER's own window (the
            // self-kill bug). With the pane id the kill cannot hit anything
            // but the pane that hosts the target pid.
            let target_spec = format!("{session}:{window}.{pane_id}");
            let status = Command::new(&ctx.spawn.tmux_command)
                .args(["kill-pane", "-t", &target_spec])
                .status()
                .map_err(|error| format!("cannot run {}: {error}", ctx.spawn.tmux_command))?;
            if !status.success() {
                return Err(format!("kill-pane {target_spec} 失败（不存在？）"));
            }
        }
        None => {
            // Process exists (list_instances_alive guaranteed it) but owns
            // no tmux pane: SIGTERM so the plugin lifecycle releases the
            // wechat lease and unregisters cleanly.
            let status = Command::new("kill")
                .args(["-TERM", &target.pid.to_string()])
                .status()
                .map_err(|error| format!("cannot run kill: {error}"))?;
            if !status.success() {
                return Err(format!("kill -TERM {} 失败", target.pid));
            }
        }
    }
    Ok(format!("✅ 已停止 {}", target.label()))
}

/// Find the tmux session whose cwd matches the target (not a name guess).
/// Where the target instance runs: `(session, window)` discovered by cwd via
/// list-windows (covers BOTH placements — dedicated sessions and windows).
/// Resolve the tmux kill target for an instance **by pid** — the presence
/// row's authoritative pid, never window-name conventions:
/// a pane whose leading process is the instance pid (plain `rpi` command),
/// or whose leading process is that pid's parent (a `sh -c "rpi; …"` wrapper).
/// Returns `(session, window)`; window `None` means kill the whole session
/// (the dedicated-session placement where the pane IS the session).
fn tmux_target_for(tmux: &str, pid: u32) -> Result<Option<(String, String, String)>, String> {
    let output = Command::new(tmux)
        .args([
            "list-panes",
            "-a",
            "-F",
            "#{session_name}\t#{window_name}\t#{window_index}\t#{pane_index}\t#{pane_pid}",
        ])
        .output()
        .map_err(|error| format!("cannot run {tmux}: {error}"))?;
    let text = String::from_utf8_lossy(&output.stdout);
    for line in text.lines() {
        let mut parts = line.splitn(5, '\t');
        let (Some(session), Some(window), Some(window_index), Some(pane_index), Some(pane_pid)) = (
            parts.next(),
            parts.next(),
            parts.next(),
            parts.next(),
            parts.next(),
        ) else {
            continue;
        };
        let Ok(pane_pid) = pane_pid.parse::<u32>() else {
            continue;
        };
        let leads_to_target = pane_pid == pid
            || pgrep_child(pane_pid).is_some_and(|children| children.iter().any(|c| *c == pid));
        if !leads_to_target {
            continue;
        }
        // Return the exact pane address. window_index (not the window NAME)
        // anchors the target even if the user renames the window between the
        // scan and the kill; pane_index pins the pane.
        let _ = window;
        return Ok(Some((
            session.to_string(),
            window_index.to_string(),
            pane_index.to_string(),
        )));
    }
    Ok(None)
}

/// Direct children of a pid (best effort; empty on any failure).
fn pgrep_child(pid: u32) -> Option<Vec<u32>> {
    let output = Command::new("pgrep")
        .arg("-P")
        .arg(pid.to_string())
        .output()
        .ok()?;
    Some(
        String::from_utf8_lossy(&output.stdout)
            .lines()
            .filter_map(|l| l.trim().parse().ok())
            .collect(),
    )
}

/// The window-name candidate for a target (basename of its cwd).

fn rows(ctx: &SpawnContext<'_>) -> Result<Vec<String>, String> {
    let now = now_ms();
    let instances = ctx
        .store
        .list_instances_alive(now, 30_000)
        .map_err(|e| e.to_string())?;
    Ok(instances.into_iter().map(|i| i.node_id).collect())
}

/// node_ids present after but not before the spawn.
fn added_rows(before: &[String], after: &[String]) -> Option<Vec<String>> {
    let added: Vec<String> = after
        .iter()
        .filter(|id| !before.contains(id))
        .cloned()
        .collect();
    (!added.is_empty()).then_some(added)
}

/// Number first (`/use 3` → the 3rd row), then name/label/unique-prefix.
pub fn resolve_by_list(instances: &[InstanceView], query: &str) -> Result<InstanceView, String> {
    if let Ok(number) = query.parse::<usize>() {
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
    let query = query.trim();
    if let Some(found) = instances
        .iter()
        .find(|i| i.label() == query || i.name == query)
    {
        return Ok(found.clone());
    }
    let matches: Vec<&InstanceView> = instances
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cwd_whitelist_rejects_injection_vectors() {
        assert!(is_safe_cwd("/Users/me/repos/x"));
        assert!(is_safe_cwd("~/repos/x"));
        for bad in [
            "/x;rm -rf",
            "/x$(whoami)",
            "/x`id`",
            "/x|nc host",
            "/x && y",
            "/x > /tmp/f",
            "",
        ] {
            assert!(!is_safe_cwd(bad), "{bad:?} must be rejected");
        }
    }

    #[test]
    fn session_base_sanitizes_and_falls_back() {
        assert_eq!(
            session_base(Path::new("/Users/me/repos/pi-rust")),
            "pi-rust"
        );
        assert_eq!(session_base(Path::new("/tmp/")), "tmp");
        assert_eq!(session_base(Path::new("/")), "instance");
        assert_eq!(session_base(Path::new("/work/我的 项目")), "instance");
    }

    #[test]
    fn home_expansion_covers_tilde_forms() {
        std::env::set_var("HOME", "/Users/test");
        assert_eq!(expand_home("~"), "/Users/test");
        assert_eq!(expand_home("~/repos/x"), "/Users/test/repos/x");
        assert_eq!(expand_home("/abs/path"), "/abs/path");
        std::env::remove_var("HOME");
    }

    #[test]
    fn added_rows_diffs_snapshots() {
        let before = vec!["a".to_string(), "b".to_string()];
        let after = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        assert_eq!(added_rows(&before, &after), Some(vec!["c".to_string()]));
        assert_eq!(added_rows(&after, &before), None);
        assert_eq!(added_rows(&before, &before), None);
    }
}
