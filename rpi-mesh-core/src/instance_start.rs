//! `/start` and `/stop`: launch and stop rpi instances on the LOCAL machine.
//!
//! Shared by the TUI commands and the phone commands — both talk to the same
//! store, which is why this lives in core rather than either plugin.
//!
//! Design:
//! - cwd omitted ⇒ the calling instance's cwd;
//! - MULTIPLE instances per cwd allowed (session/window names auto-increment;
//!   rpi appends `-<pid>` to colliding instance names);
//! - the multiplexer (tmux, later screen) is behind [`InstanceSpawner`]:
//!   `/start` asks it for a placement (a WINDOW in the current session when
//!   the host runs inside it, a detached session otherwise); `none` disables;
//! - **`/stop` never touches the multiplexer**: the presence row's pid is
//!   authoritative — SIGTERM the process, the plugin lifecycle releases the
//!   lease, and the pane closes itself when its leading process exits;
//! - success confirmed by polling the store for a new presence row;
//! - cwd/session-name cross a shell line: same whitelist as remote.rs.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::api::MeshStore;
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

/// The multiplexer behind `/start`. One implementation per multiplexer; the
/// placement decision (window in current session vs detached session) is its
/// business, because "current session" means something different in each.
pub trait InstanceSpawner {
    /// Whether this multiplexer is usable on this machine.
    fn available(&self) -> bool;
    /// Spawn `argv` in `cwd` as a WINDOW of the caller's current session
    /// (only called when the host process runs inside the multiplexer).
    fn spawn_window_in_current(
        &self,
        name: &str,
        cwd: &Path,
        argv: &[String],
    ) -> Result<(), String>;
    /// Spawn `argv` in `cwd` as a detached session named `name`.
    fn spawn_detached(&self, name: &str, cwd: &Path, argv: &[String]) -> Result<(), String>;
    /// Whether the host process currently runs inside this multiplexer.
    fn host_is_inside(&self) -> bool;
    /// True if a session with this name already exists (name dedup).
    fn session_exists(&self, name: &str) -> bool;
}

/// tmux implementation.
pub struct TmuxSpawner {
    /// The tmux binary path (config: self-compiled or non-PATH installs).
    pub binary: String,
}

impl InstanceSpawner for TmuxSpawner {
    fn available(&self) -> bool {
        Command::new(&self.binary)
            .arg("-V")
            .output()
            .map(|out| out.status.success())
            .unwrap_or(false)
    }

    fn spawn_window_in_current(
        &self,
        name: &str,
        cwd: &Path,
        argv: &[String],
    ) -> Result<(), String> {
        // No `-t <session>`: tmux inherits the caller's current session from
        // $TMUX AND picks the next free window index itself — an explicit
        // `-t <session>` targets the base index and fails with
        // "index N in use" on sessions whose indexes are not dense.
        let status = Command::new(&self.binary)
            .args(["new-window", "-d", "-n", name, "-c", &cwd.to_string_lossy()])
            .args(argv)
            .status()
            .map_err(|error| format!("cannot run {}: {error}", self.binary))?;
        if status.success() {
            Ok(())
        } else {
            Err(format!("{} new-window exited with {status}", self.binary))
        }
    }

    fn spawn_detached(&self, name: &str, cwd: &Path, argv: &[String]) -> Result<(), String> {
        let status = Command::new(&self.binary)
            .args([
                "new-session",
                "-d",
                "-s",
                name,
                "-c",
                &cwd.to_string_lossy(),
            ])
            .args(argv)
            .status()
            .map_err(|error| format!("cannot run {}: {error}", self.binary))?;
        if status.success() {
            Ok(())
        } else {
            Err(format!("{} new-session exited with {status}", self.binary))
        }
    }

    fn host_is_inside(&self) -> bool {
        std::env::var_os("TMUX").is_some_and(|v| !v.is_empty())
    }

    fn session_exists(&self, name: &str) -> bool {
        Command::new(&self.binary)
            .args(["has-session", "-t", name])
            .output()
            .map(|out| out.status.success())
            .unwrap_or(false)
    }
}

/// Base name for a session/window: the cwd's last component, stripped to
/// multiplexer-safe characters. `/tmp/` → `tmp`; no usable component →
/// `instance`.
fn base_name(cwd: &Path) -> String {
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

/// First free session name: `<prefix>-<base>`, `<prefix>-<base>-2`, …
fn next_session_name(spawner: &dyn InstanceSpawner, prefix: &str, base: &str) -> String {
    let candidate = format!("{prefix}-{base}");
    if !spawner.session_exists(&candidate) {
        return candidate;
    }
    for n in 2..=99 {
        let candidate = format!("{prefix}-{base}-{n}");
        if !spawner.session_exists(&candidate) {
            return candidate;
        }
    }
    format!("{prefix}-{base}-{}", std::process::id())
}

/// What the spawn/stop functions need from the calling plugin. Both plugins
/// assemble this from the mesh state — no cross-plugin type dependency.
pub struct SpawnContext<'a> {
    pub store: Arc<dyn MeshStore>,
    /// The calling instance's node_id (self-identification).
    pub node_id: &'a str,
    /// The calling instance's host (stop is local-machine only).
    pub host: &'a str,
    /// The calling instance's cwd (the default for `/start`).
    pub cwd: String,
    pub spawn: &'a crate::config::InstanceSpawnConfig,
}

/// Is `pid` a process on THIS machine? (signal 0 probe; EPERM still means
/// it exists locally). Cross-host pids simply fail the probe.
fn pid_exists_locally(pid: u32) -> bool {
    crate::store::pid_alive(pid)
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
    let spawner: Box<dyn InstanceSpawner> = Box::new(TmuxSpawner {
        binary: ctx.spawn.tmux_command.clone(),
    });
    if !spawner.available() {
        return Err(format!(
            "multiplexer {} 不可用（-V 失败）：/start 需要它",
            ctx.spawn.tmux_command
        ));
    }

    let argv = ctx.spawn.command.argv();
    if argv.is_empty() || argv[0].trim().is_empty() {
        return Err("instanceSpawn.command 不能为空".to_string());
    }
    let base = base_name(&cwd);
    let before = rows(ctx)?;
    // Placement lives in the spawner: a WINDOW in the current session when
    // the host runs inside the multiplexer, a detached session otherwise.
    let spawner_ref: &dyn InstanceSpawner = spawner.as_ref();
    let placement_result = if spawner_ref.host_is_inside() {
        spawner_ref.spawn_window_in_current(&base, &cwd, &argv)
    } else {
        let name = next_session_name(spawner_ref, &ctx.spawn.session_prefix, &base);
        spawner_ref.spawn_detached(&name, &cwd, &argv)
    };
    placement_result?;
    // Confirm via the presence table: a new row whose cwd matches.
    let placement_desc = if spawner_ref.host_is_inside() {
        format!("窗口 {}（当前会话）", base)
    } else {
        format!("独立会话 {}-{}", ctx.spawn.session_prefix, base)
    };
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
    // Local vs remote decided by PID, not the host string: the same machine
    // can report different hostnames (/etc/hostname vs hostname(1) vs env),
    // and a string mismatch would wrongly refuse to stop a local instance.
    if !pid_exists_locally(target.pid) {
        return Err(format!(
            "{} 在远端机器 {}，/stop 只能停本机实例",
            target.label(),
            target.host
        ));
    }
    // No multiplexer involvement: the presence row's pid is authoritative.
    // SIGTERM lets the plugin lifecycle release the wechat lease and
    // unregister cleanly.
    //
    // BEFORE killing: snapshot the parent. If the instance was started
    // MANUALLY inside a shell pane (pane leader = the user's shell, rpi =
    // its child), killing the rpi leaves an idle shell behind and the
    // window stays. If that shell hosts nothing else, TERM it after the
    // instance so the window closes; if the user has other work in it,
    // leave it alone and say so. After the kill the process is gone and
    // ppid can no longer be read — hence the snapshot ordering.
    let parent_snapshot = parent_shell_of(target.pid);
    let status = Command::new("kill")
        .args(["-TERM", &target.pid.to_string()])
        .status()
        .map_err(|error| format!("cannot run kill: {error}"))?;
    if !status.success() {
        return Err(format!("kill -TERM {} 失败", target.pid));
    }
    match parent_snapshot {
        ShellParent::LoneShell(parent) => {
            let _ = Command::new("kill").args(["-TERM", &parent.to_string()]).status();
            Ok(format!(
                "✅ 已停止 {}\n（其父 shell 已一并退出，window 已关闭）",
                target.label()
            ))
        }
        ShellParent::ShellWithOtherWork(parent) => Ok(format!(
            "✅ 已停止 {}\n（其所在 shell（pid {parent}）还有其它进程，window 未关闭）",
            target.label()
        )),
        ShellParent::None => Ok(format!("✅ 已停止 {}", target.label())),
    }
}

/// The instance's parent shell, classified BEFORE it is killed.
#[cfg(unix)]
enum ShellParent {
    /// Parent is a shell whose only child is the instance: killing the shell
    /// closes the window.
    LoneShell(u32),
    /// Parent is a shell but hosts other processes too: leave it.
    ShellWithOtherWork(u32),
    /// Parent is not a shell (instance is its pane's leader — the /start
    /// placement), or the process tree could not be read.
    None,
}

#[cfg(unix)]
fn parent_shell_of(instance_pid: u32) -> ShellParent {
    let Some(parent) = parent_pid(instance_pid) else {
        return ShellParent::None;
    };
    if !is_shell(parent) {
        return ShellParent::None;
    }
    match child_pids(parent) {
        Some(children) if children.iter().all(|c| *c == instance_pid) => {
            ShellParent::LoneShell(parent)
        }
        Some(_) => ShellParent::ShellWithOtherWork(parent),
        None => ShellParent::None,
    }
}

#[cfg(unix)]
fn parent_pid(pid: u32) -> Option<u32> {
    let output = Command::new("ps")
        .args(["-o", "ppid=", "-p", &pid.to_string()])
        .output()
        .ok()?;
    String::from_utf8_lossy(&output.stdout)
        .trim()
        .parse()
        .ok()
}

#[cfg(unix)]
fn is_shell(pid: u32) -> bool {
    let Ok(output) = Command::new("ps")
        .args(["-o", "command=", "-p", &pid.to_string()])
        .output()
    else {
        return false;
    };
    let command = String::from_utf8_lossy(&output.stdout);
    let base = command
        .trim()
        .rsplit('/')
        .next()
        .unwrap_or("")
        .to_string();
    matches!(base.as_str(), "sh" | "bash" | "zsh" | "dash" | "fish")
}

#[cfg(unix)]
fn child_pids(pid: u32) -> Option<Vec<u32>> {
    let output = Command::new("pgrep")
        .args(["-P", &pid.to_string()])
        .output()
        .ok()?;
    Some(
        String::from_utf8_lossy(&output.stdout)
            .lines()
            .filter_map(|l| l.trim().parse().ok())
            .collect(),
    )
}

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
        assert_eq!(base_name(Path::new("/Users/me/repos/pi-rust")), "pi-rust");
        assert_eq!(base_name(Path::new("/tmp/")), "tmp");
        assert_eq!(base_name(Path::new("/")), "instance");
        assert_eq!(base_name(Path::new("/work/我的 项目")), "instance");
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
