//! Delegation to an SSH-reachable machine that has **no running rpi instance**.
//!
//! The office machine can dial the server but not the other way round, so this
//! path is outbound `ssh` only: no listener, no relay, no token. The task text
//! is written to the child's **stdin** and read by `rpi -p "$(cat)"` on the far
//! side, which avoids both argv length limits and shell quoting/injection.
//!
//! This is the no-instance fallback. When an instance *is* running there, use
//! `hub_send`/`hub_ask` instead: the mailbox keeps its conversation context,
//! whereas this starts a fresh one-shot run with the remote machine's own
//! `~/.rpi` configuration.

use std::io::{Read, Write};
use std::sync::Arc;
use std::time::{Duration, Instant};

use rpi_hub_core::config::RemoteHost;
use rpi_hub_core::now_ms;

use crate::host::{Hub, ReplyOutcome};

/// Output caps: a runaway remote run must not exhaust this process.
const MAX_STDOUT_BYTES: u64 = 1024 * 1024;
const MAX_STDERR_BYTES: u64 = 32 * 1024;

/// Remote shell script. All values arrive as positional parameters, so nothing
/// is interpolated into the script text.
///
/// `$1` = cwd, `$2..` = the remote command (rpi plus extra args).
const SCRIPT: &str = r#"cd "$1" || exit 70; shift; exec "$@" -p "$(cat)""#;

pub struct Plan {
    pub program: String,
    pub args: Vec<String>,
    pub stdin: Vec<u8>,
}

/// Build the `ssh` invocation for `host`.
pub fn build_plan(host: &RemoteHost, task: &str) -> Result<Plan, String> {
    if host.ssh.trim().is_empty() {
        return Err("remoteHosts.<name>.ssh is required".into());
    }
    if !host
        .ssh
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-' | '@' | ':'))
    {
        return Err(format!("unsafe ssh destination {:?}", host.ssh));
    }
    let rpi = host.rpi.as_deref().unwrap_or("rpi");
    if !is_safe_path(rpi) {
        return Err(format!("unsafe rpi path {rpi:?}"));
    }
    let cwd = host.cwd.as_deref().unwrap_or("$HOME");
    if !is_safe_path(cwd) {
        return Err(format!("unsafe remote cwd {cwd:?}"));
    }
    for arg in &host.args {
        if arg.len() > 512 || arg.contains('\0') {
            return Err("remote rpi argument is invalid".into());
        }
    }

    let mut args: Vec<String> = vec![
        "-o".into(),
        "BatchMode=yes".into(),
        "-o".into(),
        "ConnectTimeout=10".into(),
        "-o".into(),
        "ServerAliveInterval=15".into(),
    ];
    if let Some(port) = host.port {
        args.push("-p".into());
        args.push(port.to_string());
    }
    args.push("--".into());
    args.push(host.ssh.clone());
    args.push("sh".into());
    args.push("-lc".into());
    args.push(SCRIPT.into());
    args.push("sh".into());
    args.push(cwd.to_string());
    args.push(rpi.to_string());
    args.extend(host.args.iter().cloned());

    Ok(Plan {
        program: "ssh".into(),
        args,
        stdin: task.as_bytes().to_vec(),
    })
}

fn is_safe_path(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 1024
        && value.chars().all(|c| {
            c.is_ascii_alphanumeric() || matches!(c, '/' | '.' | '_' | '-' | '~' | ' ' | '$' | ':' | '+' | '=' | ',' | '@')
        })
}

/// Run the plan, enforcing a hard timeout and output caps.
pub fn execute(plan: &Plan, timeout: Duration) -> Result<String, String> {
    let mut child = std::process::Command::new(&plan.program)
        .args(&plan.args)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .map_err(|error| format!("cannot start {}: {error}", plan.program))?;

    if let Some(mut stdin) = child.stdin.take() {
        let payload = plan.stdin.clone();
        // Writing can block if the remote side never reads; do it off-thread.
        std::thread::spawn(move || {
            let _ = stdin.write_all(&payload);
        });
    }

    let stdout_reader = child.stdout.take().map(|mut stdout| {
        std::thread::spawn(move || read_capped(&mut stdout, MAX_STDOUT_BYTES))
    });
    let stderr_reader = child.stderr.take().map(|mut stderr| {
        std::thread::spawn(move || read_capped(&mut stderr, MAX_STDERR_BYTES))
    });

    let deadline = Instant::now() + timeout;
    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) => {}
            Err(error) => {
                let _ = child.kill();
                return Err(format!("waiting for ssh failed: {error}"));
            }
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            return Err(format!(
                "remote run timed out after {}s",
                timeout.as_secs()
            ));
        }
        std::thread::sleep(Duration::from_millis(100));
    };

    let stdout = stdout_reader
        .and_then(|handle| handle.join().ok())
        .unwrap_or_default();
    let stderr = stderr_reader
        .and_then(|handle| handle.join().ok())
        .unwrap_or_default();

    if status.success() {
        let text = stdout.trim().to_string();
        if text.is_empty() {
            return Err("remote run produced no output".into());
        }
        Ok(text)
    } else {
        let detail = stderr.trim();
        Err(format!(
            "remote run failed ({}): {}",
            status.code().unwrap_or(-1),
            if detail.is_empty() {
                "no stderr"
            } else {
                detail
            }
        ))
    }
}

fn read_capped<R: Read>(reader: &mut R, limit: u64) -> String {
    let mut buffer = Vec::new();
    let _ = reader.take(limit).read_to_end(&mut buffer);
    String::from_utf8_lossy(&buffer).to_string()
}

/// Start a remote run off the caller's thread.
///
/// With a `correlation`, the result answers a waiting `hub_run` tool call;
/// without one it is surfaced as a transcript notice (`/run`).
pub fn spawn(
    hub: Arc<Hub>,
    host_name: String,
    host: RemoteHost,
    task: String,
    timeout: Duration,
    correlation: Option<String>,
) {
    std::thread::Builder::new()
        .name("rpi-hub-ssh".into())
        .spawn(move || {
            let outcome = match build_plan(&host, &task) {
                Ok(plan) => execute(&plan, timeout),
                Err(error) => Err(error),
            };
            let (ok, text) = match outcome {
                Ok(text) => (true, text),
                Err(error) => (false, error),
            };
            let delivered = correlation
                .as_deref()
                .map(|correlation| {
                    hub.answer_reply(
                        correlation,
                        ReplyOutcome {
                            ok,
                            text: text.clone(),
                            at: now_ms(),
                        },
                    )
                })
                .unwrap_or(false);
            if !delivered {
                let _ = hub.notice(&format!(
                    "{} {host_name}:\n{}",
                    if ok { "✓" } else { "✗" },
                    if text.is_empty() { "(空)" } else { &text }
                ));
            }
        })
        .ok();
}

#[cfg(test)]
mod tests {
    use super::*;

    fn host() -> RemoteHost {
        RemoteHost {
            ssh: "me@server".into(),
            cwd: Some("/work/project".into()),
            rpi: Some("/usr/local/bin/rpi".into()),
            port: Some(2222),
            args: vec!["--model".into(), "gateway/x".into()],
        }
    }

    #[test]
    fn plan_passes_values_as_argv_never_as_shell_text() {
        let plan = build_plan(&host(), "研究 $(rm -rf /) 和 `whoami`").unwrap();
        assert_eq!(plan.program, "ssh");
        assert!(plan.args.contains(&"-p".to_string()));
        assert!(plan.args.contains(&"2222".to_string()));
        assert!(plan.args.contains(&"me@server".to_string()));
        // The task travels on stdin only.
        assert!(plan.stdin.starts_with("研究".as_bytes()));
        let joined = plan.args.join(" ");
        assert!(!joined.contains("rm -rf"), "task must not reach argv: {joined}");
        assert!(!joined.contains("whoami"));
        // Positional layout: sh -lc <script> sh <cwd> <rpi> <args...>
        let script = plan.args.iter().position(|a| a == SCRIPT).unwrap();
        assert_eq!(plan.args[script + 1], "sh");
        assert_eq!(plan.args[script + 2], "/work/project");
        assert_eq!(plan.args[script + 3], "/usr/local/bin/rpi");
        assert_eq!(plan.args[script + 4], "--model");
    }

    #[test]
    fn plan_defaults_are_sane() {
        let plan = build_plan(
            &RemoteHost {
                ssh: "server".into(),
                ..Default::default()
            },
            "hi",
        )
        .unwrap();
        let script = plan.args.iter().position(|a| a == SCRIPT).unwrap();
        assert_eq!(plan.args[script + 2], "$HOME");
        assert_eq!(plan.args[script + 3], "rpi");
        assert!(!plan.args.contains(&"-p".to_string()) || plan.args.iter().any(|a| a == "BatchMode=yes"));
    }

    #[test]
    fn rejects_dangerous_destinations_and_paths() {
        let mut bad = host();
        bad.ssh = "server; rm -rf /".into();
        assert!(build_plan(&bad, "hi").is_err());

        let mut bad = host();
        bad.cwd = Some("/work/$(id)".into());
        assert!(build_plan(&bad, "hi").is_err());

        let mut bad = host();
        bad.rpi = Some("/usr/bin/rpi; id".into());
        assert!(build_plan(&bad, "hi").is_err());
    }

    #[test]
    fn execute_streams_stdin_and_captures_stdout() {
        // A local stand-in for `ssh ... rpi -p "$(cat)"`.
        let plan = Plan {
            program: "sh".into(),
            args: vec!["-c".into(), "cat".into()],
            stdin: b"task body".to_vec(),
        };
        let out = execute(&plan, Duration::from_secs(5)).unwrap();
        assert_eq!(out, "task body");
    }

    #[test]
    fn execute_reports_failure_and_timeout() {
        let failing = Plan {
            program: "sh".into(),
            args: vec!["-c".into(), "echo boom >&2; exit 3".into()],
            stdin: Vec::new(),
        };
        let error = execute(&failing, Duration::from_secs(5)).unwrap_err();
        assert!(error.contains("boom"), "{error}");

        let slow = Plan {
            program: "sh".into(),
            args: vec!["-c".into(), "sleep 5".into()],
            stdin: Vec::new(),
        };
        let error = execute(&slow, Duration::from_millis(300)).unwrap_err();
        assert!(error.contains("timed out"), "{error}");
    }
}
