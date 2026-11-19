//! `~/.rpi/agent/pi-hub/config.json`.
//!
//! Precedence: `RPI_HUB_DIR` / `RPI_HUB_INSTANCE` env, then the file, then the
//! built-in defaults. A corrupt file is kept (renamed to `config.json.broken`)
//! and replaced with defaults: a broken config must never stop the hub.

use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::{names, paths, HubError, Result};

pub const CONFIG_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase", default)]
pub struct HubConfig {
    /// Schema marker so a future change can migrate instead of guessing.
    pub version: u32,
    /// Display name for this instance; defaults to the project directory name.
    pub instance_name: Option<String>,
    pub lease: LeaseConfig,
    pub remote: RemoteConfig,
    pub channels: ChannelsConfig,
    pub limits: LimitsConfig,
    /// How `/start` launches new instances on this machine.
    #[serde(default)]
    pub instance_spawn: InstanceSpawnConfig,
}

impl Default for HubConfig {
    fn default() -> Self {
        Self {
            version: CONFIG_VERSION,
            instance_name: None,
            lease: LeaseConfig::default(),
            remote: RemoteConfig::default(),
            channels: ChannelsConfig::default(),
            limits: LimitsConfig::default(),
            instance_spawn: InstanceSpawnConfig::default(),
        }
    }
}

/// Floor for the lease TTL: it must outlast the wechat bridge's longest
/// blocking call. `getupdates` is a 40 s server-side long poll (45 s client
/// timeout); a TTL at or below that would let the lease expire mid-poll even
/// with a healthy owner — the root cause of the takeover-oscillation bug.
pub const MIN_TTL_SECONDS: u64 = 45;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase", default)]
pub struct LeaseConfig {
    /// How long a lease survives without a renewal. A paused or half-dead owner
    /// must not keep the WeChat bridge for longer than this.
    pub ttl_seconds: u64,
    /// Renewal period, enforced by the wechat OwnerLoop (the lease's single
    /// writer). Must be comfortably below `ttl_seconds`.
    pub renew_seconds: u64,
}

impl Default for LeaseConfig {
    fn default() -> Self {
        Self {
            ttl_seconds: 60,
            renew_seconds: 5,
        }
    }
}

impl LeaseConfig {
    pub fn ttl_ms(&self) -> i64 {
        (self.ttl_seconds.max(2) as i64) * 1000
    }
    pub fn renew_ms(&self) -> i64 {
        (self.renew_seconds.clamp(1, self.ttl_seconds.max(2)) as i64) * 1000
    }

    /// Timing warnings (never fatal): renew must fit well inside the TTL, and
    /// the TTL must outlast the wechat bridge's longest poll.
    pub fn validate(&self) -> Vec<String> {
        let mut warnings = Vec::new();
        if self.ttl_seconds < MIN_TTL_SECONDS {
            warnings.push(format!(
                "lease.ttlSeconds={} is below the {}s floor: the wechat bridge polls getupdates for up to 40s and the lease would expire mid-poll, inviting takeover oscillation",
                self.ttl_seconds, MIN_TTL_SECONDS
            ));
        }
        if self.renew_seconds > self.ttl_seconds / 4 {
            warnings.push(format!(
                "lease.renewSeconds={} is more than a quarter of ttlSeconds={}; renewals may not land before expiry",
                self.renew_seconds, self.ttl_seconds
            ));
        }
        warnings
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum RemoteMode {
    /// This node owns the database; no cross-machine traffic.
    #[default]
    Local,
    /// This node owns the database *and* serves remote nodes on `relay`.
    Relay,
    /// This node's hub state lives on a relay reached through `relay`.
    Client,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "camelCase", default)]
pub struct RemoteConfig {
    pub mode: RemoteMode,
    pub relay: RelayConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "camelCase", default)]
pub struct RelayConfig {
    /// Relay side: `host:port` to serve on, or `unix:<path>` for a local socket.
    /// Default `127.0.0.1:9527`; a non-loopback bind is warned about at startup
    /// because the token is then the only defense. When the port must traverse
    /// a firewall that only forwards to a real HTTP service, set
    /// `smuggle: true` and speak HTTP at the front door (see below).
    pub listen: Option<String>,
    /// Client side: `host:port` of the relay, or `unix:<path>`.
    pub connect: Option<String>,
    /// Path to the shared token file (0600, one 32-hex-char line). The relay
    /// generates it when missing; a client without it refuses to start.
    pub token_file: Option<String>,
    /// Speak HTTP at the front door so the relay can share a port that a
    /// firewall already forwards to a real HTTP service. On: the relay answers
    /// `POST /hub-rpc` with `Content-Type: application/json` bodies (one JSON
    /// request/response per call, X-Hub-Auth: <token> header), so a proxy or
    /// co-located server routes us by path. Off: raw line-delimited JSON-RPC
    /// (leaner, for a dedicated port).
    #[serde(default)]
    pub http: bool,
}

impl RelayConfig {
    pub const DEFAULT_TCP_PORT: u16 = 9527;
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "camelCase", default)]
pub struct ChannelsConfig {
    pub wechat: ChannelConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "camelCase", default)]
pub struct ChannelConfig {
    pub enabled: bool,
}

/// `/start` launch mode. Only `tmux` is implemented; `none` disables the
/// command. Unknown values fall back to tmux with a startup warning.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum SpawnMode {
    /// `tmux new-session -d` (the default) — crash-visible, attachable.
    #[default]
    Tmux,
    /// A new WINDOW inside the CURRENT tmux session (requires the host rpi
    /// process to run inside tmux; falls back to a new session when $TMUX is
    /// absent).
    CurrentSession,
    /// `/start` disabled on this machine.
    None,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InstanceSpawnConfig {
    pub mode: SpawnMode,
    /// tmux session-name prefix (`rpi-<base>` → `<prefix>-<base>`).
    #[serde(default = "default_session_prefix")]
    pub session_prefix: String,
    /// The command a new instance runs. A string is a single program; an
    /// array is program + arguments (e.g. `["rpi", "--model", "sonnet"]`).
    #[serde(default = "default_spawn_command")]
    pub command: SpawnCommand,
    /// The tmux binary path (self-compiled or non-PATH installs).
    #[serde(default = "default_tmux_command")]
    pub tmux_command: String,
}

fn default_session_prefix() -> String {
    "rpi".to_string()
}

fn default_spawn_command() -> SpawnCommand {
    SpawnCommand::Simple("rpi".to_string())
}

fn default_tmux_command() -> String {
    "tmux".to_string()
}

/// The new instance's command line: a bare program, or program + arguments.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum SpawnCommand {
    Simple(String),
    Args(Vec<String>),
}

impl SpawnCommand {
    /// The argv for the new instance (empty program is an error upstream).
    pub fn argv(&self) -> Vec<String> {
        match self {
            SpawnCommand::Simple(program) => vec![program.clone()],
            SpawnCommand::Args(argv) => argv.clone(),
        }
    }
}

impl Default for InstanceSpawnConfig {
    fn default() -> Self {
        Self {
            mode: SpawnMode::default(),
            session_prefix: default_session_prefix(),
            command: default_spawn_command(),
            tmux_command: default_tmux_command(),
        }
    }
}

impl InstanceSpawnConfig {
    /// The launch label used in warnings (`tmux`/`none`).
    pub fn mode_label(&self) -> &'static str {
        match self.mode {
            SpawnMode::Tmux => "tmux",
            SpawnMode::CurrentSession => "current",
            SpawnMode::None => "none",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct LimitsConfig {
    pub max_text_bytes: usize,
    pub inbox_batch: i64,
}

impl Default for LimitsConfig {
    fn default() -> Self {
        Self {
            max_text_bytes: 64 * 1024,
            inbox_batch: 32,
        }
    }
}

impl HubConfig {
    /// Effective display name: `RPI_HUB_INSTANCE`, then the file, then cwd.
    pub fn resolved_name(&self, cwd: &Path) -> String {
        if let Some(name) = paths::instance_override() {
            return names::sanitize(&name);
        }
        match &self.instance_name {
            Some(name) if !name.trim().is_empty() => names::sanitize(name),
            _ => names::default_name(cwd),
        }
    }
}

/// Load the config, creating a default file on first use.
///
/// Warnings are returned rather than logged: the caller owns logging.
pub fn load(dir: &Path) -> Result<(HubConfig, Vec<String>)> {
    let mut warnings = Vec::new();
    let path = paths::config_path(dir);
    let text = match std::fs::read_to_string(&path) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let config = HubConfig::default();
            write(dir, &config)?;
            return Ok((config, warnings));
        }
        Err(error) => return Err(HubError::Io(error)),
    };
    match serde_json::from_str::<HubConfig>(&text) {
        Ok(config) => {
            warnings.extend(config.lease.validate());
            Ok((config, warnings))
        }
        Err(error) => {
            warnings.push(format!(
                "{}: invalid config ({error}); using defaults (old file kept as config.json.broken)",
                path.display()
            ));
            let backup = path.with_extension("json.broken");
            let _ = std::fs::rename(&path, &backup);
            let config = HubConfig::default();
            write(dir, &config)?;
            Ok((config, warnings))
        }
    }
}

pub fn write(dir: &Path, config: &HubConfig) -> Result<()> {
    paths::ensure_private_dir(dir)?;
    let path = paths::config_path(dir);
    // Unique temp name: two instances starting at the same moment must not
    // rename each other's temp file away (that surfaces as ENOENT).
    let tmp = path.with_extension(format!("json.tmp.{}", std::process::id()));
    let text = serde_json::to_string_pretty(config)?;
    std::fs::write(&tmp, format!("{text}\n"))?;
    match std::fs::rename(&tmp, &path) {
        Ok(()) => {}
        // Another process created the file first: that is a success, not an error.
        Err(error) if error.kind() == std::io::ErrorKind::NotFound && path.exists() => {}
        Err(error) => {
            let _ = std::fs::remove_file(&tmp);
            return Err(HubError::Io(error));
        }
    }
    paths::restrict_permissions(&path, 0o600)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn temp_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "rpi-hub-config-{name}-{}-{}",
            std::process::id(),
            crate::now_ms()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn load_creates_defaults_then_round_trips() {
        let dir = temp_dir("defaults");
        let (config, warnings) = load(&dir).unwrap();
        assert!(warnings.is_empty());
        assert_eq!(config, HubConfig::default());
        assert!(paths::config_path(&dir).exists());

        let mut changed = config.clone();
        changed.instance_name = Some("home".into());
        changed.remote.mode = RemoteMode::Client;
        write(&dir, &changed).unwrap();
        let (reloaded, warnings) = load(&dir).unwrap();
        assert!(warnings.is_empty());
        assert_eq!(reloaded, changed);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn invalid_config_is_backed_up_and_defaults_are_restored() {
        let dir = temp_dir("broken");
        std::fs::write(paths::config_path(&dir), "{ not json").unwrap();
        let (config, warnings) = load(&dir).unwrap();
        assert_eq!(config, HubConfig::default());
        assert_eq!(warnings.len(), 1);
        assert!(dir.join("config.json.broken").exists());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn lease_timing_defaults_are_sane() {
        let lease = LeaseConfig::default();
        assert!(lease.renew_ms() < lease.ttl_ms());
        // The default TTL must outlast the wechat bridge's 40s long poll.
        assert_eq!(lease.ttl_ms(), 60_000);
        assert!(lease.validate().is_empty());
    }

    #[test]
    fn lease_validation_flags_bad_timing() {
        let short = LeaseConfig {
            ttl_seconds: 20,
            renew_seconds: 5,
        };
        let warnings = short.validate();
        assert_eq!(warnings.len(), 1, "{warnings:?}");
        assert!(warnings[0].contains("45s floor"), "{warnings:?}");

        let sparse = LeaseConfig {
            ttl_seconds: 60,
            renew_seconds: 30,
        };
        let warnings = sparse.validate();
        assert_eq!(warnings.len(), 1, "{warnings:?}");
        assert!(warnings[0].contains("quarter"), "{warnings:?}");
    }

    #[test]
    fn legacy_config_keys_are_ignored() {
        let dir = temp_dir("legacy-keys");
        // A config written by an older build carries autoClaim and
        // resumeGraceSeconds; the current loader must accept it unchanged.
        std::fs::write(
            paths::config_path(&dir),
            r#"{
              "version": 1,
              "lease": {"ttlSeconds": 90, "renewSeconds": 5, "resumeGraceSeconds": 10},
              "channels": {"wechat": {"enabled": true, "autoClaim": false}}
            }"#,
        )
        .unwrap();
        let (config, warnings) = load(&dir).unwrap();
        assert!(warnings.is_empty(), "{warnings:?}");
        assert_eq!(config.lease.ttl_seconds, 90);
        assert!(config.channels.wechat.enabled);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
