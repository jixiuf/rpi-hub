//! State paths. Everything lives under `~/.rpi/agent/pi-hub` unless
//! `RPI_HUB_DIR` overrides it (tests, throwaway clusters).

use std::path::{Path, PathBuf};

use crate::{HubError, Result};

pub const HUB_DIR_ENV: &str = "RPI_HUB_DIR";
pub const INSTANCE_ENV: &str = "RPI_HUB_INSTANCE";

pub const DB_FILE: &str = "hub.db";
pub const CONFIG_FILE: &str = "config.json";
pub const WECHAT_DIR: &str = "wechat";
pub const LOG_DIR: &str = "logs";
pub const RELAY_SOCKET: &str = "relay.sock";
pub const TUNNEL_SOCKET: &str = "tunnel.sock";

/// `$RPI_HUB_DIR`, else `~/.rpi/agent/pi-hub`.
pub fn hub_dir() -> Result<PathBuf> {
    if let Some(dir) = env_override(HUB_DIR_ENV) {
        return Ok(PathBuf::from(dir));
    }
    let home = home_dir()?;
    Ok(home.join(".rpi").join("agent").join("pi-hub"))
}

/// The instance-name override, if any (`RPI_HUB_INSTANCE`).
pub fn instance_override() -> Option<String> {
    env_override(INSTANCE_ENV)
}

pub fn db_path(dir: &Path) -> PathBuf {
    dir.join(DB_FILE)
}

pub fn config_path(dir: &Path) -> PathBuf {
    dir.join(CONFIG_FILE)
}

pub fn wechat_dir(dir: &Path) -> PathBuf {
    dir.join(WECHAT_DIR)
}

pub fn log_dir(dir: &Path) -> PathBuf {
    dir.join(LOG_DIR)
}

pub fn relay_socket(dir: &Path) -> PathBuf {
    dir.join(RELAY_SOCKET)
}

pub fn tunnel_socket(dir: &Path) -> PathBuf {
    dir.join(TUNNEL_SOCKET)
}

/// Create a directory (and parents) readable only by the current user.
pub fn ensure_private_dir(path: &Path) -> Result<()> {
    std::fs::create_dir_all(path)?;
    restrict_permissions(path, 0o700)?;
    Ok(())
}

/// Restrict a file or directory to the current user (no-op on Windows).
///
/// Best effort: on some filesystems (NFS homes, odd mounts) chmod may be
/// rejected. That must not stop the hub — log it through the wire hook and
/// continue, since the token file's secrecy then depends on the mount.
pub fn restrict_permissions(path: &Path, mode: u32) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let attempt = || -> std::io::Result<()> {
            let mut perms = std::fs::metadata(path)?.permissions();
            perms.set_mode(mode);
            std::fs::set_permissions(path, perms)
        };
        if let Err(error) = attempt() {
            crate::wire::log(&format!(
                "could not restrict permissions on {} to {mode:o}: {error}",
                path.display()
            ));
        }
    }
    #[cfg(not(unix))]
    {
        let _ = (path, mode);
    }
    Ok(())
}

fn env_override(key: &str) -> Option<String> {
    match std::env::var(key) {
        Ok(value) if !value.trim().is_empty() => Some(value),
        _ => None,
    }
}

fn home_dir() -> Result<PathBuf> {
    if let Some(home) = env_override("HOME").or_else(|| env_override("USERPROFILE")) {
        return Ok(PathBuf::from(home));
    }
    Err(HubError::invalid(
        "cannot resolve home directory (set HOME or RPI_HUB_DIR)",
    ))
}
