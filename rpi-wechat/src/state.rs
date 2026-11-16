//! Persisted WeChat credentials + bridge state (`wechat/` under the hub dir).
//!
//! Files are 0600; the directory is 0700. Nothing sensitive is ever logged.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use rpi_hub_core::paths;
use rpi_hub_core::Result;

/// Wechat state directory: `<hub dir>/wechat`.
pub fn wechat_dir(dir: &Path) -> PathBuf {
    paths::wechat_dir(dir)
}

/// `wechat/credentials.json` — token + bound user from the QR login.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Credentials {
    pub token: String,
    #[serde(default = "default_base_url")]
    pub base_url: String,
    pub user_id: String,
    #[serde(default)]
    pub saved_at: i64,
}

fn default_base_url() -> String {
    crate::ilink::DEFAULT_BASE_URL.to_string()
}

impl Credentials {
    pub fn load(dir: &Path) -> Option<Self> {
        let path = wechat_dir(dir).join("credentials.json");
        let text = std::fs::read_to_string(path).ok()?;
        serde_json::from_str(&text).ok()
    }

    pub fn save(&self, dir: &Path) -> Result<()> {
        let dir = wechat_dir(dir);
        paths::ensure_private_dir(&dir)?;
        let path = dir.join("credentials.json");
        let tmp = path.with_extension("json.tmp");
        let payload = serde_json::to_string_pretty(self)?;
        std::fs::write(&tmp, format!("{payload}\n"))?;
        std::fs::rename(&tmp, &path)?;
        paths::restrict_permissions(&path, 0o600)?;
        Ok(())
    }

    pub fn remove(dir: &Path) -> bool {
        std::fs::remove_file(wechat_dir(dir).join("credentials.json")).is_ok()
    }
}

/// `wechat/disabled.json` — the user explicitly stopped the bridge; heartbeat
/// must not auto-reclaim the lease past this flag (mirrors the reference
/// plugin's fix for the auto-reclaim trap).
pub fn is_disabled(dir: &Path) -> bool {
    wechat_dir(dir).join("disabled.json").exists()
}

pub fn set_disabled(dir: &Path, disabled: bool) -> Result<()> {
    let dir = wechat_dir(dir);
    paths::ensure_private_dir(&dir)?;
    let path = dir.join("disabled.json");
    if disabled {
        std::fs::write(&path, "{}\n")?;
        paths::restrict_permissions(&path, 0o600)?;
    } else {
        let _ = std::fs::remove_file(&path);
    }
    Ok(())
}

/// `wechat/cursor.txt` — the getupdates cursor (write-failures are not fatal:
/// dedupe by message id is the second line of defense).
pub fn load_cursor(dir: &Path) -> Option<String> {
    std::fs::read_to_string(wechat_dir(dir).join("cursor.txt"))
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

pub fn save_cursor(dir: &Path, cursor: &str) {
    let dir = wechat_dir(dir);
    let _ = std::fs::create_dir_all(&dir);
    let _ = std::fs::write(dir.join("cursor.txt"), cursor);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir() -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "rpi-wechat-state-{}-{}",
            std::process::id(),
            rpi_hub_core::now_ms()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn credentials_round_trip_and_remove() {
        let dir = temp_dir();
        let credentials = Credentials {
            token: "tok".into(),
            base_url: crate::ilink::DEFAULT_BASE_URL.into(),
            user_id: "u1".into(),
            saved_at: 1,
        };
        credentials.save(&dir).unwrap();
        let loaded = Credentials::load(&dir).unwrap();
        assert_eq!(loaded.token, "tok");
        assert_eq!(loaded.user_id, "u1");
        assert!(Credentials::remove(&dir));
        assert!(Credentials::load(&dir).is_none());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn disabled_flag_toggles() {
        let dir = temp_dir();
        assert!(!is_disabled(&dir));
        set_disabled(&dir, true).unwrap();
        assert!(is_disabled(&dir));
        set_disabled(&dir, false).unwrap();
        assert!(!is_disabled(&dir));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn cursor_round_trip() {
        let dir = temp_dir();
        assert!(load_cursor(&dir).is_none());
        save_cursor(&dir, "cursor-1");
        assert_eq!(load_cursor(&dir).as_deref(), Some("cursor-1"));
        let _ = std::fs::remove_dir_all(&dir);
    }
}
