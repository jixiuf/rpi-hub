//! Shared core for the rpi-hub plugins.
//!
//! The hub is deliberately **not** a daemon and not a socket protocol in the
//! local case: every rpi process opens the same SQLite database (WAL mode), so
//! presence, mailbox and the channel lease are just rows with SQLite's own
//! multi-process safety. Cross-machine nodes go through a relay that owns the
//! same database, reached over a plugin-managed SSH stream-local tunnel.
//!
//! Layering:
//! - [`config`]  — `~/.rpi/agent/pi-hub/config.json` (env overrides)
//! - [`paths`]   — where state lives, with permissions
//! - [`names`]   — instance identity/naming rules
//! - [`protocol`]— envelopes and command intents (the wire format)
//! - [`store`]   — the SQLite store: instances, leases, mailbox

pub mod api;
pub mod config;
pub mod error;
pub mod lease;
pub mod names;
pub mod render;
pub mod paths;
pub mod protocol;
pub mod status;
pub mod store;
pub mod wire;

pub use api::HubStore;
pub use error::{HubError, Result};

/// Milliseconds since the Unix epoch, saturating at `i64::MAX`.
pub fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis().min(i64::MAX as u128) as i64)
        .unwrap_or(0)
}
