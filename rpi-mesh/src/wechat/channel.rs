//! The WeChat plugin's own host state.
//!
//! Deliberately separate from rpi-mesh's `Channel`: this crate must work even when
//! the rpi-mesh plugin is absent (the lease store is the coordination point),
//! and it must not share mutable state with another plugin's statics. Both
//! plugins open the same SQLite database; the lease is the arbiter.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use rpi_mesh_core::now_ms;
use rpi_mesh_core::paths;
use rpi_mesh_core::store::Claim;
use rpi_plugin_sdk::{RuntimeActionFn, RuntimeActionId};

/// Alive window for presence rows (mirrors rpi-mesh).
pub const ALIVE_MS: i64 = 30_000;

static CURRENT: OnceLock<Mutex<Option<Arc<Channel>>>> = OnceLock::new();

fn cell() -> &'static Mutex<Option<Arc<Channel>>> {
    CURRENT.get_or_init(|| Mutex::new(None))
}

/// The mesh for the currently loaded plugin instance, if any.
pub fn current() -> Option<Arc<Channel>> {
    cell().lock().unwrap_or_else(|p| p.into_inner()).clone()
}

pub fn clear_current() {
    *cell().lock().unwrap_or_else(|p| p.into_inner()) = None;
}

/// Append a line to `logs/wechat.log` (5 MB rotation).
pub fn log(message: &str) {
    let line = format!("{} [{}] {message}\n", now_ms(), std::process::id());
    if std::env::var("RPI_MESH_DEBUG").ok().as_deref() == Some("1") {
        eprint!("rpi-wechat: {line}");
    }
    let Ok(dir) = paths::mesh_dir() else {
        return;
    };
    let dir = paths::log_dir(&dir);
    let _ = std::fs::create_dir_all(&dir);
    let path = dir.join("wechat.log");
    if let Ok(metadata) = std::fs::metadata(&path) {
        if metadata.len() > 5 * 1024 * 1024 {
            let _ = std::fs::rename(&path, dir.join("wechat.log.1"));
        }
    }
    use std::io::Write;
    if let Ok(mut file) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
    {
        let _ = file.write_all(line.as_bytes());
    }
}

/// Lease held by this instance (capability `wechat`).
pub struct HeldLease {
    pub generation: i64,
    pub expires_at: i64,
}


pub struct Channel {
    /// The process-wide mesh state (identity, store, config, runtime bridge).
    /// The old double-dylib design built a SECOND node_id/epoch/store here;
    /// in the unified plugin there is exactly one identity, so the channel
    /// borrows the mesh mesh instead of duplicating it.
    pub mesh: Arc<crate::mesh::Mesh>,
    /// The wechat lease held by this instance (generation bookkeeping; the
    /// claim / renew / release calls themselves go through `lease_owner`).
    /// Written by the OwnerLoop and the claim paths only.
    pub lease: Mutex<Option<HeldLease>>,
    /// The lease owner token for THIS process (single identity now).
    pub lease_owner: rpi_mesh_core::lease::LeaseOwner,
    /// The bound WeChat user (from credentials).
    pub user_id: Mutex<Option<String>>,
    /// Outbound reply channel (writer held by the bridge thread).
    outbound: Mutex<Option<std::sync::mpsc::Sender<String>>>,
    /// Whether the bridge thread is alive.
    running: AtomicBool,
    /// Set by the OwnerLoop while this instance holds the wechat lease; the
    /// bridge loop polls it and stops the moment it clears (single writer:
    /// the owner thread — the bridge never touches the lease itself).
    lease_alive: AtomicBool,
    /// Outbound path degraded (stale context token); the OwnerLoop composes
    /// the footer from this flag so the bridge stays a non-writer of status.
    degraded: AtomicBool,
    /// The bridge thread's last word before exiting (LeaseLost /
    /// SessionExpired / Shutdown). Consumed by the OwnerLoop: SessionExpired
    /// must release the lease instead of renewing it forever for a dead
    /// bridge (the "actively holding while dead" antipattern).
    bridge_stop: Mutex<Option<crate::wechat::bridge::StopReason>>,
    /// Resolved at bind time; media tools read credentials via these getters.
    credentials: Mutex<Option<(String, String)>>,
    /// Latest context_token from the last inbound message (media sendmessage
    /// requires it; -2 = stale → retried without token by the caller).
    context_token: Mutex<Option<String>>,
    /// The WeChat bridge may inject agent input / show notices.
    pub disabled: AtomicBool,
    /// Set when the bridge stopped with SessionExpired: the credentials no
    /// longer work, so the watcher must not auto-restart the bridge in a
    /// login-fail loop. Cleared by `/wechat login` / `/wechat start`.
    session_expired: AtomicBool,
    /// The OwnerLoop thread's handle (joined on shutdown).
    owner_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl std::fmt::Debug for Channel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Channel")
            .field("mesh", &self.mesh.dir())
            .field("node_id", &self.mesh.node_id)
            .finish()
    }
}

impl Channel {
    /// Wrap the process-wide mesh mesh. The channel adds only its own
    /// bookkeeping (lease slot, owner loop handle, credentials, context
    /// token, degraded/alive flags). Identity, store and config all come
    /// from the mesh — one node_id, one epoch, one presence row per process.
    pub fn new(mesh: Arc<crate::mesh::Mesh>) -> Arc<Self> {
        let lease_owner = rpi_mesh_core::lease::LeaseOwner::new(
            Arc::clone(&mesh.store) as Arc<dyn rpi_mesh_core::api::MeshStore>,
            mesh.node_id.clone(),
            mesh.name(),
            mesh.config.lease.ttl_ms(),
        );
        Arc::new(Self {
            mesh: Arc::clone(&mesh),
            lease_owner,
            lease: Mutex::new(None),
            user_id: Mutex::new(None),
            credentials: Mutex::new(None),
            context_token: Mutex::new(None),
            bridge_stop: Mutex::new(None),
            lease_alive: AtomicBool::new(false),
            degraded: AtomicBool::new(false),
            outbound: Mutex::new(None),
            running: AtomicBool::new(false),
            disabled: AtomicBool::new(false),
            session_expired: AtomicBool::new(false),
            owner_thread: Mutex::new(None),
        })
    }

    // ---- delegated mesh accessors (single identity) ----
    pub fn label(&self) -> String {
        self.mesh.label()
    }
    pub fn name(&self) -> String {
        self.mesh.name()
    }
    pub fn set_name(&self, name: impl Into<String>) {
        self.mesh.set_name(name);
    }
    pub fn dir(&self) -> &std::path::Path {
        &self.mesh.dir()
    }
    pub fn node_id(&self) -> &str {
        &self.mesh.node_id
    }
    pub fn epoch(&self) -> &str {
        &self.mesh.epoch
    }
    pub fn config(&self) -> &rpi_mesh_core::config::MeshConfig {
        &self.mesh.config
    }
    pub fn store(&self) -> Arc<dyn rpi_mesh_core::api::MeshStore> {
        Arc::clone(&self.mesh.store) as Arc<dyn rpi_mesh_core::api::MeshStore>
    }
    pub fn busy(&self) -> bool {
        self.mesh.busy()
    }
    pub fn shutdown(&self) -> &crate::mesh::Shutdown {
        self.mesh.shutdown()
    }
    pub fn request_shutdown(&self) {
        self.mesh.request_shutdown();
    }
    pub fn cwd(&self) -> PathBuf {
        self.mesh
            .session_snapshot()
            .cwd
            .map(PathBuf::from)
            .unwrap_or_else(|| std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")))
    }


    pub fn bridge_ptr(&self) -> *mut std::ffi::c_void {
        self.mesh.bridge_ptr()
    }

    /// Runtime-action wiring is owned by the mesh install (single bridge);
    /// kept for call-site compatibility.
    pub fn set_bridge(
        &self,
        _ptr: *mut std::ffi::c_void,
        _action_fn: RuntimeActionFn,
        _host_free: rpi_plugin_sdk::FreeStringFn,
    ) {
    }

    /// Claim the wechat lease AND record it in `self.lease` — the bridge
    /// gate checks the recorded generation, so a claim without this bookkeeping
    /// is fenced out on its first tick (the LeaseLost-on-startup bug).
    pub fn claim_wechat(&self) -> Result<Claim, String> {
        let now = now_ms();
        // The facade (rpi-mesh-core::lease::LeaseOwner) carries claim + the
        // dead-owner takeover branch; this method only records the resulting
        // generation for the bridge's fence gate.
        let claim = self.lease_owner.claim("wechat", now)?;
        match &claim {
            Claim::Granted {
                generation,
                expires_at,
            } => {
                *self.lease.lock().unwrap_or_else(|p| p.into_inner()) = Some(HeldLease {
                    generation: *generation,
                    expires_at: *expires_at,
                });
            }
            Claim::Held { .. } => {}
        }
        Ok(claim)
    }

    /// Send text into this instance's agent lane (runtime action).
    pub fn send_user_message(&self, text: &str, deliver_as: Option<&str>) -> Result<(), String> {
        let mut args = serde_json::json!({ "text": text, "triggerTurn": true });
        if let Some(deliver_as) = deliver_as {
            args["deliverAs"] = serde_json::json!(deliver_as);
        }
        self.action(RuntimeActionId::SendUserMessage, args)
            .map(|_| ())
    }

    /// Send a content-blocks user message (text + inline images) into the
    /// agent lane — the WeChat media path. `images` are base64 JPEG/PNG
    /// payloads; the host deserializes them into image content blocks the
    /// model can see.
    pub fn send_user_content(
        &self,
        text: &str,
        images: &[crate::wechat::bridge::ImageData],
        deliver_as: Option<&str>,
    ) -> Result<(), String> {
        let mut content = vec![serde_json::json!({
            "type": "text",
            "text": text,
        })];
        for image in images {
            content.push(serde_json::json!({
                "type": "image",
                "data": image.data,
                "mimeType": image.media_type,
            }));
        }
        let mut args = serde_json::json!({
            "message": {
                "kind": "user",
                "role": "user",
                "content": content,
                "timestamp": now_ms(),
            },
            "triggerTurn": true,
        });
        if let Some(deliver_as) = deliver_as {
            args["deliverAs"] = serde_json::json!(deliver_as);
        }
        self.action(RuntimeActionId::SendMessage, args).map(|_| ())
    }

    /// Show text in the transcript without running the model.
    pub fn notice(&self, text: &str) -> Result<(), String> {
        self.action(
            RuntimeActionId::SendMessage,
            serde_json::json!({
                "message": { "customType": "rpi-wechat", "content": text, "display": true },
                "options": { "triggerTurn": false }
            }),
        )
        .map(|_| ())
    }

    pub fn set_status(&self, value: &str) {
        let _ = self.action(
            RuntimeActionId::SetStatus,
            serde_json::json!({ "key": "wechat", "value": value }),
        );
    }

    /// Call a host runtime action through the mesh's single bridge.
    pub fn action(
        &self,
        action: RuntimeActionId,
        args: serde_json::Value,
    ) -> Result<serde_json::Value, String> {
        self.mesh.action(action, args)
    }

    /// Refresh the shared context token (called on every authorized inbound).
    pub fn set_context_token(&self, token: Option<&str>) {
        *self.context_token.lock().unwrap_or_else(|p| p.into_inner()) = token.map(str::to_string);
    }

    pub fn context_token(&self) -> Option<String> {
        self.context_token
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }

    /// Lease liveness flag (written by the OwnerLoop only).
    pub fn lease_alive(&self) -> bool {
        self.lease_alive.load(Ordering::Acquire)
    }

    pub fn set_lease_alive(&self, alive: bool) {
        self.lease_alive.store(alive, Ordering::Release);
    }

    /// Outbound degraded flag (written by the bridge only).
    pub fn degraded(&self) -> bool {
        self.degraded.load(Ordering::Acquire)
    }

    pub fn set_degraded(&self, degraded: bool) {
        self.degraded.store(degraded, Ordering::Release);
    }

    /// Record why the bridge thread is about to exit (written once by the
    /// bridge thread; consumed by the OwnerLoop).
    pub fn set_bridge_stop(&self, reason: crate::wechat::bridge::StopReason) {
        *self.bridge_stop.lock().unwrap_or_else(|p| p.into_inner()) = Some(reason);
    }

    pub fn clear_bridge_stop(&self) {
        *self.bridge_stop.lock().unwrap_or_else(|p| p.into_inner()) = None;
    }

    pub fn take_bridge_stop(&self) -> Option<crate::wechat::bridge::StopReason> {
        self.bridge_stop
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .take()
    }

    /// Session-expiry latch (see the field docs).
    pub fn set_session_expired(&self, expired: bool) {
        self.session_expired.store(expired, Ordering::Release);
    }

    pub fn session_expired(&self) -> bool {
        self.session_expired.load(Ordering::Acquire)
    }

    /// Register the OwnerLoop's handle so shutdown can join it.
    pub fn set_owner_thread(&self, handle: std::thread::JoinHandle<()>) {
        *self.owner_thread.lock().unwrap_or_else(|p| p.into_inner()) = Some(handle);
    }

    /// Stop and join the OwnerLoop (it observes the shutdown flag within one
    /// 200 ms wait, so this returns quickly).
    pub fn stop_owner(&self) {
        self.request_shutdown();
        let handle = self
            .owner_thread
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .take();
        if let Some(handle) = handle {
            let _ = handle.join();
        }
    }

    /// Cache credentials for the outbound media tools.
    pub fn set_credentials(&self, base_url: &str, token: &str) {
        *self.credentials.lock().unwrap_or_else(|p| p.into_inner()) =
            Some((base_url.to_string(), token.to_string()));
    }

    pub fn credentials_token(&self) -> String {
        self.credentials
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .as_ref()
            .map(|(_, token)| token.clone())
            .unwrap_or_default()
    }

    pub fn credentials_base_url(&self) -> String {
        self.credentials
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .as_ref()
            .map(|(base, _)| base.clone())
            .unwrap_or_else(|| crate::wechat::ilink::DEFAULT_BASE_URL.to_string())
    }

    /// Hand the reply channel to the bridge thread.
    pub fn set_outbound(&self, sender: std::sync::mpsc::Sender<String>) {
        *self.outbound.lock().unwrap_or_else(|p| p.into_inner()) = Some(sender);
        self.running.store(true, Ordering::Release);
    }

    /// Whether the bridge thread is alive.
    pub fn bridge_running(&self) -> bool {
        self.running.load(Ordering::Acquire)
    }

    /// Queue an assistant reply; the bridge thread drains and chunks it.
    pub fn queue_reply(&self, text: &str) {
        let sender = self
            .outbound
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone();
        let Some(sender) = sender else {
            return;
        };
        for chunk in crate::wechat::bridge::chunk_reply(text) {
            if sender.send(chunk).is_err() {
                break;
            }
        }
    }

    /// Mark the bridge stopped (called when the thread exits). Footer is
    /// deliberately NOT written here: the wechat status slot has exactly two
    /// writers — the OwnerLoop (while holding) and the watcher (otherwise).
    pub fn mark_stopped(&self) {
        self.running.store(false, Ordering::Release);
        *self.outbound.lock().unwrap_or_else(|p| p.into_inner()) = None;
    }

    /// Stop the bridge thread (bounded by the lifecycle finish budget).
    pub fn stop_thread(&self) {
        self.request_shutdown();
        // The thread observes the flag within one poll gap; the host's
        // `finish_shutdown` budget (15 s) covers the worst case.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(14);
        while self.bridge_running() && std::time::Instant::now() < deadline {
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
        self.mark_stopped();
    }

    pub fn record_error(&self, message: impl Into<String>) {
        let message = message.into();
        log(&format!("error: {message}"));
    }
}


/// Best-effort hostname (mirrors rpi-mesh's detection).
pub fn detect_host() -> String {
    for key in ["RPI_MESH_HOST", "HOSTNAME", "HOST", "COMPUTERNAME"] {
        if let Ok(value) = std::env::var(key) {
            let value = value.trim();
            if !value.is_empty() {
                return value.to_string();
            }
        }
    }
    if let Ok(value) = std::fs::read_to_string("/etc/hostname") {
        let value = value.trim();
        if !value.is_empty() {
            return value.to_string();
        }
    }
    if let Ok(output) = std::process::Command::new("hostname").output() {
        if output.status.success() {
            let value = String::from_utf8_lossy(&output.stdout).trim().to_string();
            if !value.is_empty() {
                return value;
            }
        }
    }
    "localhost".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detect_host_returns_something() {
        assert!(!detect_host().is_empty());
    }
}
