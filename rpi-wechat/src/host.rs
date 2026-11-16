//! The WeChat plugin's own host state.
//!
//! Deliberately separate from rpi-hub's `Hub`: this crate must work even when
//! the rpi-hub plugin is absent (the lease store is the coordination point),
//! and it must not share mutable state with another plugin's statics. Both
//! plugins open the same SQLite database; the lease is the arbiter.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use rpi_hub_core::config::{self, HubConfig};
use rpi_hub_core::names;
use rpi_hub_core::paths;
use rpi_hub_core::now_ms;
use rpi_hub_core::store::{Claim, InstanceRecord, Store};
use rpi_plugin_sdk::callbacks::OwnedString;
use rpi_plugin_sdk::{RuntimeActionFn, RuntimeActionId, StbString, StbStringRef};

/// Alive window for presence rows (mirrors rpi-hub).
pub const ALIVE_MS: i64 = 30_000;

static CURRENT: OnceLock<Mutex<Option<Arc<Hub>>>> = OnceLock::new();

fn cell() -> &'static Mutex<Option<Arc<Hub>>> {
    CURRENT.get_or_init(|| Mutex::new(None))
}

/// The hub for the currently loaded plugin instance, if any.
pub fn current() -> Option<Arc<Hub>> {
    cell().lock().unwrap_or_else(|p| p.into_inner()).clone()
}

pub fn clear_current() {
    *cell().lock().unwrap_or_else(|p| p.into_inner()) = None;
}

/// Append a line to `logs/wechat.log` (5 MB rotation).
pub fn log(message: &str) {
    let line = format!("{} [{}] {message}\n", now_ms(), std::process::id());
    if std::env::var("RPI_HUB_DEBUG").ok().as_deref() == Some("1") {
        eprint!("rpi-wechat: {line}");
    }
    let Ok(dir) = paths::hub_dir() else {
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
    if let Ok(mut file) = std::fs::OpenOptions::new().create(true).append(true).open(&path) {
        let _ = file.write_all(line.as_bytes());
    }
}

/// Lease held by this instance (capability `wechat`).
pub struct HeldLease {
    pub generation: i64,
    pub expires_at: i64,
}

pub struct Shutdown {
    flag: AtomicBool,
    lock: Mutex<()>,
    signal: std::sync::Condvar,
}

impl Shutdown {
    fn new() -> Self {
        Self {
            flag: AtomicBool::new(false),
            lock: Mutex::new(()),
            signal: std::sync::Condvar::new(),
        }
    }
    pub fn request(&self) {
        self.flag.store(true, Ordering::SeqCst);
        self.signal.notify_all();
    }
    pub fn requested(&self) -> bool {
        self.flag.load(Ordering::SeqCst)
    }
    /// Sleep up to `timeout`, waking early on shutdown.
    pub fn wait(&self, timeout: std::time::Duration) {
        let guard = self.lock.lock().unwrap_or_else(|p| p.into_inner());
        let _ = self
            .signal
            .wait_timeout_while(guard, timeout, |_| !self.requested());
    }
}

pub struct Hub {
    pub dir: PathBuf,
    pub config: HubConfig,
    pub store: Arc<Store>,
    pub host: String,
    pub node_id: String,
    pub epoch: String,
    pub name: Mutex<String>,
    pub busy: AtomicBool,
    bridge: std::sync::atomic::AtomicPtr<std::ffi::c_void>,
    /// Runtime-action plumbing lives behind atomics so `&Hub` (shared across
    /// threads) can install the host wiring once, at registration time.
    action_fn: std::sync::Mutex<RuntimeActionFn>,
    host_free: std::sync::Mutex<rpi_plugin_sdk::FreeStringFn>,
    pub shutdown: Shutdown,
    /// Wechat lease held by this instance.
    pub lease: Mutex<Option<HeldLease>>,
    /// The bound WeChat user (from credentials).
    pub user_id: Mutex<Option<String>>,
    /// Outbound reply channel (writer held by the bridge thread).
    outbound: Mutex<Option<std::sync::mpsc::Sender<String>>>,
    /// Whether the bridge thread is alive.
    running: AtomicBool,
    /// The WeChat bridge may inject agent input / show notices.
    pub disabled: AtomicBool,
}

impl std::fmt::Debug for Hub {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Hub")
            .field("dir", &self.dir)
            .field("node_id", &self.node_id)
            .finish()
    }
}

impl Hub {
    /// Build the hub (config + store + identity). Does not register presence —
    /// the rpi-hub plugin owns presence for this process; two rows for one pid
    /// would confuse `/instances`.
    pub fn new() -> Result<Arc<Self>, String> {
        let dir = paths::hub_dir().map_err(|e| e.to_string())?;
        paths::ensure_private_dir(&dir).map_err(|e| e.to_string())?;
        let (config, warnings) = config::load(&dir).map_err(|e| e.to_string())?;
        for warning in warnings {
            log(&warning);
        }
        let store = Arc::new(Store::open(&dir).map_err(|e| e.to_string())?);
        let host = detect_host();
        let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
        let pid = std::process::id();
        Ok(Arc::new(Self {
            store,
            host: host.clone(),
            node_id: names::node_id(&host, &cwd, pid),
            epoch: uuid::Uuid::new_v4().to_string(),
            name: Mutex::new(config.resolved_name(&cwd)),
            config,
            dir,
            busy: AtomicBool::new(false),
            bridge: std::sync::atomic::AtomicPtr::new(std::ptr::null_mut()),
            action_fn: std::sync::Mutex::new(dummy_action),
            host_free: std::sync::Mutex::new(rpi_plugin_sdk::callbacks::free_string),
            shutdown: Shutdown::new(),
            lease: Mutex::new(None),
            user_id: Mutex::new(None),
            outbound: Mutex::new(None),
            running: AtomicBool::new(false),
            disabled: AtomicBool::new(false),
        }))
    }

    pub fn label(&self) -> String {
        format!("{}@{}", self.name(), self.host)
    }

    pub fn name(&self) -> String {
        self.name.lock().unwrap_or_else(|p| p.into_inner()).clone()
    }

    pub fn set_name(&self, name: impl Into<String>) {
        *self.name.lock().unwrap_or_else(|p| p.into_inner()) = name.into();
    }

    /// Install this hub as current, stopping the previous instance's worker.
    pub fn install(self: &Arc<Self>) {
        let previous = {
            let mut slot = cell().lock().unwrap_or_else(|p| p.into_inner());
            slot.replace(Arc::clone(self))
        };
        if let Some(previous) = previous {
            previous.request_shutdown();
        }
    }

    pub fn request_shutdown(&self) {
        self.shutdown.request();
    }

    pub fn bridge_ptr(&self) -> *mut std::ffi::c_void {
        self.bridge.load(Ordering::Acquire)
    }

    pub fn set_bridge(
        &self,
        ptr: *mut std::ffi::c_void,
        action_fn: RuntimeActionFn,
        host_free: rpi_plugin_sdk::FreeStringFn,
    ) {
        self.bridge.store(ptr, Ordering::Release);
        *self.action_fn.lock().unwrap_or_else(|p| p.into_inner()) = action_fn;
        *self.host_free.lock().unwrap_or_else(|p| p.into_inner()) = host_free;
    }

    /// Claim the wechat lease (auto, when the plugin starts the bridge).
    /// Claim the wechat lease AND record it in `self.lease` — the bridge
    /// gate checks the recorded generation, so a claim without this bookkeeping
    /// is fenced out on its first tick (the LeaseLost-on-startup bug).
    pub fn claim_wechat(&self) -> Result<Claim, String> {
        let now = now_ms();
        let claim = self
            .store
            .claim_lease(
                "wechat",
                &self.node_id,
                &self.name(),
                &self.epoch,
                now,
                self.config.lease.ttl_ms(),
            )
            .map_err(|e| e.to_string())?;
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
        self.action(RuntimeActionId::SendUserMessage, args).map(|_| ())
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

    /// Call a host runtime action with the live bridge handle.
    pub fn action(
        &self,
        action: RuntimeActionId,
        args: serde_json::Value,
    ) -> Result<serde_json::Value, String> {
        let bridge = self.bridge_ptr();
        if bridge.is_null() {
            return Err("no runtime bridge (plugin not registered?)".into());
        }
        let payload = args.to_string();
        let mut out = StbString::empty();
        let action_fn = *self.action_fn.lock().unwrap_or_else(|p| p.into_inner());
        let rc = (action_fn)(
            action as u32,
            StbStringRef::from_str(&payload),
            &mut out,
            bridge,
        );
        let host_free = *self.host_free.lock().unwrap_or_else(|p| p.into_inner());
        let owned = unsafe { OwnedString::from_raw(out, Some(host_free)) };
        let text = owned.to_string_lossy();
        if rc != 0 {
            return Err(if text.is_empty() {
                format!("runtime action {action:?} failed")
            } else {
                text
            });
        }
        if text.trim().is_empty() {
            return Ok(serde_json::Value::Null);
        }
        serde_json::from_str(&text).map_err(|e| format!("invalid action result: {e}"))
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
        let sender = self.outbound.lock().unwrap_or_else(|p| p.into_inner()).clone();
        let Some(sender) = sender else {
            return;
        };
        for chunk in crate::bridge::chunk_reply(text) {
            if sender.send(chunk).is_err() {
                break;
            }
        }
    }

    /// Mark the bridge stopped (called when the thread exits).
    pub fn mark_stopped(&self) {
        self.running.store(false, Ordering::Release);
        *self.outbound.lock().unwrap_or_else(|p| p.into_inner()) = None;
        self.set_status("");
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

    /// Presence record (role `normal`; the rpi-hub plugin in the same process
    /// registers the authoritative row — this one is only written when
    /// rpi-hub is absent, and uses a distinct role so `/instances` can tell).
    pub fn record(&self, last_seen: i64) -> InstanceRecord {
        InstanceRecord {
            node_id: format!("{}-wechat", self.node_id),
            name: format!("{}-wechat", self.name()),
            host: self.host.clone(),
            pid: std::process::id(),
            cwd: std::env::current_dir()
                .unwrap_or_default()
                .to_string_lossy()
                .to_string(),
            role: "channel".into(),
            epoch: self.epoch.clone(),
            session_id: None,
            title: None,
            model: None,
            busy: self.busy.load(Ordering::Acquire),
            started_at: last_seen,
            last_seen,
        }
    }
}

/// Placeholder used before the host wires the real bridge (never called then).
extern "C" fn dummy_action(
    _action: u32,
    _args: StbStringRef,
    _out: *mut StbString,
    _user_data: *mut std::ffi::c_void,
) -> i32 {
    1
}

/// Best-effort hostname (mirrors rpi-hub's detection).
pub fn detect_host() -> String {
    for key in ["RPI_HUB_HOST", "HOSTNAME", "HOST", "COMPUTERNAME"] {
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
    fn node_id_role_marks_the_channel_row() {
        let record = InstanceRecord {
            node_id: "n-wechat".into(),
            name: "x-wechat".into(),
            host: "h".into(),
            pid: 1,
            cwd: "/work".into(),
            role: "channel".into(),
            epoch: "e".into(),
            session_id: None,
            title: None,
            model: None,
            busy: false,
            started_at: 0,
            last_seen: 0,
        };
        assert_eq!(record.role, "channel");
    }

    #[test]
    fn detect_host_returns_something() {
        assert!(!detect_host().is_empty());
    }
}
