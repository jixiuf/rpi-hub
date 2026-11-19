//! Per-load hub state: identity, store handle, runtime-action bridge, worker
//! coordination.
//!
//! One `Hub` exists per loaded plugin instance. `/reload` loads a fresh copy of
//! the dylib, so `install` replacing the previous value is normal; the previous
//! worker is told to stop first and its epoch becomes stale in the store, which
//! fences any straggling write.

use std::collections::HashMap;
use std::ffi::c_void;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicPtr, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::time::Duration;

use rpi_mesh_core::api::HubStore;
use rpi_mesh_core::config::{self, HubConfig};
use rpi_mesh_core::names;
use rpi_mesh_core::now_ms;
use rpi_mesh_core::paths;
use rpi_mesh_core::protocol::{EnvelopeKind, MAX_TEXT_BYTES};
use rpi_mesh_core::store::{InstanceRecord, InstanceView};
use rpi_plugin_sdk::callbacks::OwnedString;
use rpi_plugin_sdk::{PluginApi, RuntimeActionFn, RuntimeActionId, StbString, StbStringRef};

/// How long an instance is considered alive without a heartbeat.
pub const ALIVE_MS: i64 = 30_000;

/// A correlated `hub_ask` in flight.
#[derive(Debug, Clone)]
pub struct PendingAsk {
    pub correlation: String,
    pub to_node: String,
    pub to_name: String,
    pub created_at: i64,
    pub deadline: i64,
}

#[derive(Debug, Clone)]
pub struct ReplyOutcome {
    pub ok: bool,
    pub text: String,
    pub at: i64,
}

/// Session facts refreshed from `GetExecutionContext`.
#[derive(Debug, Clone, Default)]
pub struct SessionSnapshot {
    pub session_id: Option<String>,
    pub model: Option<String>,
    pub cwd: Option<String>,
}

pub struct Shutdown {
    flag: AtomicBool,
    lock: Mutex<()>,
    signal: Condvar,
}

impl Shutdown {
    fn new() -> Self {
        Self {
            flag: AtomicBool::new(false),
            lock: Mutex::new(()),
            signal: Condvar::new(),
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
    pub fn wait(&self, timeout: Duration) {
        let guard = self.lock.lock().unwrap_or_else(|p| p.into_inner());
        let (_guard, _timeout) = self
            .signal
            .wait_timeout_while(guard, timeout, |_| !self.requested())
            .unwrap_or_else(|p| p.into_inner());
    }

    pub fn wake(&self) {
        self.signal.notify_all();
    }
}

pub struct Hub {
    pub dir: PathBuf,
    pub config: HubConfig,
    pub store: Arc<dyn HubStore>,
    /// Read-only lease surface (rpi-hub-core::lease). The hub never claims.
    pub lease_observer: rpi_mesh_core::lease::LeaseObserver,
    backend: Mutex<Option<crate::backends::Backends>>,
    pub host: String,
    pub node_id: String,
    pub epoch: String,
    pub pid: u32,
    pub started_at: i64,
    pub sessions: Mutex<SessionSnapshot>,
    name: Mutex<String>,
    busy: AtomicBool,
    registered: AtomicBool,
    bridge: AtomicPtr<c_void>,
    action_fn: RuntimeActionFn,
    host_free: rpi_plugin_sdk::FreeStringFn,
    shutdown: Shutdown,
    worker: Mutex<Option<std::thread::JoinHandle<()>>>,
    pub pending: Mutex<Vec<PendingAsk>>,
    pub replies: Mutex<HashMap<String, ReplyOutcome>>,
    pub last_error: Mutex<Option<String>>,
}

static CURRENT: OnceLock<Mutex<Option<Arc<Hub>>>> = OnceLock::new();

fn cell() -> &'static Mutex<Option<Arc<Hub>>> {
    CURRENT.get_or_init(|| Mutex::new(None))
}

/// The hub for the currently loaded plugin instance, if any.
pub fn current() -> Option<Arc<Hub>> {
    cell().lock().unwrap_or_else(|p| p.into_inner()).clone()
}

/// Drop the reference held by the plugin once its worker has stopped, so the
/// host can unload the library without leaving callbacks reachable.
pub fn clear_current() {
    let mut slot = cell().lock().unwrap_or_else(|p| p.into_inner());
    *slot = None;
}

/// Build the hub and make it current. Any previous worker is stopped first.
pub fn install(api: &PluginApi) -> Result<Arc<Hub>, String> {
    let hub = Arc::new(Hub::new(api)?);
    let previous = {
        let mut slot = cell().lock().unwrap_or_else(|p| p.into_inner());
        slot.replace(Arc::clone(&hub))
    };
    if let Some(previous) = previous {
        previous.request_shutdown();
    }
    Ok(hub)
}

impl Hub {
    fn new(api: &PluginApi) -> Result<Self, String> {
        let dir = paths::hub_dir().map_err(|e| e.to_string())?;
        paths::ensure_private_dir(&dir).map_err(|e| e.to_string())?;
        paths::ensure_private_dir(&paths::log_dir(&dir)).map_err(|e| e.to_string())?;

        let (config, warnings) = config::load(&dir).map_err(|e| e.to_string())?;
        for warning in warnings {
            log(&warning);
        }

        let backends =
            crate::backends::Backends::start(&dir, &config).map_err(|e| e.to_string())?;
        let store = Arc::clone(&backends.store);
        let lease_observer = rpi_mesh_core::lease::LeaseObserver::new(Arc::clone(&store));
        let host = detect_host();
        let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
        let pid = std::process::id();
        let name = config.resolved_name(&cwd);

        Ok(Self {
            dir,
            config,
            store,
            lease_observer,
            backend: Mutex::new(Some(backends)),
            host: host.clone(),
            node_id: names::node_id(&host, &cwd, pid),
            epoch: uuid::Uuid::new_v4().to_string(),
            pid,
            started_at: now_ms(),
            sessions: Mutex::new(SessionSnapshot {
                cwd: Some(cwd.to_string_lossy().to_string()),
                ..Default::default()
            }),
            name: Mutex::new(name),
            busy: AtomicBool::new(false),
            registered: AtomicBool::new(false),
            bridge: AtomicPtr::new(api.user_data),
            action_fn: api.runtime_action,
            host_free: api.free_string,
            shutdown: Shutdown::new(),
            worker: Mutex::new(None),
            pending: Mutex::new(Vec::new()),
            replies: Mutex::new(HashMap::new()),
            last_error: Mutex::new(None),
        })
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    pub fn name(&self) -> String {
        self.name.lock().unwrap_or_else(|p| p.into_inner()).clone()
    }

    pub fn set_name(&self, name: impl Into<String>) {
        *self.name.lock().unwrap_or_else(|p| p.into_inner()) = name.into();
    }

    pub fn registered(&self) -> bool {
        self.registered.load(Ordering::Acquire)
    }

    /// Register presence synchronously.
    ///
    /// Doing this inside `register` (not on the worker thread) removes a real
    /// race: a process that exits right after startup — a bad `--model`, a
    /// failed provider — must still have announced itself, and tests should not
    /// depend on the worker winning a scheduling race.
    pub fn register_presence(&self) -> Result<(), String> {
        if self.registered() {
            return Ok(());
        }
        let now = now_ms();
        let record = self.record(now);
        let registered = self
            .store
            .register_instance(&record, ALIVE_MS)
            .map_err(|e| e.to_string())?;
        if registered.renamed {
            log(&format!(
                "name {} is taken on {}; using {}",
                self.name(),
                self.host,
                registered.name
            ));
        }
        log(&format!(
            "registered {} ({}) in {}",
            registered.name,
            self.node_id,
            self.dir.display()
        ));
        self.set_name(registered.name);
        self.registered.store(true, Ordering::Release);
        Ok(())
    }

    pub fn label(&self) -> String {
        format!("{}@{}", self.name(), self.host)
    }

    pub fn bridge_ptr(&self) -> *mut c_void {
        self.bridge.load(Ordering::Acquire)
    }

    pub fn busy(&self) -> bool {
        self.busy.load(Ordering::Acquire)
    }

    /// Raw atomic handle for channel event handlers that flip busy directly.
    pub fn busy_atomic(&self) -> &AtomicBool {
        &self.busy
    }

    pub fn set_busy(&self, busy: bool) {
        self.busy.store(busy, Ordering::Release);
    }

    pub fn shutdown(&self) -> &Shutdown {
        &self.shutdown
    }

    pub fn request_shutdown(&self) {
        self.shutdown.request();
    }

    pub fn text_limit(&self) -> usize {
        MAX_TEXT_BYTES.min(self.config.limits.max_text_bytes.max(1024))
    }

    pub fn inbox_batch(&self) -> i64 {
        self.config.limits.inbox_batch.max(1)
    }

    pub fn session_snapshot(&self) -> SessionSnapshot {
        self.sessions
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }

    pub fn update_sessions(&self, snapshot: SessionSnapshot) {
        *self.sessions.lock().unwrap_or_else(|p| p.into_inner()) = snapshot;
    }

    /// Call a host runtime action with the live bridge handle.
    pub fn action(
        &self,
        action: RuntimeActionId,
        args: serde_json::Value,
    ) -> Result<serde_json::Value, String> {
        let bridge = self.bridge_ptr();
        if bridge.is_null() {
            return Err("no runtime bridge (plugin not registered?)".to_string());
        }
        let payload = args.to_string();
        let mut out = StbString::empty();
        let rc = (self.action_fn)(
            action as u32,
            StbStringRef::from_str(&payload),
            &mut out,
            bridge,
        );
        // The host produced this string: reclaim it with the host's free.
        let owned = unsafe { OwnedString::from_raw(out, Some(self.host_free)) };
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

    /// Feed text into this instance's agent lane.
    pub fn send_user_message(&self, text: &str, deliver_as: Option<&str>) -> Result<(), String> {
        let mut args = serde_json::json!({ "text": text, "triggerTurn": true });
        if let Some(deliver_as) = deliver_as {
            args["deliverAs"] = serde_json::json!(deliver_as);
        }
        self.action(RuntimeActionId::SendUserMessage, args)
            .map(|_| ())
    }

    /// Show text in the transcript without running the model.
    pub fn notice(&self, text: &str) -> Result<(), String> {
        self.action(
            RuntimeActionId::SendMessage,
            serde_json::json!({
                "message": {
                    "customType": "rpi-hub",
                    "content": text,
                    "display": true,
                },
                "options": { "triggerTurn": false }
            }),
        )
        .map(|_| ())
    }

    pub fn set_status(&self, value: &str) {
        let _ = self.action(
            RuntimeActionId::SetStatus,
            serde_json::json!({ "key": "hub", "value": value }),
        );
    }

    pub fn record_error(&self, message: impl Into<String>) {
        let message = message.into();
        log(&format!("error: {message}"));
        *self.last_error.lock().unwrap_or_else(|p| p.into_inner()) = Some(message);
    }

    pub fn instances(&self, now: i64) -> Result<Vec<InstanceView>, String> {
        self.store
            .list_instances_alive(now, ALIVE_MS)
            .map_err(|e| e.to_string())
    }

    /// Resolve a target by node-id prefix, exact name, base name, or unique prefix.
    pub fn resolve_target(&self, query: &str) -> Result<InstanceView, String> {
        let query = query.trim();
        if query.is_empty() {
            return Err("target is empty".into());
        }
        let instances = self.instances(now_ms())?;
        if instances.is_empty() {
            return Err("no other rpi instance is running".into());
        }
        let by_id: Vec<&InstanceView> = instances
            .iter()
            .filter(|i| i.node_id.starts_with(query))
            .collect();
        if by_id.len() == 1 {
            return Ok(by_id[0].clone());
        }
        let labels: Vec<String> = instances
            .iter()
            .map(|i| format!("{}@{}", i.name, i.host))
            .collect();
        // Try the full `name@host` label first.
        if let Some(index) = labels.iter().position(|l| l == query) {
            return Ok(instances[index].clone());
        }
        let names: Vec<&str> = instances.iter().map(|i| i.name.as_str()).collect();
        match names::resolve(query, names.iter().copied()) {
            Ok(Some(index)) => Ok(instances[index].clone()),
            Ok(None) | Err(_) => {
                // Fallback: the query may come from a Tab completion frame
                // captured before the target restarted (its name gained a
                // -<pid> suffix). Match on the base name (suffix stripped),
                // requiring a unique hit.
                let query_base = names::base_name(query);
                let base_matches: Vec<&InstanceView> = instances
                    .iter()
                    .filter(|i| names::base_name(&i.name) == query_base)
                    .collect();
                match base_matches.len() {
                    1 => Ok(base_matches[0].clone()),
                    _ => Err(format!(
                        "no instance matches {query:?}; running: {}",
                        labels.join(", ")
                    )),
                }
            }
        }
    }

    /// Move the wechat lease to `target` — shared by `/use` and the selector.
    /// `owner_epoch` is empty here: the target instance claims the lease with
    /// its own epoch on its next lease tick (same owner_node → resume branch).
    pub fn switch_lease_to(&self, target: &InstanceView) -> Result<String, String> {
        // Self hand-off: only a no-op when THIS instance really holds the
        // channel. Otherwise it means "take the channel back for me" — a
        // perfectly legal request after another instance grabbed the lease —
        // which the old code refused with a false "already here" message.
        // Check the store, not the argument.
        if target.node_id == self.node_id {
            let now = now_ms();
            let mine = self
                .lease_observer
                .holder("wechat")
                .ok()
                .flatten()
                .is_some_and(|h| h.owner_node == self.node_id && h.expires_at > now);
            if mine {
                return Ok(format!(
                    "微信接管已在本实例（{}），无需切换",
                    target.label()
                ));
            }
            // Fall through: assign to ourselves. Bumps the generation, fences
            // the remote holder, and this machine's watcher starts the bridge
            // on the lease-change signal.
        }
        let admin = rpi_mesh_core::lease::LeaseAdmin::new(
            Arc::clone(&self.store),
            self.config.lease.ttl_ms(),
        );
        let generation = admin.assign_to("wechat", target, now_ms())?;
        Ok(format!(
            "微信接管已切换到 {}{}（generation {generation}）",
            target.label(),
            if target.node_id == self.node_id {
                "（本实例）"
            } else {
                ""
            }
        ))
    }

    /// Presence record for the store, built from the current session snapshot.
    pub fn record(&self, last_seen: i64) -> InstanceRecord {
        let snapshot = self.session_snapshot();
        InstanceRecord {
            node_id: self.node_id.clone(),
            name: self.name(),
            host: self.host.clone(),
            pid: self.pid,
            cwd: snapshot.cwd.unwrap_or_default(),
            role: "normal".to_string(),
            epoch: self.epoch.clone(),
            session_id: snapshot.session_id,
            title: None,
            model: snapshot.model,
            busy: self.busy(),
            started_at: self.started_at,
            last_seen,
        }
    }

    /// Rename this instance. Uniqueness/collision rules are the store's, so the
    /// returned name may carry a `-<pid>` suffix.
    pub fn rename(&self, name: &str) -> Result<String, String> {
        let mut record = self.record(now_ms());
        record.name = name.to_string();
        let registered = self
            .store
            .register_instance(&record, ALIVE_MS)
            .map_err(|e| e.to_string())?;
        self.set_name(registered.name.clone());
        Ok(registered.name)
    }

    /// Human-readable `/instances` output, shared by the tool and the command.
    pub fn format_instances(&self, now: i64) -> Result<String, String> {
        let instances = self.instances(now)?;
        let holder = self
            .lease_observer
            .holder("wechat")
            .map_err(|e| e.to_string())?
            .filter(|h| h.expires_at > now);
        let holder_node = holder.as_ref().map(|h| h.owner_node.clone());
        let viewer = rpi_mesh_core::render::RenderContext {
            viewer_node_id: &self.node_id,
            wechat_holder_node: holder_node.as_deref(),
            remote: !matches!(
                self.config.remote.mode,
                rpi_mesh_core::config::RemoteMode::Local
            ),
        };
        let holder_triple = holder.map(|h| (h.owner_name, h.generation, h.expires_at));
        Ok(rpi_mesh_core::render::render_instances_text(
            &instances,
            &viewer,
            now,
            holder_triple,
        ))
    }

    /// Send an envelope to a resolved target (self-targets short-circuit).
    pub fn deliver(&self, envelope: rpi_mesh_core::protocol::Envelope) -> Result<(), String> {
        if envelope.to_node == self.node_id {
            return self.deliver_local(envelope);
        }
        self.store
            .send_envelope(&envelope)
            .map(|_| ())
            .map_err(|e| e.to_string())
    }

    fn deliver_local(&self, envelope: rpi_mesh_core::protocol::Envelope) -> Result<(), String> {
        match envelope.kind {
            EnvelopeKind::Text => {
                let text = envelope.text.unwrap_or_default();
                self.send_user_message(&text, None)
            }
            EnvelopeKind::Cmd => Err("a command cannot target the originating instance".into()),
            EnvelopeKind::Reply => {
                self.answer_reply(
                    &envelope.correlation.unwrap_or_default(),
                    ReplyOutcome {
                        ok: envelope.ok.unwrap_or(true),
                        text: envelope.text.unwrap_or_default(),
                        at: now_ms(),
                    },
                );
                Ok(())
            }
            EnvelopeKind::Notice => self.notice(&envelope.text.unwrap_or_default()),
            // Assign/unassign are display-only; the hub plugin in the same
            // process shows the notice via its worker mailbox instead.
            EnvelopeKind::Assign | EnvelopeKind::Unassign => Ok(()),
        }
    }

    /// Register interest in a correlated reply *before* sending the request.
    ///
    /// `at == 0` marks the slot as still pending; without it a fast reply would
    /// look like an unsolicited message and be surfaced as a notice instead of
    /// reaching the waiting tool.
    pub fn expect_reply(&self, correlation: &str) {
        self.replies
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(
                correlation.to_string(),
                ReplyOutcome {
                    ok: false,
                    text: String::new(),
                    at: 0,
                },
            );
    }

    /// Take an answered reply, leaving a pending slot in place.
    pub fn take_reply(&self, correlation: &str) -> Option<ReplyOutcome> {
        let mut replies = self.replies.lock().unwrap_or_else(|p| p.into_inner());
        match replies.get(correlation) {
            Some(outcome) if outcome.at != 0 => replies.remove(correlation),
            Some(_) => None,
            None => None,
        }
    }

    /// Deliver an answer to a waiting slot. Returns false when nobody waits.
    pub fn answer_reply(&self, correlation: &str, outcome: ReplyOutcome) -> bool {
        let mut replies = self.replies.lock().unwrap_or_else(|p| p.into_inner());
        match replies.get_mut(correlation) {
            Some(slot) => {
                *slot = outcome;
                true
            }
            None => false,
        }
    }

    /// Drop a pending slot (timeout/cancel) and reap answered-but-unread ones.
    pub fn forget_reply(&self, correlation: &str) {
        self.replies
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(correlation);
    }

    pub fn start_worker(self: &Arc<Self>) {
        let hub = Arc::clone(self);
        let handle = std::thread::Builder::new()
            .name("rpi-hub".into())
            .spawn(move || crate::worker::run(hub))
            .ok();
        *self.worker.lock().unwrap_or_else(|p| p.into_inner()) = handle;
    }

    /// Where this node's rows live (`local …`, `relay …`, `client …`).
    pub fn backend_description(&self) -> String {
        self.backend
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .as_ref()
            .map(|backend| backend.description.clone())
            .unwrap_or_else(|| "closed".to_string())
    }

    /// Stop the tunnel/relay threads that back this node.
    pub fn stop_backends(&self) {
        if let Some(mut backend) = self
            .backend
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .take()
        {
            backend.shutdown();
        }
    }

    /// Stop and join the worker (bounded by the caller's timeout).
    pub fn stop_worker(&self) {
        self.request_shutdown();
        let handle = self.worker.lock().unwrap_or_else(|p| p.into_inner()).take();
        if let Some(handle) = handle {
            let _ = handle.join();
        }
    }
}

/// Append a line to `logs/hub.log` (5 MB rotation) and, when `RPI_HUB_DEBUG=1`,
/// to stderr.
pub fn log(message: &str) {
    let line = format!("{} [{}] {message}\n", now_ms(), std::process::id());
    if std::env::var("RPI_HUB_DEBUG").ok().as_deref() == Some("1") {
        eprint!("rpi-hub: {line}");
    }
    let Ok(dir) = paths::hub_dir() else {
        return;
    };
    let dir = paths::log_dir(&dir);
    let _ = std::fs::create_dir_all(&dir);
    let path = dir.join("hub.log");
    if let Ok(metadata) = std::fs::metadata(&path) {
        if metadata.len() > 5 * 1024 * 1024 {
            let _ = std::fs::rename(&path, dir.join("hub.log.1"));
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

/// Best-effort hostname without an extra dependency.
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
