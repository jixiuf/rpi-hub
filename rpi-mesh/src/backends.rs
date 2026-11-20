//! Which mesh.db this instance reads and writes, and who serves it.
//!
//! Two roles, decided at startup and re-negotiated at runtime:
//!
//! - **host** — opens the local `mesh.db` and serves it to guests on
//!   `listen`. The port is the mutex: exactly one host per address.
//! - **guest** — all state lives on a host reached through `connect`.
//!
//! Runtime re-negotiation (no config change needed):
//! - a host whose port is busy downgrades to guest (connecting to that same
//!   port locally) and keeps retrying the bind in the background — when the
//!   current host dies, it upgrades back;
//! - a guest whose host dies attempts to bind itself **when the host is on
//!   the same machine** (`connect` targets localhost) — taking over the
//!   mesh.db that used to live there. Cross-machine guests just keep
//!   reconnecting.
//!
//! The rest of the plugin sees `Arc<dyn MeshStore>` — a switchable proxy, so
//! role changes take effect on the next call without touching any thread.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use rpi_mesh_core::api::MeshStore;
use rpi_mesh_core::config::{MeshConfig, RelayConfig, RemoteRole};
use rpi_mesh_core::paths;
use rpi_mesh_core::store::Store;
use rpi_mesh_core::wire::{load_token, serve, Endpoint, RemoteStore, Token};

use crate::mesh::log;
use rpi_mesh_core::store::SwitchableStore;

const RETRY_TICK: Duration = Duration::from_secs(5);

pub struct Backends {
    /// The switchable proxy. `store` is its dyn handle; `switch_store` reroutes.
    pub proxy: Arc<SwitchableStore>,
    pub store: Arc<dyn MeshStore>,
    pub description: String,
    stop: Arc<AtomicBool>,
    join: Option<std::thread::JoinHandle<()>>,
}

/// Parsed intent for this instance.
#[derive(Clone)]
enum Intent {
    /// Host intent: serve `listen` for guests.
    Host { listen: Endpoint, token: Token, http: bool },
    /// Guest intent: all state on the host at `connect`.
    Guest { connect: Endpoint, token: Token, http: bool },
}

impl Backends {
    pub fn start(dir: &Path, config: &MeshConfig) -> Result<Self, String> {
        let relay_cfg = &config.remote.relay;
        let intent = match config.remote.role {
            None => {
                // Pure local: own mesh.db, no networking.
                let store = Arc::new(SwitchableStore::new(Arc::new(
                    Store::open(dir).map_err(|e| e.to_string())?,
                )));
                let proxy = Arc::new(SwitchableStore::new(store));
                return Ok(Self {
                    proxy: Arc::clone(&proxy),
                    store: proxy as Arc<dyn MeshStore>,
                    description: format!("local {}", dir.display()),
                    stop: Arc::new(AtomicBool::new(false)),
                    join: None,
                });
            }
            Some(RemoteRole::Host) => {
                let listen = listen_endpoint(relay_cfg)?;
                let token = load_token(&token_path(dir, config), true)
                    .map_err(|e| e.to_string())?;
                Intent::Host {
                    listen,
                    token,
                    http: relay_cfg.http,
                }
            }
            Some(RemoteRole::Guest) => {
                let connect = connect_endpoint(relay_cfg)?;
                let token = load_token(&token_path(dir, config), false)
                    .map_err(|e| e.to_string())?;
                Intent::Guest {
                    connect,
                    token,
                    http: relay_cfg.http,
                }
            }
        };

        let stop = Arc::new(AtomicBool::new(false));
        let proxy = Arc::new(SwitchableStore::new(match &intent {
            Intent::Host { .. } => {
                Arc::new(Store::open(dir).map_err(|e| e.to_string())?) as Arc<dyn MeshStore>
            }
            Intent::Guest { connect, token, http } => guest_store(connect, *http, token),
        }));

        // Host intent runs the negotiator thread (bind attempts + downgrade/
        // upgrade). Guest intent has no thread: RemoteStore reconnects itself.
        let join = match &intent {
            Intent::Host { .. } => Some(
                std::thread::Builder::new()
                    .name("rpi-mesh-host".into())
                    .spawn({
                        let stop = Arc::clone(&stop);
                        let proxy = Arc::clone(&proxy);
                        let dir = dir.to_path_buf();
                        let intent = intent.clone();
                        move || host_negotiator(&dir, intent, proxy, stop)
                    })
                    .map_err(|e| e.to_string())?,
            ),
            Intent::Guest { .. } => None,
        };

        let description = match &intent {
            Intent::Host { listen, .. } => {
                format!("host {listen:?} (bind attempts until it wins)")
            }
            Intent::Guest { connect, .. } => format!("guest {connect:?}"),
        };

        Ok(Self {
            proxy: Arc::clone(&proxy),
            store: proxy as Arc<dyn MeshStore>,
            description,
            stop,
            join,
        })
    }

    pub fn shutdown(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
}

/// The host-intent state machine. Runs on its own thread.
///
/// States: HOST (serving) and DEGRADED (port busy → guest against the local
/// port). Transitions: bind succeeds → HOST; bind busy → DEGRADED; while
/// DEGRADED, retry the bind every tick — winning it upgrades back to HOST.
fn host_negotiator(
    dir: &Path,
    intent: Intent,
    proxy: Arc<SwitchableStore>,
    stop: Arc<AtomicBool>,
) {
    let Intent::Host {
        listen,
        token,
        http,
    } = intent
    else {
        return;
    };
    // Open the local DB with retries — a transient failure here must NOT
    // fall back to an in-memory store: that would silently swap the proxy to
    // an empty database and the instance would vanish from every listing
    // (observed as "没有登记的实例" after reload storms).
    let local_store = || -> Arc<Store> {
        for attempt in 0..10 {
            match Store::open(dir) {
                Ok(store) => return Arc::new(store),
                Err(error) => {
                    log(&format!(
                        "host: local store open failed (attempt {attempt}): {error}"
                    ));
                    std::thread::sleep(Duration::from_millis(500));
                }
            }
        }
        log("host: local store open failed repeatedly — using in-memory store as LAST resort");
        Arc::new(Store::open_in_memory().expect("in-memory store"))
    };
    // HOST state: serve until the listener returns.
    loop {
        if stop.load(Ordering::Acquire) {
            return;
        }
        log(&format!("host: serving {listen:?}"));
        match serve(&listen, local_store(), Arc::clone(&stop), http, token.clone()) {
            Ok(()) => return, // clean shutdown
            Err(error) => {
                if !port_busy(&error) {
                    log(&format!("host stopped: {error}"));
                    return;
                }
                // Port busy: downgrade to guest against the local port and
                // keep retrying the bind — upgrade when the port frees.
                log(&format!(
                    "host: port busy → degraded to guest (local {listen:?}); retrying bind every 5s"
                ));
                let guest = guest_store(&listen, http, &token);
                proxy.switch(guest);
                loop {
                    if stop.load(Ordering::Acquire) {
                        proxy.switch(local_store());
                        return;
                    }
                    std::thread::sleep(RETRY_TICK);
                    if stop.load(Ordering::Acquire) {
                        return;
                    }
                    // Try to win the bind back. serve() binds internally; a
                    // dedicated probe keeps the state machine simple.
                    if try_bind(&listen) {
                        log(&format!("host: port freed — upgrading to HOST"));
                        proxy.switch(local_store());
                        break; // back to the serving loop
                    }
                }
            }
        }
    }
}

/// Does `error` mean "the port is held by someone else"?
fn port_busy(error: &rpi_mesh_core::MeshError) -> bool {
    let text = error.to_string();
    text.contains("already listening")
        || text.contains("Address already in use")
        || text.contains("in use")
}

/// Non-destructive bind probe: succeeds only if we can claim the port.
fn try_bind(listen: &Endpoint) -> bool {
    match listen {
        Endpoint::Tcp(host, port) => {
            std::net::TcpListener::bind((host.as_str(), *port)).is_ok()
        }
        Endpoint::Unix(path) => !path.exists() || !std::path::Path::new(path).exists(),
    }
}

/// A guest MeshStore pointing at `endpoint` (http mode per config).
fn guest_store(endpoint: &Endpoint, http: bool, token: &Token) -> Arc<dyn MeshStore> {
    Arc::new(RemoteStore::with_http(endpoint.clone(), token.clone(), http)) as Arc<dyn MeshStore>
}

fn listen_endpoint(relay_cfg: &RelayConfig) -> Result<Endpoint, String> {
    match relay_cfg.listen.as_deref() {
        Some(text) => Endpoint::parse(text).map_err(|e| e.to_string()),
        None => Ok(Endpoint::Tcp("127.0.0.1".into(), 9527)),
    }
}

fn connect_endpoint(relay_cfg: &RelayConfig) -> Result<Endpoint, String> {
    relay_cfg
        .connect
        .as_deref()
        .map(Endpoint::parse)
        .ok_or_else(|| "remote.relay.connect is required for the guest role".to_string())?
        .map_err(|e| e.to_string())
}

/// Token file location: explicit `tokenFile`, else `<mesh dir>/relay-token`.
fn token_path(dir: &Path, config: &MeshConfig) -> PathBuf {
    if let Some(path) = &config.remote.relay.token_file {
        let expanded = expand_home(path);
        return PathBuf::from(expanded);
    }
    paths::mesh_dir()
        .unwrap_or_else(|_| dir.to_path_buf())
        .join("relay-token")
}

fn expand_home(path: &str) -> String {
    if let Some(rest) = path.strip_prefix("~/") {
        if let Ok(home) = std::env::var("HOME") {
            return format!("{home}/{rest}");
        }
    }
    path.to_string()
}

impl Drop for Backends {
    fn drop(&mut self) {
        self.shutdown();
    }
}
