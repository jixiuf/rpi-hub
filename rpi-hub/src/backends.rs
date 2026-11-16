//! Which database a node talks to.
//!
//! - `local`  — this process opens `hub.db` directly (single machine).
//! - `relay`  — this process opens `hub.db` **and** serves it on a unix socket
//!   for remote nodes (normally the always-on server).
//! - `client` — all state lives on a relay, reached through an SSH stream-local
//!   forward this process supervises.
//!
//! The rest of the plugin only sees `Arc<dyn HubStore>`.

use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use rpi_hub_core::api::HubStore;
use rpi_hub_core::config::{HubConfig, RemoteMode};
use rpi_hub_core::paths;
use rpi_hub_core::store::Store;
use rpi_hub_core::wire::{load_token, serve, Endpoint, RemoteStore};

use crate::host::log;

pub struct Backends {
    pub store: Arc<dyn HubStore>,
    pub description: String,
    relay_stop: Option<Arc<AtomicBool>>,
    relay_join: Option<std::thread::JoinHandle<()>>,
}

impl Backends {
    pub fn start(dir: &Path, config: &HubConfig) -> Result<Self, String> {
        let relay = &config.remote.relay;
        if relay.is_ssh_legacy() {
            return Err(
                "remote.relay.sshHost/remoteSocket/localSocket are no longer used; \
                 configure listen/connect + tokenFile instead"
                    .to_string(),
            );
        }
        match config.remote.mode {
            RemoteMode::Local => {
                let store = Arc::new(Store::open(dir).map_err(|e| e.to_string())?);
                Ok(Self {
                    store,
                    description: format!("local {}", dir.display()),
                    relay_stop: None,
                    relay_join: None,
                })
            }
            RemoteMode::Relay => {
                let store = Arc::new(Store::open(dir).map_err(|e| e.to_string())?);
                let endpoint = match relay.listen.as_deref() {
                    Some(text) => Endpoint::parse(text).map_err(|e| e.to_string())?,
                    None => Endpoint::Tcp("127.0.0.1".into(), 9527),
                };
                // Generate/validate the token eagerly so a bad tokenFile fails
                // at startup instead of at the first remote connection, and hand
                // THE token to serve() — the file path is config, not guesswork.
                let token = load_token(&token_path(dir, config), true)
                    .map_err(|e| e.to_string())?;
                if !endpoint.is_loopback() {
                    log(&format!(
                        "relay listening on {endpoint:?} (non-loopback); the shared token is the only defense"
                    ));
                }
                let stop = Arc::new(AtomicBool::new(false));
                let endpoint_for_thread = endpoint.clone();
                let http = relay.http;
                let store_for_thread = Arc::clone(&store);
                let stop_for_thread = Arc::clone(&stop);
                let join = std::thread::Builder::new()
                    .name("rpi-hub-relay".into())
                    .spawn(move || {
                        log(&format!("relay serving {endpoint_for_thread:?}"));
                        if let Err(error) = serve(
                            &endpoint_for_thread,
                            store_for_thread,
                            stop_for_thread,
                            http,
                            token,
                        ) {
                            log(&format!("relay stopped: {error}"));
                        }
                    })
                    .map_err(|e| e.to_string())?;
                Ok(Self {
                    store,
                    description: format!(
                        "relay {endpoint:?}{}",
                        if relay.http { " (http)" } else { "" }
                    ),
                    relay_stop: Some(stop),
                    relay_join: Some(join),
                })
            }
            RemoteMode::Client => {
                let endpoint = relay
                    .connect
                    .as_deref()
                    .map(Endpoint::parse)
                    .ok_or_else(|| {
                        "remote.relay.connect is required for remote.mode=client".to_string()
                    })
                    .and_then(|r| r.map_err(|e| e.to_string()))?;
                let token = load_token(&token_path(dir, config), false)
                    .map_err(|e| e.to_string())?;
                Ok(Self {
                    store: Arc::new(RemoteStore::with_http(endpoint.clone(), token, relay.http)),
                    description: format!(
                        "client {endpoint:?}{}",
                        if relay.http { " (http)" } else { "" }
                    ),
                    relay_stop: None,
                    relay_join: None,
                })
            }
        }
    }

    pub fn shutdown(&mut self) {
        if let Some(stop) = self.relay_stop.take() {
            stop.store(true, std::sync::atomic::Ordering::SeqCst);
        }
        if let Some(join) = self.relay_join.take() {
            let _ = join.join();
        }
    }
}

/// Token file location: explicit `tokenFile`, else `<hub dir>/relay-token`.
fn token_path(dir: &Path, config: &HubConfig) -> PathBuf {
    if let Some(path) = &config.remote.relay.token_file {
        let expanded = expand_home(path);
        return PathBuf::from(expanded);
    }
    paths::hub_dir()
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
