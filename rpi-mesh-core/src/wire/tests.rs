//! Relay/client round-trip tests over a real unix socket.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use super::{serve, Endpoint, RemoteStore, Token};
use std::path::PathBuf;
use crate::api::HubStore;
use crate::protocol::Envelope;
use crate::store::{Claim, Heartbeat, InstanceRecord, Store};

const ALIVE: i64 = 30_000;
const TTL: i64 = 20_000;

struct Relay {
    dir: tempfile::TempDir,
    store: Arc<Store>,
    stop: Arc<AtomicBool>,
    handle: Option<std::thread::JoinHandle<()>>,
}

impl Relay {
    fn start() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(Store::open(dir.path()).unwrap());
        let socket = dir.path().join("relay.sock");
        let stop = Arc::new(AtomicBool::new(false));
        let endpoint = Endpoint::Unix(socket);
        // serve() and the client must share THE token file.
        let token = super::load_token(&dir.path().join("relay-token"), true).unwrap();
        let handle = {
            let endpoint = endpoint.clone();
            let store = Arc::clone(&store);
            let stop = Arc::clone(&stop);
            let token = token.clone();
            std::thread::spawn(move || {
                let _ = serve(&endpoint, store, stop, false, token);
            })
        };
        // Wait for the listener.
        let socket = dir.path().join("relay.sock");
        for _ in 0..100 {
            if socket.exists() {
                break;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        Self {
            dir,
            store,
            stop,
            handle: Some(handle),
        }
    }

    fn client(&self) -> RemoteStore {
        let endpoint = Endpoint::Unix(self.dir.path().join("relay.sock"));
        let token = super::load_token(&self.dir.path().join("relay-token"), true).unwrap();
        RemoteStore::new(endpoint, token)
    }
}

impl Drop for Relay {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

fn record(node: &str, name: &str, now: i64) -> InstanceRecord {
    InstanceRecord {
        node_id: node.to_string(),
        name: name.to_string(),
        host: "alpha".into(),
        pid: 1,
        cwd: "/work".into(),
        role: "normal".into(),
        epoch: "e1".into(),
        session_id: Some("s1".into()),
        title: None,
        model: None,
        busy: false,
        started_at: now,
        last_seen: now,
    }
}

#[test]
fn relay_round_trips_presence_leases_and_mailbox() {
    let relay = Relay::start();
    let client = relay.client();
    let now = 1_000_000;

    let registered = client.register_instance(&record("n1", "home", now), ALIVE).unwrap();
    assert_eq!(registered.name, "home");

    assert!(client
        .heartbeat(
            "n1",
            "e1",
            now + 1,
            &Heartbeat {
                busy: true,
                ..Default::default()
            },
        )
        .unwrap());
    let listed = client.list_instances(now + 2, ALIVE).unwrap();
    assert_eq!(listed.len(), 1);
    assert!(listed[0].busy);

    let claim = client
        .claim_lease("wechat", "n1", "home", now, TTL)
        .unwrap();
    assert_eq!(
        claim,
        Claim::Granted {
            generation: 1,
            expires_at: now + TTL
        }
    );
    assert!(client
        .check_lease("wechat", "n1", 1, now + 1)
        .unwrap());
    assert!(client.lease_holder("wechat").unwrap().is_some());

    let envelope = Envelope::text("n1", "home", "n2", "hello", now).unwrap();
    assert!(client.send_envelope(&envelope).unwrap());
    let inbox = client.inbox("n2", now + 1, 10).unwrap();
    assert_eq!(inbox.len(), 1);
    assert!(client.ack_envelope(&envelope.id, "n2", now + 2).unwrap());
    assert!(client.inbox("n2", now + 3, 10).unwrap().is_empty());

    // The relay's own database reflects the writes: one source of truth.
    let stats = relay.store.stats(now + 4).unwrap();
    assert_eq!(stats.instances, 1);
    assert_eq!(stats.leases, 1);
}

#[test]
fn a_second_node_is_fenced_by_the_relay_lease() {
    let relay = Relay::start();
    let a = relay.client();
    let b = relay.client();
    let now = 1_000_000;

    a.register_instance(&record("n1", "home", now), ALIVE).unwrap();
    b.register_instance(&record("n2", "models", now), ALIVE).unwrap();

    assert!(matches!(
        a.claim_lease("wechat", "n1", "home", now, TTL).unwrap(),
        Claim::Granted { .. }
    ));
    assert!(matches!(
        b.claim_lease("wechat", "n2", "models", now, TTL).unwrap(),
        Claim::Held { .. }
    ));
    // Transfer through the same relay; the old owner's gate must fail.
    let generation = b
        .assign_lease("wechat", "n2", "models", now + 1, TTL)
        .unwrap();
    assert_eq!(generation, 2);
    assert!(!a.check_lease("wechat", "n1", 1, now + 2).unwrap());
    // The new owner must claim to bind its epoch (its worker does this on the
    // next lease tick); the generation stays put because the node matches.
    assert_eq!(
        b.claim_lease("wechat", "n2", "models", now + 2, TTL)
            .unwrap(),
        Claim::Granted {
            generation: 2,
            expires_at: now + 2 + TTL
        }
    );
    assert!(b.check_lease("wechat", "n2", 2, now + 3).unwrap());
}

#[test]
fn a_missing_relay_surfaces_a_clear_error() {
    let dir = tempfile::tempdir().unwrap();
    let client = RemoteStore::new(
        Endpoint::Unix(dir.path().join("nope.sock")),
        Token::from_hex(&"ab".repeat(16)).unwrap(),
    );
    let error = client.list_instances(0, ALIVE).unwrap_err();
    assert!(error.to_string().contains("cannot reach the hub relay"), "{error}");
}

#[test]
fn serving_twice_on_the_same_socket_is_refused() {
    let relay = Relay::start();
    let socket = relay.dir.path().join("relay.sock");
    let token = Token::from_hex(&"bb".repeat(16)).unwrap();
    let error = serve(
        &Endpoint::Unix(socket),
        Arc::clone(&relay.store),
        Arc::new(AtomicBool::new(false)),
        false,
        token,
    )
    .unwrap_err();
    assert!(error.to_string().contains("already listening"));
}
