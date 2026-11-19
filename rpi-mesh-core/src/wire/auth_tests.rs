//! Auth behavior: token rules, handshake enforcement, redaction.

use super::{serve, Endpoint, RemoteStore, Token, AUTH_METHOD, TOKEN_LEN};
use crate::api::HubStore;
use crate::store::Store;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

fn temp_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "rpi-hub-wire-{name}-{}-{}",
        std::process::id(),
        crate::now_ms()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn start_relay(dir: &PathBuf, token: &Token) -> (Arc<AtomicBool>, std::thread::JoinHandle<()>) {
    let store = Arc::new(Store::open(dir).unwrap());
    let endpoint = Endpoint::Unix(dir.join("relay.sock"));
    let stop = Arc::new(AtomicBool::new(false));
    let handle = {
        let endpoint = endpoint.clone();
        let store = Arc::clone(&store);
        let stop = Arc::clone(&stop);
        let token = token.clone();
        // serve generates its own token for unix endpoints; drive the low-level
        // handshake by seeding the file it reads.
        std::fs::write(dir.join("relay-token"), format!("{}\n", token.as_str())).unwrap();
        std::thread::spawn(move || {
            let _ = serve(&endpoint, store, stop, false, token);
        })
    };
    // Wait for the listener.
    let socket = dir.join("relay.sock");
    for _ in 0..100 {
        if socket.exists() {
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    (stop, handle)
}

#[test]
fn token_rules() {
    assert!(Token::from_hex(&"ab".repeat(16)).is_ok());
    assert!(Token::from_hex("short").is_err());
    assert!(Token::from_hex(&"zz".repeat(16)).is_err());
    let token = Token::from_hex(&"ab".repeat(16)).unwrap();
    assert!(token.matches(&"ab".repeat(16)));
    assert!(!token.matches(&"ab".repeat(15)));
    assert!(!token.matches(&"cd".repeat(16)));
    assert!(!token.matches(""));
    // Debug never leaks the secret.
    assert!(!format!("{token:?}").contains("ab"));
    assert_eq!(TOKEN_LEN, 32);
}

#[test]
fn generate_and_load_round_trip() {
    let dir = temp_dir("generate");
    let path = dir.join("relay-token");
    let token = super::load_token(&path, true).unwrap();
    assert_eq!(token.as_str().len(), 32);
    let reloaded = super::load_token(&path, true).unwrap();
    assert_eq!(token, reloaded, "the file must be the source of truth");
    // Missing file on the client side is a clear error.
    assert!(super::load_token(&dir.join("nope"), false).is_err());
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn reject_bad_endpoints() {
    assert!(Endpoint::parse("host:99999").is_err());
    assert!(Endpoint::parse(":9527").is_err());
    assert!(Endpoint::parse("no-port").is_err());
    assert!(Endpoint::parse("unix:").is_err());
    assert_eq!(
        Endpoint::parse("unix:/tmp/x.sock").unwrap(),
        Endpoint::Unix(PathBuf::from("/tmp/x.sock"))
    );
    assert_eq!(
        Endpoint::parse("127.0.0.1:9527").unwrap(),
        Endpoint::Tcp("127.0.0.1".into(), 9527)
    );
    assert!(Endpoint::parse("unix:/tmp/x.sock").unwrap().is_loopback());
    assert!(Endpoint::parse("127.0.0.1:1").unwrap().is_loopback());
    assert!(!Endpoint::parse("10.0.0.5:9527").unwrap().is_loopback());
}

#[test]
fn connections_without_auth_are_refused() {
    let dir = temp_dir("unauth");
    let token = Token::from_hex(&"11".repeat(16)).unwrap();
    let (stop, handle) = start_relay(&dir, &token);
    let endpoint = Endpoint::Unix(dir.join("relay.sock"));

    // A client with the WRONG token gets an auth error, not data.
    let wrong = RemoteStore::new(
        endpoint.clone(),
        Token::from_hex(&"22".repeat(16)).unwrap(),
    );
    let error = wrong.list_instances(0, 30_000).unwrap_err();
    assert!(
        error.to_string().contains("authentication"),
        "got {error}"
    );

    // A raw connection calling a store method before authenticate is refused.
    let mut stream = super::connect_stream(&endpoint, Duration::from_secs(5)).unwrap();
    use std::io::{BufRead, BufReader, Write};
    let mut reader = BufReader::new(stream.try_clone().unwrap());
    writeln!(stream, r#"{{"id":1,"method":"list_instances","params":{{"now":0,"aliveMs":1}}}}"#).unwrap();
    let mut line = String::new();
    reader.read_line(&mut line).unwrap();
    assert!(line.contains("authentication required"), "{line}");

    // Correct token works.
    let good = RemoteStore::new(endpoint, token);
    assert!(good.list_instances(0, 30_000).is_ok());

    stop.store(true, Ordering::SeqCst);
    let _ = handle.join();
    let _ = std::fs::remove_dir_all(&dir);
}
