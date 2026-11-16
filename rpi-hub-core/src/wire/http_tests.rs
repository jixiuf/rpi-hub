//! HTTP-smuggling mode: one RPC per POST /hub-rpc, token in X-Hub-Auth.

use super::{serve, Endpoint, RemoteStore, Token};
use crate::api::HubStore;
use crate::protocol::Envelope;
use crate::store::{Claim, Store};
use std::io::{BufRead, BufReader, Read, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

const TOKEN_HEX: &str = "ef1234abcd567890ef1234abcd567890";

fn temp_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "rpi-hub-http-{name}-{}-{}",
        std::process::id(),
        crate::now_ms()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn start_http_relay(dir: &PathBuf, port: u16) -> (Arc<AtomicBool>, std::thread::JoinHandle<()>) {
    let store = Arc::new(Store::open(dir).unwrap());
    let endpoint = Endpoint::Tcp("127.0.0.1".into(), port);
    let token = Token::from_hex(TOKEN_HEX).unwrap();
    std::fs::write(dir.join("relay-token"), format!("{TOKEN_HEX}\n")).unwrap();
    // serve() picks the token file from RPI_HUB_TOKEN_FILE when set, so the
    // test's token is authoritative instead of ~/.rpi/agent/pi-hub/relay-token.
    std::env::set_var("RPI_HUB_TOKEN_FILE", dir.join("relay-token"));
    let stop = Arc::new(AtomicBool::new(false));
    let handle = {
        let endpoint = endpoint.clone();
        let store = Arc::clone(&store);
        let stop = Arc::clone(&stop);
        let token = token.clone();
        std::thread::spawn(move || {
            let _ = serve(&endpoint, store, stop, true, token);
        })
    };
    (stop, handle)
}

fn post(port: u16, path: &str, auth: Option<&str>, body: &str) -> (u16, Vec<u8>) {
    let mut stream =
        std::net::TcpStream::connect(("127.0.0.1", port)).expect("relay must be listening");
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let auth_line = auth
        .map(|token| format!("X-Hub-Auth: {token}\r\n"))
        .unwrap_or_default();
    let head = format!(
        "POST {path} HTTP/1.1\r\nHost: 127.0.0.1\r\n{auth_line}Content-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(head.as_bytes()).unwrap();
    stream.write_all(body.as_bytes()).unwrap();
    let mut reader = BufReader::new(stream);
    let mut status = String::new();
    reader.read_line(&mut status).unwrap();
    let status: u16 = status
        .split_whitespace()
        .nth(1)
        .and_then(|code| code.parse().ok())
        .unwrap_or(0);
    let mut content_length = 0usize;
    loop {
        let mut line = String::new();
        reader.read_line(&mut line).unwrap();
        let line = line.trim_end();
        if line.is_empty() {
            break;
        }
        if let Some(value) = line
            .split_once(':')
            .filter(|(name, _)| name.eq_ignore_ascii_case("content-length"))
            .and_then(|(_, value)| value.trim().parse().ok())
        {
            content_length = value;
        }
    }
    let mut body_bytes = vec![0u8; content_length];
    reader.read_exact(&mut body_bytes).unwrap();
    (status, body_bytes)
}

fn wait_port(port: u16) {
    for _ in 0..100 {
        if std::net::TcpStream::connect(("127.0.0.1", port)).is_ok() {
            return;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    panic!("relay never listened on {port}");
}

#[test]
fn http_mode_round_trips_and_enforces_auth() {
    let dir = temp_dir("round");
    let port = 29531;
    let (stop, handle) = start_http_relay(&dir, port);
    wait_port(port);
    let token = Token::from_hex(TOKEN_HEX).unwrap();

    let list_body = serde_json::json!({
        "id": 1,
        "method": "list_instances",
        "params": {"now": 0, "aliveMs": 30_000}
    })
    .to_string();

    // No token -> 401.
    let (status, _) = post(port, "/hub-rpc", None, &list_body);
    assert_eq!(status, 401);
    // Wrong token -> 401.
    let (status, _) = post(port, "/hub-rpc", Some(&"0".repeat(32)), &list_body);
    assert_eq!(status, 401);
    // Unknown path -> 404 (the real service on a shared port is untouched).
    let (status, body) = post(port, "/metrics", Some(TOKEN_HEX), &list_body);
    assert_eq!(status, 404);
    assert_eq!(body, b"not found");

    // Correct token -> full RPC.
    let (status, body) = post(port, "/hub-rpc", Some(TOKEN_HEX), &list_body);
    assert_eq!(status, 200);
    let response: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(response["id"], 1);
    assert!(response["result"].is_array());

    // The client transport speaks the same protocol.
    let client = RemoteStore::with_http(
        Endpoint::Tcp("127.0.0.1".into(), port),
        token.clone(),
        true,
    );
    let registered = client
        .register_instance(
            &crate::store::InstanceRecord {
                node_id: "n1".into(),
                name: "home".into(),
                host: "alpha".into(),
                pid: 1,
                cwd: "/work".into(),
                role: "normal".into(),
                epoch: "e1".into(),
                session_id: None,
                title: None,
                model: None,
                busy: false,
                started_at: 0,
                last_seen: 1,
            },
            30_000,
        )
        .unwrap();
    assert_eq!(registered.name, "home");
    assert!(matches!(
        client
            .claim_lease("wechat", "n1", "home", "e1", 1_000_000, 20_000)
            .unwrap(),
        Claim::Granted { .. }
    ));
    let envelope = Envelope::text("n1", "home", "n2", "hi", 1_000_000).unwrap();
    assert!(client.send_envelope(&envelope).unwrap());
    assert_eq!(client.inbox("n2", 1_000_001, 10).unwrap().len(), 1);

    stop.store(true, Ordering::SeqCst);
    let _ = handle.join();
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn a_foreign_http_server_is_untouched_in_shared_deployments() {
    // The relay answers 404 for everything that is not /hub-rpc; in the real
    // shared-port deployment the front service keeps its own paths and the
    // reverse proxy maps only /hub-rpc to us. Assert the 404 contract again in
    // isolation so a proxy mis-route is visible.
    let dir = temp_dir("404");
    let port = 29532;
    let (stop, handle) = start_http_relay(&dir, port);
    wait_port(port);
    let (status, body) = post(
        port,
        "/",
        Some(TOKEN_HEX),
        "GET / HTTP/1.1",
    );
    assert_eq!(status, 404);
    assert_eq!(body, b"not found");
    stop.store(true, Ordering::SeqCst);
    let _ = handle.join();
    let _ = std::fs::remove_dir_all(&dir);
}
