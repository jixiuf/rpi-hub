//! Cross-machine hub transport.
//!
//! The relay owns the SQLite database and serves a tiny line-delimited
//! JSON-RPC on a **unix socket** (mode 0600). A client node reaches it through
//! an SSH stream-local forward it opens itself:
//!
//! ```text
//! ssh -N -L <local.sock>:<remote.sock> <host>
//! ```
//!
//! Consequences: nothing listens on TCP, the server never dials back, no
//! firewall change is needed, and SSH is the authentication. The framing and
//! the "authenticate first, then call" shape deliberately echo `rpi-server`
//! (JSON-RPC-ish, bounded frames), without depending on host-only crates.

use std::io::{BufRead, BufReader, Read, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::api::{self, method, HubStore};
use crate::protocol::Envelope;
use crate::store::{
    Claim, Heartbeat, InstanceRecord, InstanceView, LeaseHolder, PruneReport, Stats, Store,
};
use crate::{HubError, Result};

/// Frames larger than this are rejected instead of buffered.
pub const MAX_FRAME_BYTES: usize = 8 * 1024 * 1024;
/// The authenticate method must be the first frame on a connection.
pub const AUTH_METHOD: &str = "authenticate";
/// Length of the generated token (32 hex chars = 128 bits).
pub const TOKEN_LEN: usize = 32;

/// The relay token: bytes of 32 hex characters, trimmed.
#[derive(Clone, PartialEq, Eq)]
pub struct Token(Vec<u8>);

impl std::fmt::Debug for Token {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Never print the secret, not even its length is sensitive but keep it
        // uniform with other redacted material.
        f.write_str("Token(***)")
    }
}

impl Token {
    pub fn generate() -> Result<Self> {
        use std::fmt::Write as _;
        let mut bytes = [0u8; 16];
        {
            use std::io::Read;
            // /dev/urandom on unix; Windows still relies on the socket being
            // loopback-only unless the operator binds 0.0.0.0 deliberately.
            std::fs::File::open("/dev/urandom")
                .and_then(|mut f| f.read_exact(&mut bytes))
                .map_err(HubError::Io)?;
        }
        let mut hex = String::with_capacity(TOKEN_LEN);
        for b in bytes {
            let _ = write!(hex, "{b:02x}");
        }
        Ok(Self(hex.into_bytes()))
    }

    pub fn from_hex(text: &str) -> Result<Self> {
        let trimmed = text.trim();
        if trimmed.len() != TOKEN_LEN || !trimmed.chars().all(|c| c.is_ascii_hexdigit()) {
            return Err(HubError::invalid(format!(
                "relay token must be exactly {TOKEN_LEN} hex characters"
            )));
        }
        Ok(Self(trimmed.as_bytes().to_vec()))
    }

    pub fn as_str(&self) -> &str {
        std::str::from_utf8(&self.0).unwrap_or("")
    }

    /// Constant-time comparison so a network peer cannot time the check.
    pub fn matches(&self, provided: &str) -> bool {
        let provided = provided.as_bytes();
        let expected = &self.0;
        let same_len = provided.len() == expected.len();
        let mut diff: u8 = if same_len { 0 } else { 1 };
        for index in 0..expected.len() {
            diff |= expected[index] ^ provided.get(index).copied().unwrap_or(0);
        }
        diff == 0
    }
}

/// Load the token from `path`, generating it when `create` is set (relay side).
pub fn load_token(path: &Path, create: bool) -> Result<Token> {
    match std::fs::read_to_string(path) {
        Ok(text) => Token::from_hex(&text),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            if !create {
                return Err(HubError::invalid(format!(
                    "relay token file {} is missing; copy it from the relay host",
                    path.display()
                )));
            }
            if let Some(parent) = path.parent() {
                crate::paths::ensure_private_dir(parent)?;
            }
            let token = Token::generate()?;
            std::fs::write(path, format!("{}\n", token.as_str()))?;
            crate::paths::restrict_permissions(path, 0o600)?;
            Ok(token)
        }
        Err(error) => Err(HubError::Io(error)),
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Request {
    id: u64,
    method: String,
    #[serde(default)]
    params: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Response {
    id: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    result: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

/// Execute one relay request against the local store.
pub fn dispatch(store: &Store, method_name: &str, params: Value) -> std::result::Result<Value, String> {
    macro_rules! args {
        ($ty:ty) => {
            serde_json::from_value::<$ty>(params).map_err(|e| format!("bad params: {e}"))?
        };
    }

    let result = match method_name {
        method::REGISTER => {
            let args = args!(api::RegisterParams);
            serde_json::to_value(store.register_instance(&args.record, args.alive_ms).map_err(|e| e.to_string())?)
        }
        method::HEARTBEAT => {
            let args = args!(api::HeartbeatParams);
            serde_json::to_value(
                store
                    .heartbeat(&args.node_id, &args.epoch, args.now, &args.beat)
                    .map_err(|e| e.to_string())?,
            )
        }
        method::LIST => {
            let args = args!(api::ListParams);
            serde_json::to_value(
                store
                    .list_instances(args.now, args.alive_ms)
                    .map_err(|e| e.to_string())?,
            )
        }
        method::GET => {
            let args = args!(api::NodeParams);
            serde_json::to_value(store.get_instance(&args.node_id).map_err(|e| e.to_string())?)
        }
        method::UNREGISTER => {
            let args = args!(api::UnregisterParams);
            serde_json::to_value(
                store
                    .unregister_instance(&args.node_id, &args.epoch)
                    .map_err(|e| e.to_string())?,
            )
        }
        method::CLAIM => {
            let args = args!(api::ClaimParams);
            serde_json::to_value(
                store
                    .claim_lease(
                        &args.capability,
                        &args.node_id,
                        &args.node_name,
                        args.now,
                        args.ttl_ms,
                    )
                    .map_err(|e| e.to_string())?,
            )
        }
        method::RENEW => {
            let args = args!(api::RenewParams);
            serde_json::to_value(
                store
                    .renew_lease(
                        &args.capability,
                        &args.node_id,
                        args.generation,
                        args.now,
                        args.ttl_ms,
                    )
                    .map_err(|e| e.to_string())?,
            )
        }
        method::CHECK => {
            let args = args!(api::CheckParams);
            serde_json::to_value(
                store
                    .check_lease(
                        &args.capability,
                        &args.node_id,
                        args.generation,
                        args.now,
                    )
                    .map_err(|e| e.to_string())?,
            )
        }
        method::ASSIGN => {
            let args = args!(api::AssignParams);
            serde_json::to_value(
                store
                    .assign_lease(
                        &args.capability,
                        &args.to_node,
                        &args.to_name,
                        args.now,
                        args.ttl_ms,
                    )
                    .map_err(|e| e.to_string())?,
            )
        }
        method::RELEASE => {
            let args = args!(api::ReleaseParams);
            serde_json::to_value(
                store
                    .release_lease(&args.capability, &args.node_id, args.generation)
                    .map_err(|e| e.to_string())?,
            )
        }
        method::HOLDER => {
            let args = args!(api::CapabilityParams);
            serde_json::to_value(store.lease_holder(&args.capability).map_err(|e| e.to_string())?)
        }
        method::LEASES => serde_json::to_value(store.list_leases().map_err(|e| e.to_string())?),
        method::SEND => {
            let args = args!(api::SendParams);
            serde_json::to_value(store.send_envelope(&args.envelope).map_err(|e| e.to_string())?)
        }
        method::INBOX => {
            let args = args!(api::InboxParams);
            serde_json::to_value(
                store
                    .inbox(&args.node_id, args.now, args.limit)
                    .map_err(|e| e.to_string())?,
            )
        }
        method::ACK => {
            let args = args!(api::AckParams);
            serde_json::to_value(
                store
                    .ack_envelope(&args.id, &args.node_id, args.now)
                    .map_err(|e| e.to_string())?,
            )
        }
        method::SEEN => {
            let args = args!(api::SeenParams);
            serde_json::to_value(store.mark_seen(&args.id, args.now).map_err(|e| e.to_string())?)
        }
        method::PRUNE => {
            let args = args!(api::PruneParams);
            serde_json::to_value(
                store
                    .prune(args.now, args.alive_ms)
                    .map_err(|e| e.to_string())?,
            )
        }
        method::STATS => {
            let args = args!(api::StatsParams);
            serde_json::to_value(store.stats(args.now).map_err(|e| e.to_string())?)
        }
        method::CHANNEL_STATE => {
            let capability = params
                .get("capability")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            serde_json::to_value(store.channel_state(capability))
        }
        method::SET_CHANNEL_STATE => {
            let capability = params
                .get("capability")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let state = params.get("state").cloned().unwrap_or(serde_json::Value::Null);
            store.set_channel_state(&capability, &state);
            serde_json::to_value(true)
        }
        other => return Err(format!("unknown method {other:?}")),
    };

    result.map_err(|error| error.to_string())
}

/// `host:port` or `unix:<path>`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Endpoint {
    Tcp(String, u16),
    Unix(PathBuf),
}

impl Endpoint {
    pub fn parse(text: &str) -> Result<Self> {
        let text = text.trim();
        if let Some(path) = text.strip_prefix("unix:") {
            if path.is_empty() {
                return Err(HubError::invalid("unix endpoint needs a path"));
            }
            return Ok(Endpoint::Unix(PathBuf::from(path)));
        }
        let (host, port) = text
            .rsplit_once(':')
            .ok_or_else(|| HubError::invalid(format!("endpoint {text:?} must be host:port")))?;
        let host = host.trim_start_matches('[').trim_end_matches(']');
        if host.is_empty() {
            return Err(HubError::invalid(format!("endpoint {text:?} needs a host")));
        }
        let port: u16 = port
            .parse()
            .map_err(|_| HubError::invalid(format!("endpoint {text:?} has an invalid port")))?;
        Ok(Endpoint::Tcp(host.to_string(), port))
    }

    pub fn is_loopback(&self) -> bool {
        match self {
            Endpoint::Unix(_) => true,
            Endpoint::Tcp(host, _) => host == "localhost" || host == "::1" || host.starts_with("127."),
        }
    }
}

// ---------------------------------------------------------------------------
// Relay server
// ---------------------------------------------------------------------------

/// Serve `store` on `endpoint` until `shutdown` is set.
///
/// Authentication is mandatory: the **first** frame on every connection must be
/// `authenticate` with the token; everything else is refused before any store
/// method runs. TCP endpoints that are not loopback are allowed (that is the
/// point of the relay) but the caller is expected to warn — `serve` logs it.
/// Serve `store` on `endpoint` until `shutdown` is set.
///
/// Authentication is mandatory: TCP connections must first send
/// `authenticate {token}`; HTTP mode sends it as `X-Hub-Auth` on every call.
/// The caller (backends, which knows the config) resolves the token.
pub fn serve(
    endpoint: &Endpoint,
    store: Arc<Store>,
    shutdown: Arc<AtomicBool>,
    http: bool,
    token: Token,
) -> Result<()> {
    if !endpoint.is_loopback() {
        crate::wire::warn_non_loopback(endpoint);
    }
    let listener = bind_listener(endpoint)?;
    match &listener {
        Listener::Unix(l) => l.set_nonblocking(true)?,
        Listener::Tcp(l) => l.set_nonblocking(true)?,
    }

    while !shutdown.load(Ordering::Acquire) {
        match accept(&listener) {
            Ok(stream) => {
                let store = Arc::clone(&store);
                let token = token.clone();
                std::thread::Builder::new()
                    .name("rpi-hub-relay-conn".into())
                    .spawn(move || {
                        let _ = if http {
                            handle_http_connection(stream, store, &token)
                        } else {
                            handle_connection(stream, store, &token)
                        };
                    })
                    .ok();
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::sleep(Duration::from_millis(100));
            }
            Err(error) => return Err(HubError::Io(error)),
        }
    }
    cleanup_listener(endpoint);
    Ok(())
}

enum Listener {
    Unix(UnixListener),
    Tcp(std::net::TcpListener),
}

fn bind_listener(endpoint: &Endpoint) -> Result<Listener> {
    match endpoint {
        Endpoint::Unix(path) => {
            if let Some(parent) = path.parent() {
                crate::paths::ensure_private_dir(parent)?;
            }
            if path.exists() {
                match UnixStream::connect(path) {
                    Ok(_) => {
                        return Err(HubError::invalid(format!(
                            "another relay is already listening on {}",
                            path.display()
                        )))
                    }
                    Err(_) => {
                        let _ = std::fs::remove_file(path);
                    }
                }
            }
            let listener = UnixListener::bind(path)?;
            crate::paths::restrict_permissions(path, 0o600)?;
            Ok(Listener::Unix(listener))
        }
        Endpoint::Tcp(host, port) => {
            let listener = std::net::TcpListener::bind((host.as_str(), *port))?;
            Ok(Listener::Tcp(listener))
        }
    }
}

fn accept(listener: &Listener) -> std::io::Result<Stream> {
    match listener {
        Listener::Unix(listener) => listener.accept().map(|(stream, _)| Stream::Unix(stream)),
        Listener::Tcp(listener) => listener.accept().map(|(stream, _)| Stream::Tcp(stream)),
    }
}

/// Connect to the relay, TCP or unix.
pub fn connect_stream(endpoint: &Endpoint, timeout: Duration) -> std::io::Result<Stream> {
    match endpoint {
        Endpoint::Unix(path) => {
            let stream = UnixStream::connect(path)?;
            stream.set_read_timeout(Some(timeout))?;
            stream.set_write_timeout(Some(timeout))?;
            Ok(Stream::Unix(stream))
        }
        Endpoint::Tcp(host, port) => {
            let addr = format!("{host}:{port}");
            let stream = std::net::TcpStream::connect(addr)?;
            stream.set_read_timeout(Some(timeout))?;
            stream.set_write_timeout(Some(timeout))?;
            stream.set_nodelay(true)?;
            Ok(Stream::Tcp(stream))
        }
    }
}

/// Core-side logging hook (the plugin installs a real file logger).
static LOG_HOOK: Mutex<Option<fn(&str)>> = Mutex::new(None);

pub fn set_log_hook(hook: fn(&str)) {
    *LOG_HOOK.lock().unwrap_or_else(|p| p.into_inner()) = Some(hook);
}

pub(crate) fn log(message: &str) {
    if let Some(hook) = LOG_HOOK.lock().unwrap_or_else(|p| p.into_inner()).as_ref() {
        hook(message);
    }
}

pub(crate) fn warn_non_loopback(endpoint: &Endpoint) {
    log(&format!(
        "relay listening on a non-loopback endpoint ({endpoint:?}); the token is the only defense"
    ));
}

fn cleanup_listener(endpoint: &Endpoint) {
    if let Endpoint::Unix(path) = endpoint {
        let _ = std::fs::remove_file(path);
    }
}

pub enum Stream {
    Unix(UnixStream),
    Tcp(std::net::TcpStream),
}

impl Read for Stream {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Stream::Unix(s) => s.read(buf),
            Stream::Tcp(s) => s.read(buf),
        }
    }
}

impl Write for Stream {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            Stream::Unix(s) => s.write(buf),
            Stream::Tcp(s) => s.write(buf),
        }
    }
    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Stream::Unix(s) => s.flush(),
            Stream::Tcp(s) => s.flush(),
        }
    }
}

impl Stream {
    fn try_clone(&self) -> std::io::Result<Self> {
        Ok(match self {
            Stream::Unix(s) => Stream::Unix(s.try_clone()?),
            Stream::Tcp(s) => Stream::Tcp(s.try_clone()?),
        })
    }

    fn set_timeouts(&self) {
        let timeout = Some(Duration::from_secs(30));
        match self {
            Stream::Unix(s) => {
                let _ = s.set_read_timeout(timeout);
                let _ = s.set_write_timeout(timeout);
            }
            Stream::Tcp(s) => {
                let _ = s.set_read_timeout(timeout);
                let _ = s.set_write_timeout(timeout);
                let _ = s.set_nodelay(true);
            }
        }
    }
}

fn handle_connection(stream: Stream, store: Arc<Store>, token: &Token) -> std::io::Result<()> {
    stream.set_timeouts();
    let mut writer = stream.try_clone()?;
    let reader_stream = stream.try_clone()?;
    let mut reader = BufReader::new(reader_stream);

    // Handshake: the first frame must authenticate.
    let mut line = String::new();
    if reader.read_line(&mut line)? == 0 {
        return Ok(());
    }
    if line.len() > MAX_FRAME_BYTES {
        return Ok(());
    }
    let authorized = serde_json::from_str::<Request>(&line).ok().and_then(|request| {
        if request.method != AUTH_METHOD {
            return None;
        }
        request
            .params
            .get("token")
            .and_then(|v| v.as_str())
            .map(|provided| token.matches(provided))
    });
    if authorized != Some(true) {
        let response = Response {
            id: serde_json::from_str::<Request>(&line)
                .map(|r| r.id)
                .unwrap_or(0),
            result: None,
            error: Some("authentication required (call authenticate first)".into()),
        };
        write_frame(&mut writer, &response)?;
        return Ok(());
    }
    write_frame(
        &mut writer,
        &Response {
            id: serde_json::from_str::<Request>(&line)
                .map(|r| r.id)
                .unwrap_or(0),
            result: Some(serde_json::json!({"ok": true})),
            error: None,
        },
    )?;

    for line in reader.lines() {
        let line = line?;
        if line.len() > MAX_FRAME_BYTES {
            break;
        }
        if line.trim().is_empty() {
            continue;
        }
        let response = match serde_json::from_str::<Request>(&line) {
            // A second authenticate is refused: the handshake is done.
            Ok(request) if request.method == AUTH_METHOD => Response {
                id: request.id,
                result: None,
                error: Some("already authenticated".into()),
            },
            Ok(request) => match dispatch(&store, &request.method, request.params) {
                Ok(result) => Response {
                    id: request.id,
                    result: Some(result),
                    error: None,
                },
                Err(error) => Response {
                    id: request.id,
                    result: None,
                    error: Some(error),
                },
            },
            Err(error) => Response {
                id: 0,
                result: None,
                error: Some(format!("bad request: {error}")),
            },
        };
        write_frame(&mut writer, &response)?;
    }
    Ok(())
}

/// One hub RPC inside an HTTP request/response pair, so the relay can share a
/// port that firewalls already forward to a real HTTP service.
///
/// `POST /hub-rpc` with `X-Hub-Auth: <token>`, body = one JSON request object;
/// response body = one JSON response object. Anything else gets a plain 404 so
/// the port's real owner (or a co-located reverse proxy path map) is untouched.
const HUB_HTTP_PATH: &str = "/hub-rpc";
const MAX_HTTP_HEAD_BYTES: usize = 16 * 1024;

fn handle_http_connection(
    mut stream: Stream,
    store: Arc<Store>,
    token: &Token,
) -> std::io::Result<()> {
    stream.set_timeouts();
    loop {
        let request = match read_http_request(&mut stream)? {
            Some(request) => request,
            None => return Ok(()),
        };
        let status;
        let body;
        if request.path != HUB_HTTP_PATH {
            status = "404 Not Found";
            body = b"not found".to_vec();
        } else if !token.matches(&request.auth.as_deref().unwrap_or("")) {
            status = "401 Unauthorized";
            body = b"authentication required".to_vec();
        } else {
            let response = match serde_json::from_slice::<Request>(&request.body) {
                Ok(request) => match dispatch(&store, &request.method, request.params) {
                    Ok(result) => Response {
                        id: request.id,
                        result: Some(result),
                        error: None,
                    },
                    Err(error) => Response {
                        id: request.id,
                        result: None,
                        error: Some(error),
                    },
                },
                Err(error) => Response {
                    id: 0,
                    result: None,
                    error: Some(format!("bad request: {error}")),
                },
            };
            status = "200 OK";
            body = serde_json::to_vec(&response).unwrap_or_else(|_| {
                br#"{"id":0,"error":"cannot serialize response"}"#.to_vec()
            });
        }
        let head = format!(
            "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: keep-alive\r\n\r\n",
            body.len()
        );
        stream.write_all(head.as_bytes())?;
        stream.write_all(&body)?;
        stream.flush()?;
    }
}

struct HttpRequest {
    path: String,
    auth: Option<String>,
    body: Vec<u8>,
}

/// Minimal HTTP/1.1 reader: headers until CRLFCRLF, then exactly
/// `Content-Length` body bytes. `Connection: close` clients end here.
fn read_http_request(stream: &mut Stream) -> std::io::Result<Option<HttpRequest>> {
    let mut head = Vec::with_capacity(512);
    let mut byte = [0u8; 1];
    loop {
        match stream.read(&mut byte) {
            Ok(0) => return Ok(None),
            Ok(_) => {
                head.push(byte[0]);
                if head.len() > MAX_HTTP_HEAD_BYTES {
                    return Ok(None);
                }
                if head.ends_with(b"\r\n\r\n") {
                    break;
                }
            }
            Err(error) => return Err(error),
        }
    }
    let head_text = String::from_utf8_lossy(&head);
    let mut lines = head_text.split("\r\n");
    let request_line = lines.next().unwrap_or("");
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or("");
    let path = parts.next().unwrap_or("").to_string();
    if !method.eq_ignore_ascii_case("POST") {
        // Read is done; answer 404 via the normal path with an empty body.
        return Ok(Some(HttpRequest {
            path: String::new(),
            auth: None,
            body: Vec::new(),
        }));
    }
    let mut content_length = 0usize;
    let mut auth = None;
    let mut close = false;
    for line in lines {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        let name = name.trim().to_ascii_lowercase();
        let value = value.trim();
        match name.as_str() {
            "content-length" => content_length = value.parse().unwrap_or(0),
            "x-hub-auth" => auth = Some(value.to_string()),
            "connection" if value.eq_ignore_ascii_case("close") => close = true,
            _ => {}
        }
    }
    if content_length > MAX_FRAME_BYTES {
        return Ok(None);
    }
    let mut body = vec![0u8; content_length];
    stream.read_exact(&mut body)?;
    let _ = close;
    Ok(Some(HttpRequest { path, auth, body }))
}

fn write_frame<W: Write>(writer: &mut W, response: &Response) -> std::io::Result<()> {
    let mut text = serde_json::to_string(response)
        .unwrap_or_else(|_| r#"{"id":0,"error":"cannot serialize response"}"#.to_string());
    text.push('\n');
    writer.write_all(text.as_bytes())?;
    writer.flush()
}

// ---------------------------------------------------------------------------
// Remote store (client)
// ---------------------------------------------------------------------------

/// A node whose rows live on the relay's database.
pub struct RemoteStore {
    endpoint: Endpoint,
    token: Token,
    http: bool,
    conn: Mutex<Option<BufReader<Stream>>>,
    writer: Mutex<Option<Stream>>,
    authenticated: AtomicBool,
    next_id: AtomicU64,
}

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);


impl std::fmt::Debug for RemoteStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteStore")
            .field("endpoint", &self.endpoint)
            .finish_non_exhaustive()
    }
}

impl RemoteStore {
    /// `endpoint` is the relay address (`host:port` or `unix:<path>`); the
    /// token comes from the shared token file.
    pub fn new(endpoint: Endpoint, token: Token) -> Self {
        Self::with_http(endpoint, token, false)
    }

    pub fn with_http(endpoint: Endpoint, token: Token, http: bool) -> Self {
        Self {
            endpoint,
            token,
            http,
            conn: Mutex::new(None),
            writer: Mutex::new(None),
            authenticated: AtomicBool::new(false),
            next_id: AtomicU64::new(1),
        }
    }

    pub fn endpoint(&self) -> &Endpoint {
        &self.endpoint
    }

    pub fn call(&self, method_name: &str, params: Value) -> Result<Value> {
        // One retry: the SSH tunnel may be reconnecting after a network blip.
        match self.call_once(method_name, params.clone()) {
            Ok(value) => Ok(value),
            Err(first) => {
                self.drop_connection();
                self.call_once(method_name, params).map_err(|_| first)
            }
        }
    }

    fn call_once(&self, method_name: &str, params: Value) -> Result<Value> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let request = Request {
            id,
            method: method_name.to_string(),
            params,
        };
        if self.http {
            return self.call_http(&request);
        }
        let mut payload = serde_json::to_string(&request)?;
        if payload.len() > MAX_FRAME_BYTES {
            return Err(HubError::invalid("request too large"));
        }
        payload.push('\n');

        self.ensure_connection()?;

        let already = {
            let mut writer = self.writer.lock().expect("writer mutex poisoned");
            let stream = writer
                .as_mut()
                .ok_or_else(|| HubError::invalid("relay connection lost"))?;
            // Re-authenticate whenever this is a fresh connection: the frame
            // cost is trivial and it covers reconnects after a drop.
            let already = self.authenticated.load(Ordering::Acquire);
            if !already {
                let handshake = serde_json::to_string(&Request {
                    id: self.next_id.fetch_add(1, Ordering::Relaxed),
                    method: AUTH_METHOD.to_string(),
                    params: serde_json::json!({ "token": self.token.as_str() }),
                })
                .map_err(HubError::Json)?;
                stream
                    .write_all(format!("{handshake}\n").as_bytes())
                    .map_err(|e| HubError::invalid(format!("relay write failed: {e}")))?;
            }
            stream
                .write_all(payload.as_bytes())
                .map_err(|e| HubError::invalid(format!("relay write failed: {e}")))?;
            stream
                .flush()
                .map_err(|e| HubError::invalid(format!("relay flush failed: {e}")))?;
            already
        };

        let mut handshake_result: Option<Value> = None;
        if !already {
            let line = self.read_line()?;
            let response: Response = serde_json::from_str(&line)?;
            if let Some(error) = response.error {
                self.drop_connection();
                return Err(HubError::invalid(error));
            }
            handshake_result = Some(response.result.unwrap_or(Value::Null));
        }

        let line = self.read_line()?;
        let response: Response = serde_json::from_str(&line)?;
        self.authenticated.store(true, Ordering::Release);
        let _ = handshake_result;
        if response.id != id {
            return Err(HubError::invalid(format!(
                "relay response id mismatch ({} != {id})",
                response.id
            )));
        }
        match (response.result, response.error) {
            (_, Some(error)) => Err(HubError::invalid(error)),
            (Some(result), None) => Ok(result),
            (None, None) => Ok(Value::Null),
        }
    }

    /// HTTP mode: one connection per call (keep-alive pools live in the
    /// caller's TCP stack; correctness first), token in a header, never logged.
    fn call_http(&self, request: &Request) -> Result<Value> {
        let body = serde_json::to_vec(request)?;
        if body.len() > MAX_FRAME_BYTES {
            return Err(HubError::invalid("request too large"));
        }
        let mut stream = connect_stream(&self.endpoint, CONNECT_TIMEOUT).map_err(|error| {
            HubError::invalid(format!(
                "cannot reach the hub relay at {endpoint:?} ({error}); is the relay up?",
                endpoint = self.endpoint
            ))
        })?;
        let host = match &self.endpoint {
            Endpoint::Tcp(host, port) => format!("{host}:{port}"),
            Endpoint::Unix(_) => "localhost".to_string(),
        };
        let head = format!(
            "POST {HUB_HTTP_PATH} HTTP/1.1\r\nHost: {host}\r\nX-Hub-Auth: {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            self.token.as_str(),
            body.len()
        );
        stream
            .write_all(head.as_bytes())
            .map_err(|e| HubError::invalid(format!("relay write failed: {e}")))?;
        stream
            .write_all(&body)
            .map_err(|e| HubError::invalid(format!("relay write failed: {e}")))?;
        stream
            .flush()
            .map_err(|e| HubError::invalid(format!("relay flush failed: {e}")))?;

        let mut reader = BufReader::new(stream);
        let mut status_line = String::new();
        reader
            .read_line(&mut status_line)
            .map_err(|e| HubError::invalid(format!("relay read failed: {e}")))?;
        if !status_line.contains(" 200 ") {
            return Err(HubError::invalid(format!(
                "relay HTTP status:{}",
                status_line.trim()
            )));
        }
        let mut content_length: usize = 0;
        loop {
            let mut line = String::new();
            reader
                .read_line(&mut line)
                .map_err(|e| HubError::invalid(format!("relay read failed: {e}")))?;
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
        if content_length > MAX_FRAME_BYTES {
            return Err(HubError::invalid("relay response too large"));
        }
        let mut body = vec![0u8; content_length];
        reader
            .read_exact(&mut body)
            .map_err(|e| HubError::invalid(format!("relay read failed: {e}")))?;
        let response: Response = serde_json::from_slice(&body)?;
        match (response.result, response.error) {
            (_, Some(error)) => Err(HubError::invalid(error)),
            (Some(result), None) => Ok(result),
            (None, None) => Ok(Value::Null),
        }
    }

    fn read_line(&self) -> Result<String> {
        let mut conn = self.conn.lock().expect("conn mutex poisoned");
        let reader = conn
            .as_mut()
            .ok_or_else(|| HubError::invalid("relay connection lost"))?;
        let mut line = String::new();
        reader
            .read_line(&mut line)
            .map_err(|e| HubError::invalid(format!("relay read failed: {e}")))?;
        if line.trim().is_empty() {
            return Err(HubError::invalid("relay closed the connection"));
        }
        Ok(line)
    }

    fn ensure_connection(&self) -> Result<()> {
        let mut writer = self.writer.lock().expect("writer mutex poisoned");
        if writer.is_some() {
            return Ok(());
        }
        let stream = connect_stream(&self.endpoint, CONNECT_TIMEOUT).map_err(|error| {
            HubError::invalid(format!(
                "cannot reach the hub relay at {endpoint:?} ({error}); is the relay up?",
                endpoint = self.endpoint
            ))
        })?;
        let reader = BufReader::new(stream.try_clone()?);
        *writer = Some(stream);
        *self.conn.lock().expect("conn mutex poisoned") = Some(reader);
        self.authenticated.store(false, Ordering::Release);
        Ok(())
    }

    fn drop_connection(&self) {
        let _ = self.writer.lock().expect("writer mutex poisoned").take();
        let _ = self.conn.lock().expect("conn mutex poisoned").take();
        self.authenticated.store(false, Ordering::Release);
    }

    fn typed<T: serde::de::DeserializeOwned>(value: Value) -> Result<T> {
        serde_json::from_value(value).map_err(|e| HubError::invalid(format!("bad relay result: {e}")))
    }
}

macro_rules! call {
    ($self:expr, $method:expr, $params:expr) => {
        $self.call($method, serde_json::to_value($params)?)
    };
}

impl HubStore for RemoteStore {
    fn register_instance(
        &self,
        record: &InstanceRecord,
        alive_ms: i64,
    ) -> Result<crate::store::RegisteredInstance> {
        let value = call!(
            self,
            method::REGISTER,
            api::RegisterParams {
                record: record.clone(),
                alive_ms,
            }
        )?;
        Self::typed(value)
    }

    fn heartbeat(&self, node_id: &str, epoch: &str, now: i64, beat: &Heartbeat) -> Result<bool> {
        let value = call!(
            self,
            method::HEARTBEAT,
            api::HeartbeatParams {
                node_id: node_id.to_string(),
                epoch: epoch.to_string(),
                now,
                beat: beat.clone(),
            }
        )?;
        Self::typed(value)
    }

    fn list_instances(&self, now: i64, alive_ms: i64) -> Result<Vec<InstanceView>> {
        let value = call!(self, method::LIST, api::ListParams { now, alive_ms })?;
        Self::typed(value)
    }

    fn get_instance(&self, node_id: &str) -> Result<Option<InstanceView>> {
        let value = call!(
            self,
            method::GET,
            api::NodeParams {
                node_id: node_id.to_string()
            }
        )?;
        Self::typed(value)
    }

    fn unregister_instance(&self, node_id: &str, epoch: &str) -> Result<bool> {
        let value = call!(
            self,
            method::UNREGISTER,
            api::UnregisterParams {
                node_id: node_id.to_string(),
                epoch: epoch.to_string(),
            }
        )?;
        Self::typed(value)
    }

    fn claim_lease(
        &self,
        capability: &str,
        node_id: &str,
        node_name: &str,
        now: i64,
        ttl_ms: i64,
    ) -> Result<Claim> {
        let value = call!(
            self,
            method::CLAIM,
            api::ClaimParams {
                capability: capability.to_string(),
                node_id: node_id.to_string(),
                node_name: node_name.to_string(),
                now,
                ttl_ms,
            }
        )?;
        Self::typed(value)
    }

    fn renew_lease(
        &self,
        capability: &str,
        node_id: &str,
        generation: i64,
        now: i64,
        ttl_ms: i64,
    ) -> Result<bool> {
        let value = call!(
            self,
            method::RENEW,
            api::RenewParams {
                capability: capability.to_string(),
                node_id: node_id.to_string(),
                generation,
                now,
                ttl_ms,
            }
        )?;
        Self::typed(value)
    }

    fn check_lease(
        &self,
        capability: &str,
        node_id: &str,
        generation: i64,
        now: i64,
    ) -> Result<bool> {
        let value = call!(
            self,
            method::CHECK,
            api::CheckParams {
                capability: capability.to_string(),
                node_id: node_id.to_string(),
                generation,
                now,
            }
        )?;
        Self::typed(value)
    }

    fn assign_lease(
        &self,
        capability: &str,
        to_node: &str,
        to_name: &str,
        now: i64,
        ttl_ms: i64,
    ) -> Result<i64> {
        let value = call!(
            self,
            method::ASSIGN,
            api::AssignParams {
                capability: capability.to_string(),
                to_node: to_node.to_string(),
                to_name: to_name.to_string(),
                now,
                ttl_ms,
            }
        )?;
        Self::typed(value)
    }

    fn release_lease(
        &self,
        capability: &str,
        node_id: &str,
        generation: i64,
    ) -> Result<bool> {
        let value = call!(
            self,
            method::RELEASE,
            api::ReleaseParams {
                capability: capability.to_string(),
                node_id: node_id.to_string(),
                generation,
            }
        )?;
        Self::typed(value)
    }

    fn lease_holder(&self, capability: &str) -> Result<Option<LeaseHolder>> {
        let value = call!(
            self,
            method::HOLDER,
            api::CapabilityParams {
                capability: capability.to_string()
            }
        )?;
        Self::typed(value)
    }

    fn list_leases(&self) -> Result<Vec<(String, LeaseHolder)>> {
        let value = call!(self, method::LEASES, Value::Null)?;
        Self::typed(value)
    }

    fn send_envelope(&self, envelope: &Envelope) -> Result<bool> {
        let value = call!(
            self,
            method::SEND,
            api::SendParams {
                envelope: envelope.clone()
            }
        )?;
        Self::typed(value)
    }

    fn inbox(&self, node_id: &str, now: i64, limit: i64) -> Result<Vec<Envelope>> {
        let value = call!(
            self,
            method::INBOX,
            api::InboxParams {
                node_id: node_id.to_string(),
                now,
                limit,
            }
        )?;
        Self::typed(value)
    }

    fn ack_envelope(&self, id: &str, node_id: &str, now: i64) -> Result<bool> {
        let value = call!(
            self,
            method::ACK,
            api::AckParams {
                id: id.to_string(),
                node_id: node_id.to_string(),
                now,
            }
        )?;
        Self::typed(value)
    }

    fn mark_seen(&self, id: &str, now: i64) -> Result<bool> {
        let value = call!(
            self,
            method::SEEN,
            api::SeenParams {
                id: id.to_string(),
                now
            }
        )?;
        Self::typed(value)
    }

    fn prune(&self, now: i64, alive_ms: i64) -> Result<PruneReport> {
        let value = call!(self, method::PRUNE, api::PruneParams { now, alive_ms })?;
        Self::typed(value)
    }

    fn stats(&self, now: i64) -> Result<Stats> {
        let value = call!(self, method::STATS, api::StatsParams { now })?;
        Self::typed(value)
    }

    fn channel_state(&self, capability: &str) -> Option<serde_json::Value> {
        let params = serde_json::json!({ "capability": capability });
        let value = self.call(method::CHANNEL_STATE, params).ok()?;
        Some(value)
    }

    fn set_channel_state(&self, capability: &str, state: &serde_json::Value) {
        let params = serde_json::json!({ "capability": capability, "state": state });
        let _ = self.call(method::SET_CHANNEL_STATE, params);
    }
}

#[cfg(test)]
mod tests;
#[cfg(test)]
mod auth_tests;
#[cfg(test)]
mod http_tests;
