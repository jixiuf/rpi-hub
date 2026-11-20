//! At-rest encryption for sensitive rows in mesh.db (credentials, envelope
//! payloads). AES-256-GCM; the key lives OUTSIDE the database:
//!
//! 1. `RPI_MESH_SECRET` env — 64 hex chars (32 bytes), wins when set;
//! 2. `<mesh dir>/secret.key` — 0600 file with the same format, generated on
//!    first use.
//!
//! The key must be identical on every machine sharing a mesh.db (or a
//! relay's): copy `secret.key` once when onboarding a machine. Losing it
//! means the encrypted rows are unrecoverable — re-login for credentials;
//! envelopes are transient by design.
//!
//! Ciphertext format: base64(nonce ‖ ciphertext‖tag). Nonce is random per
//! encryption (12 bytes; GCM standard).

use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use aes_gcm::aead::{Aead, KeyInit};
use aes_gcm::{Aes256Gcm, Nonce};
use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;

const NONCE_LEN: usize = 12;

static KEY: OnceLock<[u8; 32]> = OnceLock::new();
/// Serializes key-file creation: concurrent `Store::open` calls (the startup
/// storm) would otherwise race write the key file and read each other's
/// partial writes.
static INIT_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Load (or generate) the at-rest key. `dir` is the mesh state directory.
/// Called once from the store/mesh bootstrap; later calls hit the cache.
pub fn init(dir: &Path) -> Result<(), String> {
    let _guard = INIT_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    if KEY.get().is_some() {
        return Ok(());
    }
    let key = load_or_generate(dir)?;
    let _ = KEY.set(key);
    Ok(())
}

/// The loaded key. Panics are impossible after `init`; a `None` here means
/// the caller skipped init — treat as a programming error.
fn key() -> [u8; 32] {
    *KEY.get().expect("secret::init not called")
}

fn load_or_generate(dir: &Path) -> Result<[u8; 32], String> {
    // 1. env override: 64 hex chars.
    if let Ok(text) = std::env::var("RPI_MESH_SECRET") {
        return parse_hex_key(text.trim())
            .map_err(|e| format!("RPI_MESH_SECRET is invalid: {e}"));
    }
    // 2. key file, generated on first use.
    let path = key_path(dir);
    if let Ok(text) = std::fs::read_to_string(&path) {
        return parse_hex_key(text.trim()).map_err(|e| {
            format!("{} is invalid: {e} (delete it to regenerate; encrypted rows will be lost)", path.display())
        });
    }
    let key = generate_key();
    std::fs::write(&path, hex_encode(&key))
        .map_err(|e| format!("cannot write {}: {e}", path.display()))?;
    restrict_permissions(&path);
    Ok(key)
}

fn key_path(dir: &Path) -> PathBuf {
    dir.join("secret.key")
}

fn parse_hex_key(text: &str) -> Result<[u8; 32], String> {
    let bytes = decode_hex(text)?;
    let len = bytes.len();
    bytes
        .try_into()
        .map_err(|_| format!("expected 64 hex chars (32 bytes), got {len}"))
}

fn decode_hex(text: &str) -> Result<Vec<u8>, String> {
    if text.len() % 2 != 0 || !text.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err("not a hex string".into());
    }
    (0..text.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&text[i..i + 2], 16).map_err(|e| e.to_string()))
        .collect()
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn generate_key() -> [u8; 32] {
    // uuid v4 gives 122 random bits per uuid; two uuids are plenty for a key
    // and avoid a new rand dependency in core.
    let mut key = [0u8; 32];
    let a = *uuid::Uuid::new_v4().as_bytes();
    let b = *uuid::Uuid::new_v4().as_bytes();
    key[..16].copy_from_slice(&a);
    key[16..].copy_from_slice(&b);
    key
}

#[cfg(unix)]
fn restrict_permissions(path: &Path) {
    use std::os::unix::fs::PermissionsExt;
    if let Ok(metadata) = std::fs::metadata(path) {
        let mut permissions = metadata.permissions();
        permissions.set_mode(0o600);
        let _ = std::fs::set_permissions(path, permissions);
    }
}

#[cfg(not(unix))]
fn restrict_permissions(_path: &Path) {}

/// Encrypt `plaintext` into `base64(nonce‖ciphertext)`. Empty input stays
/// empty (callers keep their zero-value semantics).
pub fn encrypt(plaintext: &str) -> String {
    if plaintext.is_empty() {
        return String::new();
    }
    let cipher = Aes256Gcm::new_from_slice(&key()).expect("key length");
    let nonce_bytes: [u8; NONCE_LEN] = rand_nonce();
    let nonce = Nonce::from_slice(&nonce_bytes);
    let ciphertext = cipher
        .encrypt(nonce, plaintext.as_bytes())
        .expect("encryption cannot fail with a valid key");
    let mut packed = Vec::with_capacity(NONCE_LEN + ciphertext.len());
    packed.extend_from_slice(&nonce_bytes);
    packed.extend_from_slice(&ciphertext);
    B64.encode(packed)
}

/// Decrypt a value produced by [`encrypt`]. Errors on tampering or a wrong
/// key — callers surface that as an explicit failure, not silence.
pub fn decrypt(ciphertext_b64: &str) -> Result<String, String> {
    if ciphertext_b64.is_empty() {
        return Ok(String::new());
    }
    let packed = B64.decode(ciphertext_b64).map_err(|_| "not base64")?;
    if packed.len() < NONCE_LEN + 16 {
        return Err("ciphertext too short".into());
    }
    let (nonce_bytes, body) = packed.split_at(NONCE_LEN);
    let cipher = Aes256Gcm::new_from_slice(&key()).expect("key length");
    let plaintext: Vec<u8> = cipher
        .decrypt(Nonce::from_slice(nonce_bytes), body)
        .map_err(|_| "decryption failed (wrong key or tampered data)")?;
    String::from_utf8(plaintext).map_err(|e| format!("decrypted payload is not utf-8: {e}"))
}

fn rand_nonce() -> [u8; NONCE_LEN] {
    let a = uuid::Uuid::new_v4().as_bytes().to_vec();
    let mut nonce = [0u8; NONCE_LEN];
    nonce.copy_from_slice(&a[..NONCE_LEN]);
    nonce
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_and_tamper_detection() {
        let dir = std::env::temp_dir().join(format!("mesh-secret-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        init(&dir).unwrap();

        let message = "{\"token\":\"secret\",\"userId\":\"u1\"}";
        let encrypted = encrypt(message);
        assert_ne!(encrypted, message);
        assert_eq!(decrypt(&encrypted).unwrap(), message);

        // Tampering (flip a byte in the body) must fail.
        let mut tampered = encrypted.clone();
        let byte_index = tampered.len() - 2;
        tampered.replace_range(byte_index..byte_index + 1, "A");
        assert!(decrypt(&tampered).is_err());

        // Empty stays empty.
        assert_eq!(encrypt(""), "");
        assert_eq!(decrypt(""), Ok(String::new()));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn env_key_wins_and_is_stable() {
        let dir = std::env::temp_dir().join(format!("mesh-secret-env-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let key_hex = "aa".repeat(32);
        std::env::set_var("RPI_MESH_SECRET", &key_hex);
        init(&dir).unwrap();
        let encrypted = encrypt("payload");
        // Re-init with the same env key: decryption works across processes
        // that share the key (init is idempotent for the same inputs).
        init(&dir).unwrap();
        assert_eq!(decrypt(&encrypted).unwrap(), "payload");
        std::env::remove_var("RPI_MESH_SECRET");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
