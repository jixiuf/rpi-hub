//! QR helpers: base64 → PNG bytes (saved to a file and opened with the system
//! viewer; ASCII QR in the TUI is unreadable and wrecks the layout).

/// Decode a base64 payload into raw PNG bytes.
pub fn decode_base64(text: &str) -> Option<Vec<u8>> {
    crate::ilink::base64_decode_public(text)
}
