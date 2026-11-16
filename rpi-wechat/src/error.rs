//! Errors surfaced by the iLink client.

use std::fmt;

#[derive(Debug)]
pub enum IlinkError {
    /// HTTP/IO failure or malformed envelope.
    Transport(String),
    /// API answered with an error envelope.
    Api {
        label: String,
        status: u16,
        code: Option<i64>,
        message: String,
    },
    /// Unexpected response shape.
    Protocol(String),
}

impl fmt::Display for IlinkError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            IlinkError::Transport(m) => write!(f, "transport: {m}"),
            IlinkError::Api {
                label,
                status,
                code,
                message,
            } => write!(f, "{label} HTTP {status} code={code:?}: {message}"),
            IlinkError::Protocol(m) => write!(f, "protocol: {m}"),
        }
    }
}

impl std::error::Error for IlinkError {}

impl IlinkError {
    /// `errcode == -14`: the bot session expired and a QR re-login is required.
    pub fn is_session_expired(&self) -> bool {
        matches!(self, IlinkError::Api { code: Some(-14), .. })
    }
}
