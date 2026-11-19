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
    /// Filesystem error (media persistence).
    Io(String),
    /// A long poll was aborted on purpose (lease lost mid-poll): not a
    /// failure, the caller just stops; the new owner re-polls the batch.
    Cancelled,
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
            IlinkError::Io(m) => write!(f, "io: {m}"),
            IlinkError::Cancelled => write!(f, "cancelled"),
        }
    }
}

impl std::error::Error for IlinkError {}

impl IlinkError {
    /// `errcode == -14`: the bot session expired and a QR re-login is required.
    pub fn is_session_expired(&self) -> bool {
        matches!(
            self,
            IlinkError::Api {
                code: Some(-14),
                ..
            }
        )
    }
}
