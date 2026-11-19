//! Error type shared by the hub core.

use std::fmt;

#[derive(Debug)]
pub enum HubError {
    Sqlite(rusqlite::Error),
    Json(serde_json::Error),
    Io(std::io::Error),
    /// Caller error: bad argument, invariant violation, unsupported capability.
    Invalid(String),
}

impl fmt::Display for HubError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HubError::Sqlite(e) => write!(f, "sqlite: {e}"),
            HubError::Json(e) => write!(f, "json: {e}"),
            HubError::Io(e) => write!(f, "io: {e}"),
            HubError::Invalid(m) => f.write_str(m),
        }
    }
}

impl std::error::Error for HubError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            HubError::Sqlite(e) => Some(e),
            HubError::Json(e) => Some(e),
            HubError::Io(e) => Some(e),
            HubError::Invalid(_) => None,
        }
    }
}

impl From<rusqlite::Error> for HubError {
    fn from(e: rusqlite::Error) -> Self {
        HubError::Sqlite(e)
    }
}

impl From<serde_json::Error> for HubError {
    fn from(e: serde_json::Error) -> Self {
        HubError::Json(e)
    }
}

impl From<std::io::Error> for HubError {
    fn from(e: std::io::Error) -> Self {
        HubError::Io(e)
    }
}

impl HubError {
    pub fn invalid(message: impl Into<String>) -> Self {
        HubError::Invalid(message.into())
    }
}

pub type Result<T> = std::result::Result<T, HubError>;
