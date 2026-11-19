//! Error type shared by the mesh core.

use std::fmt;

#[derive(Debug)]
pub enum MeshError {
    Sqlite(rusqlite::Error),
    Json(serde_json::Error),
    Io(std::io::Error),
    /// Caller error: bad argument, invariant violation, unsupported capability.
    Invalid(String),
}

impl fmt::Display for MeshError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MeshError::Sqlite(e) => write!(f, "sqlite: {e}"),
            MeshError::Json(e) => write!(f, "json: {e}"),
            MeshError::Io(e) => write!(f, "io: {e}"),
            MeshError::Invalid(m) => f.write_str(m),
        }
    }
}

impl std::error::Error for MeshError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            MeshError::Sqlite(e) => Some(e),
            MeshError::Json(e) => Some(e),
            MeshError::Io(e) => Some(e),
            MeshError::Invalid(_) => None,
        }
    }
}

impl From<rusqlite::Error> for MeshError {
    fn from(e: rusqlite::Error) -> Self {
        MeshError::Sqlite(e)
    }
}

impl From<serde_json::Error> for MeshError {
    fn from(e: serde_json::Error) -> Self {
        MeshError::Json(e)
    }
}

impl From<std::io::Error> for MeshError {
    fn from(e: std::io::Error) -> Self {
        MeshError::Io(e)
    }
}

impl MeshError {
    pub fn invalid(message: impl Into<String>) -> Self {
        MeshError::Invalid(message.into())
    }
}

pub type Result<T> = std::result::Result<T, MeshError>;
