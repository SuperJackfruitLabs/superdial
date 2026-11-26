//! The crate's error type.

/// An error from a superdial operation.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{what}: HTTP {status}: {body}")]
    Http { what: String, status: u16, body: String },
    #[error("{0}")]
    Transport(String),
    #[error("malformed response: {0}")]
    Malformed(String),
    #[error("no LiveKit transport offered by the homeserver")]
    NoLiveKitTransport,
    #[error("client is not logged in")]
    NotLoggedIn,
    #[error("token issuer: {0}")]
    Issuer(#[from] crate::issuer::IssuerError),
    #[error("LiveKit identity {got} is not the expected {expected}")]
    IdentityMismatch { expected: String, got: String },
    #[error("LiveKit: {0}")]
    LiveKit(String),
    #[error("matrix: {0}")]
    Matrix(String),
    #[error("room {0} is not known to the client")]
    UnknownRoom(String),
}

impl From<reqwest::Error> for Error {
    fn from(e: reqwest::Error) -> Self {
        Error::Transport(e.to_string())
    }
}
