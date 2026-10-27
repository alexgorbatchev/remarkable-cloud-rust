use crate::Item;
use std::fmt;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ErrorKind {
    ConfigNotFound,
    InvalidConfig,
    Unauthorized,
    ItemNotFound,
    AmbiguousName,
    InvalidSchema,
    DiscoveryFailed,
    GenerationConflict,
}

impl fmt::Display for ErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::ConfigNotFound => "rmapi config file not found",
            Self::InvalidConfig => "invalid rmapi config",
            Self::Unauthorized => "unauthorized: missing or invalid credentials",
            Self::ItemNotFound => "item not found",
            Self::AmbiguousName => "ambiguous item name",
            Self::InvalidSchema => "invalid schema format",
            Self::DiscoveryFailed => "service discovery failed",
            Self::GenerationConflict => "cloud root generation conflict",
        })
    }
}

#[derive(Debug)]
pub struct StatusError {
    pub op: String,
    pub status_code: u16,
    pub body: String,
    pub kind: Option<ErrorKind>,
    /// Failure reading the rejected response body, after its headers arrived.
    pub body_error: Option<reqwest::Error>,
}

impl fmt::Display for StatusError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(kind) = self.kind {
            write!(f, "{kind}: ")?;
        }
        write!(f, "{} failed with status {}", self.op, self.status_code)?;
        if !self.body.is_empty() {
            write!(f, ": {}", self.body)?;
        }
        Ok(())
    }
}
impl std::error::Error for StatusError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.body_error.as_ref().map(|error| error as _)
    }
}

#[derive(Clone, Debug)]
pub struct AmbiguousCandidate {
    pub item: Item,
    pub folder_path: String,
    pub unreachable: bool,
}

#[derive(Debug)]
pub struct AmbiguousNameError {
    pub query: String,
    pub name: String,
    pub candidates: Vec<AmbiguousCandidate>,
}
impl fmt::Display for AmbiguousNameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ambiguous item name: {:?}", self.name)?;
        if self.query != self.name {
            write!(f, " in {:?}", self.query)?;
        }
        write!(f, " matches {} items", self.candidates.len())
    }
}
impl std::error::Error for AmbiguousNameError {}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{kind}: {message}")]
    Domain { kind: ErrorKind, message: String },
    #[error(transparent)]
    Status(#[from] StatusError),
    #[error(transparent)]
    Ambiguous(#[from] AmbiguousNameError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Http(#[from] reqwest::Error),
    #[error("cloud HTTP attempt exceeded {0:?}")]
    AttemptTimeout(std::time::Duration),
    #[error("stopped after 10 redirects")]
    RedirectLimit,
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error("{0}")]
    Message(String),
    #[error("multiple listing failures: {0:?}")]
    Multiple(Vec<Error>),
}
impl Error {
    pub fn kind(&self) -> Option<ErrorKind> {
        match self {
            Self::Domain { kind, .. } => Some(*kind),
            Self::Status(e) => e.kind,
            Self::Ambiguous(_) => Some(ErrorKind::AmbiguousName),
            _ => None,
        }
    }
    pub(crate) fn domain(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self::Domain {
            kind,
            message: message.into(),
        }
    }
    pub(crate) fn message(message: impl Into<String>) -> Self {
        Self::Message(message.into())
    }
}
pub type Result<T> = std::result::Result<T, Error>;
