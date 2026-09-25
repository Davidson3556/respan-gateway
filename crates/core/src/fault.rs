use std::time::Duration;

use http::StatusCode;

/// Why a hook, filter, source or provider call failed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{kind:?}: {message}")]
pub struct Fault {
    pub kind: FaultKind,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FaultKind {
    BadRequest,
    Unauthorized,
    Forbidden,
    NotFound,
    TooMany {
        retry_after: Duration,
    },
    /// The provider could not be reached or failed mid-response.
    Upstream,
    /// A stream ended without its format's terminal marker.
    Truncated,
    Config,
    Internal,
}

impl Fault {
    pub fn new(kind: FaultKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    pub fn bad_request(message: impl Into<String>) -> Self {
        Self::new(FaultKind::BadRequest, message)
    }

    pub fn unauthorized(message: impl Into<String>) -> Self {
        Self::new(FaultKind::Unauthorized, message)
    }

    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(FaultKind::NotFound, message)
    }

    pub fn too_many(retry_after: Duration) -> Self {
        Self::new(FaultKind::TooMany { retry_after }, "rate limit exceeded")
    }

    pub fn upstream(message: impl Into<String>) -> Self {
        Self::new(FaultKind::Upstream, message)
    }

    pub fn truncated() -> Self {
        Self::new(FaultKind::Truncated, "stream ended before completion")
    }

    pub fn config(message: impl Into<String>) -> Self {
        Self::new(FaultKind::Config, message)
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(FaultKind::Internal, message)
    }

    pub fn status(&self) -> StatusCode {
        match self.kind {
            FaultKind::BadRequest => StatusCode::BAD_REQUEST,
            FaultKind::Unauthorized => StatusCode::UNAUTHORIZED,
            FaultKind::Forbidden => StatusCode::FORBIDDEN,
            FaultKind::NotFound => StatusCode::NOT_FOUND,
            FaultKind::TooMany { .. } => StatusCode::TOO_MANY_REQUESTS,
            FaultKind::Upstream | FaultKind::Truncated => StatusCode::BAD_GATEWAY,
            FaultKind::Config | FaultKind::Internal => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    /// A short machine-readable name, used by formats when rendering errors.
    pub fn code(&self) -> &'static str {
        match self.kind {
            FaultKind::BadRequest => "bad_request",
            FaultKind::Unauthorized => "unauthorized",
            FaultKind::Forbidden => "forbidden",
            FaultKind::NotFound => "not_found",
            FaultKind::TooMany { .. } => "rate_limited",
            FaultKind::Upstream => "upstream_error",
            FaultKind::Truncated => "truncated",
            FaultKind::Config => "config_error",
            FaultKind::Internal => "internal_error",
        }
    }
}
