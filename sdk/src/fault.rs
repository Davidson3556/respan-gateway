//! Facts a [`Fault`] carries that any protocol can use: who failed, how to
//! retry, what to tell the client. The kernel reads none of them; each edge
//! renders the ones it knows.

use std::borrow::Cow;
use std::time::Duration;

use crate::Fault;

/// The client's input is not one its format or kind can take.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BadRequest;

impl BadRequest {
    /// A fault with this message and the [`BadRequest`] fact.
    pub fn fault(message: impl Into<String>) -> Fault {
        Fault::new(message).with(Self)
    }
}

/// A provider failed: unreachable, broken off, or not in its format.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Upstream;

impl Upstream {
    /// A fault with this message and the [`Upstream`] fact.
    pub fn fault(message: impl Into<String>) -> Fault {
        Fault::new(message).with(Self)
    }
}

/// A provider's stream stopped before its end.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Truncated;

impl Truncated {
    /// A fault with the [`Truncated`] fact.
    pub fn fault() -> Fault {
        Fault::new("stream ended before completion").with(Self)
    }
}

/// How long the client should wait before it tries again.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RetryAfter(pub Duration);

/// A client-facing code a format writes in its error body.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ErrorCode(pub Cow<'static, str>);
