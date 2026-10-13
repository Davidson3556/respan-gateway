//! Extensions a [`Fault`](crate::Fault) carries that any protocol can use: how
//! to retry, what to tell the client. The kernel reads none of them; each edge
//! renders the ones it knows.

use std::borrow::Cow;
use std::time::Duration;

use serde_json::{Map, Value};

/// How long the client should wait before it tries again.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RetryAfter(pub Duration);

/// A client-facing code a format writes in its error body.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ErrorCode(pub Cow<'static, str>);

/// A fault's extra fields for a client's JSON error body, as a hint
/// ([`Fault::with`](crate::Fault::with)). Formats write these into their
/// error bodies; the kernel never reads them.
#[derive(Clone, Debug, PartialEq)]
pub struct Details(pub Map<String, Value>);
