use std::sync::Arc;

use async_trait::async_trait;
use http::StatusCode;

use crate::{Context, Fault, Response, Usage};

/// How a request ended: what the client was sent, and the usage of the
/// response committed to it.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct Outcome {
    /// The status sent, after head edits (before them in
    /// [`Filter::end`]); `None` if the client left before a response
    /// started.
    pub status: Option<StatusCode>,
    /// `Ok` only for a complete, successful response. Otherwise the
    /// provider's error, the fault that broke the body or refused the
    /// request, an internal fault if a plugin or format panicked, or an
    /// [`codes::ABANDONED`](crate::codes::ABANDONED) fault if the client left.
    pub end: Result<(), Fault>,
    /// The usage reported for the committed response, including any not
    /// passed to the client. Uncommitted provider calls are not included.
    pub usage: Usage,
}

/// Done, with no status or usage, such as to test a filter's `end`.
impl Default for Outcome {
    fn default() -> Self {
        Self {
            status: None,
            end: Ok(()),
            usage: Usage::default(),
        }
    }
}

/// The only per-request extension point for features. A plugin works on a
/// kind through its types ([`Context::request_mut`], [`Context::filter`]),
/// whatever format the request comes in.
#[async_trait]
pub trait Plugin: Send + Sync + 'static {
    /// Runs once per request, in chain order. `Ok(None)` passes to the next
    /// plugin; `Ok(Some(response))` answers and skips the rest of the chain;
    /// `Err` refuses the request. If every plugin passes, the target answers
    /// ([`Context::set_target`]); with none set, the request is refused 404
    /// with [`codes::NOT_FOUND`](crate::codes::NOT_FOUND).
    async fn on_request(&self, _cx: &mut Context) -> Result<Option<Response>, Fault> {
        Ok(None)
    }

    /// Runs once per request after the response ended, for every plugin
    /// whose `on_request` started. The hooks run one after another in chain
    /// order, detached from the response: on the current tokio runtime if
    /// there is one, else on a thread of their own. A hook that panics does
    /// not stop the others; [`Gateway::idle`](crate::Gateway::idle) waits
    /// for them all. `call` is an `Err` here. `cx.target()` is the last
    /// target set, not proof that it answered.
    async fn on_close(&self, _cx: &Context, _outcome: &Outcome) {}
}

/// A shared plugin; it forwards every method.
#[async_trait]
impl<P: Plugin + ?Sized> Plugin for Arc<P> {
    async fn on_request(&self, cx: &mut Context) -> Result<Option<Response>, Fault> {
        (**self).on_request(cx).await
    }

    async fn on_close(&self, cx: &Context, outcome: &Outcome) {
        (**self).on_close(cx, outcome).await;
    }
}

/// Edits a response's events of `T`, its kind's event type, as they stream
/// ([`Context::filter`]): it may change, drop, hold back and insert them.
/// When relaying, the client's format rebuilds an item whose events changed
/// and leaves out a streamed one with every event taken. Content the
/// client's format cannot write out of place, such as another format's,
/// fails the response.
pub trait Filter<T>: Send + 'static {
    /// Take `event` and push to `out` what replaces it: it, others, or
    /// nothing yet.
    fn event(&mut self, event: T, out: &mut Vec<T>) -> Result<(), Fault>;

    /// The response ends as `outcome` says, as known before the end is
    /// written: its end, all its usage, and its status before head edits.
    /// Runs once at every end, a failure or truncation too, but not if the
    /// client leaves first or a decoder, filter or encoder panics. Push to
    /// `out` what is held back; it is written before the end. `Err` fails
    /// the response.
    fn end(&mut self, _outcome: &Outcome, _out: &mut Vec<T>) -> Result<(), Fault> {
        Ok(())
    }
}

/// A closure that changes each event in place, or fails the response.
impl<T, F: FnMut(&mut T) -> Result<(), Fault> + Send + 'static> Filter<T> for F {
    fn event(&mut self, mut event: T, out: &mut Vec<T>) -> Result<(), Fault> {
        self(&mut event)?;
        out.push(event);
        Ok(())
    }
}
