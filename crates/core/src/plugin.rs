use std::sync::Arc;

use async_trait::async_trait;
use http::StatusCode;

use crate::{Context, Fault, ProviderContext, Response, Usage};

/// How a request, or one call it made, ended. On the client edge
/// ([`Plugin::on_client_end`]): what the client was sent, and the usage of
/// the response committed to it. On the provider edge
/// ([`Plugin::on_provider_end`]): what the call got, and its usage.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct Outcome {
    /// On the client edge, the status sent, after head edits; a
    /// [`Filter::end`] there sees it before them. On the provider edge, the
    /// provider's, as its filters and the client edge's see it. `None` if
    /// no response started: the client left first, or the call was refused
    /// or failed before its head came.
    pub status: Option<StatusCode>,
    /// `Ok` if the response was delivered to its end as it said: its
    /// status tells whether it was a success, so a plugin's own 429 made of
    /// events ends `Ok`. `Err` if it failed: with the error its format read
    /// in it, such as a provider's error status or error event, the fault
    /// that broke the body or refused the request or call, an internal
    /// fault if a plugin or format panicked, or an
    /// [`codes::ABANDONED`](crate::codes::ABANDONED) fault if the client
    /// left or the call's response was dropped before its end. A call whose
    /// response is sent ends as the client's response does.
    pub end: Result<(), Fault>,
    /// On the provider edge, the call's usage. On the client edge, the
    /// usage reported for the committed response, including any not passed
    /// to the client: the committed call's, which that call's
    /// `on_provider_end` also gets, plus any [`Response::with_usage`].
    /// Other calls' usage is on their own provider edge. Do not add the two
    /// edges' usage for billing.
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

/// The only per-request extension point for features, on two edges: the
/// client edge, once per request, and the provider edge, once per call the
/// request makes, whoever makes it. A plugin implements the edge it
/// belongs to, and works on a kind through its types
/// ([`Context::input_mut`], [`Context::filter`]), whatever format the
/// request comes in.
///
/// Plugins are registered on the gateway
/// ([`Gateway::plugin`](crate::Gateway::plugin)) and see all its traffic,
/// in one order on both edges. For traffic of another kind the typed
/// accessors are an `Err` with
/// [`codes::WRONG_KIND`](crate::codes::WRONG_KIND), which `?` turns into a
/// refusal; a plugin for one kind that lets other traffic pass says so:
///
/// ```ignore
/// let chat = match cx.input_mut::<Conversation>() {
///     Err(f) if f.code() == codes::WRONG_KIND => return Ok(None),
///     chat => chat?,
/// };
/// ```
#[async_trait]
pub trait Plugin: Send + Sync + 'static {
    /// The client edge: runs once per request, in the gateway's order.
    /// `Ok(None)` passes to the next plugin; `Ok(Some(response))` answers
    /// and skips the plugins after it; `Err` refuses the request. If every plugin
    /// passes, the target answers ([`Context::set_target`]); with none set,
    /// the request is refused 404 with
    /// [`codes::NOT_FOUND`](crate::codes::NOT_FOUND).
    async fn on_client(&self, _cx: &mut Context) -> Result<Option<Response>, Fault> {
        Ok(None)
    }

    /// Runs once per request after the response ended, for every plugin
    /// whose `on_client` started, once every call the request made has
    /// ended and run its `on_provider_end` hooks: a call whose response is
    /// still open, such as one a plugin kept, ends as the request closes,
    /// [`codes::ABANDONED`](crate::codes::ABANDONED). The hooks run one
    /// after another in the gateway's order, detached from the response:
    /// on the current tokio runtime if there is one, else on a thread of
    /// their own. A hook that panics does not stop the others;
    /// [`Gateway::idle`](crate::Gateway::idle) waits for them all. `call`
    /// is an `Err` here. `cx.target()` is the last target set, not proof
    /// that it answered.
    async fn on_client_end(&self, _cx: &Context, _outcome: &Outcome) {}

    /// The provider edge: runs before every call the request makes, the
    /// target's once every plugin passes, a plugin's [`Context::call`], each
    /// of a fallback's, for each plugin in the gateway's order until one
    /// refuses, whether or not its `on_client` ran; the request is
    /// [`ProviderContext::client`]. `Err` refuses the call before anything
    /// is sent: the call returns it, and for the target's call the request
    /// is refused with it in the client's format.
    async fn on_provider(&self, _px: &mut ProviderContext) -> Result<(), Fault> {
        Ok(())
    }

    /// Runs once per call whose `on_provider` started for this plugin: when
    /// the call fails before a response, such as refused on this edge or
    /// by the transport, or once its response ended, whoever read it:
    /// sent, collected, read to its end, or dropped first or still open
    /// when the request closes, which ends it
    /// [`codes::ABANDONED`](crate::codes::ABANDONED). `outcome` is the
    /// call's own, its usage included; a response sent ends as the
    /// client's does, a failure on the client edge included, and one
    /// collected as `collect` returns. Runs like `on_client_end`: detached,
    /// in the gateway's order, panics caught, awaited by
    /// [`Gateway::idle`](crate::Gateway::idle), and always before the
    /// request's `on_client_end`.
    async fn on_provider_end(&self, _px: &ProviderContext, _outcome: &Outcome) {}
}

/// A shared plugin; it forwards every method.
#[async_trait]
impl<P: Plugin + ?Sized> Plugin for Arc<P> {
    async fn on_client(&self, cx: &mut Context) -> Result<Option<Response>, Fault> {
        (**self).on_client(cx).await
    }

    async fn on_client_end(&self, cx: &Context, outcome: &Outcome) {
        (**self).on_client_end(cx, outcome).await;
    }

    async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
        (**self).on_provider(px).await
    }

    async fn on_provider_end(&self, px: &ProviderContext, outcome: &Outcome) {
        (**self).on_provider_end(px, outcome).await;
    }
}

/// Edits a response's events of `T`, its format's output type, as they
/// stream ([`Context::filter`], [`ProviderContext::filter`]): it may
/// change, drop, hold back and insert them. When relaying, the client's
/// format rebuilds an item whose events changed and leaves out a streamed
/// one with every event taken. Content the client's format cannot write
/// out of place, such as another format's, fails the response.
pub trait Filter<T>: Send + 'static {
    /// Take `event` and push to `out` what replaces it: it, others, or
    /// nothing yet.
    fn event(&mut self, event: T, out: &mut Vec<T>) -> Result<(), Fault>;

    /// The response ends as `outcome` says, as known before the end is
    /// written: its end, all its usage, and its status before head edits.
    /// Runs once at every end, a failure or truncation too, or a call's
    /// response still open as its request closes, but not if its reader
    /// stops first (the response is dropped, or, for a call's filter, fails
    /// on the client edge), or a decoder, filter, encoder or head edit
    /// panics. Push to `out` what is held back; it is written
    /// before the end. `Err` fails the response; at a relayed provider
    /// error, whose body already told the client, that fault reaches the
    /// `Outcome`, not the client.
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
