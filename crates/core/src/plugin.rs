use std::sync::Arc;

use async_trait::async_trait;
use http::StatusCode;

use crate::{Context, Fault, ProviderContext, Response, Usage};

/// How a request ([`Plugin::on_client_end`]) or one of its calls
/// ([`Plugin::on_provider_end`]) ended.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct Outcome {
    /// The status sent to the client, after head edits; on the provider
    /// edge, the provider's. A [`Filter::end`] sees it before head edits.
    /// `None` if no response started.
    pub status: Option<StatusCode>,
    /// `Ok` if the response was delivered to its end; the status says
    /// whether it was a success, so a plugin's own 429 ends `Ok`. `Err` if
    /// it failed: a provider error its format read, the fault that broke
    /// the body or refused the request or call, an internal fault for a
    /// panic, or [`codes::ABANDONED`](crate::codes::ABANDONED) if it was
    /// given up. A call whose response is sent ends as the client's does.
    pub end: Result<(), Fault>,
    /// The call's usage; on the client edge, the committed response's,
    /// including any [`Response::with_usage`]. The committed call reports
    /// its usage on both edges, so bill from one edge only.
    pub usage: Usage,
}

/// Ended `Ok`, with no status or usage, such as to test a filter.
impl Default for Outcome {
    fn default() -> Self {
        Self {
            status: None,
            end: Ok(()),
            usage: Usage::default(),
        }
    }
}

/// The per-request extension point, on two edges: the client edge, once per
/// request, and the provider edge, once per call the request makes. A
/// plugin works on a kind through its types ([`Context::input_mut`],
/// [`Context::filter`]), whatever the request's format.
///
/// Plugins registered with [`Gateway::plugin`](crate::Gateway::plugin) see
/// all traffic, in one order on both edges. On traffic of another kind the
/// typed accessors fail with [`codes::WRONG_KIND`](crate::codes::WRONG_KIND),
/// which `?` turns into a refusal; to let such traffic pass:
///
/// ```ignore
/// let chat = match cx.input_mut::<Conversation>() {
///     Err(f) if f.code() == codes::WRONG_KIND => return Ok(None),
///     chat => chat?,
/// };
/// ```
#[async_trait]
pub trait Plugin: Send + Sync + 'static {
    /// The client edge, once per request. `Ok(None)` passes to the next
    /// plugin; `Ok(Some(response))` answers, skipping the rest; `Err`
    /// refuses. If all pass, the target answers ([`Context::set_target`]);
    /// with none, the request is refused 404
    /// [`codes::NOT_FOUND`](crate::codes::NOT_FOUND).
    async fn on_client(&self, _cx: &mut Context) -> Result<Option<Response>, Fault> {
        Ok(None)
    }

    /// Once per request, after the response ended, for every plugin whose
    /// `on_client` started. It runs after every call's `on_provider_end`;
    /// calls still open end [`codes::ABANDONED`](crate::codes::ABANDONED)
    /// when the request closes. Hooks run in order, detached: on the current
    /// tokio runtime, else a thread. A panic does not stop the others;
    /// [`Gateway::idle`](crate::Gateway::idle) waits for all. `call` fails
    /// here, and `cx.target()` is the last target set, not proof it
    /// answered.
    async fn on_client_end(&self, _cx: &Context, _outcome: &Outcome) {}

    /// The provider edge, before every call the request makes, the target's
    /// included, for each plugin in order until one refuses, whether or not
    /// its `on_client` ran. `Err` refuses the call before anything is sent:
    /// [`Context::call`] returns it, and a refused target call refuses the
    /// request.
    async fn on_provider(&self, _px: &mut ProviderContext) -> Result<(), Fault> {
        Ok(())
    }

    /// Once per call whose `on_provider` started for this plugin: when the
    /// call fails before a response, or its response ends. A sent response
    /// ends as the client's does, a collected one as `collect` returns, and
    /// one dropped or still open when the request closes ends
    /// [`codes::ABANDONED`](crate::codes::ABANDONED). Runs like
    /// `on_client_end`, and before it.
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
/// stream ([`Context::filter`], [`ProviderContext::filter`]): change, drop,
/// hold back or insert them. Relayed, an item whose events changed is
/// rebuilt, and a streamed one left empty is left out. An event the
/// client's format cannot write fails the response.
pub trait Filter<T>: Send + 'static {
    /// Take `event` and push to `out` what replaces it, if anything.
    fn event(&mut self, event: T, out: &mut Vec<T>) -> Result<(), Fault>;

    /// The response ends as `outcome` says, before the end is written: its
    /// end, usage, and status before head edits. Runs once at every end,
    /// failures included, and for a call still open when its request
    /// closes; not if the reader stops first or something panics. Push held
    /// events to `out`, written before the end. `Err` fails the response;
    /// after a relayed provider error the client already has, it reaches
    /// only the `Outcome`.
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
