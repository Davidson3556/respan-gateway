//! Helpers the integration tests share.

// Each test file uses only some helpers.
#![allow(dead_code)]

use respan_gateway_core::bytes::Bytes;
use respan_gateway_core::http::{self, HeaderMap, StatusCode};

use std::sync::{Arc, Mutex, PoisonError};

use respan_gateway_core::{
    Body, ClientContext, Fault, Filter, Gateway, Outcome, Plugin, ProviderContext, Response,
    async_trait,
};
use respan_gateway_formats::Usage;
use respan_gateway_formats::conversation::{Conversation, Finish, Piece, Reply};
pub use respan_gateway_testkit::Calls;
use respan_gateway_testkit::{MockTransport, Outcomes, post, read_whole};

pub const OPENAI: &str = "http://openai.test/v1";

/// Where OpenAI Chat clients send.
pub const CHAT: &str = "/v1/chat/completions";

/// Where Anthropic Messages clients send.
pub const MESSAGES: &str = "/v1/messages";

/// A client's request.
pub type Call = http::Request<Body>;

pub fn call(path: &str, body: impl Into<Bytes>) -> Call {
    post(path, body)
}

/// A provider to add to a gateway, as tests list them.
pub type AddProvider = Box<dyn FnOnce(Gateway) -> Result<Gateway, Fault>>;

/// Provider `name`, speaking `format` over `transport`.
pub fn add_provider(
    name: &str,
    format: impl respan_gateway_core::Format,
    transport: impl respan_gateway_core::Transport,
) -> AddProvider {
    let name = name.to_owned();
    Box::new(move |g| g.provider(name, format, transport))
}

/// A gateway with `list`, in order.
pub fn providers(list: impl IntoIterator<Item = AddProvider>) -> Result<Gateway, Fault> {
    list.into_iter().try_fold(Gateway::new(), |g, add| add(g))
}

/// `gateway` with `plugins` registered in order, named by position.
pub fn with_plugins(
    gateway: Gateway,
    plugins: impl IntoIterator<Item = std::sync::Arc<dyn Plugin>>,
) -> Result<Gateway, Fault> {
    let mut named = plugins.into_iter().enumerate();
    named.try_fold(gateway, |gw, (i, p)| gw.plugin(format!("p{i}"), p))
}

/// A gateway on a mock transport, recording every outcome and metering
/// every chat call.
pub struct Harness {
    pub gw: Gateway,
    pub mock: MockTransport,
    pub log: Outcomes,
    pub calls: Calls,
    pub usage: Metered,
}

/// One chat call's usage, summed by [`Metered`]'s filter.
#[derive(Clone, Default)]
struct CallUsage(Arc<Mutex<Usage>>);

/// Meters every chat call: a provider-edge filter sums the usage of each
/// reply its provider reports. Register it last, so its filter sees each
/// reply first.
#[derive(Clone, Default)]
pub struct Metered(Arc<Mutex<Vec<(String, Usage)>>>);

impl Metered {
    /// Each metered call's usage, in the order their end hooks ran.
    pub fn all(&self) -> Vec<Usage> {
        self.by_call().into_iter().map(|(_, u)| u).collect()
    }

    /// Each metered call's `provider/model` and usage, in the order their
    /// end hooks ran.
    pub fn by_call(&self) -> Vec<(String, Usage)> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Every metered call's usage, added up.
    pub fn total(&self) -> Usage {
        let mut total = Usage::default();
        for usage in self.all() {
            total += usage;
        }
        total
    }
}

#[async_trait]
impl Plugin for Metered {
    async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
        if !px.is_kind::<Conversation>() {
            return Ok(());
        }
        let usage = CallUsage::default();
        px.extensions_mut().insert(usage.clone());
        px.filter(move |reply: &mut Reply| {
            *usage.0.lock().unwrap_or_else(PoisonError::into_inner) += &reply.usage;
            Ok(())
        })
    }

    async fn on_provider_end(&self, px: &ProviderContext, _outcome: &Outcome) {
        if let Some(CallUsage(usage)) = px.extensions().get::<CallUsage>() {
            let usage = usage.lock().unwrap_or_else(PoisonError::into_inner).clone();
            let model = px
                .input::<Conversation>()
                .ok()
                .and_then(|c| c.model.clone());
            let call = format!("{}/{}", px.provider(), model.unwrap_or_default());
            self.0
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push((call, usage));
        }
    }
}

impl Harness {
    /// Send, read the whole response, and wait for the end hooks.
    pub async fn send(&self, req: Call) -> (StatusCode, HeaderMap, Result<Bytes, Fault>) {
        let out = read_whole(self.gw.handle(req).await.0).await;
        self.gw.idle().await;
        out
    }

    /// Like `send`, for a body that must not be aborted.
    pub async fn body(&self, call: Call) -> String {
        self.text(call).await.1
    }

    /// Like `body`, with the status.
    pub async fn text(&self, call: Call) -> (StatusCode, String) {
        let (status, _, body) = self.send(call).await;
        let body = body.unwrap_or_else(|f| panic!("aborted: {f}"));
        (status, String::from_utf8_lossy(&body).into_owned())
    }
}

/// Adds its filter of replies on the client edge.
pub struct Adds<F>(pub F);

#[async_trait]
impl<F: Filter<Reply> + Clone + Sync> Plugin for Adds<F> {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        cx.filter(self.0.clone())?;
        Ok(None)
    }
}

/// Flips whether the provider is asked to stream.
pub struct Flip;

#[async_trait]
impl Plugin for Flip {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        cx.filter(|chat: &mut Conversation| {
            chat.stream = !chat.stream;
            Ok(())
        })?;
        Ok(None)
    }
}

/// Upper-cases a conversation response's text, streamed or whole, in any
/// format.
pub struct Shout;

#[async_trait]
impl Plugin for Shout {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        cx.filter(shout)?;
        Ok(None)
    }
}

fn shout(reply: &mut Reply) -> Result<(), Fault> {
    for text in reply.texts_mut() {
        *text = text.to_uppercase();
    }
    Ok(())
}

/// Restores the name a redaction replaced with `<P1>`, even split across a
/// stream's deltas.
pub struct Unmask;

#[async_trait]
impl Plugin for Unmask {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        cx.filter(Restore::default())?;
        Ok(None)
    }
}

#[derive(Default)]
struct Restore {
    /// Text from a `<` that may begin a placeholder.
    held: String,
    block: u32,
}

impl Filter<Reply> for Restore {
    fn edit(&mut self, mut reply: Reply, out: &mut Vec<Reply>) -> Result<(), Fault> {
        // Nothing follows a finish, and text kept back goes before any
        // other content: hold nothing past them.
        let other = |p: &Piece| matches!(p, Piece::Other { .. });
        let last = reply.finish.is_some() || reply.content.iter().any(other);
        for piece in &mut reply.content {
            let Piece::Text { block, text, .. } = piece else {
                continue;
            };
            let mut t = std::mem::take(&mut self.held) + text.as_str();
            if let Some(i) = t.rfind('<').filter(|&i| !last && !t[i..].contains('>')) {
                self.held = t.split_off(i);
            }
            (self.block, *text) = (*block, t.replace("<P1>", "Alice"));
        }
        reply
            .content
            .retain(|p| !matches!(p, Piece::Text { text, .. } if text.is_empty()));
        if last {
            self.release(out);
        }
        out.push(reply);
        Ok(())
    }

    fn end(&mut self, _end: &Result<(), Fault>, out: &mut Vec<Reply>) -> Result<(), Fault> {
        self.release(out);
        Ok(())
    }
}

impl Restore {
    fn release(&mut self, out: &mut Vec<Reply>) {
        if !self.held.is_empty() {
            out.push(Reply::text(self.block, std::mem::take(&mut self.held)));
        }
    }
}

/// Keeps back every reply and releases them all at the end.
#[derive(Clone)]
pub struct HoldAll(pub Vec<Reply>);

impl Filter<Reply> for HoldAll {
    fn edit(&mut self, reply: Reply, _out: &mut Vec<Reply>) -> Result<(), Fault> {
        self.0.push(reply);
        Ok(())
    }

    fn end(&mut self, _end: &Result<(), Fault>, out: &mut Vec<Reply>) -> Result<(), Fault> {
        out.append(&mut self.0);
        Ok(())
    }
}

/// Puts a note before everything, the start included.
#[derive(Clone)]
pub struct Disclaimer;

impl Filter<Reply> for Disclaimer {
    fn edit(&mut self, reply: Reply, out: &mut Vec<Reply>) -> Result<(), Fault> {
        if reply.start.is_some() {
            out.push(Reply::text(0, "NOTE: "));
        }
        out.push(reply);
        Ok(())
    }
}

/// Drops every start.
#[derive(Clone)]
pub struct Nameless;

impl Filter<Reply> for Nameless {
    fn edit(&mut self, mut reply: Reply, out: &mut Vec<Reply>) -> Result<(), Fault> {
        reply.start = None;
        out.push(reply);
        Ok(())
    }
}

/// Drops every text.
#[derive(Clone)]
pub struct Silence;

impl Filter<Reply> for Silence {
    fn edit(&mut self, mut reply: Reply, out: &mut Vec<Reply>) -> Result<(), Fault> {
        reply.content.retain(|p| !matches!(p, Piece::Text { .. }));
        out.push(reply);
        Ok(())
    }
}

/// Appends a footer to block `.0`, keeping the finish back until the end.
#[derive(Clone)]
pub struct Footer(pub u32, pub Option<Finish>);

impl Filter<Reply> for Footer {
    fn edit(&mut self, mut reply: Reply, out: &mut Vec<Reply>) -> Result<(), Fault> {
        if let Some(finish) = reply.finish.take() {
            self.1 = Some(finish);
        }
        out.push(reply);
        Ok(())
    }

    fn end(&mut self, _end: &Result<(), Fault>, out: &mut Vec<Reply>) -> Result<(), Fault> {
        let mut footer = Reply::text(self.0, " [footer]");
        footer.finish = self.1.take();
        out.push(footer);
        Ok(())
    }
}
