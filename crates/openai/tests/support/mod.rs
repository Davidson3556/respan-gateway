//! Helpers the integration tests share.

// Each test file uses only some helpers.
#![allow(dead_code)]

use respan_gateway_http::http;

use crate::http_support::LIMIT;

use std::sync::{Arc, Mutex, PoisonError};

use respan_gateway_conversation::{Block, Delta, Finish, Input, Output};
use respan_gateway_sdk::Usage;
use respan_gateway_sdk::{
    ClientContext, Fault, Filter, Gateway, Plugin, ProviderContext, Response, async_trait,
};

/// Where OpenAI Chat clients send.
pub const CHAT: &str = "/v1/chat/completions";

/// Where Anthropic Messages clients send.
pub const MESSAGES: &str = "/v1/messages";

/// A client request.
pub type Call = http::Request<respan_gateway_http::Body>;

/// A provider to add to a gateway, as tests list them.
pub type AddProvider = Box<dyn FnOnce(Gateway) -> Result<Gateway, Fault>>;

/// Provider `name`, speaking `format` over `transport`.
pub fn add_provider<F, T>(name: &str, format: F, send: T) -> AddProvider
where
    F: respan_gateway_sdk::Format<respan_gateway_http::Outgoing, Output, Input>,
    T: Into<SendRequest>,
{
    let send = send.into();
    let connect = respan_gateway_http::connect(move |request| send(request), LIMIT);
    let name = name.to_owned();
    Box::new(move |g| g.provider(name, format, connect))
}

/// Shared native HTTP callback for scripted provider fixtures.
pub type SendRequest = Arc<
    dyn Fn(
            Call,
        ) -> respan_gateway_sdk::futures::future::BoxFuture<
            'static,
            Result<
                (
                    http::Response<respan_gateway_http::Body>,
                    Option<respan_gateway_http::Completion>,
                ),
                Fault,
            >,
        > + Send
        + Sync,
>;

pub fn sender<F, Fut>(send: F) -> SendRequest
where
    F: Fn(Call) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<
            Output = Result<
                (
                    http::Response<respan_gateway_http::Body>,
                    Option<respan_gateway_http::Completion>,
                ),
                Fault,
            >,
        > + Send
        + 'static,
{
    Arc::new(move |request| Box::pin(send(request)))
}

pub fn http_sender(client: respan_gateway_http::HttpClient) -> SendRequest {
    let client = Arc::new(client);
    sender(move |request| {
        let client = client.clone();
        async move { client.send(request).await }
    })
}

/// A gateway with `list`, in order.
pub fn providers(list: impl IntoIterator<Item = AddProvider>) -> Result<Gateway, Fault> {
    list.into_iter().try_fold(Gateway::new(), |g, add| add(g))
}

/// `gateway` with `plugins` registered in order.
pub fn with_plugins(
    gateway: Gateway,
    plugins: impl IntoIterator<Item = std::sync::Arc<dyn Plugin>>,
) -> Gateway {
    plugins.into_iter().fold(gateway, Gateway::plugin)
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
        if !px.is_kind::<Input>() {
            return Ok(());
        }
        let usage = CallUsage::default();
        px.extensions_mut().insert(usage.clone());
        px.filter(move |reply: &mut Output| {
            if let Output::Usage(more) = reply {
                *usage.0.lock().unwrap_or_else(PoisonError::into_inner) += &*more;
            }
            Ok(())
        })
    }

    async fn on_provider_end(&self, px: &ProviderContext, _end: &Result<(), Fault>) {
        if let Some(CallUsage(usage)) = px.extensions().get::<CallUsage>() {
            let usage = usage.lock().unwrap_or_else(PoisonError::into_inner).clone();
            let model = px.input::<Input>().ok().and_then(|c| c.model.clone());
            let call = format!("{}/{}", px.provider(), model.unwrap_or_default());
            self.0
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push((call, usage));
        }
    }
}

/// Adds its filter of replies on the client edge.
pub struct Adds<F>(pub F);

#[async_trait]
impl<F: Filter<Output> + Clone + Sync> Plugin for Adds<F> {
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
        let streaming = cx
            .extensions()
            .get::<respan_gateway_http::Wire>()
            .and_then(|w| w.get())
            .and_then(|r| serde_json::from_slice::<serde_json::Value>(r.body()).ok())
            .and_then(|v| v["stream"].as_bool())
            .unwrap_or(false);
        cx.extensions_mut()
            .insert(respan_gateway_codec::StreamResponse(!streaming));
        Ok(None)
    }
}

/// The events of text `text` in block `block`: it opens, says it and ends.
pub fn text(block: u32, text: &str) -> Vec<Output> {
    vec![
        Output::block_start(block, Block::Text),
        Output::text(block, text),
        Output::block_end(block),
    ]
}

/// The text `reply` adds to a text block.
pub fn text_of(reply: &Output) -> Option<&str> {
    match reply {
        Output::Delta {
            delta: Delta::Text(text),
            ..
        } => Some(text),
        _ => None,
    }
}

/// The text `reply` adds to a text block, to change.
pub fn text_of_mut(reply: &mut Output) -> Option<&mut String> {
    match reply {
        Output::Delta {
            delta: Delta::Text(text),
            ..
        } => Some(text),
        _ => None,
    }
}

/// The text blocks of a response, as its events open them: what a filter
/// that drops or holds a block's start and end needs, as those events do not
/// say what the block holds.
#[derive(Clone, Default)]
pub struct TextBlocks(Vec<u32>);

impl TextBlocks {
    /// Note what `reply` opens.
    pub fn note(&mut self, reply: &Output) {
        if let Output::BlockStart { block, kind } = reply
            && *kind == Block::Text
            && !self.0.contains(block)
        {
            self.0.push(*block);
        }
    }

    /// Whether `block` is a text block.
    pub fn contains(&self, block: u32) -> bool {
        self.0.contains(&block)
    }

    /// Whether `reply` is an event of a text block's content.
    pub fn is_text(&self, reply: &Output) -> bool {
        match reply {
            Output::BlockStart { block, .. }
            | Output::Delta { block, .. }
            | Output::BlockEnd { block } => self.0.contains(block),
            _ => false,
        }
    }
}

/// Upper-cases a conversation response's text, streamed or whole, in any
/// format.
pub struct Shout;

#[async_trait]
impl Plugin for Shout {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        cx.filter(Shouting)?;
        Ok(None)
    }
}

#[derive(Clone, Default)]
struct Shouting;

impl Filter<Output> for Shouting {
    fn edit(&mut self, mut reply: Output, out: &mut Vec<Output>) -> Result<(), Fault> {
        if let Some(text) = text_of_mut(&mut reply) {
            *text = text.to_uppercase();
        }
        out.push(reply);
        Ok(())
    }
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

impl Filter<Output> for Restore {
    fn edit(&mut self, mut reply: Output, out: &mut Vec<Output>) -> Result<(), Fault> {
        // Nothing follows a finish, and text kept back goes before any
        // other content: hold nothing past them.
        let text = matches!(
            &reply,
            Output::Delta {
                delta: Delta::Text(_),
                ..
            }
        );
        let last = !text && reply.is_content();
        if let Output::Delta {
            block,
            delta: Delta::Text(delta),
        } = &mut reply
        {
            let mut t = std::mem::take(&mut self.held) + delta.as_str();
            if let Some(i) = t.rfind('<').filter(|&i| !t[i..].contains('>')) {
                self.held = t.split_off(i);
            }
            (self.block, *delta) = (*block, t.replace("<P1>", "Alice"));
            if delta.is_empty() {
                return Ok(());
            }
        }
        if last {
            self.release(out);
        }
        out.push(reply);
        Ok(())
    }

    fn end(&mut self, _end: &Result<(), Fault>, out: &mut Vec<Output>) -> Result<(), Fault> {
        self.release(out);
        Ok(())
    }
}

impl Restore {
    fn release(&mut self, out: &mut Vec<Output>) {
        if !self.held.is_empty() {
            out.push(Output::text(self.block, std::mem::take(&mut self.held)));
        }
    }
}

/// Keeps back every reply and releases them all at the end.
#[derive(Clone)]
pub struct HoldAll(pub Vec<Output>);

impl Filter<Output> for HoldAll {
    fn edit(&mut self, reply: Output, _out: &mut Vec<Output>) -> Result<(), Fault> {
        self.0.push(reply);
        Ok(())
    }

    fn end(&mut self, _end: &Result<(), Fault>, out: &mut Vec<Output>) -> Result<(), Fault> {
        out.append(&mut self.0);
        Ok(())
    }
}

/// Puts a note, a block of its own, before everything, the start included.
#[derive(Clone)]
pub struct Disclaimer;

impl Filter<Output> for Disclaimer {
    fn edit(&mut self, reply: Output, out: &mut Vec<Output>) -> Result<(), Fault> {
        if matches!(reply, Output::Start(_)) {
            out.extend(text(u32::MAX, "NOTE: "));
        }
        out.push(reply);
        Ok(())
    }
}

/// Drops every start.
#[derive(Clone)]
pub struct Nameless;

impl Filter<Output> for Nameless {
    fn edit(&mut self, reply: Output, out: &mut Vec<Output>) -> Result<(), Fault> {
        if !matches!(reply, Output::Start(_)) {
            out.push(reply);
        }
        Ok(())
    }
}

/// Drops every text.
#[derive(Clone, Default)]
pub struct Silence(TextBlocks);

impl Filter<Output> for Silence {
    fn edit(&mut self, reply: Output, out: &mut Vec<Output>) -> Result<(), Fault> {
        self.0.note(&reply);
        if !self.0.is_text(&reply) {
            out.push(reply);
        }
        Ok(())
    }
}

/// Appends a footer to block `.0`, keeping its end and the finish back until
/// the end of the response.
#[derive(Clone)]
pub struct Footer(pub u32, pub Option<Finish>);

impl Filter<Output> for Footer {
    fn edit(&mut self, reply: Output, out: &mut Vec<Output>) -> Result<(), Fault> {
        match reply {
            Output::Finish(finish) => self.1 = Some(finish),
            Output::BlockEnd { block } if block == self.0 => {}
            reply => out.push(reply),
        }
        Ok(())
    }

    fn end(&mut self, end: &Result<(), Fault>, out: &mut Vec<Output>) -> Result<(), Fault> {
        out.push(Output::block_start(self.0, Block::Text));
        out.push(Output::text(self.0, " [footer]"));
        out.push(Output::block_end(self.0));
        // A finish kept back past a failed end is not written, as it would
        // claim the answer complete.
        let finish = self.1.take().filter(|_| end.is_ok());
        out.extend(finish.map(Output::finished));
        Ok(())
    }
}

/// The code a client's error body would carry for `fault`.
pub fn fault_code(fault: &Fault) -> String {
    respan_gateway_http::error_code(fault).to_owned()
}

/// `r` with its fault as its message, which tests can compare.
pub fn says<T>(r: Result<T, Fault>) -> Result<T, String> {
    r.map_err(|f| f.message().to_owned())
}
