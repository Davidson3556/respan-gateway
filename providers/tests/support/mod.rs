//! Helpers the integration tests share.

// Each test file uses only some helpers.
#![allow(dead_code)]

use respan_gateway_http::http;

use std::sync::{Arc, Mutex, PoisonError};

use respan_gateway_sdk::Usage;
use respan_gateway_sdk::conversation::{Conversation, Finish, Piece, Reply};
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
    F: respan_gateway_sdk::Format<respan_gateway_http::Outgoing, Reply, Conversation>,
    T: Into<SendRequest>,
{
    let send = send.into();
    let connect = respan_gateway_http::connect(move |request| send(request));
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

    async fn on_provider_end(&self, px: &ProviderContext, _end: &Result<(), Fault>) {
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
        let other = |p: &Piece| !matches!(p, Piece::Text { .. });
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

/// The code a client's error body would carry for `fault`.
pub fn fault_code(fault: &Fault) -> String {
    respan_gateway_http::error_code(fault).into_owned()
}

/// `r` with its fault as its message, which tests can compare.
pub fn says<T>(r: Result<T, Fault>) -> Result<T, String> {
    r.map_err(|f| f.message().to_owned())
}
