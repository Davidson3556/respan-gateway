//! Helpers the integration tests share.

// Each test file uses some of it.
#![allow(dead_code)]

use respan_gateway_core::bytes::Bytes;
use respan_gateway_core::http::{self, HeaderMap, StatusCode};
use respan_gateway_core::{
    Body, Context, Fault, Filter, Gateway, Outcome, Plugin, Response, async_trait,
};
use respan_gateway_formats::conversation::{Conversation, Reply};
use respan_gateway_testkit::{MockTransport, Outcomes, collect, post};

pub const OPENAI: &str = "http://openai.test/v1";

/// Where OpenAI Chat clients send.
pub const CHAT: &str = "/v1/chat/completions";

/// Where Anthropic Messages clients send.
pub const MESSAGES: &str = "/v1/messages";

/// A client's request.
pub type Call = http::Request<Body>;

/// `body` posted to `path`.
pub fn call(path: &str, body: impl Into<Bytes>) -> Call {
    post(path, body)
}

/// `gateway` with `plugins` registered in order, named by position.
pub fn with_plugins(
    gateway: Gateway,
    plugins: impl IntoIterator<Item = std::sync::Arc<dyn Plugin>>,
) -> Result<Gateway, Fault> {
    let mut named = plugins.into_iter().enumerate();
    named.try_fold(gateway, |gw, (i, p)| gw.plugin(format!("p{i}"), p))
}

/// A gateway on a mock transport, recording every outcome.
pub struct Harness {
    pub gw: Gateway,
    pub mock: MockTransport,
    pub log: Outcomes,
}

impl Harness {
    /// Send, read the whole response, and wait for the end hooks.
    pub async fn send(&self, req: Call) -> (StatusCode, HeaderMap, Result<Bytes, Fault>) {
        let out = collect(self.gw.handle(req).await).await;
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

/// Flips whether the provider is asked to stream.
pub struct Flip;

#[async_trait]
impl Plugin for Flip {
    async fn on_client(&self, cx: &mut Context) -> Result<Option<Response>, Fault> {
        let chat = cx.input_mut::<Conversation>()?;
        chat.stream = !chat.stream;
        Ok(None)
    }
}

/// Upper-cases a conversation response's text, streamed or whole, in any
/// format, as a redaction plugin would.
pub struct Shout;

#[async_trait]
impl Plugin for Shout {
    async fn on_client(&self, cx: &mut Context) -> Result<Option<Response>, Fault> {
        cx.filter(shout)?;
        Ok(None)
    }
}

fn shout(event: &mut Reply) -> Result<(), Fault> {
    if let Reply::Text { text, .. } = event {
        *text = text.to_uppercase();
    }
    Ok(())
}

/// Restores the name a redaction replaced with `<P1>`, even split across a
/// stream's deltas.
pub struct Unmask;

#[async_trait]
impl Plugin for Unmask {
    async fn on_client(&self, cx: &mut Context) -> Result<Option<Response>, Fault> {
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
    fn event(&mut self, event: Reply, out: &mut Vec<Reply>) -> Result<(), Fault> {
        let Reply::Text { block, text, .. } = event else {
            // Held text goes out before any other content.
            if !matches!(event, Reply::Start { .. }) {
                self.release(out);
            }
            out.push(event);
            return Ok(());
        };
        let mut text = std::mem::take(&mut self.held) + &text;
        if let Some(i) = text.rfind('<').filter(|&i| !text[i..].contains('>')) {
            self.held = text.split_off(i);
        }
        self.block = block;
        if !text.is_empty() {
            out.push(Reply::text(block, text.replace("<P1>", "Alice")));
        }
        Ok(())
    }

    fn end(&mut self, _outcome: &Outcome, out: &mut Vec<Reply>) -> Result<(), Fault> {
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

/// Holds back every event and releases them all at the end.
#[derive(Clone)]
pub struct HoldAll(pub Vec<Reply>);

impl Filter<Reply> for HoldAll {
    fn event(&mut self, event: Reply, _out: &mut Vec<Reply>) -> Result<(), Fault> {
        self.0.push(event);
        Ok(())
    }

    fn end(&mut self, _outcome: &Outcome, out: &mut Vec<Reply>) -> Result<(), Fault> {
        out.append(&mut self.0);
        Ok(())
    }
}

/// Puts a note before everything, the start included.
#[derive(Clone)]
pub struct Disclaimer;

impl Filter<Reply> for Disclaimer {
    fn event(&mut self, event: Reply, out: &mut Vec<Reply>) -> Result<(), Fault> {
        if matches!(event, Reply::Start { .. }) {
            out.push(Reply::text(0, "NOTE: "));
        }
        out.push(event);
        Ok(())
    }
}

/// Takes every start.
#[derive(Clone)]
pub struct Nameless;

impl Filter<Reply> for Nameless {
    fn event(&mut self, event: Reply, out: &mut Vec<Reply>) -> Result<(), Fault> {
        if !matches!(event, Reply::Start { .. }) {
            out.push(event);
        }
        Ok(())
    }
}

/// Takes every text.
#[derive(Clone)]
pub struct Silence;

impl Filter<Reply> for Silence {
    fn event(&mut self, event: Reply, out: &mut Vec<Reply>) -> Result<(), Fault> {
        if !matches!(event, Reply::Text { .. }) {
            out.push(event);
        }
        Ok(())
    }
}

/// Appends a footer to block `.0`, holding the finish until the end.
#[derive(Clone)]
pub struct Footer(pub u32, pub Option<Reply>);

impl Filter<Reply> for Footer {
    fn event(&mut self, event: Reply, out: &mut Vec<Reply>) -> Result<(), Fault> {
        match event {
            Reply::Finish(_) => self.1 = Some(event),
            event => out.push(event),
        }
        Ok(())
    }

    fn end(&mut self, _outcome: &Outcome, out: &mut Vec<Reply>) -> Result<(), Fault> {
        out.push(Reply::text(self.0, " [footer]"));
        out.extend(self.1.take());
        Ok(())
    }
}
