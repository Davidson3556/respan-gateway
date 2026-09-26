//! Helpers the integration tests share.

// Each test file uses some of it.
#![allow(dead_code)]

use respan_gateway_core::bytes::Bytes;
use respan_gateway_core::http::{self, HeaderMap, StatusCode};
use respan_gateway_core::{
    Body, Context, Fault, Filter, Gateway, Outcome, Plugin, Response, async_trait,
};
use respan_gateway_formats::conversation::{Conversation, Event};
use respan_gateway_testkit::{MockTransport, Outcomes, collect, post};

pub const OPENAI: &str = "http://openai.test";

/// A route and a request for it.
pub type Call = (&'static str, http::Request<Body>);

/// `body` sent to `route`.
pub fn call(route: &'static str, body: impl Into<Bytes>) -> Call {
    (route, post(body))
}

/// A gateway on a mock transport, recording every outcome.
pub struct Harness {
    pub gw: Gateway,
    pub mock: MockTransport,
    pub log: Outcomes,
}

impl Harness {
    /// Send, read the whole response, and wait for the close hooks.
    pub async fn send(&self, (route, req): Call) -> (StatusCode, HeaderMap, Result<Bytes, Fault>) {
        let out = collect(self.gw.handle(route, req).await).await;
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
    async fn on_request(&self, cx: &mut Context) -> Result<Option<Response>, Fault> {
        let chat = cx.request_mut::<Conversation>()?;
        chat.stream = !chat.stream;
        Ok(None)
    }
}

/// Upper-cases a conversation response's text, streamed or whole, in any
/// format, as a redaction plugin would.
pub struct Shout;

#[async_trait]
impl Plugin for Shout {
    async fn on_request(&self, cx: &mut Context) -> Result<Option<Response>, Fault> {
        cx.filter(shout)?;
        Ok(None)
    }
}

fn shout(event: &mut Event) -> Result<(), Fault> {
    if let Event::Text { text, .. } = event {
        *text = text.to_uppercase();
    }
    Ok(())
}

/// Restores the name a redaction replaced with `<P1>`, even split across a
/// stream's deltas.
pub struct Unmask;

#[async_trait]
impl Plugin for Unmask {
    async fn on_request(&self, cx: &mut Context) -> Result<Option<Response>, Fault> {
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

impl Filter<Event> for Restore {
    fn event(&mut self, event: Event, out: &mut Vec<Event>) -> Result<(), Fault> {
        let Event::Text { block, text, .. } = event else {
            // Held text goes out before any other content.
            if !matches!(event, Event::Start { .. }) {
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
            out.push(Event::text(block, text.replace("<P1>", "Alice")));
        }
        Ok(())
    }

    fn end(&mut self, _outcome: &Outcome, out: &mut Vec<Event>) -> Result<(), Fault> {
        self.release(out);
        Ok(())
    }
}

impl Restore {
    fn release(&mut self, out: &mut Vec<Event>) {
        if !self.held.is_empty() {
            out.push(Event::text(self.block, std::mem::take(&mut self.held)));
        }
    }
}

/// Holds back every event and releases them all at the end.
#[derive(Clone)]
pub struct HoldAll(pub Vec<Event>);

impl Filter<Event> for HoldAll {
    fn event(&mut self, event: Event, _out: &mut Vec<Event>) -> Result<(), Fault> {
        self.0.push(event);
        Ok(())
    }

    fn end(&mut self, _outcome: &Outcome, out: &mut Vec<Event>) -> Result<(), Fault> {
        out.append(&mut self.0);
        Ok(())
    }
}

/// Puts a note before everything, the start included.
#[derive(Clone)]
pub struct Disclaimer;

impl Filter<Event> for Disclaimer {
    fn event(&mut self, event: Event, out: &mut Vec<Event>) -> Result<(), Fault> {
        if matches!(event, Event::Start { .. }) {
            out.push(Event::text(0, "NOTE: "));
        }
        out.push(event);
        Ok(())
    }
}

/// Takes every start.
#[derive(Clone)]
pub struct Nameless;

impl Filter<Event> for Nameless {
    fn event(&mut self, event: Event, out: &mut Vec<Event>) -> Result<(), Fault> {
        if !matches!(event, Event::Start { .. }) {
            out.push(event);
        }
        Ok(())
    }
}

/// Takes every text.
#[derive(Clone)]
pub struct Silence;

impl Filter<Event> for Silence {
    fn event(&mut self, event: Event, out: &mut Vec<Event>) -> Result<(), Fault> {
        if !matches!(event, Event::Text { .. }) {
            out.push(event);
        }
        Ok(())
    }
}

/// Appends a footer to block `.0`, holding the finish until the end.
#[derive(Clone)]
pub struct Footer(pub u32, pub Option<Event>);

impl Filter<Event> for Footer {
    fn event(&mut self, event: Event, out: &mut Vec<Event>) -> Result<(), Fault> {
        match event {
            Event::Finish(_) => self.1 = Some(event),
            event => out.push(event),
        }
        Ok(())
    }

    fn end(&mut self, _outcome: &Outcome, out: &mut Vec<Event>) -> Result<(), Fault> {
        out.push(Event::text(self.0, " [footer]"));
        out.extend(self.1.take());
        Ok(())
    }
}
