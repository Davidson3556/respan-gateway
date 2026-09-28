//! Use case 3, a guardrail no request skips. A term list checks every
//! response the client gets through a client-edge [`Holdback`], so a term
//! split across stream deltas is seen whole, and a response a plugin
//! answers with is covered too; and every call's input on the provider
//! edge, tools included, as the provider receives it, so a call any plugin
//! makes, or text any plugin adds, is covered. What it cannot read (an
//! image, a tool call in the answer) is refused, never passed; this gateway
//! also serves Anthropic clients, which the guard reads. Traffic of another
//! kind is refused (see tests).
//!
//! So it is two plugins. A plugin that answers skips the plugins after it,
//! so `guard` is registered first. Input filters run in plugin order, so
//! `guard-input` runs last: a plugin registered later that edits the input
//! goes before it, `.after("guard-input", [name])`. Tests pin both.
//!
//! `cargo run -p respan-gateway-examples --example guardrail`

use respan_gateway_core::bytes::Bytes;
use respan_gateway_core::futures::stream;
use respan_gateway_core::serde_json::{Value, json};
use respan_gateway_core::{ClientContext, Fault, Gateway, Plugin, ProviderContext};
use respan_gateway_core::{Response, async_trait};
use respan_gateway_formats::conversation::{Conversation, Piece, Reply};
use respan_gateway_formats::{AnthropicMessages, OpenAiChat};
use respan_gateway_http::Body;
use respan_gateway_http::http::{Request, StatusCode};
use respan_gateway_testkit::{MockTransport, mock, post, read_whole, serve};
use respan_gateway_text::{Holdback, Readable, Rewrite, UNREADABLE, partial_match};

const OPENAI: &str = "http://openai.test/v1";
const BLOCKED: &str = "content_blocked";
const UNCOVERED: &str = "guard_uncovered";

/// Blocked terms, in lower case, matched case-insensitively. A classifier
/// fits behind the same two methods: `pending` holds what it has not
/// decided on yet.
#[derive(Clone)]
struct Terms(Vec<&'static str>);

impl Terms {
    fn check(&self, text: &str) -> Result<(), Fault> {
        let text = text.to_lowercase();
        if self.0.iter().any(|term| text.contains(term)) {
            return Err(Fault::new(StatusCode::BAD_REQUEST, BLOCKED, "blocked"));
        }
        Ok(())
    }
}

impl Rewrite for Terms {
    fn pending(&self, text: &str) -> usize {
        partial_match(&text.to_lowercase(), self.0.iter().copied())
    }

    fn rewrite(&mut self, text: &str) -> Result<String, Fault> {
        self.check(text).map(|()| text.to_owned())
    }
}

/// The client edge: every response, whoever answers it. Registered first.
struct Guard(Terms);

/// The provider edge: every call's input. Registered last.
struct GuardInput(Terms);

fn uncovered() -> Fault {
    Fault::new(StatusCode::FORBIDDEN, UNCOVERED, "no guard reads this kind")
}

#[async_trait]
impl Plugin for Guard {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        if !cx.is_kind::<Conversation>() {
            return Err(uncovered());
        }
        cx.filter(Holdback::new(self.0.clone()))?;
        // A streamed tool call's arguments come in fragments no check sees
        // whole, so the guard refuses tool calls in answers.
        cx.filter(|reply: &mut Reply| {
            let text_only = reply
                .content
                .iter()
                .all(|p| matches!(p, Piece::Text { .. }));
            if text_only {
                Ok(())
            } else {
                let message = "the guard cannot read a tool call in the answer";
                Err(Fault::new(
                    StatusCode::UNPROCESSABLE_ENTITY,
                    UNREADABLE,
                    message,
                ))
            }
        })?;
        Ok(None)
    }
}

#[async_trait]
impl Plugin for GuardInput {
    /// Every call's input, a judge's own included, after every other edit.
    async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
        if !px.is_kind::<Conversation>() {
            return Err(uncovered());
        }
        let terms = self.0.clone();
        px.filter(move |chat: &mut Conversation| terms.check(&chat.read_all()?))
    }
}

/// A stand-in for any plugin that answers, such as a cache: it answers the
/// model "cached" from memory, and its memory is poisoned.
struct Cache;

#[async_trait]
impl Plugin for Cache {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        if cx.input::<Conversation>()?.model.as_deref() != Some("cached") {
            return Ok(None);
        }
        let reply = [Reply::start("c0", "cached"), Reply::text(0, "a bomb")];
        Ok(Some(Response::stream(stream::iter(reply.map(Ok)))))
    }
}

/// `guard` first, `guard-input` last. For real use, the transport is
/// `HttpTransport::new("https://api.openai.com/v1")?.with_headers(key)?`.
fn gateway(mock: &MockTransport) -> Result<Gateway, Fault> {
    let terms = Terms(vec!["bomb", "launch code"]);
    Gateway::new()
        .provider("openai", OpenAiChat, mock.at(OPENAI))?
        .serve(OpenAiChat)?
        .serve(AnthropicMessages)?
        .plugin("guard", Guard(terms.clone()))?
        .plugin("cache", Cache)?
        .plugin("guard-input", GuardInput(terms))?
        .target("openai")
}

/// A streamed OpenAI chat of one user message.
fn chat(model: &str, content: Value) -> Request<Body> {
    let body = json!({ "model": model, "stream": true,
        "messages": [{ "role": "user", "content": content }] });
    post("/v1/chat/completions", body.to_string())
}

/// The provider streams `deltas` as OpenAI chunks.
fn deltas(deltas: &[&str]) -> mock::Reply {
    let chunk = |d| json!({ "id": "c1", "choices": [{ "index": 0, "delta": { "content": d } }] });
    let events: String = deltas
        .iter()
        .map(|d| format!("data: {}\n\n", chunk(d)))
        .collect();
    mock::sse(&[&events, "data: [DONE]\n\n"])
}

async fn send(gw: &Gateway, req: Request<Body>) -> (StatusCode, String) {
    let (status, _, body) = read_whole(serve(gw, req).await.0).await;
    let body = body.unwrap_or_else(|f| Bytes::from(f.to_string()));
    (status, String::from_utf8_lossy(&body).into_owned())
}

#[tokio::main]
async fn main() -> Result<(), Fault> {
    let mock = MockTransport::new();
    let gw = gateway(&mock)?;
    mock.reply(OPENAI, deltas(&["Chemistry is fun. A bo", "mb is not."]));
    for (what, req) in [
        ("term split across deltas", chat("gpt", "chemistry?".into())),
        ("poisoned cache", chat("cached", "hi".into())),
    ] {
        let (status, body) = send(&gw, req).await;
        println!("{what}: {status}\n{body}\n");
    }
    Ok(())
}

#[cfg(test)]
#[path = "tests/guardrail.rs"]
mod tests;
