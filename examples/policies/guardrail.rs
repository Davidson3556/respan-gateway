//! Use case 3, a guardrail no request skips. A term list checks every
//! response the client gets through a client-edge [`Holdback`], so a term
//! split across stream deltas is seen whole, and a response a plugin
//! answers with is covered too; and every call's input on the provider
//! edge, tools included, as the provider receives it, so a call any plugin
//! makes, or text any plugin adds, is covered. What it cannot read (an
//! image, a tool call in the answer, a second choice) is refused, never passed; this gateway
//! also serves Anthropic clients, which the guard reads. Traffic of another
//! kind is refused (see tests).
//!
//! So it is two plugins, and order is registration order. A plugin that
//! answers skips the plugins after it, so `guard` is registered first.
//! Input filters run in that order too, so `guard-input` is registered
//! last: register any plugin that adds to the input BEFORE it, or its text
//! goes unchecked. Tests pin both.
//!
//! `cargo run -p respan-gateway-examples --example guardrail`

#[path = "../support/text.rs"]
mod text;

use mockllm::{Answer, FakeOpenAi};
use respan_gateway_conversation::{Block, Input, Output};
use respan_gateway_http::Body;
use respan_gateway_http::Status;
use respan_gateway_http::bytes::Bytes;
use respan_gateway_http::http::{Request, StatusCode};
use respan_gateway_providers::{AnthropicMessages, OpenAiChat};
use respan_gateway_sdk::ErrorCode;
use respan_gateway_sdk::futures::stream;
use respan_gateway_sdk::{ClientContext, Fault, Gateway, Plugin, ProviderContext};
use respan_gateway_sdk::{Response, async_trait};
#[allow(dead_code)]
#[path = "../../tests/support/http.rs"]
mod http_support;
#[allow(dead_code)]
#[path = "../support/http.rs"]
mod provider_support;
use http_support::{post, read_whole, serve};
use serde_json::{Value, json};
use text::{Holdback, Readable, Rewrite, UNREADABLE, partial_match};

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
            return Err(Fault::new("blocked")
                .with(ErrorCode(BLOCKED.into()))
                .with(Status(StatusCode::BAD_REQUEST)));
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
    Fault::new("no guard reads this kind")
        .with(ErrorCode(UNCOVERED.into()))
        .with(Status(StatusCode::FORBIDDEN))
}

#[async_trait]
impl Plugin for Guard {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        if !cx.is_kind::<Input>() {
            return Err(uncovered());
        }
        cx.filter(Holdback::new(self.0.clone()))?;
        // This example allows text-only answers. Holdback can inspect
        // tool arguments, but this policy refuses tool calls outright.
        cx.filter(|reply: &mut Output| {
            let calls_a_tool = matches!(
                reply,
                Output::BlockStart {
                    kind: Block::ToolCall { .. },
                    ..
                }
            );
            if !calls_a_tool {
                Ok(())
            } else {
                let message = "the guard cannot read a tool call in the answer";
                Err(Fault::new(message)
                    .with(ErrorCode(UNREADABLE.into()))
                    .with(Status(StatusCode::UNPROCESSABLE_ENTITY)))
            }
        })?;
        Ok(None)
    }
}

#[async_trait]
impl Plugin for GuardInput {
    /// Every call's input, a judge's own included, after every other edit.
    async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
        if !px.is_kind::<Input>() {
            return Err(uncovered());
        }
        let terms = self.0.clone();
        px.filter(move |chat: &mut Input| terms.check(&chat.read_all()?))
    }
}

/// A stand-in for any plugin that answers, such as a cache: it answers the
/// model "cached" from memory, and its memory is poisoned.
struct Cache;

#[async_trait]
impl Plugin for Cache {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        if cx.input::<Input>()?.model.as_deref() != Some("cached") {
            return Ok(None);
        }
        let reply = [
            Output::start("c0", "cached"),
            Output::block_start(0, Block::Text),
            Output::text(0, "a bomb"),
            Output::block_end(0),
        ];
        Ok(Some(Response::stream(stream::iter(reply.map(Ok)))))
    }
}

/// The providers and formats, before any plugin. For real use, the
/// transport is
/// `HttpClient::new("https://api.openai.com/v1")?.with_headers(key)?`.
fn base(
    transport: impl Fn(respan_gateway_sdk::Extensions) -> respan_gateway_http::Outgoing
    + Send
    + Sync
    + 'static,
) -> Result<Gateway, Fault> {
    Gateway::new()
        .provider::<Input, _, _>("openai", OpenAiChat, transport)?
        .client::<Input, _, _>(OpenAiChat)?
        .client::<Input, _, _>(AnthropicMessages)
}

/// `guard` first, `guard-input` last, and `middle` between them.
fn guarded(gw: Gateway, middle: impl FnOnce(Gateway) -> Gateway) -> Result<Gateway, Fault> {
    let terms = Terms(vec!["bomb", "launch code"]);
    middle(gw.plugin(Guard(terms.clone())))
        .plugin(GuardInput(terms))
        .target("openai")
}

/// The guard around a cache, and `extra` registered after the cache and
/// before `guard-input`.
fn gateway_with(
    transport: impl Fn(respan_gateway_sdk::Extensions) -> respan_gateway_http::Outgoing
    + Send
    + Sync
    + 'static,
    extra: impl FnOnce(Gateway) -> Gateway,
) -> Result<Gateway, Fault> {
    guarded(base(transport)?, |gw| extra(gw.plugin(Cache)))
}

fn gateway(
    transport: impl Fn(respan_gateway_sdk::Extensions) -> respan_gateway_http::Outgoing
    + Send
    + Sync
    + 'static,
) -> Result<Gateway, Fault> {
    gateway_with(transport, |gw| gw)
}

/// A streamed OpenAI chat of one user message.
fn chat(model: &str, content: Value) -> Request<Body> {
    let body = json!({ "model": model, "stream": true,
        "messages": [{ "role": "user", "content": content }] });
    post("/v1/chat/completions", body.to_string())
}

/// The provider streams `deltas` as OpenAI chunks.
fn deltas(deltas: &[&str]) -> Answer {
    let chunk = |d| json!({ "id": "c1", "choices": [{ "index": 0, "delta": { "content": d } }] });
    let events: String = deltas
        .iter()
        .map(|d| format!("data: {}\n\n", chunk(d)))
        .collect();
    provider_support::sse(&[&events, "data: [DONE]\n\n"])
}

async fn send(gw: &Gateway, req: Request<Body>) -> (StatusCode, String) {
    let (status, _, body) = read_whole(serve(gw, req).await.0).await;
    let body = body.unwrap_or_else(|f| Bytes::from(f.to_string()));
    (status, String::from_utf8_lossy(&body).into_owned())
}

#[tokio::main]
async fn main() -> Result<(), Fault> {
    let fake = FakeOpenAi::start().await;
    let gw = gateway(provider_support::openai(&fake.base_url())?.connect())?;
    fake.answer(deltas(&["Chemistry is fun. A bo", "mb is not."]));
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
#[path = "../tests/cases/policies/guardrail.rs"]
mod tests;
