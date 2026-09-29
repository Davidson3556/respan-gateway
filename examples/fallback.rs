//! Fallback, hedging and stalls (use cases 4 and 6). `Resilient` retries
//! the target as `Retry-After` asks, then falls back to a provider in
//! another format, or, for a request marked `x-hedge`, races a second
//! deployment started 200 ms later. A `Watchdog` fails OpenAI when its head
//! takes over 2 s or its stream goes silent for 1 s. A stream that fails after
//! its first content reached the client is never retried. Not built here:
//! splitting an embedding batch and merging `/v1/models`.
//!
//! `cargo run -p respan-gateway-examples --example fallback`

use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use respan_gateway_core::{
    ClientContext, Exchange, Fault, Gateway, Outcome, Plugin, ProviderContext, Response, Transport,
    async_trait,
};
use respan_gateway_formats::conversation::{Conversation, set_model};
use respan_gateway_formats::{AnthropicMessages, OpenAiChat};
use respan_gateway_http::Asked;
use respan_gateway_http::Body;
use respan_gateway_http::http::{self, HeaderValue, StatusCode, header::RETRY_AFTER};
use respan_gateway_routing::{Attempt, Backoff, Until, Watchdog, first_success, race};
use respan_gateway_testkit::{MockTransport, mock, post, read_whole, serve};

/// Falls back from the target to Anthropic, or hedges it with `openai-eu`.
struct Resilient;

#[async_trait]
impl Plugin for Resilient {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let target = cx.target().map(str::to_owned);
        let Some(target) = target.filter(|_| cx.is_kind::<Conversation>()) else {
            return Ok(None);
        };
        // Each answer is judged at its first content; `Until::End` would
        // hold each whole and race for the first to finish.
        let asked = cx.extensions().get::<Asked>();
        if asked.is_some_and(|a| a.headers.contains_key("x-hedge")) {
            let calls = [cx.call(target), cx.call("openai-eu")];
            let stagger = Duration::from_millis(200);
            return race(calls, stagger, Until::Content).await.map(Some);
        }
        let backoff = Backoff::new(2, Duration::from_millis(250), Duration::from_secs(5));
        let calls = [cx.call(target), cx.call("anthropic")];
        first_success(calls, &backoff, Until::Content)
            .await
            .map(Some)
    }

    /// Anthropic names its models its own way: a change for one provider's
    /// calls goes here, keyed on the provider.
    async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
        if px.provider() == "anthropic" && px.is_kind::<Conversation>() {
            px.filter(set_model("claude-sonnet-4-5"))?;
        }
        Ok(())
    }
}

/// Prints each attempt as "provider model #index role end", and keeps it for the tests;
/// a deployment would send it to its log or billing store. Only the
/// attempt whose response reached the client is `Winner`.
#[derive(Clone, Default)]
struct Log(Arc<Mutex<Vec<String>>>);

#[async_trait]
impl Plugin for Log {
    async fn on_provider_end(&self, px: &ProviderContext, outcome: &Outcome) {
        let Some(attempt) = px.client().extensions().get::<Attempt>() else {
            return;
        };
        let (index, role) = (attempt.index(), attempt.role());
        let end = outcome.end.as_ref().err().map_or("ok", |f| f.code());
        let chat = px.input::<Conversation>().ok();
        let model = chat.and_then(|c| c.model.as_deref()).unwrap_or("-");
        let line = format!("{} {model} #{index} {role:?} {end}", px.provider());
        println!("  attempt {line}");
        let mut lines = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        lines.push(line);
    }
}

/// A transport that answers after a delay, to show a hedge or a stall.
struct Slow<T>(T, Duration);

#[async_trait]
impl<T: Transport> Transport for Slow<T> {
    type Ask = T::Ask;
    type Answer = T::Answer;

    async fn send(&self, call: Exchange<T::Ask>) -> Result<Exchange<T::Answer>, Fault> {
        tokio::time::sleep(self.1).await;
        self.0.send(call).await
    }
}

const OPENAI: &str = "http://openai.test/v1";
const OPENAI_EU: &str = "http://openai-eu.test/v1";
const ANTHROPIC: &str = "http://anthropic.test/v1";

/// The body the client got, and each attempt as logged, by provider.
type Run = Result<(String, Vec<String>), Fault>;

type Replies = Vec<(&'static str, mock::Reply)>;

/// A streamed chat request.
fn chat() -> http::Request<Body> {
    let body = r#"{"model":"gpt-4o","stream":true,"messages":[{"role":"user","content":"hi"}]}"#;
    post("/v1/chat/completions", body)
}

/// Runs `req` as `name` on a fresh gateway, OpenAI answering after `delay`,
/// the providers with `replies`.
async fn run(name: &str, delay: Duration, req: http::Request<Body>, replies: Replies) -> Run {
    println!("{name}");
    let (mock, log) = (MockTransport::new(), Log::default());
    // A whole answer's head comes only once it is done: a deployment waits
    // minutes for it, and seconds between stream events.
    let (head, idle) = (Duration::from_secs(2), Duration::from_secs(1));
    let openai = Watchdog::new(Slow(mock.at(OPENAI), delay), head, idle);
    let gw = Gateway::new()
        .serve(OpenAiChat)?
        .provider("openai", OpenAiChat, openai)?
        .provider("openai-eu", OpenAiChat, mock.at(OPENAI_EU))?
        .provider("anthropic", AnthropicMessages, mock.at(ANTHROPIC))?
        .plugin("log", log.clone())?
        .plugin("resilient", Resilient)?
        .target("openai")?;
    for (url, reply) in replies.into_iter().chain([(ANTHROPIC, anthropic())]) {
        mock.reply(url, reply);
    }
    let (status, _, body) = read_whole(serve(&gw, req).await.0).await;
    gw.idle().await;
    let body = String::from_utf8_lossy(&body?).into_owned();
    let last = body.trim_end().lines().last().unwrap_or_default();
    println!("  client: {status}, last event: {last}");
    let mut attempts = log.0.lock().unwrap_or_else(PoisonError::into_inner).clone();
    attempts.sort();
    Ok((body, attempts))
}

/// An OpenAI stream saying `text`, then its usage; `cut` breaks it off
/// after its first words.
fn openai(text: &str, cut: bool) -> mock::Reply {
    let say = r#"{"choices":[{"index":0,"delta":{"content":"TEXT"}}]}"#.replace("TEXT", text);
    let usage = r#"{"choices":[],"usage":{"prompt_tokens":5,"completion_tokens":2}}"#;
    let events = [&say, usage, "[DONE]"].map(|data| format!("data: {data}\n\n"));
    let mut reply = mock::sse(&events.each_ref().map(String::as_str));
    if cut {
        let body = reply.body_mut();
        body.truncate(1);
        body.push(Err(Fault::upstream("connection reset")));
    }
    reply
}

fn anthropic() -> mock::Reply {
    mock::sse(&[
        "data: {\"type\":\"message_start\",\"message\":{\"id\":\"m\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"claude-sonnet-4-5\",\"content\":[],\"usage\":{\"input_tokens\":5,\"output_tokens\":1}}}\n\n",
        "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"Hi from Claude\"}}\n\n",
        "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":3}}\n\n",
        "data: {\"type\":\"message_stop\"}\n\n",
    ])
}

fn rate_limited() -> mock::Reply {
    let mut reply = mock::json(StatusCode::TOO_MANY_REQUESTS, "{}");
    let one = HeaderValue::from_static("1");
    reply.headers_mut().insert(RETRY_AFTER, one);
    reply
}

async fn rate_limited_then_fallback() -> Run {
    let replies = vec![(OPENAI, rate_limited()), (OPENAI, rate_limited())];
    let name = "OpenAI answers 429 with Retry-After: 1, twice; Anthropic answers";
    run(name, Duration::ZERO, chat(), replies).await
}

async fn cut_after_first_words() -> Run {
    let name = "OpenAI's stream breaks after its first words: committed, not retried";
    let replies = vec![(OPENAI, openai("Hello", true))];
    run(name, Duration::ZERO, chat(), replies).await
}

async fn stalled_then_fallback() -> Run {
    let name = "OpenAI never answers: the watchdog fails each try after 2 s; Anthropic answers";
    run(name, Duration::from_secs(3600), chat(), vec![]).await
}

async fn slow_whole_answer() -> Run {
    let ok = r#"{"id":"c","model":"gpt-4o","choices":[{"index":0,"finish_reason":"stop","message":{"role":"assistant","content":"an essay"}}]}"#;
    let replies = vec![(OPENAI, mock::json(StatusCode::OK, ok))];
    let body = r#"{"model":"gpt-4o","messages":[{"role":"user","content":"write an essay"}]}"#;
    let name = "OpenAI's whole answer takes 1.5 s: within the head bound, not cut";
    let req = post("/v1/chat/completions", body);
    run(name, Duration::from_millis(1500), req, replies).await
}

async fn hedged() -> Run {
    let (late, fast) = (openai("late", false), openai("fast", false));
    let replies = vec![(OPENAI, late), (OPENAI_EU, fast)];
    let name = "Hedged: OpenAI takes 900 ms; openai-eu starts 200 ms in and wins";
    let mut req = chat();
    req.headers_mut()
        .insert("x-hedge", HeaderValue::from_static("1"));
    run(name, Duration::from_millis(900), req, replies).await
}

#[tokio::main]
async fn main() -> Result<(), Fault> {
    rate_limited_then_fallback().await?;
    cut_after_first_words().await?;
    stalled_then_fallback().await?;
    slow_whole_answer().await?;
    hedged().await?;
    Ok(())
}

#[cfg(test)]
#[path = "tests/fallback.rs"]
mod tests;
