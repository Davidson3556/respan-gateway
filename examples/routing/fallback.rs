//! Fallback, hedging and stalls (use cases 4 and 6). `Resilient` retries
//! the target as `Retry-After` asks, then falls back to a provider in
//! another format, or, for a request marked `x-hedge`, races a second
//! deployment started 200 ms later. A `watchdog` fails OpenAI when its head
//! takes over 2 s or its stream goes silent for 1 s. A stream that fails after
//! its first content reached the client is never retried. Not built here:
//! splitting an embedding batch and merging `/v1/models`.
//!
//! `cargo run -p respan-gateway-examples --example fallback`

use respan_gateway_examples::LIMIT;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use mockllm::{Answer, FakeAnthropic, FakeOpenAi};
use respan_gateway_anthropic::AnthropicMessages;
use respan_gateway_conversation::{Input, Output, set_model};
use respan_gateway_http::http::{self, HeaderValue};
use respan_gateway_http::{Body, Wire, retryable, watchdog};
use respan_gateway_openai::OpenAiChat;
use respan_gateway_plugins::routing::{Attempt, Backoff, Until, first_success, race};
use respan_gateway_sdk::{
    ClientContext, Fault, Gateway, Plugin, ProviderContext, Response, async_trait,
};
#[allow(dead_code)]
#[path = "../../tests/support/http.rs"]
mod http_support;
use http_support::{post, read_whole, serve};
#[allow(dead_code)]
#[path = "../support/http.rs"]
mod provider_support;

/// Falls back from the target to Anthropic, or hedges it with `openai-eu`.
struct Resilient;

#[async_trait]
impl Plugin for Resilient {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let target = cx.target().map(str::to_owned);
        let Some(target) = target.filter(|_| cx.is_kind::<Input>()) else {
            return Ok(None);
        };
        // Each answer is judged at its first content; `Until::End` would
        // hold each whole and race for the first to finish.
        let asked = cx.extensions().get::<Wire>().and_then(Wire::get);
        if asked.is_some_and(|a| a.headers().contains_key("x-hedge")) {
            let calls = [cx.call(target), cx.call("openai-eu")];
            let stagger = Duration::from_millis(200);
            return race(calls, stagger, Until::content(Output::is_content))
                .await
                .map(Some);
        }
        let backoff = Backoff::new(2, Duration::from_millis(250), Duration::from_secs(5));
        let calls = [cx.call(target), cx.call("anthropic")];
        first_success(
            calls,
            &backoff,
            Until::content(Output::is_content),
            retryable,
        )
        .await
        .map(Some)
    }

    /// Anthropic names its models its own way: a change for one provider's
    /// calls goes here, keyed on the provider.
    async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
        if px.provider() == "anthropic" && px.is_kind::<Input>() {
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
    async fn on_provider_end(&self, px: &ProviderContext, end: &Result<(), Fault>) {
        let Some(attempt) = px.extensions().get::<Attempt>() else {
            return;
        };
        let (index, role) = (attempt.index(), attempt.role());
        let end = end
            .as_ref()
            .err()
            .map_or("ok", respan_gateway_http::error_code);
        let chat = px.input::<Input>().ok();
        let model = chat.and_then(|c| c.model.as_deref()).unwrap_or("-");
        let line = format!("{} {model} #{index} {role:?} {end}", px.provider());
        println!("  attempt {line}");
        let mut lines = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        lines.push(line);
    }
}

#[derive(Clone, Copy)]
enum Provider {
    OpenAi,
    Europe,
}

/// The body the client got, and each attempt as logged, by provider.
type Run = Result<(String, Vec<String>, [usize; 3]), Fault>;

type Replies = Vec<(Provider, Answer)>;

/// A streamed chat request.
fn chat() -> http::Request<Body> {
    let body = r#"{"model":"gpt-4o","stream":true,"messages":[{"role":"user","content":"hi"}]}"#;
    post("/v1/chat/completions", body)
}

/// Runs `req` as `name` on a fresh gateway, OpenAI answering after `delay`,
/// the providers with `replies`.
async fn run(name: &str, delay: Duration, req: http::Request<Body>, replies: Replies) -> Run {
    println!("{name}");
    let (openai, europe, claude) = (
        FakeOpenAi::start().await,
        FakeOpenAi::start().await,
        FakeAnthropic::start().await,
    );
    let log = Log::default();
    for (provider, answer) in replies {
        match provider {
            Provider::OpenAi => openai.answer(answer),
            Provider::Europe => europe.answer(answer),
        }
    }
    claude.answer(anthropic());
    // A whole answer's head comes only once it is done: a deployment waits
    // minutes for it, and seconds between stream events.
    let (head, idle) = (Duration::from_secs(2), Duration::from_secs(1));
    let backend = provider_support::openai(&openai.base_url())?;
    let primary = watchdog(
        move |request| {
            let backend = backend.clone();
            async move {
                tokio::time::sleep(delay).await;
                backend.send(request).await
            }
        },
        head,
        idle,
    );
    let gw = Gateway::new()
        .client::<Input, _, _>(OpenAiChat::OPENAI)?
        .provider::<Input, _, _>(
            "openai",
            OpenAiChat::OPENAI,
            respan_gateway_http::connect(primary, LIMIT),
        )?
        .provider::<Input, _, _>(
            "openai-eu",
            OpenAiChat::OPENAI,
            provider_support::openai(&europe.base_url())?.connect(),
        )?
        .provider::<Input, _, _>(
            "anthropic",
            AnthropicMessages,
            provider_support::anthropic(&claude.base_url())?.connect(),
        )?
        .plugin(log.clone())
        .plugin(Resilient)
        .target("openai")?;
    let (status, _, body) = read_whole(serve(&gw, req).await.0).await;
    gw.idle().await;
    let body = String::from_utf8_lossy(&body?).into_owned();
    let last = body.trim_end().lines().last().unwrap_or_default();
    println!("  client: {status}, last event: {last}");
    let mut attempts = log.0.lock().unwrap_or_else(PoisonError::into_inner).clone();
    attempts.sort();
    let counts = [
        openai.requests().len(),
        europe.requests().len(),
        claude.requests().len(),
    ];
    Ok((body, attempts, counts))
}

/// An OpenAI stream saying `text`, then its usage; `cut` breaks it off
/// after its first words.
fn openai(text: &str, cut: bool) -> Answer {
    let say = r#"{"choices":[{"index":0,"delta":{"content":"TEXT"}}]}"#.replace("TEXT", text);
    let usage = r#"{"choices":[],"usage":{"prompt_tokens":5,"completion_tokens":2}}"#;
    let events = [&say, usage, "[DONE]"].map(|data| format!("data: {data}\n\n"));
    let reply = provider_support::sse(&events.each_ref().map(String::as_str));
    if cut { reply.cut_after(1) } else { reply }
}

fn anthropic() -> Answer {
    provider_support::sse(&[
        "data: {\"type\":\"message_start\",\"message\":{\"id\":\"m\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"claude-sonnet-4-5\",\"content\":[],\"usage\":{\"input_tokens\":5,\"output_tokens\":1}}}\n\n",
        "data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n",
        "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"Hi from Claude\"}}\n\n",
        "data: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
        "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":3}}\n\n",
        "data: {\"type\":\"message_stop\"}\n\n",
    ])
}

fn rate_limited() -> Answer {
    Answer::error(429)
        .message("rate limit exceeded")
        .retry_after(1)
}

async fn rate_limited_then_fallback() -> Run {
    let replies = vec![
        (Provider::OpenAi, rate_limited()),
        (Provider::OpenAi, rate_limited()),
    ];
    let name = "OpenAI answers 429 with Retry-After: 1, twice; Anthropic answers";
    run(name, Duration::ZERO, chat(), replies).await
}

async fn cut_after_first_words() -> Run {
    let name = "OpenAI's stream breaks after its first words: committed, not retried";
    let replies = vec![(Provider::OpenAi, openai("Hello", true))];
    run(name, Duration::ZERO, chat(), replies).await
}

async fn stalled_then_fallback() -> Run {
    let name = "OpenAI never answers: the watchdog fails each try after 2 s; Anthropic answers";
    run(name, Duration::from_secs(3600), chat(), vec![]).await
}

async fn slow_whole_answer() -> Run {
    let ok = r#"{"id":"c","model":"gpt-4o","choices":[{"index":0,"finish_reason":"stop","message":{"role":"assistant","content":"an essay"}}]}"#;
    let replies = vec![(Provider::OpenAi, Answer::raw(ok))];
    let body = r#"{"model":"gpt-4o","messages":[{"role":"user","content":"write an essay"}]}"#;
    let name = "OpenAI's whole answer takes 1.5 s: within the head bound, not cut";
    let req = post("/v1/chat/completions", body);
    run(name, Duration::from_millis(1500), req, replies).await
}

async fn hedged() -> Run {
    let (late, fast) = (openai("late", false), openai("fast", false));
    let replies = vec![(Provider::OpenAi, late), (Provider::Europe, fast)];
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
#[path = "../tests/cases/routing/fallback.rs"]
mod tests;
