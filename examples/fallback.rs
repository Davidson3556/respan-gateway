//! Fallback, hedging and stalls (use cases 4 and 6). `Resilient` retries
//! the target as `Retry-After` asks, then falls back to a provider in
//! another format, or, for a request marked `x-hedge`, races a second
//! deployment started 200 ms later. A `Watchdog` fails OpenAI when it is
//! silent for 1 s, before its head or mid-stream. A stream that fails after
//! its first content reached the client is never retried. Not built here:
//! splitting an embedding batch and merging `/v1/models`.
//!
//! Run: `cargo run -p respan-gateway-examples --example fallback`

use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use respan_gateway_core::http::{self, HeaderValue, StatusCode, header::RETRY_AFTER};
use respan_gateway_core::{
    Body, ClientContext, Fault, Gateway, Outcome, Plugin, ProviderContext, Response, Transport,
    async_trait,
};
use respan_gateway_formats::conversation::{Conversation, set_model};
use respan_gateway_formats::{AnthropicMessages, OpenAiChat};
use respan_gateway_routing::{Attempt, Backoff, Watchdog, first_success, race};
use respan_gateway_testkit::{MockTransport, mock, post, read_whole};

/// Falls back from the target to Anthropic, or hedges it with `openai-eu`.
struct Resilient;

#[async_trait]
impl Plugin for Resilient {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let target = cx.target().map(str::to_owned);
        let Some(target) = target.filter(|_| cx.is_kind::<Conversation>()) else {
            return Ok(None);
        };
        if cx.head().headers.contains_key("x-hedge") {
            let calls = [cx.call(target), cx.call("openai-eu")];
            return race(calls, Duration::from_millis(200)).await.map(Some);
        }
        // Anthropic names its models its own way.
        let backup = cx.call("anthropic");
        let backup = backup.with_filter(set_model("claude-sonnet-4-5"));
        let backoff = Backoff::new(2, Duration::from_millis(250), Duration::from_secs(5));
        first_success([cx.call(target), backup], &backoff)
            .await
            .map(Some)
    }
}

/// Prints each attempt as "provider role end", and keeps it for the tests;
/// a deployment would send it to its log or billing store. The role is
/// settled by now: it is `Pending` only if the client left first.
#[derive(Clone, Default)]
struct Log(Arc<Mutex<Vec<String>>>);

#[async_trait]
impl Plugin for Log {
    async fn on_provider_end(&self, px: &ProviderContext, outcome: &Outcome) {
        let role = px.extensions().get::<Attempt>().map(Attempt::role);
        let end = outcome.end.as_ref().err().map_or("ok", |f| f.code());
        let line = format!("{} {role:?} {end}", px.provider());
        println!("  attempt {line}, usage {:?}", outcome.usage);
        let mut lines = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        lines.push(line);
    }
}

/// A transport that answers after a delay, to show a hedge or a stall.
struct Slow<T>(T, Duration);

#[async_trait]
impl<T: Transport> Transport for Slow<T> {
    async fn send(&self, call: http::Request<Body>) -> Result<http::Response<Body>, Fault> {
        tokio::time::sleep(self.1).await;
        self.0.send(call).await
    }
}

const OPENAI: &str = "http://openai.test/v1";
const OPENAI_EU: &str = "http://openai-eu.test/v1";
const ANTHROPIC: &str = "http://anthropic.test/v1";

/// The body the client got, and each attempt as logged, by provider.
type Run = Result<(String, Vec<String>), Fault>;

/// Runs `name` on a fresh gateway, OpenAI answering after `delay`, the
/// providers with `replies`, the request hedged if `hedge`.
async fn run(name: &str, delay: Duration, hedge: bool, replies: Vec<(&str, mock::Reply)>) -> Run {
    println!("{name}");
    let (mock, log) = (MockTransport::new(), Log::default());
    let openai = Watchdog::new(Slow(mock.at(OPENAI), delay), Duration::from_secs(1));
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
    let body = r#"{"model":"gpt-4o","stream":true,"messages":[{"role":"user","content":"hi"}]}"#;
    let mut req = post("/v1/chat/completions", body);
    if hedge {
        let hedge = HeaderValue::from_static("1");
        req.headers_mut().insert("x-hedge", hedge);
    }
    let (status, _, body) = read_whole(gw.handle(req).await.0).await;
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
    run(name, Duration::ZERO, false, replies).await
}

async fn cut_after_first_words() -> Run {
    let name = "OpenAI's stream breaks after its first words: committed, not retried";
    let replies = vec![(OPENAI, openai("Hello", true))];
    run(name, Duration::ZERO, false, replies).await
}

async fn stalled_then_fallback() -> Run {
    let name = "OpenAI never answers: the watchdog fails each try after 1 s; Anthropic answers";
    run(name, Duration::from_secs(3600), false, vec![]).await
}

async fn hedged() -> Run {
    let (late, fast) = (openai("late", false), openai("fast", false));
    let replies = vec![(OPENAI, late), (OPENAI_EU, fast)];
    let name = "Hedged: OpenAI takes 900 ms; openai-eu starts 200 ms in and wins";
    run(name, Duration::from_millis(900), true, replies).await
}

#[tokio::main]
async fn main() -> Result<(), Fault> {
    rate_limited_then_fallback().await?;
    cut_after_first_words().await?;
    stalled_then_fallback().await?;
    hedged().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use tokio::time::Instant;

    use super::*;

    const LOST: &str = "openai Some(Loser) upstream";
    const CLAUDE: &str = "anthropic Some(Winner) ok";

    #[tokio::test(start_paused = true)]
    async fn retry_after_is_honoured_then_it_falls_back_across_formats() -> Result<(), Fault> {
        let start = Instant::now();
        let (body, attempts) = rate_limited_then_fallback().await?;
        assert_eq!(attempts, [CLAUDE, LOST, LOST]);
        assert!(body.contains("Hi from Claude") && start.elapsed() >= Duration::from_secs(1));
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn a_stream_cut_after_its_first_byte_is_not_retried() -> Result<(), Fault> {
        let (body, attempts) = cut_after_first_words().await?;
        assert!(body.contains("Hello") && body.contains("reset"), "{body}");
        assert_eq!(attempts, ["openai Some(Winner) upstream"]);
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn a_stalled_provider_is_cut_by_the_watchdog_and_falls_back() -> Result<(), Fault> {
        let start = Instant::now();
        let (body, attempts) = stalled_then_fallback().await?;
        assert_eq!(attempts, [CLAUDE, LOST, LOST]);
        assert!(body.contains("Hi from Claude") && start.elapsed() < Duration::from_secs(3));
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn a_hedge_keeps_the_first_to_answer_and_abandons_the_other() -> Result<(), Fault> {
        let (body, attempts) = hedged().await?;
        assert!(body.contains("fast") && !body.contains("late"), "{body}");
        let lost = "openai Some(Loser) abandoned";
        assert_eq!(attempts, [lost, "openai-eu Some(Winner) ok"]);
        Ok(())
    }
}
