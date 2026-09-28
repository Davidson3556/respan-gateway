//! A local proxy in front of Claude Code or Codex: typed Anthropic Messages
//! beside a passthrough of no kind for every other path.

mod common;

use common::{send, whole};
use respan_gateway_core::http::{self, StatusCode};
use respan_gateway_core::{Body, Gateway, codes};
use respan_gateway_formats::AnthropicMessages;
use respan_gateway_mcp::Passthrough;
use respan_gateway_testkit::{Calls, MockTransport, mock};

const ANTHROPIC: &str = "http://anthropic.test";
const OPENAI: &str = "http://openai.test";

/// Claude Code's request, with a beta header, its key and a field no
/// gateway has heard of.
fn claude(path: &str, body: &'static str) -> http::Request<Body> {
    http::Request::post(path)
        .header("x-api-key", "sk-ant-user")
        .header("anthropic-beta", "token-counting-2099-01-01")
        .header("content-type", "application/json")
        .body(whole(body))
        .unwrap_or_else(|e| panic!("{e}"))
}

/// The proxy: Messages typed, two passthroughs, each to its own provider,
/// the first default target each request can use; no router.
fn proxy(mock: &MockTransport) -> Gateway {
    let anthropic_raw = Passthrough::new("anthropic-raw", ["/v1/messages/count_tokens"]);
    let openai_raw = Passthrough::new("openai-raw", ["/v1/responses"]);
    let providers = Gateway::new()
        .provider("anthropic", AnthropicMessages, mock.at(ANTHROPIC))
        .and_then(|g| g.provider("anthropic-raw", anthropic_raw.clone(), mock.at(ANTHROPIC)))
        .and_then(|g| g.provider("openai-raw", openai_raw.clone(), mock.at(OPENAI)));
    providers
        .and_then(|g| g.serve(AnthropicMessages))
        .and_then(|g| g.serve(anthropic_raw))
        .and_then(|g| g.serve(openai_raw))
        .and_then(|g| g.target("anthropic"))
        .and_then(|g| g.target("anthropic-raw"))
        .and_then(|g| g.target("openai-raw"))
        .unwrap_or_else(|e| panic!("{e}"))
}

#[tokio::test]
async fn count_tokens_passes_every_byte_header_and_query_untouched() {
    let mock = MockTransport::new();
    mock.reply(
        ANTHROPIC,
        mock::json(StatusCode::OK, r#"{"input_tokens":12}"#),
    );
    let gw = proxy(&mock);

    let body = r#"{"model":"claude-x",  "messages":[],"brand_new_field":{"a":1}}"#;
    let (status, _, got, outcome) =
        send(&gw, claude("/v1/messages/count_tokens?beta=true", body)).await;

    assert_eq!(
        (status, got.as_str()),
        (StatusCode::OK, r#"{"input_tokens":12}"#)
    );
    let sent = &mock.sent()[0];
    assert_eq!(
        sent.uri(),
        "http://anthropic.test/v1/messages/count_tokens?beta=true"
    );
    assert_eq!(sent.body(), body.as_bytes());
    let header = |n: &str| {
        sent.headers()
            .get(n)
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned)
    };
    assert_eq!(header("x-api-key").as_deref(), Some("sk-ant-user"));
    assert_eq!(
        header("anthropic-beta").as_deref(),
        Some("token-counting-2099-01-01")
    );
    // Nothing of no kind reports usage.
    assert!(outcome.is_some_and(|o| o.end.is_ok() && o.usage.is_empty()));
}

#[tokio::test]
async fn responses_stream_to_codex_as_they_came() {
    let mock = MockTransport::new();
    let events = [
        "event: response.created\ndata: {\"a\":1}\n\n",
        "data: [DONE]\n\n",
    ];
    mock.reply(OPENAI, mock::sse(&events));
    let gw = proxy(&mock);

    let (status, headers, got, _) = send(
        &gw,
        claude("/v1/responses", r#"{"model":"gpt-x","stream":true}"#),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        headers.get("content-type").map(|v| v.as_bytes()),
        Some(&b"text/event-stream"[..])
    );
    assert_eq!(got, events.concat());
}

#[tokio::test]
async fn a_passthrough_error_keeps_the_providers_status_and_body() {
    let mock = MockTransport::new();
    let error = r#"{"type":"error","error":{"type":"rate_limit_error","message":"slow"}}"#;
    mock.reply(ANTHROPIC, mock::json(StatusCode::TOO_MANY_REQUESTS, error));
    let gw = proxy(&mock);

    let (status, _, got, _) = send(&gw, claude("/v1/messages/count_tokens", "{}")).await;

    assert_eq!(
        (status, got.as_str()),
        (StatusCode::TOO_MANY_REQUESTS, error)
    );
}

#[tokio::test]
async fn messages_stay_typed_beside_the_passthroughs() {
    let mock = MockTransport::new();
    let reply = r#"{"id":"m","type":"message","role":"assistant","content":[],"model":"claude-x","stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":1}}"#;
    mock.reply(ANTHROPIC, mock::json(StatusCode::OK, reply));
    let calls = Calls::default();
    let gw = proxy(&mock)
        .plugin("calls", std::sync::Arc::new(calls.clone()))
        .unwrap_or_else(|e| panic!("{e}"));

    let body = r#"{"model":"claude-x","max_tokens":1,"messages":[]}"#;
    let (status, _, _, outcome) = send(&gw, claude("/v1/messages", body)).await;

    assert_eq!(status, StatusCode::OK);
    // A call's usage counts on its provider edge only.
    assert!(!calls.usage().is_empty());
    assert!(outcome.is_some_and(|o| o.usage.is_empty()));
}

/// FINDINGS F7 (fixed by C10): each request goes to the first default
/// target it can use, so no router is needed.
#[tokio::test]
async fn each_format_finds_its_provider_without_a_router() {
    let mock = MockTransport::new();
    mock.reply(
        ANTHROPIC,
        mock::json(StatusCode::OK, r#"{"input_tokens":12}"#),
    );
    let gw = proxy(&mock);

    let (status, _, _, _) = send(&gw, claude("/v1/messages/count_tokens", "{}")).await;

    assert_eq!(status, StatusCode::OK);
}

/// C10: a format of no kind that no provider speaks fails at `serve`.
#[test]
fn serving_a_passthrough_no_provider_speaks_fails_at_build() {
    let mock = MockTransport::new();
    let gw = Gateway::new()
        .provider("anthropic", AnthropicMessages, mock.at(ANTHROPIC))
        .and_then(|g| g.serve(Passthrough::new("raw", [""])));
    assert!(gw.is_err_and(|f| f.code() == codes::CONFIG));
}

/// Falls back over two upstreams of no kind, checking each with `ready`.
struct RawFallback;

#[respan_gateway_core::async_trait]
impl respan_gateway_core::Plugin for RawFallback {
    async fn on_client(
        &self,
        cx: &mut respan_gateway_core::ClientContext,
    ) -> Result<Option<respan_gateway_core::Response>, respan_gateway_core::Fault> {
        // The documented idiom (Q-7): an error status is relayed as a
        // response; only a success that fails before content is a fault.
        let mut last = Err(respan_gateway_core::Fault::upstream("no provider"));
        for provider in ["raw-1", "raw-2"] {
            let resp = cx.call(provider).await?;
            if !resp.head().status.is_success() {
                last = Ok(resp);
                continue;
            }
            match resp.ready().await {
                Ok(resp) => return Ok(Some(resp)),
                Err(fault) => last = Err(fault),
            }
        }
        last.map(Some)
    }
}

/// E-3: a fallback works on a response of no kind: after a 429 the next
/// upstream's bytes are sent as they came.
#[tokio::test]
async fn ready_falls_back_over_traffic_of_no_kind() {
    let mock = MockTransport::new();
    let error = r#"{"type":"error","error":{"type":"rate_limit_error"}}"#;
    mock.reply(ANTHROPIC, mock::json(StatusCode::TOO_MANY_REQUESTS, error));
    mock.reply(OPENAI, mock::json(StatusCode::OK, r#"{"input_tokens":3}"#));
    let raw = Passthrough::new("raw", [""]);
    let gw = Gateway::new()
        .provider("raw-1", raw.clone(), mock.at(ANTHROPIC))
        .and_then(|g| g.provider("raw-2", raw.clone(), mock.at(OPENAI)))
        .and_then(|g| g.serve(raw))
        .and_then(|g| g.plugin("fallback", std::sync::Arc::new(RawFallback)))
        .unwrap_or_else(|e| panic!("{e}"));

    let (status, _, got, _) = send(&gw, claude("/v1/messages/count_tokens", "{}")).await;

    assert_eq!(
        (status, got.as_str()),
        (StatusCode::OK, r#"{"input_tokens":3}"#)
    );
    assert_eq!(mock.sent().len(), 2);
}

/// R5 (fixed by the documented idiom, Q-7): the last error status is
/// relayed as a response, so its body and headers reach the client.
#[tokio::test]
async fn the_last_failure_of_no_kind_keeps_its_body() {
    let mock = MockTransport::new();
    let error = r#"{"type":"error","error":{"type":"rate_limit_error"}}"#;
    mock.reply(ANTHROPIC, mock::json(StatusCode::TOO_MANY_REQUESTS, error));
    mock.reply(OPENAI, mock::json(StatusCode::TOO_MANY_REQUESTS, error));
    let raw = Passthrough::new("raw", [""]);
    let gw = Gateway::new()
        .provider("raw-1", raw.clone(), mock.at(ANTHROPIC))
        .and_then(|g| g.provider("raw-2", raw.clone(), mock.at(OPENAI)))
        .and_then(|g| g.serve(raw))
        .and_then(|g| g.plugin("fallback", std::sync::Arc::new(RawFallback)))
        .unwrap_or_else(|e| panic!("{e}"));

    let (status, _, got, _) = send(&gw, claude("/v1/messages/count_tokens", "{}")).await;

    assert_eq!(
        (status, got.as_str()),
        (StatusCode::TOO_MANY_REQUESTS, error)
    );
}
