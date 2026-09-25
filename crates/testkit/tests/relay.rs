use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use futures::StreamExt;
use respan_gateway_core::{
    AttemptEnd, Builder, Class, ClientRequest, ClientResponse, Config, Fault, Gateway, Op, Outcome,
    Part, Plugin, Providers, Record, units,
};
use respan_gateway_formats::OpenAiChat;
use respan_gateway_testkit::{MockResponse, MockTransport, collect, post};
use serde::Deserialize;
use serde_json::json;

const OPENAI: &str = "http://openai.test";
const BACKUP: &str = "http://backup.test";
const LIMIT: Class = Class::new("limit");

const STREAM: &[&str] = &[
    "data: {\"id\":\"c1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Hel",
    "lo\"}}]}\n\ndata: {\"id\":\"c1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\" world\"}}]}\n\n",
    "data: {\"id\":\"c1\",\"choices\":[],\"usage\":{\"prompt_tokens\":5,\"completion_tokens\":2}}\n\n",
    "data: [DONE]\n\n",
];

fn stream_bytes() -> Vec<u8> {
    STREAM.concat().into_bytes()
}

struct Stamp;

#[async_trait]
impl Plugin for Stamp {
    async fn on_client_request(&self, req: &ClientRequest) -> Result<(), Fault> {
        req.to_client().head(|h| {
            h.headers
                .insert("x-served-by", http::HeaderValue::from_static("respan"));
        });
        Ok(())
    }
}

struct Shout;

#[async_trait]
impl Plugin for Shout {
    async fn on_client_request(&self, req: &ClientRequest) -> Result<(), Fault> {
        req.to_client().map(|p| {
            if let Part::Delta(d) = p {
                *d.text_mut() = d.text().to_uppercase();
            }
        });
        Ok(())
    }
}

#[derive(Deserialize)]
struct Rpm {
    per_minute: f64,
}

struct PerKeyRate(Rpm);

#[async_trait]
impl Plugin for PerKeyRate {
    async fn on_client_request(&self, req: &ClientRequest) -> Result<(), Fault> {
        let key_id = req
            .head()
            .headers
            .get("x-key")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("anonymous")
            .to_owned();
        let op = Op::add(req.key(LIMIT, format!("rpm:{key_id}")), 1.0)
            .max(self.0.per_minute)
            .ttl(Duration::from_secs(60));
        if req.store().apply(vec![op]).await?.is_applied() {
            Ok(())
        } else {
            Err(Fault::too_many(Duration::from_secs(60)))
        }
    }
}

#[derive(Deserialize)]
struct FallbackConfig {
    providers: Vec<String>,
}

struct Fallback(FallbackConfig);

#[async_trait]
impl Plugin for Fallback {
    async fn dispatch(
        &self,
        _req: &ClientRequest,
        providers: &Providers<'_>,
    ) -> Result<ClientResponse, Fault> {
        let mut last = Err(Fault::upstream("no providers"));
        for name in &self.0.providers {
            match providers.call(name).await {
                Ok(r) if r.status().is_success() => return Ok(r.into()),
                Ok(r) => last = Ok(r.into()),
                Err(f) => last = Err(f),
            }
        }
        last
    }
}

struct Harness {
    gw: Gateway,
    mock: MockTransport,
    records: Arc<Mutex<Vec<Record>>>,
}

impl Harness {
    fn records(&self) -> Vec<Record> {
        self.records.lock().map(|r| r.clone()).unwrap_or_default()
    }
}

fn harness(routes: serde_json::Value) -> Harness {
    let mock = MockTransport::new();
    let records = Arc::new(Mutex::new(Vec::new()));
    let sink = records.clone();
    let config: Config = serde_json::from_value(json!({
        "providers": {
            "openai": { "format": "openai-chat", "base_url": OPENAI, "auth": { "type": "bearer", "key": "sk-test" } },
            "backup": { "format": "openai-chat", "base_url": BACKUP, "model": "backup-model" }
        },
        "routes": routes,
    }))
    .unwrap_or_else(|e| panic!("bad test config: {e}"));
    let gw = builder()
        .transport(mock.clone())
        .on_record(move |r| {
            if let Ok(mut v) = sink.lock() {
                v.push(r.clone());
            }
        })
        .build(&config)
        .unwrap_or_else(|e| panic!("build failed: {e}"));
    Harness { gw, mock, records }
}

fn builder() -> Builder {
    Builder::new()
        .format(OpenAiChat)
        .plugin("stamp", |_: ()| Ok(Stamp))
        .plugin("shout", |_: ()| Ok(Shout))
        .plugin("rate", |c: Rpm| Ok(PerKeyRate(c)))
        .plugin("fallback", |c: FallbackConfig| Ok(Fallback(c)))
}

fn chat_route(chain: serde_json::Value) -> serde_json::Value {
    json!([{ "path": "/v1/chat/completions", "format": "openai-chat", "chain": chain, "target": "openai" }])
}

const STREAM_REQ: &str =
    r#"{"model":"gpt-x","stream":true,"messages":[{"role":"user","content":"hi"}]}"#;

#[tokio::test]
async fn relays_a_stream_byte_for_byte_and_records_usage() {
    let h = harness(chat_route(json!([])));
    h.mock.reply(OPENAI, MockResponse::sse(STREAM));

    let (status, _, body) =
        collect(h.gw.handle(post("/v1/chat/completions", STREAM_REQ)).await).await;

    assert_eq!(status, 200);
    assert_eq!(body.to_vec(), stream_bytes());
    let r = &h.records()[0];
    assert_eq!(r.outcome, Outcome::Done);
    assert_eq!(r.usage.get(units::INPUT_TOKENS), 5.0);
    assert_eq!(r.usage.get(units::OUTPUT_TOKENS), 2.0);
    assert_eq!(r.attempts.len(), 1);
    assert_eq!(r.attempts[0].end, AttemptEnd::Committed);
    assert_eq!(r.model.as_deref(), Some("gpt-x"));
}

#[tokio::test]
async fn provider_request_gets_auth_model_and_usage_option() {
    let h = harness(
        json!([{ "path": "/v1/chat/completions", "format": "openai-chat", "target": "backup" }]),
    );
    h.mock.reply(BACKUP, MockResponse::sse(STREAM));

    collect(h.gw.handle(post("/v1/chat/completions", STREAM_REQ)).await).await;

    let sent = &h.mock.sent()[0];
    assert_eq!(sent.url, "http://backup.test/v1/chat/completions");
    let body: serde_json::Value = serde_json::from_slice(&sent.body).unwrap_or_default();
    assert_eq!(body["model"], "backup-model");
    assert_eq!(body["stream_options"]["include_usage"], true);

    let h = harness(chat_route(json!([])));
    h.mock.reply(OPENAI, MockResponse::sse(STREAM));
    collect(h.gw.handle(post("/v1/chat/completions", STREAM_REQ)).await).await;
    let auth = h.mock.sent()[0].headers.get("authorization").cloned();
    assert_eq!(
        auth.as_ref().and_then(|v| v.to_str().ok()),
        Some("Bearer sk-test")
    );
}

#[tokio::test]
async fn a_head_edit_does_not_touch_the_body() {
    let h = harness(chat_route(json!([{ "use": "stamp" }])));
    h.mock.reply(OPENAI, MockResponse::sse(STREAM));

    let (_, headers, body) =
        collect(h.gw.handle(post("/v1/chat/completions", STREAM_REQ)).await).await;

    assert_eq!(
        headers.get("x-served-by").map(|v| v.as_bytes()),
        Some(&b"respan"[..])
    );
    assert_eq!(body.to_vec(), stream_bytes());
}

#[tokio::test]
async fn a_body_edit_re_encodes_only_what_changed() {
    let h = harness(chat_route(json!([{ "use": "shout" }])));
    h.mock.reply(OPENAI, MockResponse::sse(STREAM));

    let (_, _, body) = collect(h.gw.handle(post("/v1/chat/completions", STREAM_REQ)).await).await;
    let text = String::from_utf8_lossy(&body);

    assert!(text.contains("\"content\":\"HELLO\""), "{text}");
    assert!(text.contains("\"content\":\" WORLD\""), "{text}");
    assert!(text.ends_with("data: [DONE]\n\n"), "{text}");
    assert_eq!(h.records()[0].usage.get(units::OUTPUT_TOKENS), 2.0);
}

#[tokio::test]
async fn a_cut_stream_ends_with_an_error_frame_not_a_clean_stop() {
    let h = harness(chat_route(json!([])));
    h.mock.reply(OPENAI, MockResponse::sse(&STREAM[..2]));

    let (_, _, body) = collect(h.gw.handle(post("/v1/chat/completions", STREAM_REQ)).await).await;
    let text = String::from_utf8_lossy(&body);

    assert!(text.contains("\"type\":\"truncated\""), "{text}");
    assert!(!text.contains("[DONE]"));
    assert!(matches!(h.records()[0].outcome, Outcome::Failed(_)));
}

#[tokio::test]
async fn a_refusal_uses_the_client_format_and_retry_after() {
    let h = harness(chat_route(
        json!([{ "use": "rate", "with": { "per_minute": 1 } }]),
    ));
    h.mock.reply(OPENAI, MockResponse::sse(STREAM));

    collect(h.gw.handle(post("/v1/chat/completions", STREAM_REQ)).await).await;
    let (status, headers, body) =
        collect(h.gw.handle(post("/v1/chat/completions", STREAM_REQ)).await).await;

    assert_eq!(status, 429);
    assert_eq!(
        headers.get("retry-after").map(|v| v.as_bytes()),
        Some(&b"60"[..])
    );
    let err: serde_json::Value = serde_json::from_slice(&body).unwrap_or_default();
    assert_eq!(err["error"]["type"], "rate_limited");
    assert_eq!(h.mock.sent().len(), 1);
    assert!(matches!(h.records()[1].outcome, Outcome::Refused(_)));
}

#[tokio::test]
async fn a_source_plugin_falls_back_and_every_attempt_is_recorded() {
    let h = harness(json!([{
        "path": "/v1/chat/completions", "format": "openai-chat",
        "source": { "use": "fallback", "with": { "providers": ["openai", "backup"] } }
    }]));
    h.mock.reply(
        OPENAI,
        MockResponse::json(503, "{\"error\":{\"message\":\"overloaded\"}}"),
    );
    h.mock.reply(BACKUP, MockResponse::sse(STREAM));

    let (status, _, body) =
        collect(h.gw.handle(post("/v1/chat/completions", STREAM_REQ)).await).await;

    assert_eq!(status, 200);
    assert_eq!(body.to_vec(), stream_bytes());
    let a = &h.records()[0].attempts;
    assert_eq!(a.len(), 2);
    assert_eq!(
        (a[0].provider.as_str(), a[0].end, a[0].status),
        ("openai", AttemptEnd::Failed, Some(503))
    );
    assert_eq!(
        (a[1].provider.as_str(), a[1].end),
        ("backup", AttemptEnd::Committed)
    );
}

#[tokio::test]
async fn a_client_that_leaves_still_produces_one_record() {
    let h = harness(chat_route(json!([])));
    h.mock.reply(OPENAI, MockResponse::sse(STREAM));

    let (_, mut body) =
        h.gw.handle(post("/v1/chat/completions", STREAM_REQ))
            .await
            .into_parts();
    let _first = body.next().await;
    drop(body);

    let records = h.records();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].outcome, Outcome::ClientGone);
}

#[tokio::test]
async fn a_unary_response_is_relayed_with_usage() {
    let h = harness(chat_route(json!([])));
    let reply = r#"{"id":"c2","choices":[{"message":{"content":"hi"}}],"usage":{"prompt_tokens":4,"completion_tokens":1}}"#;
    h.mock.reply(OPENAI, MockResponse::json(200, reply));

    let (status, _, body) = collect(
        h.gw.handle(post(
            "/v1/chat/completions",
            r#"{"model":"gpt-x","messages":[]}"#,
        ))
        .await,
    )
    .await;

    assert_eq!(status, 200);
    assert_eq!(&body[..], reply.as_bytes());
    let r = &h.records()[0];
    assert_eq!(r.outcome, Outcome::Done);
    assert_eq!(r.usage.get(units::INPUT_TOKENS), 4.0);
}

#[tokio::test]
async fn unknown_names_fail_at_build_time() {
    let config: Config = serde_json::from_value(json!({
        "routes": [{ "path": "/x", "format": "openai-chat", "chain": [{ "use": "nope" }], "target": "missing" }]
    }))
    .unwrap_or_else(|e| panic!("{e}"));
    let err = builder()
        .transport(MockTransport::new())
        .build(&config)
        .err()
        .map(|e| e.message)
        .unwrap_or_default();
    assert!(err.contains("unknown plugin 'nope'"), "{err}");
    assert!(err.contains("fallback, rate, shout, stamp"), "{err}");
}

#[tokio::test]
async fn unrouted_requests_are_404() {
    let h = harness(chat_route(json!([])));
    let (status, _, _) = collect(h.gw.handle(post("/nope", "{}")).await).await;
    assert_eq!(status, 404);
    assert!(h.records().is_empty());
}
