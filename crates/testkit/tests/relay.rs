use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use futures::StreamExt;
use respan_gateway_core::{
    Builder, Class, ClientRequest, Config, Credential, Fault, Flow, Gateway, Inbound, Op, Outcome,
    Part, Plugin, Principal, ProviderEnd, units,
};
use respan_gateway_formats::OpenAiChat;
use respan_gateway_sdk::record::{Record, Recorder};
use respan_gateway_testkit::{MockResponse, MockTransport, collect};
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

const STREAM_REQ: &str =
    r#"{"model":"gpt-x","stream":true,"messages":[{"role":"user","content":"hi"}]}"#;

fn stream_bytes() -> Vec<u8> {
    STREAM.concat().into_bytes()
}

fn chat() -> Inbound {
    Inbound::new("chat", STREAM_REQ)
}

struct Stamp;

#[async_trait]
impl Plugin for Stamp {
    async fn on_client_request(&self, req: &ClientRequest) -> Result<Flow, Fault> {
        req.to_client().head(|h| {
            h.headers
                .insert("x-served-by", http::HeaderValue::from_static("respan"));
        });
        Ok(Flow::Pass)
    }
}

struct Shout;

#[async_trait]
impl Plugin for Shout {
    async fn on_client_request(&self, req: &ClientRequest) -> Result<Flow, Fault> {
        req.to_client().map(|p| {
            if let Part::Delta(d) = p {
                *d.text_mut() = d.text().to_uppercase();
            }
        });
        Ok(Flow::Pass)
    }
}

/// Counts streamed characters with a read-only tap.
struct CountChars(Arc<Mutex<usize>>);

#[async_trait]
impl Plugin for CountChars {
    async fn on_client_request(&self, req: &ClientRequest) -> Result<Flow, Fault> {
        let count = self.0.clone();
        req.to_client().tap(move |p| {
            if let Part::Delta(d) = p
                && let Ok(mut n) = count.lock()
            {
                *n += d.text().len();
            }
        });
        Ok(Flow::Pass)
    }
}

/// Accepts any caller with a credential, whatever transport carried it.
struct KeyAuth;

#[async_trait]
impl Plugin for KeyAuth {
    async fn on_client_request(&self, req: &ClientRequest) -> Result<Flow, Fault> {
        let key = req
            .ext::<Credential>()
            .ok_or_else(|| Fault::unauthorized("missing credential"))?;
        req.insert_ext(Principal {
            id: key.expose().to_owned(),
            tenant: "acme".into(),
        });
        Ok(Flow::Pass)
    }
}

#[derive(Deserialize)]
struct Rpm {
    per_minute: f64,
}

struct PerKeyRate(Rpm);

#[async_trait]
impl Plugin for PerKeyRate {
    async fn on_client_request(&self, req: &ClientRequest) -> Result<Flow, Fault> {
        let who = req.ext::<Principal>().map(|p| p.id).unwrap_or_default();
        let op = Op::add(req.key(LIMIT, format!("rpm:{who}")), 1.0)
            .max(self.0.per_minute)
            .ttl(Duration::from_secs(60));
        if req.store().apply(vec![op]).await?.is_applied() {
            Ok(Flow::Pass)
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

/// Answers from the first provider that succeeds.
#[async_trait]
impl Plugin for Fallback {
    async fn on_client_request(&self, req: &ClientRequest) -> Result<Flow, Fault> {
        let mut last = Err(Fault::upstream("no providers"));
        for name in &self.0.providers {
            match req.call(name).await {
                Ok(r) if r.status().is_success() => return Ok(Flow::respond(r)),
                Ok(r) => last = Ok(Flow::respond(r)),
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
    chars: Arc<Mutex<usize>>,
}

impl Harness {
    fn records(&self) -> Vec<Record> {
        self.records.lock().map(|r| r.clone()).unwrap_or_default()
    }

    async fn send(&self, inbound: Inbound) -> (u16, http::HeaderMap, bytes::Bytes) {
        collect(self.gw.handle(inbound).await).await
    }
}

/// A `chat` route to `target`: the recorder, then `chain`.
fn harness(chain: serde_json::Value, target: serde_json::Value) -> Harness {
    let mut chain = chain.as_array().cloned().unwrap_or_default();
    chain.insert(0, json!({ "use": "recorder" }));
    let mut route = json!({ "name": "chat", "format": "openai-chat", "chain": chain });
    if let (Some(r), Some(t)) = (route.as_object_mut(), target.as_object()) {
        r.extend(t.clone());
    }
    let config: Config = serde_json::from_value(json!({
        "providers": {
            "openai": { "format": "openai-chat", "base_url": OPENAI, "auth": { "type": "bearer", "key": "sk-test" } },
            "backup": { "format": "openai-chat", "base_url": BACKUP, "model": "backup-model" }
        },
        "routes": [route],
    }))
    .unwrap_or_else(|e| panic!("bad test config: {e}"));

    let mock = MockTransport::new();
    let records = Arc::new(Mutex::new(Vec::new()));
    let chars = Arc::new(Mutex::new(0));
    let (sink, count) = (records.clone(), chars.clone());
    let gw = builder()
        .plugin("recorder", move |_: ()| {
            let sink = sink.clone();
            Ok(Recorder::new(move |r| {
                if let Ok(mut v) = sink.lock() {
                    v.push(r.clone());
                }
            }))
        })
        .plugin("count", move |_: ()| Ok(CountChars(count.clone())))
        .transport(mock.clone())
        .build(&config)
        .unwrap_or_else(|e| panic!("build failed: {e}"));
    Harness {
        gw,
        mock,
        records,
        chars,
    }
}

fn to_openai() -> serde_json::Value {
    json!({ "target": "openai" })
}

fn builder() -> Builder {
    Builder::new()
        .format(OpenAiChat)
        .plugin("stamp", |_: ()| Ok(Stamp))
        .plugin("shout", |_: ()| Ok(Shout))
        .plugin("auth", |_: ()| Ok(KeyAuth))
        .plugin("rate", |c: Rpm| Ok(PerKeyRate(c)))
        .plugin("fallback", |c: FallbackConfig| Ok(Fallback(c)))
}

#[tokio::test]
async fn relays_a_stream_byte_for_byte_and_records_usage() {
    let h = harness(json!([]), to_openai());
    h.mock.reply(OPENAI, MockResponse::sse(STREAM));

    let (status, _, body) = h.send(chat()).await;

    assert_eq!(status, 200);
    assert_eq!(body.to_vec(), stream_bytes());
    let r = &h.records()[0];
    assert_eq!(r.outcome, Outcome::Done);
    assert_eq!(r.usage().get(units::INPUT_TOKENS), 5.0);
    assert_eq!(r.usage().get(units::OUTPUT_TOKENS), 2.0);
    assert_eq!(r.attempts.len(), 1);
    assert_eq!(r.attempts[0].end, ProviderEnd::Committed);
    assert_eq!(r.model.as_deref(), Some("gpt-x"));
}

#[tokio::test]
async fn provider_request_gets_auth_model_and_usage_option() {
    let h = harness(json!([]), json!({ "target": "backup" }));
    h.mock.reply(BACKUP, MockResponse::sse(STREAM));
    h.send(chat()).await;

    let sent = &h.mock.sent()[0];
    assert_eq!(sent.url, "http://backup.test/v1/chat/completions");
    let body: serde_json::Value = serde_json::from_slice(&sent.body).unwrap_or_default();
    assert_eq!(body["model"], "backup-model");
    assert_eq!(body["stream_options"]["include_usage"], true);

    let h = harness(json!([]), to_openai());
    h.mock.reply(OPENAI, MockResponse::sse(STREAM));
    h.send(chat()).await;
    let auth = h.mock.sent()[0].headers.get("authorization").cloned();
    assert_eq!(
        auth.as_ref().and_then(|v| v.to_str().ok()),
        Some("Bearer sk-test")
    );
}

#[tokio::test]
async fn a_head_edit_does_not_touch_the_body() {
    let h = harness(json!([{ "use": "stamp" }]), to_openai());
    h.mock.reply(OPENAI, MockResponse::sse(STREAM));

    let (_, headers, body) = h.send(chat()).await;

    assert_eq!(
        headers.get("x-served-by").map(|v| v.as_bytes()),
        Some(&b"respan"[..])
    );
    assert_eq!(body.to_vec(), stream_bytes());
}

#[tokio::test]
async fn a_tap_sees_every_part_without_re_encoding() {
    let h = harness(json!([{ "use": "count" }]), to_openai());
    h.mock.reply(OPENAI, MockResponse::sse(STREAM));

    let (_, _, body) = h.send(chat()).await;

    assert_eq!(body.to_vec(), stream_bytes());
    assert_eq!(h.chars.lock().map(|n| *n).unwrap_or(0), "Hello world".len());
}

#[tokio::test]
async fn a_body_edit_re_encodes_only_what_changed() {
    let h = harness(json!([{ "use": "shout" }]), to_openai());
    h.mock.reply(OPENAI, MockResponse::sse(STREAM));

    let (_, _, body) = h.send(chat()).await;
    let text = String::from_utf8_lossy(&body);

    assert!(text.contains("\"content\":\"HELLO\""), "{text}");
    assert!(text.contains("\"content\":\" WORLD\""), "{text}");
    assert!(text.ends_with("data: [DONE]\n\n"), "{text}");
    assert_eq!(h.records()[0].usage().get(units::OUTPUT_TOKENS), 2.0);
}

#[tokio::test]
async fn a_cut_stream_ends_with_an_error_frame_not_a_clean_stop() {
    let h = harness(json!([]), to_openai());
    h.mock.reply(OPENAI, MockResponse::sse(&STREAM[..2]));

    let (_, _, body) = h.send(chat()).await;
    let text = String::from_utf8_lossy(&body);

    assert!(text.contains("\"type\":\"truncated\""), "{text}");
    assert!(!text.contains("[DONE]"));
    let r = &h.records()[0];
    assert!(matches!(r.outcome, Outcome::Failed(_)));
    assert_eq!(r.attempts[0].end, ProviderEnd::Failed);
}

#[tokio::test]
async fn auth_works_from_any_transport_through_a_credential() {
    let h = harness(json!([{ "use": "auth" }]), to_openai());
    h.mock.reply(OPENAI, MockResponse::sse(STREAM));

    let (status, _, _) = h.send(chat()).await;
    assert_eq!(status, 401);
    assert!(matches!(h.records()[0].outcome, Outcome::Refused(_)));

    let (status, _, _) = h.send(chat().with(Credential::new("key-1"))).await;
    assert_eq!(status, 200);
    let r = &h.records()[1];
    assert_eq!(
        (r.principal.as_deref(), r.tenant.as_deref()),
        (Some("key-1"), Some("acme"))
    );
}

#[tokio::test]
async fn a_refusal_uses_the_client_format_and_retry_after() {
    let h = harness(
        json!([{ "use": "auth" }, { "use": "rate", "with": { "per_minute": 1 } }]),
        to_openai(),
    );
    h.mock.reply(OPENAI, MockResponse::sse(STREAM));
    let key = || chat().with(Credential::new("key-1"));

    h.send(key()).await;
    let (status, headers, body) = h.send(key()).await;

    assert_eq!(status, 429);
    assert_eq!(
        headers.get("retry-after").map(|v| v.as_bytes()),
        Some(&b"60"[..])
    );
    let err: serde_json::Value = serde_json::from_slice(&body).unwrap_or_default();
    assert_eq!(err["error"]["type"], "rate_limited");
    assert_eq!(h.mock.sent().len(), 1);
}

#[tokio::test]
async fn a_plugin_can_answer_by_calling_providers_itself() {
    let h = harness(
        json!([{ "use": "fallback", "with": { "providers": ["openai", "backup"] } }]),
        json!({}),
    );
    h.mock.reply(
        OPENAI,
        MockResponse::json(503, "{\"error\":{\"message\":\"overloaded\"}}"),
    );
    h.mock.reply(BACKUP, MockResponse::sse(STREAM));

    let (status, _, body) = h.send(chat()).await;

    assert_eq!(status, 200);
    assert_eq!(body.to_vec(), stream_bytes());
    let a = &h.records()[0].attempts;
    assert_eq!(a.len(), 2);
    assert_eq!(
        (a[0].provider.as_str(), a[0].end, a[0].status),
        ("openai", ProviderEnd::Failed, Some(503))
    );
    assert_eq!(
        (a[1].provider.as_str(), a[1].end),
        ("backup", ProviderEnd::Committed)
    );
}

#[tokio::test]
async fn a_client_that_leaves_still_closes_everything_once() {
    let h = harness(json!([]), to_openai());
    h.mock.reply(OPENAI, MockResponse::sse(STREAM));

    let (_, mut body) = h.gw.handle(chat()).await.into_parts();
    let _first = body.next().await;
    drop(body);

    let records = h.records();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].outcome, Outcome::ClientGone);
    assert_eq!(records[0].attempts[0].end, ProviderEnd::Abandoned);
}

#[tokio::test]
async fn a_unary_response_is_relayed_with_usage() {
    let h = harness(json!([]), to_openai());
    let reply = r#"{"id":"c2","choices":[{"message":{"content":"hi"}}],"usage":{"prompt_tokens":4,"completion_tokens":1}}"#;
    h.mock.reply(OPENAI, MockResponse::json(200, reply));

    let (status, _, body) = h
        .send(Inbound::new("chat", r#"{"model":"gpt-x","messages":[]}"#))
        .await;

    assert_eq!(status, 200);
    assert_eq!(&body[..], reply.as_bytes());
    let r = &h.records()[0];
    assert_eq!(r.outcome, Outcome::Done);
    assert_eq!(r.usage().get(units::INPUT_TOKENS), 4.0);
}

#[tokio::test]
async fn unknown_names_fail_at_build_time() {
    let config: Config = serde_json::from_value(json!({
        "routes": [{ "name": "x", "format": "openai-chat", "chain": [{ "use": "nope" }], "target": "missing" }]
    }))
    .unwrap_or_else(|e| panic!("{e}"));
    let err = builder()
        .transport(MockTransport::new())
        .build(&config)
        .err()
        .map(|e| e.message)
        .unwrap_or_default();
    assert!(err.contains("unknown plugin 'nope'"), "{err}");
    assert!(err.contains("auth, fallback, rate, shout, stamp"), "{err}");
}

#[tokio::test]
async fn unknown_routes_are_404() {
    let h = harness(json!([]), to_openai());
    let (status, _, _) = h.send(Inbound::new("nope", "{}")).await;
    assert_eq!(status, 404);
    assert!(h.records().is_empty());
}
