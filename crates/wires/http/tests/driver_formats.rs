//! HTTP's driver on the real formats: relay, translation and streams
//! through `serve`, native senders and `Gateway::handle`, and what each of a
//! table of scenarios gives the client, sends the providers and ends with.
#![allow(clippy::unwrap_used)]

use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use bytes::Bytes;
use respan_gateway_conversation::Input;
use respan_gateway_conversation_http::anthropic::AnthropicMessages;
use respan_gateway_conversation_http::openai::OpenAiChat;
use respan_gateway_core::futures::{StreamExt, stream};
use respan_gateway_core::{
    ClientContext, Fault, Gateway, Plugin, Provider, ProviderContext, Response, async_trait,
};
use respan_gateway_http::http::{self, HeaderValue, StatusCode};
use respan_gateway_http::{Body, Completion, http::response::Parts as ResponseHead};
#[allow(dead_code)]
#[path = "../../../../tests/support/hooks.rs"]
mod hooks;
#[allow(dead_code, unused_imports)]
#[path = "../../../../tests/support/http.rs"]
mod http_support;
#[path = "support/scripted.rs"]
mod scripted;
use hooks::Outcomes;
use http_support::{LIMIT, post, read_whole, serve};
use scripted::{Scripted, json, sse};

const OPENAI: &str = "http://openai.test/v1";
const ANTHROPIC: &str = "http://anthropic.test";
const CHAT: &str = "/v1/chat/completions";
const MESSAGES: &str = "/v1/messages";

const STREAM: &[&str] = &[
    "data: {\"id\":\"c1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Hel",
    "lo\"}}]}\n\ndata: {\"id\":\"c1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\" world\"}}]}\n\n",
    "data: {\"id\":\"c1\",\"choices\":[],\"usage\":{\"prompt_tokens\":5,\"completion_tokens\":2}}\n\n",
    "data: [DONE]\n\n",
];
const STREAM_REQ: &str = r#"{"model":"gpt-x","stream":true,"stream_options":{"include_usage":true},"messages":[{"role":"user","content":"hi"}]}"#;
const UNARY_REQ: &str = r#"{"model":"gpt-x",  "messages":[]}"#;
const UNARY: &str = r#"{"id":"c2","choices":[{"message":{"content":"hi"}}],"usage":{"prompt_tokens":4,"completion_tokens":1}}"#;
const CLAUDE_STREAM_REQ: &str =
    r#"{"model":"claude-x","stream":true,"messages":[{"role":"user","content":"hi"}]}"#;
const ANTHROPIC_REQ: &str =
    r#"{"model":"gpt-x","max_tokens":100,"messages":[{"role":"user","content":"hi"}]}"#;
const ANTHROPIC_STREAM: &[&str] = &[
    "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_1\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"claude-x\",\"content\":[],\"stop_reason\":null,\"stop_sequence\":null,\"usage\":{\"input_tokens\":5,\"output_tokens\":1}}}\n\n",
    "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\nevent: ping\ndata: {\"type\": \"ping\"}\n\n",
    "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"Hello\"}}\n\n",
    "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\" world\"}}\n\n",
    "event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
    "event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\",\"stop_sequence\":null},\"usage\":{\"output_tokens\":2}}\n\n",
    "event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n",
];
const OPENAI_WHOLE: &str = r#"{"id":"c1","object":"chat.completion","model":"gpt-x","choices":[{"index":0,"message":{"role":"assistant","content":"hi"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1}}"#;
const RATE_LIMITED: &str = r#"{"error":{"type":"rate_limit_error","message":"slow down"}}"#;
const BAD: &str = r#"{"error":{"message":"bad model","type":"invalid_request_error","param":"model","code":null}}"#;

type Marks = Arc<Mutex<Vec<bool>>>;
type WireResult = Result<(http::Response<Body>, Option<Completion>), Fault>;

/// A native sender that records whether each call was relayed, to a
/// provider of its client's own format.
fn marking(
    at: scripted::ScriptedAt,
    marks: Marks,
) -> impl Fn(http::Request<Body>) -> respan_gateway_core::futures::future::BoxFuture<'static, WireResult>
{
    use respan_gateway_core::futures::FutureExt;
    move |request| {
        lock(&marks).push(
            request
                .extensions()
                .get::<respan_gateway_core::Extensions>()
                .is_some_and(|e| e.get::<respan_gateway_http::raw::Relayed>().is_some()),
        );
        let at = at.clone();
        async move { at.send(request).await }.boxed()
    }
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Sends Claude models to `anthropic`, others to `openai`.
struct Route;

#[async_trait]
impl Plugin for Route {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let model = cx.input::<Input>()?.model.clone().unwrap_or_default();
        cx.set_target(match model.starts_with("claude") {
            true => "anthropic",
            false => "openai",
        });
        Ok(None)
    }
}

/// Records whether each call was refused at its head.
#[derive(Clone, Default)]
struct HeadOnly(Marks);

#[async_trait]
impl Plugin for HeadOnly {
    async fn on_provider_end(&self, px: &ProviderContext, _: &Result<(), Fault>) {
        lock(&self.0).push(px.is_refused());
    }
}

/// A gateway serving both formats, calling `openai` and `anthropic`, which
/// have no keys of their own.
struct Harness {
    gw: Gateway,
    mock: Scripted,
    outcomes: Outcomes,
    head_only: HeadOnly,
    marks: Marks,
}

impl Harness {
    fn new() -> Self {
        let mock = Scripted::new();
        let (outcomes, head_only, marks) =
            (Outcomes::default(), HeadOnly::default(), Marks::default());
        let openai = marking(mock.at(OPENAI), marks.clone());
        let anthropic = marking(mock.at(ANTHROPIC), marks.clone());
        let gw = Gateway::new()
            .provider(Provider::new(
                "openai",
                OpenAiChat,
                respan_gateway_http::connect(openai, LIMIT),
            ))
            .and_then(|g| {
                g.provider(Provider::new(
                    "anthropic",
                    AnthropicMessages,
                    respan_gateway_http::connect(anthropic, LIMIT),
                ))
            })
            .and_then(|g| {
                g.client::<Input, _, _>(OpenAiChat)?
                    .client::<Input, _, _>(AnthropicMessages)
            });
        let gw = gw
            .map(|g| g.plugin(outcomes.clone()))
            .map(|g| g.plugin(head_only.clone()))
            .map(|g| g.plugin(Route).plugin(RelaySame))
            .unwrap();
        Self {
            gw,
            mock,
            outcomes,
            head_only,
            marks,
        }
    }

    /// Hand `req` to the gateway through the HTTP adapter.
    async fn serve(&self, req: http::Request<Body>) -> http::Response<Body> {
        serve(&self.gw, req).await.0
    }

    /// Send `req`, read the whole answer, and wait for the end hooks.
    async fn send(&self, req: http::Request<Body>) -> Got {
        let (status, headers, body) = read_whole(self.serve(req).await).await;
        self.gw.idle().await;
        let headers = headers
            .iter()
            .map(|(n, v)| (n.to_string(), v.to_str().unwrap_or_default().to_owned()))
            .collect();
        let body = body
            .map(|b| String::from_utf8_lossy(&b).into_owned())
            .map_err(|f| f.code().to_owned());
        Got {
            status: status.as_u16(),
            headers,
            body,
        }
    }

    /// Each session's end, as its code.
    fn ends(&self) -> Vec<String> {
        let code = |e: &Result<(), Fault>| {
            e.as_ref()
                .map_or_else(|f| f.code().to_owned(), |()| "ok".into())
        };
        self.outcomes.all().iter().map(code).collect()
    }
}

/// What a client got.
#[derive(Debug, PartialEq)]
struct Got {
    status: u16,
    headers: Vec<(String, String)>,
    /// The body, or the code it broke off with.
    body: Result<String, String>,
}

/// A client's `POST` of `body` to `path`, with its key.
fn post_keyed(path: &str, body: &str) -> http::Request<Body> {
    let mut req = post(path, body.to_owned());
    let key = HeaderValue::from_static("Bearer sk-client");
    req.headers_mut().insert("authorization", key);
    req
}

/// An SSE reply of `chunks`, cut with a failure after them.
fn cut(chunks: &[&str]) -> scripted::Reply {
    let mut reply = sse(chunks);
    reply
        .body_mut()
        .push(Err(Fault::upstream("connection reset")));
    reply
}

/// A 429 of `body`, with `Retry-After: 7`.
fn limited(body: &str) -> scripted::Reply {
    let mut reply = json(StatusCode::TOO_MANY_REQUESTS, body);
    let wait = HeaderValue::from_static("7");
    reply.headers_mut().insert("retry-after", wait);
    reply
}

/// A scenario: its name, the client's request, the providers' replies, and
/// what it must give.
type Scenario = (
    &'static str,
    fn() -> http::Request<Body>,
    fn(&Scripted),
    Want,
);

/// What a scenario gives: the client's status and a part of its body, how
/// each session ended, whether each call was refused at its head, and
/// whether each call sent was a relay.
struct Want {
    status: u16,
    body: &'static str,
    ends: &'static [&'static str],
    head_only: &'static [bool],
    relayed: &'static [bool],
}

fn scenarios() -> Vec<Scenario> {
    vec![
        (
            "unary relay",
            || post_keyed(CHAT, UNARY_REQ),
            |m| {
                m.reply(OPENAI, json(StatusCode::OK, UNARY));
            },
            Want {
                status: 200,
                body: r#""content":"hi""#,
                ends: &["ok"],
                head_only: &[false],
                relayed: &[true],
            },
        ),
        (
            "stream relay",
            || post_keyed(CHAT, STREAM_REQ),
            |m| {
                m.reply(OPENAI, sse(STREAM));
            },
            Want {
                status: 200,
                body: "data: [DONE]",
                ends: &["ok"],
                head_only: &[false],
                relayed: &[true],
            },
        ),
        (
            "openai client streams from anthropic",
            || post_keyed(CHAT, CLAUDE_STREAM_REQ),
            |m| {
                m.reply(ANTHROPIC, sse(ANTHROPIC_STREAM));
            },
            Want {
                status: 200,
                body: r#"{"content":"Hello"}"#,
                ends: &["ok"],
                head_only: &[false],
                relayed: &[false],
            },
        ),
        (
            "anthropic client, whole from openai",
            || post_keyed(MESSAGES, ANTHROPIC_REQ),
            |m| {
                m.reply(OPENAI, json(StatusCode::OK, OPENAI_WHOLE));
            },
            Want {
                status: 200,
                body: r#""type":"message""#,
                ends: &["ok"],
                head_only: &[false],
                relayed: &[false],
            },
        ),
        (
            "a cut relayed stream",
            || post_keyed(CHAT, STREAM_REQ),
            |m| {
                m.reply(OPENAI, cut(&STREAM[..2]));
            },
            Want {
                status: 200,
                body: r#""code":"upstream""#,
                ends: &["upstream"],
                head_only: &[false],
                relayed: &[true],
            },
        ),
        (
            "a truncated translated stream",
            || post_keyed(CHAT, CLAUDE_STREAM_REQ),
            |m| {
                m.reply(ANTHROPIC, sse(&ANTHROPIC_STREAM[..3]));
            },
            Want {
                status: 200,
                body: r#""code":"truncated""#,
                ends: &["truncated"],
                head_only: &[false],
                relayed: &[false],
            },
        ),
        (
            "a refusal relayed as it came",
            || post_keyed(CHAT, UNARY_REQ),
            |m| {
                m.reply(OPENAI, json(StatusCode::BAD_REQUEST, BAD));
            },
            Want {
                status: 400,
                body: BAD,
                ends: &["invalid_request_error"],
                head_only: &[true],
                relayed: &[true],
            },
        ),
        (
            "a relayed 429 with Retry-After",
            || post_keyed(CHAT, STREAM_REQ),
            |m| {
                m.reply(OPENAI, limited(RATE_LIMITED));
            },
            Want {
                status: 429,
                body: RATE_LIMITED,
                ends: &["rate_limit_error"],
                head_only: &[true],
                relayed: &[true],
            },
        ),
        (
            "a translated 429 with Retry-After",
            || post_keyed(MESSAGES, ANTHROPIC_REQ),
            |m| {
                m.reply(OPENAI, limited(RATE_LIMITED));
            },
            Want {
                status: 429,
                body: r#"{"type":"error""#,
                ends: &["rate_limit_error"],
                head_only: &[true],
                relayed: &[false],
            },
        ),
        (
            "a request that cannot be translated",
            || {
                let body = r#"{"model":"claude-x","messages":[{"role":"user","content":"hi"}],"response_format":{"type":"json_object"}}"#;
                post_keyed(CHAT, body)
            },
            |_| {},
            Want {
                status: 400,
                body: "cannot be translated",
                ends: &["bad_request"],
                head_only: &[false],
                relayed: &[],
            },
        ),
        (
            "a provider that does not answer",
            || post_keyed(CHAT, UNARY_REQ),
            |_| {},
            Want {
                status: 502,
                body: "no mock response",
                ends: &["upstream"],
                head_only: &[false],
                relayed: &[true],
            },
        ),
        (
            "no format accepts the request",
            || post_keyed("/nowhere", "{}"),
            |_| {},
            Want {
                status: 404,
                body: "no format served accepts POST /nowhere",
                ends: &[],
                head_only: &[],
                relayed: &[],
            },
        ),
    ]
}

/// Every scenario gives the client its expected status and bytes, and ends.
#[tokio::test]
async fn each_scenario_gives_what_it_should() {
    for (name, req, script, want) in scenarios() {
        let h = Harness::new();
        script(&h.mock);
        let got = h.send(req()).await;
        assert_eq!(got.status, want.status, "{name}");
        let body = got.body.unwrap_or_default();
        assert!(body.contains(want.body), "{name}: {body}");
        assert_eq!(h.ends(), want.ends, "{name}");
        assert_eq!(*lock(&h.head_only.0), want.head_only, "{name}");
        assert_eq!(*lock(&h.marks), want.relayed, "{name}");
    }
}

#[tokio::test]
async fn a_relay_round_trips_the_request_and_answers_both_ways() {
    let h = Harness::new();
    h.mock.reply(OPENAI, json(StatusCode::OK, UNARY));
    h.mock.reply(OPENAI, sse(STREAM));
    let got = h.send(post_keyed(CHAT, UNARY_REQ)).await;
    let body = got.body.unwrap_or_default();
    assert_eq!(got.status, 200);
    assert!(body.contains(r#""content":"hi""#), "{body}");
    assert!(body.contains(r#""prompt_tokens":4"#), "{body}");
    let got = h.send(post_keyed(CHAT, STREAM_REQ)).await;
    let body = got.body.unwrap_or_default();
    for said in [
        r#""content":"Hello""#,
        r#""content":" world""#,
        r#""completion_tokens":2"#,
    ] {
        assert!(body.contains(said), "{said}: {body}");
    }
    assert!(body.ends_with("data: [DONE]\n\n"), "{body}");
    let sent = h.mock.sent();
    let same = |sent: &[u8], came: &str| {
        let sent: serde_json::Value = serde_json::from_slice(sent).unwrap();
        let came: serde_json::Value = serde_json::from_str(came).unwrap();
        assert_eq!(sent, came, "the client's request, round-tripped");
    };
    same(sent[0].body(), UNARY_REQ);
    same(sent[1].body(), STREAM_REQ);
    assert_eq!(h.ends(), ["ok", "ok"]);
}

#[tokio::test]
async fn a_client_is_answered_in_its_own_format() {
    let h = Harness::new();
    h.mock.reply(ANTHROPIC, sse(ANTHROPIC_STREAM));
    h.mock.reply(OPENAI, json(StatusCode::OK, OPENAI_WHOLE));
    let got = h.send(post_keyed(CHAT, CLAUDE_STREAM_REQ)).await;
    let body = got.body.unwrap();
    assert!(body.starts_with("data: {"), "{body}");
    assert!(body.contains(r#""content":"Hello""#), "{body}");
    assert!(body.ends_with("data: [DONE]\n\n"), "{body}");
    let got = h.send(post_keyed(MESSAGES, ANTHROPIC_REQ)).await;
    let body = got.body.unwrap();
    assert!(body.contains(r#""type":"message""#), "{body}");
    assert!(body.contains(r#""text":"hi""#), "{body}");
    assert_eq!(h.ends(), ["ok", "ok"]);
}

#[tokio::test]
async fn a_cut_stream_ends_the_session_with_its_failure() {
    let h = Harness::new();
    h.mock.reply(OPENAI, cut(&STREAM[..2]));
    let got = h.send(post_keyed(CHAT, STREAM_REQ)).await;
    assert_eq!(got.status, 200, "its head went with its first bytes");
    let body = got.body.unwrap_or_default();
    assert!(body.contains(r#""content":"Hello""#), "{body}");
    assert!(!body.contains("[DONE]"), "{body}");
    assert_eq!(h.ends(), ["upstream"]);
}

#[tokio::test]
async fn a_refusal_is_relayed_as_it_came() {
    let h = Harness::new();
    h.mock.reply(OPENAI, json(StatusCode::BAD_REQUEST, BAD));
    let got = h.send(post_keyed(CHAT, UNARY_REQ)).await;
    assert_eq!((got.status, got.body), (400, Ok(BAD.to_owned())));
    assert_eq!(*lock(&h.head_only.0), [true], "refused at its head");
}

#[tokio::test]
async fn a_429_keeps_its_retry_after_relayed_or_translated() {
    let h = Harness::new();
    h.mock.reply(OPENAI, limited(RATE_LIMITED));
    h.mock.reply(OPENAI, limited(RATE_LIMITED));
    let retry = |got: &Got| {
        let retry = got.headers.iter().find(|(n, _)| n == "retry-after");
        retry.map(|(_, v)| v.clone())
    };
    let relayed = h.send(post_keyed(CHAT, STREAM_REQ)).await;
    assert_eq!(relayed.status, 429);
    assert_eq!(retry(&relayed).as_deref(), Some("7"));
    assert_eq!(relayed.body, Ok(RATE_LIMITED.to_owned()), "as it came");
    let translated = h.send(post_keyed(MESSAGES, ANTHROPIC_REQ)).await;
    assert_eq!(translated.status, 429);
    assert_eq!(retry(&translated).as_deref(), Some("7"));
    let body = translated.body.unwrap();
    assert!(
        body.contains(r#""type":"error""#),
        "in its own words: {body}"
    );
}

#[tokio::test]
async fn a_relay_is_a_call_to_a_provider_of_the_clients_format_alone() {
    let h = Harness::new();
    h.mock.reply(OPENAI, json(StatusCode::OK, UNARY));
    h.mock.reply(OPENAI, json(StatusCode::OK, OPENAI_WHOLE));
    let _ = h.send(post_keyed(CHAT, UNARY_REQ)).await;
    let _ = h.send(post_keyed(MESSAGES, ANTHROPIC_REQ)).await;
    assert_eq!(*lock(&h.marks), [true, false], "relayed, then translated");
}

/// A provider that refuses with a 429 whose body comes after a while, as
/// over a real network, on either path.
#[derive(Clone)]
struct SlowRefusal;

impl SlowRefusal {
    fn answer() -> (ResponseHead, respan_gateway_http::Frames) {
        let (mut head, ()) = http::Response::new(()).into_parts();
        head.status = StatusCode::TOO_MANY_REQUESTS;
        head.headers
            .insert("retry-after", HeaderValue::from_static("7"));
        let body = stream::once(async {
            tokio::time::sleep(Duration::from_millis(50)).await;
            Ok(Bytes::from_static(RATE_LIMITED.as_bytes()))
        });
        (head, body.boxed())
    }
}

impl SlowRefusal {
    pub async fn send(
        &self,
        _: http::Request<Body>,
    ) -> Result<(http::Response<Body>, Option<Completion>), Fault> {
        let (head, body) = Self::answer();
        Ok((http::Response::from_parts(head, Body::stream(body)), None))
    }
}

/// Over a real network: a refusal whose body comes slowly is read whole
/// before the call resolves, so `is_refused` says it was refused at
/// its head, and the client gets it whole.
#[tokio::test]
async fn a_slow_refusal_is_refused() {
    let head_only = HeadOnly::default();
    let gw = Gateway::new()
        .provider(Provider::new(
            "openai",
            OpenAiChat,
            respan_gateway_http::connect(
                |request| async move { SlowRefusal.send(request).await },
                LIMIT,
            ),
        ))
        .and_then(|g| g.client::<Input, _, _>(OpenAiChat))
        .map(|g| {
            g.plugin(head_only.clone())
                .plugin(respan_gateway_plugin_relay::Relay)
        })
        .and_then(|g| g.target("openai"))
        .unwrap();
    let resp = serve(&gw, post_keyed(CHAT, UNARY_REQ)).await;
    let (status, headers, body) = read_whole(resp.0).await;
    gw.idle().await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(
        headers.get("retry-after").map(|v| v.as_bytes()),
        Some(&b"7"[..])
    );
    assert_eq!(body.unwrap(), RATE_LIMITED.as_bytes());
    assert_eq!(*lock(&head_only.0), [true]);
}

/// A provider that refuses 503 with a body past the error-body cap (2 MiB
/// of HTML in 64 KiB frames, as a misbehaving proxy's error page).
#[derive(Clone)]
struct HugeRefusal;

impl HugeRefusal {
    pub async fn send(
        &self,
        _: http::Request<Body>,
    ) -> Result<(http::Response<Body>, Option<Completion>), Fault> {
        let (mut head, ()) = http::Response::new(()).into_parts();
        head.status = StatusCode::SERVICE_UNAVAILABLE;
        let frames = (0..32).map(|_| Ok(Bytes::from(vec![b'x'; 64 * 1024])));
        Ok((
            http::Response::from_parts(head, Body::stream(stream::iter(frames))),
            None,
        ))
    }
}

/// L2: an error answer past the cap is still a refusal at its head (rule
/// 2): `is_refused`, and the client told the provider refused.
#[tokio::test]
async fn an_error_answer_past_the_cap_is_still_refused() {
    for req in [post_keyed(CHAT, UNARY_REQ), post_keyed(CHAT, STREAM_REQ)] {
        let head_only = HeadOnly::default();
        let outcomes = Outcomes::default();
        let gw = Gateway::new()
            .provider(Provider::new(
                "openai",
                OpenAiChat,
                respan_gateway_http::connect(
                    |request| async move { HugeRefusal.send(request).await },
                    LIMIT,
                ),
            ))
            .and_then(|g| g.client::<Input, _, _>(OpenAiChat))
            .map(|g| g.plugin(outcomes.clone()))
            .map(|g| g.plugin(head_only.clone()))
            .and_then(|g| g.target("openai"))
            .unwrap();
        let resp = serve(&gw, req).await;
        let (status, _, body) = read_whole(resp.0).await;
        gw.idle().await;
        let body = body.map(|b| String::from_utf8_lossy(&b[..b.len().min(120)]).into_owned());
        let ends: Vec<_> = outcomes
            .all()
            .into_iter()
            .map(|e| e.map_err(|f| f.code().to_owned()))
            .collect();
        assert_eq!(
            (status.as_u16(), lock(&head_only.0).clone()),
            (503, vec![true]),
            "body {body:?}, ends {ends:?}"
        );
    }
}

struct RelaySame;
#[async_trait]
impl Plugin for RelaySame {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let same = (cx.format() == "openai-chat" && cx.target() == Some("openai"))
            || (cx.format() == "anthropic-messages" && cx.target() == Some("anthropic"));
        if same {
            respan_gateway_plugin_relay::Relay.on_client(cx).await
        } else {
            Ok(None)
        }
    }
    async fn on_provider(
        &self,
        px: &mut respan_gateway_core::ProviderContext,
    ) -> Result<(), Fault> {
        match px.format() == px.client().format() {
            true => respan_gateway_plugin_relay::Relay.on_provider(px).await,
            false => Ok(()),
        }
    }
}
