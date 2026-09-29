//! HTTP's driver ([`respan_gateway_http::codec`]) on the real formats:
//! relay, translation and streams through `Accepted`, `Gateway::open`,
//! `serve_dialect` and `provider_dialect`, with `dialect!` over thin
//! wrappers of `OpenAiChat` and `AnthropicMessages` (they implement the
//! old `Format` until the flip, decision 0020 step 5). The oracle runs each
//! scenario on the old path and on the driver's and wants the same bytes.
#![allow(clippy::unwrap_used)]

use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use respan_gateway_core::bytes::Bytes;
use respan_gateway_core::futures::{StreamExt, stream};
use respan_gateway_core::{
    ClientContext, Fault, Format, Gateway, Plugin, ProviderContext, Response, async_trait,
};
use respan_gateway_formats::conversation::Conversation;
use respan_gateway_formats::{AnthropicMessages, OpenAiChat};
use respan_gateway_http::codec::{self, Codec, Decoder, Encoder, Exchange};
use respan_gateway_http::http::{self, HeaderValue, StatusCode};
use respan_gateway_http::{Answer, Ask, Body, dialect};
use respan_gateway_testkit::{MockTransport, Outcomes, mock, post, read_whole};

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

/// An old format as a codec: its methods, its heads HTTP's.
#[derive(Clone)]
struct Http<F>(F);

impl<F: Format<Ask = Ask, Answer = Answer> + Clone> Codec for Http<F> {
    type Input = F::Input;
    type Output = F::Output;

    fn name(&self) -> &str {
        self.0.name()
    }

    fn accepts(&self, head: &Ask) -> bool {
        self.0.accepts(head)
    }

    fn request_decoder(&self, head: &Ask) -> Box<dyn Decoder<F::Input>> {
        self.0.request_decoder(head)
    }

    fn request_encoder(
        &self,
        client: Option<&Ask>,
        body: &mut Bytes,
        streams: bool,
    ) -> Box<dyn Encoder<F::Input, Ask>> {
        self.0.request_encoder(client, body, streams)
    }

    fn response_decoder(
        &self,
        answer: &Answer,
        call: &Ask,
        sent: &Bytes,
    ) -> Box<dyn Decoder<F::Output>> {
        self.0.response_decoder(answer, call, sent)
    }

    fn response_encoder(
        &self,
        answer: Option<&Answer>,
        ask: &Ask,
        body: &Bytes,
        refusal: Option<&Fault>,
    ) -> Box<dyn Encoder<F::Output, Answer>> {
        self.0.response_encoder(answer, ask, body, refusal)
    }

    fn is_content(&self, output: &F::Output) -> bool {
        self.0.is_content(output)
    }
}

dialect!(Http<OpenAiChat>);
dialect!(Http<AnthropicMessages>);

/// An old transport as an HTTP one, recording each call's relay mark.
struct Bridge<T>(T, Marks);

type Marks = Arc<Mutex<Vec<bool>>>;

#[async_trait]
impl<T> codec::Transport for Bridge<T>
where
    T: respan_gateway_core::Transport<Ask = Ask, Answer = Answer>,
{
    async fn send(&self, call: Exchange<Ask>) -> Result<Exchange<Answer>, Fault> {
        lock(&self.1).push(call.relayed);
        let mut old = respan_gateway_core::Exchange::new(call.head, call.body);
        old.extensions = call.extensions;
        old.relayed = call.relayed;
        old.streams = call.streams;
        let resp = self.0.send(old).await?;
        Ok(Exchange::new(resp.head, resp.body))
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
        let model = cx
            .input::<Conversation>()?
            .model
            .clone()
            .unwrap_or_default();
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
        lock(&self.0).push(px.is_head_only());
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum Path {
    /// The old `Format` and `Transport`, through `http::serve`.
    Legacy,
    /// The driver: `dialect!` codecs, through `codec::serve`.
    Driver,
}

/// A gateway serving both formats, calling `openai` and `anthropic`, which
/// have no keys of their own, on `path`.
struct Harness {
    path: Path,
    gw: Gateway,
    mock: MockTransport,
    outcomes: Outcomes,
    head_only: HeadOnly,
    marks: Marks,
}

impl Harness {
    fn new(path: Path) -> Self {
        let mock = MockTransport::new();
        let (outcomes, head_only, marks) =
            (Outcomes::default(), HeadOnly::default(), Marks::default());
        let gw = match path {
            Path::Legacy => Gateway::new()
                .provider("openai", OpenAiChat, mock.at(OPENAI))
                .and_then(|g| g.provider("anthropic", AnthropicMessages, mock.at(ANTHROPIC)))
                .and_then(|g| g.serve(OpenAiChat)?.serve(AnthropicMessages)),
            Path::Driver => Gateway::new()
                .provider_dialect(
                    "openai",
                    Http(OpenAiChat),
                    Bridge(mock.at(OPENAI), marks.clone()),
                )
                .and_then(|g| {
                    let bridge = Bridge(mock.at(ANTHROPIC), marks.clone());
                    g.provider_dialect("anthropic", Http(AnthropicMessages), bridge)
                })
                .and_then(|g| {
                    g.serve_dialect(Http(OpenAiChat))?
                        .serve_dialect(Http(AnthropicMessages))
                }),
        };
        let gw = gw
            .and_then(|g| g.plugin("outcomes", outcomes.clone()))
            .and_then(|g| g.plugin("head-only", head_only.clone()))
            .and_then(|g| g.plugin("route", Route))
            .unwrap();
        Self {
            path,
            gw,
            mock,
            outcomes,
            head_only,
            marks,
        }
    }

    /// Hand `req` to the gateway on this path's adapter.
    async fn serve(&self, req: http::Request<Body>) -> http::Response<Body> {
        match self.path {
            Path::Legacy => respan_gateway_http::serve(&self.gw, req).await.0,
            Path::Driver => codec::serve(&self.gw, req).await.0,
        }
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

    /// What each provider was sent: method, URL, headers and body.
    fn sent(&self) -> Vec<String> {
        let each = |r: &http::Request<Bytes>| {
            let body = String::from_utf8_lossy(r.body());
            format!("{} {} {:?} {body}", r.method(), r.uri(), r.headers())
        };
        self.mock.sent().iter().map(each).collect()
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
fn cut(chunks: &[&str]) -> mock::Reply {
    let mut reply = mock::sse(chunks);
    reply
        .body_mut()
        .push(Err(Fault::upstream("connection reset")));
    reply
}

/// A 429 of `body`, with `Retry-After: 7`.
fn limited(body: &str) -> mock::Reply {
    let mut reply = mock::json(StatusCode::TOO_MANY_REQUESTS, body);
    let wait = HeaderValue::from_static("7");
    reply.headers_mut().insert("retry-after", wait);
    reply
}

/// A scenario: its name, the client's request, and the providers' replies.
type Scenario = (
    &'static str,
    fn() -> http::Request<Body>,
    fn(&MockTransport),
);

fn scenarios() -> Vec<Scenario> {
    vec![
        (
            "byte-exact unary relay",
            || post_keyed(CHAT, UNARY_REQ),
            |m| {
                m.reply(OPENAI, mock::json(StatusCode::OK, UNARY));
            },
        ),
        (
            "byte-exact stream relay",
            || post_keyed(CHAT, STREAM_REQ),
            |m| {
                m.reply(OPENAI, mock::sse(STREAM));
            },
        ),
        (
            "openai client streams from anthropic",
            || post_keyed(CHAT, CLAUDE_STREAM_REQ),
            |m| {
                m.reply(ANTHROPIC, mock::sse(ANTHROPIC_STREAM));
            },
        ),
        (
            "anthropic client, whole from openai",
            || post_keyed(MESSAGES, ANTHROPIC_REQ),
            |m| {
                m.reply(OPENAI, mock::json(StatusCode::OK, OPENAI_WHOLE));
            },
        ),
        (
            "a cut relayed stream",
            || post_keyed(CHAT, STREAM_REQ),
            |m| {
                m.reply(OPENAI, cut(&STREAM[..2]));
            },
        ),
        (
            "a truncated translated stream",
            || post_keyed(CHAT, CLAUDE_STREAM_REQ),
            |m| {
                m.reply(ANTHROPIC, mock::sse(&ANTHROPIC_STREAM[..3]));
            },
        ),
        (
            "a refusal relayed as it came",
            || post_keyed(CHAT, UNARY_REQ),
            |m| {
                m.reply(OPENAI, mock::json(StatusCode::BAD_REQUEST, BAD));
            },
        ),
        (
            "a relayed 429 with Retry-After",
            || post_keyed(CHAT, STREAM_REQ),
            |m| {
                m.reply(OPENAI, limited(RATE_LIMITED));
            },
        ),
        (
            "a translated 429 with Retry-After",
            || post_keyed(MESSAGES, ANTHROPIC_REQ),
            |m| {
                m.reply(OPENAI, limited(RATE_LIMITED));
            },
        ),
        (
            "a request that cannot be translated",
            || {
                let tools = r#"{"model":"claude-x","messages":[],"tools":[{"type":"function","function":{"name":"f"}}]}"#;
                post_keyed(CHAT, tools)
            },
            |_| {},
        ),
        (
            "a provider that does not answer",
            || post_keyed(CHAT, UNARY_REQ),
            |_| {},
        ),
        (
            "no format accepts the request",
            || post_keyed("/nowhere", "{}"),
            |_| {},
        ),
    ]
}

/// The oracle: every scenario gives the client the same status, headers
/// and bytes, sends the providers the same calls, and ends the same way, on
/// the old path and through the driver.
#[tokio::test]
async fn the_driver_writes_what_the_old_path_writes() {
    for (name, req, script) in scenarios() {
        let mut runs = Vec::new();
        for path in [Path::Legacy, Path::Driver] {
            let h = Harness::new(path);
            script(&h.mock);
            let got = h.send(req()).await;
            runs.push((got, h.sent(), h.ends(), lock(&h.head_only.0).clone()));
        }
        let driver = runs.pop().unwrap();
        let legacy = runs.pop().unwrap();
        assert_eq!(legacy, driver, "{name}");
    }
}

#[tokio::test]
async fn a_relay_is_byte_exact_both_ways() {
    let h = Harness::new(Path::Driver);
    h.mock.reply(OPENAI, mock::json(StatusCode::OK, UNARY));
    h.mock.reply(OPENAI, mock::sse(STREAM));
    let got = h.send(post_keyed(CHAT, UNARY_REQ)).await;
    assert_eq!((got.status, got.body), (200, Ok(UNARY.to_owned())));
    let got = h.send(post_keyed(CHAT, STREAM_REQ)).await;
    assert_eq!(got.body, Ok(STREAM.concat()));
    let sent = h.mock.sent();
    assert_eq!(sent[0].body(), UNARY_REQ, "the client's bytes, unread");
    assert_eq!(sent[1].body(), STREAM_REQ);
    assert_eq!(h.ends(), ["ok", "ok"]);
}

#[tokio::test]
async fn a_client_is_answered_in_its_own_format() {
    let h = Harness::new(Path::Driver);
    h.mock.reply(ANTHROPIC, mock::sse(ANTHROPIC_STREAM));
    h.mock
        .reply(OPENAI, mock::json(StatusCode::OK, OPENAI_WHOLE));
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
    let h = Harness::new(Path::Driver);
    h.mock.reply(OPENAI, cut(&STREAM[..2]));
    let got = h.send(post_keyed(CHAT, STREAM_REQ)).await;
    assert_eq!(got.status, 200, "its head went with its first bytes");
    let body = got.body.unwrap_or_default();
    assert!(body.starts_with(STREAM[0]), "{body}");
    assert!(!body.contains("[DONE]"), "{body}");
    assert_eq!(h.ends(), ["upstream"]);
}

#[tokio::test]
async fn a_refusal_is_relayed_as_it_came() {
    let h = Harness::new(Path::Driver);
    h.mock
        .reply(OPENAI, mock::json(StatusCode::BAD_REQUEST, BAD));
    let got = h.send(post_keyed(CHAT, UNARY_REQ)).await;
    assert_eq!((got.status, got.body), (400, Ok(BAD.to_owned())));
    assert_eq!(*lock(&h.head_only.0), [true], "refused at its head");
}

#[tokio::test]
async fn a_429_keeps_its_retry_after_relayed_or_translated() {
    let h = Harness::new(Path::Driver);
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
async fn the_clients_key_reaches_a_provider_on_a_relay_alone() {
    let h = Harness::new(Path::Driver);
    h.mock.reply(OPENAI, mock::json(StatusCode::OK, UNARY));
    h.mock
        .reply(OPENAI, mock::json(StatusCode::OK, OPENAI_WHOLE));
    let _ = h.send(post_keyed(CHAT, UNARY_REQ)).await;
    let _ = h.send(post_keyed(MESSAGES, ANTHROPIC_REQ)).await;
    assert_eq!(*lock(&h.marks), [true, false], "Exchange::relayed");
    let sent = h.mock.sent();
    let key = |i: usize| sent[i].headers().get("authorization").cloned();
    assert_eq!(key(0), Some(HeaderValue::from_static("Bearer sk-client")));
    assert_eq!(key(1), None, "a translated call carries none");
}

/// A provider that refuses with a 429 whose body comes after a while, as
/// over a real network, on either path.
#[derive(Clone)]
struct SlowRefusal;

impl SlowRefusal {
    fn answer() -> (Answer, respan_gateway_core::Body) {
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

#[async_trait]
impl respan_gateway_core::Transport for SlowRefusal {
    type Ask = Ask;
    type Answer = Answer;

    async fn send(
        &self,
        _: respan_gateway_core::Exchange<Ask>,
    ) -> Result<respan_gateway_core::Exchange<Answer>, Fault> {
        let (head, body) = Self::answer();
        Ok(respan_gateway_core::Exchange::new(head, body))
    }
}

#[async_trait]
impl codec::Transport for SlowRefusal {
    async fn send(&self, _: Exchange<Ask>) -> Result<Exchange<Answer>, Fault> {
        let (head, body) = Self::answer();
        Ok(Exchange::new(head, body))
    }
}

/// Rule 3 over a real network: a refusal whose body comes slowly is read
/// whole before the call resolves, so `is_head_only` says it was refused at
/// its head. The old path decided on what was ready, and called it open;
/// the client gets the same bytes either way.
#[tokio::test]
async fn a_slow_refusal_is_head_only_through_the_driver() {
    let mut runs = Vec::new();
    for path in [Path::Legacy, Path::Driver] {
        let head_only = HeadOnly::default();
        let gw = match path {
            Path::Legacy => Gateway::new()
                .provider("openai", OpenAiChat, SlowRefusal)
                .and_then(|g| g.serve(OpenAiChat)),
            Path::Driver => Gateway::new()
                .provider_dialect("openai", Http(OpenAiChat), SlowRefusal)
                .and_then(|g| g.serve_dialect(Http(OpenAiChat))),
        };
        let gw = gw
            .and_then(|g| g.plugin("head-only", head_only.clone()))
            .and_then(|g| g.target("openai"))
            .unwrap();
        let resp = match path {
            Path::Legacy => {
                respan_gateway_http::serve(&gw, post_keyed(CHAT, UNARY_REQ))
                    .await
                    .0
            }
            Path::Driver => codec::serve(&gw, post_keyed(CHAT, UNARY_REQ)).await.0,
        };
        let (status, headers, body) = read_whole(resp).await;
        gw.idle().await;
        assert_eq!(status, StatusCode::TOO_MANY_REQUESTS, "{path:?}");
        assert_eq!(
            headers.get("retry-after").map(|v| v.as_bytes()),
            Some(&b"7"[..])
        );
        assert_eq!(body.unwrap(), RATE_LIMITED.as_bytes(), "{path:?}");
        runs.push(lock(&head_only.0).clone());
    }
    assert_eq!(runs, [vec![false], vec![true]]);
}
