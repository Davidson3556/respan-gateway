//! A format is one protocol's codec (decision 0007).

use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use futures::{StreamExt, TryStreamExt};
use http::header::{HOST, UPGRADE};
use http::{HeaderValue, StatusCode};
use http_body::Frame;
use http_body_util::{BodyExt, StreamBody};
use respan_gateway_core::{
    Body, ClientContext, Decoder, Encoder, Fault, Format, Gateway, Plugin, Response, Transport,
    async_trait, codes,
};
use respan_gateway_core::{bytes, futures, http, http_body, http_body_util, serde_json};
use respan_gateway_formats::conversation::{Conversation, Message, Reply, Role};
use respan_gateway_formats::relayed::Relayed;
use respan_gateway_formats::{AnthropicMessages, OpenAiChat};
use respan_gateway_testkit::{MockTransport, Outcomes, mock, post, read_whole, streamed};

/// A message in: its number in its stream, its text, and the bytes it came
/// in.
#[derive(Debug, Clone, PartialEq)]
struct In(usize, String, Relayed<In>);

impl In {
    fn new(n: usize, text: impl Into<String>) -> Self {
        In(n, text.into(), Relayed::default())
    }
}

/// A message out.
#[derive(Debug, Clone, PartialEq)]
struct Out(String);

/// A protocol of text messages at `/msg`, one per body or data frame once
/// upgraded. Its request decoder numbers the messages; its request encoder
/// numbers the frames it writes (`n|text`), puts a changed text in the path,
/// names the host a call's [`Host`] extension says, and cannot say "no". The
/// message `bad` is not `msg`, and `two` decodes to two inputs.
struct Msg;

/// The host a msg call names.
#[derive(Clone)]
struct Host(&'static str);

impl Format for Msg {
    type Input = In;
    type Output = Out;

    fn name(&self) -> &str {
        "msg"
    }

    fn accepts(&self, head: &http::request::Parts) -> bool {
        head.uri.path().starts_with("/msg")
    }

    fn request_decoder(&self, _request: &http::Request<Bytes>) -> Box<dyn Decoder<In>> {
        Box::new(Numbering(0, Arc::from("msg")))
    }

    fn request_encoder(
        &self,
        request: &mut http::Request<Bytes>,
        first: Option<&In>,
    ) -> Box<dyn Encoder<In>> {
        // The path names the input's text once changed; the client's path
        // says it for the client's own.
        if let Some(input) = first.filter(|i| i.2.bytes("msg", i).is_none()) {
            respan_gateway_formats::set_path(request, &format!("/msg/{}", input.1));
        }
        if let Some(Host(host)) = request.extensions().get::<Host>().cloned() {
            request
                .headers_mut()
                .insert(HOST, HeaderValue::from_static(host));
        }
        // The client's bytes, relayed unread, are the first frame.
        let mut numbered = Numbered(0);
        if !request.body().is_empty() {
            let text = String::from_utf8_lossy(request.body()).into_owned();
            numbered.0 += 1;
            *request.body_mut() = Bytes::from(format!("1|{text}"));
        }
        Box::new(numbered)
    }

    fn response_decoder(
        &self,
        _head: &http::response::Parts,
        _request: &http::Request<Bytes>,
    ) -> Box<dyn Decoder<Out>> {
        respan_gateway_formats::message_decoder(|raw: Bytes| {
            Ok(Out(String::from_utf8_lossy(&raw).into_owned()))
        })
    }

    fn response_encoder(
        &self,
        head: &mut http::response::Parts,
        request: &http::Request<Bytes>,
    ) -> Box<dyn Encoder<Out>> {
        if request.headers().contains_key(UPGRADE) && head.status == StatusCode::OK {
            head.status = StatusCode::SWITCHING_PROTOCOLS;
        }
        Box::new(Answers)
    }
}

/// Numbers the messages it decodes, each keeping its bytes.
struct Numbering(usize, Arc<str>);

impl Decoder<In> for Numbering {
    fn decode(&mut self, bytes: Bytes, out: &mut Vec<In>) -> Result<(), Fault> {
        let text = String::from_utf8_lossy(&bytes).into_owned();
        let kept = |n, text: String| {
            let origin = Relayed::came(&self.1, bytes.clone(), In::new(n, text.clone()));
            In(n, text, origin)
        };
        match &*text {
            "bad" => return Err(Fault::bad_request("not msg")),
            "two" => out.extend([kept(0, text.clone()), kept(0, text)]),
            _ => {
                self.0 += 1;
                out.push(kept(self.0, text));
            }
        }
        Ok(())
    }

    fn end(&mut self, _out: &mut Vec<In>) -> Result<(), Fault> {
        Ok(())
    }
}

/// Numbers the frames it writes, each as it came or as its text.
struct Numbered(usize);

impl Encoder<In> for Numbered {
    fn encode(&mut self, input: &In, out: &mut BytesMut) -> Result<(), Fault> {
        let text = match input.2.bytes("msg", input) {
            Some(raw) => String::from_utf8_lossy(raw).into_owned(),
            None => input.1.clone(),
        };
        if text.contains("no") {
            return Err(Fault::internal("msg cannot say no"));
        }
        self.0 += 1;
        out.extend_from_slice(format!("{}|{text}", self.0).as_bytes());
        Ok(())
    }

    fn end(&mut self, _end: &Result<(), Fault>, _out: &mut BytesMut) -> Result<(), Fault> {
        Ok(())
    }
}

/// Writes each answer as its text.
struct Answers;

impl Encoder<Out> for Answers {
    fn encode(&mut self, answer: &Out, out: &mut BytesMut) -> Result<(), Fault> {
        out.extend_from_slice(answer.0.as_bytes());
        Ok(())
    }

    fn end(&mut self, _end: &Result<(), Fault>, _out: &mut BytesMut) -> Result<(), Fault> {
        Ok(())
    }
}

/// A msg provider that echoes each frame of a call, noting its path and
/// what its body carried: each frame, or the code of the fault that broke
/// it off. It switches protocols when asked.
#[derive(Clone, Default)]
struct Echo(Arc<Mutex<Vec<String>>>);

impl Echo {
    fn seen(&self) -> Vec<String> {
        self.0.lock().map(|s| s.clone()).unwrap_or_default()
    }
}

#[async_trait]
impl Transport for Echo {
    async fn send(&self, call: http::Request<Body>) -> Result<http::Response<Body>, Fault> {
        let (head, body) = call.into_parts();
        let seen = self.0.clone();
        seen.lock().map(|mut s| s.push(head.uri.path().into())).ok();
        if let Some(host) = head.headers.get(HOST).and_then(|h| h.to_str().ok()) {
            seen.lock().map(|mut s| s.push(format!("host={host}"))).ok();
        }
        let echoed = body.into_data_stream().map(move |frame| {
            let noted = match &frame {
                Ok(bytes) => String::from_utf8_lossy(bytes).into_owned(),
                Err(fault) => format!("Err({})", fault.code()),
            };
            seen.lock().map(|mut s| s.push(noted)).ok();
            frame.map(Frame::data)
        });
        let mut resp = http::Response::new(StreamBody::new(echoed).boxed_unsync());
        if head.headers.contains_key(UPGRADE) {
            *resp.status_mut() = StatusCode::SWITCHING_PROTOCOLS;
        }
        Ok(resp)
    }
}

/// A gateway serving msg, calling a msg provider over `echo`, with `plugin`.
fn msg(echo: &Echo, plugin: impl Plugin) -> Gateway {
    Gateway::new()
        .provider("msg", Msg, echo.clone())
        .and_then(|g| g.serve(Msg)?.target("msg")?.plugin("p", plugin))
        .unwrap_or_else(|e| panic!("{e}"))
}

/// Runs a filter over the msg inputs that sets `from` to `to`, noting each
/// input it sees; with no `from`, it changes none.
#[derive(Clone, Default)]
struct Edit(Option<(&'static str, &'static str)>, Arc<Mutex<Vec<In>>>);

#[async_trait]
impl Plugin for Edit {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let (edit, seen) = (self.0, self.1.clone());
        cx.filter(move |input: &mut In| {
            seen.lock().map(|mut s| s.push(input.clone())).ok();
            if let Some((from, to)) = edit.filter(|(from, _)| input.1 == *from) {
                input.1 = input.1.replace(from, to);
            }
            Ok(())
        })?;
        Ok(None)
    }
}

/// An upgrading msg request, whose messages are `frames`, and the bodies of
/// its response as the client reads them.
async fn talk(gw: &Gateway, frames: &[&'static str]) -> (StatusCode, Vec<Result<Bytes, Fault>>) {
    let (send, mut req) = streamed("/msg");
    let upgrade = HeaderValue::from_static("msg");
    req.headers_mut().insert(UPGRADE, upgrade);
    let (resp, _) = gw.handle(req).await;
    for frame in frames {
        let _ = send.unbounded_send(Bytes::from_static(frame.as_bytes()));
    }
    drop(send);
    let (head, body) = resp.into_parts();
    let read = body.into_data_stream().collect::<Vec<_>>();
    let read = tokio::time::timeout(Duration::from_secs(5), read).await;
    (
        head.status,
        read.unwrap_or_else(|_| panic!("the body never ended")),
    )
}

// A stream's request decoder and encoder each keep their state across frames.
#[tokio::test]
async fn r5_a_streams_request_codecs_keep_their_state_across_frames() {
    let (echo, edit) = (Echo::default(), Edit::default());
    let gw = msg(&echo, edit.clone());

    let (status, read) = talk(&gw, &["a", "b", "c"]).await;

    assert_eq!(status, StatusCode::SWITCHING_PROTOCOLS);
    assert_eq!(read.len(), 3);
    assert_eq!(echo.seen(), ["/msg", "1|a", "2|b", "3|c"]);
    let seen = edit.1.lock().map(|s| s.clone()).unwrap_or_default();
    let numbered = [(1, "a"), (2, "b"), (3, "c")].map(|(n, t)| In::new(n, t));
    assert_eq!(seen, numbered);
}

// A stream's frames are decoded as they pass, even unread: a frame its decoder
// refuses aborts the call.
#[tokio::test]
async fn r6_a_stream_is_decoded_as_it_passes() {
    let echo = Echo::default();
    let gw = msg(&echo, Outcomes::default());

    let _ = talk(&gw, &["a", "bad"]).await;

    assert_eq!(echo.seen(), ["/msg", "1|a", "Err(bad_request)"]);
}

// A bad frame aborts the call's body.
#[tokio::test]
async fn r7_a_bad_frame_aborts_the_call() {
    let echo = Echo::default();
    let gw = msg(&echo, Edit::default());
    let _ = talk(&gw, &["a", "bad", "b"]).await;
    assert_eq!(echo.seen(), ["/msg", "1|a", "Err(bad_request)"]);
}

// Content that cannot be written is whose it is, by one rule for the first
// input and later frames alike: the client's if filters left it equal to its
// decoding, else a plugin's.
#[tokio::test]
async fn r8_the_fault_of_content_that_cannot_be_written_is_whose_it_is() {
    let cases = [
        ("yes", Some(("yes", "no")), codes::CONFIG),
        ("no", Some(("no", "no!")), codes::CONFIG),
        ("no", None, codes::BAD_REQUEST),
    ];
    for (body, edit, code) in cases {
        let echo = Echo::default();
        let gw = msg(&echo, Edit(edit, Arc::default()));
        let (_, ended) = gw.handle(post("/msg", body)).await;
        let end = ended.await.end.map_err(|f| f.code().to_owned());
        assert_eq!(end, Err(code.to_owned()), "{body} {edit:?}");
        assert!(echo.seen().is_empty(), "nothing is sent");
    }
    for (frame, edit, code) in cases {
        let echo = Echo::default();
        let gw = msg(&echo, Edit(edit, Arc::default()));
        let _ = talk(&gw, &["a", frame]).await;
        let seen = echo.seen()[1..].to_vec();
        assert_eq!(seen, ["1|a".to_owned(), format!("Err({code})")], "{frame}");
    }
}

// A format reads what it keeps in the head, such as a model in the path, from
// the first input: a changed one's values, else the client's head.
#[tokio::test]
async fn r10_the_request_encoder_reads_the_head_from_the_first_part() {
    for (edit, path) in [(None, "/msg/x"), (Some(("a", "b")), "/msg/b")] {
        let echo = Echo::default();
        let gw = msg(&echo, Edit(edit, Arc::default()));
        let _ = read_whole(gw.handle(post("/msg/x", "a")).await.0).await;
        assert_eq!(echo.seen()[0], path, "{edit:?}");
    }
}

// A bodiless response drains a call's body, whose usage counts, never a
// plugin's own stream, which may never end.
#[tokio::test]
async fn o11_1_a_head_to_an_endless_plugin_stream_ends_at_its_head() {
    struct Endless;

    #[async_trait]
    impl Plugin for Endless {
        async fn on_client(&self, _cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            Ok(Some(Response::stream(futures::stream::pending::<
                Result<Out, Fault>,
            >())))
        }
    }

    let log = Outcomes::default();
    let gw = Gateway::new()
        .serve(Msg)
        .and_then(|g| g.plugin("log", log.clone())?.plugin("endless", Endless))
        .unwrap_or_else(|e| panic!("{e}"));
    let mut head = post("/msg", "");
    *head.method_mut() = http::Method::HEAD;

    let (status, _, body) = read_whole(gw.handle(head).await.0).await;
    let idle = tokio::time::timeout(Duration::from_secs(5), gw.idle()).await;

    assert_eq!((status, body.ok()), (StatusCode::OK, Some(Bytes::new())));
    assert!(idle.is_ok(), "the gateway stays busy");
    assert_eq!(log.only().end, Ok(()));
}

const CHAT: &str = r#"{"model":"m","messages":[{"role":"user","content":"hi"}]}"#;
const UNARY: &str = r#"{"id":"c","choices":[{"message":{"content":"hi"}}]}"#;

/// A client request to `path` with `body`, and headers a proxy passes on or
/// keeps, and a query.
fn with_headers(path: &str, body: &'static str) -> http::Request<Body> {
    let mut req = post(&format!("{path}?api-version=2"), body);
    for (name, value) in [
        ("x-trace", "7"),
        ("user-agent", "sdk"),
        ("forwarded", "for=1.2.3.4"),
        ("x-forwarded-for", "1.2.3.4"),
        ("if-none-match", "\"e\""),
        ("range", "bytes=0-1"),
        ("cookie", "session=1"),
        ("accept-encoding", "gzip"),
        ("x-http-method-override", "DELETE"),
    ] {
        req.headers_mut()
            .insert(name, HeaderValue::from_static(value));
    }
    req
}

/// A gateway serving OpenAI and Anthropic clients, calling an OpenAI
/// provider on `mock`, with `plugins` in order.
fn chat(mock: &MockTransport, plugins: Vec<Arc<dyn Plugin>>) -> Gateway {
    let gw = Gateway::new()
        .provider("openai", OpenAiChat, mock.at("http://openai.test/v1"))
        .and_then(|g| {
            g.serve(OpenAiChat)?
                .serve(AnthropicMessages)?
                .target("openai")
        });
    let mut named = plugins.into_iter().enumerate();
    let gw = gw.and_then(|g| named.try_fold(g, |g, (i, p)| g.plugin(format!("p{i}"), p)));
    gw.unwrap_or_else(|e| panic!("{e}"))
}

// A relay passes the client's end-to-end headers and query as a proxy does,
// minus who sent it, conditions, ranges, overrides, cookies and encodings; a
// translated call carries none.
#[tokio::test]
async fn r3_a_relay_passes_the_clients_headers_and_query_as_a_proxy_does() {
    let mock = MockTransport::new();
    let gw = chat(&mock, Vec::new());
    mock.reply("http://openai.test/v1", mock::json(StatusCode::OK, UNARY));
    let _ = read_whole(
        gw.handle(with_headers("/v1/chat/completions", CHAT))
            .await
            .0,
    )
    .await;
    let messages = r#"{"model":"m","max_tokens":5,"messages":[{"role":"user","content":"hi"}]}"#;
    mock.reply("http://openai.test/v1", mock::json(StatusCode::OK, UNARY));
    let _ = read_whole(gw.handle(with_headers("/v1/messages", messages)).await.0).await;

    let sent = mock.sent();
    let (relayed, translated) = (&sent[0], &sent[1]);
    assert_eq!(relayed.uri().query(), Some("api-version=2"));
    assert_eq!(relayed.headers()["x-trace"], "7");
    assert_eq!(relayed.headers()["user-agent"], "sdk");
    for kept in [
        "forwarded",
        "x-forwarded-for",
        "if-none-match",
        "range",
        "cookie",
        "accept-encoding",
        "x-http-method-override",
    ] {
        assert!(relayed.headers().get(kept).is_none(), "{kept}");
    }
    assert_eq!(translated.uri().query(), None);
    assert!(translated.headers().get("x-trace").is_none());
}

// A call's own input is written from its value, with none of the client's
// bytes, headers or query, even in the client's format.
#[tokio::test]
async fn r9_a_calls_own_input_is_written_from_its_value_alone() {
    struct Judge;

    #[async_trait]
    impl Plugin for Judge {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let prompt = Conversation::new([Message::new(Role::User, "judge")]);
            let verdict = cx.call("openai").with_input(prompt).await?;
            let read: Vec<Reply> = verdict.outputs::<Reply>().try_collect().await?;
            assert!(!read.is_empty());
            Ok(None)
        }
    }

    let mock = MockTransport::new();
    let gw = chat(&mock, vec![Arc::new(Judge)]);
    for _ in 0..2 {
        mock.reply("http://openai.test/v1", mock::json(StatusCode::OK, UNARY));
    }
    let _ = read_whole(
        gw.handle(with_headers("/v1/chat/completions", CHAT))
            .await
            .0,
    )
    .await;

    let sent = mock.sent();
    let own: serde_json::Value = serde_json::from_slice(sent[0].body()).unwrap_or_default();
    assert_eq!(own["messages"][0]["content"], "judge");
    assert_eq!(sent[0].uri().query(), None);
    assert!(sent[0].headers().get("x-trace").is_none());
    assert_eq!(sent[1].body(), CHAT.as_bytes());
}

/// Anthropic Messages, noting where each reply its response encoder gets
/// came from: `own` bytes, `openai` bytes, or `none`.
#[derive(Clone, Default)]
struct Noted(Arc<Mutex<Vec<&'static str>>>);

impl Format for Noted {
    type Input = Conversation;
    type Output = Reply;

    fn name(&self) -> &str {
        AnthropicMessages.name()
    }

    fn accepts(&self, head: &http::request::Parts) -> bool {
        AnthropicMessages.accepts(head)
    }

    fn request_decoder(&self, request: &http::Request<Bytes>) -> Box<dyn Decoder<Conversation>> {
        AnthropicMessages.request_decoder(request)
    }

    fn request_encoder(
        &self,
        request: &mut http::Request<Bytes>,
        first: Option<&Conversation>,
    ) -> Box<dyn Encoder<Conversation>> {
        AnthropicMessages.request_encoder(request, first)
    }

    fn response_decoder(
        &self,
        head: &http::response::Parts,
        request: &http::Request<Bytes>,
    ) -> Box<dyn Decoder<Reply>> {
        AnthropicMessages.response_decoder(head, request)
    }

    fn response_encoder(
        &self,
        head: &mut http::response::Parts,
        request: &http::Request<Bytes>,
    ) -> Box<dyn Encoder<Reply>> {
        let encoder = AnthropicMessages.response_encoder(head, request);
        Box::new(Noting(self.0.clone(), encoder))
    }

    fn usage<'a>(&self, reply: &'a Reply) -> Option<&'a respan_gateway_core::Usage> {
        AnthropicMessages.usage(reply)
    }

    fn says(&self, reply: &Reply) -> bool {
        AnthropicMessages.says(reply)
    }
}

struct Noting(Arc<Mutex<Vec<&'static str>>>, Box<dyn Encoder<Reply>>);

impl Encoder<Reply> for Noting {
    fn encode(&mut self, reply: &Reply, out: &mut BytesMut) -> Result<(), Fault> {
        let noted = match (
            reply.origin(AnthropicMessages.name()),
            reply.origin(OpenAiChat.name()),
        ) {
            (Some(_), _) => "own",
            (_, Some(_)) => "openai",
            _ => "none",
        };
        self.0.lock().map(|mut n| n.push(noted)).ok();
        self.1.encode(reply, out)
    }

    fn end(&mut self, end: &Result<(), Fault>, out: &mut BytesMut) -> Result<(), Fault> {
        self.1.end(end, out)
    }
}

// An encoder gets another format's replies with their origin and writes the
// bytes only if they are its own format's.
#[tokio::test]
async fn r2_an_encoder_gets_another_formats_replies_with_their_origin() {
    let stream = [
        ": ping\n\n",
        "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"m\",\"model\":\"c\"}}\n\n",
        "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"hi\"}}\n\n",
        "event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n",
    ];
    let chunk =
        "data: {\"id\":\"c\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi\"}}]}\n\n";
    let body =
        r#"{"model":"m","max_tokens":5,"stream":true,"messages":[{"role":"user","content":"hi"}]}"#;
    let (mut noted, mut bodies) = (Vec::new(), Vec::new());
    for provider in ["anthropic", "openai"] {
        let (mock, noting) = (MockTransport::new(), Noted::default());
        let gw = Gateway::new()
            .provider(
                "anthropic",
                noting.clone(),
                mock.at("http://anthropic.test"),
            )
            .and_then(|g| g.provider("openai", OpenAiChat, mock.at("http://openai.test")))
            .and_then(|g| g.serve(noting.clone())?.target(provider))
            .unwrap_or_else(|e| panic!("{e}"));
        match provider {
            "anthropic" => mock.reply("http://anthropic.test", mock::sse(&stream)),
            _ => mock.reply(
                "http://openai.test",
                mock::sse(&[": ping\n\n", chunk, "data: [DONE]\n\n"]),
            ),
        };
        let (_, _, out) = read_whole(gw.handle(post("/v1/messages", body)).await.0).await;
        bodies.push(String::from_utf8_lossy(&out.unwrap_or_default()).into_owned());
        noted.push(noting.0.lock().map(|n| n.clone()).unwrap_or_default());
    }
    assert!(!noted[0].is_empty() && noted[0].iter().all(|n| *n == "own"));
    assert!(bodies[0].contains(": ping\n\n"), "{}", bodies[0]);
    // Another format's bytes come with their replies, and are not written.
    assert!(!noted[1].is_empty() && noted[1].iter().all(|n| *n == "openai"));
    assert!(
        !bodies[1].contains("choices") && !bodies[1].contains("ping"),
        "{}",
        bodies[1]
    );
}

// A filter added once an upgrading request's frames pass sees the later ones.
#[tokio::test]
async fn r13_a_filter_added_once_frames_pass_sees_the_later_ones() {
    struct Late;

    #[async_trait]
    impl Plugin for Late {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let mut outputs = cx.call("msg").await?.outputs::<Out>();
            let first = outputs.next().await;
            cx.filter(|input: &mut In| {
                input.1.make_ascii_uppercase();
                Ok(())
            })?;
            let all = futures::stream::iter(first).chain(outputs);
            Ok(Some(Response::stream(all)))
        }
    }

    let echo = Echo::default();
    let gw = msg(&echo, Late);
    let (send, mut req) = streamed("/msg");
    req.headers_mut()
        .insert(UPGRADE, HeaderValue::from_static("msg"));
    for frame in ["a", "b"] {
        let _ = send.unbounded_send(Bytes::from_static(frame.as_bytes()));
    }
    drop(send);
    let (resp, _) = gw.handle(req).await;
    let _ = read_whole(resp).await;

    assert_eq!(echo.seen(), ["/msg", "1|a", "2|B"]);
}

// A decoder's two inputs of one frame go as two.
#[tokio::test]
async fn r13_a_request_decoders_two_inputs_of_one_frame_go_as_two() {
    let echo = Echo::default();
    let gw = msg(&echo, Edit::default());

    let _ = talk(&gw, &["a", "two"]).await;

    assert_eq!(echo.seen(), ["/msg", "1|a", "2|two", "3|two"]);
}

// A call carries what a proxy passes of the client's, never its `Host`; what
// the format sets is its own.
#[tokio::test]
async fn r13_the_format_sets_what_the_kernel_never_passes() {
    struct Named(Option<&'static str>);

    #[async_trait]
    impl Plugin for Named {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            if let Some(host) = self.0 {
                cx.head_mut().extensions.insert(Host(host));
            }
            Ok(None)
        }
    }

    for (named, seen) in [
        (None, None),
        (Some("msg.example"), Some("host=msg.example")),
    ] {
        let echo = Echo::default();
        let mut req = post("/msg", "a");
        req.headers_mut()
            .insert(HOST, HeaderValue::from_static("gateway.example"));
        let _ = read_whole(msg(&echo, Named(named)).handle(req).await.0).await;
        let host = echo.seen().into_iter().find(|s| s.starts_with("host="));
        assert_eq!(host.as_deref(), seen);
    }
}
