//! A format is one protocol's codec.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use futures::{StreamExt, TryStreamExt};
use http::header::{HOST, UPGRADE};
use http::{HeaderValue, StatusCode};
use http_body_util::BodyExt;
use respan_gateway_core::futures;
use respan_gateway_core::{
    ClientContext, Fault, Gateway, Plugin, ProviderContext, Response, async_trait, codes,
};
use respan_gateway_http::bytes;
use respan_gateway_http::{Codec, Decoder, Encoder, Exchange, impl_format};
use respan_gateway_http::{http, http_body_util};
use respan_gateway_messages::conversation::{Conversation, Message, Reply, Role};
use respan_gateway_messages::relayed::Relayed;
use respan_gateway_providers::{AnthropicMessages, OpenAiChat};
use respan_gateway_testkit::{MockTransport, Outcomes, mock, post, read_whole, serve, streamed};

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
/// and cannot say "no". The
/// message `bad` is not `msg`, and `two` decodes to two inputs.
#[derive(Clone)]
struct Msg;

impl Codec for Msg {
    type Input = In;
    type Output = Out;

    fn name(&self) -> &str {
        "msg"
    }

    fn accepts(&self, head: &respan_gateway_http::RequestHead) -> bool {
        head.uri.path().starts_with("/msg")
    }

    fn request_decoder(&self, _request: &respan_gateway_http::RequestHead) -> Box<dyn Decoder<In>> {
        Box::new(Numbering(0, Arc::from("msg")))
    }

    fn request_encoder(
        &self,
        client: Option<&respan_gateway_http::RequestHead>,
        body: &mut Bytes,
        _streams: bool,
    ) -> Box<dyn Encoder<In, respan_gateway_http::RequestHead>> {
        let mut head = respan_gateway_formats::call_head(client);
        // The path names the input's text once changed; the client's path
        // says it for the client's own.
        let head = move |first: Option<&In>| {
            if let Some(input) = first.filter(|i| i.2.bytes("msg", i).is_none()) {
                respan_gateway_formats::set_path(&mut head, &format!("/msg/{}", input.1));
            }
            head
        };
        // The client's bytes, relayed unread, are the first frame.
        let mut numbered = Numbered(0);
        if !body.is_empty() {
            let text = String::from_utf8_lossy(body).into_owned();
            numbered.0 += 1;
            *body = Bytes::from(format!("1|{text}"));
        }
        respan_gateway_formats::headed(head, Box::new(numbered))
    }

    fn response_decoder(
        &self,
        _head: &respan_gateway_http::ResponseHead,
        _request: &respan_gateway_http::RequestHead,
        _sent: &Bytes,
    ) -> Box<dyn Decoder<Out>> {
        respan_gateway_formats::message_decoder(|raw: Bytes| {
            Ok(Out(String::from_utf8_lossy(&raw).into_owned()))
        })
    }

    fn response_encoder(
        &self,
        answer: Option<&respan_gateway_http::ResponseHead>,
        request: &respan_gateway_http::RequestHead,
        _body: &Bytes,
    ) -> Box<dyn Encoder<Out, respan_gateway_http::ResponseHead>> {
        let mut head = respan_gateway_formats::response_head(answer);
        if respan_gateway_http::bodiless(&request.method, head.status) {
            return respan_gateway_formats::with_head(
                head,
                respan_gateway_formats::whole::no_body(),
            );
        }
        if request.headers.contains_key(UPGRADE) && head.status == StatusCode::OK {
            head.status = StatusCode::SWITCHING_PROTOCOLS;
        }
        respan_gateway_formats::with_head(head, Box::new(Answers))
    }

    fn refusal(&self, _: &Fault, _: &Bytes, _: &mut respan_gateway_http::ResponseHead) -> Bytes {
        Bytes::new()
    }
}

impl_format!(Msg);

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

impl Encoder<In, ()> for Numbered {
    fn head(&mut self, _first: Option<&In>) {}

    fn encode(&mut self, input: &In, out: &mut BytesMut) -> Result<(), Fault> {
        let text = match input.2.bytes("msg", input) {
            Some(raw) => String::from_utf8_lossy(raw).into_owned(),
            None => input.1.clone(),
        };
        if text.contains("no") {
            return Err(input.2.whose(input, Fault::internal("msg cannot say no")));
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

impl Encoder<Out, ()> for Answers {
    fn head(&mut self, _first: Option<&Out>) {}

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

impl Echo {
    pub async fn send(
        &self,
        call: Exchange<respan_gateway_http::RequestHead>,
    ) -> Result<Exchange<respan_gateway_http::ResponseHead>, Fault> {
        let (head, body) = (call.head, call.body);
        let seen = self.0.clone();
        seen.lock().map(|mut s| s.push(head.uri.path().into())).ok();
        if let Some(host) = head.headers.get(HOST).and_then(|h| h.to_str().ok()) {
            seen.lock().map(|mut s| s.push(format!("host={host}"))).ok();
        }
        let echoed = body.map(move |frame| {
            let noted = match &frame {
                Ok(bytes) => String::from_utf8_lossy(bytes).into_owned(),
                Err(fault) => format!("Err({})", fault.code()),
            };
            seen.lock().map(|mut s| s.push(noted)).ok();
            frame
        });
        let mut resp = respan_gateway_http::fresh_response_head();
        if head.headers.contains_key(UPGRADE) {
            resp.status = StatusCode::SWITCHING_PROTOCOLS;
        }
        Ok(Exchange::new(resp, echoed.boxed()))
    }
}
impl From<Echo> for respan_gateway_http::Transport {
    fn from(value: Echo) -> Self {
        let value = std::sync::Arc::new(value);
        Self::new(move |call| {
            let value = value.clone();
            async move { value.send(call).await }
        })
    }
}

/// A gateway serving msg, calling a msg provider over `echo`, with `plugin`.
fn msg(echo: &Echo, plugin: impl Plugin) -> Gateway {
    Gateway::new()
        .provider::<In, _, _>("msg", Msg, respan_gateway_http::connect(echo.clone()))
        .and_then(|g| {
            g.client::<In, _, _>(Msg)?
                .target("msg")?
                .plugin("p", plugin)
        })
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
    let (resp, _) = serve(gw, req).await;
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
// decoding, else a plugin's. The kind tells them apart (`Relayed::whose`).
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
        let (_, ended) = serve(&gw, post("/msg", body)).await;
        let end = ended.await.map_err(|f| f.code().to_owned());
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
        let _ = read_whole(serve(&gw, post("/msg/x", "a")).await.0).await;
        assert_eq!(echo.seen()[0], path, "{edit:?}");
    }
}

// A bodiless response ends at its head: it never reads a
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
        .client::<In, _, _>(Msg)
        .and_then(|g| g.plugin("log", log.clone())?.plugin("endless", Endless))
        .unwrap_or_else(|e| panic!("{e}"));
    let mut head = post("/msg", "");
    *head.method_mut() = http::Method::HEAD;

    let (status, _, body) = read_whole(serve(&gw, head).await.0).await;
    let idle = tokio::time::timeout(Duration::from_secs(5), gw.idle()).await;

    assert_eq!((status, body.ok()), (StatusCode::OK, Some(Bytes::new())));
    assert!(idle.is_ok(), "the gateway stays busy");
    assert_eq!(log.only(), Ok(()));
}

const CHAT: &str = r#"{"model":"m","messages":[{"role":"user","content":"hi"}]}"#;
const UNARY: &str = r#"{"id":"c","choices":[{"message":{"content":"hi"}}]}"#;

/// A client request to `path` with `body`, and headers a proxy passes on or
/// keeps, and a query.
fn with_headers(path: &str, body: &'static str) -> http::Request<respan_gateway_http::Body> {
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
        .provider::<Conversation, _, _>(
            "openai",
            OpenAiChat,
            respan_gateway_http::connect(mock.at("http://openai.test/v1")),
        )
        .and_then(|g| {
            g.client::<Conversation, _, _>(OpenAiChat)?
                .client::<Conversation, _, _>(AnthropicMessages)?
                .target("openai")
        });
    let mut named = plugins.into_iter().enumerate();
    let gw = gw.and_then(|g| named.try_fold(g, |g, (i, p)| g.plugin(format!("p{i}"), p)));
    gw.unwrap_or_else(|e| panic!("{e}"))
}

// A relay passes the client's end-to-end headers and query as a proxy does,
// minus conditions, ranges, overrides and encodings, and the transport strips
// who sent it and cookies (MockTransport as HttpTransport does); a
// translated call carries none.
#[tokio::test]
async fn r3_a_relay_passes_the_clients_headers_and_query_as_a_proxy_does() {
    let mock = MockTransport::new();
    let gw = chat(&mock, Vec::new());
    mock.reply("http://openai.test/v1", mock::json(StatusCode::OK, UNARY));
    let _ = read_whole(
        serve(&gw, with_headers("/v1/chat/completions", CHAT))
            .await
            .0,
    )
    .await;
    let messages = r#"{"model":"m","max_tokens":5,"messages":[{"role":"user","content":"hi"}]}"#;
    mock.reply("http://openai.test/v1", mock::json(StatusCode::OK, UNARY));
    let _ = read_whole(serve(&gw, with_headers("/v1/messages", messages)).await.0).await;

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

// The kernel passes the client's cookies and forwarding headers on;
// stripping them is the transport's policy, so one of its own gets them.
// Conditions, ranges, overrides and encodings are the transport's to strip
// too ([`respan_gateway_http::proxy_call`]),
// which this one does, as an HTTP transport would.
#[tokio::test]
async fn cookies_and_forwarding_headers_are_the_transports_to_strip() {
    #[derive(Clone, Default)]
    struct Keeps(Arc<Mutex<Option<http::HeaderMap>>>);

    impl Keeps {
        pub async fn send(
            &self,
            call: Exchange<respan_gateway_http::RequestHead>,
        ) -> Result<Exchange<respan_gateway_http::ResponseHead>, Fault> {
            let mut head = call.head;
            respan_gateway_http::proxy_call(&mut head);
            *self.0.lock().unwrap_or_else(|e| e.into_inner()) = Some(head.headers);
            let mut resp = http::Response::new(http_body_util::Full::new(Bytes::from(UNARY)));
            let json = HeaderValue::from_static("application/json");
            resp.headers_mut().insert("content-type", json);
            Ok(respan_gateway_http::from_response(resp))
        }
    }
    impl From<Keeps> for respan_gateway_http::Transport {
        fn from(value: Keeps) -> Self {
            let value = std::sync::Arc::new(value);
            Self::new(move |call| {
                let value = value.clone();
                async move { value.send(call).await }
            })
        }
    }

    let keeps = Keeps::default();
    let gw = Gateway::new()
        .provider::<Conversation, _, _>(
            "openai",
            OpenAiChat,
            respan_gateway_http::connect(keeps.clone()),
        )
        .and_then(|g| g.client::<Conversation, _, _>(OpenAiChat)?.target("openai"))
        .unwrap_or_else(|e| panic!("{e}"));
    let _ = read_whole(
        serve(&gw, with_headers("/v1/chat/completions", CHAT))
            .await
            .0,
    )
    .await;

    let sent = keeps
        .0
        .lock()
        .ok()
        .and_then(|s| s.clone())
        .unwrap_or_default();
    for passed in ["forwarded", "x-forwarded-for", "cookie", "x-trace"] {
        assert!(sent.get(passed).is_some(), "{passed}");
    }
    for kept in [
        "if-none-match",
        "range",
        "accept-encoding",
        "x-http-method-override",
    ] {
        assert!(sent.get(kept).is_none(), "{kept}");
    }
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
        serve(&gw, with_headers("/v1/chat/completions", CHAT))
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

impl Codec for Noted {
    type Input = Conversation;
    type Output = Reply;

    fn name(&self) -> &str {
        AnthropicMessages.name()
    }

    fn accepts(&self, head: &respan_gateway_http::RequestHead) -> bool {
        AnthropicMessages.accepts(head)
    }

    fn request_decoder(
        &self,
        request: &respan_gateway_http::RequestHead,
    ) -> Box<dyn Decoder<Conversation>> {
        AnthropicMessages.request_decoder(request)
    }

    fn request_encoder(
        &self,
        client: Option<&respan_gateway_http::RequestHead>,
        body: &mut Bytes,
        streams: bool,
    ) -> Box<dyn Encoder<Conversation, respan_gateway_http::RequestHead>> {
        AnthropicMessages.request_encoder(client, body, streams)
    }

    fn response_decoder(
        &self,
        head: &respan_gateway_http::ResponseHead,
        request: &respan_gateway_http::RequestHead,
        sent: &Bytes,
    ) -> Box<dyn Decoder<Reply>> {
        AnthropicMessages.response_decoder(head, request, sent)
    }

    fn response_encoder(
        &self,
        answer: Option<&respan_gateway_http::ResponseHead>,
        request: &respan_gateway_http::RequestHead,
        body: &Bytes,
    ) -> Box<dyn Encoder<Reply, respan_gateway_http::ResponseHead>> {
        let mut encoder = AnthropicMessages.response_encoder(answer, request, body);
        let head = encoder.head(None);
        respan_gateway_formats::with_head(head, Box::new(Noting(self.0.clone(), encoder)))
    }

    fn refusal(
        &self,
        fault: &Fault,
        body: &Bytes,
        head: &mut respan_gateway_http::ResponseHead,
    ) -> Bytes {
        AnthropicMessages.refusal(fault, body, head)
    }
}

impl_format!(Noted);

struct Noting(
    Arc<Mutex<Vec<&'static str>>>,
    Box<dyn Encoder<Reply, respan_gateway_http::ResponseHead>>,
);

impl Encoder<Reply, ()> for Noting {
    fn head(&mut self, _first: Option<&Reply>) {}

    fn encode(&mut self, reply: &Reply, out: &mut BytesMut) -> Result<(), Fault> {
        let noted = match (
            reply.came_in(AnthropicMessages.name()),
            reply.came_in(OpenAiChat.name()),
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
            .provider::<<Noted as Codec>::Input, _, _>(
                "anthropic",
                noting.clone(),
                respan_gateway_http::connect(mock.at("http://anthropic.test")),
            )
            .and_then(|g| {
                g.provider::<Conversation, _, _>(
                    "openai",
                    OpenAiChat,
                    respan_gateway_http::connect(mock.at("http://openai.test")),
                )
            })
            .and_then(|g| {
                g.client::<<Noted as Codec>::Input, _, _>(noting.clone())?
                    .target(provider)
            })
            .unwrap_or_else(|e| panic!("{e}"));
        match provider {
            "anthropic" => mock.reply("http://anthropic.test", mock::sse(&stream)),
            _ => mock.reply(
                "http://openai.test",
                mock::sse(&[": ping\n\n", chunk, "data: [DONE]\n\n"]),
            ),
        };
        let (_, _, out) = read_whole(serve(&gw, post("/v1/messages", body)).await.0).await;
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
    let (resp, _) = serve(&gw, req).await;
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

// What the format sets is its own. A relay's call carries
// the client's head as the adapter handed it over, `Host` included, which a
// transport strips ([`respan_gateway_http::proxy_call`]); this one does not.
#[tokio::test]
async fn r13_the_format_sets_what_the_kernel_never_passes() {
    struct Pass;
    impl Plugin for Pass {}

    let echo = Echo::default();
    let mut req = post("/msg", "a");
    req.headers_mut()
        .insert(HOST, HeaderValue::from_static("gateway.example"));
    let _ = read_whole(serve(&msg(&echo, Pass), req).await.0).await;
    let host = echo.seen().into_iter().find(|s| s.starts_with("host="));
    assert_eq!(host.as_deref(), Some("host=gateway.example"));
}

/// Whether a call was handed to its transport is the kernel's to say, on
/// any wire: not yet in `on_provider`; at its end, yes once handed over,
/// answered or not, and no for a call that failed before, as by a hook.
#[tokio::test]
async fn a_call_says_whether_it_was_handed_to_its_transport() {
    #[derive(Clone, Default)]
    struct Sent(Arc<Mutex<Vec<(bool, bool)>>>, bool);

    #[async_trait]
    impl Plugin for Sent {
        async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
            let before = px.is_sent();
            self.0.lock().map(|mut s| s.push((before, false))).ok();
            match self.1 {
                true => Err(Fault::bad_request("refused by a hook")),
                false => Ok(()),
            }
        }

        async fn on_provider_end(&self, px: &ProviderContext, _end: &Result<(), Fault>) {
            let sent = px.is_sent();
            self.0
                .lock()
                .map(|mut s| s.last_mut().map(|last| last.1 = sent))
                .ok();
        }
    }

    // (case, the provider answers, a hook fails the call, sent)
    let cases = [
        ("answered", true, false, true),
        ("transport failed", false, false, true),
        ("a hook failed", true, true, false),
    ];
    for (case, answers, hook_fails, sent) in cases {
        let mock = MockTransport::new();
        if answers {
            mock.reply("http://openai.test/v1", mock::json(StatusCode::OK, UNARY));
        }
        let plugin = Sent(Arc::default(), hook_fails);
        let gw = chat(&mock, vec![Arc::new(plugin.clone())]);
        let _ = read_whole(serve(&gw, post("/v1/chat/completions", CHAT)).await.0).await;
        gw.idle().await;
        let seen = plugin.0.lock().map(|s| s.clone()).unwrap_or_default();
        assert_eq!(seen, [(false, sent)], "{case}");
        assert_eq!(mock.sent().len(), usize::from(sent), "{case}");
    }
}

/// A call says whether the provider's response head arrived and its body
/// decoded no value without breaking off, as when a provider refuses: what
/// a fault alone cannot say, as a refusal and a cut-off response look alike.
#[tokio::test]
async fn a_call_says_whether_its_response_was_head_only() {
    #[derive(Clone, Default)]
    struct HeadOnly(Arc<Mutex<Vec<bool>>>);

    #[async_trait]
    impl Plugin for HeadOnly {
        async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
            assert!(!px.is_refused(), "not before a head");
            Ok(())
        }

        async fn on_provider_end(&self, px: &ProviderContext, _end: &Result<(), Fault>) {
            self.0.lock().map(|mut s| s.push(px.is_refused())).ok();
        }
    }

    let refused = mock::json(
        StatusCode::TOO_MANY_REQUESTS,
        r#"{"error":{"message":"x"}}"#,
    );
    let mut cut = mock::sse(&[]);
    cut.body_mut()
        .push(Err(Fault::upstream("connection reset")));
    // (case, what the provider does, head only at the end)
    let cases = [
        ("answered", Some(mock::json(StatusCode::OK, UNARY)), false),
        ("refused", Some(refused), true),
        ("cut before any value", Some(cut), false),
        ("transport failed", None, false),
    ];
    for (case, reply, head_only) in cases {
        let mock = MockTransport::new();
        if let Some(reply) = reply {
            mock.reply("http://openai.test/v1", reply);
        }
        let plugin = HeadOnly::default();
        let gw = chat(&mock, vec![Arc::new(plugin.clone())]);
        let _ = read_whole(serve(&gw, post("/v1/chat/completions", CHAT)).await.0).await;
        gw.idle().await;
        let seen = plugin.0.lock().map(|s| s.clone()).unwrap_or_default();
        assert_eq!(seen, [head_only], "{case}");
    }
}
