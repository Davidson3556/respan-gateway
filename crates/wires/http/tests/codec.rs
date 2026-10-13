//! Native HTTP formats bind protocol streams and small body encoders.

#[allow(dead_code)]
#[path = "../../../../tests/support/hooks.rs"]
mod hooks;
#[allow(dead_code)] // The shared modules also contain helpers used by other suites.
#[path = "../../../../tests/support/http.rs"]
mod http_support;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use futures::StreamExt;
use futures::TryStreamExt;
use hooks::Outcomes;
use http::HeaderMap;
use http::header::AUTHORIZATION;
use http::header::{HOST, UPGRADE};
use http::{HeaderValue, StatusCode};
use http_body_util::BodyExt;
use http_support::{LIMIT, post, read_whole, serve, serve_live, streamed};
use mockllm::{Answer, FakeOpenAi};
use respan_gateway_conversation::{Input, Output};
use respan_gateway_conversation::{Message, Role};
use respan_gateway_conversation_http::Schema;
use respan_gateway_conversation_http::anthropic::AnthropicMessages;
use respan_gateway_conversation_http::openai::OpenAiChat;
use respan_gateway_core::Provider;
use respan_gateway_core::futures;
use respan_gateway_core::{
    ClientContext, Connection, Fault, Format, Gateway, Plugin, ProviderContext, Response,
    async_trait,
};
use respan_gateway_http::bytes;
use respan_gateway_http::{Body, ClientIo, Encoder, ProviderIo};
use respan_gateway_http::{http, http_body_util};
use respan_gateway_http_transport::HttpTransport;

/// A message in: its number in its stream and its text.
#[derive(Debug, Clone, PartialEq)]
struct In(usize, String);

impl In {
    fn new(n: usize, text: impl Into<String>) -> Self {
        In(n, text.into())
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

impl Format<ClientIo, In, Out> for Msg {
    fn name(&self) -> &str {
        "msg"
    }
    fn matches(&self, io: &ClientIo) -> bool {
        io.request().uri().path().starts_with("/msg")
    }
    fn bind(&self, io: ClientIo) -> Result<Connection<In, Out>, Fault> {
        Ok(io.with_error(|_, _, _| Bytes::new()).bind(
            |_| Box::new(Numbering(0)),
            |reply| {
                let mut head = reply.head();
                if reply.request().headers().contains_key(UPGRADE) && head.status == StatusCode::OK
                {
                    head.status = StatusCode::SWITCHING_PROTOCOLS;
                }
                Ok(reply.write(head, Box::new(Answers)))
            },
        ))
    }
}
impl Format<ProviderIo, Out, In> for Msg {
    fn name(&self) -> &str {
        "msg"
    }
    fn bind(&self, io: ProviderIo) -> Result<Connection<Out, In>, Fault> {
        Ok(io.bind(
            |head, input: Option<&In>| {
                let path = match input {
                    Some(value) => format!("/msg/{}", value.1),
                    None => "/msg".to_owned(),
                };
                respan_gateway_http::path::set_path(head, &path)?;
                Ok(Box::new(Numbered(0)) as Box<dyn Encoder<In>>)
            },
            |_, _| {
                Some(Box::new(|raw: Bytes, out: &mut Vec<Out>| {
                    out.push(Out(String::from_utf8_lossy(&raw).into_owned()));
                    Ok(())
                }))
            },
        ))
    }
}

/// Numbers the messages it decodes.
struct Numbering(usize);

impl respan_gateway_http::Decoder<In> for Numbering {
    fn decode(&mut self, bytes: Bytes, out: &mut Vec<In>) -> Result<(), Fault> {
        let text = String::from_utf8_lossy(&bytes).into_owned();
        let kept = In::new;
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
}

/// Numbers the frames it writes, each as its text.
struct Numbered(usize);

impl Numbered {
    fn encode_into(&mut self, input: &In, out: &mut BytesMut) -> Result<(), Fault> {
        let text = &input.1;
        if text.contains("no") {
            return Err(Fault::bad_request("msg cannot say no"));
        }
        self.0 += 1;
        out.extend_from_slice(format!("{}|{text}", self.0).as_bytes());
        Ok(())
    }

    fn end_into(&mut self, _end: &Result<(), Fault>, _out: &mut BytesMut) -> Result<(), Fault> {
        Ok(())
    }
}

impl Encoder<In> for Numbered {
    fn encode(
        &mut self,
        value: &In,
        frames: &mut Vec<respan_gateway_http::bytes::Bytes>,
    ) -> Result<(), Fault> {
        let mut out = respan_gateway_http::bytes::BytesMut::new();
        self.encode_into(value, &mut out)?;
        frames.push(out.freeze());
        Ok(())
    }

    fn end(
        &mut self,
        end: Result<(), Fault>,
        frames: &mut Vec<respan_gateway_http::bytes::Bytes>,
    ) -> Result<(), Fault> {
        let mut out = respan_gateway_http::bytes::BytesMut::new();
        self.end_into(&end, &mut out)?;
        frames.extend((!out.is_empty()).then(|| out.freeze()));
        Ok(())
    }
}

/// Writes each answer as its text.
struct Answers;

impl Answers {
    fn encode_into(&mut self, answer: &Out, out: &mut BytesMut) -> Result<(), Fault> {
        out.extend_from_slice(answer.0.as_bytes());
        Ok(())
    }

    fn end_into(&mut self, _end: &Result<(), Fault>, _out: &mut BytesMut) -> Result<(), Fault> {
        Ok(())
    }
}

impl Encoder<Out> for Answers {
    fn encode(
        &mut self,
        value: &Out,
        frames: &mut Vec<respan_gateway_http::bytes::Bytes>,
    ) -> Result<(), Fault> {
        let mut out = respan_gateway_http::bytes::BytesMut::new();
        self.encode_into(value, &mut out)?;
        frames.push(out.freeze());
        Ok(())
    }

    fn end(
        &mut self,
        end: Result<(), Fault>,
        frames: &mut Vec<respan_gateway_http::bytes::Bytes>,
    ) -> Result<(), Fault> {
        let mut out = respan_gateway_http::bytes::BytesMut::new();
        self.end_into(&end, &mut out)?;
        frames.extend((!out.is_empty()).then(|| out.freeze()));
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
        call: http::Request<Body>,
    ) -> Result<
        (
            http::Response<Body>,
            Option<respan_gateway_http::Completion>,
        ),
        Fault,
    > {
        let (head, body) = call.into_parts();
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
        let mut resp = respan_gateway_http::http::Response::new(()).into_parts().0;
        if head.headers.contains_key(UPGRADE) {
            resp.status = StatusCode::SWITCHING_PROTOCOLS;
        }
        Ok((http::Response::from_parts(resp, Body::stream(echoed)), None))
    }
}
type SendFuture = futures::future::BoxFuture<
    'static,
    Result<
        (
            http::Response<Body>,
            Option<respan_gateway_http::Completion>,
        ),
        Fault,
    >,
>;

impl Echo {
    fn into_sender(self) -> impl Clone + Fn(http::Request<Body>) -> SendFuture {
        move |call| {
            let value = self.clone();
            Box::pin(async move { value.send(call).await })
        }
    }
}

/// A gateway serving msg, calling a msg provider over `echo`, with `plugin`.
fn msg(echo: &Echo, plugin: impl Plugin) -> Gateway {
    Gateway::new()
        .provider(Provider::new(
            "msg",
            Msg,
            respan_gateway_http::connect(echo.clone().into_sender(), LIMIT),
        ))
        .and_then(|g| Ok(g.client::<In, _, _>(Msg)?.target("msg")?.plugin(plugin)))
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
    let (resp, _) = serve_live(gw, req).await;
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

// Content that cannot be written is a bad request, by one rule for the first
// input and later frames alike, edited or not: the gateway cannot tell.
#[tokio::test]
async fn r8_content_that_cannot_be_written_is_a_bad_request_edited_or_not() {
    let cases = [
        ("yes", Some(("yes", "no")), "bad_request"),
        ("no", Some(("no", "no!")), "bad_request"),
        ("no", None, "bad_request"),
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
    for (edit, path) in [(None, "/msg/a"), (Some(("a", "b")), "/msg/b")] {
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
        .map(|g| g.plugin(log.clone()).plugin(Endless))
        .unwrap_or_else(|e| panic!("{e}"));
    let mut head = post("/msg", "");
    *head.method_mut() = http::Method::HEAD;

    let (status, _, body) = read_whole(serve(&gw, head).await.0).await;
    let idle = tokio::time::timeout(Duration::from_secs(5), gw.idle()).await;

    assert_eq!((status, body.ok()), (StatusCode::OK, Some(Bytes::new())));
    assert!(idle.is_ok(), "the gateway stays busy");
    assert!(log.only().is_ok());
}

const CHAT: &str = r#"{"model":"m","messages":[{"role":"user","content":"hi"}]}"#;
const UNARY: &str = r#"{"id":"c","choices":[{"message":{"content":"hi"}}]}"#;

/// One scripted response with exact frames, not a URL-based provider registry.
fn response(
    status: StatusCode,
    kind: &'static str,
    frames: Vec<Result<Bytes, Fault>>,
) -> http::Response<Body> {
    let mut head = respan_gateway_http::http::Response::new(()).into_parts().0;
    head.status = status;
    head.headers
        .insert(http::header::CONTENT_TYPE, HeaderValue::from_static(kind));
    http::Response::from_parts(head, Body::stream(futures::stream::iter(frames)))
}

fn scripted(
    status: StatusCode,
    kind: &'static str,
    frames: Vec<Result<Bytes, Fault>>,
) -> impl Clone + Fn(http::Request<Body>) -> SendFuture {
    let frames = Arc::new(frames);
    move |_call| {
        let frames = frames.as_ref().clone();
        Box::pin(async move { Ok((response(status, kind, frames), None)) })
    }
}

fn real(fake: &FakeOpenAi) -> impl Clone + Fn(http::Request<Body>) -> SendFuture + use<> {
    let mut headers = HeaderMap::new();
    headers.insert(AUTHORIZATION, HeaderValue::from_static("Bearer sk-test"));
    let client = HttpTransport::new(fake.base_url())
        .and_then(|http| http.with_headers(headers))
        .unwrap_or_else(|fault| panic!("{fault}"));
    move |request| {
        let client = client.clone();
        Box::pin(async move { client.send(request).await })
    }
}

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
/// provider over `transport`, with `plugins` in order.
fn chat(
    transport: impl Fn(http::Request<Body>) -> SendFuture + Send + Sync + 'static,
    plugins: Vec<Arc<dyn Plugin>>,
) -> Gateway {
    let gw = Gateway::new()
        .provider(Provider::new(
            "openai",
            OpenAiChat,
            respan_gateway_http::connect(transport, LIMIT),
        ))
        .and_then(|g| {
            g.client::<Input, _, _>(OpenAiChat)?
                .client::<Input, _, _>(AnthropicMessages)?
                .target("openai")
        });
    let gw = gw.map(|g| plugins.into_iter().fold(g, Gateway::plugin));
    gw.unwrap_or_else(|e| panic!("{e}"))
}

// A relay passes the client's end-to-end headers and query as a proxy does,
// minus conditions, ranges, overrides and encodings, and the transport strips
// who sent it and cookies. A translated call carries none.
#[tokio::test]
async fn r3_a_relay_passes_the_clients_headers_and_query_as_a_proxy_does() {
    let fake = FakeOpenAi::start().await;
    let gw = chat(real(&fake), Vec::new());
    fake.answer(Answer::raw(UNARY));
    let _ = read_whole(
        serve(&gw, with_headers("/v1/chat/completions", CHAT))
            .await
            .0,
    )
    .await;
    let messages = r#"{"model":"m","max_tokens":5,"messages":[{"role":"user","content":"hi"}]}"#;
    fake.answer(Answer::raw(UNARY));
    let _ = read_whole(serve(&gw, with_headers("/v1/messages", messages)).await.0).await;

    let sent = fake.requests();
    assert_eq!(sent.len(), 2);
    let (relayed, translated) = (&sent[0], &sent[1]);
    assert_eq!(relayed.query, "");
    assert_eq!(relayed.header("x-trace"), None);
    assert_eq!(relayed.header("user-agent"), None);
    for kept in [
        "forwarded",
        "x-forwarded-for",
        "if-none-match",
        "range",
        "cookie",
        "accept-encoding",
        "x-http-method-override",
    ] {
        assert!(relayed.header(kept).is_none(), "{kept}");
    }
    assert!(translated.query.is_empty());
    assert!(translated.header("x-trace").is_none());
}

// The binding strips the client's cookies and forwarding headers, and
// conditions, ranges, overrides and encodings, whatever the transport: one
// of its own, which strips nothing, gets none of them.
#[tokio::test]
async fn cookies_and_forwarding_headers_reach_no_transport() {
    let seen = Arc::new(Mutex::new(None));
    let transport = {
        let seen = seen.clone();
        move |call: http::Request<Body>| {
            let (head, _) = call.into_parts();
            *seen.lock().unwrap_or_else(|e| e.into_inner()) = Some(head.headers);
            Box::pin(async move {
                let mut resp = http::Response::new(Body::full(Bytes::from(UNARY)));
                resp.headers_mut()
                    .insert("content-type", HeaderValue::from_static("application/json"));
                Ok((resp, None))
            }) as SendFuture
        }
    };
    let gw = Gateway::new()
        .provider(Provider::new(
            "openai",
            OpenAiChat,
            respan_gateway_http::connect(transport, LIMIT),
        ))
        .and_then(|g| g.client::<Input, _, _>(OpenAiChat)?.target("openai"))
        .unwrap_or_else(|e| panic!("{e}"));
    let _ = read_whole(
        serve(&gw, with_headers("/v1/chat/completions", CHAT))
            .await
            .0,
    )
    .await;

    let sent = seen.lock().ok().and_then(|s| s.clone()).unwrap_or_default();
    for passed in ["forwarded", "x-forwarded-for", "cookie", "x-trace"] {
        assert!(sent.get(passed).is_none(), "{passed}");
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
            let mut prompt = Input::new([Message::new(Role::User, "judge")]);
            prompt.model = Some("gpt-4.1-mini".into());
            let verdict = cx.call("openai").with_input(prompt).await?;
            let read: Vec<Output> = verdict.outputs::<Output>().try_collect().await?;
            assert!(!read.is_empty());
            Ok(None)
        }
    }

    let fake = FakeOpenAi::start().await;
    let gw = chat(real(&fake), vec![Arc::new(Judge)]);
    for _ in 0..2 {
        fake.answer(Answer::raw(UNARY));
    }
    let _ = read_whole(
        serve(&gw, with_headers("/v1/chat/completions", CHAT))
            .await
            .0,
    )
    .await;

    let sent = fake.requests();
    assert_eq!(sent.len(), 2);
    assert_eq!(sent[0].body["messages"][0]["content"], "judge");
    assert!(sent[0].query.is_empty());
    assert!(sent[0].header("x-trace").is_none());
    assert_eq!(sent[1].raw, CHAT.as_bytes());
}

/// Anthropic Messages, noting what each reply its response encoder gets is:
/// an `own` event, in this format's words, or an ordinary `event`.
#[derive(Clone, Default)]
struct Noted(Arc<Mutex<Vec<&'static str>>>);

impl Format<ClientIo, Input, Output> for Noted {
    fn name(&self) -> &str {
        AnthropicMessages.name()
    }
    fn matches(&self, io: &ClientIo) -> bool {
        AnthropicMessages.matches(io)
    }
    fn bind(&self, io: ClientIo) -> Result<Connection<Input, Output>, Fault> {
        let seen = self.0.clone();
        Ok(io
            .with_error(|fault, request, head| AnthropicMessages.refusal(fault, request, head))
            .bind(
                |request| AnthropicMessages.read_request(request),
                move |reply| {
                    let mut head = reply.head();
                    let encoder = AnthropicMessages.response_encoder(&mut head, reply.request());
                    Ok(reply.write(head, Box::new(Noting(seen, encoder))))
                },
            ))
    }
}
impl Format<ProviderIo, Output, Input> for Noted {
    fn name(&self) -> &str {
        AnthropicMessages.name()
    }
    fn bind(&self, io: ProviderIo) -> Result<Connection<Output, Input>, Fault> {
        AnthropicMessages.bind(io)
    }
}

struct Noting(Arc<Mutex<Vec<&'static str>>>, Box<dyn Encoder<Output>>);

impl Encoder<Output> for Noting {
    fn encode(&mut self, reply: &Output, out: &mut Vec<Bytes>) -> Result<(), Fault> {
        let noted = match reply {
            Output::Native(respan_gateway_core::NativeValue { format, .. })
                if &**format == AnthropicMessages.name() =>
            {
                "own"
            }
            Output::Native(respan_gateway_core::NativeValue { .. }) => "other",
            _ => "event",
        };
        self.0.lock().map(|mut n| n.push(noted)).ok();
        self.1.encode(reply, out)
    }

    fn end(&mut self, end: Result<(), Fault>, out: &mut Vec<Bytes>) -> Result<(), Fault> {
        self.1.end(end, out)
    }
}

// An encoder gets the events of another format as ordinary events, and of
// its own format's as such: it writes the first anew and the second as they
// are, never another format's words.
#[tokio::test]
async fn r2_an_encoder_writes_its_own_formats_events_as_they_are_and_no_others() {
    let stream = [
        "event: ping\ndata: {\"type\": \"ping\"}\n\n",
        "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"m\",\"model\":\"c\"}}\n\n",
        "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n",
        "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"hi\"}}\n\n",
        "event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n",
    ];
    let chunk =
        "data: {\"id\":\"c\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi\"}}]}\n\n";
    let body =
        r#"{"model":"m","max_tokens":5,"stream":true,"messages":[{"role":"user","content":"hi"}]}"#;
    let (mut noted, mut bodies) = (Vec::new(), Vec::new());
    for provider in ["anthropic", "openai"] {
        let noting = Noted::default();
        let sse = |chunks: &[&str]| {
            scripted(
                StatusCode::OK,
                "text/event-stream",
                chunks
                    .iter()
                    .map(|chunk| Ok(Bytes::copy_from_slice(chunk.as_bytes())))
                    .collect(),
            )
        };
        let gw = Gateway::new()
            .provider(Provider::new(
                "anthropic",
                noting.clone(),
                respan_gateway_http::connect(sse(&stream), LIMIT),
            ))
            .and_then(|g| {
                g.provider(Provider::new(
                    "openai",
                    OpenAiChat,
                    respan_gateway_http::connect(sse(&[chunk, "data: [DONE]\n\n"]), LIMIT),
                ))
            })
            .and_then(|g| g.client::<Input, _, _>(noting.clone())?.target(provider))
            .unwrap_or_else(|e| panic!("{e}"));
        let (_, _, out) = read_whole(serve(&gw, post("/v1/messages", body)).await.0).await;
        bodies.push(String::from_utf8_lossy(&out.unwrap_or_default()).into_owned());
        noted.push(noting.0.lock().map(|n| n.clone()).unwrap_or_default());
    }
    // Its own format's keep-alive is an event of its own, written as it is.
    assert!(noted[0].contains(&"own"), "{:?}", noted[0]);
    assert!(
        bodies[0].contains("event: ping\ndata: {\"type\":\"ping\"}\n\n"),
        "{}",
        bodies[0]
    );
    // Another format's events are ordinary, and none of its words is written.
    assert!(!noted[1].is_empty() && noted[1].iter().all(|n| *n == "event"));
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
    let (resp, _) = serve_live(&gw, req).await;
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
// the client's head as the adapter handed it over, but `Host`, which the
// binding strips for every transport.
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
    assert_eq!(host.as_deref(), None);
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
        let sent_to_transport = Arc::new(AtomicUsize::new(0));
        let transport = {
            let sent_to_transport = sent_to_transport.clone();
            move |_call| {
                sent_to_transport.fetch_add(1, Ordering::SeqCst);
                Box::pin(async move {
                    if answers {
                        Ok((
                            response(
                                StatusCode::OK,
                                "application/json",
                                vec![Ok(Bytes::from(UNARY))],
                            ),
                            None,
                        ))
                    } else {
                        Err(Fault::upstream("transport failed after sending"))
                    }
                }) as SendFuture
            }
        };
        let plugin = Sent(Arc::default(), hook_fails);
        let gw = chat(transport, vec![Arc::new(plugin.clone())]);
        let _ = read_whole(serve(&gw, post("/v1/chat/completions", CHAT)).await.0).await;
        gw.idle().await;
        let seen = plugin.0.lock().map(|s| s.clone()).unwrap_or_default();
        assert_eq!(seen, [(false, sent)], "{case}");
        assert_eq!(
            sent_to_transport.load(Ordering::SeqCst),
            usize::from(sent),
            "{case}"
        );
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

    // (case, what the provider does, head only at the end)
    let cases = [
        (
            "answered",
            Some(scripted(
                StatusCode::OK,
                "application/json",
                vec![Ok(Bytes::from(UNARY))],
            )),
            false,
        ),
        (
            "refused",
            Some(scripted(
                StatusCode::TOO_MANY_REQUESTS,
                "application/json",
                vec![Ok(Bytes::from_static(br#"{"error":{"message":"x"}}"#))],
            )),
            true,
        ),
        (
            "cut before any value",
            Some(scripted(
                StatusCode::OK,
                "text/event-stream",
                vec![Err(Fault::upstream("connection reset"))],
            )),
            false,
        ),
        ("transport failed", None, false),
    ];
    for (case, reply, head_only) in cases {
        let transport = move |call| -> SendFuture {
            match &reply {
                Some(send) => send(call),
                None => Box::pin(async { Err(Fault::upstream("no response head")) }),
            }
        };
        let plugin = HeadOnly::default();
        let gw = chat(transport, vec![Arc::new(plugin.clone())]);
        let _ = read_whole(serve(&gw, post("/v1/chat/completions", CHAT)).await.0).await;
        gw.idle().await;
        let seen = plugin.0.lock().map(|s| s.clone()).unwrap_or_default();
        assert_eq!(seen, [head_only], "{case}");
    }
}
