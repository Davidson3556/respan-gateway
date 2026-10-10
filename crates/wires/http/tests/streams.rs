//! Requests whose inputs stream after the head, as a WebSocket's messages
//! do, built from a channel-fed body.

#[allow(dead_code)]
#[path = "../../../../tests/support/hooks.rs"]
mod hooks;
#[allow(dead_code, unused_imports)]
#[path = "../../../../tests/support/http.rs"]
mod http_support;
use hooks::Calls;
use http_support::{LIMIT, post, serve, serve_live, streamed};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use futures::{StreamExt, TryStreamExt};
#[cfg(feature = "reqwest")]
use http::header::AUTHORIZATION;
use http::header::{CONNECTION, CONTENT_ENCODING, UPGRADE};
use http::{HeaderValue, StatusCode};
use http_body::Frame;
use http_body_util::{BodyExt, StreamBody};
#[cfg(feature = "reqwest")]
use mockllm::{Answer, FakeOpenAi};
use respan_gateway_http::Body;
use respan_gateway_http::bytes;
use respan_gateway_http::formats::opaque;

use respan_gateway_conversation::{Delta, Input, Message, Output, Role};
use respan_gateway_conversation_http::openai::OpenAiChat;
use respan_gateway_core::futures;
use respan_gateway_core::{BadRequest, Upstream};
use respan_gateway_core::{
    ClientContext, EventSink, EventStream, Fault, Filter, Format, Gateway, Plugin, ProviderContext,
    Response, async_trait,
};
use respan_gateway_http::Encoder;
use respan_gateway_http::formats::set_path;
use respan_gateway_http::{ClientIo, Completion, ProviderIo, Wire};
use respan_gateway_http::{http, http_body, http_body_util};
use serde_json::{Value, json};
#[cfg(not(feature = "reqwest"))]
use std::collections::VecDeque;

/// A provider registration used only by this suite.
type AddProvider = Box<dyn FnOnce(Gateway) -> Result<Gateway, Fault>>;

type WireResult = Result<(http::Response<Body>, Option<Completion>), Fault>;
type SendRequest = Arc<
    dyn Fn(http::Request<Body>) -> futures::future::BoxFuture<'static, WireResult> + Send + Sync,
>;

fn sender<F, Fut>(send: F) -> SendRequest
where
    F: Fn(http::Request<Body>) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = WireResult> + Send + 'static,
{
    use futures::FutureExt;
    Arc::new(move |request| send(request).boxed())
}

fn add_provider<I, O, F>(name: &str, format: F, send: SendRequest) -> AddProvider
where
    I: Clone + Send + Sync + 'static,
    O: Clone + Send + Sync + 'static,
    F: Format<ProviderIo, O, I>,
{
    let name = name.to_owned();
    Box::new(move |gw| {
        gw.provider(
            name,
            format,
            respan_gateway_http::connect(move |request| send(request), LIMIT),
        )
    })
}

fn providers(list: impl IntoIterator<Item = AddProvider>) -> Result<Gateway, Fault> {
    list.into_iter().try_fold(Gateway::new(), |gw, add| add(gw))
}

fn with_plugins(gw: Gateway, plugins: Vec<Arc<dyn Plugin>>) -> Gateway {
    plugins.into_iter().fold(gw, Gateway::plugin)
}

/// What a talk client says: one message, `{"say": ...}`, and the other
/// fields of the object.
#[derive(Clone, PartialEq)]
struct Say(String, serde_json::Map<String, Value>);

impl Say {
    fn new(text: impl Into<String>) -> Self {
        Say(text.into(), serde_json::Map::new())
    }
}

impl std::fmt::Debug for Say {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Say").field(&self.0).finish()
    }
}

/// What a talk client hears: one message, `{"heard": ...}`.
#[derive(Debug, Clone, PartialEq)]
struct Heard(String);

/// A protocol of JSON messages at `/talk`: one per data frame each way once
/// upgraded, else one request and one answer.
#[derive(Clone)]
struct Talk;

fn read_talk(request: http::Request<Body>, strict: bool) -> EventStream<Say> {
    respan_gateway_http::messages(request, move |_, raw| {
        let mut rest = match serde_json::from_slice::<Value>(&raw) {
            Ok(Value::Object(fields)) => fields,
            _ => serde_json::Map::new(),
        };
        let v = rest.remove("say");
        let text = match v.as_ref().and_then(Value::as_str) {
            _ if raw.is_empty() && !strict => String::new(),
            _ if raw.is_empty() => return Err(BadRequest::fault("an empty message is not talk")),
            Some(s) => s.to_owned(),
            None => return Err(BadRequest::fault("not talk")),
        };
        Ok(Say(text, rest))
    })
}

fn talk_reply(reply: respan_gateway_http::Reply) -> Result<EventSink<Heard>, Fault> {
    let mut head = reply.head();
    if reply.request().headers().contains_key(UPGRADE) && head.status == StatusCode::OK {
        head.status = StatusCode::SWITCHING_PROTOCOLS;
        head.headers
            .insert(UPGRADE, HeaderValue::from_static("websocket"));
    }
    Ok(reply.write(head, Box::new(Write)))
}

fn read_heard(response: http::Response<Body>, _: http::Request<Bytes>) -> EventStream<Heard> {
    respan_gateway_http::formats::parse(
        response.into_body(),
        Messages,
        Messages::decode,
        |decoder, end, out| {
            end?;
            decoder.end(out)
        },
    )
}

impl Format<ClientIo, Say, Heard> for Talk {
    fn name(&self) -> &str {
        "talk"
    }
    fn matches(&self, io: &ClientIo) -> bool {
        io.request().uri().path().ends_with("/talk")
    }
    fn bind(&self, io: ClientIo) -> Result<respan_gateway_core::Connection<Say, Heard>, Fault> {
        Ok(io.bind(
            |request| read_talk(request, false),
            talk_reply,
            |fault, _, _| json!({"error": fault.message()}).to_string().into(),
        ))
    }
}

impl Format<ProviderIo, Heard, Say> for Talk {
    fn name(&self) -> &str {
        "talk"
    }
    fn bind(&self, io: ProviderIo) -> Result<respan_gateway_core::Connection<Heard, Say>, Fault> {
        Ok(io.bind(talk_call, read_heard, |answer| {
            respan_gateway_http::provider_fault("talk", answer)
        }))
    }
}

/// A talk call to `/talk`, upgraded for live input.
fn talk_call(
    head: &mut http::request::Parts,
    say: Option<&Say>,
) -> Result<Box<dyn Encoder<Say>>, Fault> {
    set_path(head, "/talk")?;
    if say.is_none() {
        head.headers
            .insert(UPGRADE, HeaderValue::from_static("websocket"));
        head.headers
            .insert(CONNECTION, HeaderValue::from_static("upgrade"));
    }
    Ok(Box::new(Says))
}

/// Writes each talk message from its text and its other fields.
struct Says;

impl Encoder<Say> for Says {
    fn encode(&mut self, say: &Say, out: &mut BytesMut) -> Result<(), Fault> {
        let mut v = say.1.clone();
        v.insert("say".into(), say.0.clone().into());
        out.extend_from_slice(Value::Object(v).to_string().as_bytes());
        Ok(())
    }

    fn end(&mut self, _end: &Result<(), Fault>, _out: &mut BytesMut) -> Result<(), Fault> {
        Ok(())
    }
}

/// One message per data frame.
struct Messages;

impl Messages {
    fn decode(&mut self, bytes: Bytes, out: &mut Vec<Heard>) -> Result<(), Fault> {
        let heard = serde_json::from_slice::<Value>(&bytes).ok();
        match heard.as_ref().and_then(|v| v["heard"].as_str()) {
            Some(h) => out.push(Heard(h.into())),
            None => return Err(Upstream::fault("not talk")),
        }
        Ok(())
    }

    fn end(&mut self, _out: &mut Vec<Heard>) -> Result<(), Fault> {
        Ok(())
    }
}

struct Write;

impl Encoder<Heard> for Write {
    fn encode(&mut self, heard: &Heard, out: &mut BytesMut) -> Result<(), Fault> {
        out.extend_from_slice(json!({ "heard": heard.0 }).to_string().as_bytes());
        Ok(())
    }

    fn end(&mut self, end: &Result<(), Fault>, out: &mut BytesMut) -> Result<(), Fault> {
        if let Err(f) = end {
            out.extend_from_slice(json!({ "error": f.message() }).to_string().as_bytes());
        }
        Ok(())
    }
}

/// A talk provider that echoes each message it gets as it gets it, noting
/// its calls' heads and the frames they carried.
#[derive(Clone, Default)]
struct Echo {
    heads: Arc<Mutex<Vec<http::HeaderMap>>>,
    frames: Arc<Mutex<Vec<Bytes>>>,
}

impl Echo {
    fn calls(&self) -> usize {
        self.heads.lock().map(|h| h.len()).unwrap_or_default()
    }

    fn heads(&self) -> Vec<http::HeaderMap> {
        self.heads.lock().map(|h| h.clone()).unwrap_or_default()
    }

    fn frames(&self) -> Vec<String> {
        let frames = self.frames.lock().map(|f| f.clone()).unwrap_or_default();
        frames
            .iter()
            .map(|f| String::from_utf8_lossy(f).into_owned())
            .collect()
    }
}

impl Echo {
    pub async fn send(&self, call: http::Request<Body>) -> WireResult {
        let whole = call.body().full_bytes().is_some();
        let (head, body) = call.into_parts();
        self.heads
            .lock()
            .map(|mut h| h.push(head.headers.clone()))
            .ok();
        let seen = self.frames.clone();
        // A whole call is one frame; empty, it carries no data, as an
        // empty HTTP body has no data frame.
        let body =
            body.try_filter(move |frame| futures::future::ready(!whole || !frame.is_empty()));
        let echoed = body.map_ok(move |frame| {
            seen.lock().map(|mut s| s.push(frame.clone())).ok();
            let v: Value = serde_json::from_slice(&frame).unwrap_or_default();
            Bytes::from(json!({ "heard": v["say"] }).to_string())
        });
        let mut resp = http::Response::from_parts(
            respan_gateway_http::http::Response::new(()).into_parts().0,
            Body::stream(echoed),
        );
        if head.headers.contains_key(UPGRADE) {
            *resp.status_mut() = StatusCode::SWITCHING_PROTOCOLS;
            let websocket = HeaderValue::from_static("websocket");
            resp.headers_mut().insert(UPGRADE, websocket);
        }
        Ok((resp, None))
    }
}
impl Echo {
    fn sender(&self) -> SendRequest {
        let value = self.clone();
        sender(move |call| {
            let value = value.clone();
            async move { value.send(call).await }
        })
    }
}

/// A gateway serving talk, relayed to the echo, with an OpenAI provider,
/// recording every call.
fn talk(echo: &Echo, plugins: Vec<Arc<dyn Plugin>>) -> (Gateway, Calls) {
    // Most tests never call OpenAI; an unexpected call must fail explicitly.
    let unexpected = sender(|_| async { Err(Upstream::fault("unexpected OpenAI call")) });
    talk_with_openai(echo, unexpected, plugins)
}

fn talk_with_openai(
    echo: &Echo,
    openai: SendRequest,
    plugins: Vec<Arc<dyn Plugin>>,
) -> (Gateway, Calls) {
    let calls = Calls::default();
    let list = [
        add_provider("echo", Talk, echo.sender()),
        add_provider("openai", OpenAiChat::OPENAI, openai),
    ];
    let gw = providers(list).and_then(|g| g.client::<Say, _, _>(Talk));
    let gw = gw.map(|g| g.plugin(calls.clone()));
    let gw = gw.map(|g| with_plugins(g, plugins));
    let gw = gw.and_then(|g| g.target("echo"));
    (gw.unwrap_or_else(|e| panic!("{e}")), calls)
}

/// Two OpenAI replies, with exact in-memory frames when Reqwest is disabled;
/// with Reqwest enabled, exercise the real HTTP transport and pinned mockllm.
struct OpenAiFixture {
    transport: SendRequest,
    #[cfg(feature = "reqwest")]
    fake: FakeOpenAi,
    #[cfg(not(feature = "reqwest"))]
    sent: Arc<Mutex<Vec<Value>>>,
}

impl OpenAiFixture {
    async fn new(streamed: bool) -> Self {
        #[cfg(feature = "reqwest")]
        {
            let fake = FakeOpenAi::start().await;
            for text in ["one", "two"] {
                if streamed {
                    let chunk = json!({"id":"c","choices":[{"index":0,"delta":{"content":text}}]});
                    fake.answer(Answer::events(vec![
                        Value::String(format!("data: {chunk}")),
                        Value::String("data: [DONE]".into()),
                    ]));
                } else {
                    let reply = json!({"id":"c","choices":[{"message":{"content":text}}]});
                    fake.answer(Answer::raw(reply.to_string()));
                }
            }
            let mut headers = http::HeaderMap::new();
            headers.insert(AUTHORIZATION, HeaderValue::from_static("Bearer sk-test"));
            let client = respan_gateway_http::HttpTransport::new(fake.base_url())
                .and_then(|http| http.with_headers(headers))
                .unwrap_or_else(|fault| panic!("{fault}"));
            let transport = sender(move |request| {
                let client = client.clone();
                async move { client.send(request).await }
            });
            Self { transport, fake }
        }
        #[cfg(not(feature = "reqwest"))]
        {
            let sent = Arc::new(Mutex::new(Vec::new()));
            let answers: VecDeque<Vec<Bytes>> = ["one", "two"]
                .map(|text| {
                    if streamed {
                        let chunk =
                            json!({"id":"c","choices":[{"index":0,"delta":{"content":text}}]});
                        vec![
                            Bytes::from(format!("data: {chunk}\n\n")),
                            Bytes::from_static(b"data: [DONE]\n\n"),
                        ]
                    } else {
                        let reply = json!({"id":"c","choices":[{"message":{"content":text}}]});
                        vec![Bytes::from(reply.to_string())]
                    }
                })
                .into();
            let answers = Arc::new(Mutex::new(answers));
            let record = sent.clone();
            let transport = sender(move |call| {
                let (record, answers) = (record.clone(), answers.clone());
                async move {
                    let chunks: Vec<Bytes> = call.into_body().try_collect().await?;
                    let mut request = BytesMut::new();
                    for chunk in chunks {
                        request.extend_from_slice(&chunk);
                    }
                    let value = serde_json::from_slice(&request)
                        .map_err(|err| BadRequest::fault(format!("bad chat request: {err}")))?;
                    record.lock().unwrap_or_else(|p| p.into_inner()).push(value);
                    let frames = answers
                        .lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .pop_front()
                        .ok_or_else(|| Upstream::fault("unexpected chat call"))?;
                    let mut head = respan_gateway_http::http::Response::new(()).into_parts().0;
                    head.headers.insert(
                        http::header::CONTENT_TYPE,
                        HeaderValue::from_static(if streamed {
                            "text/event-stream"
                        } else {
                            "application/json"
                        }),
                    );
                    Ok((
                        http::Response::from_parts(
                            head,
                            Body::stream(futures::stream::iter(frames.into_iter().map(Ok))),
                        ),
                        None,
                    ))
                }
            });
            Self { transport, sent }
        }
    }

    fn requests(&self) -> Vec<Value> {
        #[cfg(feature = "reqwest")]
        {
            self.fake.requests().into_iter().map(|r| r.body).collect()
        }
        #[cfg(not(feature = "reqwest"))]
        {
            self.sent.lock().unwrap_or_else(|p| p.into_inner()).clone()
        }
    }
}

/// A framed talk request, and the sender of its messages.
fn upgrade() -> (
    futures::channel::mpsc::UnboundedSender<Bytes>,
    http::Request<Body>,
) {
    let (send, mut req) = streamed("/v1/talk");
    let websocket = HeaderValue::from_static("websocket");
    req.headers_mut().insert(UPGRADE, websocket);
    (send, req)
}

fn say(send: &futures::channel::mpsc::UnboundedSender<Bytes>, message: &'static str) {
    send.unbounded_send(Bytes::from(message))
        .unwrap_or_else(|e| panic!("{e}"));
}

/// The response's data frames, one message each.
async fn frames(resp: http::Response<Body>) -> Vec<String> {
    let mut body = resp.into_body();
    let mut frames = Vec::new();
    while let Some(frame) = body.frame().await {
        let frame = frame.unwrap_or_else(|f| panic!("aborted: {f}"));
        if let Ok(data) = frame.into_data() {
            frames.push(String::from_utf8_lossy(&data).into_owned());
        }
    }
    frames
}

#[tokio::test]
async fn a_101_head_goes_out_at_once() {
    let echo = Echo::default();
    let (gw, _) = talk(&echo, Vec::new());
    let (send, req) = upgrade();

    // Nothing is sent either way until the client hears the 101.
    let handled = tokio::time::timeout(Duration::from_secs(5), serve_live(&gw, req)).await;
    let (resp, _) = handled.unwrap_or_else(|_| panic!("the head waited for a first byte"));

    assert_eq!(resp.status(), StatusCode::SWITCHING_PROTOCOLS);
    assert_eq!(resp.headers()[UPGRADE], "websocket");
    say(&send, r#"{"say":"hi"}"#);
    drop(send);
    assert_eq!(frames(resp).await, [r#"{"heard":"hi"}"#]);
}

#[tokio::test]
async fn a_framed_requests_inputs_are_relayed_as_one_call_each_with_what_they_said() {
    let echo = Echo::default();
    let (gw, calls) = talk(&echo, Vec::new());
    let (send, req) = upgrade();

    let (resp, ended) = serve_live(&gw, req).await;
    say(&send, r#"{"say":"a"}"#);
    say(&send, r#"{ "say": "b" }"#);
    drop(send);
    let frames = frames(resp).await;
    gw.idle().await;

    assert_eq!(frames, [r#"{"heard":"a"}"#, r#"{"heard":"b"}"#]);
    // Each frame is written from its value: the client's spacing is not kept.
    assert_eq!(echo.frames(), [r#"{"say":"a"}"#, r#"{"say":"b"}"#]);
    assert_eq!((echo.calls(), calls.all().len()), (1, 1));
    assert!(ended.await.is_ok());
}

#[tokio::test]
async fn a_provider_edge_input_filter_redacts_streamed_inputs() {
    struct Redact;

    #[async_trait]
    impl Plugin for Redact {
        async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
            if !px.is_kind::<Say>() {
                return Ok(());
            }
            px.filter(|say: &mut Say| {
                say.0 = say.0.replace("secret", "***");
                Ok(())
            })
        }
    }

    let echo = Echo::default();
    let (gw, _) = talk(&echo, vec![Arc::new(Redact)]);
    let (send, req) = upgrade();

    let (resp, _) = serve_live(&gw, req).await;
    say(&send, r#"{"say":"my secret","id":7}"#);
    say(&send, r#"{ "say": "fine" }"#);
    drop(send);
    let frames = frames(resp).await;

    // A changed input is written with its other fields; the client's spacing
    // is not kept for the unchanged one.
    assert_eq!(
        echo.frames(),
        [r#"{"id":7,"say":"my ***"}"#, r#"{"say":"fine"}"#]
    );
    assert_eq!(frames, [r#"{"heard":"my ***"}"#, r#"{"heard":"fine"}"#]);
}

#[tokio::test]
async fn client_edge_input_filters_apply_the_first_added_first() {
    struct Append(&'static str);

    #[async_trait]
    impl Plugin for Append {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let tail = self.0;
            cx.filter(move |say: &mut Say| {
                say.0.push_str(tail);
                Ok(())
            })?;
            Ok(None)
        }
    }

    let echo = Echo::default();
    let (gw, _) = talk(&echo, vec![Arc::new(Append("1")), Arc::new(Append("2"))]);
    let (send, req) = upgrade();

    let (resp, _) = serve_live(&gw, req).await;
    say(&send, r#"{"say":"x"}"#);
    drop(send);
    let _ = frames(resp).await;

    assert_eq!(echo.frames(), [r#"{"say":"x12"}"#]);
}

#[tokio::test]
async fn an_input_filter_edits_the_input_that_came_with_the_head_at_once() {
    struct Redact;

    #[async_trait]
    impl Plugin for Redact {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            cx.filter(|say: &mut Say| {
                say.0 = say.0.replace("secret", "***");
                Ok(())
            })?;
            Ok(None)
        }
    }

    /// Notes the input as the plugins before it left it.
    #[derive(Clone, Default)]
    struct Seen(Arc<Mutex<Option<Say>>>);

    #[async_trait]
    impl Plugin for Seen {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let say = cx.input::<Say>()?.clone();
            self.0.lock().map(|mut s| *s = Some(say)).ok();
            Ok(None)
        }
    }

    let (echo, seen) = (Echo::default(), Seen::default());
    let (gw, _) = talk(&echo, vec![Arc::new(Redact), Arc::new(seen.clone())]);

    let (resp, _) = serve(&gw, post("/v1/talk", r#"{"say":"my secret","id":1}"#)).await;
    let frames = frames(resp).await;

    let seen = seen.0.lock().map(|s| s.clone()).ok().flatten();
    let mut kept = Say::new("my ***");
    kept.1.insert("id".into(), 1.into());
    assert_eq!(seen, Some(kept));
    assert_eq!(echo.frames(), [r#"{"id":1,"say":"my ***"}"#]);
    assert_eq!(frames, [r#"{"heard":"my ***"}"#]);
}

#[tokio::test]
async fn a_whole_request_carries_one_input_even_empty_and_an_upgrading_head_none() {
    /// Notes what `input` and, for a whole request, `take_inputs` give.
    #[derive(Clone, Default)]
    struct Quiet(Arc<Mutex<Vec<(String, usize)>>>);

    #[async_trait]
    impl Plugin for Quiet {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let input = match cx.input::<Say>() {
                Ok(say) => format!("{say:?}"),
                Err(f) => format!("{}: {}", respan_gateway_http::error_code(&f), f.message()),
            };
            let asked = cx.extensions().get::<Wire>().and_then(Wire::get);
            let count = match asked.is_some_and(|a| a.headers().contains_key(UPGRADE)) {
                // A framed request's inputs are left for the call.
                true => 0,
                false => cx.take_inputs::<Say>().count().await,
            };
            self.0.lock().map(|mut s| s.push((input, count))).ok();
            Ok(None)
        }
    }

    let (echo, quiet) = (Echo::default(), Quiet::default());
    let (gw, calls) = talk(&echo, vec![Arc::new(quiet.clone())]);

    // An empty whole body, and a framed request that closes at once.
    let (whole, whole_ended) = serve(&gw, post("/v1/talk", "")).await;
    let (send, req) = upgrade();
    drop(send);
    let (framed, framed_ended) = serve_live(&gw, req).await;

    assert_eq!(whole.status(), StatusCode::OK);
    assert_eq!(framed.status(), StatusCode::SWITCHING_PROTOCOLS);
    // The empty body's one empty input is written as a value, not as the
    // empty bytes it came in.
    assert_eq!(frames(whole).await, [r#"{"heard":""}"#]);
    assert!(frames(framed).await.is_empty());
    assert!(whole_ended.await.is_ok() && framed_ended.await.is_ok());
    gw.idle().await;
    let none = format!(
        "{}: a streaming request's inputs come after its head, to filters and take_inputs",
        "internal"
    );
    let noted = quiet.0.lock().map(|s| s.clone()).unwrap_or_default();
    assert_eq!(noted, [(r#"Say("")"#.to_owned(), 1), (none, 0)]);
    assert_eq!(echo.frames(), [r#"{"say":""}"#]);
    assert_eq!((echo.calls(), calls.all().len()), (2, 2));
}

#[tokio::test]
async fn each_output_of_a_framed_response_is_its_own_frame() {
    /// Says every output twice.
    struct Twice;

    impl Filter<Heard> for Twice {
        fn edit(&mut self, heard: Heard, out: &mut Vec<Heard>) -> Result<(), Fault> {
            out.push(heard.clone());
            out.push(heard);
            Ok(())
        }
    }

    struct Echoes;

    #[async_trait]
    impl Plugin for Echoes {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            cx.filter(Twice)?;
            Ok(None)
        }
    }

    let echo = Echo::default();
    let (gw, _) = talk(&echo, vec![Arc::new(Echoes)]);
    let (send, req) = upgrade();

    let (resp, _) = serve_live(&gw, req).await;
    say(&send, r#"{"say":"a"}"#);
    drop(send);

    assert_eq!(frames(resp).await, [r#"{"heard":"a"}"#, r#"{"heard":"a"}"#]);
}

#[tokio::test]
async fn a_bridge_calls_a_provider_of_another_kind_once_per_input_and_streams_back() {
    /// Asks the chat provider `0` about each message the client says.
    struct Bridge(&'static str);

    #[async_trait]
    impl Plugin for Bridge {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            if !cx.is_kind::<Say>() {
                return Ok(None);
            }
            let call = cx.call(self.0);
            let heard = cx.take_inputs::<Say>().and_then(move |say| {
                let mut chat = Input::new([Message::new(Role::User, say.0)]);
                chat.model = Some("gpt-x".into());
                let call = call.clone().with_input(chat);
                async move {
                    let replies = call.await?;
                    Ok(replies
                        .outputs::<Output>()
                        .try_filter_map(|reply| async move { Ok(heard(&reply)) }))
                }
            });
            Ok(Some(Response::stream(heard.try_flatten())))
        }
    }

    let echo = Echo::default();
    let fixture = OpenAiFixture::new(false).await;
    let (gw, calls) = talk_with_openai(
        &echo,
        fixture.transport.clone(),
        vec![Arc::new(Bridge("openai"))],
    );
    let (send, req) = upgrade();

    let (resp, ended) = serve_live(&gw, req).await;
    assert_eq!(resp.status(), StatusCode::SWITCHING_PROTOCOLS);
    say(&send, r#"{"say":"x"}"#);
    say(&send, r#"{"say":"y"}"#);
    drop(send);
    let frames = frames(resp).await;
    gw.idle().await;

    assert_eq!(frames, [r#"{"heard":"one"}"#, r#"{"heard":"two"}"#]);
    let sent = fixture.requests();
    let said: Vec<&Value> = sent.iter().map(|b| &b["messages"][0]["content"]).collect();
    assert_eq!(said, [&json!("x"), &json!("y")]);
    assert_eq!((echo.calls(), calls.all().len()), (0, 2));
    assert!(ended.await.is_ok());
}

/// The text of `reply`, if it has any, as heard.
fn heard(reply: &Output) -> Option<Heard> {
    match reply {
        Output::Delta {
            delta: Delta::Text(text),
            ..
        } if !text.is_empty() => Some(Heard(text.clone())),
        _ => None,
    }
}

/// A gateway serving talk with `plugins` and the talk providers `list`,
/// recording every call, with no target.
fn talk_to(list: Vec<AddProvider>, plugins: Vec<Arc<dyn Plugin>>) -> (Gateway, Calls) {
    let calls = Calls::default();
    let gw = providers(list).and_then(|g| g.client::<Say, _, _>(Talk));
    let gw = gw.map(|g| g.plugin(calls.clone()));
    let gw = gw.map(|g| with_plugins(g, plugins));
    (gw.unwrap_or_else(|e| panic!("{e}")), calls)
}

/// A talk provider that answers an empty 101 at once and reads the call's
/// body on its own, noting each frame.
#[derive(Clone, Default)]
struct Early(Arc<Mutex<Vec<String>>>);

impl Early {
    pub async fn send(&self, call: http::Request<Body>) -> WireResult {
        let mut body = call.into_body();
        let seen = self.0.clone();
        tokio::spawn(async move {
            while let Some(Ok(data)) = body.next().await {
                let s = String::from_utf8_lossy(&data).into_owned();
                seen.lock().map(|mut v| v.push(s)).ok();
            }
        });
        let empty = futures::stream::empty::<Result<Bytes, Fault>>();
        let mut resp = http::Response::from_parts(
            respan_gateway_http::http::Response::new(()).into_parts().0,
            Body::stream(empty),
        );
        *resp.status_mut() = StatusCode::SWITCHING_PROTOCOLS;
        let websocket = HeaderValue::from_static("websocket");
        resp.headers_mut().insert(UPGRADE, websocket);
        Ok((resp, None))
    }
}
impl Early {
    fn sender(&self) -> SendRequest {
        let value = self.clone();
        sender(move |call| {
            let value = value.clone();
            async move { value.send(call).await }
        })
    }
}

/// Notes each hook's name, and what its calls returned.
#[derive(Clone, Default)]
struct Notes(Arc<Mutex<Vec<String>>>);

impl Notes {
    fn note(&self, note: impl Into<String>) {
        self.0.lock().map(|mut n| n.push(note.into())).ok();
    }

    fn all(&self) -> Vec<String> {
        self.0.lock().map(|n| n.clone()).unwrap_or_default()
    }
}

#[tokio::test]
async fn inputs_are_cut_at_the_requests_end_and_a_calls_input_filters_end_with_it() {
    /// Redacts on the client edge; notes a provider edge input filter's end.
    #[derive(Clone, Default)]
    struct Redact(Notes);

    struct NoteEnd(Notes);

    impl Filter<Say> for NoteEnd {
        fn edit(&mut self, say: Say, out: &mut Vec<Say>) -> Result<(), Fault> {
            out.push(say);
            Ok(())
        }

        fn end(&mut self, _end: &Result<(), Fault>, _out: &mut Vec<Say>) -> Result<(), Fault> {
            self.0.note("input filter ended");
            Ok(())
        }
    }

    #[async_trait]
    impl Plugin for Redact {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            cx.filter(|say: &mut Say| {
                say.0 = say.0.replace("secret", "***");
                Ok(())
            })?;
            Ok(None)
        }

        async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
            px.filter(NoteEnd(self.0.clone()))
        }
    }

    let (early, redact) = (Early::default(), Redact::default());
    let list = vec![add_provider("early", Talk, early.sender())];
    let (gw, _) = talk_to(list, vec![Arc::new(redact.clone())]);
    let gw = gw.target("early").unwrap_or_else(|e| panic!("{e}"));
    let (send, req) = upgrade();

    let (resp, ended) = serve_live(&gw, req).await;
    assert!(frames(resp).await.is_empty());
    let _ = ended.await;
    gw.idle().await;
    // The request ended; what the client sends now goes nowhere.
    let _ = send.unbounded_send(Bytes::from_static(br#"{"say":"my secret"}"#));
    tokio::time::sleep(Duration::from_millis(50)).await;

    assert!(early.0.lock().map(|v| v.is_empty()).unwrap_or_default());
    assert_eq!(redact.0.all(), ["input filter ended"]);
}

/// Inputs end at a filter's failure, and every input filter ends with it,
/// not abandoned when the request ends.
#[tokio::test]
async fn an_input_filters_failure_ends_every_input_filter_with_it() {
    #[derive(Clone, Default)]
    struct Guard(Notes);

    struct NoteEnd(Notes, &'static str);

    impl Filter<Say> for NoteEnd {
        fn edit(&mut self, say: Say, out: &mut Vec<Say>) -> Result<(), Fault> {
            out.push(say);
            Ok(())
        }

        fn end(&mut self, end: &Result<(), Fault>, _out: &mut Vec<Say>) -> Result<(), Fault> {
            let code = end
                .as_ref()
                .err()
                .map_or("ok", respan_gateway_http::error_code);
            self.0.note(format!("{} {code}", self.1));
            Ok(())
        }
    }

    #[async_trait]
    impl Plugin for Guard {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            cx.filter(|say: &mut Say| match say.0.as_str() {
                "bad" => Err(BadRequest::fault("blocked")),
                _ => Ok(()),
            })?;
            cx.filter(NoteEnd(self.0.clone(), "client"))?;
            Ok(None)
        }

        async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
            px.filter(NoteEnd(self.0.clone(), "call"))
        }
    }

    let echo = Echo::default();
    let guard = Guard::default();
    let (gw, _) = talk(&echo, vec![Arc::new(guard.clone())]);
    let (send, req) = upgrade();

    let (resp, ended) = serve_live(&gw, req).await;
    say(&send, r#"{"say":"bad"}"#);
    let _ = BodyExt::collect(resp.into_body()).await;
    let _ = ended.await;
    gw.idle().await;

    assert_eq!(guard.0.all(), ["client bad_request", "call bad_request"]);
}

/// A panic while a call's input filter ends keeps the call's end, and its end
/// hooks still run.
#[tokio::test]
async fn a_panic_ending_a_call_keeps_its_end() {
    #[derive(Clone, Default)]
    struct Boom(Notes);

    struct PanicAtEnd;

    impl Filter<Say> for PanicAtEnd {
        fn edit(&mut self, say: Say, out: &mut Vec<Say>) -> Result<(), Fault> {
            out.push(say);
            Ok(())
        }

        fn end(&mut self, _end: &Result<(), Fault>, _out: &mut Vec<Say>) -> Result<(), Fault> {
            panic!("an input filter's end panics");
        }
    }

    #[async_trait]
    impl Plugin for Boom {
        async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
            px.filter(PanicAtEnd)
        }

        async fn on_provider_end(
            &self,
            _px: &ProviderContext,
            o: &Result<(), respan_gateway_core::Fault>,
        ) {
            let code = o
                .as_ref()
                .err()
                .map_or("ok", respan_gateway_http::error_code);
            self.0.note(code.to_owned());
        }
    }

    let boom = Boom::default();
    let list = vec![add_provider("early", Talk, Early::default().sender())];
    let (gw, _) = talk_to(list, vec![Arc::new(boom.clone())]);
    let gw = gw.target("early").unwrap_or_else(|e| panic!("{e}"));
    let (_send, req) = upgrade();

    let (resp, ended) = serve_live(&gw, req).await;
    let _ = frames(resp).await;
    let _ = ended.await;
    gw.idle().await;

    assert_eq!(boom.0.all(), ["ok"]);
}

#[tokio::test]
async fn a_call_pending_at_the_requests_end_ends_abandoned_before_it() {
    /// Calls a provider that never answers, from a task of its own.
    #[derive(Clone, Default)]
    struct Spawner(Notes);

    #[async_trait]
    impl Plugin for Spawner {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let (call, notes) = (cx.call("never"), self.0.clone());
            let (sent, pending) = futures::channel::oneshot::channel();
            tokio::spawn(async move {
                let call = call.with_extension(());
                let mut call = std::pin::pin!(call);
                // Polled once, so the call is sent and pending.
                let _ = futures::poll!(call.as_mut());
                let _ = sent.send(());
                let end = call
                    .await
                    .err()
                    .map(|f| respan_gateway_http::error_code(&f).to_owned());
                notes.note(format!("call {}", end.unwrap_or_default()));
            });
            let _ = pending.await;
            tokio::task::yield_now().await;
            Ok(Some(Response::stream(futures::stream::empty::<
                Result<Heard, Fault>,
            >())))
        }

        async fn on_client_end(
            &self,
            _cx: &ClientContext,
            _o: &Result<(), respan_gateway_core::Fault>,
        ) {
            self.0.note("on_client_end");
        }

        async fn on_provider_end(
            &self,
            _px: &ProviderContext,
            o: &Result<(), respan_gateway_core::Fault>,
        ) {
            let end = o
                .as_ref()
                .err()
                .map(|f| respan_gateway_http::error_code(f).to_owned());
            self.0
                .note(format!("on_provider_end {}", end.unwrap_or_default()));
        }
    }

    let spawner = Spawner::default();
    let (gw, _) = talk_to(
        vec![add_provider(
            "never",
            Talk,
            sender(|call| async move {
                let _call = call;
                std::future::pending().await
            }),
        )],
        vec![Arc::new(spawner.clone())],
    );

    let (resp, ended) = serve(&gw, post("/v1/talk", r#"{"say":"x"}"#)).await;
    let _ = frames(resp).await;
    let _ = ended.await;
    tokio::time::timeout(Duration::from_secs(5), gw.idle())
        .await
        .unwrap_or_else(|_| panic!("the pending call outlived its request"));
    tokio::time::sleep(Duration::from_millis(20)).await;

    let notes = spawner.0.all();
    let hooks: Vec<&String> = notes.iter().filter(|n| n.starts_with("on_")).collect();
    assert_eq!(hooks, ["on_provider_end abandoned", "on_client_end"]);
    assert!(notes.contains(&"call abandoned".to_owned()), "{notes:?}");
}

#[tokio::test]
async fn a_retry_after_a_call_that_failed_unread_gets_every_input() {
    /// Calls `down`, then `echo` when that fails.
    struct Retry;

    #[async_trait]
    impl Plugin for Retry {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            match cx.call("down").await {
                Ok(resp) => Ok(Some(resp)),
                Err(_) => Ok(Some(cx.call("echo").await?)),
            }
        }
    }

    let echo = Echo::default();
    let list = vec![
        add_provider(
            "down",
            Talk,
            sender(|call| async move {
                let _call = call;
                Err(Upstream::fault("unreachable"))
            }),
        ),
        add_provider("echo", Talk, echo.sender()),
    ];
    let (gw, _) = talk_to(list, vec![Arc::new(Retry)]);
    let (send, req) = upgrade();

    let (resp, _) = serve_live(&gw, req).await;
    say(&send, r#"{"say":"a"}"#);
    say(&send, r#"{"say":"b"}"#);
    drop(send);

    assert_eq!(frames(resp).await, [r#"{"heard":"a"}"#, r#"{"heard":"b"}"#]);
    assert_eq!(echo.frames(), [r#"{"say":"a"}"#, r#"{"say":"b"}"#]);
}

#[tokio::test]
async fn inputs_are_taken_only_by_a_reader_that_is_polled() {
    /// Makes a reader of the inputs and drops it unpolled.
    struct Peek;

    #[async_trait]
    impl Plugin for Peek {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            drop(cx.take_inputs::<Say>());
            Ok(None)
        }
    }

    let echo = Echo::default();
    let (gw, _) = talk(&echo, vec![Arc::new(Peek)]);
    let (send, req) = upgrade();

    let (resp, _) = serve_live(&gw, req).await;
    say(&send, r#"{"say":"a"}"#);
    drop(send);

    assert_eq!(frames(resp).await, [r#"{"heard":"a"}"#]);
}

#[tokio::test]
async fn a_101_is_read_until_content_at_once() {
    /// Answers with the target's response once read to its content.
    struct UntilContent;

    #[async_trait]
    impl Plugin for UntilContent {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let mut response = cx.call("echo").await?;
            response.buffer_until(|_: &Heard| true).await?;
            Ok(Some(response))
        }
    }

    let echo = Echo::default();
    let (gw, _) = talk(&echo, vec![Arc::new(UntilContent)]);
    let (send, req) = upgrade();

    // The echo says nothing until the client does, which waits for the 101.
    let handled = tokio::time::timeout(Duration::from_secs(5), serve_live(&gw, req)).await;
    let (resp, _) = handled.unwrap_or_else(|_| panic!("until_content waited for a 101's content"));
    say(&send, r#"{"say":"a"}"#);
    drop(send);

    assert_eq!(resp.status(), StatusCode::SWITCHING_PROTOCOLS);
    assert_eq!(frames(resp).await, [r#"{"heard":"a"}"#]);
}

#[tokio::test]
async fn a_101_and_an_upgrading_call_carry_connection_upgrade() {
    let echo = Echo::default();
    let (gw, _) = talk(&echo, Vec::new());
    let (send, req) = upgrade();

    let (resp, _) = serve_live(&gw, req).await;
    drop(send);

    assert_eq!(resp.headers()[CONNECTION], "upgrade");
    assert!(frames(resp).await.is_empty());
    let heads = echo.heads();
    assert_eq!(heads.len(), 1);
    assert_eq!(heads[0][UPGRADE], "websocket");
    assert_eq!(heads[0][CONNECTION], "upgrade");
}

#[tokio::test]
async fn a_streamed_call_goes_without_the_clients_content_encoding() {
    let echo = Echo::default();
    let (gw, _) = talk(&echo, Vec::new());
    let (send, mut req) = upgrade();
    let gzip = HeaderValue::from_static("gzip");
    req.headers_mut().insert(CONTENT_ENCODING, gzip.clone());

    let (resp, _) = serve_live(&gw, req).await;
    drop(send);
    let _ = frames(resp).await;
    // A whole body relayed as it came keeps it.
    let mut whole = post("/v1/talk", r#"{"say":"a"}"#);
    whole.headers_mut().insert(CONTENT_ENCODING, gzip);
    let _ = frames(serve(&gw, whole).await.0).await;

    let heads = echo.heads();
    assert_eq!(heads.len(), 2);
    assert!(!heads[0].contains_key(CONTENT_ENCODING), "{:?}", heads[0]);
    assert!(!heads[1].contains_key(CONTENT_ENCODING));
}

/// Talk over plain HTTP: the same kind, whose calls never switch protocols.
#[derive(Clone)]
struct TalkHttp;

impl Format<ProviderIo, Heard, Say> for TalkHttp {
    fn name(&self) -> &str {
        "talk-http"
    }
    fn bind(&self, io: ProviderIo) -> Result<respan_gateway_core::Connection<Heard, Say>, Fault> {
        Ok(io.bind(
            |head, say: Option<&Say>| {
                say.ok_or_else(|| Fault::config("talk-http cannot stream a request"))?;
                set_path(head, "/talk")?;
                Ok(Box::new(Says) as Box<dyn Encoder<Say>>)
            },
            read_heard,
            |answer| respan_gateway_http::provider_fault("talk-http", answer),
        ))
    }
}

#[tokio::test]
async fn a_framed_requests_inputs_go_only_to_a_call_that_switches_protocols() {
    let echo = Echo::default();
    let list = vec![add_provider("http", TalkHttp, echo.sender())];
    let (gw, calls) = talk_to(list, Vec::new());
    let gw = gw.target("http").unwrap_or_else(|e| panic!("{e}"));
    let (send, req) = upgrade();

    let (resp, ended) = serve_live(&gw, req).await;
    drop(send);

    assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let _ = frames(resp).await;
    assert_eq!(
        ended
            .await
            .map_err(|f| respan_gateway_http::error_code(&f).to_owned()),
        Err("config".into())
    );
    gw.idle().await;
    assert_eq!((echo.calls(), calls.all().len()), (0, 1));
}

/// A format of no kind, a kind of one, that relays everything as it came,
/// its calls with the client's method and headers.
#[derive(Clone)]
struct Raw;

impl Format<ClientIo, opaque::Input<Raw>, opaque::Output<Raw>> for Raw {
    fn name(&self) -> &str {
        "raw"
    }
    fn matches(&self, _: &ClientIo) -> bool {
        true
    }
    fn bind(
        &self,
        io: ClientIo,
    ) -> Result<respan_gateway_core::Connection<opaque::Input<Raw>, opaque::Output<Raw>>, Fault>
    {
        Ok(io.bind(
            opaque::read_request,
            opaque::write_response,
            respan_gateway_http::plain,
        ))
    }
}
impl Format<ProviderIo, opaque::Output<Raw>, opaque::Input<Raw>> for Raw {
    fn name(&self) -> &str {
        "raw"
    }
    fn bind(
        &self,
        io: ProviderIo,
    ) -> Result<respan_gateway_core::Connection<opaque::Output<Raw>, opaque::Input<Raw>>, Fault>
    {
        Ok(io.bind(
            |head, input| {
                let encoder = opaque::write_request(head, input)?;
                set_path(head, "/")?;
                Ok(encoder)
            },
            opaque::read_response,
            |answer| respan_gateway_http::provider_fault("raw", answer),
        ))
    }
}

#[tokio::test]
async fn a_request_that_asks_to_switch_protocols_is_framed_in_any_format() {
    let echo = Echo::default();
    let gw = providers([add_provider("raw", Raw, echo.sender())])
        .and_then(|g| g.client::<opaque::Input<Raw>, _, _>(Raw)?.target("raw"))
        .unwrap_or_else(|e| panic!("{e}"))
        .plugin(explicit::Relay::RAW);
    let (send, req) = upgrade();

    // Raw says nothing of framing: the request's `Upgrade` does.
    let handled = tokio::time::timeout(Duration::from_secs(5), serve_live(&gw, req)).await;
    let (resp, _) = handled.unwrap_or_else(|_| panic!("the body was read whole"));
    say(&send, r#"{"say":"a"}"#);
    say(&send, "");
    drop(send);

    assert_eq!(resp.status(), StatusCode::SWITCHING_PROTOCOLS);
    let frames = frames(resp).await;
    // Every data frame is an input, an empty one too, and writes its frame.
    assert_eq!(echo.frames(), [r#"{"say":"a"}"#, ""]);
    assert_eq!(frames, [r#"{"heard":"a"}"#, r#"{"heard":null}"#]);
}

#[tokio::test]
async fn a_101_goes_only_to_a_request_that_asked() {
    /// Switches protocols whatever it is asked.
    #[derive(Clone)]
    struct Switches;

    impl Switches {
        pub async fn send(&self, _call: http::Request<Body>) -> WireResult {
            let mut resp = http::Response::from_parts(
                respan_gateway_http::http::Response::new(()).into_parts().0,
                Body::full(Bytes::new()),
            );
            *resp.status_mut() = StatusCode::SWITCHING_PROTOCOLS;
            Ok((resp, None))
        }
    }
    impl Switches {
        fn sender(&self) -> SendRequest {
            let value = self.clone();
            sender(move |call| {
                let value = value.clone();
                async move { value.send(call).await }
            })
        }
    }

    let (gw, _) = talk_to(
        vec![add_provider("switches", Talk, Switches.sender())],
        Vec::new(),
    );
    let gw = gw.target("switches").unwrap_or_else(|e| panic!("{e}"));

    let (resp, ended) = serve(&gw, post("/v1/talk", r#"{"say":"a"}"#)).await;

    assert_eq!(resp.status(), StatusCode::BAD_GATEWAY);
    let _ = frames(resp).await;
    assert_eq!(
        ended
            .await
            .map_err(|f| respan_gateway_http::error_code(&f).to_owned()),
        Err("upstream".into())
    );
}

#[tokio::test]
async fn a_clone_of_a_polled_call_keeps_its_settings() {
    struct Again;

    #[async_trait]
    impl Plugin for Again {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let mut call = cx.call("echo");
            drop((&mut call).await);
            // "A clone is an unsent copy": send it again.
            Ok(Some(call.clone().await?))
        }
    }

    let echo = Echo::default();
    let (gw, _) = talk(&echo, vec![Arc::new(Again)]);
    let (resp, ended) = serve(&gw, post("/v1/talk", r#"{"say":"x"}"#)).await;
    let status = resp.status();
    let _ = BodyExt::collect(resp.into_body()).await;
    let end = ended
        .await
        .err()
        .map(|f| format!("{} {}", respan_gateway_http::error_code(&f), f.message()));
    assert_eq!((status, end), (StatusCode::OK, None));
}

#[tokio::test]
async fn a_pending_call_left_unpolled_still_ends_with_its_request() {
    #[derive(Clone, Default)]
    struct Hold(Notes, Arc<Mutex<Vec<respan_gateway_core::Call>>>);

    #[async_trait]
    impl Plugin for Hold {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let mut call = cx.call("never");
            // Sent: its on_provider ran and it waits on the transport.
            let _ = futures::poll!(&mut call);
            for _ in 0..100 {
                if self.0.all().iter().any(|n| n == "on_provider") {
                    break;
                }
                tokio::task::yield_now().await;
            }
            self.1.lock().map(|mut v| v.push(call)).ok();
            Ok(Some(Response::stream(futures::stream::empty::<
                Result<Heard, Fault>,
            >())))
        }

        async fn on_provider(&self, _px: &mut ProviderContext) -> Result<(), Fault> {
            self.0.note("on_provider");
            Ok(())
        }

        async fn on_provider_end(
            &self,
            _px: &ProviderContext,
            o: &Result<(), respan_gateway_core::Fault>,
        ) {
            let end = o
                .as_ref()
                .err()
                .map(|f| respan_gateway_http::error_code(f).to_owned());
            self.0
                .note(format!("on_provider_end {}", end.unwrap_or_default()));
        }
    }

    let hold = Hold::default();
    let (gw, _) = talk_to(
        vec![add_provider(
            "never",
            Talk,
            sender(|call| async move {
                let _call = call;
                std::future::pending().await
            }),
        )],
        vec![Arc::new(hold.clone())],
    );
    let (resp, ended) = serve(&gw, post("/v1/talk", r#"{"say":"x"}"#)).await;
    let _ = frames(resp).await;
    let _ = ended.await;
    let idle = tokio::time::timeout(Duration::from_secs(2), gw.idle()).await;
    assert!(
        idle.is_ok(),
        "the request ended, but its pending call did not"
    );
    assert_eq!(hold.0.all(), ["on_provider", "on_provider_end abandoned"]);
}

#[tokio::test]
async fn a_call_kept_past_its_request_does_not_keep_the_clients_body() {
    use std::sync::atomic::{AtomicBool, Ordering};

    struct Flag(Arc<AtomicBool>);
    impl Drop for Flag {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    #[derive(Clone, Default)]
    struct Stash(Arc<Mutex<Vec<respan_gateway_core::Call>>>);

    #[async_trait]
    impl Plugin for Stash {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            self.0.lock().map(|mut v| v.push(cx.call("echo"))).ok();
            Ok(Some(Response::stream(futures::stream::empty::<
                Result<Heard, Fault>,
            >())))
        }
    }

    let (echo, stash) = (Echo::default(), Stash::default());
    let (gw, _) = talk(&echo, vec![Arc::new(stash.clone())]);
    let dropped = Arc::new(AtomicBool::new(false));
    let (_send, frames_in) = futures::channel::mpsc::unbounded::<Bytes>();
    let flag = Flag(dropped.clone());
    let body = frames_in.map(move |f| {
        let _ = &flag;
        Ok::<_, Fault>(Frame::data(f))
    });
    let mut req = http::Request::new(StreamBody::new(body).boxed_unsync());
    *req.uri_mut() = http::Uri::from_static("/v1/talk");
    req.headers_mut()
        .insert(UPGRADE, HeaderValue::from_static("websocket"));

    let (resp, ended) = serve_live(&gw, req).await;
    let _ = frames(resp).await;
    let _ = ended.await;
    gw.idle().await;
    // The request ended; its inputs were cut off, yet the client's
    // connection body lives on in the stashed call.
    assert!(
        dropped.load(Ordering::SeqCst),
        "the client's body outlived its request"
    );
}

#[tokio::test]
async fn a_retry_after_a_call_that_read_the_end_gets_a_clean_end() {
    /// Reads the call's body to its end, then fails.
    #[derive(Clone)]
    struct ReadThenFail;

    impl ReadThenFail {
        pub async fn send(&self, call: http::Request<Body>) -> WireResult {
            let _ = StreamExt::collect::<Vec<_>>(call.into_body()).await;
            Err(Upstream::fault("unreachable after reading"))
        }
    }
    impl ReadThenFail {
        fn sender(&self) -> SendRequest {
            let value = self.clone();
            sender(move |call| {
                let value = value.clone();
                async move { value.send(call).await }
            })
        }
    }

    struct Retry;

    #[async_trait]
    impl Plugin for Retry {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            match cx.call("flaky").await {
                Ok(resp) => Ok(Some(resp)),
                Err(_) => Ok(Some(cx.call("echo").await?)),
            }
        }
    }

    let echo = Echo::default();
    let list = vec![
        add_provider("flaky", Talk, ReadThenFail.sender()),
        add_provider("echo", Talk, echo.sender()),
    ];
    let (gw, _) = talk_to(list, vec![Arc::new(Retry)]);
    let (send, req) = upgrade();
    // The client says nothing and closes.
    drop(send);
    let (resp, ended) = serve_live(&gw, req).await;
    let mut body = resp.into_body();
    let mut got = Vec::new();
    while let Some(frame) = body.frame().await {
        match frame {
            Ok(f) => got.extend(
                f.into_data()
                    .ok()
                    .map(|d| String::from_utf8_lossy(&d).into_owned()),
            ),
            Err(f) => got.push(format!("aborted {f}")),
        }
    }
    let end = ended
        .await
        .err()
        .map(|f| respan_gateway_http::error_code(&f).to_owned());
    assert_eq!((got, end), (Vec::<String>::new(), None));
}

#[tokio::test]
async fn without_a_kept_call_the_clients_body_is_dropped() {
    use std::sync::atomic::{AtomicBool, Ordering};

    struct Flag(Arc<AtomicBool>);
    impl Drop for Flag {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    struct Own;

    #[async_trait]
    impl Plugin for Own {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            drop(cx.call("echo"));
            Ok(Some(Response::stream(futures::stream::empty::<
                Result<Heard, Fault>,
            >())))
        }
    }

    let echo = Echo::default();
    let (gw, _) = talk(&echo, vec![Arc::new(Own)]);
    let dropped = Arc::new(AtomicBool::new(false));
    let (_send, frames_in) = futures::channel::mpsc::unbounded::<Bytes>();
    let flag = Flag(dropped.clone());
    let body = frames_in.map(move |f| {
        let _ = &flag;
        Ok::<_, Fault>(Frame::data(f))
    });
    let mut req = http::Request::new(StreamBody::new(body).boxed_unsync());
    *req.uri_mut() = http::Uri::from_static("/v1/talk");
    req.headers_mut()
        .insert(UPGRADE, HeaderValue::from_static("websocket"));
    let (resp, ended) = serve_live(&gw, req).await;
    let _ = frames(resp).await;
    let _ = ended.await;
    gw.idle().await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(dropped.load(Ordering::SeqCst));
}

#[tokio::test]
async fn a_head_request_to_an_endless_stream_gets_its_head() {
    /// Opens with a first output, as a stream that waits on the client
    /// does, then never ends.
    struct Endless;

    #[async_trait]
    impl Plugin for Endless {
        async fn on_client(&self, _cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let opens = futures::stream::iter([Ok(Heard("ready".into()))]);
            let endless = opens.chain(futures::stream::pending::<Result<Heard, Fault>>());
            Ok(Some(Response::stream(endless)))
        }
    }

    let echo = Echo::default();
    let (gw, _) = talk(&echo, vec![Arc::new(Endless)]);
    let mut req = post("/v1/talk", r#"{"say":"x"}"#);
    *req.method_mut() = http::Method::HEAD;
    let handled = tokio::time::timeout(Duration::from_secs(2), serve(&gw, req)).await;
    assert!(
        handled.is_ok(),
        "a HEAD waited for the whole body it never sends"
    );
}

#[tokio::test]
async fn a_calls_empty_bodyless_response_ends_ok() {
    let calls = Calls::default();
    let empty = sender(|_call| async {
        let mut head = respan_gateway_http::http::Response::new(()).into_parts().0;
        head.status = StatusCode::NO_CONTENT;
        Ok((
            http::Response::from_parts(head, Body::full(Bytes::new())),
            None,
        ))
    });
    let gw = providers([add_provider("raw", Raw, empty)])
        .and_then(|g| {
            Ok(g.client::<opaque::Input<Raw>, _, _>(Raw)?
                .target("raw")?
                .plugin(calls.clone()))
        })
        .unwrap_or_else(|e| panic!("{e}"));
    let (resp, ended) = serve(&gw, post("/any", "x")).await;
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);
    // A host never polls a 204's body.
    drop(resp);
    assert!(ended.await.is_ok());
    gw.idle().await;
    // Its empty body was read to its end as the head went out.
    let all = calls.all();
    assert!(all.len() == 1 && all[0].is_ok());
}

// ---- r6 internals probes ----

/// The request's next call's `on_provider` does not wait for the end hooks
/// of a call that ended: here the
/// dropped call's `on_provider_end` waits for the next call's `on_provider`,
/// which the old rule deadlocked. The request's own end hooks still come
/// after its calls'.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn r6_the_next_calls_on_provider_does_not_wait_for_an_ended_calls_end_hooks() {
    for _ in 0..50 {
        let run = tokio::time::timeout(std::time::Duration::from_secs(5), dropped_then_retried());
        assert!(run.await.is_ok(), "on_provider waited for an end hook");
    }
}

async fn dropped_then_retried() {
    /// Notes each hook; says when an `on_provider` ran, and when `never2`'s
    /// did, which `never`'s end hook waits for.
    #[derive(Clone, Default)]
    struct Retry(Notes, Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>);

    #[async_trait]
    impl Plugin for Retry {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            // Each call is given up on once its `on_provider` ran.
            for provider in ["never", "never2"] {
                let call = cx.call(provider);
                tokio::select! {
                    _ = call => unreachable!("the provider never answers"),
                    () = self.1.notified() => {}
                }
            }
            Ok(Some(Response::stream(futures::stream::empty::<
                Result<Heard, Fault>,
            >())))
        }

        async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
            if px.provider() == "never2" {
                self.2.notify_one();
            }
            self.1.notify_one();
            Ok(())
        }

        async fn on_provider_end(
            &self,
            px: &ProviderContext,
            _o: &Result<(), respan_gateway_core::Fault>,
        ) {
            if px.provider() == "never" {
                self.2.notified().await;
            }
            self.0.note(format!("on_provider_end {}", px.provider()));
        }

        async fn on_client_end(
            &self,
            _cx: &ClientContext,
            _o: &Result<(), respan_gateway_core::Fault>,
        ) {
            self.0.note("on_client_end");
        }
    }

    let retry = Retry::default();
    let (gw, _) = talk_to(
        vec![
            add_provider(
                "never",
                Talk,
                sender(|call| async move {
                    let _call = call;
                    std::future::pending().await
                }),
            ),
            add_provider(
                "never2",
                Talk,
                sender(|call| async move {
                    let _call = call;
                    std::future::pending().await
                }),
            ),
        ],
        vec![Arc::new(retry.clone())],
    );
    let (resp, ended) = serve(&gw, post("/v1/talk", r#"{"say":"x"}"#)).await;
    let _ = frames(resp).await;
    let _ = ended.await;
    gw.idle().await;
    let mut notes = retry.0.all();
    assert_eq!(notes.pop().as_deref(), Some("on_client_end"));
    notes.sort();
    assert_eq!(notes, ["on_provider_end never", "on_provider_end never2"]);
}

tokio::task_local! {
    static TRACE: u32;
}

/// A call's `on_provider` runs in the request's task context, as
/// `on_client` does, so task-locals (and a tracing span entered around
/// `handle`) reach it.
#[tokio::test]
async fn r6_on_provider_sees_the_requests_task_locals() {
    #[derive(Clone, Default)]
    struct Seen(Notes);

    #[async_trait]
    impl Plugin for Seen {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            self.0
                .note(format!("on_client {}", TRACE.try_with(|t| *t).is_ok()));
            cx.call("down").await.map(Some)
        }

        async fn on_provider(&self, _px: &mut ProviderContext) -> Result<(), Fault> {
            self.0
                .note(format!("on_provider {}", TRACE.try_with(|t| *t).is_ok()));
            Ok(())
        }
    }

    let seen = Seen::default();
    let (gw, _) = talk_to(
        vec![add_provider(
            "down",
            Talk,
            sender(|call| async move {
                let _call = call;
                Err(Upstream::fault("unreachable"))
            }),
        )],
        vec![Arc::new(seen.clone())],
    );
    TRACE
        .scope(7, async {
            let (resp, _) = serve(&gw, post("/v1/talk", r#"{"say":"x"}"#)).await;
            let _ = frames(resp).await;
        })
        .await;
    gw.idle().await;
    assert_eq!(seen.0.all(), ["on_client true", "on_provider true"]);
}

/// Talk, but an empty message is not talk, as for any JSON format.
#[derive(Clone)]
struct Strict;

impl Format<ClientIo, Say, Heard> for Strict {
    fn name(&self) -> &str {
        "talk"
    }
    fn matches(&self, io: &ClientIo) -> bool {
        Talk.matches(io)
    }
    fn bind(&self, io: ClientIo) -> Result<respan_gateway_core::Connection<Say, Heard>, Fault> {
        Ok(io.bind(
            |request| read_talk(request, true),
            talk_reply,
            |fault, _, _| json!({"error": fault.message()}).to_string().into(),
        ))
    }
}
impl Format<ProviderIo, Heard, Say> for Strict {
    fn name(&self) -> &str {
        "talk"
    }
    fn bind(&self, io: ProviderIo) -> Result<respan_gateway_core::Connection<Heard, Say>, Fault> {
        Talk.bind(io)
    }
}

// An input a filter adds at the end is sent.
#[tokio::test]
async fn r8_4_an_input_a_filter_adds_at_the_end_is_sent() {
    struct SignOff;

    impl Filter<Say> for SignOff {
        fn edit(&mut self, v: Say, out: &mut Vec<Say>) -> Result<(), Fault> {
            out.push(v);
            Ok(())
        }
        fn end(&mut self, _e: &Result<(), Fault>, out: &mut Vec<Say>) -> Result<(), Fault> {
            out.push(Say::new("bye"));
            Ok(())
        }
    }

    struct P;

    #[async_trait]
    impl Plugin for P {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            cx.filter(SignOff)?;
            Ok(None)
        }
    }

    let echo = Echo::default();
    let gw = Gateway::new()
        .provider::<Say, _, _>(
            "echo",
            Strict,
            respan_gateway_http::connect(
                {
                    let echo = echo.clone();
                    move |request| {
                        let echo = echo.clone();
                        async move { echo.send(request).await }
                    }
                },
                LIMIT,
            ),
        )
        .and_then(|g| g.client::<Say, _, _>(Strict))
        .map(|g| g.plugin(P))
        .and_then(|g| g.target("echo"))
        .unwrap_or_else(|e| panic!("{e}"));
    let (send, req) = upgrade();
    let (resp, ended) = serve_live(&gw, req).await;
    say(&send, r#"{"say":"hi"}"#);
    drop(send);
    let frames = frames(resp).await;
    let end = ended.await;
    assert_eq!(
        echo.frames(),
        [r#"{"say":"hi"}"#, r#"{"say":"bye"}"#],
        "{frames:?} {end:?}"
    );
}

// A filter added once the inputs ended sees none and gets its end at once.
#[tokio::test]
async fn r8_an_input_filter_added_after_the_inputs_ended_ends_at_once() {
    struct Late(Arc<Mutex<Vec<String>>>);

    struct Ends(Arc<Mutex<Vec<String>>>);

    impl Filter<Say> for Ends {
        fn edit(&mut self, say: Say, out: &mut Vec<Say>) -> Result<(), Fault> {
            out.push(say);
            Ok(())
        }

        fn end(&mut self, end: &Result<(), Fault>, _out: &mut Vec<Say>) -> Result<(), Fault> {
            let noted = format!("ended {}", end.is_ok());
            self.0.lock().map(|mut n| n.push(noted)).ok();
            Ok(())
        }
    }

    #[async_trait]
    impl Plugin for Late {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let read = cx.take_inputs::<Say>().count().await;
            let added = cx.filter(Ends(self.0.clone()));
            let code = added
                .err()
                .map(|f| respan_gateway_http::error_code(&f).to_owned());
            self.0
                .lock()
                .map(|mut n| n.push(format!("{read} {code:?}")))
                .ok();
            Ok(Some(Response::stream(futures::stream::empty::<
                Result<Heard, Fault>,
            >())))
        }
    }

    let noted = Arc::new(Mutex::new(Vec::new()));
    let echo = Echo::default();
    let (gw, _) = talk(&echo, vec![Arc::new(Late(noted.clone()))]);
    let (send, req) = upgrade();
    say(&send, r#"{"say":"hi"}"#);
    drop(send);
    let (resp, _) = serve_live(&gw, req).await;
    drop(frames(resp).await);
    gw.idle().await;
    let noted = noted.lock().map(|n| n.clone()).unwrap_or_default();
    assert_eq!(noted, ["ended true", "1 None"]);
}

// --- Plugin 3: WS -> SSE bridge --------------------------------------------

/// Answers each WebSocket message with a streamed chat call, one frame per
/// text delta.
struct WsToSse(&'static str);

#[async_trait]
impl Plugin for WsToSse {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let asked = cx.extensions().get::<Wire>().and_then(Wire::get);
        if !cx.is_kind::<Say>() || !asked.is_some_and(|a| a.headers().contains_key(UPGRADE)) {
            return Ok(None);
        }
        let call = cx.call(self.0);
        let heard = cx
            .take_inputs::<Say>()
            .and_then(move |say| {
                let mut chat = Input::new([Message::new(Role::User, say.0)]);
                chat.model = Some("gpt-x".into());

                let reply = call
                    .clone()
                    .with_input(chat)
                    .with_extension(respan_gateway_conversation_http::StreamResponse(true));
                async move { Ok(reply.await?.outputs::<Output>()) }
            })
            .try_flatten()
            .try_filter_map(|reply| async move { Ok(heard(&reply)) });
        Ok(Some(Response::stream(heard)))
    }
}

#[tokio::test]
async fn r11_plugin_ws_to_sse_bridge() {
    let echo = Echo::default();
    let fixture = OpenAiFixture::new(true).await;
    let (gw, calls) = talk_with_openai(
        &echo,
        fixture.transport.clone(),
        vec![Arc::new(WsToSse("openai"))],
    );
    let (send, req) = upgrade();
    let (resp, ended) = serve_live(&gw, req).await;
    assert_eq!(resp.status(), StatusCode::SWITCHING_PROTOCOLS);
    say(&send, r#"{"say":"x"}"#);
    say(&send, r#"{"say":"y"}"#);
    drop(send);
    let frames = frames(resp).await;
    gw.idle().await;
    assert_eq!(frames, [r#"{"heard":"one"}"#, r#"{"heard":"two"}"#]);
    let sent = fixture.requests();
    assert_eq!(sent.len(), 2, "one chat call for each input");
    assert_eq!(
        sent.iter()
            .map(|r| &r["messages"][0]["content"])
            .collect::<Vec<_>>(),
        [&json!("x"), &json!("y")]
    );
    assert_eq!(calls.all().len(), 2);
    assert!(ended.await.is_ok());
}

use respan_gateway_http::relay as explicit;
