//! A user-defined kind with two formats, unknown to the kernel.

#[allow(dead_code)] // Calls is shared with other suites, but this suite needs Outcomes only.
#[path = "../../tests/support/hooks.rs"]
mod hooks;
#[allow(dead_code)] // The root helpers cover more request shapes than this suite uses.
#[path = "../../tests/support/http.rs"]
mod http_support;
#[allow(dead_code)] // Shared support also serves the events suite.
#[path = "formats_support/mod.rs"]
mod support;

use hooks::Outcomes;
use http_support::{post, read_whole, serve};
use mockllm::{Answer as FakeAnswer, FakeOpenAi};
use respan_gateway_http::bytes::{Bytes, BytesMut};
use respan_gateway_http::formats::opaque::{self, Got, Sent};
use respan_gateway_http::formats::whole::Relay;
use respan_gateway_http::formats::{self as formats, default_content_type, sse};
use respan_gateway_http::http::header::CONTENT_TYPE;
use respan_gateway_http::http::{self, HeaderMap, HeaderValue, StatusCode};
use respan_gateway_http::{
    Asked, Body, HttpClient, Incoming, Outgoing, Status, http::request::Parts as RequestHead,
    http::response::Parts as Head,
};
use respan_gateway_http::{Encoder, credentials, proxy_call, strip_client_origin_headers};
use respan_gateway_sdk::async_trait;
use respan_gateway_sdk::conversation::Conversation;
use respan_gateway_sdk::futures;
use respan_gateway_sdk::futures::{StreamExt, TryStreamExt};
use respan_gateway_sdk::{BadRequest, ErrorCode, Upstream};
use respan_gateway_sdk::{
    ClientContext, Connection, EventSink, EventStream, Fault, Filter, Format, Gateway, Plugin,
    Response,
};
use serde_json::{Value, json};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use support::{AddProvider, add_provider, providers, with_plugins};

/// A `POST` of `body` to `/{format}`, where that format's clients send.
fn to(format: &str, body: impl Into<Bytes>) -> http::Request<respan_gateway_http::Body> {
    post(&format!("/{format}"), body)
}

fn matches_post(io: &Incoming, path: &str) -> bool {
    let mut head = respan_gateway_http::http::Request::new(()).into_parts().0;
    head.method = io.request().method().clone();
    head.uri = io.request().uri().clone();
    formats::posts_to(&head, path)
}

/// A gateway serving `format`: `providers`, `plugins` in order, `target`.
fn serving<F, I, O>(
    format: F,
    list: impl IntoIterator<Item = AddProvider>,
    plugins: Vec<Arc<dyn Plugin>>,
    target: Option<&str>,
) -> Result<Gateway, Fault>
where
    F: Format<Incoming, I, O>,
    I: Clone + Send + Sync + 'static,
    O: Clone + Send + Sync + 'static,
{
    let gw = with_plugins(providers(list)?.client(format)?, plugins);
    match target {
        Some(t) => gw.target(t),
        None => Ok(gw),
    }
}

/// A call observed by the precise local provider closure.
#[derive(Clone)]
struct SentCall {
    uri: String,
    headers: HeaderMap,
    body: Bytes,
}

#[derive(Clone, Default)]
struct SentCalls(Arc<Mutex<Vec<SentCall>>>);

impl SentCalls {
    fn all(&self) -> Vec<SentCall> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

/// Only custom, malformed and byte-exact protocols use this transport. Each
/// test specifies its response directly; it has no URL registry or queue.
fn local(
    answer: impl Fn(usize, &RequestHead) -> http::Response<Body> + Send + Sync + 'static,
) -> (
    impl Clone + Fn(http::Request<Body>) -> SendFuture,
    SentCalls,
) {
    local_with_headers(HeaderMap::new(), answer)
}

fn local_with_headers(
    headers: HeaderMap,
    answer: impl Fn(usize, &RequestHead) -> http::Response<Body> + Send + Sync + 'static,
) -> (
    impl Clone + Fn(http::Request<Body>) -> SendFuture,
    SentCalls,
) {
    let sent = SentCalls::default();
    let calls = sent.clone();
    let index = Arc::new(AtomicUsize::new(0));
    let answer = Arc::new(answer);
    let transport = move |call: http::Request<Body>| {
        let (mut head, body) = call.into_parts();
        proxy_call(&mut head);
        strip_client_origin_headers(&mut head.headers);
        let mut call = http::Request::from_parts(head, body);
        credentials(&mut call, &headers);
        let (head, body) = call.into_parts();
        let calls = calls.clone();
        let response = answer(index.fetch_add(1, Ordering::Relaxed), &head);
        Box::pin(async move {
            let body = body.try_collect::<Vec<_>>().await?.concat();
            calls
                .0
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(SentCall {
                    uri: head.uri.to_string(),
                    headers: head.headers,
                    body: Bytes::from(body),
                });
            Ok((response, None))
        }) as SendFuture
    };
    (transport, sent)
}

fn response(
    status: StatusCode,
    mime: Option<&'static str>,
    frames: Vec<Result<Bytes, Fault>>,
) -> http::Response<Body> {
    let mut head = respan_gateway_http::http::Response::new(()).into_parts().0;
    head.status = status;
    if let Some(mime) = mime {
        head.headers
            .insert(CONTENT_TYPE, HeaderValue::from_static(mime));
    }
    http::Response::from_parts(head, Body::stream(futures::stream::iter(frames)))
}

fn whole(status: StatusCode, body: impl Into<Bytes>) -> http::Response<Body> {
    response(status, Some("application/json"), vec![Ok(body.into())])
}

fn stream(chunks: &[&str]) -> http::Response<Body> {
    response(
        StatusCode::OK,
        Some("text/event-stream"),
        chunks
            .iter()
            .map(|part| Ok(Bytes::copy_from_slice(part.as_bytes())))
            .collect(),
    )
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

fn unused() -> impl Clone + Fn(http::Request<Body>) -> SendFuture {
    |_| Box::pin(async { Err(Fault::internal("provider should not be called")) })
}

fn openai(fake: &FakeOpenAi) -> impl Clone + Fn(http::Request<Body>) -> SendFuture + use<> {
    let mut headers = HeaderMap::new();
    headers.insert("authorization", HeaderValue::from_static("Bearer sk-test"));
    let client = HttpClient::new(fake.base_url())
        .and_then(|client| client.with_headers(headers))
        .unwrap_or_else(|e| panic!("OpenAI transport: {e}"));
    move |request| {
        let client = client.clone();
        Box::pin(async move { client.send(request).await })
    }
}

/// The jev kind's request: a prompt in, an answer out.
#[derive(Clone, PartialEq)]
struct Jev {
    prompt: String,
    model: Option<String>,
    /// Fields of the body jev has no place for, by name, kept for the format
    /// that decoded them (`from`) alone.
    unknown: Vec<(String, Value)>,
    /// The format that decoded `unknown`.
    from: &'static str,
}

/// A jev response message: an answer, or, with no text, one that says
/// nothing, such as a keep-alive comment.
#[derive(Clone, PartialEq, Default)]
struct Answer {
    text: Option<String>,
}

impl Answer {
    fn new(text: impl Into<String>) -> Self {
        Answer {
            text: Some(text.into()),
        }
    }
}

/// Two formats of jev, which name the prompt and the answer differently:
/// v1 says `input`/`output`, v2 says `prompt`/`text`.
#[derive(Clone, Copy)]
struct JevFormat {
    name: &'static str,
    prompt: &'static str,
    answer: &'static str,
}

const V1: JevFormat = JevFormat {
    name: "jev-v1",
    prompt: "input",
    answer: "output",
};

const V2: JevFormat = JevFormat {
    name: "jev-v2",
    prompt: "prompt",
    answer: "text",
};

impl Format<Incoming, Jev, Answer> for JevFormat {
    fn name(&self) -> &str {
        self.name
    }
    fn matches(&self, io: &Incoming) -> bool {
        matches_post(io, &format!("/{}", self.name))
    }
    fn bind(&self, io: Incoming) -> Result<Connection<Jev, Answer>, Fault> {
        let jev = *self;
        Ok(io.bind(
            move |request| jev.inputs(request),
            move |reply, source| jev.outputs(reply, source),
            move |fault, _, head| jev.refusal(fault, head),
        ))
    }
}

impl Format<Outgoing, Answer, Jev> for JevFormat {
    fn name(&self) -> &str {
        self.name
    }
    fn bind(&self, io: Outgoing) -> Result<Connection<Answer, Jev>, Fault> {
        let jev = *self;
        Ok(io.bind(
            move |source| jev.prepare(source),
            move |response, _| jev.replies(response),
            respan_gateway_http::success,
        ))
    }
}

impl JevFormat {
    fn inputs(&self, request: http::Request<Body>) -> Result<EventStream<Jev>, Fault> {
        let jev = *self;
        respan_gateway_http::messages(request, move |_, raw| jev.read(&raw))
    }

    fn prepare(
        &self,
        source: &mut EventStream<Jev>,
    ) -> Result<(http::Request<Body>, EventSink<Jev>), Fault> {
        let mut head = http::Request::new(()).into_parts().0;
        head.method = http::Method::POST;
        formats::set_path(&mut head, "/complete");
        head.headers
            .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        let (request, sink) =
            respan_gateway_http::request(self.name, source, Box::new(Prompts(*self)))?;
        Ok((http::Request::from_parts(head, request.into_body()), sink))
    }

    fn replies(
        &self,
        response: http::Response<Body>,
    ) -> futures::stream::BoxStream<'static, Result<Answer, Fault>> {
        let (answer, name) = (self.answer, self.name);
        let decode = move |raw: Bytes, out: &mut Vec<Answer>| {
            if raw.starts_with(b":") {
                out.push(Answer::default());
                return Ok(());
            }
            let v = formats::json(&raw);
            if let Some(message) = v.as_ref().and_then(|v| v["error"]["message"].as_str()) {
                return Err(Upstream::fault(message).with(
                    respan_gateway_http::ProviderError::new(
                        name.to_owned(),
                        http::Response::new(raw),
                    ),
                ));
            }
            let text = v.as_ref().and_then(|v| v[answer].as_str());
            let text = text.ok_or_else(|| Upstream::fault("not a jev answer"))?;
            out.push(Answer::new(text));
            Ok(())
        };
        sse::or_whole(self.name, response, decode)
    }

    fn outputs(
        &self,
        reply: respan_gateway_http::Reply,
        source: &mut EventStream<Answer>,
    ) -> Result<EventSink<Answer>, Fault> {
        let mut head = reply.head();
        if respan_gateway_http::bodiless(reply.request().method(), head.status) {
            return Ok(reply.empty(head, source));
        }
        let encoder = self.response_body(&mut head);
        Ok(reply.write(head, encoder))
    }

    fn refusal(&self, fault: &Fault, head: &mut Head) -> Bytes {
        default_content_type(head, "application/json");
        match fault
            .get::<respan_gateway_http::RawReply>()
            .map(|r| r.0.body())
        {
            Some(raw) => raw.clone(),
            None => json!({ "error": fault.message() }).to_string().into(),
        }
    }
}

impl JevFormat {
    fn response_body(&self, head: &mut Head) -> Box<dyn Encoder<Answer>> {
        default_content_type(head, "application/json");
        // Answers and failures are written in jev: in a stream, as an event.
        let (key, stream) = (self.answer, sse::is_stream(head));
        let frame = move |v: Value| -> Bytes {
            match stream {
                true => sse::event(None, v).into(),
                false => v.to_string().into(),
            }
        };
        let write = move |answers: &[Answer]| {
            let texts: Vec<&str> = answers.iter().filter_map(|a| a.text.as_deref()).collect();
            if texts.is_empty() && answers.iter().all(|a| a.text.is_none()) {
                // No content is a keep-alive comment in a stream, else nothing.
                return Ok(match stream {
                    true => sse::comment("keep-alive").into(),
                    false => Bytes::new(),
                });
            }
            Ok(frame(json!({ key: texts.concat() })))
        };
        let fail =
            move |f: &Fault| match f.get::<respan_gateway_http::RawReply>().map(|r| r.0.body()) {
                Some(raw) => raw.clone(),
                None => frame(json!({ "error": f.message() })),
            };
        Box::new(Relay::new(write, fail, stream))
    }

    /// A body with no prompt is not jev at all; other fields are noted.
    fn read(&self, raw: &[u8]) -> Result<Jev, Fault> {
        let v: Option<Value> = serde_json::from_slice(raw).ok();
        let v = v.as_ref().and_then(Value::as_object);
        let prompt = v
            .and_then(|v| v.get(self.prompt)?.as_str())
            .ok_or_else(|| BadRequest::fault("no prompt"))?;
        let known = [self.prompt, "model"];
        let unknown = v.into_iter().flatten();
        Ok(Jev {
            prompt: prompt.to_owned(),
            model: v.and_then(|v| v.get("model")?.as_str()).map(Into::into),
            unknown: unknown
                .filter(|(k, _)| !known.contains(&k.as_str()))
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            from: self.name,
        })
    }

    /// A jev body: the prompt and model, and the unknown fields only for
    /// the format that decoded them, else no field may be lost.
    fn write(&self, jev: &Jev) -> Result<Value, Fault> {
        if let Some((name, _)) = jev.unknown.first()
            && jev.from != self.name
        {
            return Err(BadRequest::fault(format!("'{name}' cannot be translated")));
        }
        let mut v = serde_json::Map::from_iter(jev.unknown.iter().cloned());
        v.insert(self.prompt.into(), jev.prompt.clone().into());
        if let Some(model) = &jev.model {
            v.insert("model".into(), model.clone().into());
        }
        Ok(Value::Object(v))
    }
}

/// Writes jev requests from their values.
struct Prompts(JevFormat);

impl Encoder<Jev> for Prompts {
    fn encode(&mut self, jev: &Jev, out: &mut BytesMut) -> Result<(), Fault> {
        out.extend_from_slice(self.0.write(jev)?.to_string().as_bytes());
        Ok(())
    }

    fn end(&mut self, _end: &Result<(), Fault>, _out: &mut BytesMut) -> Result<(), Fault> {
        Ok(())
    }
}

/// Works on any jev format through the kind's types.
struct Polite;

#[async_trait]
impl Plugin for Polite {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        cx.filter(|jev: &mut Jev| {
            jev.prompt = format!("please, {}", jev.prompt);
            Ok(())
        })?;
        cx.filter(|a: &mut Answer| {
            if let Some(text) = &mut a.text {
                text.push_str(", thanks");
            }
            Ok(())
        })?;
        Ok(None)
    }
}

#[tokio::test]
async fn a_kind_the_kernel_has_never_seen_translates_between_its_formats() {
    let (transport, sent) = local(|_, _| whole(StatusCode::OK, r#"{"text":"hello"}"#));
    let gw = serving(
        V1,
        [add_provider("v2", V2, transport)],
        Vec::new(),
        Some("v2"),
    )
    .unwrap_or_else(|e| panic!("{e}"));

    let request = r#"{"input":"hi","model":"j1"}"#;
    let (_, _, body) = read_whole(serve(&gw, to("jev-v1", request)).await.0).await;

    let sent: Value = serde_json::from_slice(&sent.all()[0].body).unwrap_or_default();
    assert_eq!(sent, json!({ "prompt": "hi", "model": "j1" }));
    assert_eq!(body.ok().as_deref(), Some(&br#"{"output":"hello"}"#[..]));
}

#[tokio::test]
async fn a_field_jev_has_no_place_for_is_kept_or_refused() {
    let (v2, sent) = local(|_, _| whole(StatusCode::OK, r#"{"text":"hello"}"#));
    let gw = |plugins: Vec<Arc<dyn Plugin>>, target| {
        let list = [
            add_provider("v1", V1, unused()),
            add_provider("v2", V2, v2.clone()),
        ];
        serving(V2, list, plugins, Some(target)).unwrap_or_else(|e| panic!("{e}"))
    };
    let (same, other) = (gw(vec![Arc::new(Polite)], "v2"), gw(Vec::new(), "v1"));
    let request = r#"{"prompt":"hi","seed":7}"#;

    // Untranslated, the edited body keeps the field.
    let _ = read_whole(serve(&same, to("jev-v2", request)).await.0).await;
    let request_sent: Value = serde_json::from_slice(&sent.all()[0].body).unwrap_or_default();
    assert_eq!(request_sent, json!({ "prompt": "please, hi", "seed": 7 }));

    // Translation would lose it, so it is refused as a bad request.
    let (status, _, _) = read_whole(serve(&other, to("jev-v2", request)).await.0).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(sent.all().len(), 1);
}

#[tokio::test]
async fn a_plugin_works_on_every_format_of_its_kind() {
    let (transport, sent) = local(|_, _| whole(StatusCode::OK, r#"{"text":"hello"}"#));
    let gw = serving(
        V1,
        [add_provider("v2", V2, transport)],
        vec![Arc::new(Polite)],
        Some("v2"),
    )
    .unwrap_or_else(|e| panic!("{e}"));

    let (_, _, body) = read_whole(serve(&gw, to("jev-v1", r#"{"input":"hi"}"#)).await.0).await;

    let sent: Value = serde_json::from_slice(&sent.all()[0].body).unwrap_or_default();
    assert_eq!(sent, json!({ "prompt": "please, hi" }));
    assert_eq!(
        body.ok().as_deref(),
        Some(&br#"{"output":"hello, thanks"}"#[..])
    );
}

#[tokio::test]
async fn a_default_target_of_another_kind_is_not_used() {
    let (transport, sent) = local(|_, _| panic!("wrong kind reached provider"));
    let openai = add_provider("openai", respan_gateway_providers::OpenAiChat, transport);
    let gw = serving(V1, [openai], Vec::new(), Some("openai")).expect("builds");

    let (resp, ended) = serve(&gw, to("jev-v1", r#"{"input":"hi"}"#)).await;
    let (status, _, _) = read_whole(resp).await;

    // A target exists, so the setup, not the request, is at fault.
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    let end = ended
        .await
        .map_err(|f| respan_gateway_http::error_code(&f).into_owned());
    assert_eq!(end, Err("config".to_owned()));
    assert!(sent.all().is_empty());
}

// One default target, which a later `target` replaces; a
// request it cannot take fails CONFIG, and routing by format is a plugin.
#[tokio::test]
async fn a_later_default_target_replaces_the_first_and_a_plugin_routes_by_format() {
    /// Sends chat requests to OpenAI; the rest keep the default target.
    struct ByFormat;

    #[async_trait]
    impl Plugin for ByFormat {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            if cx.format() == respan_gateway_providers::OpenAiChat.name() {
                cx.set_target("openai");
            }
            Ok(None)
        }
    }

    let fake = FakeOpenAi::start().await;
    let (v2, sent) = local(|_, _| whole(StatusCode::OK, r#"{"text":"jev"}"#));
    let gw = |plugins: Vec<Arc<dyn Plugin>>| {
        let openai = add_provider(
            "openai",
            respan_gateway_providers::OpenAiChat,
            openai(&fake),
        );
        providers([openai, add_provider("v2", V2, v2.clone())])
            .and_then(|gw| {
                gw.client::<Jev, _, _>(V1)?
                    .client::<Conversation, _, _>(respan_gateway_providers::OpenAiChat)
            })
            .map(|gw| plugins.into_iter().fold(gw, Gateway::plugin))
            .and_then(|gw| gw.target("openai")?.target("v2"))
            .unwrap_or_else(|e| panic!("{e}"))
    };
    let chat = || {
        post(
            "/v1/chat/completions",
            r#"{"model":"gpt-4.1-mini","messages":[{"role":"user","content":"hi"}]}"#,
        )
    };

    // Without the plugin, a chat request goes to `v2`, which cannot take it.
    let (resp, ended) = serve(&gw(Vec::new()), chat()).await;
    assert_eq!(read_whole(resp).await.0, StatusCode::INTERNAL_SERVER_ERROR);
    let end = ended
        .await
        .map_err(|f| respan_gateway_http::error_code(&f).into_owned());
    assert_eq!(end, Err("config".to_owned()));
    assert!(sent.all().is_empty() && fake.requests().is_empty());

    let gw = gw(vec![Arc::new(ByFormat)]);
    fake.answer(FakeAnswer::raw(r#"{"id":"c","choices":[]}"#));
    let jev = read_whole(serve(&gw, to("jev-v1", r#"{"input":"hi"}"#)).await.0).await;
    let chat = read_whole(serve(&gw, chat()).await.0).await;

    assert_eq!((jev.0, chat.0), (StatusCode::OK, StatusCode::OK));
    assert_eq!(
        sent.all()
            .iter()
            .map(|r| r.uri.as_str())
            .collect::<Vec<_>>(),
        ["/complete"]
    );
    assert_eq!(
        fake.requests()
            .iter()
            .map(|r| r.path.as_str())
            .collect::<Vec<_>>(),
        ["/v1/chat/completions"]
    );
}

// A format of no kind is a kind of one, so serving it needs no provider.
#[tokio::test]
async fn a_format_of_no_kind_is_served_like_any_other() {
    assert!(
        providers([])
            .and_then(|gw| gw.client::<Sent<Anywhere>, _, _>(Anywhere))
            .is_ok()
    );
}

#[tokio::test]
async fn a_call_to_a_provider_of_another_kind_is_refused_when_made() {
    struct Stray;

    #[async_trait]
    impl Plugin for Stray {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            Ok(Some(cx.call("openai").await?))
        }
    }

    let (transport, sent) = local(|_, _| panic!("wrong kind reached provider"));
    let openai = add_provider("openai", respan_gateway_providers::OpenAiChat, transport);
    let gw = serving(V1, [openai], vec![Arc::new(Stray)], None).expect("builds");

    let (resp, ended) = serve(&gw, to("jev-v1", r#"{"input":"hi"}"#)).await;
    let (status, _, body) = read_whole(resp).await;

    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    let body = String::from_utf8(body.unwrap_or_default().to_vec()).unwrap_or_default();
    assert!(body.contains("cannot use"), "{body}");
    let end = ended
        .await
        .map_err(|f| respan_gateway_http::error_code(&f).into_owned());
    assert_eq!(end, Err("config".to_owned()));
    assert!(sent.all().is_empty(), "nothing left for the provider");
}

#[tokio::test]
async fn a_plugin_for_another_kind_is_refused_not_ignored() {
    struct Redact;

    #[async_trait]
    impl Plugin for Redact {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            cx.filter(|_: &mut respan_gateway_sdk::conversation::Reply| Ok(()))?;
            Ok(None)
        }
    }

    let gw = serving(
        V2,
        [add_provider("v2", V2, unused())],
        vec![Arc::new(Redact)],
        Some("v2"),
    )
    .unwrap_or_else(|e| panic!("{e}"));

    let (status, _, body) =
        read_whole(serve(&gw, to("jev-v2", r#"{"prompt":"hi"}"#)).await.0).await;

    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    let body = String::from_utf8_lossy(&body.unwrap_or_default()).into_owned();
    assert!(body.contains("another kind"), "{body}");
}

#[tokio::test]
async fn a_plugin_made_response_of_another_kind_fails_loudly() {
    struct Wrong;

    #[async_trait]
    impl Plugin for Wrong {
        async fn on_client(&self, _cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let wrong = [Ok(respan_gateway_sdk::conversation::Reply::delta(0, "hi"))];
            Ok(Some(Response::stream(futures::stream::iter(wrong))))
        }
    }

    let gw = serving(V2, [], vec![Arc::new(Wrong)], None).unwrap_or_else(|e| panic!("{e}"));

    let (_, _, body) = read_whole(serve(&gw, to("jev-v2", r#"{"prompt":"hi"}"#)).await.0).await;

    let body = String::from_utf8_lossy(&body.unwrap_or_default()).into_owned();
    assert!(body.contains("another kind"), "{body}");
}

#[tokio::test]
async fn a_plugin_asking_for_another_kind_is_told_so_before_decoding() {
    struct Chatty;

    #[async_trait]
    impl Plugin for Chatty {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            type Chat = respan_gateway_sdk::conversation::Conversation;
            // `is_kind` sets it apart from an undecodable body.
            assert!(!cx.is_kind::<Chat>());
            cx.input::<Chat>()?;
            Ok(None)
        }
    }

    let gw = serving(V2, [], vec![Arc::new(Chatty)], None).unwrap_or_else(|e| panic!("{e}"));

    // The plugin's kind, not the bad body, is the fault.
    let (status, _, body) = read_whole(serve(&gw, to("jev-v2", "not json")).await.0).await;

    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    let body = String::from_utf8_lossy(&body.unwrap_or_default()).into_owned();
    assert!(body.contains("another kind"), "{body}");
    // The Rust type name goes to logs only.
    assert!(!body.contains("respan_gateway"), "{body}");
}

#[tokio::test]
async fn a_request_a_plugin_only_looked_at_is_sent_with_what_it_said() {
    struct Look;

    #[async_trait]
    impl Plugin for Look {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            cx.filter(|jev: &mut Jev| {
                jev.prompt = jev.prompt.clone();
                Ok(())
            })?;
            Ok(None)
        }
    }

    let (transport, sent) = local(|_, _| whole(StatusCode::OK, r#"{"text":"hello"}"#));
    let gw = serving(
        V2,
        [add_provider("v2", V2, transport)],
        vec![Arc::new(Look)],
        Some("v2"),
    )
    .unwrap_or_else(|e| panic!("{e}"));
    // Spacing a re-encoding would not reproduce.
    let request = r#"{"prompt":"hi",  "seed":7}"#;

    let _ = read_whole(serve(&gw, to("jev-v2", request)).await.0).await;

    let sent: Value = serde_json::from_slice(&sent.all()[0].body).unwrap_or_default();
    assert_eq!(sent, json!({ "prompt": "hi", "seed": 7 }));
}

#[test]
fn formats_of_one_name_and_different_kinds_do_not_build() {
    // A jev format named like OpenAI's.
    let impostor = JevFormat {
        name: "openai-chat",
        ..V1
    };
    let err = serving(
        respan_gateway_providers::OpenAiChat,
        [add_provider("jev", impostor, unused())],
        Vec::new(),
        Some("jev"),
    )
    .err();
    assert_eq!(
        err.as_ref().map(Fault::message),
        Some("two formats named openai-chat are of different kinds")
    );
}

#[test]
fn a_kind_of_one_type_for_both_directions_does_not_build() {
    /// Jev whose kind names `Jev` as both its input and output.
    #[derive(Clone)]
    struct OneWay;

    impl Format<Incoming, Jev, Jev> for OneWay {
        fn name(&self) -> &str {
            "jev-one-way"
        }
        fn bind(&self, _: Incoming) -> Result<Connection<Jev, Jev>, Fault> {
            panic!("invalid kind must be rejected before binding")
        }
    }
    impl Format<Outgoing, Jev, Jev> for OneWay {
        fn name(&self) -> &str {
            "jev-one-way"
        }
        fn bind(&self, _: Outgoing) -> Result<Connection<Jev, Jev>, Fault> {
            panic!("invalid kind must be rejected before binding")
        }
    }

    let err = serving(
        OneWay,
        [add_provider("jev", OneWay, unused())],
        Vec::new(),
        Some("jev"),
    )
    .err();
    assert_eq!(
        err.as_ref().map(Fault::message),
        Some("the kind of format jev-one-way has one type for both directions")
    );
}

#[tokio::test]
async fn a_format_of_no_kind_relays_and_has_no_input_type() {
    /// A protocol the gateway only passes on.
    #[derive(Clone)]
    struct Opaque;

    impl Format<Incoming, Sent<Opaque>, Got<Opaque>> for Opaque {
        fn name(&self) -> &str {
            "opaque"
        }
        fn matches(&self, io: &Incoming) -> bool {
            io.request().method() == http::Method::POST && io.request().uri().path() == "/opaque"
        }
        fn bind(&self, io: Incoming) -> Result<Connection<Sent<Opaque>, Got<Opaque>>, Fault> {
            Ok(io.bind(
                opaque::read_request,
                |reply, source| Ok(opaque::write_response("opaque", reply, source)),
                respan_gateway_http::plain_refusal,
            ))
        }
    }
    impl Format<Outgoing, Got<Opaque>, Sent<Opaque>> for Opaque {
        fn name(&self) -> &str {
            "opaque"
        }
        fn bind(&self, io: Outgoing) -> Result<Connection<Got<Opaque>, Sent<Opaque>>, Fault> {
            Ok(io.bind(
                |source| {
                    let (mut request, sink) = opaque::write_request("opaque", source)?;
                    *request.method_mut() = http::Method::POST;
                    let (mut head, body) = request.into_parts();
                    formats::set_path(&mut head, "/");
                    Ok((http::Request::from_parts(head, body), sink))
                },
                |response, request| {
                    opaque::read_response("opaque", response, &request.into_parts().0)
                },
                respan_gateway_http::success,
            ))
        }
    }

    /// A guardrail that asks for the jev request.
    struct Guard;

    /// A rate limit, refusing every request.
    struct Limit;

    #[async_trait]
    impl Plugin for Limit {
        async fn on_client(&self, _cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            Err(Fault::new("slow down")
                .with(ErrorCode("limited".into()))
                .with(Status(429)))
        }
    }

    #[async_trait]
    impl Plugin for Guard {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            cx.input::<Jev>()?;
            Ok(None)
        }
    }

    let key = HeaderMap::from_iter([(
        http::HeaderName::from_static("x-key"),
        HeaderValue::from_static("provider"),
    )]);
    let (transport, sent) = local_with_headers(key, |index, _| match index {
        0 => response(
            StatusCode::OK,
            None,
            vec![Ok(Bytes::from("anything at all"))],
        ),
        1 => response(
            StatusCode::SERVICE_UNAVAILABLE,
            Some("application/json"),
            vec![Ok(Bytes::from("busy"))],
        ),
        _ => response(
            StatusCode::OK,
            None,
            vec![Ok(Bytes::from("half")), Err(Upstream::fault("cut"))],
        ),
    });
    let gw = |plugins: Vec<Arc<dyn Plugin>>| {
        let list = [add_provider("opaque", Opaque, transport.clone())];
        serving(Opaque, list, plugins, Some("opaque")).unwrap_or_else(|e| panic!("{e}"))
    };
    let (open, guarded) = (gw(Vec::new()), gw(vec![Arc::new(Guard)]));
    let limited = gw(vec![Arc::new(Limit)]);

    // Client headers reach the provider, save hop-by-hop, host, credential
    // and encoding ones, and the cookies the transport strips.
    let mut ping = to("opaque", "ping");
    let headers = ping.headers_mut();
    headers.insert("host", HeaderValue::from_static("gateway.example"));
    headers.insert("x-trace", HeaderValue::from_static("7"));
    headers.insert("te", HeaderValue::from_static("gzip, Trailers"));
    headers.insert("connection", HeaderValue::from_static("X-Hop, x-key"));
    headers.insert("x-hop", HeaderValue::from_static("1"));
    headers.insert("expect", HeaderValue::from_static("100-continue"));
    headers.insert("content-length", HeaderValue::from_static("4"));
    headers.insert("authorization", HeaderValue::from_static("Bearer gateway"));
    headers.insert("proxy-authorization", HeaderValue::from_static("Basic x"));
    headers.insert("cookie", HeaderValue::from_static("session=1"));
    headers.insert("accept-encoding", HeaderValue::from_static("gzip"));
    let (_, head, body) = read_whole(serve(&open, ping).await.0).await;
    assert_eq!(body.ok().as_deref(), Some(&b"anything at all"[..]));
    // The provider's head is unchanged; no content type is invented.
    assert!(head.get("content-type").is_none());
    let first = sent.all();
    let first = &first[0];
    assert_eq!(&first.body[..], b"ping");
    assert_eq!(first.headers["x-trace"], "7");
    assert!(first.headers.get("host").is_none());
    for gone in ["x-hop", "expect", "content-length"] {
        assert!(first.headers.get(gone).is_none(), "{gone}");
    }
    // `TE` goes like every connection header, as trailers
    // are not delivered.
    assert!(first.headers.get("te").is_none());
    // A header `Connection` names is dropped; the provider's own still goes.
    assert_eq!(first.headers["x-key"], "provider");
    for credential in [
        "authorization",
        "proxy-authorization",
        "cookie",
        "accept-encoding",
    ] {
        assert!(first.headers.get(credential).is_none(), "{credential}");
    }

    // A provider's failure is relayed as is and ends `Err`.
    let (resp, ended) = serve(&open, to("opaque", "ping")).await;
    let (status, _, body) = read_whole(resp).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        body.ok().as_deref(),
        Some(&b"the provider returned 503"[..])
    );
    let end = ended
        .await
        .map_err(|f| respan_gateway_http::status(&f).as_u16());
    assert_eq!(end, Err(StatusCode::SERVICE_UNAVAILABLE.as_u16()));

    // A body cut short upstream is cut short for the client.
    let (status, _, body) = read_whole(serve(&open, to("opaque", "ping")).await.0).await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.is_err(), "{body:?}");

    // A refusal says why, in plain text.
    let (status, headers, body) = read_whole(serve(&limited, to("opaque", "ping")).await.0).await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(headers["content-type"], "text/plain; charset=utf-8");
    assert_eq!(body.ok().as_deref(), Some(&b"slow down"[..]));

    // A plugin for jev that does not skip other traffic refuses it.
    let (status, _, _) = read_whole(serve(&guarded, to("opaque", "ping")).await.0).await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(sent.all().len(), 3, "the guarded request never left");
}

#[tokio::test]
async fn a_format_that_writes_errors_as_200_is_refused_once() {
    /// Jev over a protocol that puts errors in a 200, as JSON-RPC does.
    #[derive(Clone)]
    struct InBand;

    impl Format<Incoming, Jev, Answer> for InBand {
        fn name(&self) -> &str {
            V2.name
        }
        fn matches(&self, io: &Incoming) -> bool {
            matches_post(io, &format!("/{}", V2.name))
        }
        fn bind(&self, io: Incoming) -> Result<Connection<Jev, Answer>, Fault> {
            Ok(io.bind(
                move |request| V2.inputs(request),
                move |reply, source| V2.outputs(reply, source),
                |fault, _, head| {
                    let words = V2.refusal(fault, head);
                    head.status = StatusCode::OK;
                    words
                },
            ))
        }
    }
    impl Format<Outgoing, Answer, Jev> for InBand {
        fn name(&self) -> &str {
            V2.name
        }
        fn bind(&self, io: Outgoing) -> Result<Connection<Answer, Jev>, Fault> {
            Ok(io.bind(
                |source| V2.prepare(source),
                |response, _| V2.replies(response),
                respan_gateway_http::success,
            ))
        }
    }

    struct Deny;

    #[async_trait]
    impl Plugin for Deny {
        async fn on_client(&self, _cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            Err(Fault::new("no")
                .with(ErrorCode("denied".into()))
                .with(Status(403)))
        }
    }

    let gw = serving(InBand, [], vec![Arc::new(Deny)], None).unwrap_or_else(|e| panic!("{e}"));

    let (status, _, _) = read_whole(serve(&gw, to("jev-v2", r#"{"prompt":"hi"}"#)).await.0).await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn a_format_that_panics_building_its_encoder_is_a_plain_500() {
    #[derive(Clone)]
    struct Broken;

    impl Format<Incoming, Jev, Answer> for Broken {
        fn name(&self) -> &str {
            V2.name
        }
        fn matches(&self, io: &Incoming) -> bool {
            matches_post(io, &format!("/{}", V2.name))
        }
        fn bind(&self, io: Incoming) -> Result<Connection<Jev, Answer>, Fault> {
            Ok(io.bind(
                move |request| V2.inputs(request),
                |_, _| panic!("a buggy format"),
                |_, _, _| panic!("a buggy format"),
            ))
        }
    }
    impl Format<Outgoing, Answer, Jev> for Broken {
        fn name(&self) -> &str {
            V2.name
        }
        fn bind(&self, io: Outgoing) -> Result<Connection<Answer, Jev>, Fault> {
            Ok(io.bind(
                |source| V2.prepare(source),
                |response, _| V2.replies(response),
                respan_gateway_http::success,
            ))
        }
    }

    let log = Outcomes::default();
    let gw =
        serving(Broken, [], vec![Arc::new(log.clone())], None).unwrap_or_else(|e| panic!("{e}"));

    let (status, headers, _) =
        read_whole(serve(&gw, to("jev-v2", r#"{"prompt":"hi"}"#)).await.0).await;
    gw.idle().await;

    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(headers["content-type"], "text/plain; charset=utf-8");
    let end = log.only();
    assert!(matches!(end, Err(f) if respan_gateway_http::error_code(&f) == "internal"));
}

#[tokio::test]
async fn a_providers_error_body_is_relayed_once() {
    let refusal = r#"{"error":"no"}"#;
    let (transport, _) = local(|_, _| whole(StatusCode::TOO_MANY_REQUESTS, r#"{"error":"no"}"#));
    let gw = serving(
        V2,
        [add_provider("v2", V2, transport)],
        Vec::new(),
        Some("v2"),
    )
    .expect("builds");

    let (status, _, body) =
        read_whole(serve(&gw, to("jev-v2", r#"{"prompt":"hi"}"#)).await.0).await;

    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(body.ok().as_deref(), Some(refusal.as_bytes()));
}

#[tokio::test]
async fn a_jev_stream_is_written_and_edited_event_by_event() {
    /// Drops the answer `b`.
    struct Mute;

    #[async_trait]
    impl Plugin for Mute {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            struct NoB;
            impl Filter<Answer> for NoB {
                fn edit(&mut self, a: Answer, out: &mut Vec<Answer>) -> Result<(), Fault> {
                    if a.text.as_deref() != Some("b") {
                        out.push(a);
                    }
                    Ok(())
                }
            }
            cx.filter(NoB)?;
            Ok(None)
        }
    }

    let chunks = [
        "id: 1\ndata: {\"text\":\"a\"}\n\n",
        "data: {\"text\":\"b\"}\n\n",
    ];
    let (transport, _) = local(move |_, _| stream(&chunks));
    let gw = |plugins: Vec<Arc<dyn Plugin>>| {
        let list = [add_provider("v2", V2, transport.clone())];
        serving(V2, list, plugins, Some("v2"))
            .unwrap_or_else(|e| panic!("{e}"))
            .plugin(ChosenHeads)
    };
    let (plain, polite, drop) = (
        gw(Vec::new()),
        gw(vec![Arc::new(Polite)]),
        gw(vec![Arc::new(Mute)]),
    );

    let hi = || to("jev-v2", r#"{"prompt":"hi"}"#);
    let (_, _, plain) = read_whole(serve(&plain, hi()).await.0).await;
    let (_, _, polite) = read_whole(serve(&polite, hi()).await.0).await;
    let (_, _, dropped) = read_whole(serve(&drop, hi()).await.0).await;

    // Each event is written from its answer: the provider's `id:` field is
    // not carried.
    assert_eq!(
        dropped.ok().as_deref(),
        Some(&b"data: {\"text\":\"a\"}\n\n"[..])
    );

    let plain = String::from_utf8(plain.unwrap_or_default().to_vec()).unwrap_or_default();
    assert_eq!(
        plain,
        "data: {\"text\":\"a\"}\n\ndata: {\"text\":\"b\"}\n\n"
    );
    let polite = String::from_utf8(polite.unwrap_or_default().to_vec()).unwrap_or_default();
    assert_eq!(
        polite,
        "data: {\"text\":\"a, thanks\"}\n\ndata: {\"text\":\"b, thanks\"}\n\n"
    );
}

#[tokio::test]
async fn a_translated_jev_stream_and_a_whole_body_with_a_tail_are_one_document_each() {
    struct Tail;

    #[async_trait]
    impl Plugin for Tail {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            struct Sign;
            impl Filter<Answer> for Sign {
                fn edit(&mut self, a: Answer, out: &mut Vec<Answer>) -> Result<(), Fault> {
                    out.push(a);
                    Ok(())
                }
                fn end(
                    &mut self,
                    _end: &Result<(), Fault>,
                    out: &mut Vec<Answer>,
                ) -> Result<(), Fault> {
                    out.push(Answer::new("!"));
                    Ok(())
                }
            }
            cx.filter(Sign)?;
            Ok(None)
        }
    }

    let (transport, _) = local(|index, _| match index {
        0 => stream(&["data: {\"text\":\"a\"}\n\n", "data: {\"text\":\"b\"}\n\n"]),
        _ => whole(StatusCode::OK, r#"{"text":"hi"}"#),
    });
    let v2 = || [add_provider("v2", V2, transport.clone())];
    let v1 = serving(V1, v2(), Vec::new(), Some("v2")).unwrap_or_else(|e| panic!("{e}"));
    let tail = serving(V2, v2(), vec![Arc::new(Tail)], Some("v2"));
    let tail = tail.unwrap_or_else(|e| panic!("{e}"));

    let (_, _, v1) = read_whole(serve(&v1, to("jev-v1", r#"{"input":"hi"}"#)).await.0).await;
    let (_, _, tail) = read_whole(serve(&tail, to("jev-v2", r#"{"prompt":"hi"}"#)).await.0).await;

    assert_eq!(v1.ok().as_deref(), Some(&br#"{"output":"ab"}"#[..]));
    assert_eq!(tail.ok().as_deref(), Some(&br#"{"text":"hi!"}"#[..]));
}

#[tokio::test]
async fn a_redirect_is_passed_on_as_it_came_not_read_as_an_answer() {
    let (transport, _) = local(|_, _| {
        response(
            StatusCode::MOVED_PERMANENTLY,
            Some("text/html"),
            vec![Ok(Bytes::from("<html>moved</html>"))],
        )
    });
    let gw = serving(
        V2,
        [add_provider("v2", V2, transport)],
        vec![Arc::new(Polite)],
        Some("v2"),
    )
    .unwrap_or_else(|e| panic!("{e}"))
    .plugin(ChosenHeads);

    let (resp, ended) = serve(&gw, to("jev-v2", r#"{"prompt":"hi"}"#)).await;
    let (status, headers, body) = read_whole(resp).await;

    assert_eq!(status, StatusCode::MOVED_PERMANENTLY);
    assert_eq!(headers["content-type"], "text/html");
    assert_eq!(body.ok().as_deref(), Some(&b"<html>moved</html>"[..]));
    // The redirect is a fault, a 502 to any other format, that
    // carries the provider's answer, 301 and all.
    let end = ended.await.err();
    assert_eq!(
        end.as_ref()
            .map(|f| respan_gateway_http::status(f).as_u16()),
        Some(StatusCode::BAD_GATEWAY.as_u16())
    );
    let answer = end.as_ref().and_then(|f| {
        f.get::<respan_gateway_http::ProviderError>()
            .map(|e| &e.response)
    });
    assert_eq!(
        answer.map(|a| a.status()),
        Some(StatusCode::MOVED_PERMANENTLY)
    );
}

#[tokio::test]
async fn a_user_formats_failure_keeps_the_providers_words() {
    let error = r#"{"error":{"type":"invalid_request_error","message":"bad prompt"}}"#;
    let (transport, _) = local(|_, _| {
        whole(
            StatusCode::BAD_REQUEST,
            r#"{"error":{"type":"invalid_request_error","message":"bad prompt"}}"#,
        )
    });
    let gw = serving(
        V2,
        [add_provider("v2", V2, transport)],
        Vec::new(),
        Some("v2"),
    )
    .unwrap_or_else(|e| panic!("{e}"))
    .plugin(ChosenHeads);

    let (resp, ended) = serve(&gw, to("jev-v2", r#"{"prompt":"hi"}"#)).await;
    let (status, _, body) = read_whole(resp).await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body.ok().as_deref(), Some(error.as_bytes()));
    let end = ended.await;
    let end = end.map_err(|f| {
        (
            respan_gateway_http::error_code(&f).into_owned(),
            f.message().to_owned(),
        )
    });
    let said = ("invalid_request_error".to_owned(), "bad prompt".to_owned());
    assert_eq!(end, Err(said));
}

#[tokio::test]
async fn a_jev_stream_keeps_its_comments_and_says_a_providers_error() {
    let kept = [": keep-alive\n\n", "data: {\"text\":\"a\"}\n\n"];
    let failed = [
        "data: {\"text\":\"a\"}\n\n",
        "data: {\"error\":{\"message\":\"overloaded\"}}\n\n",
    ];
    let (transport, _) = local(move |index, _| match index {
        0 => stream(&kept),
        1 => stream(&failed),
        _ => whole(StatusCode::OK, r#"{"error":{"message":"overloaded"}}"#),
    });
    let gw = serving(
        V2,
        [add_provider("v2", V2, transport)],
        Vec::new(),
        Some("v2"),
    )
    .unwrap_or_else(|e| panic!("{e}"))
    .plugin(ChosenHeads);

    let mut ends = Vec::new();
    for _ in 0..3 {
        let (resp, ended) = serve(&gw, to("jev-v2", r#"{"prompt":"hi"}"#)).await;
        let (status, _, body) = read_whole(resp).await;
        let end = ended.await.map_err(|f| f.message().to_owned());
        ends.push((status, body.unwrap_or_default(), end));
    }

    // SSE comments pass through; a provider's error after content is
    // relayed as it came, and one before any content refused. r14 (D4): the
    // kernel strips nothing, so the refusal, in the provider's own format,
    // carries its error body as it came; translated, it is the client's words
    // (a_jev_error_inside_a_success_translated_is_refused_in_the_clients_words).
    let (ok, overloaded) = (StatusCode::OK, Err("overloaded".to_owned()));
    assert_eq!(ends[0], (ok, Bytes::from(kept.concat()), Ok(())));
    let failed = Bytes::from(failed.concat());
    assert_eq!(ends[1], (ok, failed, overloaded.clone()));
    let refused = Bytes::from_static(br#"{"error":{"message":"overloaded"}}"#);
    assert_eq!(ends[2], (StatusCode::BAD_GATEWAY, refused, overloaded));
}

#[tokio::test]
async fn a_jev_error_inside_a_success_translated_is_refused_in_the_clients_words() {
    let (transport, _) =
        local(|_, _| whole(StatusCode::OK, r#"{"error":{"message":"overloaded"}}"#));
    let gw = serving(
        V1,
        [add_provider("v2", V2, transport)],
        Vec::new(),
        Some("v2"),
    )
    .unwrap_or_else(|e| panic!("{e}"));

    let (status, _, body) = read_whole(serve(&gw, to("jev-v1", r#"{"input":"hi"}"#)).await.0).await;

    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert_eq!(
        body.ok().as_deref(),
        Some(&br#"{"error":"overloaded"}"#[..])
    );
}

#[tokio::test]
async fn the_input_is_read_from_the_head_as_it_arrived() {
    /// Jev whose prompt is in the `x-prompt` header, which it passes on.
    #[derive(Clone)]
    struct HeaderPrompt;

    impl Format<Incoming, Jev, Answer> for HeaderPrompt {
        fn name(&self) -> &str {
            "jev-headed"
        }
        fn matches(&self, io: &Incoming) -> bool {
            matches_post(io, &format!("/{}", "jev-headed"))
        }
        fn bind(&self, io: Incoming) -> Result<Connection<Jev, Answer>, Fault> {
            Ok(io.bind(
                |request| {
                    respan_gateway_http::messages(request, |head, _| {
                        let prompt = head
                            .headers
                            .get("x-prompt")
                            .and_then(|p| p.to_str().ok())
                            .ok_or_else(|| BadRequest::fault("no prompt"))?;
                        Ok(Jev {
                            prompt: prompt.to_owned(),
                            model: None,
                            unknown: Vec::new(),
                            from: "jev-headed",
                        })
                    })
                },
                |reply, source| {
                    let mut head = reply.head();
                    if respan_gateway_http::bodiless(reply.request().method(), head.status) {
                        return Ok(reply.empty(head, source));
                    }
                    let encoder = V2.response_body(&mut head);
                    Ok(reply.write(head, encoder))
                },
                |fault, _, head| V2.refusal(fault, head),
            ))
        }
    }
    impl Format<Outgoing, Answer, Jev> for HeaderPrompt {
        fn name(&self) -> &str {
            "jev-headed"
        }
        fn bind(&self, io: Outgoing) -> Result<Connection<Answer, Jev>, Fault> {
            Ok(io.bind(
                |source| V2.prepare(source),
                |response, _| V2.replies(response),
                respan_gateway_http::success,
            ))
        }
    }

    /// Reads the input.
    #[derive(Clone, Default)]
    struct Read(std::sync::Arc<std::sync::Mutex<String>>);

    #[async_trait]
    impl Plugin for Read {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let prompt = cx.input::<Jev>()?.prompt.clone();
            self.0.lock().map(|mut seen| *seen = prompt).ok();
            Ok(None)
        }
    }

    let read = Read::default();
    let (transport, sent) = local(|_, _| whole(StatusCode::OK, r#"{"text":"hello"}"#));
    let gw = serving(
        HeaderPrompt,
        [add_provider("h", HeaderPrompt, transport)],
        vec![Arc::new(read.clone())],
        Some("h"),
    )
    .unwrap_or_else(|e| panic!("{e}"));
    let mut req = to("jev-headed", r#"{"model":"j1"}"#);
    let arrived = HeaderValue::from_static("arrived");
    req.headers_mut().insert("x-prompt", arrived);

    assert_eq!(read_whole(serve(&gw, req).await.0).await.0, StatusCode::OK);

    let seen = read.0.lock().map(|s| s.clone()).unwrap_or_default();
    assert_eq!(seen, "arrived");
    // A relay passes the head and the body as they came.
    let sent = sent.all();
    assert!(!sent[0].headers.contains_key("x-prompt"));
    assert!(String::from_utf8_lossy(&sent[0].body).contains("arrived"));
}

/// A protocol of no kind that accepts any `POST`, relayed as it came.
#[derive(Clone)]
struct Anywhere;

impl Format<Incoming, Sent<Anywhere>, Got<Anywhere>> for Anywhere {
    fn name(&self) -> &str {
        "anywhere"
    }
    fn matches(&self, io: &Incoming) -> bool {
        io.request().method() == http::Method::POST
    }
    fn bind(&self, io: Incoming) -> Result<Connection<Sent<Anywhere>, Got<Anywhere>>, Fault> {
        Ok(io.bind(
            opaque::read_request,
            |reply, source| Ok(opaque::write_response("anywhere", reply, source)),
            respan_gateway_http::plain_refusal,
        ))
    }
}
impl Format<Outgoing, Got<Anywhere>, Sent<Anywhere>> for Anywhere {
    fn name(&self) -> &str {
        "anywhere"
    }
    fn bind(&self, io: Outgoing) -> Result<Connection<Got<Anywhere>, Sent<Anywhere>>, Fault> {
        Ok(io.bind(
            |source| {
                let (mut request, sink) = opaque::write_request("anywhere", source)?;
                *request.method_mut() = http::Method::POST;
                let (mut head, body) = request.into_parts();
                formats::set_path(&mut head, "/");
                Ok((http::Request::from_parts(head, body), sink))
            },
            |response, request| {
                opaque::read_response("anywhere", response, &request.into_parts().0)
            },
            respan_gateway_http::success,
        ))
    }
}

/// Notes the client's format, then refuses the request.
#[derive(Clone, Default)]
struct NoteFormat(Arc<std::sync::Mutex<Vec<String>>>);

#[async_trait]
impl Plugin for NoteFormat {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        self.0
            .lock()
            .map(|mut n| n.push(cx.format().to_owned()))
            .ok();
        Err(Fault::new("noted")
            .with(ErrorCode("noted".into()))
            .with(Status(403)))
    }
}

#[tokio::test]
async fn the_first_format_served_that_accepts_a_request_serves_it() {
    let serving_by = |gw: Gateway, anywhere_first: bool| match anywhere_first {
        true => gw
            .client::<Sent<Anywhere>, _, _>(Anywhere)?
            .client::<Jev, _, _>(V1),
        false => gw
            .client::<Jev, _, _>(V1)?
            .client::<Jev, _, _>(V2)?
            .client::<Sent<Anywhere>, _, _>(Anywhere),
    };
    let served = async |anywhere_first: bool| {
        let note = NoteFormat::default();
        let any = add_provider("any", Anywhere, unused());
        let gw = providers([any])
            .and_then(|gw| Ok(serving_by(gw, anywhere_first)?.plugin(note.clone())))
            .unwrap_or_else(|e| panic!("{e}"));
        let mut get = to("jev-v1", "{}");
        *get.method_mut() = http::Method::GET;
        let mut statuses = Vec::new();
        for req in [
            to("jev-v1", "{}"),
            to("jev-v2", "{}"),
            to("other", "{}"),
            get,
        ] {
            statuses.push(read_whole(serve(&gw, req).await.0).await.0.as_u16());
        }
        let noted = note.0.lock().map(|n| n.clone()).unwrap_or_default();
        (noted, statuses)
    };

    let (noted, statuses) = served(false).await;
    assert_eq!(noted, ["jev-v1", "jev-v2", "anywhere"]);
    assert_eq!(statuses, [403, 403, 403, 404]);
    let (noted, statuses) = served(true).await;
    assert_eq!(noted, ["anywhere", "anywhere", "anywhere"]);
    assert_eq!(statuses, [403, 403, 403, 404]);
}

#[tokio::test]
async fn a_router_overrides_the_default_target() {
    let (v1, sent_v1) = local(|_, _| whole(StatusCode::OK, r#"{"output":"hello"}"#));
    let (v2, sent_v2) = local(|_, _| whole(StatusCode::OK, r#"{"text":"hello"}"#));
    let list = [add_provider("v1", V1, v1), add_provider("v2", V2, v2)];
    let router = respan_gateway_plugins::ModelRouter::new(|j: &Jev| j.model.as_deref());
    let router = router.route("old", "v1");
    let gw = serving(V2, list, vec![Arc::new(router)], Some("v2")).expect("builds");

    for model in ["old-1", "new-1"] {
        let request = format!(r#"{{"prompt":"hi","model":"{model}"}}"#);
        let (status, _, body) = read_whole(serve(&gw, to("jev-v2", request)).await.0).await;
        assert_eq!(status, StatusCode::OK, "{model}");
        assert_eq!(body.ok().as_deref(), Some(&br#"{"text":"hello"}"#[..]));
    }

    assert_eq!(
        sent_v1
            .all()
            .iter()
            .map(|s| s.uri.as_str())
            .collect::<Vec<_>>(),
        ["/complete"]
    );
    assert_eq!(
        sent_v2
            .all()
            .iter()
            .map(|s| s.uri.as_str())
            .collect::<Vec<_>>(),
        ["/complete"]
    );
}

#[tokio::test]
async fn a_plugin_for_one_kind_lets_other_traffic_pass_when_it_says_so() {
    /// [`Polite`], for jev only: other traffic passes untouched.
    struct PoliteJev;

    #[async_trait]
    impl Plugin for PoliteJev {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            match cx.is_kind::<Jev>() {
                false => Ok(None),
                true => Polite.on_client(cx).await,
            }
        }
    }

    let (jev_transport, jev_sent) = local(|_, _| whole(StatusCode::OK, r#"{"text":"hi"}"#));
    let (any_transport, any_sent) =
        local(|_, _| response(StatusCode::OK, None, vec![Ok(Bytes::from("pong"))]));

    // Each client targets the provider of its own format, as an adapter's
    // mark would say.
    let mut jev = to("jev-v2", r#"{"prompt":"hi"}"#);
    jev.headers_mut()
        .insert("x-to", HeaderValue::from_static("v2"));
    let mut other = to("other", "ping");
    other
        .headers_mut()
        .insert("x-to", HeaderValue::from_static("any"));
    let gw = providers([
        add_provider("v2", V2, jev_transport),
        add_provider("any", Anywhere, any_transport),
    ])
    .and_then(|gw| {
        let gw = gw
            .client::<Jev, _, _>(V2)?
            .client::<Sent<Anywhere>, _, _>(Anywhere)?;
        Ok(gw.plugin(ToHeader).plugin(PoliteJev))
    })
    .unwrap_or_else(|e| panic!("{e}"));

    let (_, _, jev) = read_whole(serve(&gw, jev).await.0).await;
    let (status, _, other) = read_whole(serve(&gw, other).await.0).await;

    assert_eq!(jev.ok().as_deref(), Some(&br#"{"text":"hi, thanks"}"#[..]));
    assert_eq!(status, StatusCode::OK);
    assert_eq!(other.ok().as_deref(), Some(&b"pong"[..]));
    let prompt: Value = serde_json::from_slice(&jev_sent.all()[0].body).unwrap_or_default();
    assert_eq!(prompt["prompt"], "please, hi");
    assert_eq!(&any_sent.all()[0].body[..], b"ping");
}

/// Targets the provider the adapter named in `x-to`.
struct ToHeader;

#[async_trait]
impl Plugin for ToHeader {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let asked = cx.extensions().get::<Asked>();
        let to = asked.and_then(|a| a.headers.get("x-to")?.to_str().ok());
        if let Some(to) = to.map(str::to_owned) {
            cx.set_target(to);
        }
        Ok(None)
    }
}

/// Runs `plugin` for a jev-v2 client whose provider speaks jev-v2 too.
fn jev_relay(
    plugin: impl Plugin,
    transport: impl Fn(http::Request<Body>) -> SendFuture + Send + Sync + 'static,
) -> Gateway {
    let v2 = add_provider("v2", V2, transport);
    serving(V2, [v2], vec![Arc::new(plugin)], Some("v2"))
        .unwrap_or_else(|e| panic!("{e}"))
        .plugin(ChosenHeads)
}

#[tokio::test]
async fn what_filters_kept_back_goes_before_a_relayed_providers_error() {
    /// Keeps every answer back until the end; passes a keep-alive at once,
    /// as every value reaches a filter.
    struct Late;

    #[async_trait]
    impl Plugin for Late {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            cx.filter(KeepAll(Vec::new()))?;
            Ok(None)
        }
    }

    struct KeepAll(Vec<Answer>);

    impl Filter<Answer> for KeepAll {
        fn edit(&mut self, event: Answer, out: &mut Vec<Answer>) -> Result<(), Fault> {
            match event.text {
                None => out.push(event),
                Some(_) => self.0.push(event),
            }
            Ok(())
        }

        fn end(&mut self, _end: &Result<(), Fault>, out: &mut Vec<Answer>) -> Result<(), Fault> {
            out.append(&mut self.0);
            Ok(())
        }
    }

    let error = "data: {\"error\":{\"message\":\"overloaded\"}}\n\n";
    // The comment goes out at once, so the error cannot be refused.
    let (transport, _) =
        local(move |_, _| stream(&[": ping\n\n", "data: {\"text\":\"a\"}\n\n", error]));
    let gw = jev_relay(Late, transport);

    let (status, _, body) =
        read_whole(serve(&gw, to("jev-v2", r#"{"prompt":"hi"}"#)).await.0).await;

    assert_eq!(status, StatusCode::OK);
    let body = String::from_utf8(body.unwrap_or_default().to_vec()).unwrap_or_default();
    assert!(body.contains("\"a\"") && body.ends_with(error), "{body}");
}

#[tokio::test]
async fn an_input_that_cannot_be_translated_is_a_bad_request_whoever_brought_it() {
    /// Adds a field jev-v2 cannot say.
    struct Add;

    #[async_trait]
    impl Plugin for Add {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            cx.filter(|jev: &mut Jev| {
                jev.unknown.push(("extra".into(), Value::Null));
                Ok(())
            })?;
            Ok(None)
        }
    }

    let (transport, sent) = local(|_, _| panic!("untranslatable input reached provider"));
    let v2 = || add_provider("v2", V2, transport.clone());
    let plain = serving(V1, [v2()], Vec::new(), Some("v2")).unwrap_or_else(|e| panic!("{e}"));
    let adding = serving(V1, [v2()], vec![Arc::new(Add)], Some("v2"));
    let adding = adding.unwrap_or_else(|e| panic!("{e}"));

    let refused = read_whole(
        serve(&plain, to("jev-v1", r#"{"input":"hi","extra":1}"#))
            .await
            .0,
    )
    .await;
    let added = read_whole(serve(&adding, to("jev-v1", r#"{"input":"hi"}"#)).await.0).await;

    assert_eq!(refused.0, StatusCode::BAD_REQUEST);
    assert_eq!(added.0, StatusCode::BAD_REQUEST);
    assert!(sent.all().is_empty());
}

#[tokio::test]
async fn traffic_of_no_kind_is_a_kind_of_its_own() {
    /// Notes whether the request is of anywhere's kind, then of jev's kind
    /// by its input and its output type, then refuses.
    #[derive(Clone, Default)]
    struct Kinds(Arc<std::sync::Mutex<Vec<(bool, bool, bool)>>>);

    #[async_trait]
    impl Plugin for Kinds {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            // Traffic of no kind is a kind of its own, which no plugin for
            // another kind reads.
            let other = cx
                .input::<Jev>()
                .err()
                .map(|f| respan_gateway_http::error_code(&f).into_owned());
            let jev = cx.is_kind::<Jev>();
            assert_eq!(other.is_some_and(|c| c == "internal"), !jev);
            let kinds = (
                cx.is_kind::<Sent<Anywhere>>(),
                cx.is_kind::<Jev>(),
                cx.is_kind::<Answer>(),
            );
            self.0.lock().map(|mut k| k.push(kinds)).ok();
            Err(Fault::new("noted")
                .with(ErrorCode("noted".into()))
                .with(Status(403)))
        }
    }

    let kinds = Kinds::default();
    let any = add_provider("any", Anywhere, unused());
    let gw = providers([any])
        .and_then(|gw| {
            Ok(gw
                .client::<Jev, _, _>(V1)?
                .client::<Sent<Anywhere>, _, _>(Anywhere)?
                .plugin(kinds.clone()))
        })
        .unwrap_or_else(|e| panic!("{e}"));

    let _ = read_whole(serve(&gw, to("any", "ping")).await.0).await;
    let _ = read_whole(serve(&gw, to("jev-v1", r#"{"input":"hi"}"#)).await.0).await;

    assert_eq!(
        kinds.0.lock().map(|k| k.clone()).ok(),
        Some(vec![(true, false, false), (false, true, true)])
    );
}

#[tokio::test]
async fn a_refusal_goes_out_with_its_faults_status_even_a_success() {
    /// Refuses as JSON-RPC does: an error in a 200.
    struct Rpc;

    #[async_trait]
    impl Plugin for Rpc {
        async fn on_client(&self, _cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            Err(Fault::new("no")
                .with(ErrorCode("rpc_error".into()))
                .with(Status(200)))
        }
    }

    let (transport, sent) = local(|_, _| panic!("refusal reached provider"));
    let gw = jev_relay(Rpc, transport);

    let (status, _, body) =
        read_whole(serve(&gw, to("jev-v2", r#"{"prompt":"hi"}"#)).await.0).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body.ok().as_deref(), Some(&br#"{"error":"no"}"#[..]));
    assert!(sent.all().is_empty());
}

/// A provider whose stream sends its first bytes and stays open.
struct Open;

impl Open {
    fn into_sender(self) -> impl Clone + Fn(http::Request<Body>) -> SendFuture {
        |_| {
            Box::pin(async {
                let first = Bytes::from("data: hello\n\n");
                let body = futures::stream::iter([Ok(first)]).chain(futures::stream::pending());
                Ok((http::Response::new(Body::stream(body)), None))
            })
        }
    }
}

#[tokio::test]
async fn until_content_returns_a_response_of_no_kind_at_its_first_bytes_or_its_failure() {
    /// Sends the call's answer read to its content, or refuses with its failure.
    struct Race;

    #[async_trait]
    impl Plugin for Race {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let mut response = cx.call("any").await?;
            response.buffer_until(|_: &Got<Anywhere>| true).await?;
            Ok(Some(response))
        }
    }

    let open = add_provider("any", Anywhere, Open.into_sender());
    let gw = serving(Anywhere, [open], vec![Arc::new(Race)], None);
    let gw = gw.unwrap_or_else(|e| panic!("{e}"));
    let started = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        serve(&gw, to("any", "ping")),
    );
    assert!(
        started.await.is_ok(),
        "until_content waits for an open stream's end"
    );

    let (transport, _) = local(|_, _| whole(StatusCode::SERVICE_UNAVAILABLE, "busy"));
    let failing = add_provider("any", Anywhere, transport);
    let gw = serving(Anywhere, [failing], vec![Arc::new(Race)], None);
    let gw = gw.unwrap_or_else(|e| panic!("{e}"));

    let (status, _, body) = read_whole(serve(&gw, to("any", "ping")).await.0).await;

    // The provider's error response, as the client gets it when sent.
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        body.ok().as_deref(),
        Some(&b"the provider returned 503"[..])
    );
}

// A translated success's head is the client format's alone; a
// `Retry-After` crosses a translation only on a fault (`retry_after`).
#[tokio::test]
async fn a_translating_encoder_gets_none_of_the_providers_head() {
    /// jev v1, noting the headers its encoder is given.
    #[derive(Clone, Default)]
    struct Noting(Arc<std::sync::Mutex<Option<http::HeaderMap>>>);

    impl Format<Incoming, Jev, Answer> for Noting {
        fn name(&self) -> &str {
            V1.name
        }
        fn matches(&self, io: &Incoming) -> bool {
            matches_post(io, &format!("/{}", V1.name))
        }
        fn bind(&self, io: Incoming) -> Result<Connection<Jev, Answer>, Fault> {
            let seen = self.0.clone();
            Ok(io.bind(
                move |request| V1.inputs(request),
                move |reply, source| {
                    seen.lock()
                        .map(|mut h| *h = Some(reply.head().headers))
                        .ok();
                    V1.outputs(reply, source)
                },
                |fault, _, head| V1.refusal(fault, head),
            ))
        }
    }
    impl Format<Outgoing, Answer, Jev> for Noting {
        fn name(&self) -> &str {
            V1.name
        }
        fn bind(&self, io: Outgoing) -> Result<Connection<Answer, Jev>, Fault> {
            Ok(io.bind(
                |source| V1.prepare(source),
                |response, _| V1.replies(response),
                respan_gateway_http::success,
            ))
        }
    }

    let noting = Noting::default();
    let (transport, _) = local(|_, _| {
        let mut answer = whole(StatusCode::OK, r#"{"text":"hi"}"#);
        answer
            .headers_mut()
            .insert("x-jev-trace", HeaderValue::from_static("t1"));
        answer
            .headers_mut()
            .insert("retry-after", HeaderValue::from_static("7"));
        answer
    });
    let v2 = [add_provider("v2", V2, transport)];
    let gw = serving(noting.clone(), v2, Vec::new(), Some("v2")).unwrap_or_else(|e| panic!("{e}"));

    let (status, headers, body) =
        read_whole(serve(&gw, to("jev-v1", r#"{"input":"hi"}"#)).await.0).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body.ok().as_deref(), Some(&br#"{"output":"hi"}"#[..]));
    let given = noting
        .0
        .lock()
        .map(|h| h.clone())
        .ok()
        .flatten()
        .unwrap_or_default();
    // It describes the provider's protocol, which is not the client's.
    assert!(given.get("x-jev-trace").is_none());
    assert!(given.get("retry-after").is_none());
    assert!(headers.get("x-jev-trace").is_none());
}

// A message that is no content, such as an SSE comment, reaches filters, and
// goes out where it came when they pass it on.
#[tokio::test]
async fn a_keep_alive_reaches_filters_and_goes_on_as_a_comment() {
    /// Counts the keep-alives it is shown, passing everything on.
    #[derive(Clone, Default)]
    struct Counts(Arc<std::sync::atomic::AtomicUsize>);

    #[async_trait]
    impl Plugin for Counts {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let seen = self.0.clone();
            cx.filter(move |a: &mut Answer| {
                if a.text.is_none() {
                    seen.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                Ok(())
            })?;
            Ok(None)
        }
    }

    let chunks = [
        "data: {\"text\":\"a\"}\n\n",
        ": keep-alive\n\n",
        "data: {\"text\":\"b\"}\n\n",
    ];
    let (transport, _) = local(move |_, _| stream(&chunks));
    let counts = Counts::default();
    let gw = jev_relay(counts.clone(), transport);

    let (resp, ended) = serve(&gw, to("jev-v2", r#"{"prompt":"hi"}"#)).await;
    let (_, _, body) = read_whole(resp).await;

    assert_eq!(body.ok().as_deref(), Some(chunks.concat().as_bytes()));
    assert!(ended.await.is_ok());
    assert_eq!(counts.0.load(std::sync::atomic::Ordering::Relaxed), 1);
}

use respan_gateway_http::relay as explicit;

struct ChosenHeads;
#[async_trait]
impl Plugin for ChosenHeads {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        explicit::Relay::HEADS.on_client(cx).await
    }
    async fn on_provider(&self, px: &mut respan_gateway_sdk::ProviderContext) -> Result<(), Fault> {
        let client = px.client().format().to_owned();
        px.filter::<Answer>(explicit::RawErrors { client })?;
        Ok(())
    }
}
