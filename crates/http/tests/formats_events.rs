//! Events carry their bytes: what formats hold for a relay.

#[allow(dead_code)] // Outcomes and Calls expose methods unused by this suite.
#[path = "../../../tests/support/hooks.rs"]
mod hooks;
#[allow(dead_code)] // The root helpers cover more request shapes than this suite uses.
#[path = "../../../tests/support/http.rs"]
mod http_support;
#[allow(dead_code)] // Shared support also serves the custom-format suite.
#[path = "formats_support/mod.rs"]
mod support;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use hooks::{Calls, Outcomes};
use http_support::{LIMIT, post, read_whole, serve};
use mockllm::{Answer, FakeAnthropic, FakeOpenAi};
use respan_gateway_anthropic::AnthropicMessages;
use respan_gateway_codec::Codec;
use respan_gateway_conversation::{Block, Input, Output, units};
use respan_gateway_http::Status;
use respan_gateway_http::bytes::Bytes;
use respan_gateway_http::formats::opaque;
use respan_gateway_http::http::{self, HeaderMap, HeaderValue, StatusCode};
use respan_gateway_http::{Body, HttpClient, Incoming, Outgoing, connect};
use respan_gateway_openai::OpenAiChat;
use respan_gateway_sdk::ErrorCode;
use respan_gateway_sdk::Usage;
use respan_gateway_sdk::futures;
use respan_gateway_sdk::futures::TryStreamExt;
use respan_gateway_sdk::{
    ClientContext, Connection, Fault, Filter, Format, Gateway, Plugin, ProviderContext, Response,
    async_trait,
};
use serde_json::json;
use support::{Metered, with_plugins};

/// A protocol the gateway relays unread: a kind of its own.
struct Raw;

impl Format<Incoming, opaque::Input<Raw>, opaque::Output<Raw>> for Raw {
    fn name(&self) -> &str {
        "raw"
    }
    fn matches(&self, io: &Incoming) -> bool {
        io.request().uri().path().starts_with("/raw")
    }
    fn bind(
        &self,
        io: Incoming,
    ) -> Result<Connection<opaque::Input<Raw>, opaque::Output<Raw>>, Fault> {
        Ok(io.bind(
            opaque::read_request,
            opaque::write_response,
            respan_gateway_http::plain,
        ))
    }
}
impl Format<Outgoing, opaque::Output<Raw>, opaque::Input<Raw>> for Raw {
    fn name(&self) -> &str {
        "raw"
    }
    fn bind(
        &self,
        io: Outgoing,
    ) -> Result<Connection<opaque::Output<Raw>, opaque::Input<Raw>>, Fault> {
        Ok(
            io.bind(opaque::write_request, opaque::read_response, |answer| {
                respan_gateway_http::provider_fault("raw", answer)
            }),
        )
    }
}

/// A gateway serving `format` over the caller's transport, recording outcomes
/// on both edges.
fn gateway<F, I, O>(
    format: F,
    transport: impl Fn(http::Request<Body>) -> SendFuture + Send + Sync + 'static,
    plugins: Vec<Arc<dyn Plugin>>,
) -> (Gateway, Outcomes, Calls)
where
    F: Clone + Format<Incoming, I, O> + Format<Outgoing, O, I>,
    I: Clone + Send + Sync + 'static,
    O: Clone + Send + Sync + 'static,
{
    let (log, calls) = (Outcomes::default(), Calls::default());
    let gw = Gateway::new()
        .provider::<I, O, _>("p", format.clone(), connect(transport, LIMIT))
        .and_then(|g| {
            Ok(g.client::<I, O, _>(format)?
                .plugin(log.clone())
                .plugin(calls.clone()))
        });
    let gw = gw.map(|g| with_plugins(g, plugins));
    let gw = gw.and_then(|g| g.target("p"));
    (gw.unwrap_or_else(|e| panic!("{e}")), log, calls)
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

fn provider(
    base: &str,
    header: &'static str,
    key: &'static str,
) -> impl Clone + Fn(http::Request<Body>) -> SendFuture + use<> {
    let mut headers = HeaderMap::new();
    headers.insert(header, HeaderValue::from_static(key));
    let client = HttpClient::new(base)
        .and_then(|http| http.with_headers(headers))
        .unwrap_or_else(|e| panic!("provider transport: {e}"));
    move |request| {
        let client = client.clone();
        Box::pin(async move { client.send(request).await })
    }
}

fn openai(fake: &FakeOpenAi) -> impl Clone + Fn(http::Request<Body>) -> SendFuture + use<> {
    provider(&fake.base_url(), "authorization", "Bearer sk-test")
}

fn anthropic(fake: &FakeAnthropic) -> impl Clone + Fn(http::Request<Body>) -> SendFuture + use<> {
    provider(&fake.base_url(), "x-api-key", "sk-ant-test")
}

fn events(events: &[&str]) -> Answer {
    Answer::events(events.iter().map(|event| json!(event.trim_end())).collect())
}

/// A deliberately local transport for byte-exact opaque relay and decoder
/// laziness, where a provider fake would validate or reframe the bytes.
fn local(
    status: StatusCode,
    mime: &'static str,
    chunks: Vec<Bytes>,
    sent: Arc<Mutex<Vec<Bytes>>>,
) -> impl Clone + Fn(http::Request<Body>) -> SendFuture {
    move |call| {
        let chunks = chunks.clone();
        let sent = sent.clone();
        Box::pin(async move {
            let body = call.into_body().try_collect::<Vec<_>>().await?.concat();
            sent.lock()
                .unwrap_or_else(|e| e.into_inner())
                .push(Bytes::from(body));
            let mut head = respan_gateway_http::http::Response::new(()).into_parts().0;
            head.status = status;
            head.headers
                .insert("content-type", HeaderValue::from_static(mime));
            Ok((
                http::Response::from_parts(
                    head,
                    Body::stream(futures::stream::iter(chunks.into_iter().map(Ok))),
                ),
                None,
            ))
        })
    }
}

fn never_called() -> impl Clone + Fn(http::Request<Body>) -> SendFuture {
    |_| Box::pin(async { Err(Fault::internal("provider should not be called")) })
}

impl Clone for Raw {
    fn clone(&self) -> Self {
        Raw
    }
}

// Must hold: a passthrough of no kind relays an event stream byte for
// byte, however the provider split it, its comments included.
#[tokio::test]
async fn a_passthrough_of_no_kind_relays_an_event_stream_byte_for_byte() {
    let stream = [
        ": open\n\nda",
        "ta: {\"a\":1}\n",
        "\nevent: x\r\ndata: 2\r\n\r\n: ping\n\n",
    ];
    let sent = Arc::new(Mutex::new(Vec::new()));
    let transport = local(
        StatusCode::OK,
        "text/event-stream",
        stream.iter().copied().map(Bytes::from).collect(),
        sent.clone(),
    );
    let (gw, log, _) = gateway(Raw, transport, Vec::new());

    let (status, _, body) = read_whole(serve(&gw, post("/raw", "anything")).await.0).await;
    gw.idle().await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body.ok(), Some(Bytes::from(stream.concat())));
    assert_eq!(
        sent.lock().unwrap_or_else(|e| e.into_inner()).as_slice(),
        &[Bytes::from_static(b"anything")]
    );
    assert!(log.only().is_ok());
}

// Must hold: the same, for a whole body, and a failure's body as it came.
#[tokio::test]
async fn a_passthrough_of_no_kind_relays_a_whole_body_and_a_failure_as_they_came() {
    for (status, body) in [(200, "{not json"), (503, "busy, try later")] {
        let status = StatusCode::from_u16(status).unwrap_or_default();
        let transport = local(
            status,
            "application/json",
            vec![Bytes::from(body)],
            Arc::default(),
        );
        let (gw, _, _) = gateway(Raw, transport, Vec::new());
        let (got, _, read) = read_whole(serve(&gw, post("/raw", "x")).await.0).await;

        assert_eq!(
            (got, read.ok()),
            (
                status,
                Some(Bytes::from(if status.is_success() {
                    body
                } else {
                    "the provider returned 503"
                }))
            )
        );
    }
}

/// Notes every reply its filter sees, on the client edge.
#[derive(Clone, Default)]
struct Seen(Arc<Mutex<Vec<Output>>>);

impl Seen {
    /// How many were no content.
    fn silent(&self) -> usize {
        let seen = self.0.lock().map(|s| s.clone()).unwrap_or_default();
        seen.iter().filter(|r| !r.is_content()).count()
    }

    fn all(&self) -> usize {
        self.0.lock().map(|s| s.len()).unwrap_or_default()
    }
}

#[async_trait]
impl Plugin for Seen {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let seen = self.0.clone();
        cx.filter(move |r: &mut Output| {
            seen.lock().map(|mut s| s.push(r.clone())).ok();
            Ok(())
        })?;
        Ok(None)
    }
}

// Keep-alives and other events that are no content reach every filter, and a
// client of the provider's format.
#[tokio::test]
async fn keep_alives_reach_every_filter_and_a_same_format_client_in_place() {
    let openai_events = [
        "data: keep-alive\n\n",
        "data: {\"id\":\"c1\",\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Hi\"}}]}\n\n",
        "data: keep-alive\n\n",
        "data: {\"id\":\"c1\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
        "data: [DONE]\n\n",
    ];
    let seen = Seen::default();
    let fake = FakeOpenAi::start().await;
    fake.answer(events(&openai_events));
    let (gw, _, _) = gateway(
        OpenAiChat::OPENAI,
        openai(&fake),
        vec![Arc::new(seen.clone())],
    );
    let chat = r#"{"model":"gpt-4.1-mini","stream":true,"stream_options":{"include_usage":true},"messages":[{"role":"user","content":"hi"}]}"#;

    let (_, _, body) = read_whole(serve(&gw, post("/v1/chat/completions", chat)).await.0).await;

    let body = String::from_utf8_lossy(&body.unwrap_or_default()).into_owned();
    // Each keep-alive, an event of the format, is written as it came.
    assert_eq!(body.matches("data: keep-alive\n\n").count(), 2, "{body}");
    assert!(body.contains("\"content\":\"Hi\""), "{body}");
    assert!(body.ends_with("data: [DONE]\n\n"), "{body}");
    // Two keep-alives, a start, a block's opening, its text and end, and a finish.
    assert_eq!((seen.silent(), seen.all()), (2, 7));

    let anthropic_events = [
        "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"m1\",\"model\":\"c\",\"usage\":{\"input_tokens\":5,\"output_tokens\":1}}}\n\n",
        "event: ping\ndata: {\"type\": \"ping\"}\n\n",
        "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n",
        "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"Hi\"}}\n\n",
        ": comment\n\n",
        "event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
        "event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":2}}\n\n",
        "event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n",
    ];
    let seen = Seen::default();
    let fake = FakeAnthropic::start().await;
    fake.answer(events(&anthropic_events));
    let (gw, _, _) = gateway(
        AnthropicMessages,
        anthropic(&fake),
        vec![Arc::new(seen.clone())],
    );
    let chat = r#"{"model":"claude-sonnet-4-5","max_tokens":5,"stream":true,"messages":[{"role":"user","content":"hi"}]}"#;
    let mut request = post("/v1/messages", chat);
    request
        .headers_mut()
        .insert("anthropic-version", HeaderValue::from_static("2023-06-01"));

    let (_, _, body) = read_whole(serve(&gw, request).await.0).await;

    let body = String::from_utf8_lossy(&body.unwrap_or_default()).into_owned();
    assert!(
        body.contains("event: ping\ndata: {\"type\":\"ping\"}\n\n"),
        "{body}"
    );
    assert!(body.contains("\"text\":\"Hi\""), "{body}");
    assert!(body.ends_with("event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"));
    // A comment says nothing. The ping and the two usage reports are no
    // content; with the start, the block's three events and the finish
    // they are all.
    assert_eq!((seen.silent(), seen.all()), (3, 8));
}

/// OpenAI Chat, counting lazy request parsing.
#[derive(Clone, Default)]
struct Counted(Arc<AtomicUsize>);

impl Format<Incoming, Input, Output> for Counted {
    fn name(&self) -> &str {
        OpenAiChat::OPENAI.name()
    }
    fn matches(&self, io: &Incoming) -> bool {
        OpenAiChat::OPENAI.matches(io)
    }
    fn bind(&self, io: Incoming) -> Result<Connection<Input, Output>, Fault> {
        let count = self.0.clone();
        Ok(io.bind(
            move |request| {
                respan_gateway_http::messages(request, move |_, raw| {
                    count.fetch_add(1, Ordering::Relaxed);
                    OpenAiChat::OPENAI.decode_request(raw)
                })
            },
            |reply| OpenAiChat::OPENAI.write_response(reply),
            |fault, request, head| OpenAiChat::OPENAI.refusal(fault, request, head),
        ))
    }
}
impl Format<Outgoing, Output, Input> for Counted {
    fn name(&self) -> &str {
        OpenAiChat::OPENAI.name()
    }
    fn bind(&self, io: Outgoing) -> Result<Connection<Output, Input>, Fault> {
        OpenAiChat::OPENAI.bind(io)
    }
}

/// Reads the input that came with the head.
struct Reads;

#[async_trait]
impl Plugin for Reads {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        cx.input::<Input>()?;
        Ok(None)
    }
}

// Must hold: a whole request is decoded once, whether or not a plugin reads
// it, and goes as the conversation it says.
#[tokio::test]
async fn a_request_no_plugin_reads_is_decoded_for_encoding() {
    const UNARY: &str = r#"{"id":"c","choices":[{"message":{"content":"hi"}}]}"#;
    let chat = r#"{"model":"m",  "messages":[{"role":"user","content":"hi"}]}"#;
    for (plugins, decoded) in [
        (Vec::new(), 1),
        (vec![Arc::new(Reads) as Arc<dyn Plugin>], 1),
    ] {
        let format = Counted::default();
        let sent = Arc::new(Mutex::new(Vec::new()));
        let transport = local(
            StatusCode::OK,
            "application/json",
            vec![Bytes::from(UNARY)],
            sent.clone(),
        );
        let (gw, _, _) = gateway(format.clone(), transport, plugins);

        let (status, _, _) =
            read_whole(serve(&gw, post("/v1/chat/completions", chat)).await.0).await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(format.0.load(Ordering::Relaxed), decoded);
        let sent = sent.lock().unwrap_or_else(|e| e.into_inner());
        let [sent] = sent.as_slice() else {
            panic!("{sent:?}");
        };
        let said: serde_json::Value = serde_json::from_slice(sent).unwrap_or_default();
        let wanted: serde_json::Value = serde_json::from_str(chat).unwrap_or_default();
        assert_eq!(said, wanted);
    }
}

/// Cuts every call off once its replies report more than `.0` output
/// tokens: a provider-edge filter, as a mid-stream cut is.
struct Cap(u64);

#[async_trait]
impl Plugin for Cap {
    async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
        let (cap, mut spent) = (self.0, 0);
        px.filter(move |reply: &mut Output| {
            if let Output::Usage(usage) = reply {
                spent += usage.get(units::OUTPUT_TOKENS).unwrap_or_default();
            }
            match spent > cap {
                true => Err(Fault::new("over the cap")
                    .with(ErrorCode("cap".into()))
                    .with(Status(StatusCode::PAYMENT_REQUIRED))),
                false => Ok(()),
            }
        })
    }
}

// The kernel counts no usage. A meter registered after a filter that cuts
// a stream runs first, so it sees every increment decoded, the one the cut
// failed on too; one registered before it misses that one.
#[tokio::test]
async fn a_meter_registered_after_a_cutting_filter_sees_every_increment() {
    let chunk = |text: &str, out: u32| {
        format!(
            "data: {{\"id\":\"c1\",\"choices\":[{{\"index\":0,\"delta\":{{\"content\":\"{text}\"}}}}],\"usage\":{{\"completion_tokens\":{out}}}}}\n\n"
        )
    };
    let stream = [chunk("a", 10), chunk("b", 20), chunk("c", 30)];
    let chunks: Vec<&str> = stream.iter().map(String::as_str).collect();
    for (after, metered) in [(true, 20), (false, 10)] {
        let fake = FakeOpenAi::start().await;
        fake.answer(events(&chunks));
        let meter = Metered::default();
        let mut plugins: Vec<Arc<dyn Plugin>> = vec![Arc::new(meter.clone())];
        match after {
            true => plugins.insert(0, Arc::new(Cap(15))),
            false => plugins.push(Arc::new(Cap(15))),
        }
        let (gw, _, calls) = gateway(OpenAiChat::OPENAI, openai(&fake), plugins);
        let chat =
            r#"{"model":"gpt-4.1-mini","stream":true,"messages":[{"role":"user","content":"hi"}]}"#;

        let (_, _, body) = read_whole(serve(&gw, post("/v1/chat/completions", chat)).await.0).await;
        gw.idle().await;

        let body = String::from_utf8_lossy(&body.unwrap_or_default()).into_owned();
        assert!(body.contains("\"a\"") && !body.contains("\"b\""), "{body}");
        let call = &calls.all()[0];
        assert!(matches!(call, Err(f) if respan_gateway_http::error_code(f) == "cap"));
        let usage = meter.total();
        assert_eq!(
            usage.get(units::OUTPUT_TOKENS),
            Some(metered),
            "after: {after}"
        );
    }
}

// Usage in values a plugin makes is shown, and reaches no provider edge, so
// no meter there. A cached answer's usage
// reaches billing in an extension.
#[tokio::test]
async fn usage_a_plugin_makes_reaches_no_meter_and_an_extension_bills_it() {
    #[derive(Clone)]
    struct CachedUsage(Usage);

    struct Answers;

    #[async_trait]
    impl Plugin for Answers {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let (mut shown, mut cached) = (Usage::default(), Usage::default());
            shown.add(units::INPUT_TOKENS, 7);
            cached.add(units::INPUT_TOKENS, 3);
            cx.extensions_mut().insert(CachedUsage(cached));
            let hi = [
                Output::start("own", "m"),
                Output::block_start(0, Block::Text),
                Output::text(0, "hi"),
                Output::block_end(0),
                Output::usage(shown),
            ];
            let answer = futures::stream::iter(hi.map(Ok));
            Ok(Some(Response::stream(answer)))
        }
    }

    #[derive(Clone, Default)]
    struct Bills(Arc<Mutex<Usage>>);

    #[async_trait]
    impl Plugin for Bills {
        async fn on_client_end(&self, cx: &ClientContext, _end: &Result<(), Fault>) {
            if let Some(CachedUsage(cached)) = cx.extensions().get() {
                *self.0.lock().unwrap_or_else(|e| e.into_inner()) = cached.clone();
            }
        }
    }

    let (bills, meter) = (Bills::default(), Metered::default());
    let plugins: Vec<Arc<dyn Plugin>> = vec![
        Arc::new(bills.clone()),
        Arc::new(meter.clone()),
        Arc::new(Answers),
    ];
    let (gw, _, _) = gateway(OpenAiChat::OPENAI, never_called(), plugins);
    let chat = r#"{"model":"gpt-4.1-mini","messages":[{"role":"user","content":"hi"}]}"#;

    let (_, _, body) = read_whole(serve(&gw, post("/v1/chat/completions", chat)).await.0).await;
    gw.idle().await;

    let body: serde_json::Value =
        serde_json::from_slice(&body.unwrap_or_default()).unwrap_or_default();
    assert_eq!(body["usage"]["prompt_tokens"], 7);
    assert!(meter.total().is_empty());
    let billed = bills.0.lock().map(|u| u.get(units::INPUT_TOKENS)).ok();
    assert_eq!(billed, Some(Some(3)));
}

/// Fails on the usage it sees.
struct Refuses;

impl Filter<Output> for Refuses {
    fn edit(&mut self, reply: Output, out: &mut Vec<Output>) -> Result<(), Fault> {
        if matches!(reply, Output::Usage(_)) {
            return Err(Fault::new("no")
                .with(ErrorCode("blocked".into()))
                .with(Status(StatusCode::FORBIDDEN)));
        }
        out.push(reply);
        Ok(())
    }
}

// A meter registered after a guard sees the usage in the event the guard
// refuses: its filter runs first.
#[tokio::test]
async fn a_meter_after_a_guard_sees_the_usage_in_the_reply_it_refuses() {
    struct Guard;

    #[async_trait]
    impl Plugin for Guard {
        async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
            px.filter(Refuses)
        }
    }

    const UNARY: &str = r#"{"id":"c","choices":[{"message":{"content":"hi"}}],"usage":{"prompt_tokens":4,"completion_tokens":1}}"#;
    let fake = FakeOpenAi::start().await;
    fake.answer(Answer::raw(UNARY));
    let meter = Metered::default();
    let plugins: Vec<Arc<dyn Plugin>> = vec![Arc::new(Guard), Arc::new(meter.clone())];
    let (gw, _, _) = gateway(OpenAiChat::OPENAI, openai(&fake), plugins);
    let chat = r#"{"model":"gpt-4.1-mini","messages":[{"role":"user","content":"hi"}]}"#;

    let (status, _, _) = read_whole(serve(&gw, post("/v1/chat/completions", chat)).await.0).await;
    gw.idle().await;

    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(meter.total().get(units::INPUT_TOKENS), Some(4));
}
