//! Events carry their bytes (decision 0008): what formats hold for a relay.

mod common;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use common::Metered;
use http::StatusCode;
use respan_gateway_core::{
    ClientContext, Decoder, Encoder, Fault, Filter, Format, Gateway, Outcome, Plugin,
    ProviderContext, Response, async_trait,
};
use respan_gateway_core::{bytes, futures, http};
use respan_gateway_formats::conversation::{Conversation, Reply, units};
use respan_gateway_formats::opaque::{self, Got, Sent};
use respan_gateway_formats::{AnthropicMessages, OpenAiChat, Usage};
use respan_gateway_testkit::{Calls, MockTransport, Outcomes, mock, post, read_whole};

const OPENAI: &str = "http://openai.test/v1";
const ANTHROPIC: &str = "http://anthropic.test";

/// A protocol the gateway relays unread: a kind of its own.
struct Raw;

impl Format for Raw {
    type Input = Sent<Raw>;
    type Output = Got<Raw>;

    fn name(&self) -> &str {
        "raw"
    }

    fn accepts(&self, head: &http::request::Parts) -> bool {
        head.uri.path().starts_with("/raw")
    }

    fn request_decoder(&self, _request: &http::Request<Bytes>) -> Box<dyn Decoder<Sent<Raw>>> {
        opaque::request_decoder()
    }

    fn request_encoder(
        &self,
        _request: &mut http::Request<Bytes>,
        _first: Option<&Sent<Raw>>,
    ) -> Box<dyn Encoder<Sent<Raw>>> {
        opaque::request_encoder()
    }

    fn response_decoder(
        &self,
        head: &http::response::Parts,
        _request: &http::Request<Bytes>,
    ) -> Box<dyn Decoder<Got<Raw>>> {
        opaque::response_decoder("raw", head)
    }

    fn response_encoder(
        &self,
        head: &mut http::response::Parts,
        _request: &http::Request<Bytes>,
    ) -> Box<dyn Encoder<Got<Raw>>> {
        opaque::response_encoder("raw", head)
    }
}

/// A gateway serving `format`, calling `provider` of `format` over `mock`
/// at `base`, with `plugins`, recording outcomes on both edges.
fn gateway(
    format: impl Format + Clone,
    mock: &MockTransport,
    base: &str,
    plugins: Vec<Arc<dyn Plugin>>,
) -> (Gateway, Outcomes, Calls) {
    let (log, calls) = (Outcomes::default(), Calls::default());
    let gw = Gateway::new()
        .provider("p", format.clone(), mock.at(base))
        .and_then(|g| {
            g.serve(format)?
                .plugin("log", log.clone())?
                .plugin("calls", calls.clone())
        });
    let mut named = plugins.into_iter().enumerate();
    let gw = gw.and_then(|g| named.try_fold(g, |g, (i, p)| g.plugin(format!("p{i}"), p)));
    let gw = gw.and_then(|g| g.target("p"));
    (gw.unwrap_or_else(|e| panic!("{e}")), log, calls)
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
    let mock = MockTransport::new();
    let (gw, log, _) = gateway(Raw, &mock, "http://raw.test", Vec::new());
    let stream = [
        ": open\n\nda",
        "ta: {\"a\":1}\n",
        "\nevent: x\r\ndata: 2\r\n\r\n: ping\n\n",
    ];
    mock.reply("http://raw.test", mock::sse(&stream));

    let (status, _, body) = read_whole(gw.handle(post("/raw", "anything")).await.0).await;
    gw.idle().await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body.ok(), Some(Bytes::from(stream.concat())));
    assert_eq!(mock.sent()[0].body(), "anything".as_bytes());
    assert_eq!(log.only().end, Ok(()));
}

// Must hold: the same, for a whole body, and a failure's body as it came.
#[tokio::test]
async fn a_passthrough_of_no_kind_relays_a_whole_body_and_a_failure_as_they_came() {
    let mock = MockTransport::new();
    let (gw, _, _) = gateway(Raw, &mock, "http://raw.test", Vec::new());
    for (status, body) in [(200, "{not json"), (503, "busy, try later")] {
        let status = StatusCode::from_u16(status).unwrap_or_default();
        mock.reply("http://raw.test", mock::json(status, body));

        let (got, _, read) = read_whole(gw.handle(post("/raw", "x")).await.0).await;

        assert_eq!((got, read.ok()), (status, Some(Bytes::from(body))));
    }
}

/// Notes every reply its filter sees, on the client edge.
#[derive(Clone, Default)]
struct Seen(Arc<Mutex<Vec<Reply>>>);

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
        cx.filter(move |r: &mut Reply| {
            seen.lock().map(|mut s| s.push(r.clone())).ok();
            Ok(())
        })?;
        Ok(None)
    }
}

// Keep-alives and other messages that are no content reach every filter,
// and a client of the provider's format in place.
#[tokio::test]
async fn keep_alives_reach_every_filter_and_a_same_format_client_in_place() {
    let openai = [
        ": keep-alive\n\n",
        "data: {\"id\":\"c1\",\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Hi\"}}]}\n\n",
        ": keep-alive\n\n",
        "data: {\"id\":\"c1\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
        "data: [DONE]\n\n",
    ];
    let seen = Seen::default();
    let mock = MockTransport::new();
    let (gw, _, _) = gateway(OpenAiChat, &mock, OPENAI, vec![Arc::new(seen.clone())]);
    mock.reply(OPENAI, mock::sse(&openai));
    let chat =
        r#"{"model":"m","stream":true,"stream_options":{"include_usage":true},"messages":[]}"#;

    let (_, _, body) = read_whole(gw.handle(post("/v1/chat/completions", chat)).await.0).await;

    assert_eq!(body.ok(), Some(Bytes::from(openai.concat())));
    assert_eq!((seen.silent(), seen.all()), (2, 4));

    let anthropic = [
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
    let mock = MockTransport::new();
    let (gw, _, _) = gateway(
        AnthropicMessages,
        &mock,
        ANTHROPIC,
        vec![Arc::new(seen.clone())],
    );
    mock.reply(ANTHROPIC, mock::sse(&anthropic));
    let chat = r#"{"model":"c","max_tokens":5,"stream":true,"messages":[]}"#;

    let (_, _, body) = read_whole(gw.handle(post("/v1/messages", chat)).await.0).await;

    assert_eq!(body.ok(), Some(Bytes::from(anthropic.concat())));
    assert_eq!((seen.silent(), seen.all()), (4, 7));
}

/// OpenAI Chat, counting the request decoders it builds.
#[derive(Clone, Default)]
struct Counted(Arc<AtomicUsize>);

impl Format for Counted {
    type Input = Conversation;
    type Output = Reply;

    fn name(&self) -> &str {
        OpenAiChat.name()
    }

    fn accepts(&self, head: &http::request::Parts) -> bool {
        OpenAiChat.accepts(head)
    }

    fn request_decoder(&self, request: &http::Request<Bytes>) -> Box<dyn Decoder<Conversation>> {
        self.0.fetch_add(1, Ordering::Relaxed);
        OpenAiChat.request_decoder(request)
    }

    fn request_encoder(
        &self,
        request: &mut http::Request<Bytes>,
        first: Option<&Conversation>,
    ) -> Box<dyn Encoder<Conversation>> {
        OpenAiChat.request_encoder(request, first)
    }

    fn response_decoder(
        &self,
        head: &http::response::Parts,
        request: &http::Request<Bytes>,
    ) -> Box<dyn Decoder<Reply>> {
        OpenAiChat.response_decoder(head, request)
    }

    fn response_encoder(
        &self,
        head: &mut http::response::Parts,
        request: &http::Request<Bytes>,
    ) -> Box<dyn Encoder<Reply>> {
        OpenAiChat.response_encoder(head, request)
    }
}

/// Reads the input that came with the head.
struct Reads;

#[async_trait]
impl Plugin for Reads {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        cx.input::<Conversation>()?;
        Ok(None)
    }
}

// Must hold: a whole request no plugin reads is relayed undecoded, as the
// client's bytes; one read is decoded once, and still goes as it came.
#[tokio::test]
async fn a_request_no_plugin_reads_stays_undecoded() {
    const UNARY: &str = r#"{"id":"c","choices":[{"message":{"content":"hi"}}]}"#;
    let chat = r#"{"model":"m",  "messages":[{"role":"user","content":"hi"}]}"#;
    for (plugins, decoded) in [
        (Vec::new(), 0),
        (vec![Arc::new(Reads) as Arc<dyn Plugin>], 1),
    ] {
        let (mock, format) = (MockTransport::new(), Counted::default());
        let (gw, _, _) = gateway(format.clone(), &mock, OPENAI, plugins);
        mock.reply(OPENAI, mock::json(StatusCode::OK, UNARY));

        let (status, _, _) =
            read_whole(gw.handle(post("/v1/chat/completions", chat)).await.0).await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(format.0.load(Ordering::Relaxed), decoded);
        assert_eq!(mock.sent()[0].body(), chat.as_bytes());
    }
}

/// Cuts every call off once its replies report more than `.0` output
/// tokens: a provider-edge filter, as a mid-stream cut is.
struct Cap(f64);

#[async_trait]
impl Plugin for Cap {
    async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
        let (cap, mut spent) = (self.0, 0.0);
        px.filter(move |reply: &mut Reply| {
            spent += reply.usage.get(units::OUTPUT_TOKENS).unwrap_or_default();
            match spent > cap {
                true => Err(Fault::new(
                    StatusCode::PAYMENT_REQUIRED,
                    "cap",
                    "over the cap",
                )),
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
    for (after, metered) in [(true, 20.0), (false, 10.0)] {
        let mock = MockTransport::new();
        let meter = Metered::default();
        let mut plugins: Vec<Arc<dyn Plugin>> = vec![Arc::new(meter.clone())];
        match after {
            true => plugins.insert(0, Arc::new(Cap(15.0))),
            false => plugins.push(Arc::new(Cap(15.0))),
        }
        let (gw, _, calls) = gateway(OpenAiChat, &mock, OPENAI, plugins);
        mock.reply(OPENAI, mock::sse(&chunks));
        let chat = r#"{"model":"m","stream":true,"messages":[]}"#;

        let (_, _, body) = read_whole(gw.handle(post("/v1/chat/completions", chat)).await.0).await;
        gw.idle().await;

        let body = String::from_utf8_lossy(&body.unwrap_or_default()).into_owned();
        assert!(body.contains("\"a\"") && !body.contains("\"b\""), "{body}");
        let call = &calls.all()[0];
        assert!(matches!(&call.end, Err(f) if f.code() == "cap"));
        let usage = meter.total();
        assert_eq!(
            usage.get(units::OUTPUT_TOKENS),
            Some(metered),
            "after: {after}"
        );
    }
}

// Usage in values a plugin makes is shown, and reaches no provider edge, so
// no meter there (0009, which removed `with_usage`). A cached answer's usage
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
            shown.add(units::INPUT_TOKENS, 7.0);
            cached.add(units::INPUT_TOKENS, 3.0);
            cx.head_mut().extensions.insert(CachedUsage(cached));
            let mut hi = Reply::text(0, "hi");
            hi.usage = shown;
            let answer = futures::stream::iter([Ok(Reply::start("own", "m")), Ok(hi)]);
            Ok(Some(Response::stream(answer)))
        }
    }

    #[derive(Clone, Default)]
    struct Bills(Arc<Mutex<Usage>>);

    #[async_trait]
    impl Plugin for Bills {
        async fn on_client_end(&self, cx: &ClientContext, _outcome: &Outcome) {
            if let Some(CachedUsage(cached)) = cx.head().extensions.get() {
                *self.0.lock().unwrap_or_else(|e| e.into_inner()) = cached.clone();
            }
        }
    }

    let mock = MockTransport::new();
    let (bills, meter) = (Bills::default(), Metered::default());
    let plugins: Vec<Arc<dyn Plugin>> = vec![
        Arc::new(bills.clone()),
        Arc::new(meter.clone()),
        Arc::new(Answers),
    ];
    let (gw, _, _) = gateway(OpenAiChat, &mock, OPENAI, plugins);
    let chat = r#"{"model":"m","messages":[]}"#;

    let (_, _, body) = read_whole(gw.handle(post("/v1/chat/completions", chat)).await.0).await;
    gw.idle().await;

    let body: respan_gateway_core::serde_json::Value =
        respan_gateway_core::serde_json::from_slice(&body.unwrap_or_default()).unwrap_or_default();
    assert_eq!(body["usage"]["prompt_tokens"], 7);
    assert!(meter.total().is_empty());
    let billed = bills.0.lock().map(|u| u.get(units::INPUT_TOKENS)).ok();
    assert_eq!(billed, Some(Some(3.0)));
}

/// Fails on every reply it sees.
struct Refuses;

impl Filter<Reply> for Refuses {
    fn edit(&mut self, _reply: Reply, _out: &mut Vec<Reply>) -> Result<(), Fault> {
        Err(Fault::new(StatusCode::FORBIDDEN, "blocked", "no"))
    }
}

// A meter registered after a guard sees the usage in the reply the guard
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
    let mock = MockTransport::new();
    let meter = Metered::default();
    let plugins: Vec<Arc<dyn Plugin>> = vec![Arc::new(Guard), Arc::new(meter.clone())];
    let (gw, _, _) = gateway(OpenAiChat, &mock, OPENAI, plugins);
    mock.reply(OPENAI, mock::json(StatusCode::OK, UNARY));

    let (status, _, _) = read_whole(gw.handle(post("/v1/chat/completions", "{}")).await.0).await;
    gw.idle().await;

    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(meter.total().get(units::INPUT_TOKENS), Some(4.0));
}
