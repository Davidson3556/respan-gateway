//! The kernel's promises on ends and filters, probed with a scripted line
//! format whose decoder turns each line into one value. The kernel counts
//! no usage: [`Meter`], registered last, sums what lines report through a
//! provider-edge filter, so these tests show what such a filter sees.

use respan_gateway_conversation::Input;
use respan_gateway_core::{BadRequest, ErrorCode, Truncated, Upstream};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use bytes::{Bytes, BytesMut};
use futures::StreamExt;
use http::StatusCode;
use http_body_util::BodyExt;
use respan_gateway_conversation::Usage;
use respan_gateway_core::futures;
use respan_gateway_core::{
    Call, ClientContext, EventStream, Fault, Filter, Gateway, Plugin, ProviderContext, Response,
    async_trait,
};
use respan_gateway_http::Body;
use respan_gateway_http::Encoder;
use respan_gateway_http::Status;
use respan_gateway_http::bodiless;
use respan_gateway_http::bytes;
use respan_gateway_http::http::request::Parts as RequestHead;
use respan_gateway_http::{http, http_body_util};
#[allow(dead_code, unused_imports)]
#[path = "../../../../tests/support/http.rs"]
mod http_helpers;
use http_helpers::{LIMIT, serve, serve_live};

/// What a client sends: one message.
#[derive(Debug, Clone, PartialEq)]
struct LIn(String);

/// What a line says: text, usage, or no content.
#[derive(Debug, Clone, PartialEq)]
struct LOut(String, Usage);

/// A line a plugin says.
fn out(text: impl Into<String>) -> LOut {
    LOut(text.into(), Usage::default())
}

const LINE: &str = "line";

/// Lines: `i:X` text, `u:N` usage of N `t`, `e` the end, `x:M` a failure;
/// any other line is no content. Each line is one value; a body ends at its
/// end, complete only after `e`.
#[derive(Clone)]
struct Line;

impl respan_gateway_core::Format<respan_gateway_http::ClientIo, LIn, LOut> for Line {
    fn name(&self) -> &str {
        LINE
    }
    fn matches(&self, io: &respan_gateway_http::ClientIo) -> bool {
        io.request().uri().path().starts_with("/line")
    }
    fn bind(
        &self,
        io: respan_gateway_http::ClientIo,
    ) -> Result<respan_gateway_core::Connection<LIn, LOut>, Fault> {
        Ok(io.bind(
            |request| {
                respan_gateway_http::messages(request, |_, raw| {
                    let text = String::from_utf8_lossy(&raw).into_owned();
                    Ok(LIn(text))
                })
            },
            |reply| {
                let head = reply.head();
                if reply.request().method() == http::Method::GET {
                    Ok(reply.open(
                        head,
                        Bytes::from(respan_gateway_http::formats::sse::comment("open")),
                        Box::new(LineEncoder(None)),
                    ))
                } else {
                    Ok(reply.write(head, Box::new(LineEncoder(None))))
                }
            },
            |fault, _, _| Bytes::from(format!("x:{}\n", fault.message())),
        ))
    }
}

impl respan_gateway_core::Format<respan_gateway_http::ProviderIo, LOut, LIn> for Line {
    fn name(&self) -> &str {
        LINE
    }
    fn bind(
        &self,
        io: respan_gateway_http::ProviderIo,
    ) -> Result<respan_gateway_core::Connection<LOut, LIn>, Fault> {
        Ok(io.bind(line_call, line_outputs, line_refused))
    }
}

fn line_outputs(
    response: http::Response<Body>,
    request: http::Request<Bytes>,
) -> EventStream<LOut> {
    if bodiless(request.method(), response.status()) {
        return EventStream::empty();
    }
    respan_gateway_http::formats::parse(
        response.into_body(),
        LineDecoder::default(),
        |state, bytes, out| state.decode(bytes, out),
        |state, end, out| {
            end?;
            state.end(out)
        },
    )
}

/// A call as Line writes it: at `/line`, each input as its text.
fn line_call(head: &mut RequestHead, _: Option<&LIn>) -> Result<Box<dyn Encoder<LIn>>, Fault> {
    respan_gateway_http::formats::set_path(head, "/line")?;
    Ok(Box::new(Lines))
}

/// A refused answer: the provider's fault, with the failure its lines say.
fn line_refused(answer: http::Response<Bytes>) -> Fault {
    let fault = respan_gateway_http::provider_fault(LINE, answer.clone());
    let mut lines = LineDecoder::default();
    match lines.decode(answer.into_body(), &mut Vec::new()) {
        Err(said) => said,
        Ok(()) => fault,
    }
}

/// Writes each input as its text.
struct Lines;

impl Encoder<LIn> for Lines {
    fn encode(&mut self, input: &LIn, out: &mut BytesMut) -> Result<(), Fault> {
        out.extend_from_slice(input.0.as_bytes());
        Ok(())
    }

    fn end(&mut self, _end: &Result<(), Fault>, _out: &mut BytesMut) -> Result<(), Fault> {
        Ok(())
    }
}

/// Whether it decoded any line, and whether `e` came.
#[derive(Default)]
struct LineDecoder(bool, bool);

impl LineDecoder {
    fn decode(&mut self, bytes: Bytes, output: &mut Vec<LOut>) -> Result<(), Fault> {
        for line in String::from_utf8_lossy(&bytes).lines() {
            self.0 = true;
            if self.1 {
                continue;
            }
            let said = match line.split_once(':') {
                Some(("i", x)) => out(x),
                Some(("u", n)) => {
                    let u = Usage::new(0, n.parse().unwrap_or(0));
                    LOut(String::new(), u)
                }
                Some(("x", m)) => return Err(Upstream::fault(m)),
                _ if line == "e" => {
                    self.1 = true;
                    continue;
                }
                _ => out(""),
            };
            output.push(said);
        }
        Ok(())
    }

    /// Like a JSON format, an empty body is not a response of this format.
    fn end(&mut self, _out: &mut Vec<LOut>) -> Result<(), Fault> {
        match (self.0, self.1) {
            (false, _) => Err(Upstream::fault("not a line body")),
            (true, false) => Err(Truncated::fault()),
            (true, true) => Ok(()),
        }
    }
}

/// Opens a `GET`'s body with an SSE comment; holds usage for the next text
/// or the end, so a usage line never sends the head before content.
struct LineEncoder(Option<Usage>);

impl LineEncoder {
    fn release(&mut self, out: &mut BytesMut) {
        if let Some(u) = self.0.take() {
            let n = u.output_tokens;
            out.extend_from_slice(format!("u:{n}\n").as_bytes());
        }
    }
}

impl Encoder<LOut> for LineEncoder {
    fn encode(&mut self, o: &LOut, out: &mut BytesMut) -> Result<(), Fault> {
        if !o.1.is_empty() {
            *self.0.get_or_insert_default() += &o.1;
        }
        if !o.0.is_empty() {
            self.release(out);
            out.extend_from_slice(format!("i:{}\n", o.0).as_bytes());
        }
        Ok(())
    }

    fn end(&mut self, end: &Result<(), Fault>, out: &mut BytesMut) -> Result<(), Fault> {
        match end {
            Ok(()) => {
                self.release(out);
                out.extend_from_slice(b"e\n");
            }
            Err(f) => out.extend_from_slice(format!("x:{}\n", f.message()).as_bytes()),
        }
        Ok(())
    }
}

/// A provider that answers each call with the next script: a status and frames.
#[derive(Clone, Default)]
struct Script(Arc<Mutex<VecDeque<Reply>>>);

type Reply = (StatusCode, Vec<&'static str>);
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

impl Script {
    fn then(self, status: StatusCode, frames: Vec<&'static str>) -> Self {
        lock(&self.0).push_back((status, frames));
        self
    }
}

impl Script {
    pub async fn send(
        &self,
        _call: http::Request<Body>,
    ) -> Result<
        (
            http::Response<Body>,
            Option<respan_gateway_http::Completion>,
        ),
        Fault,
    > {
        let (status, frames) = lock(&self.0).pop_front().expect("scripted");
        let frames =
            futures::stream::iter(frames).map(|f| Ok::<_, Fault>(Bytes::from_static(f.as_bytes())));
        let mut head = respan_gateway_http::http::Response::new(()).into_parts().0;
        head.status = status;
        Ok((http::Response::from_parts(head, Body::stream(frames)), None))
    }
}
impl Script {
    fn into_sender(self) -> impl Clone + Fn(http::Request<Body>) -> SendFuture {
        move |call| {
            let value = self.clone();
            Box::pin(async move { value.send(call).await })
        }
    }
}

/// Records every provider-edge outcome and every client-edge outcome, and
/// what [`Meter`] metered.
#[derive(Clone, Default)]
struct Log(
    Shared<String>,
    Shared<Result<(), Fault>>,
    Shared<Result<(), Fault>>,
    Shared<Usage>,
);

type Shared<T> = Arc<Mutex<Vec<T>>>;

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

impl Log {
    fn note(&self, s: impl Into<String>) {
        lock(&self.0).push(s.into());
    }
    fn notes(&self) -> Vec<String> {
        lock(&self.0).clone()
    }
    fn calls(&self) -> Vec<Result<(), Fault>> {
        lock(&self.1).clone()
    }
    fn clients(&self) -> Vec<Result<(), Fault>> {
        lock(&self.2).clone()
    }
    fn metered(&self) -> Vec<Usage> {
        lock(&self.3).clone()
    }
}

/// One call's usage, summed by [`Meter`]'s filter.
#[derive(Clone, Default)]
struct LineUsage(Arc<Mutex<Usage>>);

/// Meters each call of this kind into the log: a provider-edge filter sums
/// the usage of the lines it sees. Registered last, so its filter sees each
/// line first.
struct Meter(Log);

#[async_trait]
impl Plugin for Meter {
    async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
        if !px.is_kind::<LOut>() {
            return Ok(());
        }
        let usage = LineUsage::default();
        px.extensions_mut().insert(usage.clone());
        px.filter(move |o: &mut LOut| {
            *lock(&usage.0) += &o.1;
            Ok(())
        })
    }

    async fn on_provider_end(&self, px: &ProviderContext, _end: &Result<(), Fault>) {
        if let Some(LineUsage(usage)) = px.extensions().get::<LineUsage>() {
            let usage = *lock(usage);
            lock(&self.0.3).push(usage);
        }
    }
}

#[async_trait]
impl Plugin for Log {
    async fn on_provider_end(&self, _px: &ProviderContext, end: &Result<(), Fault>) {
        lock(&self.1).push(end.clone());
    }
    async fn on_client_end(&self, _cx: &ClientContext, end: &Result<(), Fault>) {
        lock(&self.2).push(end.clone());
    }
}

fn gateway(script: Script, log: &Log, plugin: impl Plugin) -> Gateway {
    Gateway::new()
        .provider::<LIn, _, _>(
            "p",
            Line,
            respan_gateway_http::connect(script.into_sender(), LIMIT),
        )
        .and_then(|g| g.client::<LIn, _, _>(Line))
        .and_then(|g| g.target("p"))
        .map(|g| g.plugin(log.clone()))
        .map(|g| g.plugin(plugin))
        .map(|g| g.plugin(Meter(log.clone())))
        .unwrap_or_else(|e| panic!("{e}"))
}

fn req(method: http::Method) -> http::Request<Body> {
    let body = Body::full(Bytes::from_static(b"hi"));
    let mut r = http::Request::new(body);
    *r.method_mut() = method;
    *r.uri_mut() = http::Uri::from_static("/line");
    r
}

async fn run(gw: &Gateway, r: http::Request<Body>) -> (StatusCode, Result<Bytes, Fault>) {
    let (resp, ended) = serve(gw, r).await;
    let status = resp.status();
    let body = BodyExt::collect(resp.into_body())
        .await
        .map(|b| b.to_bytes());
    let _ = ended.await;
    gw.idle().await;
    (status, body)
}

/// The `t` a call's lines reported, as [`Meter`] saw them.
fn t(usage: &Usage) -> Option<u64> {
    Some(usage.output_tokens).filter(|&n| n > 0)
}

// Usage read by `until_content`, then dropped, reaches the call's meter: provider-edge
// filters run as a response is decoded, whoever reads it.

#[tokio::test]
async fn f1_usage_read_until_content_is_metered_even_if_the_response_is_dropped() {
    struct Peek;

    #[async_trait]
    impl Plugin for Peek {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let call: Call = cx.call("p");
            // Read to its first content; the plugin answers on its own.
            let mut read = call.await?;
            read.buffer_until(|out: &LOut| !out.0.is_empty()).await?;
            drop(read);
            let own = futures::stream::iter([Ok(out("own"))]);
            Ok(Some(Response::stream(own)))
        }
    }

    let log = Log::default();
    let script = Script::default().then(StatusCode::OK, vec!["u:5\ni:hi\n", "i:more\ne\n"]);
    let gw = gateway(script, &log, Peek);
    let (status, _) = run(&gw, req(http::Method::POST)).await;
    assert_eq!(status, StatusCode::OK);
    // The provider billed 5, which its edge's meter saw.
    assert_eq!(log.calls().len(), 1);
    assert_eq!(log.metered().iter().map(t).collect::<Vec<_>>(), [Some(5)]);
}

// A client-edge filter failing on a value that shares a frame with usage
// leaves that usage metered on the provider edge, whose filters ran first.

#[tokio::test]
async fn f2_usage_is_metered_on_the_call_though_the_client_edge_failed_first() {
    struct Guard;

    #[async_trait]
    impl Plugin for Guard {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            cx.filter(|o: &mut LOut| match o.0.as_str() {
                "bad" => Err(BadRequest::fault("blocked")),
                _ => Ok(()),
            })?;
            Ok(None)
        }
    }

    let log = Log::default();
    let script = Script::default().then(StatusCode::OK, vec!["i:bad\nu:7\ne\n"]);
    let gw = gateway(script, &log, Guard);
    let (status, _) = run(&gw, req(http::Method::POST)).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(t(&log.metered()[0]), Some(7));
}

// A `HEAD` whose provider sends no body passes its head.

#[tokio::test]
async fn f3_a_head_whose_provider_sends_no_body_passes_its_head() {
    let log = Log::default();
    let script = Script::default().then(StatusCode::OK, vec![]);
    let gw = gateway(script, &log, Log::default()).plugin(explicit::Relay::RAW);
    let (status, body) = run(&gw, req(http::Method::HEAD)).await;
    assert_eq!(status, StatusCode::OK, "{body:?}");
    assert!(log.calls()[0].is_ok());
}

/// The same for a provider's 204, translated between the standard formats.
/// A translated success's head is the client's format's, so the
/// status goes; the call still ends as its head says.
#[tokio::test]
async fn f3b_a_translated_204_is_a_success_of_the_clients_format() {
    use respan_gateway_conversation_http::anthropic::AnthropicMessages;
    use respan_gateway_conversation_http::openai::OpenAiChat;
    // Deliberately nonstandard provider success: no chat payload to decode.
    let transport = |_| async {
        let mut response = http::Response::new(());
        *response.status_mut() = StatusCode::NO_CONTENT;
        Ok((
            http::Response::from_parts(response.into_parts().0, Body::full(Bytes::new())),
            None,
        ))
    };
    let gw = Gateway::new()
        .provider::<Input, _, _>(
            "anthropic",
            AnthropicMessages,
            respan_gateway_http::connect(transport, LIMIT),
        )
        .and_then(|g| g.client::<Input, _, _>(OpenAiChat::OPENAI))
        .and_then(|g| g.target("anthropic"))
        .unwrap_or_else(|e| panic!("{e}"));
    let r = http_helpers::post(
        "/v1/chat/completions",
        r#"{"model":"claude-x","messages":[{"role":"user","content":"hi"}]}"#,
    );
    let (status, _) = run(&gw, r).await;
    assert_eq!(status, StatusCode::OK);
}

// A filter whose edit failed still gets its end.

#[tokio::test]
async fn f4_a_filter_whose_edit_failed_still_gets_its_end() {
    struct Once(Log);

    impl Filter<LOut> for Once {
        fn edit(&mut self, o: LOut, out: &mut Vec<LOut>) -> Result<(), Fault> {
            if o.0 == "bad" {
                return Err(BadRequest::fault("blocked"));
            }
            out.push(o);
            Ok(())
        }
        fn end(&mut self, end: &Result<(), Fault>, _out: &mut Vec<LOut>) -> Result<(), Fault> {
            self.0.note(format!("end {}", end.is_ok()));
            Ok(())
        }
    }

    struct Guard(Log);

    #[async_trait]
    impl Plugin for Guard {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            cx.filter(Once(self.0.clone()))?;
            Ok(None)
        }
    }

    let log = Log::default();
    let script = Script::default().then(StatusCode::OK, vec!["i:ok\n", "i:bad\n", "e\n"]);
    let gw = gateway(script, &log, Guard(log.clone()));
    let _ = run(&gw, req(http::Method::POST)).await;
    assert_eq!(log.notes(), ["end false"]);
}

// A call settles at the end of its body, so `until_content` then drop abandons it
// however the frames fell.

async fn until_content_then_drop(frames: Vec<&'static str>) -> Result<(), Fault> {
    struct Peek;

    #[async_trait]
    impl Plugin for Peek {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let mut response = cx.call("p").await?;
            response
                .buffer_until(|out: &LOut| !out.0.is_empty())
                .await?;
            drop(response);
            let own = futures::stream::iter([Ok(out("own"))]);
            Ok(Some(Response::stream(own)))
        }
    }
    let log = Log::default();
    let gw = gateway(Script::default().then(StatusCode::OK, frames), &log, Peek);
    let _ = run(&gw, req(http::Method::POST)).await;
    log.calls()[0].clone()
}

/// Abandoned whether or not `e` shared a frame with the content.
#[tokio::test]
async fn f5_until_content_then_drop_ends_the_call_at_the_bodys_end_only() {
    for frames in [vec!["i:hi\ne\n"], vec!["i:hi\n", "e\n"]] {
        let end = until_content_then_drop(frames).await;
        assert_eq!(
            end.err()
                .map(|f| respan_gateway_http::error_code(&f).to_owned())
                .as_deref(),
            Some("abandoned")
        );
    }
}

// A refused `HEAD` ends with its fault on the client edge.

#[tokio::test]
async fn f6_a_refused_head_request_ends_with_its_fault() {
    struct No;

    #[async_trait]
    impl Plugin for No {
        async fn on_client(&self, _cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            Err(BadRequest::fault("no"))
        }
    }

    let log = Log::default();
    let gw = gateway(Script::default(), &log, No);
    let (status, _) = run(&gw, req(http::Method::HEAD)).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let end = log.clients()[0].clone();
    assert_eq!(
        end.err()
            .map(|f| respan_gateway_http::error_code(&f).to_owned())
            .as_deref(),
        Some("bad_request")
    );
}

// Usage decoded after a call's filter failed, in the same frame, reaches no
// filter, so no meter.

#[tokio::test]
async fn f7_usage_decoded_after_a_calls_filter_failed_reaches_no_meter() {
    struct Guard;

    #[async_trait]
    impl Plugin for Guard {
        async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
            px.filter(|o: &mut LOut| match o.0.as_str() {
                "bad" => Err(BadRequest::fault("blocked")),
                _ => Ok(()),
            })
        }
    }

    let log = Log::default();
    let script = Script::default().then(StatusCode::OK, vec!["i:ok\n", "i:bad\nu:7\ne\n"]);
    let gw = gateway(script, &log, Guard);
    let _ = run(&gw, req(http::Method::POST)).await;
    assert!(log.calls()[0].is_err());
    assert_eq!(t(&log.metered()[0]), None);
}

// A client-edge filter panicking ends the call internal.

#[tokio::test]
async fn f8_a_client_edge_panic_ends_the_call_internal() {
    struct Boom;

    #[async_trait]
    impl Plugin for Boom {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            cx.filter(|o: &mut LOut| {
                assert!(o.0 != "boom", "a filter panics");
                Ok(())
            })?;
            Ok(None)
        }
    }

    let log = Log::default();
    let script = Script::default().then(StatusCode::OK, vec!["i:ok\n", "i:boom\n", "e\n"]);
    let gw = gateway(script, &log, Boom);
    let _ = run(&gw, req(http::Method::POST)).await;
    let client = log.clients()[0].clone();
    let call = log.calls()[0].clone();
    assert_eq!(
        client
            .err()
            .map(|f| respan_gateway_http::error_code(&f).to_owned())
            .as_deref(),
        Some("internal")
    );
    assert_eq!(
        call.err()
            .map(|f| respan_gateway_http::error_code(&f).to_owned())
            .as_deref(),
        Some("internal")
    );
}

// A 101 to a whole request is refused, though its provider sent it: the
// call did not ask to switch, so its answer is no success by HTTP's rule
// ([`ProviderIo::bind`](respan_gateway_http::ProviderIo::bind)), and is refused at its head, whatever
// its body says. Its relayed head never reaches the client.

#[tokio::test]
async fn f9_a_101_to_a_whole_request_is_refused_whoever_sent_it() {
    let log = Log::default();
    let script = Script::default().then(StatusCode::SWITCHING_PROTOCOLS, vec!["e\n"]);
    let gw = gateway(script, &log, Log::default()).plugin(explicit::Relay::HEADS);
    let (status, _) = run(&gw, req(http::Method::POST)).await;
    assert_eq!(status, StatusCode::BAD_GATEWAY);
    let end = log.clients()[0].clone();
    assert_eq!(
        end.err().map(|f| (
            respan_gateway_http::error_code(&f).to_owned(),
            f.message().to_owned()
        )),
        Some((
            "upstream".to_owned(),
            "the provider returned 101".to_owned()
        ))
    );
}

// A call's extensions are read on its provider edge as they are written.

#[tokio::test]
async fn y1_a_calls_extensions_are_read_on_its_provider_edge() {
    #[derive(Clone)]
    struct Tag(&'static str);

    struct Tagged(Log);

    #[async_trait]
    impl Plugin for Tagged {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            cx.call("p").with_extension(Tag("own")).await.map(Some)
        }

        async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
            let tag = px
                .client()
                .extensions()
                .get::<Tag>()
                .map_or("none", |t| t.0);
            self.0.note(tag);
            Ok(())
        }
    }

    let log = Log::default();
    let script = Script::default().then(StatusCode::OK, vec!["i:hi\ne\n"]);
    let gw = gateway(script, &log, Tagged(log.clone()));
    let (status, _) = run(&gw, req(http::Method::POST)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(log.notes(), ["own"]);
}

// A format that opens a quiet stream sends its head at once with its opening
// bytes, and the plugin's outputs stay its own.

#[tokio::test]
async fn y3_an_opened_quiet_stream_sends_its_head_at_once() {
    type Outputs = futures::channel::mpsc::UnboundedReceiver<Result<LOut, Fault>>;

    struct Quiet(Mutex<Option<Outputs>>);

    #[async_trait]
    impl Plugin for Quiet {
        async fn on_client(&self, _cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let outputs = lock(&self.0).take().expect("one request");
            Ok(Some(Response::stream(outputs)))
        }
    }

    let (send, outputs) = futures::channel::mpsc::unbounded();
    let log = Log::default();
    let gw = gateway(Script::default(), &log, Quiet(Mutex::new(Some(outputs))));
    let wait = std::time::Duration::from_secs(5);
    let handled = tokio::time::timeout(wait, serve(&gw, req(http::Method::GET))).await;
    let (resp, _) = handled.expect("the head waited for an output");
    assert_eq!(resp.status(), StatusCode::OK);

    let mut body = resp.into_body();
    let mut next = async || {
        let frame = body.frame().await.and_then(|f| f.ok()?.into_data().ok());
        String::from_utf8_lossy(&frame.unwrap_or_default()).into_owned()
    };
    assert_eq!(next().await, ": open\n\n");
    let _ = send.unbounded_send(Ok(out("hi")));
    assert_eq!(next().await, "i:hi\n");
    drop(send);
    assert_eq!(next().await, "e\n");
}

/// Notes its end.
struct Ends(Log, &'static str);

impl Filter<LIn> for Ends {
    fn edit(&mut self, v: LIn, out: &mut Vec<LIn>) -> Result<(), Fault> {
        out.push(v);
        Ok(())
    }
    fn end(&mut self, _e: &Result<(), Fault>, _out: &mut Vec<LIn>) -> Result<(), Fault> {
        self.0.note(format!("end {}", self.1));
        Ok(())
    }
}

// An input filter added after a failed one still gets its end.
#[tokio::test]
async fn r8_1_an_input_filter_added_after_a_failed_one_still_gets_its_end() {
    struct Two(Log);

    #[async_trait]
    impl Plugin for Two {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let _ = cx.filter(|_: &mut LIn| Err(BadRequest::fault("first fails")));
            let _ = cx.filter(Ends(self.0.clone(), "second"));
            Err(BadRequest::fault("refused"))
        }
    }

    let log = Log::default();
    let gw = gateway(Script::default(), &log, Two(log.clone()));
    let (status, _) = run(&gw, req(http::Method::POST)).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(log.notes(), ["end second"]);
}

// A client-edge filter panicking before the head refuses the request
// internal and ends the call internal.
#[tokio::test]
async fn r8_3_a_panic_before_the_head_ends_the_call_internal_not_abandoned() {
    struct Boom;

    #[async_trait]
    impl Plugin for Boom {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            cx.filter(|_: &mut LOut| -> Result<(), Fault> { panic!("boom") })?;
            Ok(None)
        }
    }

    let log = Log::default();
    let script = Script::default().then(StatusCode::OK, vec!["i:a\n", "i:b\ne\n"]);
    let gw = gateway(script, &log, Boom);
    let (status, _) = run(&gw, req(http::Method::POST)).await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    let calls = log.calls();
    let code = calls[0]
        .as_ref()
        .err()
        .map(|f| respan_gateway_http::error_code(f).to_owned());
    assert_eq!(code.as_deref(), Some("internal"), "{:?}", calls[0]);
}

// A failed filter releases nothing at its end.
#[tokio::test]
async fn r8_a_failed_filter_releases_nothing_at_its_end() {
    struct Hold(Vec<LOut>);

    impl Filter<LOut> for Hold {
        fn edit(&mut self, v: LOut, out: &mut Vec<LOut>) -> Result<(), Fault> {
            match v.0.as_str() {
                "A bo" => self.0.push(v),
                "mb" => return Err(BadRequest::fault("blocked")),
                _ => out.push(v),
            }
            Ok(())
        }
        fn end(&mut self, _e: &Result<(), Fault>, out: &mut Vec<LOut>) -> Result<(), Fault> {
            out.append(&mut self.0);
            Ok(())
        }
    }

    struct Guard;

    #[async_trait]
    impl Plugin for Guard {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            cx.filter(Hold(Vec::new()))?;
            Ok(None)
        }
    }

    let log = Log::default();
    let script = Script::default().then(StatusCode::OK, vec!["i:ok\n", "i:A bo\n", "i:mb\ne\n"]);
    let gw = gateway(script, &log, Guard);
    let (status, body) = run(&gw, req(http::Method::POST)).await;
    assert_eq!(status, StatusCode::OK);
    let body = body.unwrap_or_default();
    assert_eq!(&body[..], b"i:ok\nx:blocked\n", "{body:?}");
}

// A whole request's input filter whose edit panics still gets its end.
#[tokio::test]
async fn r8_an_input_filter_whose_edit_panics_still_gets_its_end() {
    struct Panics(Log);

    impl Filter<LIn> for Panics {
        fn edit(&mut self, _v: LIn, _out: &mut Vec<LIn>) -> Result<(), Fault> {
            panic!("a buggy filter")
        }
        fn end(&mut self, _e: &Result<(), Fault>, _out: &mut Vec<LIn>) -> Result<(), Fault> {
            self.0.note("end");
            Ok(())
        }
    }

    struct P(Log);

    #[async_trait]
    impl Plugin for P {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            cx.filter(Panics(self.0.clone()))?;
            Ok(None)
        }
    }

    let log = Log::default();
    let gw = gateway(Script::default(), &log, P(log.clone()));
    let (status, _) = run(&gw, req(http::Method::POST)).await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(log.notes(), ["end"]);
}

/// Falls back from a call with no content to the next provider.
struct Fallback;

#[async_trait]
impl Plugin for Fallback {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let head = respan_gateway_http::ProviderHead::default();
        let mut call = cx.call("p").with_extension(head.clone());
        if let Some(wire) = cx.extensions().get::<respan_gateway_http::Wire>()
            && wire.get().is_some_and(|r| r.method() == http::Method::HEAD)
        {
            call = call.with_extension(respan_gateway_http::SendRaw(wire.clone()));
        }
        let mut resp = call.await?;
        if let Some(client) = cx.extensions().get::<respan_gateway_http::ClientHead>() {
            client.set(head.get());
        }

        match resp.buffer_until(|out: &LOut| !out.0.is_empty()).await {
            Ok(()) => Ok(Some(resp)),
            Err(_) => {
                drop(resp);
                cx.call("p").await.map(Some)
            }
        }
    }
}

// A 204 behind a fallback is the answer, not a failure to fall back from.
#[tokio::test]
async fn r8_until_content_returns_a_204_at_once() {
    let log = Log::default();
    let script = Script::default()
        .then(StatusCode::NO_CONTENT, vec![])
        .then(StatusCode::OK, vec!["i:backup\ne\n"]);
    let gw = gateway(script, &log, Fallback);
    let (status, _) = run(&gw, req(http::Method::POST)).await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    assert_eq!(log.calls().len(), 1);
}

/// And so is a response to `HEAD`.
#[tokio::test]
async fn r8_until_content_returns_a_response_to_head_at_once() {
    let log = Log::default();
    let script = Script::default()
        .then(StatusCode::OK, vec![])
        .then(StatusCode::OK, vec!["i:backup\ne\n"]);
    let gw = gateway(script, &log, Fallback);
    let (status, _) = run(&gw, req(http::Method::HEAD)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(log.calls().len(), 1);
}

/// Notes `name end <ok>` at its end; passes every value on.
struct Noting<T>(Log, &'static str, std::marker::PhantomData<fn(T)>);

impl<T> Clone for Noting<T> {
    fn clone(&self) -> Self {
        Noting(self.0.clone(), self.1, std::marker::PhantomData)
    }
}

fn noting<T>(log: &Log, name: &'static str) -> Noting<T> {
    Noting(log.clone(), name, std::marker::PhantomData)
}

impl<T: Send + 'static> Filter<T> for Noting<T> {
    fn edit(&mut self, v: T, out: &mut Vec<T>) -> Result<(), Fault> {
        out.push(v);
        Ok(())
    }
    fn end(&mut self, e: &Result<(), Fault>, _out: &mut Vec<T>) -> Result<(), Fault> {
        self.0.note(format!("{} end {}", self.1, e.is_ok()));
        Ok(())
    }
}

// A 204 read through `outputs` ends `Ok`, as sent or read by `until_content`.
#[tokio::test]
async fn r9_1_a_204_read_through_outputs_ends_ok() {
    struct Read(Log);

    #[async_trait]
    impl Plugin for Read {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let resp = cx.call("p").await?;
            let read: Vec<Result<LOut, Fault>> = resp.outputs::<LOut>().collect().await;
            self.0
                .note(format!("read {}", read.iter().all(Result::is_ok)));
            Ok(Some(Response::stream(futures::stream::iter([Ok(out(
                "x",
            ))]))))
        }
    }

    let log = Log::default();
    let script = Script::default().then(StatusCode::NO_CONTENT, vec![]);
    let gw = gateway(script, &log, Read(log.clone()));
    let _ = run(&gw, req(http::Method::POST)).await;
    assert_eq!(log.notes(), ["read true"]);
    assert!(log.calls()[0].is_ok());
}

// A call filter, added by a marked call's `on_provider`, before one of the
// wrong kind still gets its end.
#[tokio::test]
async fn r9_2_a_call_filter_before_a_failing_one_gets_its_end() {
    #[derive(Clone)]
    struct Mark;

    struct P(Log);

    #[async_trait]
    impl Plugin for P {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let failed = cx.call("p").with_extension(Mark).await.is_err();
            self.0.note(format!("failed {failed}"));
            Ok(Some(Response::stream(futures::stream::iter([Ok(out(
                "x",
            ))]))))
        }

        async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
            if px.extensions().get::<Mark>().is_some() {
                px.filter(noting::<LOut>(&self.0, "first"))?;
                px.filter(|_: &mut u32| Ok(()))?;
            }
            Ok(())
        }
    }

    let log = Log::default();
    let gw = gateway(Script::default(), &log, P(log.clone()));
    let _ = run(&gw, req(http::Method::POST)).await;
    let mut notes = log.notes();
    notes.sort();
    assert_eq!(notes, ["failed true", "first end false"]);
}

// Usage before the value a filter fails on, in its frame, is metered once.
#[tokio::test]
async fn r9_3_usage_before_the_value_a_filter_fails_on_is_metered_once() {
    struct P;

    #[async_trait]
    impl Plugin for P {
        async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
            px.filter(over_budget)
        }
    }

    let log = Log::default();
    let script = Script::default().then(StatusCode::OK, vec!["u:5\ni:hi\n", "e\n"]);
    let gw = gateway(script, &log, P);
    let _ = run(&gw, req(http::Method::POST)).await;
    assert_eq!(log.metered().iter().map(t).collect::<Vec<_>>(), [Some(5)]);
    assert!(log.calls()[0].is_err());
}

// The client's `Outcome` keeps the refusal's end though an output filter
// fails at close.
#[tokio::test]
async fn r9_4_a_filter_failing_at_close_does_not_rewrite_the_outcome() {
    struct P;

    #[async_trait]
    impl Plugin for P {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            struct Fails;
            impl Filter<LOut> for Fails {
                fn edit(&mut self, v: LOut, out: &mut Vec<LOut>) -> Result<(), Fault> {
                    out.push(v);
                    Ok(())
                }
                fn end(&mut self, _e: &Result<(), Fault>, _o: &mut Vec<LOut>) -> Result<(), Fault> {
                    Err(Fault::internal("filter end"))
                }
            }
            cx.filter(Fails)?;
            Err(BadRequest::fault("no"))
        }
    }

    let log = Log::default();
    let gw = gateway(Script::default(), &log, P);
    let (status, body) = run(&gw, req(http::Method::POST)).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(&body.unwrap_or_default()[..], b"x:no\n");
    let end = log.clients()[0].clone();
    assert_eq!(
        end.err().map(|f| f.message().to_owned()).as_deref(),
        Some("no")
    );
}

// A dropped call stays abandoned though a provider-edge filter fails at close.
#[tokio::test]
async fn r9_4b_an_abandoned_call_stays_abandoned() {
    struct P;

    #[async_trait]
    impl Plugin for P {
        async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
            struct Fails;
            impl Filter<LOut> for Fails {
                fn edit(&mut self, v: LOut, out: &mut Vec<LOut>) -> Result<(), Fault> {
                    out.push(v);
                    Ok(())
                }
                fn end(&mut self, _e: &Result<(), Fault>, _o: &mut Vec<LOut>) -> Result<(), Fault> {
                    Err(Fault::internal("filter end"))
                }
            }
            px.filter(Fails)
        }
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let mut response = cx.call("p").await?;
            response
                .buffer_until(|out: &LOut| !out.0.is_empty())
                .await?;
            drop(response);
            Ok(Some(Response::stream(futures::stream::iter([Ok(out(
                "x",
            ))]))))
        }
    }

    let log = Log::default();
    let script = Script::default().then(StatusCode::OK, vec!["i:hi\n", "e\n"]);
    let gw = gateway(script, &log, P);
    let _ = run(&gw, req(http::Method::POST)).await;
    let end = log.calls()[0].clone();
    assert_eq!(
        end.err()
            .map(|f| respan_gateway_http::error_code(&f).to_owned())
            .as_deref(),
        Some("abandoned")
    );
}

// At close, a panicking output filter's end skips no other filter's end.
#[tokio::test]
async fn r9_5_a_panicking_output_filter_end_at_close_skips_no_other_filter() {
    struct P(Log);

    #[async_trait]
    impl Plugin for P {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            struct Panics;
            impl Filter<LOut> for Panics {
                fn edit(&mut self, v: LOut, out: &mut Vec<LOut>) -> Result<(), Fault> {
                    out.push(v);
                    Ok(())
                }
                fn end(&mut self, _e: &Result<(), Fault>, _o: &mut Vec<LOut>) -> Result<(), Fault> {
                    panic!("a buggy filter")
                }
            }
            // Applied the last added first: Panics, then the noting one.
            cx.filter(noting::<LOut>(&self.0, "outputs"))?;
            cx.filter(Panics)?;
            Err(BadRequest::fault("no"))
        }
    }

    let log = Log::default();
    let gw = gateway(Script::default(), &log, P(log.clone()));
    let _ = run(&gw, req(http::Method::POST)).await;
    assert_eq!(log.notes(), ["outputs end false"]);
}

// An input filter panicking at the request's close skips no output filter's end.
#[tokio::test]
async fn r9_5b_a_panicking_input_filter_end_skips_no_output_filter() {
    struct P(Log);

    #[async_trait]
    impl Plugin for P {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            struct Panics;
            impl Filter<LIn> for Panics {
                fn edit(&mut self, v: LIn, out: &mut Vec<LIn>) -> Result<(), Fault> {
                    out.push(v);
                    Ok(())
                }
                fn end(&mut self, _e: &Result<(), Fault>, _o: &mut Vec<LIn>) -> Result<(), Fault> {
                    panic!("a buggy filter")
                }
            }
            cx.filter(Panics)?;
            cx.filter(noting::<LOut>(&self.0, "outputs"))?;
            Ok(Some(Response::stream(futures::stream::pending::<
                Result<LOut, Fault>,
            >())))
        }
    }

    let log = Log::default();
    let gw = gateway(Script::default(), &log, P(log.clone()));
    let (_send, frames) = futures::channel::mpsc::unbounded::<Bytes>();
    let body = Body::stream(frames.map(Ok));
    let mut r = http::Request::new(body);
    *r.uri_mut() = http::Uri::from_static("/line");
    r.headers_mut().insert(
        http::header::UPGRADE,
        http::HeaderValue::from_static("websocket"),
    );
    let (resp, ended) = serve_live(&gw, r).await;
    drop(resp); // the client leaves
    let _ = ended.await;
    gw.idle().await;
    assert_eq!(log.notes(), ["outputs end false"]);
}

// Values a filter releases are sent though a later filter's end fails.
#[tokio::test]
async fn r9_6_released_values_are_sent_though_a_later_filters_end_fails() {
    struct Hold(Vec<LOut>);
    impl Filter<LOut> for Hold {
        fn edit(&mut self, v: LOut, out: &mut Vec<LOut>) -> Result<(), Fault> {
            match v.0.as_str() {
                "kept" => self.0.push(v),
                _ => out.push(v),
            }
            Ok(())
        }
        fn end(&mut self, _e: &Result<(), Fault>, out: &mut Vec<LOut>) -> Result<(), Fault> {
            out.append(&mut self.0);
            Ok(())
        }
    }
    struct EndFails;
    impl Filter<LOut> for EndFails {
        fn edit(&mut self, v: LOut, out: &mut Vec<LOut>) -> Result<(), Fault> {
            out.push(v);
            Ok(())
        }
        fn end(&mut self, _e: &Result<(), Fault>, _o: &mut Vec<LOut>) -> Result<(), Fault> {
            Err(Fault::internal("end fails"))
        }
    }
    struct P;

    #[async_trait]
    impl Plugin for P {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            // Applied the last added first: Hold, then EndFails.
            cx.filter(EndFails)?;
            cx.filter(Hold(Vec::new()))?;
            Ok(None)
        }
    }

    let log = Log::default();
    let script = Script::default().then(StatusCode::OK, vec!["i:a\n", "i:kept\ne\n"]);
    let gw = gateway(script, &log, P);
    let (_, body) = run(&gw, req(http::Method::POST)).await;
    let body = body.unwrap_or_default();
    assert!(
        body.starts_with(b": open") || body.starts_with(b"i:"),
        "{body:?}"
    );
    assert!(
        String::from_utf8_lossy(&body).contains("i:kept"),
        "{body:?}"
    );
}

/// A filter over budget at the line `hi`.
fn over_budget(o: &mut LOut) -> Result<(), Fault> {
    match o.0 == "hi" {
        true => Err(Fault::new("over budget")
            .with(ErrorCode("budget".into()))
            .with(Status(StatusCode::PAYMENT_REQUIRED))),
        false => Ok(()),
    }
}

// A call filter's failure is written, and what follows it in its frame is
// not, nor does it reach any filter.
#[tokio::test]
async fn k9_7_what_follows_a_call_filters_failure_is_unwritten() {
    struct P;

    #[async_trait]
    impl Plugin for P {
        async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
            px.filter(over_budget)
        }
    }

    let log = Log::default();
    let frames = vec!["i:ok\n", "i:hi\nu:5\n", "e\n"];
    let script = Script::default().then(StatusCode::OK, frames);
    let gw = gateway(script, &log, P);
    let (_, body) = run(&gw, req(http::Method::POST)).await;
    assert_eq!(&body.unwrap_or_default()[..], b"i:ok\nx:over budget\n");
    assert_eq!(t(&log.metered()[0]), None);
}

// A client filter's failure stops a plugin's own outputs; they pass no
// provider edge, so no meter sees them.
#[tokio::test]
async fn k9_7_a_client_filter_fails_a_plugins_own_outputs_which_count_nothing() {
    struct P;

    #[async_trait]
    impl Plugin for P {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            cx.filter(over_budget)?;
            let mut usage = out("");
            usage.1.output_tokens += 5;
            let own = [out("ok"), out("hi"), usage].map(Ok::<_, Fault>);
            Ok(Some(Response::stream(futures::stream::iter(own))))
        }
    }

    let log = Log::default();
    let gw = gateway(Script::default(), &log, P);
    let (_, body) = run(&gw, req(http::Method::POST)).await;
    assert_eq!(&body.unwrap_or_default()[..], b"i:ok\nx:over budget\n");
    assert!(log.metered().is_empty());
}

/// Line's kind under another name, so calls to it translate: it puts the
/// model, the input, in the call's path, and cannot say "untranslatable".
#[derive(Clone)]
struct Pathed;

impl respan_gateway_core::Format<respan_gateway_http::ProviderIo, LOut, LIn> for Pathed {
    fn name(&self) -> &str {
        "pathed"
    }
    fn bind(
        &self,
        io: respan_gateway_http::ProviderIo,
    ) -> Result<respan_gateway_core::Connection<LOut, LIn>, Fault> {
        Ok(io.bind(
            move |head: &mut RequestHead, input: Option<&LIn>| {
                // A model of several words is escaped: its head is written
                // before its body says whether it can be.
                let model = input.map_or("none", |input| input.0.as_str());
                let model = model.replace(' ', "%20");
                head.method = http::Method::POST;
                respan_gateway_http::formats::set_path(head, &format!("/models/{model}"))?;
                Ok(Box::new(Untranslatable) as Box<dyn Encoder<LIn>>)
            },
            line_outputs,
            line_refused,
        ))
    }
}

/// Writes as Line does, but cannot say "untranslatable".
struct Untranslatable;

impl Encoder<LIn> for Untranslatable {
    fn encode(&mut self, input: &LIn, out: &mut BytesMut) -> Result<(), Fault> {
        match input.0.contains("untranslatable") {
            true => Err(BadRequest::fault("pathed cannot say it")),
            false => Lines.encode(input, out),
        }
    }

    fn end(&mut self, end: &Result<(), Fault>, out: &mut BytesMut) -> Result<(), Fault> {
        Lines.end(end, out)
    }
}

/// Answers every call, noting its path.
#[derive(Clone, Default)]
struct Paths(Shared<String>);

impl Paths {
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
        lock(&self.0).push(call.uri().path().to_owned());
        let body = futures::stream::iter([Ok(Bytes::from_static(b"i:ok\ne\n"))]);
        Ok((http::Response::new(Body::stream(body)), None))
    }
}
impl Paths {
    fn into_sender(self) -> impl Clone + Fn(http::Request<Body>) -> SendFuture {
        move |call| {
            let value = self.clone();
            Box::pin(async move { value.send(call).await })
        }
    }
}

/// Sets the input to its text, if any.
struct Model(Option<&'static str>);

#[async_trait]
impl Plugin for Model {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        if let Some(model) = self.0 {
            cx.filter(move |input: &mut LIn| {
                input.0 = model.to_owned();
                Ok(())
            })?;
        }
        Ok(None)
    }
}

fn pathed(paths: &Paths, model: Option<&'static str>) -> Gateway {
    Gateway::new()
        .provider::<LIn, _, _>(
            "pathed",
            Pathed,
            respan_gateway_http::connect(paths.clone().into_sender(), LIMIT),
        )
        .and_then(|g| g.client::<LIn, _, _>(Line))
        .and_then(|g| g.target("pathed"))
        .map(|g| g.plugin(Model(model)))
        .unwrap_or_else(|e| panic!("{e}"))
}

fn line_req(body: &'static str) -> http::Request<Body> {
    let body = Body::full(Bytes::from_static(body.as_bytes()));
    let mut r = http::Request::new(body);
    *r.method_mut() = http::Method::POST;
    *r.uri_mut() = http::Uri::from_static("/line");
    r
}

// The request encoder reads the model from `first`, so a format can put it
// in the call's path.
#[tokio::test]
async fn k9_8_a_format_writes_the_model_into_the_calls_path() {
    let paths = Paths::default();
    let (status, _) = run(&pathed(&paths, None), line_req("m1")).await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = run(&pathed(&paths, Some("m2")), line_req("m1")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(lock(&paths.0).clone(), ["/models/m1", "/models/m2"]);
}

// Client content the provider's format cannot say is a bad request, a 400,
// changed by a filter or not: the gateway cannot tell.
#[tokio::test]
async fn v9_1_untranslatable_content_is_a_bad_request_edited_or_not() {
    let paths = Paths::default();
    let (status, _) = run(&pathed(&paths, Some("m2 untranslatable")), line_req("m1")).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let (status, _) = run(&pathed(&paths, None), line_req("untranslatable")).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    /// Changes the input, keeping the client's content.
    struct Tag;

    #[async_trait]
    impl Plugin for Tag {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            cx.filter(|input: &mut LIn| {
                input.0.push_str(" m2");
                Ok(())
            })?;
            Ok(None)
        }
    }

    let gw = Gateway::new()
        .provider::<LIn, _, _>(
            "pathed",
            Pathed,
            respan_gateway_http::connect(paths.clone().into_sender(), LIMIT),
        )
        .and_then(|g| g.client::<LIn, _, _>(Line))
        .and_then(|g| g.target("pathed"))
        .map(|g| g.plugin(Tag))
        .unwrap_or_else(|e| panic!("{e}"));
    let (status, _) = run(&gw, line_req("untranslatable")).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(lock(&paths.0).is_empty());
}

fn upgrading(frames: Vec<Result<&'static str, Fault>>) -> http::Request<Body> {
    let frames = futures::stream::iter(frames).map(|f| f.map(|f| Bytes::from_static(f.as_bytes())));
    let mut r = http::Request::new(Body::stream(frames));
    *r.uri_mut() = http::Uri::from_static("/line");
    r.headers_mut().insert(
        http::header::UPGRADE,
        http::HeaderValue::from_static("websocket"),
    );
    r
}

fn own() -> Response {
    Response::stream(futures::stream::iter([Ok(out("x"))]))
}

// An input filter added after an upgrading request's body broke gets the
// inputs' real end.
#[tokio::test]
async fn r10_1_a_late_input_filter_gets_the_inputs_real_end() {
    struct Drain(Log);

    #[async_trait]
    impl Plugin for Drain {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let read: Vec<Result<LIn, Fault>> = cx.take_inputs::<LIn>().collect().await;
            self.0.note(format!("read {}", read.len()));
            let added = cx.filter(noting::<LIn>(&self.0, "late"));
            self.0.note(format!("added {}", added.is_ok()));
            Ok(Some(own()))
        }
    }

    let log = Log::default();
    let gw = gateway(Script::default(), &log, Drain(log.clone()));
    let r = upgrading(vec![
        Ok("a"),
        Err(Upstream::fault("the client's body broke")),
    ]);
    let (resp, ended) = serve_live(&gw, r).await;
    let _ = BodyExt::collect(resp.into_body()).await;
    let _ = ended.await;
    gw.idle().await;
    // Like a whole request whose input failed: the filter's end is told
    // the failure, and adding it returns it.
    assert_eq!(log.notes(), ["read 2", "late end false", "added false"]);
}

// A 204 dropped after `until_content` ends `Ok`, as its end is decoded.
#[tokio::test]
async fn r10_2_a_204_dropped_after_until_content_ends_ok() {
    struct DropIt;

    #[async_trait]
    impl Plugin for DropIt {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let mut response = cx.call("p").await?;
            response
                .buffer_until(|out: &LOut| !out.0.is_empty())
                .await?;
            drop(response);
            Ok(Some(own()))
        }
    }

    let log = Log::default();
    let script = Script::default()
        .then(StatusCode::OK, vec!["i:hi\ne\n"])
        .then(StatusCode::NO_CONTENT, vec![]);
    let gw = gateway(script, &log, DropIt);
    let _ = run(&gw, req(http::Method::POST)).await;
    let _ = run(&gw, req(http::Method::POST)).await;
    // The 200's end is its body's, which `until_content` did not reach; the 204's
    // comes with its head.
    let ends = log.calls();
    let code = |e: &Result<(), Fault>| {
        e.as_ref()
            .err()
            .map(|f| respan_gateway_http::error_code(f).to_owned())
    };
    assert_eq!(code(&ends[0]).as_deref(), Some("abandoned"));
    assert!(ends[1].is_ok());
}

// An output filter panicking at close leaves the refusal's end.
#[tokio::test]
async fn r10_3_a_panic_while_closing_ends_the_edge_internal() {
    struct P;

    #[async_trait]
    impl Plugin for P {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            struct Panics;
            impl Filter<LOut> for Panics {
                fn edit(&mut self, v: LOut, out: &mut Vec<LOut>) -> Result<(), Fault> {
                    out.push(v);
                    Ok(())
                }
                fn end(&mut self, _e: &Result<(), Fault>, _o: &mut Vec<LOut>) -> Result<(), Fault> {
                    panic!("a buggy filter")
                }
            }
            cx.filter(Panics)?;
            Err(BadRequest::fault("no"))
        }
    }

    let log = Log::default();
    let gw = gateway(Script::default(), &log, P);
    let _ = run(&gw, req(http::Method::POST)).await;
    let end = log.clients()[0].clone();
    // The end is known at close, so a panic changes it no more than a failure.
    assert_eq!(
        end.err()
            .map(|f| respan_gateway_http::error_code(&f).to_owned())
            .as_deref(),
        Some("bad_request")
    );
}

/// Line's kind under another name, whose calls are always a `POST`, as
/// OpenAI's and Anthropic's are.
#[derive(Clone)]
struct Posted;

impl respan_gateway_core::Format<respan_gateway_http::ProviderIo, LOut, LIn> for Posted {
    fn name(&self) -> &str {
        "posted"
    }
    fn bind(
        &self,
        io: respan_gateway_http::ProviderIo,
    ) -> Result<respan_gateway_core::Connection<LOut, LIn>, Fault> {
        Ok(io.bind(
            move |head: &mut RequestHead, input: Option<&LIn>| {
                head.method = http::Method::POST;
                line_call(head, input)
            },
            line_outputs,
            line_refused,
        ))
    }
}

// `until_content` judges the response as its provider sent it, the call's method,
// not the client's `HEAD`.
#[tokio::test]
async fn r10_4_until_content_judges_a_head_as_the_provider_sent_it() {
    struct Fallback(Log);

    #[async_trait]
    impl Plugin for Fallback {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let mut read = cx.call("posted").await?;
            let held = read.buffer_until(|out: &LOut| !out.0.is_empty()).await;
            self.0.note(format!("content {}", held.is_ok()));
            drop(read);
            Ok(Some(own()))
        }
    }

    let log = Log::default();
    let script = Script::default().then(StatusCode::OK, vec!["x:boom\n"]);
    let gw = Gateway::new()
        .provider::<LIn, _, _>(
            "posted",
            Posted,
            respan_gateway_http::connect(script.clone().into_sender(), LIMIT),
        )
        .and_then(|g| g.client::<LIn, _, _>(Line))
        .map(|g| g.plugin(log.clone()))
        .map(|g| g.plugin(Fallback(log.clone())))
        .unwrap_or_else(|e| panic!("{e}"));
    let _ = run(&gw, req(http::Method::HEAD)).await;
    assert_eq!(log.notes(), ["content false"]);
}

/// Line under its own name, so calls to it relay, noting whether its
/// request encoder got a first input.
#[derive(Clone)]
struct Seen(Shared<bool>);

impl respan_gateway_core::Format<respan_gateway_http::ProviderIo, LOut, LIn> for Seen {
    fn name(&self) -> &str {
        "line"
    }
    fn bind(
        &self,
        io: respan_gateway_http::ProviderIo,
    ) -> Result<respan_gateway_core::Connection<LOut, LIn>, Fault> {
        let seen = self.0.clone();
        Ok(io.bind(
            move |head: &mut RequestHead, input: Option<&LIn>| {
                lock(&seen).push(input.is_some());
                line_call(head, input)
            },
            line_outputs,
            line_refused,
        ))
    }
}

// A call's first input is always given as a value, filtered or not.
#[tokio::test]
async fn r10_6_first_is_always_a_value() {
    /// Reads the input, then sets it to `.0`, if any.
    struct Touch(Option<&'static str>);

    #[async_trait]
    impl Plugin for Touch {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            cx.input::<LIn>()?;
            if let Some(to) = self.0 {
                cx.filter(move |input: &mut LIn| {
                    input.0 = to.to_owned();
                    Ok(())
                })?;
            }
            Ok(None)
        }
    }

    let seen = Shared::default();
    for to in [None, Some("hi"), Some("m2")] {
        let script = Script::default().then(StatusCode::OK, vec!["i:ok\ne\n"]);
        let gw = Gateway::new()
            .provider::<LIn, _, _>(
                "p",
                Seen(seen.clone()),
                respan_gateway_http::connect(script.into_sender(), LIMIT),
            )
            .and_then(|g| g.client::<LIn, _, _>(Line))
            .and_then(|g| g.target("p"))
            .map(|g| g.plugin(Touch(to)))
            .unwrap_or_else(|e| panic!("{e}"));
        let (status, _) = run(&gw, req(http::Method::POST)).await;
        assert_eq!(status, StatusCode::OK);
    }
    // Read only, or filtered even to what it was: a value.
    assert_eq!(lock(&seen).clone(), [true, true, true]);
}

// Usage before content does not send the head, so a success that then fails
// is refused.
#[tokio::test]
async fn r11_4_usage_before_content_sends_the_head_of_a_failed_success() {
    let log = Log::default();
    let script = Script::default().then(StatusCode::OK, vec!["u:3\n", "x:boom\n"]);
    let gw = gateway(script, &log, Log::default());
    let (status, _) = run(&gw, req(http::Method::POST)).await;
    assert_eq!(status, StatusCode::BAD_GATEWAY);
}

// A plugin's answer to an upgrading request sends its head at once, so a
// bridge does not wait on itself.
#[tokio::test]
async fn r11_5_a_plugins_answer_to_an_upgrading_request_heads_out_at_once() {
    struct Echoes;

    #[async_trait]
    impl Plugin for Echoes {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            use futures::TryStreamExt;
            let echoed = cx.take_inputs::<LIn>().map_ok(|i| out(i.0));
            Ok(Some(Response::stream(echoed)))
        }
    }

    let log = Log::default();
    let gw = gateway(Script::default(), &log, Echoes);
    let (_send, body) = futures::channel::mpsc::unbounded::<Result<Bytes, Fault>>();
    let mut r = http::Request::new(Body::stream(body));
    *r.uri_mut() = http::Uri::from_static("/line");
    // Not a GET, so this format writes no opening bytes.
    *r.method_mut() = http::Method::POST;
    r.headers_mut().insert(
        http::header::UPGRADE,
        http::HeaderValue::from_static("websocket"),
    );
    let headed = tokio::time::timeout(std::time::Duration::from_secs(1), serve_live(&gw, r)).await;
    assert!(
        headed.is_ok(),
        "the head waits for the client, which waits for it"
    );
}

// A client `HEAD` whose call is a `POST` gets no body, and the call's body is
// not read: it ends abandoned as the head goes out.
#[tokio::test]
async fn r11_a_heads_post_call_ends_abandoned_unread() {
    let log = Log::default();
    let script = Script::default().then(StatusCode::OK, vec!["i:hi\n", "u:5\ne\n"]);
    let gw = Gateway::new()
        .provider::<LIn, _, _>(
            "posted",
            Posted,
            respan_gateway_http::connect(script.into_sender(), LIMIT),
        )
        .and_then(|g| g.client::<LIn, _, _>(Line))
        .and_then(|g| g.target("posted"))
        .map(|g| g.plugin(log.clone()))
        .map(|g| g.plugin(Meter(log.clone())))
        .unwrap_or_else(|e| panic!("{e}"));
    let (status, body) = run(&gw, req(http::Method::HEAD)).await;
    assert_eq!((status, body.ok()), (StatusCode::OK, Some(Bytes::new())));
    let call = &log.calls()[0];
    let end = call
        .clone()
        .map_err(|f| respan_gateway_http::error_code(&f).to_owned());
    assert_eq!(
        (t(&log.metered()[0]), end),
        (None, Err("abandoned".to_owned()))
    );
}

/// Line's kind, as a format that reads the first input for its head, as a
/// model-in-path format does; notes whether it got a first input, and its
/// text.
#[derive(Clone)]
struct InPath(Shared<(bool, String)>);

impl respan_gateway_core::Format<respan_gateway_http::ProviderIo, LOut, LIn> for InPath {
    fn name(&self) -> &str {
        "line"
    }
    fn bind(
        &self,
        io: respan_gateway_http::ProviderIo,
    ) -> Result<respan_gateway_core::Connection<LOut, LIn>, Fault> {
        let seen = self.0.clone();
        Ok(io.bind(
            move |head: &mut RequestHead, input: Option<&LIn>| {
                let first = input.map(|first| first.0.clone());
                lock(&seen).push((first.is_some(), first.unwrap_or_default()));
                line_call(head, input)
            },
            line_outputs,
            line_refused,
        ))
    }
}

// The first input a format reads for its head is the changed one.
#[tokio::test]
async fn r11_first_is_the_changed_input() {
    struct Change;

    #[async_trait]
    impl Plugin for Change {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            cx.filter(|input: &mut LIn| {
                input.0 = "m2".to_owned();
                Ok(())
            })?;
            Ok(None)
        }
    }

    let seen = Shared::default();
    let script = Script::default().then(StatusCode::OK, vec!["i:ok\ne\n"]);
    let gw = Gateway::new()
        .provider::<LIn, _, _>(
            "p",
            InPath(seen.clone()),
            respan_gateway_http::connect(script.into_sender(), LIMIT),
        )
        .and_then(|g| g.client::<LIn, _, _>(Line))
        .and_then(|g| g.target("p"))
        .map(|g| g.plugin(Change))
        .unwrap_or_else(|e| panic!("{e}"));
    let (status, _) = run(&gw, req(http::Method::POST)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(lock(&seen).clone(), [(true, "m2".to_owned())]);
}

/// Adds a provider filter that panics on every output.
struct PanicsOnOutput;

#[async_trait]
impl Plugin for PanicsOnOutput {
    async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
        px.filter(|_: &mut LOut| -> Result<(), Fault> { panic!("a buggy filter") })
    }
}

// A filter that panics fails the call internal, and the usage after the
// value it panics on, in its frame, reaches no filter.
#[tokio::test]
async fn r11_a_filter_that_panics_fails_the_call_internal() {
    let log = Log::default();
    let script = Script::default().then(StatusCode::OK, vec!["i:hi\nu:5\ne\n"]);
    let gw = gateway(script, &log, PanicsOnOutput);
    let _ = run(&gw, req(http::Method::POST)).await;
    let call = &log.calls()[0];
    let internal = call
        .as_ref()
        .is_err_and(|f| respan_gateway_http::error_code(f) == "internal");
    assert_eq!((t(&log.metered()[0]), internal), (None, true));
}

use respan_gateway_http::relay as explicit;
