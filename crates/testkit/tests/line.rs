//! The kernel's promises on ends and filters, probed with a scripted line
//! format whose decoder turns each line into one value. The kernel counts
//! no usage: [`Meter`], registered last, sums what lines report through a
//! provider-edge filter, so these tests show what such a filter sees.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use respan_gateway_formats::relayed::Relayed;

use bytes::{Bytes, BytesMut};
use futures::StreamExt;
use http::StatusCode;
use http_body::Frame;
use http_body_util::{BodyExt, StreamBody};
use respan_gateway_core::futures;
use respan_gateway_core::{
    Call, ClientContext, Fault, Filter, Gateway, Plugin, ProviderContext, Response, async_trait,
};
use respan_gateway_formats::Usage;
use respan_gateway_http::Body;
use respan_gateway_http::Status;
use respan_gateway_http::bodiless;
use respan_gateway_http::bytes;
use respan_gateway_http::{Codec, Decoder, Encoder, Exchange, Transport, impl_format};
use respan_gateway_http::{http, http_body, http_body_util};
use respan_gateway_testkit::serve;

/// What a client sends: one message, which keeps the bytes it came in.
#[derive(Debug, Clone, PartialEq)]
struct LIn(String, Relayed<LIn>);

/// What a line says: text, usage, or no content, with its bytes.
#[derive(Debug, Clone, PartialEq)]
struct LOut(String, Usage, Relayed<LOut>);

/// A line a plugin says.
fn out(text: impl Into<String>) -> LOut {
    LOut(text.into(), Usage::default(), Relayed::default())
}

const LINE: &str = "line";

/// Lines: `i:X` text, `u:N` usage of N `t`, `e` the end, `x:M` a failure;
/// any other line is no content. Each line is one value; a body ends at its
/// end, complete only after `e`.
#[derive(Clone)]
struct Line;

impl Codec for Line {
    type Input = LIn;
    type Output = LOut;

    fn name(&self) -> &str {
        LINE
    }

    fn accepts(&self, head: &respan_gateway_http::RequestHead) -> bool {
        head.uri.path().starts_with("/line")
    }

    fn request_decoder(
        &self,
        _request: &respan_gateway_http::RequestHead,
    ) -> Box<dyn Decoder<LIn>> {
        let name: Arc<str> = Arc::from(LINE);
        respan_gateway_formats::message_decoder(move |raw: Bytes| {
            let text = String::from_utf8_lossy(&raw).into_owned();
            let said = LIn(text.clone(), Relayed::default());
            Ok(LIn(text, Relayed::came(&name, raw, said)))
        })
    }

    fn request_encoder(
        &self,
        client: Option<&respan_gateway_http::RequestHead>,
        _body: &mut Bytes,
        _streams: bool,
    ) -> Box<dyn Encoder<LIn, respan_gateway_http::RequestHead>> {
        respan_gateway_formats::with_head(line_head(client), Box::new(Lines))
    }

    /// Nothing for a success HTTP gives no body, as the standard formats.
    fn response_decoder(
        &self,
        head: &respan_gateway_http::ResponseHead,
        request: &respan_gateway_http::RequestHead,
        _sent: &Bytes,
    ) -> Box<dyn Decoder<LOut>> {
        if head.status.is_success() && bodiless(&request.method, head.status) {
            return respan_gateway_formats::whole::nothing();
        }
        Box::new(LineDecoder::default())
    }

    fn response_encoder(
        &self,
        answer: Option<&respan_gateway_http::ResponseHead>,
        request: &respan_gateway_http::RequestHead,
        _body: &Bytes,
    ) -> Box<dyn Encoder<LOut, respan_gateway_http::ResponseHead>> {
        let head = respan_gateway_formats::response_head(answer);
        if bodiless(&request.method, head.status) {
            let body = respan_gateway_formats::whole::no_body();
            return respan_gateway_formats::with_head(head, body);
        }
        let body = LineEncoder(request.method == http::Method::GET, None);
        respan_gateway_formats::with_head(head, Box::new(body))
    }

    /// A refusal under its fault's status.
    fn refusal(
        &self,
        fault: &Fault,
        _: &Bytes,
        _: &mut respan_gateway_http::ResponseHead,
    ) -> Bytes {
        Bytes::from(format!("x:{}\n", fault.message()))
    }

    fn is_content(&self, output: &LOut) -> bool {
        !output.0.is_empty()
    }
}

impl_format!(Line);

/// A call's head as Line writes it: the client's, or a fresh one, at `/line`.
fn line_head(
    client: Option<&respan_gateway_http::RequestHead>,
) -> respan_gateway_http::RequestHead {
    let mut head = respan_gateway_formats::call_head(client);
    respan_gateway_formats::set_path(&mut head, "/line");
    head
}

/// Line's call encoder with `edit` run on its head, which sees the first
/// input.
struct EditHead<F>(
    Box<dyn Encoder<LIn, respan_gateway_http::RequestHead>>,
    Option<F>,
);

impl<F> Encoder<LIn, respan_gateway_http::RequestHead> for EditHead<F>
where
    F: FnOnce(&mut respan_gateway_http::RequestHead, Option<&LIn>) + Send + 'static,
{
    fn head(&mut self, first: Option<&LIn>) -> respan_gateway_http::RequestHead {
        let mut head = self.0.head(first);
        if let Some(edit) = self.1.take() {
            edit(&mut head, first);
        }
        head
    }

    fn open(&mut self, out: &mut BytesMut) -> Result<bool, Fault> {
        self.0.open(out)
    }

    fn encode(&mut self, input: &LIn, out: &mut BytesMut) -> Result<(), Fault> {
        self.0.encode(input, out)
    }

    fn end(&mut self, end: &Result<(), Fault>, out: &mut BytesMut) -> Result<(), Fault> {
        self.0.end(end, out)
    }
}

/// The members a format of Line's kind takes from Line as they are.
macro_rules! as_line {
    () => {
        type Input = LIn;
        type Output = LOut;

        fn request_decoder(
            &self,
            request: &respan_gateway_http::RequestHead,
        ) -> Box<dyn Decoder<LIn>> {
            Line.request_decoder(request)
        }

        fn response_decoder(
            &self,
            head: &respan_gateway_http::ResponseHead,
            request: &respan_gateway_http::RequestHead,
            sent: &Bytes,
        ) -> Box<dyn Decoder<LOut>> {
            Line.response_decoder(head, request, sent)
        }

        fn response_encoder(
            &self,
            answer: Option<&respan_gateway_http::ResponseHead>,
            request: &respan_gateway_http::RequestHead,
            body: &Bytes,
        ) -> Box<dyn Encoder<LOut, respan_gateway_http::ResponseHead>> {
            Line.response_encoder(answer, request, body)
        }

        fn refusal(
            &self,
            fault: &Fault,
            body: &Bytes,
            head: &mut respan_gateway_http::ResponseHead,
        ) -> Bytes {
            Line.refusal(fault, body, head)
        }

        fn is_content(&self, output: &LOut) -> bool {
            Line.is_content(output)
        }
    };
}

/// Writes each input as it came while unchanged, else its text.
struct Lines;

impl Encoder<LIn, ()> for Lines {
    fn head(&mut self, _first: Option<&LIn>) {}

    fn encode(&mut self, input: &LIn, out: &mut BytesMut) -> Result<(), Fault> {
        match input.1.bytes(LINE, input) {
            Some(raw) => out.extend_from_slice(raw),
            None => out.extend_from_slice(input.0.as_bytes()),
        }
        Ok(())
    }

    fn end(&mut self, _end: &Result<(), Fault>, _out: &mut BytesMut) -> Result<(), Fault> {
        Ok(())
    }
}

/// Whether it decoded any line, and whether `e` came.
#[derive(Default)]
struct LineDecoder(bool, bool);

impl Decoder<LOut> for LineDecoder {
    fn decode(&mut self, bytes: Bytes, output: &mut Vec<LOut>) -> Result<(), Fault> {
        let name: Arc<str> = Arc::from(LINE);
        for line in String::from_utf8_lossy(&bytes).lines() {
            self.0 = true;
            if self.1 {
                continue;
            }
            let raw = Bytes::from(format!("{line}\n"));
            let said = match line.split_once(':') {
                Some(("i", x)) => out(x),
                Some(("u", n)) => {
                    let mut u = Usage::default();
                    u.add("t", n.parse().unwrap_or(0.0));
                    LOut(String::new(), u, Relayed::default())
                }
                Some(("x", m)) => return Err(Fault::upstream(m)),
                _ if line == "e" => {
                    self.1 = true;
                    continue;
                }
                _ => out(""),
            };
            let came = Relayed::came(&name, raw, said.clone());
            output.push(LOut(said.0, said.1, came));
        }
        Ok(())
    }

    /// Like a JSON format, an empty body is not a response of this format.
    fn end(&mut self, _out: &mut Vec<LOut>) -> Result<(), Fault> {
        match (self.0, self.1) {
            (false, _) => Err(Fault::upstream("not a line body")),
            (true, false) => Err(Fault::truncated()),
            (true, true) => Ok(()),
        }
    }
}

/// Opens a `GET`'s body with an SSE comment; holds usage for the next text
/// or the end, so a usage line never sends the head before content.
struct LineEncoder(bool, Option<Usage>);

impl LineEncoder {
    fn release(&mut self, out: &mut BytesMut) {
        if let Some(u) = self.1.take() {
            let n = u.get("t").unwrap_or(0.0);
            out.extend_from_slice(format!("u:{n}\n").as_bytes());
        }
    }
}

impl Encoder<LOut, ()> for LineEncoder {
    fn head(&mut self, _first: Option<&LOut>) {}

    fn open(&mut self, out: &mut BytesMut) -> Result<bool, Fault> {
        if self.0 {
            out.extend_from_slice(respan_gateway_formats::sse::comment("open").as_bytes());
        }
        Ok(false)
    }

    fn encode(&mut self, o: &LOut, out: &mut BytesMut) -> Result<(), Fault> {
        if !o.1.is_empty() {
            *self.1.get_or_insert_default() += &o.1;
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

impl Script {
    fn then(self, status: StatusCode, frames: Vec<&'static str>) -> Self {
        lock(&self.0).push_back((status, frames));
        self
    }
}

#[async_trait]
impl Transport for Script {
    async fn send(
        &self,
        _call: Exchange<respan_gateway_http::RequestHead>,
    ) -> Result<Exchange<respan_gateway_http::ResponseHead>, Fault> {
        let (status, frames) = lock(&self.0).pop_front().expect("scripted");
        let frames =
            futures::stream::iter(frames).map(|f| Ok::<_, Fault>(Bytes::from_static(f.as_bytes())));
        let mut head = respan_gateway_http::fresh_response_head();
        head.status = status;
        Ok(Exchange::new(head, frames.boxed()))
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
            let usage = lock(usage).clone();
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
        .provider("p", Line, script)
        .and_then(|g| g.serve(Line))
        .and_then(|g| g.target("p"))
        .and_then(|g| g.plugin("log", log.clone()))
        .and_then(|g| g.plugin("x", plugin))
        .and_then(|g| g.plugin("meter", Meter(log.clone())))
        .unwrap_or_else(|e| panic!("{e}"))
}

fn req(method: http::Method) -> http::Request<Body> {
    let body = http_body_util::Full::new(Bytes::from_static(b"hi"))
        .map_err(Fault::from)
        .boxed_unsync();
    let mut r = http::Request::new(body);
    *r.method_mut() = method;
    *r.uri_mut() = http::Uri::from_static("/line");
    r
}

async fn run(gw: &Gateway, r: http::Request<Body>) -> (StatusCode, Result<Bytes, Fault>) {
    let (resp, ended) = serve(gw, r).await;
    let status = resp.status();
    let body = resp.into_body().collect().await.map(|b| b.to_bytes());
    let _ = ended.await;
    gw.idle().await;
    (status, body)
}

/// The `t` a call's lines reported, as [`Meter`] saw them.
fn t(usage: &Usage) -> Option<f64> {
    usage.get("t")
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
            let read = call.await?.until_content().await?;
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
    assert_eq!(log.metered().iter().map(t).collect::<Vec<_>>(), [Some(5.0)]);
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
                "bad" => Err(Fault::bad_request("blocked")),
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
    assert_eq!(t(&log.metered()[0]), Some(7.0));
}

// A `HEAD` whose provider sends no body passes its head.

#[tokio::test]
async fn f3_a_head_whose_provider_sends_no_body_passes_its_head() {
    let log = Log::default();
    let script = Script::default().then(StatusCode::OK, vec![]);
    let gw = gateway(script, &log, Log::default());
    let (status, body) = run(&gw, req(http::Method::HEAD)).await;
    assert_eq!(status, StatusCode::OK, "{body:?}");
    assert_eq!(log.calls()[0], Ok(()));
}

/// The same for a provider's 204, translated between the standard formats.
/// A translated success's head is the client's format's, so the
/// status goes; the call still ends as its head says.
#[tokio::test]
async fn f3b_a_translated_204_is_a_success_of_the_clients_format() {
    use respan_gateway_formats::{AnthropicMessages, OpenAiChat};
    use respan_gateway_testkit::MockTransport;
    const ANTHROPIC: &str = "http://anthropic.test";
    let mock = MockTransport::new();
    let gw = Gateway::new()
        .provider("anthropic", AnthropicMessages, mock.at(ANTHROPIC))
        .and_then(|g| g.serve(OpenAiChat))
        .and_then(|g| g.target("anthropic"))
        .unwrap_or_else(|e| panic!("{e}"));
    let empty = http::Response::builder().status(StatusCode::NO_CONTENT);
    mock.reply(ANTHROPIC, empty.body(Vec::new()).unwrap_or_default());
    let r = respan_gateway_testkit::post(
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
                return Err(Fault::bad_request("blocked"));
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
            drop(cx.call("p").await?.until_content().await?);
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
            end.err().map(|f| f.code().to_owned()).as_deref(),
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
            Err(Fault::bad_request("no"))
        }
    }

    let log = Log::default();
    let gw = gateway(Script::default(), &log, No);
    let (status, _) = run(&gw, req(http::Method::HEAD)).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let end = log.clients()[0].clone();
    assert_eq!(
        end.err().map(|f| f.code().to_owned()).as_deref(),
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
                "bad" => Err(Fault::bad_request("blocked")),
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
        client.err().map(|f| f.code().to_owned()).as_deref(),
        Some("internal")
    );
    assert_eq!(
        call.err().map(|f| f.code().to_owned()).as_deref(),
        Some("internal")
    );
}

// A 101 to a whole request is refused, though its provider sent it. The
// adapter refuses it ([`respan_gateway_http::handle`]), so the provider's
// body must be one this format reads for its relayed head to get there;
// the standard formats fail an unasked 101 as they decode it.

#[tokio::test]
async fn f9_a_101_to_a_whole_request_is_refused_whoever_sent_it() {
    let log = Log::default();
    let script = Script::default().then(StatusCode::SWITCHING_PROTOCOLS, vec!["e\n"]);
    let gw = gateway(script, &log, Log::default());
    let (status, body) = run(&gw, req(http::Method::POST)).await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    let body = String::from_utf8_lossy(&body.unwrap_or_default()).into_owned();
    assert!(body.contains("did not ask to switch protocols"), "{body}");
    // The adapter drops the gateway's response, so the request ends
    // abandoned.
    let end = log.clients()[0].clone();
    assert_eq!(
        end.err().map(|f| f.code().to_owned()).as_deref(),
        Some("abandoned")
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
            let _ = cx.filter(|_: &mut LIn| Err(Fault::bad_request("first fails")));
            let _ = cx.filter(Ends(self.0.clone(), "second"));
            Err(Fault::bad_request("refused"))
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
    let code = calls[0].as_ref().err().map(|f| f.code().to_owned());
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
                "mb" => return Err(Fault::bad_request("blocked")),
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
        match cx.call("p").await?.until_content().await {
            Ok(resp) => Ok(Some(resp)),
            Err(_) => cx.call("p").await.map(Some),
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
    assert_eq!(log.calls()[0], Ok(()));
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
    assert_eq!(log.metered().iter().map(t).collect::<Vec<_>>(), [Some(5.0)]);
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
            Err(Fault::bad_request("no"))
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
            drop(cx.call("p").await?.until_content().await?);
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
        end.err().map(|f| f.code().to_owned()).as_deref(),
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
            Err(Fault::bad_request("no"))
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
    let body = StreamBody::new(frames.map(|f| Ok::<_, Fault>(Frame::data(f))));
    let mut r = http::Request::new(body.boxed_unsync());
    *r.uri_mut() = http::Uri::from_static("/line");
    r.headers_mut().insert(
        http::header::UPGRADE,
        http::HeaderValue::from_static("websocket"),
    );
    let (resp, ended) = serve(&gw, r).await;
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
        true => Err(Fault::new("budget", "over budget").with(Status(402))),
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
// provider edge, so no meter (0009).
#[tokio::test]
async fn k9_7_a_client_filter_fails_a_plugins_own_outputs_which_count_nothing() {
    struct P;

    #[async_trait]
    impl Plugin for P {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            cx.filter(over_budget)?;
            let mut usage = out("");
            usage.1.add("t", 5.0);
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

impl Codec for Pathed {
    as_line!();

    fn name(&self) -> &str {
        "pathed"
    }

    fn accepts(&self, _head: &respan_gateway_http::RequestHead) -> bool {
        false
    }

    fn request_encoder(
        &self,
        client: Option<&respan_gateway_http::RequestHead>,
        _body: &mut Bytes,
        _streams: bool,
    ) -> Box<dyn Encoder<LIn, respan_gateway_http::RequestHead>> {
        let mut head = respan_gateway_formats::call_head(client);
        let head = move |first: Option<&LIn>| {
            // The model: the input, translated or changed.
            let model = first.map_or("none", |i| &i.0).to_owned();
            head.method = http::Method::POST;
            respan_gateway_formats::set_path(&mut head, &format!("/models/{model}"));
            head
        };
        respan_gateway_formats::headed(head, Box::new(Untranslatable))
    }
}

impl_format!(Pathed);

/// Writes as Line does, but cannot say "untranslatable".
struct Untranslatable;

impl Encoder<LIn, ()> for Untranslatable {
    fn head(&mut self, _first: Option<&LIn>) {}

    fn encode(&mut self, input: &LIn, out: &mut BytesMut) -> Result<(), Fault> {
        match input.0.contains("untranslatable") {
            true => Err(input
                .1
                .whose(input, Fault::internal("pathed cannot say it"))),
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

#[async_trait]
impl Transport for Paths {
    async fn send(
        &self,
        call: Exchange<respan_gateway_http::RequestHead>,
    ) -> Result<Exchange<respan_gateway_http::ResponseHead>, Fault> {
        lock(&self.0).push(call.head.uri.path().to_owned());
        let body = futures::stream::iter([Ok(Bytes::from_static(b"i:ok\ne\n"))]);
        Ok(Exchange::new(
            respan_gateway_http::fresh_response_head(),
            body.boxed(),
        ))
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
        .provider("pathed", Pathed, paths.clone())
        .and_then(|g| g.serve(Line))
        .and_then(|g| g.target("pathed"))
        .and_then(|g| g.plugin("model", Model(model)))
        .unwrap_or_else(|e| panic!("{e}"))
}

fn line_req(body: &'static str) -> http::Request<Body> {
    let body = http_body_util::Full::new(Bytes::from_static(body.as_bytes()));
    let mut r = http::Request::new(body.map_err(Fault::from).boxed_unsync());
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

// Client content the provider's format cannot say is the client's fault, a
// 400, only as decoded; changed by a filter, it is a 500, whatever the change.
#[tokio::test]
async fn v9_1_untranslatable_content_is_the_clients_fault_only_as_decoded() {
    let paths = Paths::default();
    let (status, _) = run(&pathed(&paths, Some("m2 untranslatable")), line_req("m1")).await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
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
        .provider("pathed", Pathed, paths.clone())
        .and_then(|g| g.serve(Line))
        .and_then(|g| g.target("pathed"))
        .and_then(|g| g.plugin("tag", Tag))
        .unwrap_or_else(|e| panic!("{e}"));
    let (status, _) = run(&gw, line_req("untranslatable")).await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert!(lock(&paths.0).is_empty());
}

fn upgrading(frames: Vec<Result<&'static str, Fault>>) -> http::Request<Body> {
    let frames = futures::stream::iter(frames)
        .map(|f| f.map(|f| Frame::data(Bytes::from_static(f.as_bytes()))));
    let mut r = http::Request::new(StreamBody::new(frames).boxed_unsync());
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
        Err(Fault::upstream("the client's body broke")),
    ]);
    let _ = run(&gw, r).await;
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
            drop(cx.call("p").await?.until_content().await?);
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
    let code = |e: &Result<(), Fault>| e.as_ref().err().map(|f| f.code().to_owned());
    assert_eq!(code(&ends[0]).as_deref(), Some("abandoned"));
    assert_eq!(ends[1], Ok(()));
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
            Err(Fault::bad_request("no"))
        }
    }

    let log = Log::default();
    let gw = gateway(Script::default(), &log, P);
    let _ = run(&gw, req(http::Method::POST)).await;
    let end = log.clients()[0].clone();
    // The end is known at close, so a panic changes it no more than a failure.
    assert_eq!(
        end.err().map(|f| f.code().to_owned()).as_deref(),
        Some("bad_request")
    );
}

/// Line's kind under another name, whose calls are always a `POST`, as
/// OpenAI's and Anthropic's are.
#[derive(Clone)]
struct Posted;

impl Codec for Posted {
    as_line!();

    fn name(&self) -> &str {
        "posted"
    }

    fn accepts(&self, _head: &respan_gateway_http::RequestHead) -> bool {
        false
    }

    fn request_encoder(
        &self,
        client: Option<&respan_gateway_http::RequestHead>,
        body: &mut Bytes,
        streams: bool,
    ) -> Box<dyn Encoder<LIn, respan_gateway_http::RequestHead>> {
        let line = Line.request_encoder(client, body, streams);
        let post = |head: &mut respan_gateway_http::RequestHead, _: Option<&LIn>| {
            head.method = http::Method::POST;
        };
        Box::new(EditHead(line, Some(post)))
    }
}

impl_format!(Posted);

// `until_content` judges the response as its provider sent it, the call's method,
// not the client's `HEAD`.
#[tokio::test]
async fn r10_4_until_content_judges_a_head_as_the_provider_sent_it() {
    struct Fallback(Log);

    #[async_trait]
    impl Plugin for Fallback {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let read = cx.call("posted").await?.until_content().await;
            self.0.note(format!("content {}", read.is_ok()));
            Ok(Some(own()))
        }
    }

    let log = Log::default();
    let script = Script::default().then(StatusCode::OK, vec!["x:boom\n"]);
    let gw = Gateway::new()
        .provider("posted", Posted, script.clone())
        .and_then(|g| g.serve(Line))
        .and_then(|g| g.plugin("log", log.clone()))
        .and_then(|g| g.plugin("x", Fallback(log.clone())))
        .unwrap_or_else(|e| panic!("{e}"));
    let _ = run(&gw, req(http::Method::HEAD)).await;
    assert_eq!(log.notes(), ["content false"]);
}

/// Line under its own name, so calls to it relay, noting whether its
/// request encoder got a first input.
#[derive(Clone)]
struct Seen(Shared<bool>);

impl Codec for Seen {
    as_line!();

    fn name(&self) -> &str {
        "line"
    }

    fn accepts(&self, _head: &respan_gateway_http::RequestHead) -> bool {
        false
    }

    fn request_encoder(
        &self,
        client: Option<&respan_gateway_http::RequestHead>,
        body: &mut Bytes,
        streams: bool,
    ) -> Box<dyn Encoder<LIn, respan_gateway_http::RequestHead>> {
        let line = Line.request_encoder(client, body, streams);
        let seen = self.0.clone();
        let note = move |_: &mut respan_gateway_http::RequestHead, first: Option<&LIn>| {
            lock(&seen).push(first.is_some());
        };
        Box::new(EditHead(line, Some(note)))
    }
}

impl_format!(Seen);

// A call's first input is given as a value exactly when a filter ran over it;
// an unfiltered one goes as the client's bytes.
#[tokio::test]
async fn r10_6_first_is_an_item_only_for_an_input_not_the_clients() {
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
            .provider("p", Seen(seen.clone()), script)
            .and_then(|g| g.serve(Line))
            .and_then(|g| g.target("p"))
            .and_then(|g| g.plugin("x", Touch(to)))
            .unwrap_or_else(|e| panic!("{e}"));
        let (status, _) = run(&gw, req(http::Method::POST)).await;
        assert_eq!(status, StatusCode::OK);
    }
    // Read only: the client's bytes; filtered, even to what it was: a value.
    assert_eq!(lock(&seen).clone(), [false, true, true]);
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
    let (_send, body) = futures::channel::mpsc::unbounded::<Result<Frame<Bytes>, Fault>>();
    let mut r = http::Request::new(StreamBody::new(body).boxed_unsync());
    *r.uri_mut() = http::Uri::from_static("/line");
    // Not a GET, so this format writes no opening bytes.
    *r.method_mut() = http::Method::POST;
    r.headers_mut().insert(
        http::header::UPGRADE,
        http::HeaderValue::from_static("websocket"),
    );
    let headed = tokio::time::timeout(std::time::Duration::from_secs(1), serve(&gw, r)).await;
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
        .provider("posted", Posted, script)
        .and_then(|g| g.serve(Line))
        .and_then(|g| g.target("posted"))
        .and_then(|g| g.plugin("log", log.clone()))
        .and_then(|g| g.plugin("meter", Meter(log.clone())))
        .unwrap_or_else(|e| panic!("{e}"));
    let (status, body) = run(&gw, req(http::Method::HEAD)).await;
    assert_eq!((status, body.ok()), (StatusCode::OK, Some(Bytes::new())));
    let call = &log.calls()[0];
    let end = call.clone().map_err(|f| f.code().to_owned());
    assert_eq!(
        (t(&log.metered()[0]), end),
        (None, Err(respan_gateway_core::codes::ABANDONED.to_owned()))
    );
}

/// Line's kind, whose request encoder writes a change into the client's
/// bytes, as a model-in-path format does; notes whether it got a first
/// input, and its bytes.
#[derive(Clone)]
struct InPath(Shared<(bool, Bytes)>);

impl Codec for InPath {
    as_line!();

    fn name(&self) -> &str {
        "line"
    }

    fn accepts(&self, _head: &respan_gateway_http::RequestHead) -> bool {
        false
    }

    fn request_encoder(
        &self,
        client: Option<&respan_gateway_http::RequestHead>,
        _body: &mut Bytes,
        _streams: bool,
    ) -> Box<dyn Encoder<LIn, respan_gateway_http::RequestHead>> {
        let (seen, head) = (self.0.clone(), line_head(client));
        let head = move |first: Option<&LIn>| {
            let raw = first
                .and_then(|i| i.1.origin(LINE))
                .map(|o| o.bytes.clone());
            lock(&seen).push((first.is_some(), raw.unwrap_or_default()));
            head
        };
        respan_gateway_formats::headed(head, Box::new(AsItCame))
    }
}

impl_format!(InPath);

/// Writes an input's bytes as they are, changed or not, as a format whose
/// model is in the path does; one without, as Line does.
struct AsItCame;

impl Encoder<LIn, ()> for AsItCame {
    fn head(&mut self, _first: Option<&LIn>) {}

    fn encode(&mut self, input: &LIn, out: &mut BytesMut) -> Result<(), Fault> {
        match input.1.origin(LINE) {
            Some(origin) => {
                out.extend_from_slice(&origin.bytes);
                Ok(())
            }
            None => Lines.encode(input, out),
        }
    }

    fn end(&mut self, end: &Result<(), Fault>, out: &mut BytesMut) -> Result<(), Fault> {
        Lines.end(end, out)
    }
}

// The first input is the changed one, given as `first`, even with the
// client's bytes to write it into.
#[tokio::test]
async fn r11_first_is_a_changed_input_even_with_the_clients_bytes() {
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
        .provider("p", InPath(seen.clone()), script)
        .and_then(|g| g.serve(Line))
        .and_then(|g| g.target("p"))
        .and_then(|g| g.plugin("x", Change))
        .unwrap_or_else(|e| panic!("{e}"));
    let (status, _) = run(&gw, req(http::Method::POST)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(lock(&seen).clone(), [(true, Bytes::from_static(b"hi"))]);
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
    let internal = call.as_ref().is_err_and(|f| f.code() == "internal");
    assert_eq!((t(&log.metered()[0]), internal), (None, true));
}
