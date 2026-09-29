//! HTTP's driver on a toy codec: the byte half of the gateway's
//! guarantees, moved here from the core's tests (decision 0020), and the
//! driver's own rules. Requests and
//! responses are strings; a response is complete once it says `DONE`, and
//! a status other than 200 fails its decoder.
#![allow(clippy::unwrap_used)]

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use respan_gateway_core::futures::channel::mpsc;
use respan_gateway_core::futures::future::{self, BoxFuture};
use respan_gateway_core::futures::{FutureExt, StreamExt, stream};
use respan_gateway_core::{Dialed, Extensions, Fault, Gateway, async_trait};
use respan_gateway_http::http::StatusCode;
use respan_gateway_http::http_body_util::BodyExt;
use respan_gateway_http::{
    Accepted, Answer, Ask, Body, Codec, Decoder, Dialer, Encoder, Exchange, Frames, Transport,
    dialect, fresh_answer, fresh_ask, http,
};
use respan_gateway_testkit::{Calls, Outcomes};

#[derive(Clone, PartialEq, Debug)]
struct In(String);
#[derive(Clone, PartialEq, Debug)]
struct Out(String);

/// The toy codec; its request encoder opens with `open`'s bytes, saying
/// whether the body is done, its calls ask to switch if `upgrade`, and it
/// cannot write a refusal if `unwritable`.
#[derive(Clone, Default)]
struct Toy {
    open: Option<(&'static str, bool)>,
    upgrade: bool,
    unwritable: bool,
    /// The status its answers say, if not 200.
    answers: Option<u16>,
}

fn opening(bytes: &'static str, done: bool) -> Toy {
    Toy {
        open: Some((bytes, done)),
        ..Toy::default()
    }
}

impl Codec for Toy {
    type Input = In;
    type Output = Out;

    fn name(&self) -> &str {
        "toy"
    }

    fn accepts(&self, _: &Ask) -> bool {
        true
    }

    fn request_decoder(&self, _: &Ask) -> Box<dyn Decoder<In>> {
        Box::new(ReqDec)
    }

    fn request_encoder(
        &self,
        _: Option<&Ask>,
        _: &mut Bytes,
        _: bool,
    ) -> Box<dyn Encoder<In, Ask>> {
        Box::new(ReqEnc {
            open: self.open,
            upgrade: self.upgrade,
        })
    }

    fn response_decoder(&self, answer: &Answer, _: &Ask, _: &Bytes) -> Box<dyn Decoder<Out>> {
        Box::new(RespDec {
            done: false,
            status: answer.status.as_u16(),
        })
    }

    fn response_encoder(
        &self,
        _: Option<&Answer>,
        _: &Ask,
        _: &Bytes,
        refusal: Option<&Fault>,
    ) -> Box<dyn Encoder<Out, Answer>> {
        assert!(!(self.unwritable && refusal.is_some()), "unwritable");
        let status = refusal.map_or(self.answers.unwrap_or(200), |f| {
            respan_gateway_http::status(f).as_u16()
        });
        Box::new(RespEnc { status })
    }
}

dialect!(Toy);

/// `BAD` decodes to `before-bad` and then fails, in the same chunk.
struct ReqDec;

impl Decoder<In> for ReqDec {
    fn decode(&mut self, bytes: Bytes, out: &mut Vec<In>) -> Result<(), Fault> {
        let s = String::from_utf8_lossy(&bytes).to_string();
        if s == "BAD" {
            out.push(In("before-bad".into()));
            return Err(Fault::bad_request("bad"));
        }
        out.push(In(s));
        Ok(())
    }

    fn end(&mut self, _: &mut Vec<In>) -> Result<(), Fault> {
        Ok(())
    }
}

/// Writes an input as its text, and `<END>` for a clean end, nothing for a
/// failed one. `ENCPANIC` panics.
struct ReqEnc {
    open: Option<(&'static str, bool)>,
    upgrade: bool,
}

impl Encoder<In, Ask> for ReqEnc {
    fn open(&mut self, out: &mut BytesMut) -> Result<bool, Fault> {
        let Some((bytes, done)) = self.open else {
            return Ok(false);
        };
        out.extend_from_slice(bytes.as_bytes());
        Ok(done)
    }

    fn head(&mut self, _: Option<&In>) -> Ask {
        let mut head = fresh_ask();
        head.uri = http::Uri::from_static("/x");
        if self.upgrade {
            let websocket = http::HeaderValue::from_static("websocket");
            head.headers.insert(http::header::UPGRADE, websocket);
        }
        head
    }

    fn encode(&mut self, v: &In, out: &mut BytesMut) -> Result<(), Fault> {
        if v.0 == "ENCPANIC" {
            panic!("encoder panic");
        }
        out.extend_from_slice(v.0.as_bytes());
        Ok(())
    }

    fn end(&mut self, e: &Result<(), Fault>, out: &mut BytesMut) -> Result<(), Fault> {
        if e.is_ok() {
            out.extend_from_slice(b"<END>");
        }
        Ok(())
    }
}

struct RespDec {
    done: bool,
    status: u16,
}

impl Decoder<Out> for RespDec {
    fn decode(&mut self, bytes: Bytes, out: &mut Vec<Out>) -> Result<(), Fault> {
        if self.status != 200 {
            return Err(Fault::new("refused", "refused")
                .with(respan_gateway_http::Status(self.status))
                .with_answer("toy", ()));
        }
        let s = String::from_utf8_lossy(&bytes).to_string();
        match s.as_str() {
            "DONE" => self.done = true,
            "ERR" => return Err(Fault::upstream("boom")),
            _ => out.push(Out(s)),
        }
        Ok(())
    }

    fn end(&mut self, _: &mut Vec<Out>) -> Result<(), Fault> {
        match self.done {
            true => Ok(()),
            false => Err(Fault::truncated()),
        }
    }
}

struct RespEnc {
    status: u16,
}

impl Encoder<Out, Answer> for RespEnc {
    fn head(&mut self, _: Option<&Out>) -> Answer {
        let mut head = fresh_answer();
        head.status = StatusCode::from_u16(self.status).unwrap();
        head
    }

    fn encode(&mut self, v: &Out, out: &mut BytesMut) -> Result<(), Fault> {
        out.extend_from_slice(v.0.as_bytes());
        out.extend_from_slice(b";");
        Ok(())
    }

    fn end(&mut self, e: &Result<(), Fault>, out: &mut BytesMut) -> Result<(), Fault> {
        match e {
            Ok(()) => out.extend_from_slice(b"!"),
            Err(f) => out.extend_from_slice(format!("ERR {}", f.code()).as_bytes()),
        }
        Ok(())
    }
}

type Answering =
    dyn Fn(Exchange<Ask>) -> BoxFuture<'static, Result<Exchange<Answer>, Fault>> + Send + Sync;

/// A transport of a function.
struct Fnt(Arc<Answering>);

#[async_trait]
impl Transport for Fnt {
    async fn send(&self, call: Exchange<Ask>) -> Result<Exchange<Answer>, Fault> {
        (self.0)(call).await
    }
}

fn fnt<F>(f: F) -> Fnt
where
    F: Fn(Exchange<Ask>) -> BoxFuture<'static, Result<Exchange<Answer>, Fault>>
        + Send
        + Sync
        + 'static,
{
    Fnt(Arc::new(f))
}

/// A response of `status` with body `frames`.
fn answer(status: u16, frames: Frames) -> Exchange<Answer> {
    let mut head = fresh_answer();
    head.status = StatusCode::from_u16(status).unwrap();
    Exchange::new(head, frames)
}

fn chunks(v: Vec<&'static str>) -> Frames {
    stream::iter(v.into_iter().map(|s| Ok(Bytes::from(s)))).boxed()
}

/// Chunks that yield between each.
fn slow(v: Vec<&'static str>) -> Frames {
    stream::unfold(v.into_iter(), |mut it| async move {
        tokio::task::yield_now().await;
        it.next().map(|s| (Ok(Bytes::from(s)), it))
    })
    .boxed()
}

/// `a`, `DONE`.
fn done() -> Frames {
    slow(vec!["a", "DONE"])
}

fn text(bytes: &Bytes) -> String {
    String::from_utf8_lossy(bytes).to_string()
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// A body as text frames, or the failure code it broke with.
type Seen_ = Vec<Result<String, String>>;

fn frames(of: &[Result<&str, &str>]) -> Seen_ {
    of.iter()
        .map(|f| f.map(str::to_string).map_err(str::to_string))
        .collect()
}

/// The frames a transport read of a call's body.
#[derive(Clone, Default)]
struct Seen {
    frames: Arc<Mutex<Seen_>>,
    done: Arc<AtomicBool>,
}

impl Seen {
    /// A transport that reads the call's body in its own task, as reqwest
    /// does, and answers with `answer`, once the body ended if `wait`.
    fn transport(&self, wait: bool, answer: fn() -> Frames) -> Fnt {
        let seen = self.clone();
        fnt(move |c| {
            let seen = seen.clone();
            async move {
                let mut body = c.body;
                let read = tokio::spawn(async move {
                    while let Some(f) = body.next().await {
                        let f = f.map(|b| text(&b)).map_err(|e| e.code().to_string());
                        lock(&seen.frames).push(f);
                    }
                    seen.done.store(true, Ordering::SeqCst);
                });
                if wait {
                    let _ = read.await;
                }
                Ok(self::answer(200, answer()))
            }
            .boxed()
        })
    }

    fn frames(&self) -> Seen_ {
        lock(&self.frames).clone()
    }
}

/// Serves the toy, calling provider `a` of `codec` over `transport`, with
/// recorders of both edges' ends.
fn gateway_of(codec: Toy, transport: Fnt) -> (Gateway, Calls, Outcomes) {
    let (calls, outcomes) = (Calls::default(), Outcomes::default());
    let gw = Gateway::new()
        .serve(Toy::default())
        .unwrap()
        .provider("a", codec, transport)
        .unwrap()
        .target("a")
        .unwrap()
        .plugin("calls", calls.clone())
        .unwrap()
        .plugin("outcomes", outcomes.clone())
        .unwrap();
    (gw, calls, outcomes)
}

type Upload = mpsc::UnboundedSender<Result<Bytes, Fault>>;

/// A body whose frames are sent through the returned sender.
fn upload() -> (Upload, Frames) {
    let (tx, rx) = mpsc::unbounded();
    (tx, rx.boxed())
}

fn send(tx: &Upload, text: &'static str) {
    tx.unbounded_send(Ok(Bytes::from(text))).unwrap();
}

fn ask() -> Ask {
    let mut head = fresh_ask();
    head.method = http::Method::POST;
    head
}

/// `frames` as a request, streaming if `streams`, through `Accepted` and
/// `Gateway::open`: the answer, and how the session ended.
async fn open(
    gw: &Gateway,
    frames: Frames,
    streams: bool,
) -> (
    Option<http::Response<Body>>,
    BoxFuture<'static, Result<(), Fault>>,
) {
    let (wire, answer) = Accepted::new(ask(), frames, streams);
    let end = gw.handle(wire, Extensions::new()).await;
    (answer.await, end)
}

/// What a client got: its status and body as text, or the failure it
/// broke with.
async fn read(resp: Option<http::Response<Body>>) -> Result<(u16, String), String> {
    let resp = resp.ok_or("unanswered")?;
    let status = resp.status().as_u16();
    let mut body = resp.into_body();
    let mut out = String::new();
    while let Some(frame) = body.frame().await {
        match frame {
            Ok(frame) => out.push_str(&text(&frame.into_data().unwrap_or_default())),
            Err(e) => return Err(format!("{} after {out}", e.code())),
        }
    }
    Ok((status, out))
}

fn code(end: &Result<(), Fault>) -> String {
    end.as_ref()
        .map_or_else(|f| f.code().to_string(), |()| "ok".into())
}

fn codes(ends: Vec<Result<(), Fault>>) -> Vec<String> {
    ends.iter().map(code).collect()
}

/// B2: the request encoder panics on a later frame while the transport reads
/// the body in a task of its own, as reqwest does, where nothing unwinds to
/// the call. The call still ends `internal`, and its body ends with an `Err`.
#[tokio::test(flavor = "multi_thread")]
async fn a_request_encoder_panic_on_a_later_frame_ends_the_call_internal() {
    let seen = Seen::default();
    let (gw, calls, outcomes) = gateway_of(Toy::default(), seen.transport(true, done));
    let (tx, body) = upload();
    send(&tx, "first");
    send(&tx, "ENCPANIC");
    let (resp, end) = open(&gw, body, true).await;
    let got = read(resp).await;
    drop(tx);
    gw.idle().await;
    assert_eq!(seen.frames(), frames(&[Ok("first"), Err("internal")]));
    assert_eq!(codes(calls.all()), ["internal"], "client got {got:?}");
    assert_eq!(codes(outcomes.all()), ["internal"]);
    assert_eq!(code(&end.await), "internal");
}

/// An encoder that fails what it is given ends its body there: nothing
/// follows the `Err`, not even a clean end.
#[tokio::test(flavor = "multi_thread")]
async fn a_body_ends_at_its_first_failure() {
    let seen = Seen::default();
    let (gw, _, _) = gateway_of(Toy::default(), seen.transport(true, done));
    let (tx, body) = upload();
    for text in ["one", "ENCPANIC", "two"] {
        send(&tx, text);
    }
    let (resp, _) = open(&gw, body, true).await;
    let _ = read(resp).await;
    assert_eq!(seen.frames(), frames(&[Ok("one"), Err("internal")]));
}

/// B5: `Encoder::open`'s bytes come before the body, the client's or the
/// written one.
#[tokio::test(flavor = "current_thread")]
async fn a_request_encoders_opening_bytes_come_first() {
    let seen = Seen::default();
    let (gw, _, _) = gateway_of(opening("<OPEN>", false), seen.transport(true, done));
    let (resp, _) = open(&gw, chunks(vec!["q"]), false).await;
    let _ = read(resp).await;
    // a relay's first frame is the client's bytes, unread
    assert_eq!(seen.frames(), frames(&[Ok("<OPEN>q<END>")]));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_streaming_requests_opening_bytes_are_its_first_frame() {
    let seen = Seen::default();
    let (gw, _, _) = gateway_of(opening("<OPEN>", false), seen.transport(true, done));
    let (tx, body) = upload();
    send(&tx, "x");
    drop(tx);
    let (resp, _) = open(&gw, body, true).await;
    let _ = read(resp).await;
    assert_eq!(seen.frames(), frames(&[Ok("<OPEN>"), Ok("x"), Ok("<END>")]));
}

/// B5: `Ok(true)` from a request encoder's `open`, as from a response
/// encoder's, means nothing more follows: the body is the opening bytes.
#[tokio::test(flavor = "multi_thread")]
async fn a_request_encoders_open_saying_done_is_the_whole_body() {
    let seen = Seen::default();
    let (gw, _, _) = gateway_of(opening("<OPEN>", true), seen.transport(true, done));
    let (resp, _) = open(&gw, chunks(vec!["q"]), false).await;
    assert_eq!(read(resp).await, Ok((200, "a;!".into())));
    assert_eq!(seen.frames(), frames(&[Ok("<OPEN>")]), "a whole request");

    let seen = Seen::default();
    let (gw, _, _) = gateway_of(opening("<OPEN>", true), seen.transport(true, done));
    let (tx, body) = upload();
    send(&tx, "x");
    let (resp, _) = open(&gw, body, true).await;
    assert_eq!(read(resp).await, Ok((200, "a;!".into())));
    drop(tx);
    assert_eq!(
        seen.frames(),
        frames(&[Ok("<OPEN>")]),
        "a streaming request"
    );
}

/// An empty data frame is an input, and reaches the provider as a frame.
#[tokio::test(flavor = "multi_thread")]
async fn an_empty_frame_is_still_a_frame() {
    let seen = Seen::default();
    let (gw, _, _) = gateway_of(Toy::default(), seen.transport(true, done));
    let (tx, body) = upload();
    send(&tx, "one");
    send(&tx, "");
    drop(tx);
    let (resp, _) = open(&gw, body, true).await;
    let _ = read(resp).await;
    assert_eq!(seen.frames(), frames(&[Ok("one"), Ok(""), Ok("<END>")]));
}

/// B1: the client's body breaks; the provider's body ends with that `Err`,
/// and no end is written after it.
#[tokio::test(flavor = "multi_thread")]
async fn a_broken_upload_ends_the_providers_body_with_the_err() {
    let seen = Seen::default();
    let (gw, _, _) = gateway_of(Toy::default(), seen.transport(true, done));
    let (tx, body) = upload();
    send(&tx, "hello");
    tx.unbounded_send(Err(Fault::bad_request("client broke")))
        .unwrap();
    let (resp, _) = open(&gw, body, true).await;
    let _ = read(resp).await;
    gw.idle().await;
    assert_eq!(seen.frames(), frames(&[Ok("hello"), Err("bad_request")]));
}

/// Rule 1: a streaming request's inputs are never ended at once, even with
/// its body ended; a one-shot request's are.
#[tokio::test]
async fn a_streaming_requests_inputs_never_end_at_once() {
    let (tx, body) = upload();
    drop(tx);
    let (wire, _answer) = Accepted::new(ask(), body, true);
    let (origin, inputs) =
        respan_gateway_http::accept(&Toy::default(), wire, future::pending().boxed()).await;
    let mut inputs = inputs.unwrap();
    assert!(
        inputs.next().now_or_never().is_none(),
        "its first poll waits"
    );
    assert!(inputs.next().await.is_none());
    let origin = origin.unwrap();
    assert!(respan_gateway_http::opening(&Toy::default(), &origin).is_none());

    let (wire, _answer) = Accepted::new(ask(), chunks(vec!["q"]), false);
    let (origin, inputs) =
        respan_gateway_http::accept(&Toy::default(), wire, future::pending().boxed()).await;
    assert!(matches!(inputs.unwrap().next().now_or_never(), Some(None)));
    let opening = respan_gateway_http::opening(&Toy::default(), &origin.unwrap());
    assert_eq!(opening.map(|o| o.ok()), Some(Some(In("q".into()))));
}

/// Dial the toy through `transport`, with one input and none to follow.
fn dial(codec: &Toy, transport: Fnt) -> Dialed<Out> {
    let dialer = Dialer::from(transport);
    let input = In("q".into());
    respan_gateway_http::dial(
        codec,
        &dialer,
        Extensions::new(),
        None,
        Some((&input, true)),
        None,
    )
    .unwrap()
}

/// Rule 6: a provider's outputs say `Pending` between the frames it sent
/// apart, so each frame is its own batch, even when all are ready.
#[tokio::test]
async fn a_providers_frames_are_read_apart() {
    let transport = fnt(|_| future::ready(Ok(answer(200, chunks(vec!["a", "b", "DONE"])))).boxed());
    let (_, outputs) = dial(&Toy::default(), transport).await.unwrap();
    let mut outputs = outputs.unwrap();
    let mut cx = Context::from_waker(std::task::Waker::noop());
    let mut polls = Vec::new();
    for _ in 0..6 {
        polls.push(match outputs.poll_next_unpin(&mut cx) {
            Poll::Pending => "pending".to_string(),
            Poll::Ready(Some(Ok(Out(s)))) => s,
            Poll::Ready(Some(Err(f))) => f.code().to_string(),
            Poll::Ready(None) => "end".to_string(),
        });
    }
    assert_eq!(polls, ["a", "pending", "b", "pending", "pending", "end"]);
}

/// A body that comes after a while.
fn late(text: &'static str) -> Frames {
    stream::once(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        Ok(Bytes::from(text))
    })
    .boxed()
}

/// Rule 3: a non-2xx answer is read whole before the call resolves, so its
/// refusal is known at its head however slowly its body comes; a 2xx one's
/// is read as far as it is ready.
#[tokio::test]
async fn a_slow_refusal_is_known_at_its_head() {
    let transport = fnt(|_| future::ready(Ok(answer(429, late("busy")))).boxed());
    let (_, outputs) = dial(&Toy::default(), transport).await.unwrap();
    let fault = outputs.err().unwrap();
    assert_eq!(fault.code(), "refused");
    assert_eq!(fault.get(), Some(&respan_gateway_http::Status(429)));

    let transport = fnt(|_| future::ready(Ok(answer(200, late("DONE")))).boxed());
    let (_, outputs) = dial(&Toy::default(), transport).await.unwrap();
    assert!(outputs.is_ok(), "open, its body still to come");
}

/// Rule 3's exception: a 101 to a call that asked to switch is open at
/// once; to one that did not, it is read whole like any other non-2xx.
#[tokio::test]
async fn a_101_is_open_only_to_a_call_that_asked_to_switch() {
    let switched = || fnt(|_| future::ready(Ok(answer(101, stream::pending().boxed()))).boxed());
    let asked = Toy {
        upgrade: true,
        ..Toy::default()
    };
    let dialed = tokio::time::timeout(Duration::from_secs(2), dial(&asked, switched()));
    let (_, outputs) = dialed.await.unwrap().unwrap();
    assert!(outputs.is_ok());

    let dialed = tokio::time::timeout(
        Duration::from_millis(100),
        dial(&Toy::default(), switched()),
    );
    assert!(dialed.await.is_err(), "still reading the body");
}

/// `Exchange::relayed` is set on a call written from the client's origin
/// alone: a relay of the client's own message, not a call of its own input.
#[tokio::test]
async fn only_a_call_written_from_the_clients_origin_is_relayed() {
    let marks = Arc::new(Mutex::new(Vec::new()));
    let marking = {
        let marks = marks.clone();
        fnt(move |c| {
            lock(&marks).push(c.relayed);
            future::ready(Ok(answer(200, chunks(vec!["a", "DONE"])))).boxed()
        })
    };
    let dialer = Dialer::from(marking);
    let (wire, _answer) = Accepted::new(ask(), chunks(vec!["q"]), false);
    let (origin, _) =
        respan_gateway_http::accept(&Toy::default(), wire, future::pending().boxed()).await;
    let origin = origin.unwrap();
    let toy = Toy::default();
    let relayed =
        respan_gateway_http::dial(&toy, &dialer, Extensions::new(), Some(&origin), None, None);
    let _ = relayed.unwrap().await.unwrap();
    let input = In("own".into());
    let own = respan_gateway_http::dial(
        &toy,
        &dialer,
        Extensions::new(),
        None,
        Some((&input, false)),
        None,
    );
    let _ = own.unwrap().await.unwrap();
    assert_eq!(*lock(&marks), [true, false]);
}

/// A refusal no codec can write goes as plain text, with its body, even to
/// a `HEAD` request: the adapter's rule for a body does not apply to it.
#[tokio::test]
async fn a_refusal_no_codec_can_write_keeps_its_body_even_to_a_head() {
    let toy = Toy {
        unwritable: true,
        ..Toy::default()
    };
    // No target: every request is refused.
    let gw = Gateway::new().serve(toy).unwrap();
    for method in [http::Method::POST, http::Method::HEAD] {
        let req = http::Request::builder().method(method.clone()).uri("/x");
        let req = req.body(respan_gateway_http::full("q")).unwrap();
        let (resp, _) = respan_gateway_http::serve(&gw, req).await;
        let got = read(Some(resp)).await.unwrap();
        assert_eq!(got.0, 500, "{method}");
        assert!(!got.1.is_empty(), "{method}: the refusal's text");
    }
}

/// An error answer is read whole to be relayed or translated, up to a cap
/// (`ERROR_BODY_CAP`); past it, it is refused with what was read. Before
/// the cap: a non-2xx body that never ends, pulled without bound.
///
/// (ADV-1: a non-2xx answer is read whole before `dial` resolves, with no
/// bound: an error body that never ends is pulled forever, every frame
/// held in memory, and the call never resolves.)
#[tokio::test(flavor = "multi_thread")]
async fn an_error_answer_that_never_ends_is_refused_at_a_cap() {
    let pulled = Arc::new(AtomicUsize::new(0));
    let counter = pulled.clone();
    let transport = fnt(move |_| {
        let counter = counter.clone();
        let body = stream::unfold(counter, |c| async move {
            tokio::task::yield_now().await;
            c.fetch_add(1, Ordering::SeqCst);
            Some((Ok(Bytes::from(vec![b'x'; 64 * 1024])), c))
        })
        .boxed();
        future::ready(Ok(answer(503, body))).boxed()
    });
    let dialed = tokio::time::timeout(Duration::from_secs(2), dial(&Toy::default(), transport));
    let resolved = dialed.await.is_ok();
    let frames = pulled.load(Ordering::SeqCst);
    let mib = frames * 64 / 1024;
    assert!(
        resolved,
        "dial still pending after 2s; it pulled {frames} frames ({mib} MiB) of a 503's body into memory"
    );
}

/// ADV-2: the adapter drains the body of a bodiless answer (a HEAD) in a
/// detached task. A provider stream that never ends keeps that task, the
/// session and its call alive forever after the client has gone, and
/// `Gateway::idle` never resolves. The same answer to a GET ends as the
/// client drops it.
#[tokio::test(flavor = "multi_thread")]
async fn a_bodiless_answer_ends_with_its_client() {
    for method in [http::Method::GET, http::Method::HEAD] {
        let transport = fnt(|_| {
            let body = stream::iter([Ok(Bytes::from("a"))])
                .chain(stream::pending())
                .boxed();
            future::ready(Ok(answer(200, body))).boxed()
        });
        let (gw, calls, _) = gateway_of(Toy::default(), transport);
        let req = http::Request::builder().method(method.clone()).uri("/x");
        let req = req.body(respan_gateway_http::full("q")).unwrap();
        let (resp, _ended) = respan_gateway_http::serve(&gw, req).await;
        assert_eq!(resp.status(), 200);
        drop(resp); // the client leaves
        let idle = tokio::time::timeout(Duration::from_secs(1), gw.idle()).await;
        assert!(
            idle.is_ok(),
            "{method}: the session outlived its client; call ends so far: {:?}",
            codes(calls.all())
        );
    }
}

/// A body that panics when polled, as a buggy adapter body could.
fn panicking() -> Frames {
    stream::poll_fn(|_| -> Poll<Option<Result<Bytes, Fault>>> { panic!("body panic") }).boxed()
}

/// ADV-3: a panic in `accept`'s future (here, reading a whole body) is
/// documented as an internal fault for the adapter to write; it ends the
/// session `abandoned` instead, so the client gets a 499.
#[tokio::test(flavor = "multi_thread")]
async fn a_panic_while_accepting_ends_internal() {
    let (gw, _, _) = gateway_of(Toy::default(), fnt(|_| future::pending().boxed()));
    let (resp, end) = open(&gw, panicking(), false).await;
    assert!(resp.is_none(), "no codec wrote it");
    assert_eq!(code(&end.await), "internal");
}
/// Answers with the target's response read to its end first.
struct ReadToEnd;

#[async_trait]
impl respan_gateway_core::Plugin for ReadToEnd {
    async fn on_client(
        &self,
        cx: &mut respan_gateway_core::ClientContext,
    ) -> Result<Option<respan_gateway_core::Response>, Fault> {
        let (resp, _) = cx.call("a").await?.until_end().await?;
        Ok(Some(resp))
    }
}

/// ADV-4: `Writer::ended` says it is "dropped first, so a client that
/// leaves ends the request before its answer's call", but `values` is
/// declared (so dropped) before it: a client that leaves after the head
/// ends a call whose end was read ahead `Ok`, not abandoned.
#[tokio::test(flavor = "multi_thread")]
async fn a_client_that_leaves_ends_the_request_before_its_call() {
    let transport = fnt(|_| future::ready(Ok(answer(200, chunks(vec!["a", "b", "DONE"])))).boxed());
    let calls = Calls::default();
    let gw = Gateway::new()
        .serve(Toy::default())
        .unwrap()
        .provider("a", Toy::default(), transport)
        .unwrap()
        .plugin("calls", calls.clone())
        .unwrap()
        .plugin("read", ReadToEnd)
        .unwrap();
    let (resp, end) = open(&gw, chunks(vec!["q"]), false).await;
    let resp = resp.unwrap();
    assert_eq!(resp.status(), 200);
    drop(resp); // the client leaves after the head, its body unread
    assert_eq!(code(&end.await), "abandoned");
    gw.idle().await;
    assert_eq!(
        codes(calls.all()),
        ["abandoned"],
        "the call outlived the request"
    );
}

/// Replaces every call's input with its own prompt, through a filter.
struct Rewrites;

#[async_trait]
impl respan_gateway_core::Plugin for Rewrites {
    async fn on_provider(
        &self,
        px: &mut respan_gateway_core::ProviderContext,
    ) -> Result<(), Fault> {
        px.filter(|input: &mut In| {
            *input = In("the plugin's own prompt".into());
            Ok(())
        })
    }
}

/// Answers with a call of its own input to `a`, not the client's.
struct Asks;

#[async_trait]
impl respan_gateway_core::Plugin for Asks {
    async fn on_client(
        &self,
        cx: &mut respan_gateway_core::ClientContext,
    ) -> Result<Option<respan_gateway_core::Response>, Fault> {
        let call = cx.call("a").with_input(In("own prompt".into()));
        Ok(Some(call.await?))
    }
}

/// What the provider got, as (`relayed`, body), of a client's `question`
/// through `plugin`.
async fn provider_saw(plugin: impl respan_gateway_core::Plugin) -> Vec<(bool, String)> {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let noting = {
        let seen = seen.clone();
        fnt(move |c| {
            let seen = seen.clone();
            async move {
                let body: Vec<_> = c.body.collect().await;
                let body = body
                    .into_iter()
                    .map(|b| text(&b.unwrap()))
                    .collect::<String>();
                lock(&seen).push((c.relayed, body));
                Ok(answer(200, chunks(vec!["a", "DONE"])))
            }
            .boxed()
        })
    };
    let gw = Gateway::new()
        .serve(Toy::default())
        .unwrap()
        .provider("a", Toy::default(), noting)
        .unwrap()
        .target("a")
        .unwrap()
        .plugin("plugin", plugin)
        .unwrap();
    let (resp, _) = open(&gw, chunks(vec!["client question"]), false).await;
    let _ = read(resp).await;
    lock(&seen).clone()
}

/// `Exchange::relayed` means the session's own inputs to a format of the
/// same name, as the plugins' filters left them: a filter-edited input
/// still relays (a redactor keeps a subscription passthrough working); a
/// call with its own input (`Call::with_input`) does not.
#[tokio::test(flavor = "multi_thread")]
async fn a_filter_edited_input_still_relays_but_a_calls_own_input_does_not() {
    let edited = provider_saw(Rewrites).await;
    assert_eq!(edited, [(true, "the plugin's own prompt<END>".to_string())]);
    let own = provider_saw(Asks).await;
    assert_eq!(own, [(false, "own prompt<END>".to_string())]);
}
