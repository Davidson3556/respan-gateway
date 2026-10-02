//! HTTP's driver on a toy codec: the byte half of the gateway's
//! guarantees and the driver's own rules. Requests and responses are strings;
//! a response is complete once it says `DONE`, and
//! a status other than 200 fails its decoder.
#![allow(clippy::unwrap_used)]

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use respan_gateway_http::http::StatusCode;
use respan_gateway_http::http_body_util::BodyExt;
use respan_gateway_http::{
    Body, Completion, Encoder, Frames, Incoming, Outgoing, http,
    http::request::Parts as RequestHead,
};
use respan_gateway_sdk::futures::channel::mpsc;
use respan_gateway_sdk::futures::future::{self, BoxFuture};
use respan_gateway_sdk::futures::{FutureExt, SinkExt, StreamExt, stream};
use respan_gateway_sdk::{BadRequest, ErrorCode, Truncated, Upstream};
use respan_gateway_sdk::{EventSink, EventStream, Extensions, Fault, Gateway, async_trait};
#[allow(dead_code)]
#[path = "../../tests/support/hooks.rs"]
mod hooks;
use hooks::{Calls, Outcomes};

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

impl respan_gateway_sdk::Format<Incoming, In, Out> for Toy {
    fn name(&self) -> &str {
        "toy"
    }
    fn matches(&self, _: &Incoming) -> bool {
        true
    }
    fn bind(&self, io: Incoming) -> Result<respan_gateway_sdk::Connection<In, Out>, Fault> {
        let toy = self.clone();
        let unwritable = self.unwritable;
        Ok(io.bind(
            |request| {
                if request.body().full_bytes().is_some() {
                    respan_gateway_http::messages(request, |_, bytes| {
                        let mut values = Vec::new();
                        ReqDec.decode(bytes, &mut values)?;
                        values
                            .into_iter()
                            .next()
                            .ok_or_else(|| BadRequest::fault("empty input"))
                    })
                } else {
                    Ok(EventStream::stream(respan_gateway_http::formats::read(
                        request.into_body(),
                        ReqDec,
                        ReqDec::decode,
                        |decoder, end, values| {
                            end?;
                            decoder.end(values)
                        },
                    )))
                }
            },
            move |reply, source| {
                let mut head = respan_gateway_http::http::Response::new(()).into_parts().0;
                head.status = StatusCode::from_u16(toy.answers.unwrap_or(200)).unwrap();
                let _ = source;
                Ok(reply.write(head, Box::new(RespEnc)))
            },
            move |fault, _, _| {
                assert!(!unwritable, "unwritable");
                Bytes::from(format!("ERR {}", respan_gateway_http::error_code(fault)))
            },
        ))
    }
}

impl respan_gateway_sdk::Format<Outgoing, Out, In> for Toy {
    fn name(&self) -> &str {
        "toy"
    }
    fn bind(&self, io: Outgoing) -> Result<respan_gateway_sdk::Connection<Out, In>, Fault> {
        let toy = self.clone();
        Ok(io.bind(
            move |source| {
                let (mut request, sink) = if toy.open.is_some_and(|(_, done)| done) {
                    source.stop();
                    (
                        http::Request::new(Body::full(toy.open.unwrap().0)),
                        EventSink::new(
                            respan_gateway_sdk::futures::sink::drain().sink_map_err(Fault::from),
                        ),
                    )
                } else {
                    let (mut request, sink) =
                        respan_gateway_http::request("toy", source, Box::new(ReqEnc))?;
                    if let Some((prefix, _)) = toy.open {
                        let body = std::mem::replace(request.body_mut(), Body::full(Bytes::new()));
                        *request.body_mut() =
                            body.with_prefix(Bytes::from_static(prefix.as_bytes()));
                    }
                    (request, sink)
                };
                *request.uri_mut() = http::Uri::from_static("/x");
                if toy.upgrade {
                    request.headers_mut().insert(
                        http::header::UPGRADE,
                        http::HeaderValue::from_static("websocket"),
                    );
                }
                Ok((request, sink))
            },
            |answer, _| {
                let decoder = RespDec {
                    done: false,
                    status: answer.status().as_u16(),
                };
                respan_gateway_http::formats::read(
                    answer.into_body(),
                    decoder,
                    RespDec::decode,
                    |decoder, end, values| {
                        end?;
                        decoder.end(values)
                    },
                )
            },
            |answer, request| {
                answer.status.is_success()
                    || (answer.status == StatusCode::SWITCHING_PROTOCOLS
                        && request.headers.contains_key(http::header::UPGRADE))
            },
        ))
    }
}

/// `BAD` decodes to `before-bad` and then fails, in the same chunk.
struct ReqDec;

impl ReqDec {
    fn decode(&mut self, bytes: Bytes, out: &mut Vec<In>) -> Result<(), Fault> {
        let s = String::from_utf8_lossy(&bytes).to_string();
        if s == "BAD" {
            out.push(In("before-bad".into()));
            return Err(BadRequest::fault("bad"));
        }
        out.push(In(s));
        Ok(())
    }

    fn end(&mut self, _: &mut Vec<In>) -> Result<(), Fault> {
        Ok(())
    }
}

/// Writes an input as text, and `<END>` for clean end. `ENCPANIC` panics.
struct ReqEnc;

impl Encoder<In> for ReqEnc {
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

impl RespDec {
    fn decode(&mut self, bytes: Bytes, out: &mut Vec<Out>) -> Result<(), Fault> {
        if self.status != 200 {
            return Err(Fault::new("refused")
                .with(ErrorCode("refused".into()))
                .with(respan_gateway_http::Status(self.status))
                .with(()));
        }
        let s = String::from_utf8_lossy(&bytes).to_string();
        match s.as_str() {
            "DONE" => self.done = true,
            "ERR" => return Err(Upstream::fault("boom")),
            _ => out.push(Out(s)),
        }
        Ok(())
    }

    fn end(&mut self, _: &mut Vec<Out>) -> Result<(), Fault> {
        match self.done {
            true => Ok(()),
            false => Err(Truncated::fault()),
        }
    }
}

struct RespEnc;

impl Encoder<Out> for RespEnc {
    fn encode(&mut self, v: &Out, out: &mut BytesMut) -> Result<(), Fault> {
        if v.0 == "REJECT" {
            return Err(Fault::config("response encoding rejected"));
        }
        out.extend_from_slice(v.0.as_bytes());
        out.extend_from_slice(b";");
        Ok(())
    }

    fn end(&mut self, e: &Result<(), Fault>, out: &mut BytesMut) -> Result<(), Fault> {
        match e {
            Ok(()) => out.extend_from_slice(b"!"),
            Err(f) => out.extend_from_slice(
                format!("ERR {}", respan_gateway_http::error_code(f)).as_bytes(),
            ),
        }
        Ok(())
    }
}

type WireResult = Result<(http::Response<Body>, Option<Completion>), Fault>;

fn fnt<F>(f: F) -> F
where
    F: Fn(http::Request<Body>) -> BoxFuture<'static, WireResult> + Send + Sync + 'static,
{
    f
}

/// A response of `status` with body `frames`, with completed upload handoff.
fn answer(status: u16, frames: Frames) -> (http::Response<Body>, Option<Completion>) {
    let mut response = http::Response::new(Body::stream(frames));
    *response.status_mut() = StatusCode::from_u16(status).unwrap();
    (response, None)
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
    fn transport(
        &self,
        wait: bool,
        answer: fn() -> Frames,
    ) -> impl Fn(http::Request<Body>) -> BoxFuture<'static, WireResult> + Send + Sync + 'static
    {
        let seen = self.clone();
        fnt(move |c| {
            let seen = seen.clone();
            async move {
                let mut body = c.into_body().into_data_stream();
                let read = tokio::spawn(async move {
                    while let Some(f) = body.next().await {
                        let f = f
                            .map(|b| text(&b))
                            .map_err(|e| respan_gateway_http::error_code(&e).to_string());
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
fn gateway_of(
    codec: Toy,
    transport: impl Fn(http::Request<Body>) -> BoxFuture<'static, WireResult> + Send + Sync + 'static,
) -> (Gateway, Calls, Outcomes) {
    let (calls, outcomes) = (Calls::default(), Outcomes::default());
    let gw = Gateway::new()
        .client::<In, _, _>(Toy::default())
        .unwrap()
        .provider::<In, _, _>("a", codec, respan_gateway_http::connect(transport))
        .unwrap()
        .target("a")
        .unwrap()
        .plugin(calls.clone())
        .plugin(outcomes.clone());
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

fn ask() -> RequestHead {
    let mut head = respan_gateway_http::http::Request::new(()).into_parts().0;
    head.method = http::Method::POST;
    head
}

/// `frames` as a request, streaming if `streams`, through `Incoming` and
/// `Gateway::open`: the answer, and how the session ended.
async fn open(
    gw: &Gateway,
    frames: Frames,
    streams: bool,
) -> (
    Option<http::Response<Body>>,
    BoxFuture<'static, Result<(), Fault>>,
) {
    let (wire, answer) = Incoming::accept(
        http::Request::from_parts(ask(), Body::stream(frames)),
        streams,
    );
    let handled = gw.handle(wire, Extensions::new()).boxed();
    match respan_gateway_http::drive(handled, answer).await {
        Ok((resp, end)) => (Some(resp), end),
        Err(fault) => (None, future::ready(Err(fault)).boxed()),
    }
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
            Err(e) => {
                return Err(format!(
                    "{} after {out}",
                    respan_gateway_http::error_code(&e)
                ));
            }
        }
    }
    Ok((status, out))
}

fn code(end: &Result<(), Fault>) -> String {
    end.as_ref().map_or_else(
        |f| respan_gateway_http::error_code(f).to_string(),
        |()| "ok".into(),
    )
}

/// What a client reads of a result: a fault's code and message.
fn said<T>(result: Result<T, Fault>) -> Result<(), (String, String)> {
    result.map(|_| ()).map_err(|f| {
        let code = respan_gateway_http::error_code(&f).into_owned();
        (code, f.message().to_owned())
    })
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

/// B5: a prepared request's prefix comes before raw or encoded body bytes.
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

/// B5: a prebuilt native request body consumes no input and writes only its bytes.
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
    tx.unbounded_send(Err(BadRequest::fault("client broke")))
        .unwrap();
    let (resp, _) = open(&gw, body, true).await;
    let _ = read(resp).await;
    gw.idle().await;
    assert_eq!(seen.frames(), frames(&[Ok("hello"), Err("bad_request")]));
}

/// A streaming request's inputs follow, even with its body ended, and its
/// origin holds no input; a one-shot request's do not, and its input is its
/// body.
#[tokio::test]
async fn a_streaming_requests_inputs_follow_and_a_one_shots_do_not() {
    let (tx, body) = upload();
    drop(tx);
    let (wire, _answer) =
        Incoming::accept(http::Request::from_parts(ask(), Body::stream(body)), true);
    let (incoming, _outgoing) = respan_gateway_sdk::Format::bind(&Toy::default(), wire).unwrap();
    let incoming = incoming.await.unwrap();
    assert!(!incoming.is_single());
    assert!(incoming.boxed().next().await.is_none());

    let (wire, _answer) = Incoming::accept(
        http::Request::from_parts(ask(), Body::stream(chunks(vec!["q"]))),
        false,
    );
    let (incoming, _outgoing) = respan_gateway_sdk::Format::bind(&Toy::default(), wire).unwrap();
    let incoming = incoming.await.unwrap();
    assert!(incoming.is_single());
    assert_eq!(
        incoming.value().map(|v| v.cloned().ok()),
        Some(Some(In("q".into())))
    );
}

// These direct edge probes use finite sources; gateway tests exercise the
// concurrent live-source pump.
fn deliver<T: Send + Sync + 'static>(
    mut sink: EventSink<T>,
    mut source: EventStream<T>,
) -> Result<BoxFuture<'static, Result<(), Fault>>, Fault> {
    sink.prepare(&mut source)?;
    Ok(async move {
        while let Some(item) = source.next().await {
            sink.feed(item).await?;
        }
        sink.close().await
    }
    .boxed())
}

/// Drive the provider edge with one input, preserving opening separately
/// from events so tests can examine refusal classification.
fn dial(
    codec: &Toy,
    transport: impl Fn(http::Request<Body>) -> BoxFuture<'static, WireResult> + Send + Sync + 'static,
) -> BoxFuture<'static, Result<EventStream<Out>, Fault>> {
    let factory = respan_gateway_http::connect(transport);
    let (incoming, outgoing) =
        respan_gateway_sdk::Format::bind(codec, factory(Extensions::new())).unwrap();
    async move {
        let writing = deliver(outgoing, EventStream::single(In("q".into())))?;
        let (opened, written) = future::join(incoming, writing).await;
        written?;
        opened
    }
    .boxed()
}

/// Rule 5: a provider's outputs say `Pending` between the frames it sent
/// apart, so each frame is its own batch, even when all are ready.
#[tokio::test]
async fn a_providers_frames_are_read_apart() {
    let transport = fnt(|_| future::ready(Ok(answer(200, chunks(vec!["a", "b", "DONE"])))).boxed());
    let outputs = dial(&Toy::default(), transport).await.unwrap();
    let mut outputs = outputs.boxed();
    let mut cx = Context::from_waker(std::task::Waker::noop());
    let mut polls = Vec::new();
    for _ in 0..6 {
        polls.push(match outputs.poll_next_unpin(&mut cx) {
            Poll::Pending => "pending".to_string(),
            Poll::Ready(Some(Ok(Out(s)))) => s,
            Poll::Ready(Some(Err(f))) => respan_gateway_http::error_code(&f).to_string(),
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

/// Rule 2: a non-2xx answer is read whole before the call resolves, so its
/// refusal is known at its head however slowly its body comes; a 2xx one's
/// is read as far as it is ready.
#[tokio::test]
async fn a_slow_refusal_is_known_at_its_head() {
    let transport = fnt(|_| future::ready(Ok(answer(429, late("busy")))).boxed());
    let outputs = dial(&Toy::default(), transport).await.unwrap();
    let fault = outputs.refusal().unwrap();
    assert_eq!(respan_gateway_http::error_code(fault), "refused");
    assert_eq!(fault.get(), Some(&respan_gateway_http::Status(429)));

    let transport = fnt(|_| future::ready(Ok(answer(200, late("DONE")))).boxed());
    let outputs = dial(&Toy::default(), transport).await.unwrap();
    assert!(outputs.refusal().is_none(), "open, its body still to come");
}

/// Rule 2's exception: a 101 to a call that asked to switch is open at
/// once; to one that did not, it is read whole like any other non-2xx.
#[tokio::test]
async fn a_101_is_open_only_to_a_call_that_asked_to_switch() {
    let switched = || fnt(|_| future::ready(Ok(answer(101, stream::pending().boxed()))).boxed());
    let asked = Toy {
        upgrade: true,
        ..Toy::default()
    };
    let dialed = tokio::time::timeout(Duration::from_secs(2), dial(&asked, switched()));
    let outputs = dialed.await.unwrap().unwrap();
    assert!(outputs.refusal().is_none());

    let dialed = tokio::time::timeout(
        Duration::from_millis(100),
        dial(&Toy::default(), switched()),
    );
    assert!(dialed.await.is_err(), "still reading the body");
}

/// the native request relay marker is set on a call written from the client's origin
/// alone: a relay of the client's own message, not a call of its own input.
#[tokio::test]
async fn only_a_call_written_from_the_clients_origin_is_relayed() {
    let marks = Arc::new(Mutex::new(Vec::new()));
    let marking = {
        let marks = marks.clone();
        fnt(move |c| {
            lock(&marks).push(
                c.extensions()
                    .get::<respan_gateway_sdk::Extensions>()
                    .is_some_and(|e| e.get::<respan_gateway_http::SendRaw>().is_some()),
            );
            future::ready(Ok(answer(200, chunks(vec!["a", "DONE"])))).boxed()
        })
    };
    let factory = respan_gateway_http::connect(marking);
    let (wire, _answer) = Incoming::accept(
        http::Request::from_parts(ask(), Body::stream(chunks(vec!["q"]))),
        false,
    );
    let (incoming, _outgoing) = respan_gateway_sdk::Format::bind(&Toy::default(), wire).unwrap();
    let inputs = incoming.await.unwrap();
    let toy = Toy::default();
    let (incoming, outgoing) =
        respan_gateway_sdk::Format::bind(&toy, factory(Extensions::new())).unwrap();
    let written = deliver(outgoing, inputs).unwrap();
    let (opened, written) = future::join(incoming, written).await;
    opened.unwrap();
    written.unwrap();
    let (incoming, outgoing) =
        respan_gateway_sdk::Format::bind(&toy, factory(Extensions::new())).unwrap();
    let written = deliver(outgoing, EventStream::single(In("own".into()))).unwrap();
    let (opened, written) = future::join(incoming, written).await;
    opened.unwrap();
    written.unwrap();
    assert_eq!(*lock(&marks), [false, false]);
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
    let gw = Gateway::new().client::<In, _, _>(toy).unwrap();
    for method in [http::Method::POST, http::Method::HEAD] {
        let req = http::Request::builder().method(method.clone()).uri("/x");
        let req = req.body(respan_gateway_http::Body::full("q")).unwrap();
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
        let req = req.body(respan_gateway_http::Body::full("q")).unwrap();
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

/// A bodiless answer whose end is not ready when its head goes out has
/// nothing to say the end with: the client leaving ends it `abandoned`.
#[tokio::test(flavor = "multi_thread")]
async fn a_bodiless_answer_whose_end_is_not_ready_ends_abandoned() {
    let transport = fnt(|_| {
        let body = stream::iter([Ok(Bytes::from("a"))])
            .chain(stream::pending())
            .boxed();
        future::ready(Ok(answer(200, body))).boxed()
    });
    let (gw, calls, outcomes) = gateway_of(Toy::default(), transport);
    let req = http::Request::builder()
        .method(http::Method::HEAD)
        .uri("/x");
    let req = req.body(respan_gateway_http::Body::full("q")).unwrap();
    let (resp, ended) = respan_gateway_http::serve(&gw, req).await;
    assert_eq!(resp.status(), 200);
    drop(resp);
    let end = ended.await;
    gw.idle().await;
    assert_eq!(
        (code(&end), codes(calls.all()), codes(outcomes.all())),
        (
            "abandoned".to_string(),
            vec!["abandoned".to_string()],
            vec!["abandoned".to_string()]
        )
    );
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
impl respan_gateway_sdk::Plugin for ReadToEnd {
    async fn on_client(
        &self,
        cx: &mut respan_gateway_sdk::ClientContext,
    ) -> Result<Option<respan_gateway_sdk::Response>, Fault> {
        let mut resp = cx.call("a").await?;
        resp.until_end().await?;
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
        .client::<In, _, _>(Toy::default())
        .unwrap()
        .provider::<In, _, _>("a", Toy::default(), respan_gateway_http::connect(transport))
        .unwrap()
        .plugin(calls.clone())
        .plugin(ReadToEnd);
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
impl respan_gateway_sdk::Plugin for Rewrites {
    async fn on_provider(&self, px: &mut respan_gateway_sdk::ProviderContext) -> Result<(), Fault> {
        px.filter(|input: &mut In| {
            *input = In("the plugin's own prompt".into());
            Ok(())
        })
    }
}

/// Answers with a call of its own input to `a`, not the client's.
struct Asks;

#[async_trait]
impl respan_gateway_sdk::Plugin for Asks {
    async fn on_client(
        &self,
        cx: &mut respan_gateway_sdk::ClientContext,
    ) -> Result<Option<respan_gateway_sdk::Response>, Fault> {
        let call = cx.call("a").with_input(In("own prompt".into()));
        Ok(Some(call.await?))
    }
}

/// What the provider got, as (`relayed`, body), of a client's `question`
/// through `plugin`.
async fn provider_saw(plugin: impl respan_gateway_sdk::Plugin) -> Vec<(bool, String)> {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let noting = {
        let seen = seen.clone();
        fnt(move |c| {
            let seen = seen.clone();
            async move {
                let relayed = c
                    .extensions()
                    .get::<respan_gateway_sdk::Extensions>()
                    .is_some_and(|e| e.get::<respan_gateway_http::SendRaw>().is_some());
                let body: Vec<_> = c.into_body().into_data_stream().collect().await;
                let body = body
                    .into_iter()
                    .map(|b| text(&b.unwrap()))
                    .collect::<String>();
                lock(&seen).push((relayed, body));
                Ok(answer(200, chunks(vec!["a", "DONE"])))
            }
            .boxed()
        })
    };
    let gw = Gateway::new()
        .client::<In, _, _>(Toy::default())
        .unwrap()
        .provider::<In, _, _>("a", Toy::default(), respan_gateway_http::connect(noting))
        .unwrap()
        .target("a")
        .unwrap()
        .plugin(plugin);
    let (resp, _) = open(&gw, chunks(vec!["client question"]), false).await;
    let _ = read(resp).await;
    lock(&seen).clone()
}

/// the native request relay marker means the session's own inputs to a format of the
/// same name, as the plugins' filters left them: a filter-edited input
/// still relays (a redactor keeps a subscription passthrough working); a
/// call with its own input (`Call::with_input`) does not.
#[tokio::test(flavor = "multi_thread")]
async fn a_filter_edited_input_still_relays_but_a_calls_own_input_does_not() {
    let edited = provider_saw(Rewrites).await;
    assert_eq!(
        edited,
        [(false, "the plugin's own prompt<END>".to_string())]
    );
    let own = provider_saw(Asks).await;
    assert_eq!(own, [(false, "own prompt<END>".to_string())]);
}

/// L2-1: a HEAD whose provider answer is complete and ready at once: the
/// client got all a HEAD can carry, yet how does the session end?
#[tokio::test(flavor = "multi_thread")]
async fn a_complete_bodiless_answer_ends_ok() {
    let transport = fnt(|_| future::ready(Ok(answer(200, chunks(vec!["a", "DONE"])))).boxed());
    let (gw, calls, outcomes) = gateway_of(Toy::default(), transport);
    let req = http::Request::builder()
        .method(http::Method::HEAD)
        .uri("/x");
    let req = req.body(respan_gateway_http::Body::full("q")).unwrap();
    let (resp, ended) = respan_gateway_http::serve(&gw, req).await;
    assert_eq!(resp.status(), 200);
    let got = read(Some(resp)).await;
    let end = ended.await;
    gw.idle().await;
    assert_eq!(
        (code(&end), codes(calls.all()), codes(outcomes.all())),
        (
            "ok".to_string(),
            vec!["ok".to_string()],
            vec!["ok".to_string()]
        ),
        "client read {got:?}"
    );
}

/// A complete bodiless answer whose ready body is far past any bound on
/// bytes (a plugin or in-process dialer answering a HEAD with its whole
/// content) still ends `ok`: its end was ready, and is read through.
#[tokio::test]
async fn a_complete_bodiless_answer_with_a_large_ready_body_ends_ok() {
    let transport = fnt(|_| {
        let big = "a".repeat(4 << 20);
        let body = stream::iter([Ok(Bytes::from(big)), Ok(Bytes::from("DONE"))]).boxed();
        future::ready(Ok(answer(200, body))).boxed()
    });
    let (gw, calls, outcomes) = gateway_of(Toy::default(), transport);
    let req = http::Request::builder()
        .method(http::Method::HEAD)
        .uri("/x");
    let req = req.body(respan_gateway_http::Body::full("q")).unwrap();
    let (resp, ended) = respan_gateway_http::serve(&gw, req).await;
    assert_eq!(resp.status(), 200);
    let _ = read(Some(resp)).await;
    let end = ended.await;
    gw.idle().await;
    assert_eq!(
        (code(&end), codes(calls.all()), codes(outcomes.all())),
        (
            "ok".to_string(),
            vec!["ok".to_string()],
            vec!["ok".to_string()]
        )
    );
}

/// A HEAD answer whose body only ever yields, or is always ready and never
/// ends, still has its head sent: the drain is bounded, on the thread that
/// runs it.
#[test]
fn a_bodiless_answer_whose_body_never_settles_does_not_hold_the_adapter() {
    type Never = stream::BoxStream<'static, Result<Bytes, Fault>>;
    let yielding = || -> Never {
        stream::poll_fn(|cx| {
            cx.waker().wake_by_ref();
            Poll::<Option<Result<Bytes, Fault>>>::Pending
        })
        .boxed()
    };
    let endless = || -> Never { stream::repeat_with(|| Ok(Bytes::from("a"))).boxed() };
    for (which, body) in ["yielding", "endless"]
        .into_iter()
        .zip([yielding as fn() -> Never, endless])
    {
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap();
            let status = rt.block_on(async {
                let transport = fnt(move |_| future::ready(Ok(answer(200, body()))).boxed());
                let (gw, _, _) = gateway_of(Toy::default(), transport);
                // Streaming, so the head goes before any frame, as a yielding body has none.
                let req = http::Request::builder()
                    .method(http::Method::HEAD)
                    .uri("/x")
                    .header("upgrade", "websocket");
                let req = req.body(respan_gateway_http::Body::full("q")).unwrap();
                let (resp, _ended) = respan_gateway_http::serve(&gw, req).await;
                resp.status()
            });
            let _ = tx.send(status);
            // A runtime that hung would block its drop.
            std::mem::forget(rt);
        });
        let status = rx.recv_timeout(Duration::from_secs(10));
        assert_eq!(
            status,
            Ok(StatusCode::OK),
            "the HEAD's head never left ({which})"
        );
    }
}

/// A job handed to the gateway through the HTTP adapter (as
/// `examples/jobs/jobs_background.rs` does) from an end hook: `serve` is called
/// in the hook and its future awaited in the job's task. `idle` waits for
/// the job's request, however the task is scheduled.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn idle_waits_for_a_job_handed_through_serve_from_an_end_hook() {
    #[derive(Clone)]
    struct Job;
    struct Detach(Arc<AtomicUsize>);
    #[async_trait]
    impl respan_gateway_sdk::Plugin for Detach {
        async fn on_client_end(
            &self,
            cx: &respan_gateway_sdk::ClientContext,
            _end: &Result<(), Fault>,
        ) {
            if cx.extensions().get::<Job>().is_some() {
                self.0.fetch_add(1, Ordering::SeqCst);
                return;
            }
            let mut job = http::Request::post("/x")
                .body(respan_gateway_http::Body::full("job"))
                .unwrap();
            let mut marked = Extensions::new();
            marked.insert(Job);
            job.extensions_mut().insert(marked);
            // Called here, so the request counts from here.
            let serving = respan_gateway_http::serve(&cx.gateway(), job);
            tokio::spawn(async move {
                let (resp, ended) = serving.await;
                let _ = BodyExt::collect(resp.into_body()).await;
                let _ = ended.await;
            });
        }
    }
    for round in 0..300 {
        let done = Arc::new(AtomicUsize::new(0));
        let transport = fnt(|_| future::ready(Ok(answer(200, chunks(vec!["a", "DONE"])))).boxed());
        let (gw, _, _) = gateway_of(Toy::default(), transport);
        let gw = gw.plugin(Detach(done.clone()));
        let req = http::Request::post("/x")
            .body(respan_gateway_http::Body::full("q"))
            .unwrap();
        let (resp, ended) = respan_gateway_http::serve(&gw, req).await;
        let _ = BodyExt::collect(resp.into_body()).await;
        let _ = ended.await;
        tokio::time::timeout(Duration::from_secs(2), gw.idle())
            .await
            .unwrap();
        assert_eq!(done.load(Ordering::SeqCst), 1, "round {round}");
    }
}

/// A complete bodiless answer of several small frames, all ready (a plugin's
/// or an in-process dialer's answer to a HEAD): its end was ready, so it
/// ends `ok`. (How many ready frames the drain reads is the task's budget.)
#[tokio::test]
async fn a_complete_bodiless_answer_of_many_ready_frames_ends_ok() {
    let transport = fnt(|_| {
        let frames = (0..8).map(|_| Ok(Bytes::from("a")));
        let body = stream::iter(frames.chain([Ok(Bytes::from("DONE"))])).boxed();
        future::ready(Ok(answer(200, body))).boxed()
    });
    let (gw, calls, outcomes) = gateway_of(Toy::default(), transport);
    let req = http::Request::builder()
        .method(http::Method::HEAD)
        .uri("/x");
    let req = req.body(respan_gateway_http::Body::full("q")).unwrap();
    let (resp, ended) = respan_gateway_http::serve(&gw, req).await;
    assert_eq!(resp.status(), 200);
    let _ = read(Some(resp)).await;
    let end = ended.await;
    gw.idle().await;
    assert_eq!(
        (code(&end), codes(calls.all()), codes(outcomes.all())),
        (
            "ok".to_string(),
            vec!["ok".to_string()],
            vec!["ok".to_string()]
        )
    );
}

/// The same answer with its frames sent apart (each after a yield, as a
/// source that says `Pending` between outputs does): its end is not ready
/// when the head goes out (rule 4: outputs dropped unread end their call as
/// its end, if ready, says, else `abandoned`), however soon the next frame
/// wakes itself, so it ends `abandoned`.
#[tokio::test]
async fn a_bodiless_answer_of_frames_sent_apart_ends_abandoned() {
    let transport = fnt(|_| {
        let body = stream::unfold(0, |n| async move {
            tokio::task::yield_now().await;
            match n {
                3000 => Some((Ok(Bytes::from("DONE")), n + 1)),
                n if n > 3000 => None,
                n => Some((Ok(Bytes::from("a")), n + 1)),
            }
        })
        .boxed();
        future::ready(Ok(answer(200, body))).boxed()
    });
    let (gw, calls, outcomes) = gateway_of(Toy::default(), transport);
    let req = http::Request::builder()
        .method(http::Method::HEAD)
        .uri("/x");
    let req = req.body(respan_gateway_http::Body::full("q")).unwrap();
    let (resp, ended) = respan_gateway_http::serve(&gw, req).await;
    assert_eq!(resp.status(), 200);
    let _ = read(Some(resp)).await;
    let end = ended.await;
    gw.idle().await;
    assert_eq!(
        (code(&end), codes(calls.all()), codes(outcomes.all())),
        (
            "abandoned".to_string(),
            vec!["abandoned".to_string()],
            vec!["abandoned".to_string()]
        )
    );
}

// ===== loop 6 audit =====

/// `drain_ready` bounds its work by tokio's cooperative budget alone. Under
/// `tokio::task::unconstrained` (a public tokio API an embedder may wrap a
/// request's future in) the budget is never spent, so a bodiless answer
/// whose body is always ready and never ends (an in-process dialer's
/// endless stream) holds the thread forever: `serve` never returns.
/// The doc says such a body "cannot hold the thread".
#[test]
fn l6_an_endless_ready_bodiless_answer_never_holds_the_thread() {
    let (tx, rx) = std::sync::mpsc::channel();
    // Its own thread and runtime, leaked if it spins.
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async move {
            let transport = fnt(|_| {
                let body = stream::repeat_with(|| Ok(Bytes::from("a"))).boxed();
                future::ready(Ok(answer(200, body))).boxed()
            });
            let (gw, _calls, _outcomes) = gateway_of(Toy::default(), transport);
            let req = http::Request::builder()
                .method(http::Method::HEAD)
                .uri("/x");
            let req = req.body(respan_gateway_http::Body::full("q")).unwrap();
            let (resp, _ended) =
                tokio::task::unconstrained(respan_gateway_http::serve(&gw, req)).await;
            let _ = tx.send(resp.status());
        });
    });
    let got = rx.recv_timeout(Duration::from_secs(3));
    assert!(
        got.is_ok(),
        "serve of a HEAD did not return in 3 s: the drain spins"
    );
}

/// A transport may open its response while upload delivery is still in
/// progress. Reaching response EOF must wait for that delivery and report
/// a late write failure through both provider and client end hooks.
#[tokio::test]
async fn response_opening_does_not_finish_an_outstanding_transport_write() {
    for failure in [false, true] {
        let (finished, completion) = respan_gateway_sdk::futures::channel::oneshot::channel();
        let completion =
            async move { completion.await.unwrap_or_else(|_| Err(Fault::abandoned())) }.boxed();
        let completion = Arc::new(Mutex::new(Some(completion)));
        let transport = fnt(move |_| {
            let completion = lock(&completion).take().unwrap();
            async move {
                let mut response = answer(200, chunks(vec!["a", "DONE"]));
                response.1 = Some(completion);
                Ok(response)
            }
            .boxed()
        });
        let (gateway, calls, outcomes) = gateway_of(Toy::default(), transport);
        let (response, ended) = tokio::time::timeout(
            Duration::from_secs(1),
            open(&gateway, chunks(vec!["q"]), false),
        )
        .await
        .unwrap();
        assert_eq!(response.as_ref().unwrap().status(), 200);
        let mut reading = read(response).boxed();
        assert!(reading.as_mut().now_or_never().is_none());
        assert!(
            calls.all().is_empty(),
            "response EOF settled an unfinished write"
        );
        let end = if failure {
            Err(Upstream::fault("late write failure"))
        } else {
            Ok(())
        };
        finished.send(end).unwrap();
        let _ = reading.await;
        let expected = if failure { "upstream" } else { "ok" };
        assert_eq!(code(&ended.await), expected);
        gateway.idle().await;
        assert_eq!(codes(calls.all()), [expected]);
        assert_eq!(codes(outcomes.all()), [expected]);
    }
}

/// A refused handshake is a complete answer even if its transport would
/// otherwise wait forever for the client to send session inputs.
#[tokio::test]
async fn refusal_does_not_wait_for_transport_write_completion() {
    let transport = fnt(|_| {
        async {
            let mut response = answer(429, chunks(vec!["busy"]));
            response.1 = Some(future::pending().boxed());
            Ok(response)
        }
        .boxed()
    });
    let (gateway, calls, _) = gateway_of(Toy::default(), transport);
    tokio::time::timeout(Duration::from_secs(1), async {
        let (response, ended) = open(&gateway, chunks(vec!["q"]), true).await;
        assert_eq!(response.as_ref().unwrap().status(), 429);
        let _ = read(response).await;
        assert_eq!(code(&ended.await), "refused");
        gateway.idle().await;
        assert_eq!(codes(calls.all()), ["refused"]);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn accepted_incoming_moves_into_one_connection() {
    let (incoming, reply) = Incoming::accept(
        http::Request::from_parts(ask(), Body::stream(chunks(vec!["q"]))),
        false,
    );
    assert_eq!(incoming.request().method(), http::Method::POST);
    // Incoming is move-only: binding transfers exclusive ownership, rather
    // than keeping a clone whose second bind must fail at runtime.
    let connection = respan_gateway_sdk::Format::bind(&Toy::default(), incoming).unwrap();
    drop(connection);
    assert!(reply.await.is_none());
}

#[tokio::test]
async fn transport_factory_is_lazy_and_keeps_attempt_extensions_separate() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let transport = fnt({
        let seen = seen.clone();
        move |call| {
            lock(&seen).push(
                *call
                    .extensions()
                    .get::<Extensions>()
                    .unwrap()
                    .get::<usize>()
                    .unwrap(),
            );
            async { Ok(answer(200, chunks(vec!["a", "DONE"]))) }.boxed()
        }
    });
    let factory = respan_gateway_http::connect(transport);
    let connection = |id| {
        let mut extensions = Extensions::new();
        extensions.insert(id);
        respan_gateway_sdk::Format::bind(&Toy::default(), factory(extensions)).unwrap()
    };
    let first = connection(1usize);
    let second = connection(2usize);
    assert!(lock(&seen).is_empty());
    let drive = |(incoming, outgoing): respan_gateway_sdk::Connection<Out, In>| async move {
        let writing = deliver(outgoing, EventStream::single(In("q".into()))).unwrap();
        let (opened, written) = future::join(incoming, writing).await;
        opened.unwrap();
        written.unwrap();
    };
    future::join(drive(first), drive(second)).await;
    let mut seen = lock(&seen).clone();
    seen.sort();
    assert_eq!(seen, [1, 2]);
}

#[tokio::test]
async fn an_unmatched_owned_incoming_does_not_hold_a_rejection_open() {
    let gateway = Gateway::new();
    let (transport, answer) = Incoming::accept(
        http::Request::from_parts(ask(), Body::stream(chunks(vec!["q"]))),
        false,
    );
    let result = tokio::time::timeout(
        Duration::from_secs(1),
        respan_gateway_http::drive(gateway.handle(transport, Extensions::new()).boxed(), answer),
    )
    .await
    .unwrap();
    assert_eq!(
        respan_gateway_http::error_code(&result.err().unwrap()),
        "not_found"
    );
}

#[tokio::test]
async fn a_completed_handler_keeps_an_already_ready_reply() {
    let (transport, answer) = Incoming::accept(
        http::Request::from_parts(ask(), Body::stream(chunks(vec!["q"]))),
        false,
    );
    let (incoming, outgoing) =
        respan_gateway_sdk::Format::bind(&Toy::default(), transport).unwrap();
    incoming.await.unwrap();
    let mut writing = deliver(outgoing, EventStream::refused(Upstream::fault("refused"))).unwrap();
    // Queue the encoded refusal, leaving its delivery pending.
    assert!(writing.as_mut().now_or_never().is_none());
    let handled = future::ready(Err(Upstream::fault("handling failed"))).boxed();
    let (response, _) = respan_gateway_http::drive(handled, answer).await.unwrap();
    assert!(!response.status().is_success());
}

#[tokio::test]
async fn response_encoding_failure_closes_without_reading_a_pending_tail() {
    for opened in [false, true] {
        let transport = fnt(move |_| {
            let values = if opened {
                vec!["a", "REJECT"]
            } else {
                vec!["REJECT"]
            };
            let body = chunks(values).chain(stream::pending()).boxed();
            future::ready(Ok(answer(200, body))).boxed()
        });
        let (gateway, calls, outcomes) = gateway_of(Toy::default(), transport);
        tokio::time::timeout(Duration::from_secs(1), async {
            let (response, ended) = open(&gateway, chunks(vec!["q"]), false).await;
            let expected = if opened {
                (200, "a;ERR config")
            } else {
                (500, "ERR config")
            };
            assert_eq!(read(response).await, Ok((expected.0, expected.1.into())));
            assert_eq!(code(&ended.await), "config");
            gateway.idle().await;
            assert_eq!(codes(calls.all()), ["config"]);
            assert_eq!(codes(outcomes.all()), ["config"]);
        })
        .await
        .expect("an encoder failure must not wait for another event");
    }
}

#[tokio::test]
async fn sink_send_fault_waits_for_its_refusal_bytes() {
    let (transport, answer) = Incoming::accept(
        http::Request::from_parts(ask(), Body::stream(chunks(vec!["q"]))),
        false,
    );
    let (incoming, mut outgoing) =
        respan_gateway_sdk::Format::bind(&Toy::default(), transport).unwrap();
    incoming.await.unwrap();
    outgoing.prepare(&mut EventStream::empty()).unwrap();
    let fault = Upstream::fault("failed output");
    let mut sending = outgoing.send(Err(fault.clone())).boxed();
    assert!(
        sending.as_mut().now_or_never().is_none(),
        "flush must wait for the body consumer"
    );
    let (response, ended) = answer.await.unwrap();
    assert_eq!(read(Some(response)).await, Ok((502, "ERR upstream".into())));
    assert_eq!(said(sending.await), said(Err::<(), _>(fault.clone())));
    assert_eq!(said(ended.await), said(Err::<(), _>(fault)));
}

#[tokio::test]
async fn upload_sink_flush_waits_for_its_terminal_body_error() {
    let transport = fnt(|call| {
        future::ready(Ok(answer(200, call.into_body().into_data_stream().boxed()))).boxed()
    });
    let factory = respan_gateway_http::connect(transport);
    let (incoming, mut outgoing) =
        respan_gateway_sdk::Format::bind(&Toy::default(), factory(Extensions::new())).unwrap();
    let mut source = EventStream::stream(stream::pending());
    outgoing.prepare(&mut source).unwrap();
    let fault = BadRequest::fault("broken upload");
    let mut sending = outgoing.send(Err(fault.clone())).boxed();
    assert!(
        sending.as_mut().now_or_never().is_none(),
        "terminal body error is still unread"
    );
    let mut response = incoming.await.unwrap();
    assert_eq!(
        said(response.next().await.unwrap()),
        said(Err::<(), _>(fault.clone()))
    );
    assert_eq!(said(sending.await), said(Err::<(), _>(fault)));
}
