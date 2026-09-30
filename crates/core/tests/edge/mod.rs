//! A toy format on the core's public API alone, on the edge contract
//! ([`Format`]): no codec and no bytes. A client wire carries its frames as
//! text, each one input; a provider's dialer answers a status and text
//! frames, each one event: `DONE` completes it, `ERR` fails it, `PANIC`
//! panics its stream, and one that ends without `DONE` is truncated. The
//! edges retain their own metadata and report actual writer completion.
#![allow(dead_code, clippy::unwrap_used)]
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::task::Poll;

use respan_gateway_core::futures::Sink;
use respan_gateway_core::futures::channel::{mpsc, oneshot};
use respan_gateway_core::futures::future::BoxFuture;
use respan_gateway_core::futures::stream::BoxStream;
use respan_gateway_core::futures::{FutureExt, StreamExt, stream};
use respan_gateway_core::*;

#[derive(Clone, PartialEq, Debug)]
pub struct In(pub String);
#[derive(Clone, PartialEq, Debug)]
pub struct Out(pub String);

/// The toy's own hint, as HTTP's status is for HTTP.
#[derive(Clone)]
pub struct Wire(pub u16);

/// The number the toy writes for `fault`: its hint, else its code's.
pub fn number(fault: &Fault) -> u16 {
    fault.get::<Wire>().map_or_else(
        || match fault.code() {
            "bad_request" => 400,
            "not_found" => 404,
            "upstream" | "truncated" => 502,
            _ => 500,
        },
        |w| w.0,
    )
}

/// The injection point that panics, for the panic matrix; 0 is none.
pub static PANIC: AtomicUsize = AtomicUsize::new(0);

/// The injection points that fired, for the matrix to check none is idle.
pub static FIRED: Mutex<Vec<usize>> = Mutex::new(Vec::new());

pub fn boom(n: usize) {
    if PANIC.load(Ordering::SeqCst) == n {
        lock(&FIRED).push(n);
        panic!("injected panic {n}");
    }
}

pub type Text = BoxStream<'static, Result<String>>;

/// Private protocol metadata: the client's head and optional whole body.
pub struct ToyOrigin {
    pub head: String,
    pub body: Option<String>,
}

/// How a toy client edge treats its reply, for the drop sweep: 0 as it
/// should; 1 drops its completion sender at once and writes on; 2 writes
/// the head alone, dropping outputs unread, then completes successfully.
pub type Quirk = u8;

/// A client connection as the adapter hands it over.
pub struct ToyWire {
    frames: Text,
    streams: bool,
    quirk: Quirk,
    slot: oneshot::Sender<BoxFuture<'static, (ToyReply, End)>>,
}

/// What the adapter writes: a status and text frames, `Err` if it broke off.
pub struct ToyReply {
    pub status: u16,
    pub body: Text,
    /// The provider's head, its status, if it reached the client (a relay).
    pub head: Option<u16>,
}

/// A request as a test builds it.
pub struct Req {
    pub frames: Text,
    pub streams: bool,
    pub quirk: Quirk,
}

impl Io {
    /// The wire, and the adapter's future of the reply and how it ended, as
    /// the writer reports: `None` if the gateway dropped the wire
    /// unanswered.
    pub fn accept(req: Req) -> (Io, BoxFuture<'static, Option<(ToyReply, End)>>) {
        let (slot, reply) = oneshot::channel::<BoxFuture<'static, (ToyReply, End)>>();
        let wire = ToyWire {
            frames: req.frames,
            streams: req.streams,
            quirk: req.quirk,
            slot,
        };
        let reply = reply.then(|writer| async move {
            match writer {
                Ok(writer) => Some(writer.await),
                Err(_) => None,
            }
        });
        (Io::Client(wire), reply.boxed())
    }
}

pub struct Toy;

impl Format<Io, In, Out> for Toy {
    fn name(&self) -> &str {
        "toy"
    }

    fn matches(&self, io: &Io) -> bool {
        boom(17);
        matches!(io, Io::Client(_))
    }

    fn bind(&self, wire: Io) -> Result<Connection<In, Out>> {
        accept_toy(wire, None)
    }
}

impl Format<Io, Out, In> for Toy {
    fn name(&self) -> &str {
        "toy"
    }

    fn bind(&self, io: Io) -> Result<Connection<Out, In>> {
        boom(12);
        let Io::Provider { dialer, extensions } = io else {
            return Err(Fault::config("a provider needs outgoing I/O"));
        };
        let dial = dialer.0;
        let (answer, answered) = oneshot::channel();
        let incoming = async move { answered.await.map_err(|_| Fault::abandoned()) };
        let outgoing = respan_gateway_core::write(move |flow: Events<In>| {
            if extensions.get::<Unsendable>().is_some() {
                return Err(Fault::config("the toy cannot send this call"));
            }
            let relayed = flow.metadata::<ToyOrigin>("toy");
            let text = if flow.is_lazy() {
                relayed.and_then(|o| o.body.clone())
            } else {
                flow.value().transpose()?.map(|v| v.0.clone())
            };
            let streams = !flow.is_single();
            let relayed = relayed.is_some();
            let body = if streams {
                written(flow)
            } else {
                stream::iter([Ok(format!("{}<END>", text.unwrap_or_default()))]).boxed()
            };
            let call = ToyCall {
                relayed,
                streams,
                body,
                extensions,
            };
            let answering = dial(call);
            Ok(async move {
                let result = answering.await?;
                let status = result.status;
                let flow = if status != 200 {
                    Events::refused(
                        Fault::new("x", "x")
                            .with(Wire(status))
                            .with_answer("toy", ()),
                    )
                } else {
                    Events::stream(events(result.body))
                }
                .with_metadata("toy", status);
                let _ = answer.send(flow);
                Ok(())
            })
        });
        Ok((incoming.boxed(), Box::pin(outgoing)))
    }
}

/// Build the toy edge, optionally counting lazy decodes for the relay test.
#[allow(clippy::type_complexity)]
pub fn accept_toy(
    io: Io,
    decodes: Option<Arc<AtomicUsize>>,
) -> Result<(
    BoxFuture<'static, Result<Events<In>>>,
    Pin<Box<dyn Sink<Events<Out>, Error = Fault> + Send>>,
)> {
    boom(18);
    let Io::Client(wire) = io else {
        return Err(Fault::config("a client needs incoming I/O"));
    };
    let ToyWire {
        frames,
        streams,
        quirk,
        slot,
    } = wire;
    let incoming = async move {
        boom(19);
        if streams {
            return Ok(Events::stream(inputs(frames)).with_metadata(
                "toy",
                ToyOrigin {
                    head: "/".into(),
                    body: None,
                },
            ));
        }
        let (mut frames, mut all) = (frames, String::new());
        while let Some(frame) = frames.next().await {
            all.push_str(&frame?);
        }
        let body = all.clone();
        Ok(Events::lazy(move || {
            boom(14);
            if let Some(decodes) = &decodes {
                decodes.fetch_add(1, Ordering::SeqCst);
            }
            match body.as_str() {
                "BAD" => Err(Fault::bad_request("bad")),
                _ => Ok(In(body.clone())),
            }
        })
        .with_metadata(
            "toy",
            ToyOrigin {
                head: "/".into(),
                body: Some(all),
            },
        ))
    };
    let outgoing = respan_gateway_core::write(move |flow| {
        Ok(async move {
            let (reply, end) = write(flow, streams, quirk).await;
            let (observe, observed) = oneshot::channel();
            if slot
                .send(
                    async move {
                        (
                            reply,
                            observed
                                .map(|e| e.unwrap_or_else(|_| Err(Fault::abandoned())))
                                .boxed(),
                        )
                    }
                    .boxed(),
                )
                .is_err()
            {
                return Err(Fault::abandoned());
            }
            let result = end.await;
            let _ = observe.send(result.clone());
            result
        })
    });
    Ok((incoming.boxed(), Box::pin(outgoing)))
}

/// A call's extension that makes the toy's `dial` fail at once, unsent.
#[derive(Clone)]
pub struct Unsendable;

/// A client's frames as inputs; `BAD` is `before-bad`, then a failure.
fn inputs(frames: Text) -> BoxStream<'static, Result<In>> {
    frames
        .flat_map(|frame| {
            boom(1);
            stream::iter(match frame {
                Ok(text) if text == "BAD" => {
                    vec![Ok(In("before-bad".into())), Err(Fault::bad_request("bad"))]
                }
                Ok(text) => vec![Ok(In(text))],
                Err(fault) => vec![Err(fault)],
            })
        })
        .boxed()
}

/// A call's inputs inputs as text frames: each its text, then `<END>` if
/// they ended cleanly, else the failure.
fn written(inputs: Events<In>) -> Text {
    stream::unfold(Some(inputs), |inputs| async move {
        let mut inputs = inputs?;
        match inputs.next().await {
            Some(Ok(input)) => Some((Ok(input.0), Some(inputs))),
            Some(Err(fault)) => Some((Err(fault), None)),
            None => Some((Ok("<END>".into()), None)),
        }
    })
    .boxed()
}

/// A provider's text frames as events.
fn events(body: Text) -> BoxStream<'static, Result<Out>> {
    stream::unfold(Some((body, false)), |state| async move {
        let (mut body, mut done) = state?;
        loop {
            let frame = body.next().await;
            boom(5);
            return match frame {
                Some(Ok(text)) => match text.as_str() {
                    "DONE" => {
                        done = true;
                        continue;
                    }
                    "ERR" => Some((Err(Fault::upstream("boom")), None)),
                    "PANIC" => panic!("provider stream panic"),
                    _ => Some((Ok(Out(text)), Some((body, done)))),
                },
                Some(Err(fault)) => Some((Err(fault), None)),
                None if done => None,
                None => Some((Err(Fault::truncated()), None)),
            };
        }
    })
    .boxed()
}

/// Writes a reply, and how it ended once told: a refusal as one; an open
/// answer's head at once if it streams, else with its first bytes, a
/// failure before any refused.
async fn write(flow: Events<Out>, streams: bool, quirk: Quirk) -> (ToyReply, End) {
    let head = flow.metadata::<u16>("toy").copied();
    let refusal = flow.refusal().cloned();
    let (tell, told) = oneshot::channel();
    let complete: Completion = Box::new(move |end| {
        let _ = tell.send(end);
    });
    let outputs = match refusal {
        Some(fault) => Err(fault),
        None => Ok(flow),
    };
    let reply = written_reply(head, outputs, complete, streams, quirk).await;
    (
        reply,
        told.map(|end| end.unwrap_or_else(|_| Err(Fault::abandoned())))
            .boxed(),
    )
}

type Completion = Box<dyn FnOnce(Result<()>) + Send>;

async fn written_reply(
    head: Option<u16>,
    outputs: Result<Events<Out>>,
    complete: Completion,
    streams: bool,
    quirk: Quirk,
) -> ToyReply {
    let mut writer = Writer {
        outputs: Events::stream(stream::empty()),
        ended: Some(complete),
        peeked: None,
        done: false,
        head,
    };
    let outputs = match outputs {
        Ok(outputs) => outputs,
        Err(fault) => {
            let status = number(&fault);
            writer.peeked = Some(Some(Err(fault)));
            return writer.reply(status);
        }
    };
    writer.outputs = outputs;
    if quirk == 1 {
        drop(writer.ended.take());
    }
    if !streams {
        let first = writer.outputs.next().await;
        if let Some(Err(fault)) = &first {
            let status = number(fault);
            writer.peeked = Some(first);
            return writer.reply(status);
        }
        writer.peeked = Some(first);
    }
    if quirk == 2 {
        writer.outputs = Events::stream(stream::empty());
        writer.peeked = None;
        writer.done = true;
        writer.end(Ok(()));
    }
    writer.reply(200)
}

struct Writer {
    outputs: Events<Out>,
    ended: Option<Completion>,
    peeked: Option<Option<Result<Out>>>,
    done: bool,
    head: Option<u16>,
}

impl Writer {
    fn reply(self, status: u16) -> ToyReply {
        let head = self.head;
        let body = stream::unfold(self, |mut w| async move { w.frame().await.map(|f| (f, w)) });
        ToyReply {
            status,
            body: body.boxed(),
            head,
        }
    }

    async fn frame(&mut self) -> Option<Result<String>> {
        if self.done {
            return None;
        }
        let item = match self.peeked.take() {
            Some(item) => item,
            None => self.outputs.next().await,
        };
        Some(Ok(match item {
            Some(Ok(out)) => format!("{};", out.0),
            Some(Err(fault)) => {
                self.done = true;
                let text = format!("ERR {}", fault.code());
                self.end(Err(fault));
                text
            }
            None => {
                self.done = true;
                self.end(Ok(()));
                "!".into()
            }
        }))
    }

    fn end(&mut self, end: Result<()>) {
        if let Some(ended) = self.ended.take() {
            ended(end);
        }
    }
}

/// What a toy provider is sent.
pub struct ToyCall {
    pub relayed: bool,
    pub streams: bool,
    pub body: Text,
    pub extensions: Extensions,
}

/// What a toy provider answers.
pub struct Answer {
    pub status: u16,
    pub body: Text,
}

pub fn answer(status: u16, body: Text) -> Answer {
    Answer { status, body }
}

pub type Dial = dyn Fn(ToyCall) -> BoxFuture<'static, Result<Answer>> + Send + Sync;
pub struct Dialer(pub Arc<Dial>);

/// One attempt's I/O, constructed by the adapter or provider factory.
pub enum Io {
    Client(ToyWire),
    Provider {
        dialer: Dialer,
        extensions: Extensions,
    },
}

pub fn connect(dialer: Dialer) -> impl Fn(Extensions) -> Io + Send + Sync {
    move |extensions| Io::Provider {
        dialer: Dialer(dialer.0.clone()),
        extensions,
    }
}

pub fn fnt<F>(f: F) -> Dialer
where
    F: Fn(ToyCall) -> BoxFuture<'static, Result<Answer>> + Send + Sync + 'static,
{
    Dialer(Arc::new(f))
}

pub fn chunks(v: Vec<&'static str>) -> Text {
    stream::iter(v.into_iter().map(|s| Ok(s.to_string()))).boxed()
}

/// Frames that yield between each, so a reader can be dropped between them.
pub fn slow(v: Vec<&'static str>) -> Text {
    stream::unfold(v.into_iter(), |mut it| async move {
        tokio::task::yield_now().await;
        it.next().map(|s| (Ok(s.to_string()), it))
    })
    .boxed()
}

pub fn ok_resp(v: Vec<&'static str>) -> Answer {
    answer(200, chunks(v))
}

pub type End = BoxFuture<'static, Result<()>>;

/// `Gateway::handle` on the toy wire, as its adapter does it: the reply and
/// its end, or the fault the adapter writes plainly if no edge replies.
pub async fn handle(gw: &Gateway, req: Req) -> (Result<ToyReply>, End) {
    let (wire, reply) = Io::accept(req);
    let handled = gw.handle(wire, Extensions::new()).boxed();
    let (result, pending) = match futures::future::select(handled, reply).await {
        futures::future::Either::Left((end, remaining)) => (remaining.await, Err(end)),
        futures::future::Either::Right((reply, remaining)) => (reply, Ok(remaining)),
    };
    match result {
        Some((mut reply, end)) => {
            if let Ok(handle) = pending {
                // The body owns and drives the gateway until delivery finishes.
                // Dropping the body therefore cancels the entire exchange.
                reply.body = stream::unfold(
                    (reply.body, Some(handle)),
                    |(mut body, mut handle)| async move {
                        let item = if let Some(work) = handle.take() {
                            match futures::future::select(work, body.next()).await {
                                futures::future::Either::Left((_, next)) => next.await,
                                futures::future::Either::Right((item, work)) => {
                                    if item.is_none() {
                                        let _ = work.await;
                                    } else {
                                        handle = Some(work);
                                    }
                                    item
                                }
                            }
                        } else {
                            body.next().await
                        };
                        item.map(|item| (item, (body, handle)))
                    },
                )
                .boxed();
            }
            (Ok(reply), end)
        }
        None => {
            let fault = match pending {
                Err(Err(fault)) => fault,
                _ => Fault::internal("no reply"),
            };
            (Err(fault.clone()), std::future::ready(Err(fault)).boxed())
        }
    }
}

pub fn req(body: Vec<&'static str>) -> Req {
    req_of(body, false)
}

/// A request that streams its inputs, or not; its frames are `body`.
pub fn req_of(body: Vec<&'static str>, streams: bool) -> Req {
    Req {
        frames: chunks(body),
        streams,
        quirk: 0,
    }
}

pub type Upload = mpsc::UnboundedSender<Result<String>>;

/// A streaming request whose inputs are sent through the returned sender.
pub fn upload() -> (Upload, Req) {
    let (tx, rx) = mpsc::unbounded();
    let req = Req {
        frames: rx.boxed(),
        streams: true,
        quirk: 0,
    };
    (tx, req)
}

/// The same, not streaming: the body is read in full.
pub fn whole_upload() -> (Upload, Req) {
    let (tx, mut req) = upload();
    req.streams = false;
    (tx, req)
}

pub fn send(tx: &Upload, text: &'static str) {
    tx.unbounded_send(Ok(text.to_string())).unwrap();
}

/// A body as text frames, or the failure code it broke with.
pub type Frames = Vec<std::result::Result<String, String>>;

/// The frames a dialer read of a call's body, and whether it read to its
/// end.
#[derive(Clone, Default)]
pub struct Seen {
    frames: Arc<Mutex<Frames>>,
    done: Arc<AtomicBool>,
}

impl Seen {
    /// A dialer that reads the call's body in its own task, as reqwest
    /// does, and answers with `answer`, once the body ended if `wait`.
    pub fn transport(&self, wait: bool, answer: fn() -> Text) -> Dialer {
        let seen = self.clone();
        fnt(move |c| {
            let seen = seen.clone();
            async move {
                let mut body = c.body;
                let read = tokio::spawn(async move {
                    while let Some(f) = body.next().await {
                        let f = f.map_err(|e| e.code().to_string());
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

    pub fn frames(&self) -> Frames {
        lock(&self.frames).clone()
    }

    pub async fn wait_for(&self, n: usize) {
        for _ in 0..500 {
            if lock(&self.frames).len() >= n {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        panic!("the call's body never had {n} frames: {:?}", self.frames());
    }

    pub async fn finished(&self) -> Frames {
        for _ in 0..500 {
            if self.done.load(Ordering::SeqCst) {
                return self.frames();
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        panic!("the call's body never ended: {:?}", self.frames());
    }
}

/// `a`, `DONE`.
pub fn done() -> Text {
    slow(vec!["a", "DONE"])
}

/// A response that never ends after `a`.
pub fn never() -> Text {
    stream::once(async { Ok("a".to_string()) })
        .chain(stream::pending())
        .boxed()
}

pub fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// What a client got: the reply's status and frames as text, or the
/// failure it broke with.
pub async fn read(res: Result<ToyReply>) -> std::result::Result<(u16, String), String> {
    let reply = res.map_err(|f| format!("adapter: {}", f.code()))?;
    let mut body = reply.body;
    let mut out = String::new();
    while let Some(x) = body.next().await {
        match x {
            Ok(text) => out.push_str(&text),
            Err(e) => return Err(format!("{} after {out}", e.code())),
        }
    }
    Ok((reply.status, out))
}

/// The code of an end, or "ok".
pub fn code(end: &Result<()>) -> String {
    end.as_ref()
        .map_or_else(|f| f.code().to_string(), |()| "ok".into())
}

#[derive(Default, Debug)]
pub struct Log {
    pub client_start: usize,
    pub client_end: Vec<Result<()>>,
    pub prov_start: usize,
    /// Whether the call was sent, and how it ended.
    pub prov_end: Vec<(bool, Result<()>)>,
    /// Whether each call ended refused at its head.
    pub head_only: Vec<bool>,
}

impl Log {
    pub fn client_codes(&self) -> Vec<String> {
        self.client_end.iter().map(code).collect()
    }
    pub fn prov_codes(&self) -> Vec<String> {
        self.prov_end.iter().map(|(_, e)| code(e)).collect()
    }
}

/// Records every hook, and panics in the plugin points of the matrix.
#[derive(Clone, Default)]
pub struct Rec(pub Arc<Mutex<Log>>);
impl Rec {
    pub fn log(&self) -> MutexGuard<'_, Log> {
        lock(&self.0)
    }
}
#[async_trait]
impl Plugin for Rec {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>> {
        self.log().client_start += 1;
        tokio::task::yield_now().await;
        boom(20);
        let n = PANIC.load(Ordering::SeqCst);
        if n == 14 {
            // A single flow is lazy: only a read decodes it.
            let _ = cx.input::<In>();
        }
        if n == 26 {
            cx.filter(|_: &mut Out| -> Result<()> {
                boom(26);
                Ok(())
            })?;
        }
        if n == 27 {
            cx.filter(|_: &mut In| -> Result<()> {
                boom(27);
                Ok(())
            })?;
        }
        Ok(None)
    }
    async fn on_client_end(&self, _: &ClientContext, end: &Result<()>) {
        boom(22);
        self.log().client_end.push(end.clone());
    }
    async fn on_provider(&self, px: &mut ProviderContext) -> Result<()> {
        self.log().prov_start += 1;
        tokio::task::yield_now().await;
        boom(21);
        let n = PANIC.load(Ordering::SeqCst);
        if n == 24 {
            px.filter(|_: &mut Out| -> Result<()> {
                boom(24);
                Ok(())
            })?;
        }
        if n == 25 {
            struct F;
            impl Filter<Out> for F {
                fn edit(&mut self, v: Out, o: &mut Vec<Out>) -> Result<()> {
                    o.push(v);
                    Ok(())
                }
                fn end(&mut self, _: &Result<()>, _: &mut Vec<Out>) -> Result<()> {
                    boom(25);
                    Ok(())
                }
            }
            px.filter(F)?;
        }
        if n == 32 {
            px.filter(|_: &mut In| -> Result<()> {
                boom(32);
                Ok(())
            })?;
        }
        Ok(())
    }
    async fn on_provider_end(&self, px: &ProviderContext, end: &Result<()>) {
        boom(23);
        let mut log = self.log();
        log.prov_end.push((px.is_sent(), end.clone()));
        log.head_only.push(px.is_refused());
    }
}

/// Polls a future at most `left` times: a drop at every await point.
pub struct Limit<F> {
    f: Pin<Box<F>>,
    left: usize,
}
impl<F: Future> Future for Limit<F> {
    type Output = Option<F::Output>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        if self.left == 0 {
            return Poll::Ready(None);
        }
        self.left -= 1;
        match self.f.as_mut().poll(cx) {
            Poll::Ready(v) => Poll::Ready(Some(v)),
            Poll::Pending => {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }
}
pub fn limit<F: Future>(f: F, n: usize) -> Limit<F> {
    Limit {
        f: Box::pin(f),
        left: n,
    }
}

/// Answers with the first of two calls read to its content.
pub struct Racer;
#[async_trait]
impl Plugin for Racer {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>> {
        let race = ["a", "b"].map(|p| {
            let call = cx.call(p);
            Box::pin(async move {
                call.await?
                    .buffer_until(|out: &Out| !out.0.starts_with("keepalive"))
                    .await
            })
        });
        let (resp, _) = futures::future::select_ok(race).await?;
        Ok(Some(resp))
    }
}

/// Answers with call `a`, read as far as its mode says: 0 not at all,
/// 1 `until_content`, 2 `until_end`.
pub struct Mode(pub u8);
#[async_trait]
impl Plugin for Mode {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>> {
        let resp = cx.call("a").await?;
        Ok(Some(match self.0 {
            0 => resp,
            1 => {
                resp.buffer_until(|out: &Out| !out.0.starts_with("keepalive"))
                    .await?
            }
            _ => resp.until_end().await?.0,
        }))
    }
}

/// A gateway of one of four scenarios: 0 a slow success, 1 a provider that
/// reads the request's body first, 2 a race of two, 3 a cut response.
pub fn gateway(scenario: usize, rec: &Rec) -> Gateway {
    let slow_ok = fnt(|_| async { Ok(answer(200, slow(vec!["a", "b", "DONE"]))) }.boxed());
    let fast_ok = fnt(|_| async { Ok(answer(200, slow(vec!["x", "DONE"]))) }.boxed());
    let cut = fnt(|_| async { Ok(answer(200, slow(vec!["a", "ERR", "b"]))) }.boxed());
    let reads_body = fnt(|c| {
        async move {
            let mut body = c.body;
            while let Some(_f) = body.next().await {}
            Ok(answer(200, slow(vec!["a", "DONE"])))
        }
        .boxed()
    });
    let gw = Gateway::new().client::<In, _, _>(Toy).unwrap();
    let gw = match scenario {
        0 => gw
            .provider::<In, _, _>("a", Toy, connect(slow_ok))
            .unwrap()
            .target("a")
            .unwrap(),
        1 => gw
            .provider::<In, _, _>("a", Toy, connect(reads_body))
            .unwrap()
            .target("a")
            .unwrap(),
        2 => gw
            .provider::<In, _, _>("a", Toy, connect(slow_ok))
            .unwrap()
            .provider::<In, _, _>("b", Toy, connect(fast_ok))
            .unwrap()
            .plugin("racer", Racer)
            .unwrap(),
        3 => gw
            .provider::<In, _, _>("a", Toy, connect(cut))
            .unwrap()
            .target("a")
            .unwrap(),
        _ => unreachable!(),
    };
    gw.plugin("rec", rec.clone())
        .unwrap()
        .plugin("rec2", rec.clone())
        .unwrap()
}

/// Counts each filter's end, to check every one gets exactly one.
#[derive(Clone, Default)]
pub struct Ends(pub Arc<Mutex<Vec<Arc<AtomicUsize>>>>);
pub struct CountF<T>(Arc<AtomicUsize>, std::marker::PhantomData<fn() -> T>);
impl<T: Send + 'static> Filter<T> for CountF<T> {
    fn edit(&mut self, v: T, out: &mut Vec<T>) -> Result<()> {
        out.push(v);
        Ok(())
    }
    fn end(&mut self, _: &Result<()>, _: &mut Vec<T>) -> Result<()> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}
impl Ends {
    fn f<T: Send + 'static>(&self) -> CountF<T> {
        let c = Arc::new(AtomicUsize::new(0));
        lock(&self.0).push(c.clone());
        CountF(c, std::marker::PhantomData)
    }
    pub fn counts(&self) -> Vec<usize> {
        lock(&self.0)
            .iter()
            .map(|c| c.load(Ordering::SeqCst))
            .collect()
    }
}
#[async_trait]
impl Plugin for Ends {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>> {
        cx.filter::<Out>(self.f())?;
        cx.filter::<In>(self.f())?;
        tokio::task::yield_now().await;
        Ok(None)
    }
    async fn on_provider(&self, px: &mut ProviderContext) -> Result<()> {
        px.filter::<Out>(self.f())?;
        px.filter::<In>(self.f())?;
        tokio::task::yield_now().await;
        Ok(())
    }
}
