//! A toy format on the core's public API alone, so the kernel's promises are
//! tested without any real format: requests and responses are strings, a
//! response body is complete once it says `DONE`, and `ERR`, `PANIC` and
//! `BAD` make its decoders fail.
#![allow(dead_code, clippy::unwrap_used)]
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::task::Poll;

use respan_gateway_core::bytes::{Bytes, BytesMut};
use respan_gateway_core::futures::channel::mpsc;
use respan_gateway_core::futures::future::BoxFuture;
use respan_gateway_core::futures::{FutureExt, StreamExt, stream};
use respan_gateway_core::*;

#[derive(Clone, Debug)]
pub struct Ask(pub String);
#[derive(Debug)]
pub struct Ans(pub u16);
#[derive(Clone, PartialEq, Debug)]
pub struct In(pub String);
#[derive(Clone, PartialEq, Debug)]
pub struct Out(pub String);

/// The injection point that panics, for the panic matrix; 0 is none.
pub static PANIC: AtomicUsize = AtomicUsize::new(0);

pub fn boom(n: usize) {
    if PANIC.load(Ordering::SeqCst) == n {
        panic!("injected panic {n}");
    }
}

pub struct Toy;

impl Format for Toy {
    type Ask = Ask;
    type Answer = Ans;
    type Input = In;
    type Output = Out;
    fn name(&self) -> &str {
        "toy"
    }
    fn accepts(&self, _: &Ask) -> bool {
        boom(17);
        true
    }
    fn request_decoder(&self, _: &Ask) -> Box<dyn Decoder<In>> {
        boom(14);
        Box::new(ReqDec)
    }
    fn request_encoder(
        &self,
        _: Option<&Ask>,
        _: &mut Bytes,
        _: bool,
    ) -> Box<dyn Encoder<In, Ask>> {
        boom(12);
        Box::new(ReqEnc { open: None })
    }
    fn response_decoder(&self, a: &Ans, _: &Ask, _: &Bytes) -> Box<dyn Decoder<Out>> {
        boom(10);
        Box::new(RespDec {
            done: false,
            status: a.0,
        })
    }
    fn is_content(&self, out: &Out) -> bool {
        boom(13);
        !out.0.starts_with("keepalive")
    }
    fn response_encoder(
        &self,
        _: Option<&Ans>,
        _: &Ask,
        _: &Bytes,
        refusal: Option<&Fault>,
    ) -> Box<dyn Encoder<Out, Ans>> {
        boom(11);
        Box::new(RespEnc {
            status: refusal.map_or(200, |f| f.status()),
        })
    }
}

/// The toy format whose request encoder opens with `bytes`, saying `done`.
pub struct Opening(pub &'static str, pub bool);

impl Format for Opening {
    type Ask = Ask;
    type Answer = Ans;
    type Input = In;
    type Output = Out;
    fn name(&self) -> &str {
        "toy"
    }
    fn accepts(&self, head: &Ask) -> bool {
        Toy.accepts(head)
    }
    fn request_decoder(&self, head: &Ask) -> Box<dyn Decoder<In>> {
        Toy.request_decoder(head)
    }
    fn request_encoder(
        &self,
        _: Option<&Ask>,
        _: &mut Bytes,
        _: bool,
    ) -> Box<dyn Encoder<In, Ask>> {
        Box::new(ReqEnc {
            open: Some((self.0, self.1)),
        })
    }
    fn response_decoder(&self, a: &Ans, c: &Ask, s: &Bytes) -> Box<dyn Decoder<Out>> {
        Toy.response_decoder(a, c, s)
    }
    fn response_encoder(
        &self,
        a: Option<&Ans>,
        ask: &Ask,
        body: &Bytes,
        refusal: Option<&Fault>,
    ) -> Box<dyn Encoder<Out, Ans>> {
        Toy.response_encoder(a, ask, body, refusal)
    }
}

/// `BAD` decodes to `before-bad` and then fails, in the same chunk.
struct ReqDec;
impl Decoder<In> for ReqDec {
    fn decode(&mut self, bytes: Bytes, out: &mut Vec<In>) -> Result<(), Fault> {
        boom(1);
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
        boom(2);
        Ask("/x".into())
    }
    fn encode(&mut self, v: &In, out: &mut BytesMut) -> Result<(), Fault> {
        if v.0 == "ENCPANIC" {
            panic!("encoder panic");
        }
        boom(3);
        out.extend_from_slice(v.0.as_bytes());
        Ok(())
    }
    fn end(&mut self, e: &Result<(), Fault>, out: &mut BytesMut) -> Result<(), Fault> {
        boom(4);
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
        boom(5);
        if self.status != 200 {
            return Err(Fault::new(self.status, "x", "x").with_answer("toy", ()));
        }
        let s = String::from_utf8_lossy(&bytes).to_string();
        match s.as_str() {
            "DONE" => self.done = true,
            "ERR" => return Err(Fault::upstream("boom")),
            "PANIC" => panic!("decoder panic"),
            _ => out.push(Out(s)),
        }
        Ok(())
    }
    fn end(&mut self, _: &mut Vec<Out>) -> Result<(), Fault> {
        boom(6);
        if self.done {
            Ok(())
        } else {
            Err(Fault::truncated())
        }
    }
}

struct RespEnc {
    status: u16,
}
impl Encoder<Out, Ans> for RespEnc {
    fn open(&mut self, _: &mut BytesMut) -> Result<bool, Fault> {
        boom(15);
        Ok(false)
    }
    fn head(&mut self, _: Option<&Out>) -> Ans {
        boom(7);
        Ans(self.status)
    }
    fn encode(&mut self, v: &Out, out: &mut BytesMut) -> Result<(), Fault> {
        boom(8);
        out.extend_from_slice(v.0.as_bytes());
        out.extend_from_slice(b";");
        Ok(())
    }
    fn end(&mut self, e: &Result<(), Fault>, out: &mut BytesMut) -> Result<(), Fault> {
        boom(9);
        match e {
            Ok(()) => out.extend_from_slice(b"!"),
            Err(f) => out.extend_from_slice(format!("ERR {}", f.code()).as_bytes()),
        }
        Ok(())
    }
}

pub type Send_ =
    dyn Fn(Exchange<Ask>) -> BoxFuture<'static, Result<Exchange<Ans>, Fault>> + Send + Sync;
pub struct Fnt(pub Arc<Send_>);
#[async_trait]
impl Transport for Fnt {
    type Ask = Ask;
    type Answer = Ans;
    async fn send(&self, call: Exchange<Ask>) -> Result<Exchange<Ans>, Fault> {
        (self.0)(call).await
    }
}
pub fn fnt<F>(f: F) -> Fnt
where
    F: Fn(Exchange<Ask>) -> BoxFuture<'static, Result<Exchange<Ans>, Fault>>
        + Send
        + Sync
        + 'static,
{
    Fnt(Arc::new(f))
}

pub fn chunks(v: Vec<&'static str>) -> Body {
    stream::iter(v.into_iter().map(|s| Ok(Bytes::from(s)))).boxed()
}

/// Chunks that yield between each, so a reader can be dropped between them.
pub fn slow(v: Vec<&'static str>) -> Body {
    stream::unfold(v.into_iter(), |mut it| async move {
        tokio::task::yield_now().await;
        it.next().map(|s| (Ok(Bytes::from(s)), it))
    })
    .boxed()
}

pub fn ok_resp(v: Vec<&'static str>) -> Exchange<Ans> {
    Exchange::new(Ans(200), chunks(v))
}

/// A whole request of `body`.
pub fn req(body: Vec<&'static str>) -> Exchange<Ask> {
    Exchange::new(Ask("/".into()), chunks(body))
}

/// A request that streams its inputs, or not; its body's frames are `body`.
pub fn req_of(body: Vec<&'static str>, streams: bool) -> Exchange<Ask> {
    let mut req = req(body);
    req.streams = streams;
    req
}

pub type Upload = mpsc::UnboundedSender<Result<Bytes, Fault>>;

/// A streaming request whose inputs are sent through the returned sender.
pub fn upload() -> (Upload, Exchange<Ask>) {
    let (tx, rx) = mpsc::unbounded();
    let mut req = Exchange::new(Ask("/".into()), rx.boxed());
    req.streams = true;
    (tx, req)
}

/// The same, not streaming: the body is read in full.
pub fn whole_upload() -> (Upload, Exchange<Ask>) {
    let (tx, mut req) = upload();
    req.streams = false;
    (tx, req)
}

pub fn send(tx: &Upload, text: &'static str) {
    tx.unbounded_send(Ok(Bytes::from(text))).unwrap();
}

pub fn text(bytes: &Bytes) -> String {
    String::from_utf8_lossy(bytes).to_string()
}

/// A body as text frames, or the failure code it broke with.
pub type Frames = Vec<Result<String, String>>;

/// The frames a transport read of a call's body, and whether it read to
/// the body's end.
#[derive(Clone, Default)]
pub struct Seen {
    frames: Arc<Mutex<Frames>>,
    done: Arc<AtomicBool>,
}

impl Seen {
    /// A transport that reads the call's body in its own task, as reqwest
    /// does, and answers with `answer`, once the body ended if `wait`.
    pub fn transport(&self, wait: bool, answer: fn() -> Body) -> Fnt {
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
                Ok(Exchange::new(Ans(200), answer()))
            }
            .boxed()
        })
    }

    /// The frames read so far.
    pub fn frames(&self) -> Frames {
        lock(&self.frames).clone()
    }

    /// Resolves once at least `n` frames were read.
    pub async fn wait_for(&self, n: usize) {
        for _ in 0..500 {
            if lock(&self.frames).len() >= n {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        panic!("the call's body never had {n} frames: {:?}", self.frames());
    }

    /// Resolves once the body ended, or panics after a few seconds.
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
pub fn done() -> Body {
    slow(vec!["a", "DONE"])
}

/// A response that never ends after `a`.
pub fn never() -> Body {
    stream::once(async { Ok(Bytes::from("a")) })
        .chain(stream::pending())
        .boxed()
}

pub fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// What a client got: the body's frames as text, or the failure it broke
/// with.
pub async fn read(res: Result<Exchange<Ans>, Fault>) -> Result<(u16, String), String> {
    let ex = res.map_err(|f| format!("adapter: {}", f.code()))?;
    let head = ex.head.0;
    let mut body = ex.body;
    let mut out = String::new();
    while let Some(x) = body.next().await {
        match x {
            Ok(b) => out.push_str(&text(&b)),
            Err(e) => return Err(format!("{} after {out}", e.code())),
        }
    }
    Ok((head, out))
}

/// The code of an end, or "ok".
pub fn code(end: &Result<(), Fault>) -> String {
    end.as_ref()
        .map_or_else(|f| f.code().to_string(), |()| "ok".into())
}

#[derive(Default, Debug)]
pub struct Log {
    pub client_start: usize,
    pub client_end: Vec<Result<(), Fault>>,
    pub prov_start: usize,
    /// Whether the call was sent, and how it ended.
    pub prov_end: Vec<(bool, Result<(), Fault>)>,
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
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        self.log().client_start += 1;
        tokio::task::yield_now().await;
        boom(20);
        let n = PANIC.load(Ordering::SeqCst);
        if n == 26 {
            cx.filter(|_: &mut Out| -> Result<(), Fault> { panic!("p26") })?;
        }
        if n == 27 {
            cx.filter(|_: &mut In| -> Result<(), Fault> { panic!("p27") })?;
        }
        if n == 30 {
            cx.edit_response_head(|_: &mut Ans| -> Result<(), Fault> { panic!("p30") });
        }
        Ok(None)
    }
    async fn on_client_end(&self, _: &ClientContext, end: &Result<(), Fault>) {
        boom(22);
        self.log().client_end.push(end.clone());
    }
    async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
        self.log().prov_start += 1;
        tokio::task::yield_now().await;
        boom(21);
        let n = PANIC.load(Ordering::SeqCst);
        if n == 24 {
            px.filter(|_: &mut Out| -> Result<(), Fault> { panic!("p24") })?;
        }
        if n == 25 {
            struct F;
            impl Filter<Out> for F {
                fn edit(&mut self, v: Out, o: &mut Vec<Out>) -> Result<(), Fault> {
                    o.push(v);
                    Ok(())
                }
                fn end(&mut self, _: &Result<(), Fault>, _: &mut Vec<Out>) -> Result<(), Fault> {
                    panic!("p25")
                }
            }
            px.filter(F)?;
        }
        if n == 31 {
            px.edit_call(|_: &mut Ask, _| -> Result<(), Fault> { panic!("p31") });
        }
        if n == 32 {
            px.filter(|_: &mut In| -> Result<(), Fault> { panic!("p32") })?;
        }
        Ok(())
    }
    async fn on_provider_end(&self, px: &ProviderContext, end: &Result<(), Fault>) {
        boom(23);
        self.log().prov_end.push((px.is_sent(), end.clone()));
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
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let race = ["a", "b"].map(|p| {
            let call = cx.call(p);
            Box::pin(async move { call.await?.until_content().await })
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
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let resp = cx.call("a").await?;
        Ok(Some(match self.0 {
            0 => resp,
            1 => resp.until_content().await?,
            _ => resp.until_end().await?.0,
        }))
    }
}

/// A gateway of one of four scenarios: 0 a slow success, 1 a provider that
/// reads the request's body first, 2 a race of two, 3 a cut response.
pub fn gateway(scenario: usize, rec: &Rec) -> Gateway {
    let slow_ok =
        fnt(|_| async { Ok(Exchange::new(Ans(200), slow(vec!["a", "b", "DONE"]))) }.boxed());
    let fast_ok = fnt(|_| async { Ok(Exchange::new(Ans(200), slow(vec!["x", "DONE"]))) }.boxed());
    let cut = fnt(|_| async { Ok(Exchange::new(Ans(200), slow(vec!["a", "ERR", "b"]))) }.boxed());
    let reads_body = fnt(|c| {
        async move {
            let mut body = c.body;
            while let Some(_f) = body.next().await {}
            Ok(Exchange::new(Ans(200), slow(vec!["a", "DONE"])))
        }
        .boxed()
    });
    let gw = Gateway::new().serve(Toy).unwrap();
    let gw = match scenario {
        0 => gw.provider("a", Toy, slow_ok).unwrap().target("a").unwrap(),
        1 => gw
            .provider("a", Toy, reads_body)
            .unwrap()
            .target("a")
            .unwrap(),
        2 => gw
            .provider("a", Toy, slow_ok)
            .unwrap()
            .provider("b", Toy, fast_ok)
            .unwrap()
            .plugin("racer", Racer)
            .unwrap(),
        3 => gw.provider("a", Toy, cut).unwrap().target("a").unwrap(),
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
    fn edit(&mut self, v: T, out: &mut Vec<T>) -> Result<(), Fault> {
        out.push(v);
        Ok(())
    }
    fn end(&mut self, _: &Result<(), Fault>, _: &mut Vec<T>) -> Result<(), Fault> {
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
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        cx.filter::<Out>(self.f())?;
        cx.filter::<In>(self.f())?;
        tokio::task::yield_now().await;
        Ok(None)
    }
    async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
        px.filter::<Out>(self.f())?;
        px.filter::<In>(self.f())?;
        tokio::task::yield_now().await;
        Ok(())
    }
}
