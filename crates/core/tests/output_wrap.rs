//! Output wrapping: early termination,
//! never read, panic or outlive their source; backpressure; a retry and a
//! fallback of wrapped inputs.
#![allow(clippy::unwrap_used)]

mod edge;
mod faults;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use edge::*;
use respan_gateway_core::futures::stream::BoxStream;
use respan_gateway_core::futures::{FutureExt, StreamExt, TryStreamExt, stream};
use respan_gateway_core::*;

/// A provider-edge output filter whose `end` is counted, with the end.
#[derive(Clone, Default)]
struct Meter(Arc<Mutex<Vec<String>>>);

#[async_trait]
impl Plugin for Meter {
    async fn on_provider(&self, px: &mut ProviderContext) -> Result<()> {
        struct F(Arc<Mutex<Vec<String>>>);
        impl Filter<Out> for F {
            fn edit(&mut self, v: Out, o: &mut Vec<Out>) -> Result<()> {
                lock(&self.0).push(format!("saw {}", v.0));
                o.push(v);
                Ok(())
            }
            fn end(&mut self, e: &Result<()>, _: &mut Vec<Out>) -> Result<()> {
                lock(&self.0).push(format!("end {}", code(e)));
                Ok(())
            }
        }
        px.filter(F(self.0.clone()))
    }
}

struct OutWrap(fn(EventStream<Out>) -> BoxStream<'static, Result<Out>>);

#[async_trait]
impl Plugin for OutWrap {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>> {
        let f = self.0;
        cx.wrap(move |outputs: EventStream<Out>| f(outputs))?;
        Ok(None)
    }
}

fn gw(body: fn() -> Text, wrap: OutWrap, meter: &Meter, rec: &Rec) -> Gateway {
    Gateway::new()
        .client::<In, _, _>(Toy)
        .unwrap()
        .provider(Provider::new::<In, _, _>(
            "p",
            Toy,
            connect(fnt(move |_| async move { Ok(answer(200, body())) }.boxed())),
        ))
        .unwrap()
        .plugin(rec.clone())
        .plugin(meter.clone())
        .plugin(wrap)
        .target("p")
        .unwrap()
}

fn ends(rec: &Rec) -> (usize, usize, usize, usize) {
    let l = rec.log();
    (
        l.client_start,
        l.client_end.len(),
        l.prov_start,
        l.prov_end.len(),
    )
}

async fn settle(gw: &Gateway) {
    tokio::time::timeout(Duration::from_secs(2), gw.idle())
        .await
        .expect("idle resolves");
}

fn abc() -> Text {
    slow(vec!["a", "b", "c", "DONE"])
}

/// An output wrap that ends early: the reply ends cleanly, the call once,
/// the meter's end once.
#[tokio::test]
async fn an_output_wrap_that_ends_early_ends_the_call_once() {
    let (meter, rec) = (Meter::default(), Rec::default());
    let gw = gw(abc, OutWrap(|o| o.take(1).boxed()), &meter, &rec);
    let (res, end) = handle(&gw, req(vec!["q"])).await;
    assert_eq!(read(res).await, Ok((200, "a;!".into())));
    assert_eq!(code(&end.await), "ok");
    settle(&gw).await;
    assert_eq!(ends(&rec), (1, 1, 1, 1));
    let ended = lock(&meter.0)
        .iter()
        .filter(|s| s.starts_with("end"))
        .count();
    assert_eq!(ended, 1, "{:?}", lock(&meter.0));
    // Cut cleanly, its outputs dropped unread: no one waits for the rest.
    assert_eq!(rec.log().prov_codes(), ["abandoned"]);
}

/// An output wrap that never polls its source.
#[tokio::test]
async fn an_output_wrap_that_never_polls_its_source_ends_the_call_once() {
    let (meter, rec) = (Meter::default(), Rec::default());
    let gw = gw(
        abc,
        OutWrap(|_| stream::iter([Ok(Out("own".into()))]).boxed()),
        &meter,
        &rec,
    );
    let (res, end) = handle(&gw, req(vec!["q"])).await;
    assert_eq!(read(res).await, Ok((200, "own;!".into())));
    assert_eq!(code(&end.await), "ok");
    settle(&gw).await;
    assert_eq!(ends(&rec), (1, 1, 1, 1));
    assert_eq!(rec.log().prov_codes(), ["abandoned"]);
    assert_eq!(*lock(&meter.0), ["end abandoned"]);
}

/// An output wrap whose stream panics after its first item.
#[tokio::test]
async fn an_output_wrap_that_panics_mid_stream_ends_everything_once() {
    let (meter, rec) = (Meter::default(), Rec::default());
    let gw = gw(
        abc,
        OutWrap(|o| {
            o.enumerate()
                .map(|(i, x)| {
                    if i == 1 {
                        panic!("wrap panics mid-stream");
                    }
                    x
                })
                .boxed()
        }),
        &meter,
        &rec,
    );
    let (res, end) = handle(&gw, req(vec!["q"])).await;
    assert_eq!(read(res).await, Ok((200, "a;ERR internal".into())));
    assert_eq!(code(&end.await), "internal");
    settle(&gw).await;
    assert_eq!(ends(&rec), (1, 1, 1, 1));
    // The reply's failure ends its call.
    assert_eq!(rec.log().prov_codes(), ["internal"]);
    assert_eq!(*lock(&meter.0), ["saw a", "saw b", "end internal"]);
}

/// An output wrap that swallows the provider's failure and then yields
/// items after its source ended.
#[tokio::test]
async fn an_output_wrap_that_yields_after_its_source_ended() {
    let (meter, rec) = (Meter::default(), Rec::default());
    fn failing() -> Text {
        slow(vec!["a", "ERR"])
    }
    let gw = gw(
        failing,
        OutWrap(|o| {
            o.take_while(|r| std::future::ready(r.is_ok()))
                .chain(stream::iter([Ok(Out("late".into()))]))
                .boxed()
        }),
        &meter,
        &rec,
    );
    let (res, end) = handle(&gw, req(vec!["q"])).await;
    assert_eq!(read(res).await, Ok((200, "a;late;!".into())));
    assert_eq!(code(&end.await), "ok");
    settle(&gw).await;
    assert_eq!(ends(&rec), (1, 1, 1, 1));
    // The call ended as its provider did, whatever the wrap made of it.
    assert_eq!(rec.log().prov_codes(), ["upstream"]);
}

/// Output wraps holding their source while the client reads slowly: the
/// wrap is pulled as the client reads (backpressure), never ahead.
#[tokio::test]
async fn an_output_wrap_is_pulled_only_as_the_client_reads() {
    static PULLED: AtomicUsize = AtomicUsize::new(0);
    let (meter, rec) = (Meter::default(), Rec::default());
    fn many() -> Text {
        stream::iter(
            (0..100)
                .map(|_| Ok("x".to_string()))
                .chain([Ok("DONE".into())]),
        )
        .boxed()
    }
    let gw = gw(
        many,
        OutWrap(|o| {
            o.inspect(|_| {
                PULLED.fetch_add(1, Ordering::SeqCst);
            })
            .boxed()
        }),
        &meter,
        &rec,
    );
    let (res, _end) = handle(&gw, req_of(vec!["q"], true)).await;
    let mut body = res.unwrap().body;
    let _ = body.next().await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    let pulled = PULLED.load(Ordering::SeqCst);
    assert!(
        pulled <= 2,
        "the wrap was pulled {pulled} times for one read"
    );
}

/// A retry after a call read part of a wrapped session: the retry must get
/// an `Err`, never a clean empty upload; and the fault it gets says whose
/// fault it is.
#[tokio::test]
async fn a_retry_after_part_of_a_wrapped_session_was_read() {
    struct Retry;
    #[async_trait]
    impl Plugin for Retry {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>> {
            cx.wrap(|i: EventStream<In>| i.map_ok(|x| In(x.0.to_uppercase())))?;
            let call = cx.call("p");
            let mut resp = call.clone().await?;
            match resp
                .buffer_until(|out: &Out| !out.0.starts_with("keepalive"))
                .await
            {
                Ok(()) => Ok(Some(resp)),
                Err(_) => Ok(Some(call.await?)),
            }
        }
    }
    let bodies = Arc::new(Mutex::new(Vec::<Frames>::new()));
    let n = Arc::new(AtomicUsize::new(0));
    let (b, nn) = (bodies.clone(), n.clone());
    let dialer = fnt(move |c| {
        let (b, nn) = (b.clone(), nn.clone());
        async move {
            let first = nn.fetch_add(1, Ordering::SeqCst) == 0;
            let mut body = c.body;
            let mut got = vec![];
            if first {
                // Reads one input, then refuses.
                if let Some(f) = body.next().await {
                    got.push(f.map_err(|e| faults::code(&e).to_string()));
                }
                lock(&b).push(got);
                return Ok(answer(503, chunks(vec![])));
            }
            let b2 = b.clone();
            tokio::spawn(async move {
                while let Some(f) = body.next().await {
                    got.push(f.map_err(|e| faults::code(&e).to_string()));
                }
                lock(&b2).push(got);
            });
            Ok(answer(200, never()))
        }
        .boxed()
    });
    let gw = Gateway::new()
        .client::<In, _, _>(Toy)
        .unwrap()
        .provider(Provider::new::<In, _, _>("p", Toy, connect(dialer)))
        .unwrap()
        .plugin(Retry);
    let (tx, r) = upload();
    send(&tx, "x");
    send(&tx, "y");
    let (res, _end) = handle(&gw, r).await;
    let got = tokio::time::timeout(Duration::from_secs(2), read(res)).await;
    assert_eq!(got.unwrap(), Ok((200, "a;ERR internal".into())));
    tokio::time::sleep(Duration::from_millis(50)).await;
    let bodies = lock(&bodies).clone();
    assert_eq!(bodies, [vec![Ok("X".into())], vec![Err("internal".into())]]);
    drop(tx);
}

/// A call that consumes no wrapped input returns its pending check to the
/// next attempt. The input reaches the fallback once and is checked once.
#[tokio::test]
async fn fallback_resumes_a_pending_input_check_once() {
    struct Gate {
        checks: Arc<AtomicUsize>,
        started: Arc<tokio::sync::Notify>,
        resume: Arc<tokio::sync::Notify>,
    }
    #[async_trait]
    impl Plugin for Gate {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>> {
            let checks = self.checks.clone();
            let started = self.started.clone();
            let resume = self.resume.clone();
            cx.wrap(move |inputs: EventStream<In>| {
                inputs.and_then(move |input| {
                    let checks = checks.clone();
                    let started = started.clone();
                    let resume = resume.clone();
                    async move {
                        checks.fetch_add(1, Ordering::SeqCst);
                        started.notify_one();
                        resume.notified().await;
                        Ok(In(input.0.to_uppercase()))
                    }
                })
            })?;
            let failed = cx.call("p").await.unwrap_err();
            assert_eq!(faults::code(&failed), "upstream");
            Ok(Some(cx.call("p").await?))
        }
    }
    let checks = Arc::new(AtomicUsize::new(0));
    let attempts = Arc::new(AtomicUsize::new(0));
    let calls = attempts.clone();
    let checked = checks.clone();
    let started = Arc::new(tokio::sync::Notify::new());
    let resume = Arc::new(tokio::sync::Notify::new());
    let gate = Gate {
        checks: checks.clone(),
        started: started.clone(),
        resume: resume.clone(),
    };
    let dialer = fnt(move |call| {
        let first = calls.fetch_add(1, Ordering::SeqCst) == 0;
        let checked = checked.clone();
        let started = started.clone();
        let resume = resume.clone();
        async move {
            let mut body = call.body;
            if first {
                tokio::select! {
                    _ = started.notified() => {},
                    item = body.next() => panic!("input escaped its pending check: {item:?}"),
                }
                assert_eq!(checked.load(Ordering::SeqCst), 1);
                resume.notify_one();
                return Err(faults::upstream("try the next attempt"));
            }
            let frames = body.try_collect::<Vec<_>>().await?;
            assert_eq!(frames, ["X", "<END>"]);
            Ok(ok_resp(vec!["answer", "DONE"]))
        }
        .boxed()
    });
    let gateway = Gateway::new()
        .client::<In, _, _>(Toy)
        .unwrap()
        .provider(Provider::new::<In, _, _>("p", Toy, connect(dialer)))
        .unwrap()
        .plugin(gate);
    let (tx, request) = upload();
    send(&tx, "x");
    drop(tx);
    let (response, end) = handle(&gateway, request).await;
    assert_eq!(read(response).await, Ok((200, "answer;!".into())));
    assert!(end.await.is_ok());
    gateway.idle().await;
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    assert_eq!(checks.load(Ordering::SeqCst), 1);
}
