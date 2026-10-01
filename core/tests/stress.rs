//! Many requests on many threads, cut at random: nothing leaks or hangs.
#![allow(clippy::unwrap_used)]

mod edge;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use edge::*;
use respan_gateway_core::futures::StreamExt;
use respan_gateway_core::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn cut_requests_leave_nothing_behind() {
    let rec = Rec::default();
    let ends = Ends::default();
    let gw = gateway(2, &rec).plugin("ends", ends.clone()).unwrap();
    let mut tasks = vec![];
    for i in 0..2000u64 {
        let gw = gw.clone();
        tasks.push(tokio::spawn(async move {
            let cut = Duration::from_micros((i * 37) % 900);
            let work = async {
                let (res, _) = handle(&gw, req_of(vec!["one", "two"], i % 2 == 0)).await;
                if let Ok(reply) = res {
                    let mut body = reply.body;
                    while body.next().await.is_some() {}
                }
            };
            let _ = tokio::time::timeout(cut, work).await;
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
    tokio::time::timeout(Duration::from_secs(10), gw.idle())
        .await
        .expect("idle hung");
    let l = rec.log();
    assert_eq!(l.client_start, l.client_end.len());
    assert_eq!(l.prov_start, l.prov_end.len());
    assert!(
        ends.counts().iter().all(|c| *c == 1),
        "a filter did not end once"
    );
}

/// Counts the toy's decodes of the input in an origin, to show a relay
/// never decodes.
struct Counted(Arc<AtomicUsize>);
impl Format<Io, In, Out> for Counted {
    fn name(&self) -> &str {
        "toy"
    }

    fn matches(&self, w: &Io) -> bool {
        <Toy as Format<Io, In, Out>>::matches(&Toy, w)
    }

    fn bind(&self, w: Io) -> Result<Connection<In, Out>> {
        accept_toy(w, Some(self.0.clone()))
    }
}

impl Format<Io, Out, In> for Counted {
    fn name(&self) -> &str {
        "toy"
    }

    fn bind(&self, io: Io) -> Result<Connection<Out, In>> {
        <Toy as Format<Io, Out, In>>::bind(&Toy, io)
    }
}

/// A big body goes through a one-shot request to the provider intact, as
/// the origin's own, never decoded.
#[tokio::test(flavor = "multi_thread")]
async fn a_big_body_relays_whole() {
    let big = "x".repeat(8 * 1024 * 1024);
    let len = Arc::new(AtomicUsize::new(0));
    let seen = len.clone();
    let t = fnt(move |c| {
        let seen = seen.clone();
        Box::pin(async move {
            let mut body = c.body;
            while let Some(x) = body.next().await {
                seen.fetch_add(x.unwrap().len(), Ordering::SeqCst);
            }
            Ok(ok_resp(vec!["a", "DONE"]))
        })
    });
    let decodes = Arc::new(AtomicUsize::new(0));
    let gw = Gateway::new()
        .client::<In, _, _>(Counted(decodes.clone()))
        .unwrap()
        .provider::<In, _, _>("a", Toy, connect(t))
        .unwrap()
        .target("a")
        .unwrap();
    let frames = futures::stream::iter([Ok(big)]).boxed();
    let req = Req {
        frames,
        streams: false,
        quirk: 0,
    };
    let (res, _) = handle(&gw, req).await;
    assert_eq!(read(res).await, Ok((200, "a;!".into())));
    // the body, and the toy's end marker
    assert_eq!(len.load(Ordering::SeqCst), 8 * 1024 * 1024 + "<END>".len());
    assert_eq!(decodes.load(Ordering::SeqCst), 0, "a relay decodes nothing");
}

/// Wrapped sessions on many threads, each dropped at a different point: an
/// input wrap that swallows the cut and an output wrap, both yielding on
/// every item. Every hook ends once, the end and `idle` resolve, and the
/// provider never sees a clean end of an upload that was never finished
/// (B1).
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn wrapped_sessions_dropped_at_random_end_once() {
    struct Both;
    #[async_trait]
    impl Plugin for Both {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>> {
            cx.wrap(|inputs: EventStream<In>| {
                inputs
                    .take_while(|i| std::future::ready(i.is_ok()))
                    .then(|i| async move {
                        tokio::task::yield_now().await;
                        i
                    })
            })?;
            cx.wrap(|outputs: EventStream<Out>| {
                outputs.then(|o| async move {
                    tokio::task::yield_now().await;
                    o
                })
            })?;
            Ok(None)
        }
    }
    fn endless() -> Text {
        let a = |()| async {
            tokio::task::yield_now().await;
            Some((Ok("a".to_string()), ()))
        };
        futures::stream::unfold((), a).boxed()
    }
    let mut failures = vec![];
    for round in 0..400u64 {
        let (rec, seen) = (Rec::default(), Seen::default());
        let gw = Gateway::new()
            .client::<In, _, _>(Toy)
            .unwrap()
            .provider::<In, _, _>("p", Toy, connect(seen.transport(false, endless)))
            .unwrap()
            .plugin("rec", rec.clone())
            .unwrap()
            .plugin("both", Both)
            .unwrap()
            .target("p")
            .unwrap();
        let (tx, req) = upload();
        let sender = tokio::spawn(async move {
            for _ in 0..20 {
                if tx.unbounded_send(Ok("i".into())).is_err() {
                    break;
                }
                tokio::task::yield_now().await;
            }
            // Never finished: the upload stays open.
            std::future::pending::<()>().await;
        });
        let (res, end) = handle(&gw, req).await;
        let mut body = res.unwrap().body;
        for _ in 0..round % 7 {
            let _ = body.next().await;
        }
        for _ in 0..round % 5 {
            tokio::task::yield_now().await;
        }
        drop(body);
        let end = tokio::time::timeout(Duration::from_millis(500), end).await;
        let idle = tokio::time::timeout(Duration::from_millis(500), gw.idle()).await;
        sender.abort();
        let (c0, c1, p0, p1) = {
            let l = rec.log();
            (
                l.client_start,
                l.client_end.len(),
                l.prov_start,
                l.prov_end.len(),
            )
        };
        if end.is_err() || idle.is_err() || c0 != c1 || p0 != p1 {
            let end = end.map(|e| code(&e));
            let idle = idle.is_ok();
            failures.push(format!(
                "round {round}: end {end:?} idle {idle} hooks {c0}/{c1} {p0}/{p1}"
            ));
        }
        // Time for the provider to read what the cut left.
        tokio::time::sleep(Duration::from_millis(1)).await;
        let frames = seen.frames();
        if frames.contains(&Ok("<END>".to_string())) {
            failures.push(format!("round {round}: a clean end {frames:?}"));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
