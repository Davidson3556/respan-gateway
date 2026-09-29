//! Many requests on many threads, cut at random: nothing leaks or hangs.
#![allow(clippy::unwrap_used)]

mod edge;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use edge::*;
use respan_gateway_core::futures::StreamExt;
use respan_gateway_core::futures::future::BoxFuture;
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
impl Format for Counted {
    type Input = In;
    type Output = Out;
    type Wire = ToyWire;
    type Dialer = Dialer;
    fn name(&self) -> &str {
        "toy"
    }
    fn accepts(&self, w: &ToyWire) -> bool {
        Toy.accepts(w)
    }
    fn accept(
        &self,
        w: ToyWire,
        r: futures::future::BoxFuture<'static, (Half<Out>, Ended)>,
    ) -> futures::future::BoxFuture<'static, Result<(Option<Origin>, Option<Events<In>>), Fault>>
    {
        Toy.accept(w, r)
    }
    fn input(&self, o: &Origin) -> Option<Result<In, Fault>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Toy.input(o)
    }
    fn dial(
        &self,
        d: &Dialer,
        e: Extensions,
        r: Option<&Origin>,
        o: Option<&In>,
        l: Option<Events<In>>,
    ) -> Result<BoxFuture<'static, Result<Half<Out>, Fault>>, Fault> {
        Toy.dial(d, e, r, o, l)
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
        .serve(Counted(decodes.clone()))
        .unwrap()
        .provider("a", Toy, t)
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
