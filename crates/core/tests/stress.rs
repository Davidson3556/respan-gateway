//! Many requests on many threads, cut at random: nothing leaks or hangs.
#![allow(clippy::unwrap_used)]

mod toy;

use std::time::Duration;

use respan_gateway_core::futures::StreamExt;
use respan_gateway_core::*;
use toy::*;

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
                if let Ok(ex) = res {
                    let mut body = ex.body;
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

/// A big body goes through a whole request to the provider intact.
#[tokio::test(flavor = "multi_thread")]
async fn a_big_body_relays_whole() {
    let big = respan_gateway_core::bytes::Bytes::from(vec![b'x'; 8 * 1024 * 1024]);
    let len = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let seen = len.clone();
    let t = fnt(move |c| {
        let seen = seen.clone();
        Box::pin(async move {
            let mut body = c.body;
            while let Some(x) = body.next().await {
                seen.fetch_add(x.unwrap().len(), std::sync::atomic::Ordering::SeqCst);
            }
            Ok(ok_resp(vec!["a", "DONE"]))
        })
    });
    let gw = Gateway::new()
        .serve(Toy)
        .unwrap()
        .provider("a", Toy, t)
        .unwrap()
        .target("a")
        .unwrap();
    let body = respan_gateway_core::futures::stream::iter([Ok(big)]).boxed();
    let (res, _) = handle(&gw, Exchange::new(Ask("/".into()), body)).await;
    assert_eq!(read(res).await, Ok((200, "a;!".into())));
    // the body, and the toy's end marker
    assert_eq!(
        len.load(std::sync::atomic::Ordering::SeqCst),
        8 * 1024 * 1024 + "<END>".len()
    );
}
