//! Request and response ownership: values are released by idle,
//! and dropping a handle releases its request at every polling stage.
#![allow(clippy::unwrap_used)]

mod edge;
mod faults;

use std::sync::Arc;
use std::time::Duration;

use edge::*;
use respan_gateway_core::futures::stream::BoxStream;
use respan_gateway_core::futures::{FutureExt, StreamExt, TryStreamExt, stream};
use respan_gateway_core::*;

/// A value every request and call carries in its extensions.
#[derive(Clone)]
struct Marker(#[allow(dead_code)] Arc<()>);

/// Puts the marker in the request's extensions, and wraps as `how` says:
/// 0 none; 1 outputs cut after one; 2 inputs checked asynchronously;
/// 3 outputs never read by the wrap.
struct Mark(Arc<()>, u8);

#[async_trait]
impl Plugin for Mark {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>> {
        cx.extensions_mut().insert(Marker(self.0.clone()));
        match self.1 {
            1 => {
                cx.wrap(|o: EventStream<Out>| o.take(1).boxed() as BoxStream<'static, Result<Out>>)?
            }
            2 => cx.wrap(|i: EventStream<In>| {
                i.and_then(|x| async move {
                    tokio::task::yield_now().await;
                    Ok(x)
                })
                .boxed()
            })?,
            3 => cx.wrap(|_o: EventStream<Out>| {
                stream::pending().boxed() as BoxStream<'static, Result<Out>>
            })?,
            _ => {}
        }
        Ok(None)
    }
}

fn gw(mark: &Arc<()>, how: u8, body: fn() -> Text) -> Gateway {
    let dialer = fnt(move |_| async move { Ok(answer(200, body())) }.boxed());
    Gateway::new()
        .client::<In, _, _>(Toy)
        .unwrap()
        .provider::<In, _, _>("p", Toy, connect(dialer))
        .unwrap()
        .plugin(Mark(mark.clone(), how))
        .target("p")
        .unwrap()
}

async fn idle(gw: &Gateway) {
    tokio::time::timeout(Duration::from_secs(2), gw.idle())
        .await
        .expect("idle resolves");
}

/// Once `idle` resolves, no request's values are kept, whatever the client
/// edge did (every quirk), however the reply was read (to its end, partly,
/// not at all) and whether the adapter still holds the reply's body.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn idle_finds_every_requests_values_dropped() {
    let mut kept = Vec::new();
    for how in 0..4u8 {
        for quirk in 0..3u8 {
            for streams in [false, true] {
                for read_to in [0usize, 1, 99] {
                    let mark = Arc::new(());
                    let g = gw(&mark, how, || slow(vec!["a", "b", "c", "DONE"]));
                    let (tx, mut r) = upload();
                    r.streams = streams;
                    r.quirk = quirk;
                    send(&tx, "x");
                    drop(tx);
                    // A one-shot reply's head waits for its first output, which a
                    // wrap that never reads never gives: the client gives up.
                    let handled =
                        tokio::time::timeout(Duration::from_millis(300), handle(&g, r)).await;
                    let Ok((res, end)) = handled else {
                        idle(&g).await;
                        let n = Arc::strong_count(&mark) - 2;
                        if n != 0 {
                            kept.push(format!(
                                "how {how} quirk {quirk} streams {streams} gave up: {n} kept"
                            ));
                        }
                        continue;
                    };
                    // Read `read_to` frames, and keep the body.
                    let mut body = res.ok().map(|r| r.body);
                    if let Some(b) = body.as_mut() {
                        let reading = async {
                            for _ in 0..read_to {
                                if b.next().await.is_none() {
                                    break;
                                }
                            }
                        };
                        // A wrap that never reads gives nothing: the client stops waiting.
                        let _ = tokio::time::timeout(Duration::from_millis(300), reading).await;
                    }
                    if read_to != 99 {
                        body = None; // dropped partway
                    }
                    let _ = tokio::time::timeout(Duration::from_millis(300), end).await;
                    if body.is_some() {
                        // The adapter still holds the body; a stalled one it drops.
                        let stalled = tokio::time::timeout(Duration::from_millis(300), g.idle())
                            .await
                            .is_err();
                        if stalled {
                            body = None;
                        }
                    }
                    idle(&g).await;
                    let n = Arc::strong_count(&mark) - 2; // the gateway's plugin holds one
                    if n != 0 {
                        kept.push(format!(
                            "how {how} quirk {quirk} streams {streams} read {read_to}: {n} kept"
                        ));
                    }
                    drop(body);
                }
            }
        }
    }
    assert!(kept.is_empty(), "{}", kept.join("\n"));
}

/// `handle`'s future dropped at every point: before it is polled, and after
/// each poll. `idle` always resolves and no value is kept.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn handle_dropped_at_every_poll_leaks_nothing() {
    for polls in 0..12usize {
        for streams in [false, true] {
            let mark = Arc::new(());
            let g = gw(&mark, 0, || slow(vec!["a", "DONE"]));
            let (tx, mut r) = upload();
            r.streams = streams;
            send(&tx, "x");
            let (wire, reply) = Io::accept(r);
            let mut handled = Box::pin(g.handle(wire, Extensions::new()));
            let waker = respan_gateway_core::futures::task::noop_waker();
            let mut cx = std::task::Context::from_waker(&waker);
            let mut done = false;
            for _ in 0..polls {
                if handled.as_mut().poll(&mut cx).is_ready() {
                    done = true;
                    break;
                }
                tokio::task::yield_now().await;
            }
            drop(handled);
            drop(tx);
            if done {
                // The edge has its reply: the adapter drops it unread.
                drop(reply);
            } else {
                let _ = tokio::time::timeout(Duration::from_millis(200), reply).await;
            }
            idle(&g).await;
            assert_eq!(
                Arc::strong_count(&mark),
                2,
                "polls {polls} streams {streams}"
            ); // the gateway's plugin holds one
        }
    }
}
