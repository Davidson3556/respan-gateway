//! Dropping a request or its reply at every await point: every hook and
//! filter still ends once, `idle` resolves and the end future resolves.
//! On the toy edge, the sweeps add the client edge's quirks (see `edge::Quirk`), as the edge
//! owns its delivery completion channel.
#![allow(clippy::unwrap_used)]

mod edge;
mod faults;

use std::time::Duration;

use edge::*;
use respan_gateway_core::futures::{FutureExt, StreamExt};
use respan_gateway_core::*;

/// Drive `handle` and the reply for `h` polls, then its body for `b` polls
/// per frame, and drop whatever is left.
async fn drive(gw: &Gateway, streams: bool, quirk: Quirk, h: usize, b: usize) -> Option<End> {
    let mut req = req_of(vec!["one", "two"], streams);
    req.quirk = quirk;
    let (res, end) = limit(handle(gw, req), h).await?;
    if let Ok(reply) = res {
        let mut body = reply.body;
        let mut n = 0;
        while let Some(Some(_)) = limit(body.next(), b).await {
            n += 1;
            if n > 50 {
                break;
            }
        }
    }
    Some(end)
}

const SCENARIOS: std::ops::Range<usize> = 0..4;
const POINTS: std::ops::Range<usize> = 0..24;
const QUIRKS: std::ops::Range<u8> = 0..3;

#[tokio::test(flavor = "current_thread")]
async fn cancellation_finishes_hooks_filters_and_delivery_once() {
    let mut failures = vec![];
    for quirk in QUIRKS {
        for scenario in SCENARIOS {
            for streams in [false, true] {
                for h in POINTS {
                    for b in POINTS {
                        let rec = Rec::default();
                        let ends = Ends::default();
                        let gw = gateway(scenario, &rec).plugin(ends.clone());
                        let end = drive(&gw, streams, quirk, h, b).await;
                        let idle = tokio::time::timeout(Duration::from_secs(2), gw.idle()).await;
                        let (cs, ce, ps, pe) = {
                            let l = rec.log();
                            (
                                l.client_start,
                                l.client_end.len(),
                                l.prov_start,
                                l.prov_end.len(),
                            )
                        };
                        let at = format!(
                            "quirk {quirk} scenario {scenario} streams {streams} h {h} b {b}"
                        );
                        if idle.is_err() || cs != ce || ps != pe {
                            failures.push(format!(
                                "{at}: idle {} client {cs}/{ce} provider {ps}/{pe}",
                                idle.is_ok()
                            ));
                        }
                        let counts = ends.counts();
                        if counts.iter().any(|count| *count != 1) {
                            failures.push(format!("{at}: filter completions {counts:?}"));
                        }
                        if let Some(end) = end
                            && tokio::time::timeout(Duration::from_secs(1), end)
                                .await
                                .is_err()
                        {
                            failures.push(format!("{at}: the end future never resolved"));
                        }
                    }
                }
            }
        }
    }
    assert!(
        failures.is_empty(),
        "{} failures: {:#?}",
        failures.len(),
        &failures[..failures.len().min(10)]
    );
}

/// A call kept by a spawned task ends with its request, however far it got.
type Tasks = std::sync::Arc<parking_lot::Mutex<Vec<tokio::task::JoinHandle<Result<()>>>>>;
struct Spawner(Tasks);
#[async_trait]
impl Plugin for Spawner {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>> {
        let call = cx.call("a");
        let task = tokio::spawn(async move {
            let mut outputs = call.await?.outputs::<Out>();
            while let Some(x) = outputs.next().await {
                x?;
            }
            Ok(())
        });
        self.0.lock().push(task);
        tokio::task::yield_now().await;
        let z = futures::stream::iter([Ok::<_, Fault>(Out("z".into()))]);
        Ok(Some(Response::stream(z)))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn spawned_calls_end_with_their_request() {
    for i in 0..200 {
        let rec = Rec::default();
        let tasks = std::sync::Arc::default();
        let delay = i % 7;
        let t = fnt(move |_| {
            async move {
                for _ in 0..delay {
                    tokio::task::yield_now().await;
                }
                Ok(answer(200, futures::stream::pending().boxed()))
            }
            .boxed()
        });
        let gw = Gateway::new()
            .client::<In, _, _>(Toy)
            .unwrap()
            .provider(Provider::new::<In, _, _>("a", Toy, connect(t)))
            .unwrap()
            .plugin(rec.clone())
            .plugin(Spawner(std::sync::Arc::clone(&tasks)));
        let (res, _) = handle(&gw, req(vec!["q"])).await;
        let _ = read(res).await;
        tokio::time::timeout(Duration::from_secs(2), gw.idle())
            .await
            .unwrap_or_else(|_| panic!("idle hung at {i}"));
        let tasks: Vec<_> = tasks.lock().drain(..).collect();
        for task in tasks {
            let ended = tokio::time::timeout(Duration::from_secs(2), task).await;
            let ended = ended
                .unwrap_or_else(|_| panic!("a spawned call hung at {i}"))
                .unwrap();
            let code = ended.as_ref().err().map(|f| faults::code(f).to_string());
            assert!(
                matches!(code.as_deref(), Some("abandoned" | "internal")),
                "at {i}: {ended:?}"
            );
        }
        let l = rec.log();
        assert_eq!(
            (l.prov_start, l.client_end.len()),
            (l.prov_end.len(), 1),
            "at {i}"
        );
    }
}

/// Provider end hooks, even slow ones, finish before the client's.
struct Ordered(std::sync::Arc<parking_lot::Mutex<Vec<&'static str>>>);
#[async_trait]
impl Plugin for Ordered {
    async fn on_client_end(&self, _: &ClientContext, _: &Result<()>) {
        self.0.lock().push("client_end");
    }
    async fn on_provider_end(&self, _: &ProviderContext, _: &Result<()>) {
        tokio::time::sleep(Duration::from_millis(20)).await;
        self.0.lock().push("provider_end");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn provider_end_hooks_precede_the_clients() {
    // (scenario, frames read before the client leaves)
    for (scenario, leave_after) in [
        (0, usize::MAX),
        (0, 0),
        (0, 1),
        (1, usize::MAX),
        (2, usize::MAX),
        (3, usize::MAX),
    ] {
        let log = std::sync::Arc::default();
        let gw = gateway(scenario, &Rec::default()).plugin(Ordered(std::sync::Arc::clone(&log)));
        let (res, _) = handle(&gw, req(vec!["q"])).await;
        let mut body = res.unwrap().body;
        let mut n = 0;
        while body.next().await.is_some() {
            n += 1;
            if n > leave_after {
                break;
            }
        }
        drop(body);
        gw.idle().await;
        let log = log.lock().clone();
        // a plugin that answers first leaves this one's client hook unrun
        let first_client = log
            .iter()
            .position(|x| *x == "client_end")
            .unwrap_or(log.len());
        let before = log[..first_client]
            .iter()
            .filter(|x| **x == "provider_end")
            .count();
        let all = log.iter().filter(|x| **x == "provider_end").count();
        assert_eq!(
            before, all,
            "scenario {scenario} leave {leave_after}: {log:?}"
        );
    }
}
