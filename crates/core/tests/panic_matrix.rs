//! A panic injected at every point where user code runs in the kernel's
//! reach: a plugin, a filter, a `Format` method and an edge's stream, on
//! one-shot and streaming requests: the request finishes, every end hook
//! runs once and `idle` resolves. The codec's points are HTTP's driver's
//! (`crates/http/tests/driver_toy.rs`).
#![allow(clippy::unwrap_used)]

mod edge;

use std::sync::atomic::Ordering;
use std::time::Duration;

use edge::*;
use respan_gateway_core::futures::{FutureExt, StreamExt, stream};
use respan_gateway_core::*;

/// Points: 1 the client edge's input stream; 5 the provider edge's event
/// stream; 12 `dial`; 13 `is_content`; 14 `opening`; 17 `accepts`; 18
/// `accept`, 19 its future; 20, 21 plugin hooks; 22, 23 end hooks; 24-27, 32 filters; 28 a
/// dial future; 29 a provider stream's first poll.
const POINTS: [usize; 20] = [
    1, 5, 12, 13, 14, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 32, 0,
];

#[tokio::test(flavor = "multi_thread")]
async fn a_panic_anywhere_still_ends_everything_once() {
    std::panic::set_hook(Box::new(|_| {}));
    let mut bad = vec![];
    for n in POINTS {
        for streams in [false, true] {
            for transport_mode in 0..3 {
                for script in 0..3 {
                    PANIC.store(n, Ordering::SeqCst);
                    let rec = Rec::default();
                    let transport = fnt(move |c| {
                        async move {
                            if n == 28 {
                                boom(28);
                            }
                            let mut body = c.body;
                            if transport_mode == 1 {
                                while body.next().await.is_some() {}
                            }
                            if transport_mode == 2 {
                                tokio::spawn(async move { while body.next().await.is_some() {} });
                            }
                            let chunks = match script {
                                0 => vec!["a", "b", "DONE"],
                                1 => vec!["a", "ERR"],
                                _ => vec!["a", "b"],
                            };
                            let body = match n == 29 {
                                true => stream::once(async {
                                    lock(&FIRED).push(29);
                                    panic!("p29");
                                    #[allow(unreachable_code)]
                                    Ok::<String, Fault>(String::new())
                                })
                                .boxed(),
                                false => slow(chunks),
                            };
                            Ok(answer(200, body))
                        }
                        .boxed()
                    });
                    let gw = Gateway::new()
                        .serve(Toy)
                        .unwrap()
                        .provider("a", Toy, transport)
                        .unwrap()
                        .target("a")
                        .unwrap()
                        .plugin("rec", rec.clone())
                        .unwrap();
                    let (tx, mut req) = upload();
                    req.streams = streams;
                    send(&tx, "one");
                    send(&tx, "two");
                    drop(tx);
                    let gw2 = gw.clone();
                    let driver = tokio::spawn(async move {
                        let (res, end) = handle(&gw2, req).await;
                        let _ = read(res).await;
                        tokio::time::timeout(Duration::from_secs(2), end).await.ok()
                    });
                    let label = format!(
                        "panic {n} streams {streams} transport {transport_mode} script {script}"
                    );
                    match tokio::time::timeout(Duration::from_secs(3), driver).await {
                        Err(_) => {
                            bad.push(format!("{label}: hung"));
                            continue;
                        }
                        Ok(Err(e)) => {
                            bad.push(format!("{label}: a panic escaped: {}", e.is_panic()))
                        }
                        Ok(Ok(None)) => bad.push(format!("{label}: the end future never resolved")),
                        // A panic in `accept`'s future is `internal`, not
                        // `abandoned`, as `Gateway::handle` says.
                        Ok(Ok(Some(end))) if n == 19 => {
                            let code = end.as_ref().err().map(Fault::code);
                            if code != Some(codes::INTERNAL) {
                                bad.push(format!("{label}: ended {code:?}, not internal"));
                            }
                        }
                        Ok(Ok(Some(_))) => {}
                    }
                    if tokio::time::timeout(Duration::from_secs(2), gw.idle())
                        .await
                        .is_err()
                    {
                        bad.push(format!("{label}: idle hung"));
                    }
                    let l = rec.log();
                    let counted =
                        l.client_start == l.client_end.len() && l.prov_start == l.prov_end.len();
                    if n != 22 && n != 23 && !counted {
                        bad.push(format!(
                            "{label}: hooks {}/{} {}/{}",
                            l.client_start,
                            l.client_end.len(),
                            l.prov_start,
                            l.prov_end.len()
                        ));
                    }
                }
            }
        }
    }
    PANIC.store(0, Ordering::SeqCst);
    let _ = std::panic::take_hook();
    let fired = lock(&FIRED).clone();
    let idle: Vec<_> = POINTS
        .iter()
        .filter(|p| **p != 0 && !fired.contains(p))
        .collect();
    assert!(idle.is_empty(), "points that never fired: {idle:?}");
    assert!(
        bad.is_empty(),
        "{} problems:\n{}",
        bad.len(),
        bad.iter().take(40).cloned().collect::<Vec<_>>().join("\n")
    );
}
