//! A panic injected at every point where user code runs, in a plugin, a
//! filter, a format or a transport, on whole and streaming requests: the
//! request finishes, every end hook runs once and `idle` resolves. It owns
//! its process's panic switch, so it is alone in this file.
#![allow(clippy::unwrap_used)]

mod toy;

use std::sync::atomic::Ordering;
use std::time::Duration;

use respan_gateway_core::bytes::Bytes;
use respan_gateway_core::futures::{FutureExt, StreamExt, stream};
use respan_gateway_core::*;
use toy::*;

#[tokio::test(flavor = "multi_thread")]
async fn a_panic_anywhere_still_ends_everything_once() {
    std::panic::set_hook(Box::new(|_| {}));
    let mut bad = vec![];
    // 16, 18 and 19 are unused; 22 and 23 panic in the recorder's own end
    // hooks; 30 is a client output filter's end, 31 the request encoder's
    // `open`, where a call's head is written.
    for n in (1..=17usize).chain(20..=32) {
        for streams in [false, true] {
            for transport_mode in 0..3 {
                for script in 0..3 {
                    PANIC.store(n, Ordering::SeqCst);
                    let rec = Rec::default();
                    let transport = fnt(move |c| {
                        async move {
                            if n == 28 {
                                panic!("p28");
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
                                    panic!("p29");
                                    #[allow(unreachable_code)]
                                    Ok::<Bytes, Fault>(Bytes::new())
                                })
                                .boxed(),
                                false => slow(chunks),
                            };
                            Ok(Exchange::new(Ans(200), body))
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
                        tokio::time::timeout(Duration::from_secs(2), end)
                            .await
                            .is_ok()
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
                        Ok(Ok(false)) => {
                            bad.push(format!("{label}: the end future never resolved"))
                        }
                        Ok(Ok(true)) => {}
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
    assert!(
        bad.is_empty(),
        "{} problems:\n{}",
        bad.len(),
        bad.iter().take(40).cloned().collect::<Vec<_>>().join("\n")
    );
}
