#![cfg(feature = "ws")]
//! A session and the input wraps of plugins.
#![allow(clippy::unwrap_used)]

mod support;

use std::time::{Duration, Instant};

use futures::StreamExt;
use respan_gateway_http::formats::opaque::Sent;
use respan_gateway_sdk::{ClientContext, EventStream, Fault, Plugin, Response, async_trait};
use support::*;

const PATH: &str = "/v1/realtime";
const WAIT: Duration = Duration::from_millis(1500);

/// A plugin whose wrap holds each input for `WAIT`, as a remote check does.
struct Slow;

#[async_trait]
impl Plugin for Slow {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        cx.wrap(|inputs: EventStream<Sent<Raw>>| {
            inputs.then(|input| async move {
                tokio::time::sleep(WAIT).await;
                input
            })
        })?;
        Ok(None)
    }
}

// A client that leaves while an input wrap is still waiting ends its
// session at once, not when the wait is over, and the provider never gets
// the input that was waiting.
#[tokio::test(flavor = "multi_thread")]
async fn a_client_that_leaves_during_a_wrap_ends_its_session_then() {
    let probe = Probe::start().await;
    let (gateway, ends, calls) = relay(&probe.base());
    let gateway = gateway.plugin(Slow);
    let server = serve(gateway).await;
    let mut ws = connect(server.addr, PATH, &[]).await.unwrap();
    probe.connected(0).await;
    say(&mut ws, "waiting").await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    let left = Instant::now();
    drop(ws);
    idle(&server.gateway).await;
    assert!(
        left.elapsed() < WAIT / 2,
        "the session outlived its client by {:?}",
        left.elapsed()
    );
    tokio::time::sleep(WAIT).await;
    assert!(
        probe.texts().is_empty(),
        "the provider got {:?}",
        probe.texts()
    );
    assert_eq!(ends.all().len(), 1);
    assert_eq!(calls.all().len(), 1);
}

// ===== loop 7 audit: WebSocket ingress wrapping =====

/// A wrap that holds each input a little, per a seed.
struct Jitter(u64);

#[async_trait]
impl Plugin for Jitter {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let seed = self.0;
        cx.wrap(move |inputs: EventStream<Sent<Raw>>| {
            let mut s = seed;
            inputs.then(move |input| {
                s = s
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                let ms = (s >> 33) % 30;
                async move {
                    tokio::time::sleep(Duration::from_millis(ms)).await;
                    input
                }
            })
        })?;
        Ok(None)
    }
}

/// Random sessions under a slow wrap: frames, then a Close, a drop or a
/// binary frame, each at a random moment. The provider gets the frames in
/// order with no gap (all of them before a Close), a drop never ends
/// clean, a Close always does, and every hook runs once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn l7_look_ahead_keeps_order_and_ends_right_under_random_sessions() {
    use futures::SinkExt;
    use tokio_tungstenite::tungstenite::Message;
    let mut bad = Vec::new();
    for seed in 0..120u64 {
        let probe = Probe::start().await;
        let (gateway, ends, calls) = relay(&probe.base());
        let gateway = gateway.plugin(Jitter(seed));
        let server = serve(gateway).await;
        let mut ws = connect(server.addr, PATH, &[]).await.unwrap();
        probe.connected(0).await;
        let mut s = seed
            .wrapping_mul(2862933555777941757)
            .wrapping_add(3037000493);
        let mut rnd = move |n: u64| {
            s = s
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            (s >> 33) % n
        };
        let n = rnd(5) as usize;
        let sent: Vec<String> = (0..n).map(|i| format!("m{i}")).collect();
        for t in &sent {
            say(&mut ws, t).await;
            tokio::time::sleep(Duration::from_millis(rnd(15))).await;
        }
        let how = rnd(3);
        match how {
            0 => close(&mut ws, 1000, "bye").await,
            1 => drop(ws),
            _ => ws.send(Message::Binary(vec![1u8].into())).await.unwrap(),
        }
        idle(&server.gateway).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        let texts = probe.texts();
        let end: Vec<String> = ends.all().iter().map(|e| code(e).to_owned()).collect();
        let call: Vec<String> = calls.all().iter().map(|e| code(e).to_owned()).collect();
        let prefix = texts.len() <= sent.len() && texts[..] == sent[..texts.len()];
        let ok = prefix
            && end.len() == 1
            && call.len() == 1
            && match how {
                0 => texts == sent && end[0] == "ok",
                1 => end[0] != "ok" && call[0] != "ok",
                _ => end[0] != "ok",
            };
        if !ok {
            bad.push(format!("seed {seed} how {how} sent {sent:?} got {texts:?} ends {end:?} calls {call:?} seen {:?}", probe.seen()));
        }
    }
    assert!(bad.is_empty(), "{}", bad.join("\n"));
}
