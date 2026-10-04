#![cfg(feature = "ws")]
//! `until` against a client that does not read.
#![allow(clippy::unwrap_used)]

mod support;

use std::sync::atomic::Ordering;
use std::time::Duration;

use axum::extract::ws::WebSocketUpgrade;
use axum::extract::{Request, State};
use respan_gateway_sdk::Gateway;
use respan_gateway_ws::Limits;
use support::*;

const PATH: &str = "/v1/realtime";

async fn served_until(
    probe: &Probe,
    after: Duration,
) -> (Outcomes, Ws, tokio::task::JoinHandle<()>, Gateway) {
    let (gateway, ends, _calls) = relay(&probe.base());
    let gateway = gateway.plugin(Relay::RAW);
    let app = axum::Router::new()
        .fallback(
            move |State(gw): State<Gateway>, ws: WebSocketUpgrade, req: Request| async move {
                respan_gateway_axum::ws::serve_ws_until(
                    &gw,
                    ws,
                    req,
                    Limits::default(),
                    async move {
                        tokio::time::sleep(after).await;
                        "session_too_long"
                    },
                )
                .await
            },
        )
        .with_state(gateway.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let ws = connect(addr, PATH, &[]).await.unwrap();
    probe.connected(0).await;
    (ends, ws, task, gateway)
}

// The deadline must end a session whose client stopped reading: that is
// what bounds a session's length and drains it at shutdown.
#[tokio::test(flavor = "multi_thread")]
async fn until_ends_a_session_whose_client_does_not_read() {
    let probe = Probe::with(Behaviour {
        commands: true,
        ..Behaviour::default()
    })
    .await;
    let (ends, mut ws, task, gateway) = served_until(&probe, Duration::from_millis(300)).await;
    say(&mut ws, "!flood:20000:4096").await;
    // The client never reads again, but keeps its connection.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let sent = probe.sent.load(Ordering::SeqCst);
    let idle = tokio::time::timeout(Duration::from_secs(5), gateway.idle()).await;
    eprintln!("provider sent {sent} frames; ends = {:?}", ends.all());
    assert!(
        idle.is_ok(),
        "the session outlived its `until` by 5 s: the gateway never went idle"
    );
    drop(ws);
    task.abort();
}
