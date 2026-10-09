#![cfg(feature = "ws")]
//! `stop` against a client that does not read.
#![allow(clippy::unwrap_used)]

mod support;

use std::sync::atomic::Ordering;
use std::time::Duration;

use respan_gateway_ws::Limits;
use support::*;

const PATH: &str = "/v1/realtime";

// `stop` must end a session whose client stopped reading: that is what
// drains it at shutdown.
#[tokio::test(flavor = "multi_thread")]
async fn stop_ends_a_session_whose_client_does_not_read() {
    let probe = Probe::with(Behaviour {
        commands: true,
        ..Behaviour::default()
    })
    .await;
    let (gateway, ends, _calls) = relay(&probe.base());
    let stop = tokio::time::sleep(Duration::from_millis(300));
    let server = serve_with(gateway.plugin(Relay::RAW), Limits::default(), stop).await;
    let mut ws = connect(server.addr, PATH, &[]).await.unwrap();
    probe.connected(0).await;
    say(&mut ws, "!flood:20000:4096").await;
    // The client never reads again, but keeps its connection.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let sent = probe.sent.load(Ordering::SeqCst);
    let idle = tokio::time::timeout(Duration::from_secs(5), server.gateway.idle()).await;
    eprintln!("provider sent {sent} frames; ends = {:?}", ends.all());
    assert!(
        idle.is_ok(),
        "the session outlived `stop` by 5 s: the gateway never went idle"
    );
    drop(ws);
}
