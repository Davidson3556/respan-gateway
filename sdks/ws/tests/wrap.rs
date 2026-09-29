//! A session and the input wraps of plugins.
#![allow(clippy::unwrap_used)]

mod support;

use std::time::{Duration, Instant};

use futures::StreamExt;
use respan_gateway_core::{ClientContext, Events, Fault, Plugin, Response, async_trait};
use respan_gateway_formats::opaque::Sent;
use support::*;

const PATH: &str = "/v1/realtime";
const WAIT: Duration = Duration::from_millis(1500);

/// A plugin whose wrap holds each input for `WAIT`, as a remote check does.
struct Slow;

#[async_trait]
impl Plugin for Slow {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        cx.wrap(|inputs: Events<Sent<Raw>>| {
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
    let gateway = gateway.plugin("slow", Slow).unwrap();
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
