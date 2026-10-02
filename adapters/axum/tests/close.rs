#![cfg(feature = "ws")]
//! How a session ends, from either side, and what each hook is told.
#![allow(clippy::unwrap_used)]

mod support;

use std::sync::atomic::Ordering;

use respan_gateway_sdk::Gateway;
use support::*;

const PATH: &str = "/v1/realtime";

/// A session through a probe provider, with its recorders.
async fn session() -> (Probe, Server, Outcomes, Calls, Ws) {
    let probe = Probe::start().await;
    let (gateway, ends, calls) = relay(&probe.base());
    let server = serve(gateway).await;
    let ws = connect(server.addr, PATH, &[]).await.unwrap();
    probe.connected(0).await;
    (probe, server, ends, calls, ws)
}

// The client's Close reaches the provider (with its code), ends the input
// clean, and the session ends `Ok` on both edges, once each.
#[tokio::test(flavor = "multi_thread")]
async fn a_graceful_client_close_reaches_the_provider() {
    let (probe, server, ends, calls, mut ws) = session().await;
    say(&mut ws, "one").await;
    say(&mut ws, "two").await;
    close(&mut ws, 1000, "bye").await;
    // The client's own close handshake completes.
    assert!(matches!(
        hear(&mut ws).await,
        Heard::Closed(_) | Heard::Broken
    ));
    idle(&server.gateway).await;
    assert_eq!(
        probe.seen(),
        [
            Seen::Text("one".into()),
            Seen::Text("two".into()),
            Seen::Close(Some(1000))
        ]
    );
    assert_eq!(code(&ends.only()), "ok");
    assert_eq!(calls.all().iter().map(code).collect::<Vec<_>>(), ["ok"]);
    eventually("the provider's connection closed", || {
        probe.live.load(Ordering::SeqCst) == 0
    })
    .await;
}

// A provider's Close reaches the client with its code, where a Close may
// carry it: 1000 is a clean end, other codes a failure the client sees as
// that code.
#[tokio::test(flavor = "multi_thread")]
async fn a_provider_close_reaches_the_client_with_its_code() {
    for (sent, want, end) in [
        (1000, 1000, "ok"),
        (1001, 1001, "upstream"),
        (1008, 1008, "upstream"),
        (1011, 1011, "upstream"),
        (4001, 4001, "upstream"),
    ] {
        let (probe, server, ends, calls, mut ws) = session().await;
        say(&mut ws, "hello").await;
        probe.tell(0, Cmd::Close(sent));
        match hear(&mut ws).await {
            Heard::Closed(Some((got, reason))) => {
                assert_eq!(got, want, "{sent}");
                // Only a fault's code is said, never a provider's words.
                assert!(reason == "upstream" || reason.is_empty(), "{reason}");
                assert_eq!(reason.is_empty(), end == "ok", "{sent}");
            }
            other => panic!("{sent}: {other:?}"),
        }
        idle(&server.gateway).await;
        assert_eq!(code(&ends.only()), end, "{sent}");
        assert_eq!(
            calls.all().iter().map(code).collect::<Vec<_>>(),
            [end],
            "{sent}"
        );
    }
}

// A provider that drops without a Close is a truncated stream, and the
// client is told 1011 with `truncated`, not `stop` or a clean close.
#[tokio::test(flavor = "multi_thread")]
async fn a_provider_that_drops_is_truncated() {
    let (probe, server, ends, calls, mut ws) = session().await;
    probe.tell(0, Cmd::Drop);
    assert_eq!(
        hear(&mut ws).await,
        Heard::Closed(Some((1011, "truncated".into())))
    );
    idle(&server.gateway).await;
    assert_eq!(code(&ends.only()), "truncated");
    assert_eq!(
        calls.all().iter().map(code).collect::<Vec<_>>(),
        ["truncated"]
    );
}

// A provider that accepts the upgrade and drops at once: the client has its
// 101 already (a streaming request's head goes at once), then a Close 1011.
#[tokio::test(flavor = "multi_thread")]
async fn a_provider_that_drops_at_once_is_a_101_then_a_close() {
    let probe = Probe::with(Behaviour {
        drop_at_once: true,
        ..Behaviour::default()
    })
    .await;
    let (gateway, ends, _calls) = relay(&probe.base());
    let server = serve(gateway).await;
    let mut ws = connect(server.addr, PATH, &[]).await.unwrap();
    assert_eq!(
        hear(&mut ws).await,
        Heard::Closed(Some((1011, "truncated".into())))
    );
    idle(&server.gateway).await;
    assert_eq!(code(&ends.only()), "truncated");
}

// An abrupt client drop tears the provider's connection down without a
// Close; each hook runs once, told `abandoned`, and the gateway goes idle.
#[tokio::test(flavor = "multi_thread")]
async fn an_abrupt_client_drop_tears_down_the_provider() {
    let (probe, server, ends, calls, mut ws) = session().await;
    say(&mut ws, "one").await;
    eventually("the provider read it", || probe.texts() == ["one"]).await;
    drop(ws);
    idle(&server.gateway).await;
    eventually("the provider's connection closed", || {
        probe.live.load(Ordering::SeqCst) == 0
    })
    .await;
    // The provider saw a connection that ended, never a Close.
    assert_eq!(probe.seen(), [Seen::Text("one".into()), Seen::Ended]);
    assert_eq!(
        ends.all().iter().map(code).collect::<Vec<_>>(),
        ["abandoned"]
    );
    assert_eq!(
        calls.all().iter().map(code).collect::<Vec<_>>(),
        ["abandoned"]
    );
}

// The same, before the client said anything.
#[tokio::test(flavor = "multi_thread")]
async fn an_abrupt_drop_before_any_frame_ends_the_same() {
    let (probe, server, ends, calls, ws) = session().await;
    drop(ws);
    idle(&server.gateway).await;
    eventually("the provider's connection closed", || {
        probe.live.load(Ordering::SeqCst) == 0
    })
    .await;
    assert_eq!(
        ends.all().iter().map(code).collect::<Vec<_>>(),
        ["abandoned"]
    );
    assert_eq!(
        calls.all().iter().map(code).collect::<Vec<_>>(),
        ["abandoned"]
    );
}

// A client's binary frame is a fault: the client is closed with 1003, the
// provider's connection is torn down, and no hook runs twice.
#[tokio::test(flavor = "multi_thread")]
async fn a_binary_frame_from_the_client_closes_it_with_1003() {
    use futures::SinkExt;
    let (probe, server, ends, calls, mut ws) = session().await;
    ws.send(tokio_tungstenite::tungstenite::Message::Binary(
        vec![1, 2, 3].into(),
    ))
    .await
    .unwrap();
    assert_eq!(
        hear(&mut ws).await,
        Heard::Closed(Some((1003, "binary".into())))
    );
    idle(&server.gateway).await;
    eventually("the provider's connection closed", || {
        probe.live.load(Ordering::SeqCst) == 0
    })
    .await;
    // The request ends `abandoned`, as it does for any client that leaves.
    assert_eq!(
        ends.all().iter().map(code).collect::<Vec<_>>(),
        ["abandoned"]
    );
    assert_eq!(
        calls.all().iter().map(code).collect::<Vec<_>>(),
        ["abandoned"]
    );
    assert!(
        probe.seen().iter().all(|s| !matches!(s, Seen::Binary(_))),
        "never relayed"
    );
}

// A provider's binary frame is a fault too: the client is told 1011.
#[tokio::test(flavor = "multi_thread")]
async fn a_binary_frame_from_the_provider_is_a_fault() {
    let (probe, server, ends, _calls, mut ws) = session().await;
    probe.tell(0, Cmd::SendBinary(vec![9]));
    assert_eq!(
        hear(&mut ws).await,
        Heard::Closed(Some((1011, "upstream".into())))
    );
    idle(&server.gateway).await;
    assert_eq!(code(&ends.only()), "upstream");
}

// What the client sees of a provider that is not there: an ordinary HTTP
// answer to the handshake, before any upgrade (502 with text/plain and no
// address in it), because the call is sent before the head is.
#[tokio::test(flavor = "multi_thread")]
async fn an_unreachable_provider_is_a_502_to_the_handshake() {
    let gone = nowhere().await;
    let (gateway, ends, calls) = relay(&format!("ws://{gone}"));
    let server = serve(gateway).await;
    let refused = connect(server.addr, PATH, &[]).await;
    let Err(tokio_tungstenite::tungstenite::Error::Http(resp)) = refused else {
        panic!("expected an HTTP answer to the handshake")
    };
    assert_eq!(resp.status(), 502);
    assert_eq!(resp.headers()["content-type"], "text/plain; charset=utf-8");
    let body = String::from_utf8(resp.body().clone().unwrap()).unwrap();
    assert_eq!(body, "the provider could not be reached");
    assert!(!body.contains(&gone.to_string()), "no address: {body}");
    idle(&server.gateway).await;
    assert_eq!(code(&ends.only()), "upstream");
    assert_eq!(
        calls.all().iter().map(code).collect::<Vec<_>>(),
        ["upstream"]
    );
}

// A request no served format accepts is refused before the upgrade too.
#[tokio::test(flavor = "multi_thread")]
async fn a_request_no_format_accepts_is_a_404_to_the_handshake() {
    let probe = Probe::start().await;
    let (gateway, ..) = relay(&probe.base());
    // A gateway that serves nothing.
    let _ = gateway;
    let bare = respan_gateway_sdk::Gateway::new();
    let server = serve(bare).await;
    let refused = connect(server.addr, PATH, &[]).await;
    let Err(tokio_tungstenite::tungstenite::Error::Http(resp)) = refused else {
        panic!("expected an HTTP answer to the handshake")
    };
    assert_eq!(resp.status(), 404);
    assert_eq!(probe.total.load(Ordering::SeqCst), 0);
}

// Pings are each hop's own: a client's is answered by the gateway and a
// provider's by the transport, and neither is carried to the other side.
#[tokio::test(flavor = "multi_thread")]
async fn pings_are_answered_by_each_hop_and_carried_by_none() {
    use futures::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::Message;
    let (probe, server, ends, _calls, mut ws) = session().await;
    ws.send(Message::Ping(vec![1, 2].into())).await.unwrap();
    let pong = tokio::time::timeout(PATIENCE, ws.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(pong, Message::Pong(vec![1, 2].into()));
    // The provider pings; the session goes on, and the client hears no text.
    probe.tell(0, Cmd::Ping);
    probe.tell(0, Cmd::Send("after".into()));
    assert_eq!(heard_text(&mut ws).await, "after");
    say(&mut ws, "still here").await;
    eventually("the provider read it", || probe.texts() == ["still here"]).await;
    close(&mut ws, 1000, "").await;
    idle(&server.gateway).await;
    assert_eq!(code(&ends.only()), "ok");
}

/// A session served with `until` made per session, through a probe provider.
async fn ending(
    until: impl Fn() -> std::pin::Pin<Box<dyn std::future::Future<Output = &'static str> + Send>>
    + Clone
    + Send
    + Sync
    + 'static,
) -> (
    Probe,
    Outcomes,
    Calls,
    Ws,
    tokio::task::JoinHandle<()>,
    Gateway,
) {
    use axum::extract::ws::WebSocketUpgrade;
    use axum::extract::{Request, State};
    let probe = Probe::start().await;
    let (gateway, ends, calls) = relay(&probe.base());
    let gateway = gateway.plugin(Relay::RAW);
    let app = axum::Router::new()
        .fallback(
            move |State(gw): State<Gateway>, ws: WebSocketUpgrade, req: Request| {
                let until = until.clone();
                async move { respan_gateway_axum::ws::serve_ws_until(&gw, ws, req, until()).await }
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
    (probe, ends, calls, ws, task, gateway)
}

// A session over its maximum length is closed 1001 with the reason, even
// idle, and the provider's connection goes with it.
#[tokio::test(flavor = "multi_thread")]
async fn a_session_over_its_maximum_length_is_closed_going_away() {
    let (probe, _ends, _calls, mut ws, task, gateway) = ending(|| {
        Box::pin(async {
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            "session_too_long"
        })
    })
    .await;
    say(&mut ws, "hello").await;
    assert_eq!(
        hear(&mut ws).await,
        Heard::Closed(Some((1001, "session_too_long".into())))
    );
    idle(&gateway).await;
    assert_eq!(probe.texts(), ["hello"]);
    eventually("the provider's connection closed", || {
        probe.live.load(Ordering::SeqCst) == 0
    })
    .await;
    task.abort();
}

// At shutdown every open session is closed 1001 "draining"; one that has
// already ended is not affected.
#[tokio::test(flavor = "multi_thread")]
async fn a_shutdown_drains_every_open_session() {
    let (stop, stopped) = tokio::sync::watch::channel(false);
    let (probe, ends, _calls, mut ws, task, gateway) = ending(move || {
        let mut stopped = stopped.clone();
        Box::pin(async move {
            let _ = stopped.wait_for(|stop| *stop).await;
            "draining"
        })
    })
    .await;
    say(&mut ws, "one").await;
    eventually("the provider read it", || probe.texts().len() == 1).await;
    stop.send(true).unwrap();
    assert_eq!(
        hear(&mut ws).await,
        Heard::Closed(Some((1001, "draining".into())))
    );
    idle(&gateway).await;
    assert_eq!(ends.all().len(), 1, "the request ended once");
    task.abort();
}

// A session that ends first is not touched by an `until` that never comes.
#[tokio::test(flavor = "multi_thread")]
async fn a_session_that_ends_first_closes_as_it_would() {
    let (_probe, ends, _calls, mut ws, task, gateway) =
        ending(|| Box::pin(std::future::pending())).await;
    say(&mut ws, "one").await;
    close(&mut ws, 1000, "bye").await;
    assert!(matches!(
        hear(&mut ws).await,
        Heard::Closed(_) | Heard::Broken
    ));
    idle(&gateway).await;
    assert_eq!(code(&ends.only()), "ok");
    task.abort();
}
