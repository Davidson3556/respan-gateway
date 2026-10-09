#![cfg(feature = "ws")]
//! How a session ends, from either side, and what each hook is told: each
//! relayed unread with `Relay::RAW` and typed as JSON events with no relay
//! ([`Via`]). Texts are JSON, which both ways carry as they are.
#![allow(clippy::unwrap_used)]

mod support;

use std::sync::atomic::Ordering;

use respan_gateway_realtime::Input;
use respan_gateway_sdk::{ClientContext, ErrorCode, Fault, Gateway, Plugin, Response, async_trait};
use respan_gateway_ws::{CloseCode, Limits, ProviderClose};
use support::*;

const PATH: &str = "/v1/realtime";

/// A session through a probe provider, sent `via`, with its recorders.
async fn session(via: Via) -> (Probe, Server, Outcomes, Calls, Ws) {
    let probe = Probe::start().await;
    let (gateway, ends, calls) = via.gateway(&probe.base());
    let server = serve_typed(gateway).await;
    let ws = connect(server.addr, PATH, &[]).await.unwrap();
    probe.connected(0).await;
    (probe, server, ends, calls, ws)
}

// The client's Close reaches the provider (as 1000), ends the input clean,
// and the session ends `Ok` on both edges, once each.
#[tokio::test(flavor = "multi_thread")]
async fn a_graceful_client_close_reaches_the_provider() {
    for via in Via::ALL {
        let (probe, server, ends, calls, mut ws) = session(via).await;
        say(&mut ws, r#""one""#).await;
        say(&mut ws, r#""two""#).await;
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
                Seen::Text(r#""one""#.into()),
                Seen::Text(r#""two""#.into()),
                Seen::Close(Some(1000))
            ],
            "{via:?}"
        );
        assert_eq!(code(&ends.only()), "ok");
        assert_eq!(calls.all().iter().map(code).collect::<Vec<_>>(), ["ok"]);
        eventually("the provider's connection closed", || {
            probe.live.load(Ordering::SeqCst) == 0
        })
        .await;
    }
}

// A provider's Close 1000 is a clean end. Another code is an `upstream`
// fault with that `ProviderClose`; the client is closed with that code only
// on a relay, which passes the provider's errors on, else with 1011.
#[tokio::test(flavor = "multi_thread")]
async fn a_provider_close_reaches_the_client_with_its_code_only_on_a_relay() {
    for via in Via::ALL {
        for sent in [1000, 1001, 1008, 1011, 4001] {
            let (probe, server, ends, calls, mut ws) = session(via).await;
            say(&mut ws, r#""hello""#).await;
            probe.tell(0, Cmd::Close(sent));
            let (want, end) = match (sent, via) {
                (1000, _) => (1000, "ok"),
                (_, Via::Relay) => (sent, "upstream"),
                (_, Via::Typed) => (1011, "upstream"),
            };
            match hear(&mut ws).await {
                Heard::Closed(Some((got, reason))) => {
                    assert_eq!(got, want, "{via:?} {sent}");
                    // Only a fault's code is said, never a provider's words.
                    assert_eq!(reason, if end == "ok" { "" } else { end }, "{sent}");
                }
                other => panic!("{via:?} {sent}: {other:?}"),
            }
            idle(&server.gateway).await;
            assert_eq!(code(&ends.only()), end, "{via:?} {sent}");
            let calls = calls.all();
            assert_eq!(calls.iter().map(code).collect::<Vec<_>>(), [end]);
            if let Err(fault) = &calls[0] {
                assert_eq!(fault.get::<ProviderClose>(), Some(&ProviderClose(sent)));
                let relayed = fault.get::<CloseCode>().is_some();
                assert_eq!(relayed, via == Via::Relay, "{via:?} {sent}");
            }
        }
    }
}

// A provider that drops without a Close is a truncated stream, and the
// client is told 1011 with `truncated`, not `stop` or a clean close.
#[tokio::test(flavor = "multi_thread")]
async fn a_provider_that_drops_is_truncated() {
    for via in Via::ALL {
        let (probe, server, ends, calls, mut ws) = session(via).await;
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
}

// A provider that accepts the upgrade and drops at once: the client has its
// 101 already (a streaming request's head goes at once), then a Close 1011.
#[tokio::test(flavor = "multi_thread")]
async fn a_provider_that_drops_at_once_is_a_101_then_a_close() {
    for via in Via::ALL {
        let probe = Probe::with(Behaviour {
            drop_at_once: true,
            ..Behaviour::default()
        })
        .await;
        let (gateway, ends, _calls) = via.gateway(&probe.base());
        let server = serve_typed(gateway).await;
        let mut ws = connect(server.addr, PATH, &[]).await.unwrap();
        assert_eq!(
            hear(&mut ws).await,
            Heard::Closed(Some((1011, "truncated".into())))
        );
        idle(&server.gateway).await;
        assert_eq!(code(&ends.only()), "truncated");
    }
}

// An abrupt client drop closes the provider's connection going away (1001);
// each hook runs once, told `abandoned`, and the gateway goes idle.
#[tokio::test(flavor = "multi_thread")]
async fn an_abrupt_client_drop_closes_the_provider_going_away() {
    for via in Via::ALL {
        let (probe, server, ends, calls, mut ws) = session(via).await;
        say(&mut ws, r#""one""#).await;
        eventually("the provider read it", || probe.texts() == [r#""one""#]).await;
        drop(ws);
        idle(&server.gateway).await;
        eventually("the provider's connection closed", || {
            probe.live.load(Ordering::SeqCst) == 0
        })
        .await;
        assert_eq!(
            probe.seen(),
            [Seen::Text(r#""one""#.into()), Seen::Close(Some(1001))],
            "{via:?}"
        );
        assert_eq!(
            ends.all().iter().map(code).collect::<Vec<_>>(),
            ["abandoned"]
        );
        assert_eq!(
            calls.all().iter().map(code).collect::<Vec<_>>(),
            ["abandoned"]
        );
    }
}

// The same, before the client said anything.
#[tokio::test(flavor = "multi_thread")]
async fn an_abrupt_drop_before_any_frame_ends_the_same() {
    for via in Via::ALL {
        let (probe, server, ends, calls, ws) = session(via).await;
        drop(ws);
        idle(&server.gateway).await;
        eventually("the provider's connection closed", || {
            probe.live.load(Ordering::SeqCst) == 0
        })
        .await;
        assert_eq!(probe.seen(), [Seen::Close(Some(1001))], "{via:?}");
        assert_eq!(
            ends.all().iter().map(code).collect::<Vec<_>>(),
            ["abandoned"]
        );
        assert_eq!(
            calls.all().iter().map(code).collect::<Vec<_>>(),
            ["abandoned"]
        );
    }
}

// A client's binary frame is a fault: the client is closed with 1003, the
// provider's connection is closed going away, and no hook runs twice.
#[tokio::test(flavor = "multi_thread")]
async fn a_binary_frame_from_the_client_closes_it_with_1003() {
    use futures::SinkExt;
    for via in Via::ALL {
        let (probe, server, ends, calls, mut ws) = session(via).await;
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
        // Never relayed.
        assert_eq!(probe.seen(), [Seen::Close(Some(1001))], "{via:?}");
    }
}

// A provider's binary frame is a fault too: the client is told 1011.
#[tokio::test(flavor = "multi_thread")]
async fn a_binary_frame_from_the_provider_is_a_fault() {
    for via in Via::ALL {
        let (probe, server, ends, _calls, mut ws) = session(via).await;
        probe.tell(0, Cmd::SendBinary(vec![9]));
        assert_eq!(
            hear(&mut ws).await,
            Heard::Closed(Some((1011, "upstream".into())))
        );
        idle(&server.gateway).await;
        assert_eq!(code(&ends.only()), "upstream");
    }
}

/// A plugin whose filter refuses a client's `"forbidden"` with Close 1008.
struct Forbid;

#[async_trait]
impl Plugin for Forbid {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        cx.filter(|e: &mut Input| match e.json() == "forbidden" {
            true => Err(Fault::new("the input is refused")
                .with(ErrorCode("policy".into()))
                .with(CloseCode(1008))),
            false => Ok(()),
        })?;
        Ok(None)
    }
}

// An input a filter refuses ends the session: the provider is sent Close
// with the fault's `CloseCode`, and the client is closed with it and the
// fault's code.
#[tokio::test(flavor = "multi_thread")]
async fn a_refused_input_closes_both_sides_with_its_code() {
    let probe = Probe::start().await;
    let (gateway, ends, calls) = Via::Typed.gateway(&probe.base());
    let server = serve_typed(gateway.plugin(Forbid)).await;
    let mut ws = connect(server.addr, PATH, &[]).await.unwrap();
    probe.connected(0).await;
    say(&mut ws, r#""fine""#).await;
    say(&mut ws, r#""forbidden""#).await;
    assert_eq!(
        hear(&mut ws).await,
        Heard::Closed(Some((1008, "policy".into())))
    );
    idle(&server.gateway).await;
    eventually("the provider's connection closed", || {
        probe.live.load(Ordering::SeqCst) == 0
    })
    .await;
    assert_eq!(
        probe.seen(),
        [Seen::Text(r#""fine""#.into()), Seen::Close(Some(1008))]
    );
    assert_eq!(code(&ends.only()), "policy");
    assert_eq!(calls.all().iter().map(code).collect::<Vec<_>>(), ["policy"]);
}

// What the client sees of a provider that is not there: an ordinary HTTP
// answer to the handshake, before any upgrade (502 with text/plain and no
// address in it), because the call is sent before the head is. The JSON
// events format says only the error code; `Raw` says the message.
#[tokio::test(flavor = "multi_thread")]
async fn an_unreachable_provider_is_a_502_to_the_handshake() {
    for via in Via::ALL {
        let gone = nowhere().await;
        let (gateway, ends, calls) = via.gateway(&format!("ws://{gone}"));
        let server = serve_typed(gateway).await;
        let refused = connect(server.addr, PATH, &[]).await;
        let Err(tokio_tungstenite::tungstenite::Error::Http(resp)) = refused else {
            panic!("expected an HTTP answer to the handshake")
        };
        assert_eq!(resp.status(), 502);
        assert_eq!(resp.headers()["content-type"], "text/plain; charset=utf-8");
        let body = String::from_utf8(resp.body().clone().unwrap()).unwrap();
        let said = match via {
            Via::Relay => "the provider could not be reached",
            Via::Typed => "upstream",
        };
        assert_eq!(body, said);
        assert!(!body.contains(&gone.to_string()), "no address: {body}");
        idle(&server.gateway).await;
        assert_eq!(code(&ends.only()), "upstream");
        assert_eq!(
            calls.all().iter().map(code).collect::<Vec<_>>(),
            ["upstream"]
        );
    }
}

// A request no served format accepts is refused before the upgrade too.
#[tokio::test(flavor = "multi_thread")]
async fn a_request_no_format_accepts_is_a_404_to_the_handshake() {
    let probe = Probe::start().await;
    // A gateway that serves nothing.
    let server = serve(Gateway::new()).await;
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
    for via in Via::ALL {
        let (probe, server, ends, _calls, mut ws) = session(via).await;
        ws.send(Message::Ping(vec![1, 2].into())).await.unwrap();
        let pong = tokio::time::timeout(PATIENCE, ws.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(pong, Message::Pong(vec![1, 2].into()));
        // The provider pings; the session goes on, and the client hears no text.
        probe.tell(0, Cmd::Ping);
        probe.tell(0, Cmd::Send(r#""after""#.into()));
        assert_eq!(heard_text(&mut ws).await, r#""after""#);
        say(&mut ws, r#""still here""#).await;
        eventually("the provider read it", || {
            probe.texts() == [r#""still here""#]
        })
        .await;
        close(&mut ws, 1000, "").await;
        idle(&server.gateway).await;
        assert_eq!(code(&ends.only()), "ok");
    }
}

/// A session sent `via`, served with `until` made per session, through a
/// probe provider.
async fn ending(
    via: Via,
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
    let (gateway, ends, calls) = via.gateway(&probe.base());
    let app = axum::Router::new()
        .fallback(
            move |State(gw): State<Gateway>, ws: WebSocketUpgrade, req: Request| {
                let until = until.clone();
                async move {
                    let limits = Limits::default();
                    respan_gateway_axum::ws::serve_ws_until(&gw, ws, req, limits, until()).await
                }
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
// idle, and the provider's connection is closed going away too.
#[tokio::test(flavor = "multi_thread")]
async fn a_session_over_its_maximum_length_is_closed_going_away() {
    for via in Via::ALL {
        let (probe, _ends, _calls, mut ws, task, gateway) = ending(via, || {
            Box::pin(async {
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                "session_too_long"
            })
        })
        .await;
        say(&mut ws, r#""hello""#).await;
        assert_eq!(
            hear(&mut ws).await,
            Heard::Closed(Some((1001, "session_too_long".into())))
        );
        idle(&gateway).await;
        eventually("the provider's connection closed", || {
            probe.live.load(Ordering::SeqCst) == 0
        })
        .await;
        assert_eq!(
            probe.seen(),
            [Seen::Text(r#""hello""#.into()), Seen::Close(Some(1001))],
            "{via:?}"
        );
        task.abort();
    }
}

// At shutdown every open session is closed 1001 "draining"; one that has
// already ended is not affected.
#[tokio::test(flavor = "multi_thread")]
async fn a_shutdown_drains_every_open_session() {
    for via in Via::ALL {
        let (stop, stopped) = tokio::sync::watch::channel(false);
        let (probe, ends, _calls, mut ws, task, gateway) = ending(via, move || {
            let mut stopped = stopped.clone();
            Box::pin(async move {
                let _ = stopped.wait_for(|stop| *stop).await;
                "draining"
            })
        })
        .await;
        say(&mut ws, r#""one""#).await;
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
}

// A session that ends first is not touched by an `until` that never comes.
#[tokio::test(flavor = "multi_thread")]
async fn a_session_that_ends_first_closes_as_it_would() {
    for via in Via::ALL {
        let (_probe, ends, _calls, mut ws, task, gateway) =
            ending(via, || Box::pin(std::future::pending())).await;
        say(&mut ws, r#""one""#).await;
        close(&mut ws, 1000, "bye").await;
        assert!(matches!(
            hear(&mut ws).await,
            Heard::Closed(_) | Heard::Broken
        ));
        idle(&gateway).await;
        assert_eq!(code(&ends.only()), "ok");
        task.abort();
    }
}
