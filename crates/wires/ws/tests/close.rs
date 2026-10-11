//! How a session ends, from either side, and what each hook is told: each
//! relayed unread with `Relay::RAW` and typed as JSON events with no relay
//! ([`Via`]). Texts are JSON, which both ways carry as they are.
#![allow(clippy::unwrap_used)]

mod support;

use std::sync::atomic::Ordering;

use respan_gateway_core::{
    ClientContext, ErrorCode, Fault, Gateway, Plugin, Response, async_trait,
};
use respan_gateway_realtime::Input;
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

// A client's binary frame, or a message over `max_message`, is the
// client's fault, not its leaving: it is closed with 1003 or 1009, the
// provider with the same, and each hook is told `bad_request` once.
#[tokio::test(flavor = "multi_thread")]
async fn a_binary_or_oversized_client_message_closes_it_with_1003_or_1009() {
    use futures::SinkExt;
    use tokio_tungstenite::tungstenite::Message;
    let limits = Limits {
        max_message: 1024,
        ..Limits::default()
    };
    for via in Via::ALL {
        for (message, want) in [
            (Message::Binary(vec![1, 2, 3].into()), 1003),
            (Message::Text("x".repeat(4096).into()), 1009),
        ] {
            let probe = Probe::start().await;
            let (gateway, ends, calls) = via.gateway(&probe.base());
            let server = serve_with(gateway, limits, std::future::pending()).await;
            let mut ws = connect(server.addr, PATH, &[]).await.unwrap();
            probe.connected(0).await;
            ws.send(message).await.unwrap();
            assert_eq!(
                hear(&mut ws).await,
                Heard::Closed(Some((want, "bad_request".into()))),
                "{via:?}"
            );
            idle(&server.gateway).await;
            eventually("the provider's connection closed", || {
                probe.live.load(Ordering::SeqCst) == 0
            })
            .await;
            assert_eq!(code(&ends.only()), "bad_request", "{via:?} {want}");
            let calls = calls.all();
            assert_eq!(calls.iter().map(code).collect::<Vec<_>>(), ["bad_request"]);
            // Never relayed.
            assert_eq!(probe.seen(), [Seen::Close(Some(want))], "{via:?}");
        }
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
        cx.filter(
            |e: &mut Input| match matches!(e, Input::Native(e) if *e.value == "forbidden") {
                true => Err(Fault::new("the input is refused")
                    .with(ErrorCode("policy".into()))
                    .with(CloseCode(1008))),
                false => Ok(()),
            },
        )?;
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

// At shutdown (`stop`) every open session is closed 1001, the provider's
// connection too; the request ends once.
#[tokio::test(flavor = "multi_thread")]
async fn a_shutdown_drains_every_open_session() {
    for via in Via::ALL {
        let probe = Probe::start().await;
        let (gateway, ends, _calls) = via.gateway(&probe.base());
        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        let stopped = async {
            let _ = stopped.await;
        };
        let server = serve_with(gateway, Limits::default(), stopped).await;
        let mut ws = connect(server.addr, PATH, &[]).await.unwrap();
        probe.connected(0).await;
        say(&mut ws, r#""one""#).await;
        eventually("the provider read it", || probe.texts().len() == 1).await;
        stop.send(()).unwrap();
        assert_eq!(
            hear(&mut ws).await,
            Heard::Closed(Some((1001, String::new())))
        );
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
        assert_eq!(ends.all().len(), 1, "the request ended once");
    }
}

// A request that is no upgrade is served as plain HTTP by the same router.
#[tokio::test(flavor = "multi_thread")]
async fn a_request_that_is_no_upgrade_is_served_as_http() {
    let probe = Probe::start().await;
    let (gateway, ..) = Via::Typed.gateway(&probe.base());
    let server = serve_typed(gateway).await;
    let resp = reqwest::get(format!("http://{}{PATH}", server.addr))
        .await
        .unwrap();
    // `router`'s answer: no format serves a `GET` that is no upgrade.
    assert_eq!(resp.status(), 404);
    let body = resp.text().await.unwrap();
    assert_eq!(body, format!("no format served accepts GET {PATH}"));
    assert_eq!(probe.total.load(Ordering::SeqCst), 0);
}

// An upgrade to a WebSocket that is no valid handshake (here, no key) is
// refused 400 before the gateway sees it.
#[tokio::test(flavor = "multi_thread")]
async fn an_upgrade_that_is_no_handshake_is_a_400() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let probe = Probe::start().await;
    let (gateway, ends, _calls) = Via::Typed.gateway(&probe.base());
    let server = serve_typed(gateway).await;
    let mut socket = tokio::net::TcpStream::connect(server.addr).await.unwrap();
    let head = format!(
        "GET {PATH} HTTP/1.1\r\nhost: x\r\nupgrade: websocket\r\nconnection: upgrade\r\n\r\n"
    );
    socket.write_all(head.as_bytes()).await.unwrap();
    let mut answer = vec![0; 1024];
    let n = socket.read(&mut answer).await.unwrap();
    let answer = String::from_utf8_lossy(&answer[..n]);
    assert!(answer.starts_with("HTTP/1.1 400"), "{answer}");
    assert!(answer.ends_with("bad_request"), "{answer}");
    assert!(ends.all().is_empty());
}

/// Limits that ping a quiet peer after 100 ms, so it is gone after 200 ms.
fn pinging() -> Limits {
    Limits {
        ping_after: std::time::Duration::from_millis(100),
        ..Limits::default()
    }
}

// A client that is quiet but reads, so answers pings, keeps its session; one
// that does not answer is gone, as if it dropped: the provider is closed
// going away, and each hook is told `abandoned`.
#[tokio::test(flavor = "multi_thread")]
async fn a_client_silent_through_a_ping_is_gone() {
    use futures::StreamExt;
    use tokio_tungstenite::tungstenite::Message;
    for via in Via::ALL {
        let probe = Probe::start().await;
        let (gateway, ends, calls) = via.gateway(&probe.base());
        let server = serve_with(gateway, pinging(), std::future::pending()).await;
        let mut ws = connect(server.addr, PATH, &[]).await.unwrap();
        probe.connected(0).await;
        // Reading, it is pinged and answers.
        let mut pings = 0;
        let quiet = tokio::time::sleep(std::time::Duration::from_millis(500));
        tokio::pin!(quiet);
        loop {
            tokio::select! {
                () = &mut quiet => break,
                message = ws.next() => match message {
                    Some(Ok(Message::Ping(_))) => pings += 1,
                    other => panic!("{via:?}: {other:?}"),
                },
            }
        }
        assert!(pings >= 2, "{via:?}: {pings} pings");
        say(&mut ws, r#""still here""#).await;
        eventually("the provider read it", || probe.texts().len() == 1).await;
        // Not reading, it answers none.
        idle(&server.gateway).await;
        eventually("the provider's connection closed", || {
            probe.live.load(Ordering::SeqCst) == 0
        })
        .await;
        assert_eq!(probe.seen().last(), Some(&Seen::Close(Some(1001))));
        assert_eq!(code(&ends.only()), "abandoned");
        assert_eq!(
            calls.all().iter().map(code).collect::<Vec<_>>(),
            ["abandoned"]
        );
        drop(ws);
    }
}

// A provider that does not answer a ping is gone too: `truncated`.
#[tokio::test(flavor = "multi_thread")]
async fn a_provider_silent_through_a_ping_is_truncated() {
    let probe = Probe::start().await;
    // Not reading, it answers no ping.
    probe.reading.store(false, Ordering::SeqCst);
    let (ends, calls) = (Outcomes::default(), Calls::default());
    let transport = respan_gateway_ws::WsTransport::new(probe.base())
        .unwrap()
        .with_limits(pinging());
    let gateway = Gateway::new()
        .client::<Input, _, _>(respan_gateway_realtime_ws::OpenAiRealtime)
        .unwrap()
        .provider::<Input, _, _>(
            "p",
            respan_gateway_realtime_ws::OpenAiRealtime,
            transport.connect(),
        )
        .unwrap()
        .plugin(ends.clone())
        .plugin(calls.clone())
        .target("p")
        .unwrap();
    let server = serve_typed(gateway).await;
    let mut ws = connect(server.addr, PATH, &[]).await.unwrap();
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
