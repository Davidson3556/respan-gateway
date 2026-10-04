#![cfg(feature = "ws")]
//! A session relayed through the opaque format arrives byte for byte, both
//! ways, against two providers' real protocols (mockllm's OpenAI Realtime
//! and Gemini Live) through the same `WsTransport`: the SDK knows neither.
#![allow(clippy::unwrap_used)]

mod support;

use mockllm::{Answer, FakeGemini, FakeOpenAi};
use serde_json::{Value, json};
use support::*;

const REALTIME: &str = "/v1/realtime?model=gpt-realtime";
const LIVE: &str =
    "/ws/google.ai.generativelanguage.v1beta.GenerativeService.BidiGenerateContent?key=AIza-test";

fn ws_base(http_base: &str) -> String {
    let url = http_base.replace("http://", "ws://");
    url.trim_end_matches("/v1")
        .trim_end_matches("/v1beta")
        .to_owned()
}

fn is(text: &str, name: &str) -> bool {
    serde_json::from_str::<Value>(text).is_ok_and(|v| v["type"] == name)
}

// Odd spacing and key order that a JSON round trip would lose.
const UPDATE: &str = r#"{ "type" : "session.update",  "session":{"type":"realtime","output_modalities":["text"]} , "event_id":"evt_1" }"#;
const CREATE: &str =
    "{\"type\":\"response.create\",\"response\":{\"instructions\":\"\\u0048i \\/ there\"}}";

#[tokio::test(flavor = "multi_thread")]
async fn a_realtime_session_is_byte_exact_both_ways() {
    let openai = FakeOpenAi::start().await;
    openai.answer(Answer::text("Hi there"));
    let tap = Tap::to(ws_base(&openai.base_url())).await;
    let (gateway, ends, calls) = relay(&tap.base());
    let server = serve(gateway).await;

    let mut ws = connect(
        server.addr,
        REALTIME,
        &[("authorization", "Bearer sk-test")],
    )
    .await
    .unwrap();
    let mut heard = vec![heard_text(&mut ws).await];
    assert!(is(&heard[0], "session.created"), "{}", heard[0]);
    say(&mut ws, UPDATE).await;
    heard.push(heard_text(&mut ws).await);
    assert!(is(heard.last().unwrap(), "session.updated"));
    say(&mut ws, CREATE).await;
    let turn = hear_until(&mut ws, |t| is(t, "response.done")).await;
    let types: Vec<String> = turn
        .iter()
        .map(|t| serde_json::from_str::<Value>(t).unwrap()["type"].to_string())
        .collect();
    assert!(
        types.contains(&"\"response.output_text.delta\"".to_owned()),
        "{types:?}"
    );
    heard.extend(turn);

    // What the client sent is what the provider got; what the provider sent
    // is what the client heard: the same strings, not the same JSON.
    assert_eq!(tap.sent_by_gateway(), [UPDATE, CREATE]);
    assert_eq!(tap.sent_by_provider(), heard);
    // ... and the provider's fake did read them as Realtime events.
    let events: Vec<Value> = openai
        .requests()
        .iter()
        .filter(|r| r.method == "WS")
        .map(|r| r.body.clone())
        .collect();
    assert_eq!(events[0]["session"]["output_modalities"], json!(["text"]));

    close(&mut ws, 1000, "").await;
    assert!(matches!(
        hear(&mut ws).await,
        Heard::Closed(_) | Heard::Broken
    ));
    idle(&server.gateway).await;
    assert_eq!(code(&ends.only()), "ok");
    assert_eq!(calls.all().iter().map(code).collect::<Vec<_>>(), ["ok"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_gemini_live_session_is_byte_exact_both_ways() {
    let gemini = FakeGemini::start().await;
    gemini.answer(Answer::text("Paris it is"));
    let tap = Tap::to(ws_base(&gemini.base_url())).await;
    let (gateway, ends, calls) = relay(&tap.base());
    let server = serve(gateway).await;

    // The key travels in the query: the adapter lifts it, a relay restores it.
    let mut ws = connect(server.addr, LIVE, &[]).await.unwrap();
    let setup = r#"{"setup": {"model":"models/gemini-3.8-live", "generationConfig":{"responseModalities":["TEXT"]}}}"#;
    say(&mut ws, setup).await;
    let mut heard = vec![heard_text(&mut ws).await];
    assert_eq!(heard[0], r#"{"setupComplete":{}}"#);
    let ask = r#"{"realtimeInput":  {"text":"capital of France?"}}"#;
    say(&mut ws, ask).await;
    heard.extend(hear_until(&mut ws, |t| t.contains("turnComplete")).await);

    assert_eq!(tap.sent_by_gateway(), [setup, ask]);
    assert_eq!(tap.sent_by_provider(), heard);
    let text: String = heard
        .iter()
        .filter_map(|t| serde_json::from_str::<Value>(t).ok())
        .filter_map(|v| {
            v["serverContent"]["modelTurn"]["parts"][0]["text"]
                .as_str()
                .map(str::to_owned)
        })
        .collect();
    assert_eq!(text, "Paris it is");

    close(&mut ws, 1000, "").await;
    assert!(matches!(
        hear(&mut ws).await,
        Heard::Closed(_) | Heard::Broken
    ));
    idle(&server.gateway).await;
    assert_eq!(code(&ends.only()), "ok");
    assert_eq!(calls.all().iter().map(code).collect::<Vec<_>>(), ["ok"]);
}

// The provider's refusal of the upgrade (mockllm answers 401 to a key-less
// handshake) reaches the client under its status, in plain text; the
// passthrough reads no body's words, so the call's fault is `upstream`. The gateway
// sends a streaming request's head at once, so the refusal as the provider's
// format would write it arrives as a head followed by a failing body; the
// adapter writes the failure whole instead.
#[tokio::test(flavor = "multi_thread")]
async fn a_provider_that_refuses_the_upgrade_is_refused_under_its_status() {
    let openai = FakeOpenAi::start().await;
    let (gateway, ends, calls) = relay(&ws_base(&openai.base_url()));
    let server = serve(gateway).await;
    let refused = connect(server.addr, REALTIME, &[]).await;
    let Err(tokio_tungstenite::tungstenite::Error::Http(resp)) = refused else {
        panic!("expected an HTTP answer to the handshake")
    };
    assert_eq!(resp.status(), 401);
    let body = String::from_utf8(resp.body().clone().unwrap()).unwrap();
    assert!(body.contains("You didn't provide an API key"), "{body}");
    idle(&server.gateway).await;
    assert_eq!(code(&ends.only()), "upstream");
    assert_eq!(calls.all().len(), 1);
}

// A 3xx to the handshake is no switch: the session the call asked for did
// not open, so the call is refused, as a 401 is (a fallback may try another
// provider), and the client gets the provider's 3xx as it came, to follow
// if it will (RFC 6455 4.1). To a request that is no upgrade a 3xx is an
// answer, relayed (`apps/gateway/tests/redirects.rs`).
#[tokio::test(flavor = "multi_thread")]
async fn a_redirect_to_the_upgrade_is_a_refusal_the_client_gets_as_it_came() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((mut sock, _)) = listener.accept().await {
            let (mut buf, mut got) = (vec![0u8; 4096], Vec::new());
            while !got.windows(4).any(|w| w == b"\r\n\r\n") {
                match sock.read(&mut buf).await {
                    Ok(0) | Err(_) => break,
                    Ok(n) => got.extend_from_slice(&buf[..n]),
                }
            }
            let answer = "HTTP/1.1 302 Found\r\nlocation: ws://elsewhere/v1/realtime\r\n\
                          content-length: 5\r\n\r\nmoved";
            let _ = sock.write_all(answer.as_bytes()).await;
        }
    });
    let (gateway, ends, calls) = relay(&format!("ws://{addr}"));
    let server = serve(gateway).await;
    let refused = connect(server.addr, REALTIME, &[]).await;
    let Err(tokio_tungstenite::tungstenite::Error::Http(resp)) = refused else {
        panic!("expected an HTTP answer to the handshake")
    };
    assert_eq!(resp.status(), 302);
    assert_eq!(resp.headers()["location"], "ws://elsewhere/v1/realtime");
    assert_eq!(resp.body().as_deref(), Some(&b"moved"[..]));
    idle(&server.gateway).await;
    let (end, call) = (ends.only(), calls.all());
    assert_ne!(code(&end), "ok", "the request is refused");
    assert_eq!(call.len(), 1);
    assert_ne!(code(&call[0]), "ok", "the call is refused");
}
