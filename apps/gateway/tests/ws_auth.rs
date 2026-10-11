//! A browser's WebSocket can set no header, so with `keys:` set it presents
//! the gateway key as a subprotocol (OpenAI's convention) or as `?key=`
//! (Gemini's). The gateway accepts both on an upgrade, refuses a wrong key
//! on either, and never passes the key on.

#![allow(clippy::unwrap_used)]

#[allow(dead_code)]
#[path = "../../../tests/support/realtime/mod.rs"]
mod realtime;
mod support;

use support::Gateway;
use tokio_tungstenite::tungstenite::Error;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;

const LIVE: &str = "/ws/google.ai.generativelanguage.v1beta.GenerativeService.BidiGenerateContent";

/// What a browser's upgrade to `path` gets: the subprotocol chosen, or the
/// status that refused it.
async fn upgrade(gw: &Gateway, path: &str, protocols: &str) -> Result<Option<String>, u16> {
    let url = format!("{}{path}", gw.base.replace("http://", "ws://"));
    let mut req = url.into_client_request().unwrap();
    if !protocols.is_empty() {
        let offered = protocols.parse().unwrap();
        req.headers_mut().insert("sec-websocket-protocol", offered);
    }
    match tokio_tungstenite::connect_async(req).await {
        Ok((_, resp)) => {
            let chosen = resp.headers().get("sec-websocket-protocol");
            Ok(chosen.and_then(|v| v.to_str().ok()).map(str::to_owned))
        }
        Err(Error::Http(resp)) => Err(resp.status().as_u16()),
        Err(e) => panic!("{e}"),
    }
}

async fn gateway(openai: &realtime::Fake, gemini: &realtime::Fake) -> Gateway {
    let http = |ws: String| ws.replace("ws://", "http://");
    Gateway::start(&format!(
        "client_keys: [gw-key]
providers:
  openai: {{ vendor: openai, base_url: '{}', key: sk-provider }}
  gemini: {{ vendor: gemini, base_url: '{}/v1beta/openai', key: AIza-provider }}
formats: [openai-realtime, gemini-live]
",
        http(openai.base_url()),
        http(gemini.base_url())
    ))
    .await
}

/// Nothing a fake read carries the gateway key.
fn never_saw_the_key(fake: &realtime::Fake) {
    let seen = format!("{:?}", fake.handshakes());
    assert!(!seen.contains("gw-key"), "{seen}");
}

#[tokio::test]
async fn a_browser_presents_the_gateway_key_as_a_subprotocol() {
    let (openai, gemini) = (
        realtime::openai::start().await,
        realtime::gemini::start().await,
    );
    let gw = gateway(&openai, &gemini).await;
    let path = "/v1/realtime?model=gpt-realtime";

    let offer = "realtime, openai-insecure-api-key.gw-key";
    let chosen = upgrade(&gw, path, offer).await;
    assert_eq!(chosen, Ok(Some("realtime".to_owned())));
    let seen = &openai.handshakes()[0];
    assert_eq!(seen.header("authorization"), Some("Bearer sk-provider"));
    never_saw_the_key(&openai);

    let wrong = "realtime, openai-insecure-api-key.sk-other";
    assert_eq!(upgrade(&gw, path, wrong).await, Err(401));
    assert_eq!(upgrade(&gw, path, "realtime").await, Err(401));
    assert_eq!(openai.handshakes().len(), 1);
}

#[tokio::test]
async fn a_browser_presents_the_gateway_key_as_a_query_parameter() {
    let (openai, gemini) = (
        realtime::openai::start().await,
        realtime::gemini::start().await,
    );
    let gw = gateway(&openai, &gemini).await;

    let live = format!("{LIVE}?model=gemini-live&key=gw-key&alt=json");
    assert_eq!(upgrade(&gw, &live, "").await, Ok(None));
    let seen = &gemini.handshakes()[0];
    assert_eq!(seen.header("x-goog-api-key"), Some("AIza-provider"));
    assert_eq!(
        seen.query("alt"),
        None,
        "a Gemini setup is made by the writer"
    );
    never_saw_the_key(&gemini);

    let wrong = format!("{LIVE}?model=gemini-live&key=sk-other");
    assert_eq!(upgrade(&gw, &wrong, "").await, Err(401));
    let none = format!("{LIVE}?model=gemini-live");
    assert_eq!(upgrade(&gw, &none, "").await, Err(401));
    assert_eq!(gemini.handshakes().len(), 1);

    // The same on the other route: OpenAI's client by `?key=`.
    let path = "/v1/realtime?model=gpt-realtime&key=gw-key";
    assert_eq!(
        upgrade(&gw, path, "realtime").await,
        Ok(Some("realtime".to_owned()))
    );
    never_saw_the_key(&openai);
}
