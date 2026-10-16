//! A gateway between a realtime client and a strict fake, and a client of
//! it that keeps what it said and heard.

use parking_lot::Mutex;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use respan_gateway_core::futures::{SinkExt, StreamExt};
use respan_gateway_core::{
    ClientContext, Fault, Format, Gateway, Plugin, Provider, Response, async_trait,
};
use respan_gateway_http::http::{HeaderMap, HeaderValue};
use respan_gateway_http::{ClientIo, ProviderIo};
use respan_gateway_realtime::{Input, Output};
use respan_gateway_ws::WsTransport;
use serde_json::Value;
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

use crate::realtime::Fake;

/// The events plugins saw at the client's edge.
#[derive(Clone, Default)]
pub struct Seen {
    inputs: Arc<Mutex<Vec<Input>>>,
    outputs: Arc<Mutex<Vec<Output>>>,
}

impl Seen {
    pub fn inputs(&self) -> Vec<Input> {
        self.inputs.lock().clone()
    }

    pub fn outputs(&self) -> Vec<Output> {
        self.outputs.lock().clone()
    }
}

#[async_trait]
impl Plugin for Seen {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let inputs = self.inputs.clone();
        cx.filter(move |e: &mut Input| {
            inputs.lock().push(e.clone());
            Ok(())
        })?;
        let outputs = self.outputs.clone();
        cx.filter(move |e: &mut Output| {
            outputs.lock().push(e.clone());
            Ok(())
        })?;
        Ok(None)
    }
}

/// A key a provider takes, as a header.
pub const OPENAI_KEY: (&str, &str) = ("authorization", "Bearer sk-test");
pub const GOOGLE_KEY: (&str, &str) = ("x-goog-api-key", "AIza-test");

/// A transport `transport(base_url)` makes, sending `key`.
pub fn keyed(transport: WsTransport, key: (&'static str, &str)) -> WsTransport {
    let mut headers = HeaderMap::new();
    headers.insert(key.0, HeaderValue::from_str(key.1).unwrap());
    transport.with_headers(headers).unwrap()
}

/// A gateway serving `client`, its one provider of format `provider` at
/// `transport`, its target.
pub async fn serve<C, P>(client: C, provider: P, transport: WsTransport) -> (SocketAddr, Seen)
where
    C: Format<ClientIo, Input, Output>,
    P: Format<ProviderIo, Output, Input>,
{
    let seen = Seen::default();
    let gateway = Gateway::new()
        .client::<Input, _, _>(client)
        .unwrap()
        .provider(Provider::new("p", provider, transport.connect()))
        .unwrap()
        .plugin(seen.clone())
        .target("p")
        .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = respan_gateway_serve::Host::new(gateway)
        .limit(|_| 32 << 20)
        .router();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (addr, seen)
}

/// A client of the gateway, which keeps what it said and heard.
pub struct Client {
    ws: WebSocketStream<MaybeTlsStream<TcpStream>>,
    pub said: Vec<Value>,
    pub heard: Vec<Value>,
}

impl Client {
    pub async fn connect(addr: SocketAddr, path: &str) -> Self {
        let url = format!("ws://{addr}{path}");
        let (ws, _) = tokio_tungstenite::connect_async(url).await.unwrap();
        Self {
            ws,
            said: Vec::new(),
            heard: Vec::new(),
        }
    }

    /// The status a session at `path` is refused with.
    pub async fn refused(addr: SocketAddr, path: &str) -> u16 {
        let url = format!("ws://{addr}{path}");
        match tokio_tungstenite::connect_async(url).await {
            Err(tokio_tungstenite::tungstenite::Error::Http(answer)) => answer.status().as_u16(),
            other => panic!("expected a refusal, got {other:?}"),
        }
    }

    pub async fn say(&mut self, event: Value) {
        let text = event.to_string().into();
        self.ws.send(Message::Text(text)).await.unwrap();
        self.said.push(event);
    }

    pub async fn hear(&mut self) -> Value {
        match self.next().await {
            Ok(event) => event,
            Err(code) => panic!("expected an event, the session closed {code:?}"),
        }
    }

    /// The code the session closes with, past what it hears first.
    pub async fn closed(&mut self) -> Option<u16> {
        loop {
            if let Err(code) = self.next().await {
                return code;
            }
        }
    }

    /// The next event, or the code of the session's close.
    async fn next(&mut self) -> Result<Value, Option<u16>> {
        loop {
            let next = tokio::time::timeout(Duration::from_secs(5), self.ws.next());
            let event: Value = match next.await.expect("an event in time") {
                Some(Ok(Message::Text(text))) => serde_json::from_str(&text).unwrap(),
                Some(Ok(Message::Ping(_) | Message::Pong(_))) => continue,
                Some(Ok(Message::Close(frame))) => return Err(frame.map(|f| f.code.into())),
                other => panic!("expected an event, heard {other:?}"),
            };
            self.heard.push(event.clone());
            return Ok(event);
        }
    }

    /// What it hears up to and including the first event `last` says ends.
    pub async fn until(&mut self, last: impl Fn(&Value) -> bool) -> Vec<Value> {
        let mut events = Vec::new();
        loop {
            let event = self.hear().await;
            let done = last(&event);
            events.push(event);
            if done {
                return events;
            }
        }
    }

    /// The fake read what the client said, and the client heard what the
    /// fake sent, event for event.
    pub async fn relayed(&self, fake: &Fake) {
        assert_eq!(fake.received(self.said.len()).await, self.said);
        assert_eq!(self.heard, fake.sent());
    }
}

pub fn of_type(kind: &'static str) -> impl Fn(&Value) -> bool {
    move |event| event["type"] == kind
}

pub fn has(field: &'static str) -> impl Fn(&Value) -> bool {
    move |message| message.get(field).is_some()
}

/// `ms` of 16-bit PCM at `rate`, a ramp.
pub fn pcm_at(rate: usize, ms: usize) -> Vec<u8> {
    (0..ms * rate / 1_000)
        .flat_map(|i| (i as i16).to_le_bytes())
        .collect()
}

/// `ms` of 24 kHz PCM.
pub fn pcm(ms: usize) -> Vec<u8> {
    pcm_at(24_000, ms)
}

pub fn base64(data: &[u8]) -> String {
    STANDARD.encode(data)
}

pub fn unbase64(v: &Value) -> Vec<u8> {
    STANDARD.decode(v.as_str().unwrap()).unwrap()
}
