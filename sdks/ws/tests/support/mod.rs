//! What the tests of `respan-gateway-ws` share: a served gateway, a client
//! over a real socket, a scripted provider that records what it sees
//! (`Probe`), and a tap that forwards to another provider verbatim
//! (`Tap`).
#![allow(dead_code, clippy::unwrap_used)]

use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use respan_gateway_core::bytes::Bytes;
use respan_gateway_core::{Decoder, Encoder, Fault, Format, Gateway};
use respan_gateway_formats::opaque::{self, Got, Sent};
use respan_gateway_http::http::header::UPGRADE;
use respan_gateway_http::http::{HeaderMap, Method};
use respan_gateway_http::{Answer, Ask};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::{Error, Message};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

/// The longest a test waits for something that must happen.
pub const PATIENCE: Duration = Duration::from_secs(5);

/// A WebSocket session relayed unread: opaque traffic, the kernel's format
/// for what it does not read.
pub struct Raw;

impl Format for Raw {
    type Ask = Ask;
    type Answer = Answer;
    type Input = Sent<Raw>;
    type Output = Got<Raw>;

    fn name(&self) -> &str {
        "raw"
    }

    fn accepts(&self, head: &Ask) -> bool {
        head.method == Method::GET && head.headers.contains_key(UPGRADE)
    }

    fn request_decoder(&self, _head: &Ask) -> Box<dyn Decoder<Sent<Raw>>> {
        opaque::request_decoder::<Raw>()
    }

    fn request_encoder(
        &self,
        client: Option<&Ask>,
        _body: &mut Bytes,
        streams: bool,
    ) -> Box<dyn Encoder<Sent<Raw>, Ask>> {
        opaque::request_encoder::<Raw>(client, streams)
    }

    fn response_decoder(
        &self,
        answer: &Answer,
        call: &Ask,
        _sent: &Bytes,
    ) -> Box<dyn Decoder<Got<Raw>>> {
        opaque::response_decoder::<Raw>("raw", answer, call)
    }

    fn response_encoder(
        &self,
        answer: Option<&Answer>,
        ask: &Ask,
        _body: &Bytes,
        refusal: Option<&Fault>,
    ) -> Box<dyn Encoder<Got<Raw>, Answer>> {
        opaque::response_encoder::<Raw>("raw", answer, ask, refusal)
    }
}

/// A gateway serving over real sockets.
pub struct Server {
    pub addr: SocketAddr,
    pub gateway: Gateway,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Serve `gateway` on a free local port with the SDK's router.
pub async fn serve(gateway: Gateway) -> Server {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = respan_gateway_ws::router(gateway.clone());
    let task = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    Server {
        addr,
        gateway,
        task,
    }
}

/// An address nothing listens on.
pub async fn nowhere() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    listener.local_addr().unwrap()
}

pub type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// Connect as a client to `addr` at `path` (with its query), with `headers`.
pub async fn connect(addr: SocketAddr, path: &str, headers: &[(&str, &str)]) -> Result<Ws, Error> {
    let mut req = format!("ws://{addr}{path}").into_client_request().unwrap();
    for (name, value) in headers {
        let name: tokio_tungstenite::tungstenite::http::HeaderName = name.parse().unwrap();
        req.headers_mut().insert(name, value.parse().unwrap());
    }
    tokio_tungstenite::connect_async(req)
        .await
        .map(|(ws, _)| ws)
}

pub async fn say(ws: &mut Ws, text: &str) {
    ws.send(Message::Text(text.into())).await.unwrap();
}

/// What a client hears next.
#[derive(Debug, PartialEq)]
pub enum Heard {
    Text(String),
    /// A Close frame: its code and reason.
    Closed(Option<(u16, String)>),
    /// The connection ended without a Close, or broke.
    Broken,
}

pub async fn hear(ws: &mut Ws) -> Heard {
    let next = tokio::time::timeout(PATIENCE, ws.next()).await;
    match next.expect("nothing heard in time") {
        Some(Ok(Message::Text(text))) => Heard::Text(text.to_string()),
        Some(Ok(Message::Close(frame))) => {
            Heard::Closed(frame.map(|f| (u16::from(f.code), f.reason.to_string())))
        }
        Some(Ok(Message::Ping(_) | Message::Pong(_))) => Box::pin(hear(ws)).await,
        Some(Ok(other)) => panic!("unexpected {other:?}"),
        None | Some(Err(_)) => Heard::Broken,
    }
}

pub async fn heard_text(ws: &mut Ws) -> String {
    match hear(ws).await {
        Heard::Text(text) => text,
        other => panic!("expected a text frame, heard {other:?}"),
    }
}

/// Texts up to and including the first for which `done` is true.
pub async fn hear_until(ws: &mut Ws, done: impl Fn(&str) -> bool) -> Vec<String> {
    let mut all = Vec::new();
    loop {
        let text = heard_text(ws).await;
        let last = done(&text);
        all.push(text);
        if last {
            return all;
        }
    }
}

/// Close as a client: send a Close and wait for the end.
pub async fn close(ws: &mut Ws, code: u16, reason: &str) {
    let frame = CloseFrame {
        code: CloseCode::from(code),
        reason: reason.to_owned().into(),
    };
    ws.send(Message::Close(Some(frame))).await.unwrap();
}

/// Wait until `check` holds, or fail.
pub async fn eventually(what: &str, mut check: impl FnMut() -> bool) {
    let start = std::time::Instant::now();
    while !check() {
        assert!(start.elapsed() < PATIENCE, "never: {what}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// `gateway.idle()`, or fail.
pub async fn idle(gateway: &Gateway) {
    tokio::time::timeout(PATIENCE, gateway.idle())
        .await
        .expect("the gateway never went idle");
}

/// What a provider saw of a connection.
#[derive(Debug, Clone, PartialEq)]
pub enum Seen {
    Text(String),
    Binary(Vec<u8>),
    /// A Close frame, with its code.
    Close(Option<u16>),
    /// The connection ended without a Close.
    Ended,
}

/// What a test tells a `Probe`'s latest connection.
#[derive(Debug)]
pub enum Cmd {
    Send(String),
    SendBinary(Vec<u8>),
    Ping,
    /// Send Close with the code, then read on.
    Close(u16),
    /// Drop the connection without a Close.
    Drop,
}

/// The upgrade request of a connection, as the provider saw it.
#[derive(Debug, Clone)]
pub struct Upgrade {
    pub uri: String,
    pub headers: HeaderMap,
}

/// A provider of scripted behaviour that records what each connection sent
/// it: a raw WebSocket server.
pub struct Probe {
    pub addr: SocketAddr,
    seen: Arc<Mutex<Vec<Seen>>>,
    upgrades: Arc<Mutex<Vec<Upgrade>>>,
    /// Connections open now.
    pub live: Arc<AtomicUsize>,
    /// Connections accepted.
    pub total: Arc<AtomicUsize>,
    /// Frames a flood has sent.
    pub sent: Arc<AtomicUsize>,
    cmds: Arc<Mutex<Vec<mpsc::UnboundedSender<Cmd>>>>,
    /// Whether it reads at all; a paused one lets the socket fill.
    pub reading: Arc<std::sync::atomic::AtomicBool>,
    task: tokio::task::JoinHandle<()>,
}

/// What a `Probe` does by itself.
#[derive(Clone, Copy, Default)]
pub struct Behaviour {
    /// Reply to each text `t` with `echo:t`.
    pub echo: bool,
    /// Drop each connection as soon as it is made.
    pub drop_at_once: bool,
    /// Obey a text of `!drop` (drop the connection), `!close` (send Close
    /// 1001, then read on) and `!flood:<frames>:<size>` (send that many
    /// frames of that many bytes, as `<index>:xxx...`, counting them in
    /// [`Probe::sent`]).
    pub commands: bool,
}

impl Drop for Probe {
    fn drop(&mut self) {
        self.task.abort();
    }
}

struct Live(Arc<AtomicUsize>);

impl Drop for Live {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

impl Probe {
    pub async fn start() -> Self {
        Self::with(Behaviour::default()).await
    }

    pub async fn echoing() -> Self {
        Self::with(Behaviour {
            echo: true,
            ..Behaviour::default()
        })
        .await
    }

    pub async fn with(behaviour: Behaviour) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let shared: Shared = (
            Arc::default(),
            Arc::default(),
            Arc::default(),
            Arc::default(),
            Arc::default(),
            Arc::new(std::sync::atomic::AtomicBool::new(true)),
            Arc::default(),
        );
        let (seen, upgrades, live, total, cmds, reading, sent) = shared.clone();
        let task = tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                tokio::spawn(connection(stream, behaviour, shared.clone()));
            }
        });
        Probe {
            addr,
            seen,
            upgrades,
            live,
            total,
            sent,
            cmds,
            reading,
            task,
        }
    }

    /// The `ws://` base URL to give a transport.
    pub fn base(&self) -> String {
        format!("ws://{}", self.addr)
    }

    pub fn seen(&self) -> Vec<Seen> {
        lock(&self.seen).clone()
    }

    /// The texts received, in order.
    pub fn texts(&self) -> Vec<String> {
        let all = self.seen().into_iter();
        all.filter_map(|s| if let Seen::Text(t) = s { Some(t) } else { None })
            .collect()
    }

    pub fn upgrades(&self) -> Vec<Upgrade> {
        lock(&self.upgrades).clone()
    }

    /// Tell the `n`th connection (0 is the first) something.
    pub fn tell(&self, n: usize, cmd: Cmd) {
        let cmds = lock(&self.cmds);
        cmds[n].send(cmd).unwrap();
    }

    /// Wait for the `n`th connection to be accepted.
    pub async fn connected(&self, n: usize) {
        eventually("a provider connection", || lock(&self.cmds).len() > n).await;
    }
}

type Shared = (
    Arc<Mutex<Vec<Seen>>>,
    Arc<Mutex<Vec<Upgrade>>>,
    Arc<AtomicUsize>,
    Arc<AtomicUsize>,
    Arc<Mutex<Vec<mpsc::UnboundedSender<Cmd>>>>,
    Arc<std::sync::atomic::AtomicBool>,
    Arc<AtomicUsize>,
);

async fn connection(stream: TcpStream, behaviour: Behaviour, shared: Shared) {
    let (seen, upgrades, live, total, cmds, reading, sent) = shared;
    let _live = {
        live.fetch_add(1, Ordering::SeqCst);
        Live(live)
    };
    total.fetch_add(1, Ordering::SeqCst);
    #[allow(clippy::result_large_err)]
    let record = |req: &Request, mut resp: Response| {
        let (uri, headers) = (req.uri().to_string(), req.headers().clone());
        // A subprotocol the client offers is the one chosen.
        if let Some(offered) = headers.get("sec-websocket-protocol") {
            resp.headers_mut()
                .insert("sec-websocket-protocol", offered.clone());
        }
        lock(&upgrades).push(Upgrade { uri, headers });
        Ok(resp)
    };
    let Ok(mut ws) = tokio_tungstenite::accept_hdr_async(stream, record).await else {
        return;
    };
    let (tx, mut rx) = mpsc::unbounded_channel();
    lock(&cmds).push(tx);
    if behaviour.drop_at_once {
        return;
    }
    loop {
        if !reading.load(Ordering::SeqCst) {
            // Not reading: the socket fills. Commands still come.
            let cmd = tokio::time::timeout(Duration::from_millis(10), rx.recv()).await;
            if let Ok(Some(cmd)) = cmd
                && !act(&mut ws, cmd).await
            {
                return;
            }
            continue;
        }
        tokio::select! {
            message = ws.next() => match message {
                Some(Ok(Message::Text(text))) => {
                    lock(&seen).push(Seen::Text(text.to_string()));
                    if behaviour.commands {
                        match text.as_str() {
                            "!drop" => return,
                            "!close" => {
                                if !act(&mut ws, Cmd::Close(1001)).await {
                                    return;
                                }
                                continue;
                            }
                            flood if flood.starts_with("!flood:") => {
                                let mut it = flood.split(':').skip(1).map(|n| n.parse::<usize>().unwrap());
                                let (frames, size) = (it.next().unwrap(), it.next().unwrap());
                                for i in 0..frames {
                                    let head = format!("{i:08}:");
                                    let body = format!("{head}{}", "x".repeat(size.saturating_sub(head.len())));
                                    if ws.send(Message::Text(body.into())).await.is_err() {
                                        return;
                                    }
                                    sent.fetch_add(1, Ordering::SeqCst);
                                }
                                continue;
                            }
                            _ => {}
                        }
                    }
                    if behaviour.echo && ws.send(Message::Text(format!("echo:{text}").into())).await.is_err() {
                        return;
                    }
                }
                Some(Ok(Message::Binary(bytes))) => lock(&seen).push(Seen::Binary(bytes.to_vec())),
                Some(Ok(Message::Close(frame))) => {
                    lock(&seen).push(Seen::Close(frame.map(|f| u16::from(f.code))));
                    // The library replies as it is read on.
                    let _ = ws.next().await;
                    return;
                }
                Some(Ok(_)) => {}
                Some(Err(_)) | None => {
                    lock(&seen).push(Seen::Ended);
                    return;
                }
            },
            Some(cmd) = rx.recv() => if !act(&mut ws, cmd).await { return },
        }
    }
}

/// Do `cmd`; false if the connection is to be dropped.
async fn act(ws: &mut Ws2, cmd: Cmd) -> bool {
    match cmd {
        Cmd::Send(text) => ws.send(Message::Text(text.into())).await.is_ok(),
        Cmd::SendBinary(bytes) => ws.send(Message::Binary(bytes.into())).await.is_ok(),
        Cmd::Ping => ws.send(Message::Ping(vec![7].into())).await.is_ok(),
        Cmd::Close(code) => {
            let frame = CloseFrame {
                code: CloseCode::from(code),
                reason: "scripted".into(),
            };
            ws.send(Message::Close(Some(frame))).await.is_ok()
        }
        Cmd::Drop => false,
    }
}

type Ws2 = WebSocketStream<TcpStream>;

/// A provider in front of another that forwards every text frame verbatim
/// and records what went each way, so a test can compare what a client sent
/// with what the provider got, and the reverse.
pub struct Tap {
    pub addr: SocketAddr,
    /// The frames the gateway sent.
    pub from_gateway: Arc<Mutex<Vec<String>>>,
    /// The frames the provider sent.
    pub from_provider: Arc<Mutex<Vec<String>>>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Tap {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Tap {
    /// A tap to the provider at `upstream` (`ws://host:port`).
    pub async fn to(upstream: String) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (from_gateway, from_provider) =
            (Arc::new(Mutex::default()), Arc::new(Mutex::default()));
        let (a, b) = (from_gateway.clone(), from_provider.clone());
        let task = tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                tokio::spawn(tap(stream, upstream.clone(), a.clone(), b.clone()));
            }
        });
        Tap {
            addr,
            from_gateway,
            from_provider,
            task,
        }
    }

    pub fn base(&self) -> String {
        format!("ws://{}", self.addr)
    }

    pub fn sent_by_gateway(&self) -> Vec<String> {
        lock(&self.from_gateway).clone()
    }

    pub fn sent_by_provider(&self) -> Vec<String> {
        lock(&self.from_provider).clone()
    }
}

async fn tap(
    stream: TcpStream,
    upstream: String,
    from_gateway: Arc<Mutex<Vec<String>>>,
    from_provider: Arc<Mutex<Vec<String>>>,
) {
    let request = Arc::new(Mutex::new(None));
    let taken = request.clone();
    #[allow(clippy::result_large_err)]
    let record = move |req: &Request, resp: Response| {
        *lock(&taken) = Some((req.uri().to_string(), req.headers().clone()));
        Ok(resp)
    };
    let Ok(mut gateway) = tokio_tungstenite::accept_hdr_async(stream, record).await else {
        return;
    };
    let (uri, headers) = lock(&request).take().unwrap();
    let mut req = format!("{upstream}{uri}").into_client_request().unwrap();
    for name in ["authorization", "x-goog-api-key"] {
        if let Some(value) = headers.get(name) {
            req.headers_mut().insert(name, value.clone());
        }
    }
    let Ok((mut provider, _)) = tokio_tungstenite::connect_async(req).await else {
        return;
    };
    loop {
        tokio::select! {
            message = gateway.next() => match message {
                Some(Ok(Message::Text(text))) => {
                    lock(&from_gateway).push(text.to_string());
                    if provider.send(Message::Text(text)).await.is_err() { return }
                }
                Some(Ok(Message::Close(_))) | Some(Err(_)) | None => {
                    let _ = provider.close(None).await;
                    return;
                }
                Some(Ok(_)) => {}
            },
            message = provider.next() => match message {
                Some(Ok(Message::Text(text))) => {
                    lock(&from_provider).push(text.to_string());
                    if gateway.send(Message::Text(text)).await.is_err() { return }
                }
                Some(Ok(Message::Close(frame))) => {
                    let _ = gateway.send(Message::Close(frame)).await;
                    return;
                }
                Some(Err(_)) | None => return,
                Some(Ok(_)) => {}
            },
        }
    }
}

pub use respan_gateway_testkit::{Calls, Outcomes};
use respan_gateway_ws::WsTransport;

/// A gateway that relays every session to the provider at `base` in format
/// `Raw`, with the recorders of how requests and calls end registered first.
pub fn relay(base: &str) -> (Gateway, Outcomes, Calls) {
    let (ends, calls) = (Outcomes::default(), Calls::default());
    let transport = WsTransport::new(base).unwrap();
    let gateway = Gateway::new()
        .serve(Raw)
        .unwrap()
        .provider("p", Raw, transport)
        .unwrap()
        .plugin("ends", ends.clone())
        .unwrap()
        .plugin("calls", calls.clone())
        .unwrap()
        .target("p")
        .unwrap();
    (gateway, ends, calls)
}

/// The code of an end: `ok` or the fault's code.
pub fn code(end: &Result<(), Fault>) -> &str {
    match end {
        Ok(()) => "ok",
        Err(fault) => fault.code(),
    }
}
