//! Strict fakes of the realtime providers, each a WebSocket server a test
//! drives: [`openai::start`] (OpenAI Realtime, GA) and [`gemini::start`]
//! (Gemini Live). Each answers as its API does, rejects what its API
//! rejects, and records every frame it read and wrote and every handshake.
//!
//! A test scripts what the model says with [`Fake::answer`], one [`Turn`] a
//! response, and what the user does with [`Fake::user_speaks`],
//! [`Fake::user_stops`] (the provider's voice detection) and
//! [`Fake::go_away`]. Frames reach a fake asynchronously: a test waits for
//! them with [`Fake::received`] before it acts on them.

pub mod gemini;
pub mod openai;

use std::collections::VecDeque;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use respan_gateway_core::futures::{SinkExt, StreamExt};
use serde_json::Value;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Notify, mpsc};
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::handshake::server::{ErrorResponse, Request, Response};
use tokio_tungstenite::tungstenite::http::StatusCode;
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::{Bytes, Message, Utf8Bytes};

/// What the model says in one response, in the session's formats.
#[derive(Debug, Clone, Default)]
pub struct Turn {
    /// What the user said before it, as the provider transcribes it, if
    /// the session asked for transcripts.
    pub heard: Option<String>,
    /// The model's audio, as the session's output format encodes it.
    pub audio: Option<Vec<u8>>,
    /// The transcript of its audio, sent if the session asked for it.
    pub transcript: Option<String>,
    /// Its text, in a session that answers in text.
    pub text: Option<String>,
    /// A tool it calls, after what it says.
    pub tool_call: Option<ToolCall>,
    /// What the response used.
    pub usage: Usage,
    /// Whether the response stays open after its content, until the user
    /// barges in or the client cancels it.
    pub held: bool,
}

/// A call the model makes.
#[derive(Debug, Clone)]
pub struct ToolCall {
    /// The tool's name.
    pub name: String,
    /// Its arguments, a JSON object.
    pub arguments: Value,
}

/// Token counts of a response.
#[derive(Debug, Clone, Copy, Default)]
pub struct Usage {
    pub input_tokens: u64,
    pub output_tokens: u64,
    /// Of the input, those of audio.
    pub input_audio_tokens: u64,
    /// Of the output, those of audio.
    pub output_audio_tokens: u64,
    /// Of the input, those read from a cache.
    pub cached_tokens: u64,
}

/// A WebSocket upgrade a fake read.
#[derive(Debug, Clone)]
pub struct Handshake {
    /// Its path and query.
    pub uri: String,
    /// Its headers, names lowercase.
    pub headers: Vec<(String, String)>,
}

impl Handshake {
    /// The value of header `name`, if it came.
    pub fn header(&self, name: &str) -> Option<&str> {
        let found = self.headers.iter().find(|(n, _)| n == name);
        found.map(|(_, v)| v.as_str())
    }

    /// The value of query parameter `name`, if it came.
    pub fn query(&self, name: &str) -> Option<&str> {
        let query = self.uri.split_once('?').map_or("", |(_, q)| q);
        let pairs = query.split('&').filter_map(|pair| pair.split_once('='));
        pairs.into_iter().find(|(n, _)| *n == name).map(|(_, v)| v)
    }
}

/// What the test tells a session.
#[derive(Debug, Clone, Copy)]
enum Command {
    Speaks,
    Stops,
    GoAway,
}

/// What a fake and its sessions share.
#[derive(Default)]
struct Shared {
    turns: Mutex<VecDeque<Turn>>,
    received: Mutex<Vec<Value>>,
    sent: Mutex<Vec<Value>>,
    handshakes: Mutex<Vec<Handshake>>,
    changed: Notify,
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// A running fake: its sessions are served one after another, and the
/// test's commands go to the one open.
pub struct Fake {
    base_url: String,
    shared: Arc<Shared>,
    commands: mpsc::UnboundedSender<Command>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Fake {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Fake {
    /// Its base URL, as its vendor's SDKs take it.
    pub fn base_url(&self) -> String {
        self.base_url.clone()
    }

    /// The next response says `turn`; one with none queued says nothing.
    pub fn answer(&self, turn: Turn) {
        lock(&self.shared.turns).push_back(turn);
    }

    /// The provider hears the user begin to speak.
    pub fn user_speaks(&self) {
        let _ = self.commands.send(Command::Speaks);
    }

    /// The provider hears the user stop: the end of the user's turn.
    pub fn user_stops(&self) {
        let _ = self.commands.send(Command::Stops);
    }

    /// The provider is about to end the session.
    pub fn go_away(&self) {
        let _ = self.commands.send(Command::GoAway);
    }

    /// Every frame it read, once it has read at least `n`; a frame that is
    /// no JSON is its text as a string.
    pub async fn received(&self, n: usize) -> Vec<Value> {
        let wait = async {
            loop {
                let changed = self.shared.changed.notified();
                let frames = lock(&self.shared.received).clone();
                if frames.len() >= n {
                    return frames;
                }
                changed.await;
            }
        };
        match tokio::time::timeout(Duration::from_secs(5), wait).await {
            Ok(frames) => frames,
            Err(_) => panic!(
                "the fake read {:?}, not {n} frames",
                lock(&self.shared.received)
            ),
        }
    }

    /// Every frame it wrote.
    pub fn sent(&self) -> Vec<Value> {
        lock(&self.shared.sent).clone()
    }

    /// Every upgrade it read, refused ones too.
    pub fn handshakes(&self) -> Vec<Handshake> {
        lock(&self.shared.handshakes).clone()
    }
}

/// A session of a fake: what it does with each frame and command, until
/// it ends; it gives its connection back.
type Run = fn(Connection) -> Pin<Box<dyn Future<Output = Connection> + Send>>;

/// A fake's handshake check: `Ok` to upgrade, with the `101` edited as the
/// API answers, or the refusal's status and body.
type Check = fn(&Request, &mut Response) -> Result<(), (StatusCode, &'static str)>;

/// Serve a fake at `prefix` (its base URL's path), checking each upgrade
/// with `check` and running each session with `run`, one after another.
async fn serve(prefix: &str, check: Check, run: Run) -> Fake {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("listen");
    let addr: SocketAddr = listener.local_addr().expect("address");
    let shared = Arc::new(Shared::default());
    let (commands, mut queued) = mpsc::unbounded_channel();
    let state = shared.clone();
    let task = tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let recorded = state.clone();
            // Its refusal is tungstenite's, a whole response.
            #[allow(clippy::result_large_err)]
            let upgrade = move |request: &Request, mut response: Response| {
                let headers = request.headers().iter().map(|(n, v)| {
                    let v = String::from_utf8_lossy(v.as_bytes()).into_owned();
                    (n.as_str().to_owned(), v)
                });
                lock(&recorded.handshakes).push(Handshake {
                    uri: request.uri().to_string(),
                    headers: headers.collect(),
                });
                match check(request, &mut response) {
                    Ok(()) => Ok(response),
                    Err((status, body)) => {
                        let mut refusal = ErrorResponse::new(Some(body.to_owned()));
                        *refusal.status_mut() = status;
                        Err(refusal)
                    }
                }
            };
            let Ok(ws) = tokio_tungstenite::accept_hdr_async(stream, upgrade).await else {
                continue;
            };
            let connection = Connection {
                ws,
                shared: state.clone(),
                commands: queued,
                ids: 0,
            };
            queued = run(connection).await.commands;
        }
    });
    Fake {
        base_url: format!("ws://{addr}{prefix}"),
        shared,
        commands,
        task,
    }
}

/// What a session reads next.
enum Event {
    /// A frame's JSON.
    Frame(Value),
    /// The test's command.
    Command(Command),
    /// The client closed, or the connection broke.
    Closed,
}

/// A session's socket, with what it shares with its fake.
struct Connection {
    ws: WebSocketStream<TcpStream>,
    shared: Arc<Shared>,
    commands: mpsc::UnboundedReceiver<Command>,
    /// The last id it made up.
    ids: u64,
}

impl Connection {
    /// The next frame or command, each frame recorded; a frame that is no
    /// JSON is its text as a string.
    async fn next(&mut self) -> Event {
        let frame = tokio::select! {
            frame = self.ws.next() => frame,
            Some(command) = self.commands.recv() => return Event::Command(command),
        };
        let raw = match frame {
            Some(Ok(Message::Text(text))) => Bytes::from(text),
            Some(Ok(Message::Binary(bytes))) => bytes,
            Some(Ok(Message::Ping(_) | Message::Pong(_) | Message::Frame(_))) => {
                return Box::pin(self.next()).await;
            }
            Some(Ok(Message::Close(_)) | Err(_)) | None => return Event::Closed,
        };
        let value = serde_json::from_slice(&raw)
            .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&raw).into_owned()));
        lock(&self.shared.received).push(value.clone());
        self.shared.changed.notify_waiters();
        Event::Frame(value)
    }

    /// Write `value`, in a binary frame if `binary`.
    async fn send(&mut self, value: Value, binary: bool) {
        let text = value.to_string();
        lock(&self.shared.sent).push(value);
        let message = match binary {
            true => Message::Binary(Bytes::from(text)),
            false => Message::Text(Utf8Bytes::from(text)),
        };
        let _ = self.ws.send(message).await;
    }

    /// Close with `code` and `reason`, and read until the client closes.
    async fn close(&mut self, code: u16, reason: &str) {
        let frame = CloseFrame {
            code: CloseCode::from(code),
            reason: Utf8Bytes::from(reason.to_owned()),
        };
        let _ = self.ws.send(Message::Close(Some(frame))).await;
        while let Some(Ok(_)) = self.ws.next().await {}
    }

    /// The upgrade that opened it.
    fn handshake(&self) -> Handshake {
        let handshakes = lock(&self.shared.handshakes);
        handshakes.last().cloned().expect("a session's handshake")
    }

    /// A new id, `prefix` and a number.
    fn id(&mut self, prefix: &str) -> String {
        self.ids += 1;
        format!("{prefix}_{}", self.ids)
    }

    /// The turn the next response says.
    fn turn(&self) -> Turn {
        lock(&self.shared.turns).pop_front().unwrap_or_default()
    }

    /// What the user said, as the next response's turn has it.
    fn heard(&self) -> Option<String> {
        lock(&self.shared.turns)
            .front()
            .and_then(|t| t.heard.clone())
    }
}

/// `data` as standard base64.
fn base64(data: &[u8]) -> String {
    STANDARD.encode(data)
}

/// The bytes of standard base64 `text`, if it is.
fn unbase64(text: &str) -> Option<Vec<u8>> {
    STANDARD.decode(text).ok()
}

/// `text` in parts, a word each, its spaces kept: as a model streams it.
fn words(text: &str) -> Vec<String> {
    text.split_inclusive(' ').map(str::to_owned).collect()
}
