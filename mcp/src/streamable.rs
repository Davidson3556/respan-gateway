//! MCP over streamable HTTP as a [`Format`] of the MCP kind (MCP
//! 2025-06-18, Transports, "Streamable HTTP").
//!
//! The client's request decides the shape of its answer:
//!
//! - A `POST` that holds no request (only notifications or responses) is
//!   answered `202 Accepted` with no body ("the server MUST return HTTP
//!   status code 202 Accepted with no body").
//! - A `POST` with requests is answered by an event stream, on which the
//!   server may send its own requests and notifications before its answers,
//!   each as it comes; by one JSON body only if the client's `Accept` takes
//!   no event stream. It is complete once every request is answered.
//! - A `GET` opens the server's own event stream, which may stay quiet.
//! - A `DELETE` ends the session: `204 No Content`, with no body.
//!
//! A head a plugin chose ([`ClientHead`](respan_gateway_http::ClientHead))
//! gives the status and headers instead, but never the framing. What MCP
//! carries in headers is the kind's: [`ToServer`]'s session, protocol
//! version and last event id are read from the client's request and written
//! into the call, and a session the server names ([`ToClient::session`]) is
//! written into the head of the client's answer.

use std::pin::Pin;
use std::sync::{Arc, Mutex, OnceLock};
use std::task::{Context, Poll, ready};

use respan_gateway_http::bytes::{Bytes, BytesMut};
use respan_gateway_http::formats::{self as formats, sse, whole};
use respan_gateway_http::http::header::{ACCEPT, CONTENT_TYPE};
use respan_gateway_http::http::request::Parts as RequestHead;
use respan_gateway_http::http::response::Parts as ResponseHead;
use respan_gateway_http::http::{HeaderMap, HeaderValue, Method, StatusCode};
use respan_gateway_http::http::{Request, Response};
use respan_gateway_http::{self as h, Body, Incoming, Outgoing, bodiless, error_code};
use respan_gateway_sdk::futures::stream::BoxStream;
use respan_gateway_sdk::futures::{Sink, SinkExt, StreamExt, stream};
use respan_gateway_sdk::{
    BadRequest, Connection, EventSink, EventStream, Fault, Format, Truncated,
};
use serde_json::{Value, json};

use crate::{
    PARSE_ERROR, RpcCode, ToClient, ToServer, body, error, lock, messages, read, request_ids,
};

/// The header naming an MCP session (`Mcp-Session-Id`).
pub const SESSION: &str = "mcp-session-id";
/// The header naming the protocol version a session agreed on.
const PROTOCOL_VERSION: &str = "mcp-protocol-version";
/// The header naming the last event a resuming client got (SSE).
const LAST_EVENT_ID: &str = "last-event-id";

const NAME: &str = "mcp";
const JSON: &str = "application/json";
const SSE: &str = "text/event-stream";

/// MCP over streamable HTTP, served at `path` and sent to `path` relative
/// to each provider's transport.
#[derive(Debug, Clone)]
pub struct StreamableHttp {
    path: &'static str,
}

impl StreamableHttp {
    /// Served and sent at `path`.
    pub fn new(path: &'static str) -> Self {
        Self { path }
    }
}

/// At `/mcp`.
impl Default for StreamableHttp {
    fn default() -> Self {
        Self::new("/mcp")
    }
}

impl Format<Incoming, ToServer, ToClient> for StreamableHttp {
    fn name(&self) -> &str {
        NAME
    }

    fn matches(&self, io: &Incoming) -> bool {
        let head = io.request();
        [Method::POST, Method::GET, Method::DELETE].contains(head.method())
            && formats::clean_path(head.uri().path()) == formats::clean_path(self.path)
    }

    fn bind(&self, io: Incoming) -> Result<Connection<ToServer, ToClient>, Fault> {
        let started = Started::default();
        let refused = started.clone();
        Ok(io.bind(
            |request| Ok(h::messages(request, decode)),
            move |reply, source| Ok(answer(reply, source, &started)),
            move |fault, request, head| {
                formats::default_content_type(head, JSON);
                match refused.get() {
                    Some(answered) => lock(answered).refusal(fault),
                    None => Answered::of(request.body()).refusal(fault),
                }
                .into()
            },
        ))
    }
}

/// A client's request as the kind's input.
fn decode(head: &RequestHead, raw: Bytes) -> Result<ToServer, Fault> {
    let mut input = ToServer::new(head.method.clone(), Vec::new());
    if head.method == Method::POST {
        // JSON-RPC 2.0 (5.1): a body that is no JSON is a parse error.
        let json = serde_json::from_slice(&raw).map_err(|e| {
            let fault = BadRequest::fault("the body is not JSON");
            fault.with(PARSE_ERROR).with_source(e)
        })?;
        let read = messages(json).filter(|(m, _)| !m.is_empty());
        let bad = || BadRequest::fault("not a JSON-RPC message or batch");
        (input.messages, input.batch) = read.ok_or_else(bad)?;
    }
    input.session = header(head, SESSION)?;
    input.protocol_version = header(head, PROTOCOL_VERSION)?;
    input.last_event_id = header(head, LAST_EVENT_ID)?;
    Ok(input)
}

/// The value of the client's header `name`, if it sent one; `Err` with
/// `bad_request` if it is not text, as no MCP header may be.
fn header(head: &RequestHead, name: &str) -> Result<Option<String>, Fault> {
    let value = head.headers.get(name).map(HeaderValue::to_str).transpose();
    let value = value.map_err(|_| BadRequest::fault(format!("the {name} header is not text")))?;
    Ok(value.map(str::to_owned))
}

/// Set header `name` to `value`; `Err` with `internal` if it cannot be one,
/// as when a plugin edited it so.
fn put(headers: &mut HeaderMap, name: &'static str, value: &str) -> Result<(), Fault> {
    let value = HeaderValue::from_str(value).map_err(|e| {
        Fault::internal(format!("the {name} header cannot hold {value:?}")).with_source(e)
    })?;
    headers.insert(name, value);
    Ok(())
}

impl Format<Outgoing, ToClient, ToServer> for StreamableHttp {
    fn name(&self) -> &str {
        NAME
    }

    fn bind(&self, io: Outgoing) -> Result<Connection<ToClient, ToServer>, Fault> {
        let path = self.path;
        Ok(io.bind(
            move |source: &mut EventStream<ToServer>| {
                let Some(input) = source.value() else {
                    let streamed = "a call in mcp carries one body, not streamed inputs";
                    return Err(Fault::config(streamed));
                };
                let head = call_head(input?, path)?;
                let write = |input: &ToServer| {
                    Ok(match input.method {
                        Method::POST => body(&input.messages, input.batch).to_string().into(),
                        _ => Bytes::new(),
                    })
                };
                let (request, sink) = h::request(source, formats::each(write))?;
                Ok((Request::from_parts(head, request.into_body()), sink))
            },
            response_events,
            |answer, _| answer.status.is_success(),
        ))
    }
}

/// The head of a call of `input` to `path`.
fn call_head(input: &ToServer, path: &str) -> Result<RequestHead, Fault> {
    let mut head = Request::new(()).into_parts().0;
    head.method = input.method.clone();
    formats::set_path(&mut head, path)?;
    // MCP: a client lists both in its `Accept`.
    let both = HeaderValue::from_static("application/json, text/event-stream");
    head.headers.insert(ACCEPT, both);
    if input.method == Method::POST {
        head.headers
            .insert(CONTENT_TYPE, HeaderValue::from_static(JSON));
    }
    for (name, value) in [
        (SESSION, &input.session),
        (PROTOCOL_VERSION, &input.protocol_version),
        (LAST_EVENT_ID, &input.last_event_id),
    ] {
        if let Some(value) = value {
            put(&mut head.headers, name, value)?;
        }
    }
    Ok(head)
}

fn response_events(
    answer: Response<Body>,
    sent: Request<Bytes>,
) -> BoxStream<'static, Result<ToClient, Fault>> {
    if answer.status().is_success() && bodiless(sent.method(), answer.status()) {
        return stream::empty().boxed();
    }
    let (head, body) = answer.into_parts();
    let session = head.headers.get(SESSION).and_then(|v| v.to_str().ok());
    let session = session.map(str::to_owned);
    let output = move |read: Option<(Vec<Value>, bool)>| ToClient {
        messages: read.map(|(messages, _)| messages).unwrap_or_default(),
        session: session.clone(),
        ..ToClient::default()
    };
    let inner = if sse::is_stream(&head) {
        let on_event = move |_: &mut (), raw: Bytes, out: &mut Vec<ToClient>| {
            let data = sse::event_data(&raw);
            let mut output = output(data.and_then(|d| read(d.as_bytes())));
            output.event_id = field(&raw, "id");
            output.event = field(&raw, "event");
            out.push(output);
            Ok(())
        };
        sse::read(body, (), on_event, |_, end, _| end)
    } else {
        let on_body = move |raw: Bytes, out: &mut Vec<ToClient>| {
            if !raw.is_empty() {
                out.push(output(read(&raw)));
            }
            Ok(())
        };
        whole::read(NAME, Response::from_parts(head, body), on_body)
    };
    let posted = read(sent.body()).unwrap_or_default().0;
    let pending: Vec<Value> = request_ids(&posted).cloned().collect();
    stream::unfold(
        (inner, pending, false),
        |(mut inner, mut pending, done)| async move {
            if done {
                return None;
            }
            match inner.next().await {
                Some(Ok(output)) => {
                    forget(&mut pending, &output);
                    Some((Ok(output), (inner, pending, false)))
                }
                Some(Err(fault)) => Some((Err(fault), (inner, pending, true))),
                None if !pending.is_empty() => {
                    Some((Err(Truncated::fault()), (inner, pending, true)))
                }
                None => None,
            }
        },
    )
    .boxed()
}

/// Drops from `pending` the requests `output` answers.
fn forget(pending: &mut Vec<Value>, output: &ToClient) {
    for id in output.answers() {
        pending.retain(|p| p != id);
    }
}

/// What a request has been answered so far, once its answer started:
/// shared by its writer and its refusal, so a refusal written midway keeps
/// the answers it has.
type Started = Arc<OnceLock<Arc<Mutex<Answered>>>>;

#[derive(Debug)]
struct Answered {
    /// The ids of its requests not answered yet.
    pending: Vec<Value>,
    /// Whether it came as a batch.
    batch: bool,
    /// The messages of an answer written as one JSON body, kept until it
    /// ends.
    messages: Vec<Value>,
}

impl Answered {
    /// A request of `body`, not answered yet.
    fn of(body: &[u8]) -> Self {
        let (messages, batch) = read(body).unwrap_or_default();
        Self {
            pending: request_ids(&messages).cloned().collect(),
            batch,
            messages: Vec::new(),
        }
    }

    /// The body of a refusal of `fault`: the messages so far, then an error
    /// to each request left, or to `null` if none is (JSON-RPC 2.0, 5).
    fn refusal(&self, fault: &Fault) -> String {
        let ids = match self.pending.is_empty() {
            true => &[Value::Null][..],
            false => &self.pending,
        };
        let mut all = self.messages.clone();
        all.extend(ids.iter().map(|id| failure(id, fault)));
        body(&all, self.batch).to_string()
    }
}

/// The client's answer, shaped by its request.
fn answer(
    reply: h::Reply,
    source: &mut EventStream<ToClient>,
    started: &Started,
) -> EventSink<ToClient> {
    let ask = reply.request();
    let answered = Arc::new(Mutex::new(Answered::of(ask.body())));
    let asks = !lock(&answered).pending.is_empty();
    let head = |status| {
        let mut head = Response::new(()).into_parts().0;
        head.status = status;
        reply.head_or(head)
    };
    let (status, framing) = match *ask.method() {
        Method::DELETE => (StatusCode::NO_CONTENT, None),
        Method::POST if !asks => (StatusCode::ACCEPTED, None),
        Method::POST if !takes_events(ask.headers()) => (StatusCode::OK, Some(JSON)),
        _ => (StatusCode::OK, Some(SSE)),
    };
    let mut head = head(status);
    let Some(framing) = framing else {
        head.headers.remove(CONTENT_TYPE);
        return reply.empty(head, source);
    };
    head.headers
        .insert(CONTENT_TYPE, HeaderValue::from_static(framing));
    let _ = started.set(answered.clone());
    let encoder = Box::new(Answer {
        answered,
        events: framing == SSE,
    });
    if *ask.method() == Method::GET {
        return reply.open(head, sse::comment("stream").into(), encoder);
    }
    EventSink::new(Headed {
        start: Some((reply, head, encoder)),
        sink: None,
        first: None,
    })
}

/// Whether the client takes an event stream: it sent no `Accept`, or the
/// most specific of its media ranges that matches `text/event-stream` has
/// no `q=0` (RFC 9110 12.5.1, 12.4.2).
fn takes_events(headers: &HeaderMap) -> bool {
    let values = headers.get_all(ACCEPT).iter();
    let ranges: Vec<&str> = values
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .collect();
    if ranges.is_empty() {
        return true;
    }
    let matching = ranges.iter().filter_map(|range| {
        let mut params = range.split(';');
        let specific = match params.next()?.trim().to_ascii_lowercase().as_str() {
            SSE => 2,
            "text/*" => 1,
            "*/*" => 0,
            _ => return None,
        };
        let mut weights = params.filter_map(|p| p.split_once('='));
        let refused = weights.any(|(name, q)| {
            name.trim().eq_ignore_ascii_case("q") && q.trim().parse::<f32>().is_ok_and(|q| q <= 0.0)
        });
        Some((specific, !refused))
    });
    matching
        .max_by_key(|&(specific, _)| specific)
        .is_some_and(|(_, taken)| taken)
}

/// Encodes an answer: each output as an event, or, not `events`, all of
/// them as one JSON body at its end. A failure is written as an error event
/// to each request left unanswered; a JSON body's head is not out before
/// its end, so its refusal ([`Answered::refusal`]) writes the failure.
struct Answer {
    answered: Arc<Mutex<Answered>>,
    events: bool,
}

impl h::Encoder<ToClient> for Answer {
    fn encode(&mut self, output: &ToClient, out: &mut BytesMut) -> Result<(), Fault> {
        let mut answered = lock(&self.answered);
        forget(&mut answered.pending, output);
        match self.events {
            true => out.extend_from_slice(&event(output)?),
            false => answered.messages.extend_from_slice(&output.messages),
        }
        Ok(())
    }

    fn end(&mut self, end: &Result<(), Fault>, out: &mut BytesMut) -> Result<(), Fault> {
        let answered = lock(&self.answered);
        match (end, self.events) {
            (Ok(()), false) if !answered.messages.is_empty() => {
                let json = body(&answered.messages, answered.batch).to_string();
                out.extend_from_slice(json.as_bytes());
            }
            (Err(fault), true) => {
                for id in &answered.pending {
                    out.extend_from_slice(sse::event(None, failure(id, fault)).as_bytes());
                }
            }
            _ => {}
        }
        Ok(())
    }
}

/// One step of a sink's work, such as `poll_flush`.
type SinkStep = fn(&mut EventSink<ToClient>, &mut Context<'_>) -> Poll<Result<(), Fault>>;

/// A `POST`'s answer, its head held until the first output, whose session
/// the head names: a server assigns a session in the head of its answer to
/// `initialize` (MCP, "Session Management").
struct Headed {
    start: Option<(h::Reply, ResponseHead, Box<dyn h::Encoder<ToClient>>)>,
    sink: Option<EventSink<ToClient>>,
    /// The first output, sent once `sink` is ready.
    first: Option<Result<ToClient, Fault>>,
}

impl Headed {
    /// Starts the answer, its head naming `session` if any.
    fn begin(&mut self, session: Option<&str>) -> Result<(), Fault> {
        let Some((reply, mut head, encoder)) = self.start.take() else {
            return Ok(());
        };
        let named = session.map_or(Ok(()), |s| put(&mut head.headers, SESSION, s));
        self.sink = Some(reply.write(head, encoder));
        named
    }

    /// `op` on the sink, once the first output is sent to it; nothing to
    /// do before the answer started.
    fn drive(&mut self, cx: &mut Context<'_>, op: SinkStep) -> Poll<Result<(), Fault>> {
        let Some(sink) = &mut self.sink else {
            return Poll::Ready(Ok(()));
        };
        if self.first.is_some() {
            ready!(sink.poll_ready_unpin(cx))?;
            if let Some(first) = self.first.take() {
                sink.start_send_unpin(first)?;
            }
        }
        op(sink, cx)
    }
}

impl Sink<Result<ToClient, Fault>> for Headed {
    type Error = Fault;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Fault>> {
        self.get_mut()
            .drive(cx, |sink, cx| sink.poll_ready_unpin(cx))
    }

    fn start_send(self: Pin<&mut Self>, item: Result<ToClient, Fault>) -> Result<(), Fault> {
        let this = self.get_mut();
        match &mut this.sink {
            Some(sink) => sink.start_send_unpin(item),
            None => {
                let session = item.as_ref().ok().and_then(|o| o.session.as_deref());
                let named = this.begin(session);
                this.first = Some(named.and(item));
                Ok(())
            }
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Fault>> {
        self.get_mut()
            .drive(cx, |sink, cx| sink.poll_flush_unpin(cx))
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Fault>> {
        let this = self.get_mut();
        this.begin(None)?;
        this.drive(cx, |sink, cx| sink.poll_close_unpin(cx))
    }
}

/// An answer dropped before it started is abandoned, as one dropped later
/// is.
impl Drop for Headed {
    fn drop(&mut self) {
        let _ = self.begin(None);
    }
}

/// The value of the last `name:` line of an event, if any.
fn field(raw: &[u8], name: &str) -> Option<String> {
    let text = String::from_utf8_lossy(raw);
    let lines = text.split(['\n', '\r']);
    let mut values = lines.filter_map(|l| l.strip_prefix(name)?.strip_prefix(':'));
    values
        .next_back()
        .map(|v| v.strip_prefix(' ').unwrap_or(v).to_owned())
}

/// An output as an event: its messages as `data:`, with its `id:` and
/// `event:` if it has them. An event with an id and no messages stays, as the
/// event that primes a client's `Last-Event-ID`. An id or name that would
/// break its line (a plugin's edit) is refused rather than written.
fn event(output: &ToClient) -> Result<Bytes, Fault> {
    if output.messages.is_empty() && output.event_id.is_none() {
        return Ok(Bytes::new());
    }
    let mut out = String::new();
    for (name, value) in [("id", &output.event_id), ("event", &output.event)] {
        if let Some(value) = value {
            if value.contains(['\r', '\n', '\0']) {
                return Err(Fault::internal(format!(
                    "an SSE {name} cannot hold a line break or NUL"
                )));
            }
            out.push_str(&format!("{name}: {value}\n"));
        }
    }
    let data = match output.messages.is_empty() {
        true => String::new(),
        false => body(&output.messages, false).to_string(),
    };
    out.push_str(&sse::event(None, data));
    Ok(out.into())
}

/// A fault as a JSON-RPC error to `id`: its [`RpcCode`], else -32600 for
/// a `BadRequest` and -32603 for any other (JSON-RPC 2.0, 5.1), its
/// [`error_code`] as `data.code`.
fn failure(id: &Value, fault: &Fault) -> Value {
    let code = match fault.get::<RpcCode>() {
        Some(RpcCode(code)) => *code,
        None if fault.is::<BadRequest>() => -32600,
        None => -32603,
    };
    let mut e = error(id.clone(), code, fault.message());
    e["error"]["data"] = json!({ "code": error_code(fault) });
    e
}
