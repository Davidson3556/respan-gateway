//! MCP over streamable HTTP as a [`Format`] of the MCP kind (MCP
//! 2025-06-18, Transports, "Streamable HTTP").
//!
//! The client's request decides the shape of its answer:
//!
//! - A `POST` that holds no request (only notifications or responses) is
//!   answered `202 Accepted` with no body ("the server MUST return HTTP
//!   status code 202 Accepted with no body") once the server accepted it.
//! - A `POST` with requests is answered by an event stream, on which the
//!   server may send its own requests and notifications before its answers,
//!   each as it comes; by one JSON body only if the client's `Accept` takes
//!   no event stream. It is complete once every request is answered.
//! - A `GET` opens the server's own event stream, which may stay quiet.
//! - A `DELETE` ends the session: `204 No Content`, with no body, once the
//!   server ended it.
//!
//! A head a plugin chose ([`ClientHead`](respan_gateway_http::ClientHead))
//! gives the status and headers instead, but never the framing. What MCP
//! carries in headers is the kind's: [`Input`]'s session, protocol
//! version and last event id are read from the client's request and written
//! into the call, and a session the server names ([`Output::session`]) is
//! written into the head of the client's answer.

use std::pin::Pin;
use std::sync::{Arc, Mutex, OnceLock};
use std::task::{Context, Poll, ready};

use respan_gateway_core::futures::{Sink, SinkExt, StreamExt, stream};
use respan_gateway_core::{
    BadRequest, Connection, Details, EventSink, EventStream, Fault, Format, RetryAfter, Truncated,
    Upstream,
};
use respan_gateway_http::bytes::{Bytes, BytesMut};
use respan_gateway_http::framing::{self, sse, whole};
use respan_gateway_http::http::header::{ACCEPT, CONTENT_TYPE};
use respan_gateway_http::http::request::Parts as RequestHead;
use respan_gateway_http::http::response::Parts as ResponseHead;
use respan_gateway_http::http::{HeaderMap, HeaderValue, Method, StatusCode};
use respan_gateway_http::http::{Request, Response};
use respan_gateway_http::{
    self as h, Body, ClientIo, ProviderError, ProviderIo, Status, bodiless, error_code,
};
use serde_json::{Value, json};

use crate::{PARSE_ERROR, body, lock, messages, read};
use respan_gateway_mcp::{Input, Output, RpcCode, error};
use respan_gateway_mcp::{Message, Outcome};

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
    path: Arc<str>,
    /// If set, served at `path/{name}` for each of these names instead.
    servers: Option<Arc<[String]>>,
}

impl StreamableHttp {
    /// Served and sent at `path`.
    pub fn new(path: impl AsRef<str>) -> Self {
        Self {
            path: path.as_ref().into(),
            servers: None,
        }
    }

    /// Served to clients at `{path}/{name}` for each of `names`, and at no
    /// other path, so one gateway can front several servers (the caller
    /// routes by name). It is still sent to a provider at `path`, so give
    /// a provider's format its own.
    pub fn serving(mut self, names: impl IntoIterator<Item: Into<String>>) -> Self {
        self.servers = Some(names.into_iter().map(Into::into).collect());
        self
    }

    /// Whether it is served at `path`, as cleaned.
    fn serves(&self, path: &str) -> bool {
        let base = framing::clean_path(&self.path);
        match &self.servers {
            None => path == base,
            Some(names) => path
                .strip_prefix(base.trim_end_matches('/'))
                .and_then(|rest| rest.strip_prefix('/'))
                .is_some_and(|name| names.iter().any(|n| n == name)),
        }
    }
}

/// At `/mcp`.
impl Default for StreamableHttp {
    fn default() -> Self {
        Self::new("/mcp")
    }
}

impl Format<ClientIo, Input, Output> for StreamableHttp {
    fn name(&self) -> &str {
        NAME
    }

    fn matches(&self, io: &ClientIo) -> bool {
        let head = io.request();
        [Method::POST, Method::GET, Method::DELETE].contains(head.method())
            && self.serves(&framing::clean_path(head.uri().path()))
    }

    fn bind(&self, io: ClientIo) -> Result<Connection<Input, Output>, Fault> {
        let started = Started::default();
        let refused = started.clone();
        Ok(io.bind(
            |request| h::messages(request, decode),
            move |reply| Ok(answer(reply, &started)),
            move |fault, request, head| {
                framing::default_content_type(head, JSON);
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
fn decode(head: &RequestHead, raw: Bytes) -> Result<Input, Fault> {
    let mut input = Input::new(head.method.clone(), Vec::new());
    if head.method == Method::POST {
        // JSON-RPC 2.0 (5.1): a body that is no JSON is a parse error.
        let json = serde_json::from_slice(&raw).map_err(|e| {
            let fault = BadRequest::fault("the body is not JSON");
            fault.with(PARSE_ERROR).with_source(e)
        })?;
        let bad = || BadRequest::fault("not a JSON-RPC message or batch");
        (input.messages, input.batch) = messages(json).ok_or_else(bad)?;
    }
    input.session = header(&head.headers, SESSION)?;
    input.protocol_version = header(&head.headers, PROTOCOL_VERSION)?;
    input.last_event_id = header(&head.headers, LAST_EVENT_ID)?;
    input.query = head.uri.query().map(str::to_owned);
    Ok(input)
}

/// One scalar MCP header. A resume cursor is UTF-8; session IDs are
/// nonempty visible ASCII, as required by MCP's session management.
fn header(headers: &HeaderMap, name: &str) -> Result<Option<String>, Fault> {
    let mut values = headers.get_all(name).iter();
    let Some(value) = values.next() else {
        return Ok(None);
    };
    if values.next().is_some() {
        return Err(BadRequest::fault(format!("the {name} header is repeated")));
    }
    let value = match name {
        LAST_EVENT_ID => std::str::from_utf8(value.as_bytes()).ok(),
        _ => value.to_str().ok(),
    }
    .ok_or_else(|| BadRequest::fault(format!("the {name} header is not text")))?;
    if name == SESSION && !valid_session(value) {
        return Err(BadRequest::fault("the MCP session ID is not visible ASCII"));
    }
    Ok(Some(value.to_owned()))
}

fn valid_session(value: &str) -> bool {
    !value.is_empty() && value.bytes().all(|b| (0x21..=0x7e).contains(&b))
}

/// Set header `name` to `value`; `Err` with `internal` if it cannot be one,
/// as when a plugin edited it so.
fn put(headers: &mut HeaderMap, name: &'static str, value: &str) -> Result<(), Fault> {
    if name == SESSION && !valid_session(value) {
        return Err(Fault::internal("the MCP session ID is not visible ASCII"));
    }
    let value = HeaderValue::from_str(value).map_err(|e| {
        Fault::internal(format!("the {name} header cannot hold {value:?}")).with_source(e)
    })?;
    headers.insert(name, value);
    Ok(())
}

impl Format<ProviderIo, Output, Input> for StreamableHttp {
    fn name(&self) -> &str {
        NAME
    }

    fn bind(&self, io: ProviderIo) -> Result<Connection<Output, Input>, Fault> {
        let path = self.path.clone();
        Ok(io.bind(
            move |head, input: Option<&Input>| {
                let streamed = "a call in mcp carries one body, not streamed inputs";
                call_head(head, input.ok_or_else(|| Fault::config(streamed))?, &path)?;
                Ok(framing::each(|input: &Input| match input.method {
                    Method::POST if input.messages.is_empty() => Err(Fault::config(
                        "a POST to an MCP server carries at least one message",
                    )),
                    Method::POST => {
                        Ok(body(input.messages.clone(), input.batch).to_string().into())
                    }
                    _ => Ok(Bytes::new()),
                }))
            },
            response_events,
            |answer| rpc_refusal(h::provider_fault(NAME, answer)),
        ))
    }
}

/// Write the head of a call of `input` to `path`.
fn call_head(head: &mut RequestHead, input: &Input, path: &str) -> Result<(), Fault> {
    head.method = input.method.clone();
    match &input.query {
        Some(query) => framing::set_path(head, &format!("{path}?{query}"))?,
        None => framing::set_path(head, path)?,
    }
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
    Ok(())
}

/// The outputs of a server's answer: its JSON-RPC messages, whole or as
/// events, each request of a `POST` answered once, or `truncated`.
fn response_events(answer: Response<Body>, sent: Request<Bytes>) -> EventStream<Output> {
    let posted = read(sent.body()).unwrap_or_default().0;
    let pending: Vec<Value> = posted.iter().filter_map(Message::request_id).collect();
    // GET may resume replies from previous POSTs; only a POST's replies
    // are correlated against the requests actually sent in this call.
    let correlate = sent.method() == Method::POST && !pending.is_empty();
    if answer.status().is_success()
        && (bodiless(sent.method(), answer.status()) || answer.status() == StatusCode::ACCEPTED)
    {
        if !pending.is_empty() {
            return EventStream::stream(stream::iter([Err(Truncated::fault())]));
        }
        return EventStream::empty();
    }
    let (head, body) = answer.into_parts();
    let Ok(session) = header(&head.headers, SESSION) else {
        let invalid = "the MCP server sent an invalid session header";
        return EventStream::stream(stream::iter([Err(Upstream::fault(invalid))]));
    };
    let output = move |read: Option<(Vec<Message>, bool)>| Output {
        messages: read.map(|(messages, _)| messages).unwrap_or_default(),
        session: session.clone(),
        ..Output::default()
    };
    let inner = if sse::is_stream(&head) {
        let on_event = move |_: &mut (), raw: Bytes, out: &mut Vec<Output>| {
            let data = sse::event_data(&raw);
            let messages = match data.filter(|d| !d.is_empty()) {
                Some(data) => Some(read(data.as_bytes()).ok_or_else(|| {
                    Upstream::fault("the MCP server sent an invalid JSON-RPC message")
                })?),
                None => None,
            };
            let mut output = output(messages);
            output.event_id = field(&raw, "id");
            output.event = field(&raw, "event");
            out.push(output);
            Ok(())
        };
        sse::read(body, (), on_event, |_, end, _| end).boxed()
    } else {
        let on_body = move |raw: Bytes, out: &mut Vec<Output>| {
            if !raw.is_empty() {
                let messages = read(&raw).ok_or_else(|| {
                    Upstream::fault("the MCP server sent an invalid JSON-RPC message")
                })?;
                out.push(output(Some(messages)));
            }
            Ok(())
        };
        whole::read(body, on_body).boxed()
    };
    EventStream::stream(stream::unfold(
        (inner, pending, false),
        move |(mut inner, mut pending, done)| async move {
            if done {
                return None;
            }
            match inner.next().await {
                Some(Ok(output)) => {
                    if correlate {
                        for id in output.answers() {
                            let Some(at) = pending.iter().position(|p| *p == id) else {
                                let fault = Upstream::fault(
                                    "the MCP server replied to an ID that is not pending",
                                );
                                return Some((Err(fault), (inner, pending, true)));
                            };
                            pending.remove(at);
                        }
                    }
                    let done = correlate && pending.is_empty();
                    Some((Ok(output), (inner, pending, done)))
                }
                Some(Err(fault)) => Some((Err(fault), (inner, pending, true))),
                None if !pending.is_empty() => {
                    Some((Err(Truncated::fault()), (inner, pending, true)))
                }
                None => None,
            }
        },
    ))
}

/// `fault`, a refusal whose body says a JSON-RPC error (a single one or the
/// first of a batch), as that error: its [`RpcCode`] and message, its `data`
/// as [`Details`]`{"data": ..}`; the status, `Retry-After` and the answer
/// stay. Any other fault, or a body that says none, as it is. The body is
/// the one the binding collected, so it is within the body's limit.
fn rpc_refusal(fault: Fault) -> Fault {
    let Some(error) = fault.get::<ProviderError>().cloned() else {
        return fault;
    };
    let all = match serde_json::from_slice(error.response.body()) {
        Ok(Value::Array(all)) => all,
        Ok(one) => vec![one],
        Err(_) => Vec::new(),
    };
    let said = all.into_iter().find_map(|mut m| {
        // MCP permits an ID-less error in an HTTP refusal. Normalize
        // only here; an ordinary stream message still needs its ID.
        m.as_object_mut()?.entry("id").or_insert(Value::Null);
        match Message::try_from(m).ok()? {
            Message::Response {
                outcome: Outcome::Error(said),
                ..
            } => Some(said),
            _ => None,
        }
    });
    let (Some(said), Some(Status(status))) = (said, fault.get::<Status>().copied()) else {
        return fault;
    };
    let mut read = Upstream::fault(said.message)
        .with(Status(status))
        .with(RpcCode(said.code));
    if let Some(data) = said.data {
        read = read.with(Details(serde_json::Map::from_iter([("data".into(), data)])));
    }
    if let Some(&wait) = fault.get::<RetryAfter>() {
        read = read.with(wait);
    }
    read.with(error)
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
            pending: messages.iter().filter_map(Message::request_id).collect(),
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
        body(all, self.batch).to_string()
    }
}

/// The client's answer, shaped by its request.
fn answer(reply: h::Reply, started: &Started) -> EventSink<Output> {
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
        // A 202 is held until the server's answer ends, so a server's
        // failure reaches the client as one; a 204 has no body to wait for.
        head.headers.remove(CONTENT_TYPE);
        return reply.write(head, Box::new(Bodiless));
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

impl h::Encoder<Output> for Answer {
    fn encode(&mut self, output: &Output, out: &mut BytesMut) -> Result<(), Fault> {
        let mut answered = lock(&self.answered);
        if !self.events && output.messages.iter().any(|m| m.request_id().is_some()) {
            return Err(Upstream::fault(
                "a server request requires an MCP event stream",
            ));
        }
        for id in output.answers() {
            answered.pending.retain(|p| *p != id);
        }
        match self.events {
            true => out.extend_from_slice(&event(output)?),
            // A JSON response contains the posted requests' responses, not
            // notifications mixed into an invented response batch. SSE is
            // needed to receive the server's independent messages.
            false => {
                let responses = output.messages.iter().filter(|m| m.method().is_none());
                answered
                    .messages
                    .extend(responses.cloned().map(Value::from));
            }
        }
        Ok(())
    }

    fn end(&mut self, end: &Result<(), Fault>, out: &mut BytesMut) -> Result<(), Fault> {
        let answered = lock(&self.answered);
        match (end, self.events) {
            (Ok(()), false) if !answered.messages.is_empty() => {
                let json = body(answered.messages.clone(), answered.batch).to_string();
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

/// Encodes the answer to a `POST` of no request or to a `DELETE`: no body.
struct Bodiless;

impl h::Encoder<Output> for Bodiless {
    fn encode(&mut self, _: &Output, _: &mut BytesMut) -> Result<(), Fault> {
        Ok(())
    }
}

/// One step of a sink's work, such as `poll_flush`.
type SinkStep = fn(&mut EventSink<Output>, &mut Context<'_>) -> Poll<Result<(), Fault>>;

/// A `POST`'s answer, its head held until the first output, whose session
/// the head names: a server assigns a session in the head of its answer to
/// `initialize` (MCP, "Session Management").
struct Headed {
    start: Option<(h::Reply, ResponseHead, Box<dyn h::Encoder<Output>>)>,
    sink: Option<EventSink<Output>>,
    /// The first output, sent once `sink` is ready.
    first: Option<Result<Output, Fault>>,
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

impl Sink<Result<Output, Fault>> for Headed {
    type Error = Fault;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Fault>> {
        self.get_mut()
            .drive(cx, |sink, cx| sink.poll_ready_unpin(cx))
    }

    fn start_send(self: Pin<&mut Self>, item: Result<Output, Fault>) -> Result<(), Fault> {
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
    let mut values = lines.filter_map(|line| {
        let (field, value) = line.split_once(':').unwrap_or((line, ""));
        let value = value.strip_prefix(' ').unwrap_or(value);
        (field == name && (name != "id" || !value.contains('\0'))).then_some(value)
    });
    values.next_back().map(str::to_owned)
}

/// An output as an event: its messages as `data:`, with its `id:` and
/// `event:` if it has them. An event with an id and no messages stays, as the
/// event that primes a client's `Last-Event-ID`. An id or name that would
/// break its line (a plugin's edit) is refused rather than written.
fn event(output: &Output) -> Result<Bytes, Fault> {
    if output.messages.is_empty() && output.event_id.is_none() {
        return Ok(Bytes::new());
    }
    let mut out = String::new();
    for (name, value) in [("id", &output.event_id), ("event", &output.event)] {
        if let Some(value) = value {
            if value.contains(['\r', '\n']) || (name == "id" && value.contains('\0')) {
                return Err(Fault::internal(format!(
                    "an SSE {name} contains invalid characters"
                )));
            }
            out.push_str(&format!("{name}: {value}\n"));
        }
    }
    let data = match output.messages.is_empty() {
        true => String::new(),
        false => body(output.messages.clone(), false).to_string(),
    };
    out.push_str(&sse::event(None, data));
    Ok(out.into())
}

/// A fault as a JSON-RPC error to `id`: its [`RpcCode`], else -32600 for
/// a `BadRequest` and -32603 for any other (JSON-RPC 2.0, 5.1), its
/// [`error_code`] as `data.code`, with the fields of its [`Details`] beside it.
fn failure(id: &Value, fault: &Fault) -> Value {
    let code = match fault.get::<RpcCode>() {
        Some(RpcCode(code)) => *code,
        None if fault.is::<BadRequest>() => -32600,
        None => -32603,
    };
    let mut e = error(id.clone(), code, fault.message());
    let mut data = json!({ "code": error_code(fault) });
    if let Some(Details(details)) = fault.get() {
        data.as_object_mut()
            .into_iter()
            .for_each(|d| d.extend(details.clone()));
    }
    e["error"]["data"] = data;
    e
}
