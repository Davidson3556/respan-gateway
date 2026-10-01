//! MCP over streamable HTTP as a [`Format`] of the MCP kind.
//!
//! - `POST` carries a message or a batch. Requests are answered by JSON or
//!   by an event stream, complete once every request is answered;
//!   notifications and responses get `202 Accepted` with no body.
//! - `GET` opens the server's own event stream, which may stay quiet.
//! - `DELETE` ends the session, with no body.
//!
//! The session header ([`SESSION`]) passes both ways with the heads, as a
//! relay passes end-to-end headers.

use std::sync::{Arc, Mutex};

use respan_gateway_http::bytes::Bytes;
use respan_gateway_http::formats::{self as formats, sse, whole::Relay};
use respan_gateway_http::http::header::{ACCEPT, CONTENT_TYPE};
use respan_gateway_http::http::{HeaderValue, Method, StatusCode};
use respan_gateway_http::http::{Request, Response};
use respan_gateway_http::{
    self as h, Body, Incoming, Outgoing, bodiless, http::response::Parts as ResponseHead,
};
use respan_gateway_sdk::futures::stream::BoxStream;
use respan_gateway_sdk::futures::{StreamExt, stream};
use respan_gateway_sdk::relayed::Relayed;
use respan_gateway_sdk::{Connection, EventSink, EventStream, Fault, Format, codes};
use serde_json::{Value, json};

use crate::{ToClient, ToServer, body, error, forget_answered, lock, read, requests};

/// The header naming an MCP session.
pub const SESSION: &str = "mcp-session-id";

const NAME: &str = "mcp";
// Output provenance includes wire framing: a rebuilt response may be written
// with a different content type than the provider answer it collected.
const JSON_ORIGIN: &str = "mcp/json";
const SSE_ORIGIN: &str = "mcp/sse";
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

/// The ids of the requests a body posts, and whether it is a batch.
fn posted(body: &Bytes) -> (Vec<Value>, bool) {
    let (messages, batch) = read(body).unwrap_or_default();
    (requests(&messages).cloned().collect(), batch)
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
        Ok(io.bind(
            NAME,
            |request| {
                h::messages(request, |head, raw| {
                    let mut input = ToServer::new(head.method.clone(), Vec::new());
                    if head.method == Method::POST {
                        let bad = || Fault::bad_request("not a JSON-RPC message or batch");
                        let read = read(&raw).filter(|(m, _)| !m.is_empty());
                        (input.messages, input.batch) = read.ok_or_else(bad)?;
                    }
                    input.origin = Relayed::came(&Arc::from(NAME), raw, input.clone());
                    Ok(input)
                })
            },
            |reply, source| {
                let head = h::response_head(NAME, source);
                Ok(response_body(reply, head, source))
            },
            |fault, request, head| {
                formats::default_content_type(head, JSON);
                let (pending, batch) = posted(request.body());
                failure(fault, &pending, false, batch).into()
            },
        ))
    }
}

impl Format<Outgoing, ToClient, ToServer> for StreamableHttp {
    fn name(&self) -> &str {
        NAME
    }

    fn bind(&self, io: Outgoing) -> Result<Connection<ToClient, ToServer>, Fault> {
        let path = self.path;
        Ok(io.bind(
            NAME,
            move |source: &mut EventStream<ToServer>| {
                if !source.is_single() {
                    return Err(Fault::config(
                        "a call in mcp carries one body, not streamed inputs",
                    ));
                }
                let mut head = h::request_head(NAME, source)
                    .cloned()
                    .unwrap_or_else(|| Request::new(()).into_parts().0);
                if h::raw_body(NAME, source).is_none() {
                    formats::body_changed(&mut head);
                    if let Some(value) = source.value() {
                        head.method = value?.method.clone();
                    }
                }
                formats::set_path(&mut head, path);
                head.headers
                    .entry(ACCEPT)
                    .or_insert(HeaderValue::from_static(
                        "application/json, text/event-stream",
                    ));
                if head.method == Method::POST {
                    head.headers
                        .entry(CONTENT_TYPE)
                        .or_insert(HeaderValue::from_static(JSON));
                }
                let write = |inputs: &[ToServer]| {
                    let posts = inputs.iter().filter(|i| i.method == Method::POST);
                    let mut bytes = posts.map(|i| match i.origin.bytes(NAME, i) {
                        Some(bytes) => bytes.clone(),
                        None => body(&i.messages, i.batch).to_string().into(),
                    });
                    Ok::<_, Fault>(bytes.next_back().unwrap_or_default())
                };
                let (request, sink) = h::request(
                    NAME,
                    source,
                    Box::new(Relay::new(write, |_: &Fault| Bytes::new(), false)),
                )?;
                Ok((Request::from_parts(head, request.into_body()), sink))
            },
            response_events,
            |answer, _| answer.status.is_success(),
        ))
    }
}

fn response_events(
    answer: Response<Body>,
    sent: Request<Bytes>,
) -> BoxStream<'static, Result<ToClient, Fault>> {
    if answer.status().is_success() && bodiless(sent.method(), answer.status()) {
        return stream::empty().boxed();
    }
    let (head, body) = answer.into_parts();
    let is_event = sse::is_stream(&head);
    let origin = Arc::from(if is_event { SSE_ORIGIN } else { JSON_ORIGIN });
    let on_message = move |raw: Bytes, out: &mut Vec<ToClient>| {
        if !raw.is_empty() {
            let mut output = ToClient::new(Vec::new());
            (output.messages, output.batch) = read(&raw).unwrap_or_default();
            output.origin = Relayed::came(&origin, raw, output.clone());
            out.push(output);
        }
        Ok(())
    };
    let inner = if is_event {
        sse::read(
            body,
            (),
            move |_, raw, out| on_message(raw, out),
            |_, end, _| end,
        )
    } else {
        formats::whole::read(NAME, Response::from_parts(head, body), on_message)
    };
    stream::unfold(
        (inner, posted(sent.body()).0, false),
        |(mut inner, mut pending, done)| async move {
            if done {
                return None;
            }
            match inner.next().await {
                Some(Ok(output)) => {
                    forget_answered(&mut pending, std::slice::from_ref(&output));
                    Some((Ok(output), (inner, pending, false)))
                }
                Some(Err(fault)) => Some((Err(fault), (inner, pending, true))),
                None if !pending.is_empty() => {
                    Some((Err(Fault::truncated()), (inner, pending, true)))
                }
                None => None,
            }
        },
    )
    .boxed()
}

/// The body of a response to the original client's request.
fn response_body(
    reply: h::Reply,
    mut head: ResponseHead,
    source: &mut EventStream<ToClient>,
) -> EventSink<ToClient> {
    let ask = reply.request();
    if bodiless(ask.method(), head.status) || head.status == StatusCode::ACCEPTED {
        return reply.empty(head, source);
    }
    let listen = ask.method() == Method::GET;
    if !matches!(head.status, StatusCode::ACCEPTED | StatusCode::NO_CONTENT) {
        let stream = listen && head.status.is_success();
        formats::default_content_type(&mut head, if stream { SSE } else { JSON });
    }
    let stream = sse::is_stream(&head);
    let (pending, batch) = posted(ask.body());
    let pending = Arc::new(Mutex::new(pending));
    let left = pending.clone();
    let write = move |outputs: &[ToClient]| {
        forget_answered(&mut lock(&left), outputs);
        Ok::<_, Fault>(if stream {
            outputs.first().map(event).unwrap_or_default()
        } else {
            whole(outputs)
        })
    };
    let fail = move |f: &Fault| -> Bytes { failure(f, &lock(&pending), stream, batch).into() };
    let encoder = Box::new(Relay::new(write, fail, stream));
    if stream && listen {
        reply.open(head, sse::comment("stream").into(), encoder)
    } else {
        reply.write(head, encoder)
    }
}

/// An output as an event: its bytes while it says the same.
fn event(output: &ToClient) -> Bytes {
    if let Some(bytes) = output.origin.bytes(SSE_ORIGIN, output) {
        return bytes.clone();
    } else if output.messages.is_empty() {
        return Bytes::new();
    }
    let like = output.origin.came_in(SSE_ORIGIN).map(|o| &o.bytes);
    sse::event_as(like, None, body(&output.messages, output.batch)).into()
}

/// Outputs as one JSON body: the one's bytes while it says the same.
fn whole(outputs: &[ToClient]) -> Bytes {
    if let [one] = outputs
        && let Some(bytes) = one.origin.bytes(JSON_ORIGIN, one)
    {
        return bytes.clone();
    }
    let batch = outputs.len() > 1 || outputs.iter().any(|o| o.batch);
    let all: Vec<Value> = outputs.iter().flat_map(|o| o.messages.clone()).collect();
    if all.is_empty() {
        return Bytes::new();
    }
    body(&all, batch || all.len() != 1).to_string().into()
}

/// A fault as a JSON-RPC error to each of `pending`, or to `null`.
fn failure(fault: &Fault, pending: &[Value], stream: bool, batch: bool) -> String {
    let code = if fault.code() == codes::BAD_REQUEST {
        -32600
    } else {
        -32603
    };
    let ids = if pending.is_empty() {
        &[Value::Null][..]
    } else {
        pending
    };
    let errors = ids.iter().map(|id| {
        let mut e = error(id.clone(), code, fault.message());
        e["error"]["data"] = json!({ "code": fault.code() });
        e
    });
    if stream {
        errors.map(|e| sse::event(None, e)).collect()
    } else {
        body(&errors.collect::<Vec<_>>(), batch).to_string()
    }
}
