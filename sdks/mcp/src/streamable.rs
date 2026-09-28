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

use respan_gateway_core::bytes::Bytes;
use respan_gateway_core::http::header::{ACCEPT, CONTENT_TYPE};
use respan_gateway_core::http::{self, HeaderValue, Method, StatusCode};
use respan_gateway_core::serde_json::{Value, json};
use respan_gateway_core::{Decoder, Encoder, Fault, Format, codes};
use respan_gateway_formats::{self as formats, relayed::Relayed, sse, whole::Relay};
use respan_gateway_http::bodiless;

use crate::{ToClient, ToServer, body, error, forget_answered, lock, read, requests};

/// The header naming an MCP session.
pub const SESSION: &str = "mcp-session-id";

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

/// The ids of the requests `request` posts, and whether it is a batch.
fn posted(request: &http::Request<Bytes>) -> (Vec<Value>, bool) {
    let (messages, batch) = read(request.body()).unwrap_or_default();
    (requests(&messages).cloned().collect(), batch)
}

impl Format for StreamableHttp {
    type Input = ToServer;
    type Output = ToClient;

    fn name(&self) -> &str {
        NAME
    }

    fn accepts(&self, head: &http::request::Parts) -> bool {
        let mcp = [Method::POST, Method::GET, Method::DELETE].contains(&head.method);
        head.uri.path() == self.path && mcp
    }

    fn request_decoder(&self, request: &http::Request<Bytes>) -> Box<dyn Decoder<ToServer>> {
        let (method, name) = (request.method().clone(), Arc::from(NAME));
        formats::message_decoder(move |raw| {
            let mut input = ToServer::new(method.clone(), Vec::new());
            if method == Method::POST {
                let bad = || Fault::bad_request("not a JSON-RPC message or batch");
                let read = read(&raw).filter(|(m, _)| !m.is_empty());
                (input.messages, input.batch) = read.ok_or_else(bad)?;
            }
            input.origin = Relayed::came(&name, raw, input.clone());
            Ok(input)
        })
    }

    fn request_encoder(
        &self,
        request: &mut http::Request<Bytes>,
        first: Option<&ToServer>,
        streams: bool,
    ) -> Box<dyn Encoder<ToServer>> {
        if streams {
            return formats::whole::unstreamable(NAME);
        }
        if first.is_some() {
            formats::body_changed(request);
        }
        formats::set_path(request, self.path);
        if let Some(first) = first {
            *request.method_mut() = first.method.clone();
        }
        let post = request.method() == Method::POST;
        let headers = request.headers_mut();
        let accept = HeaderValue::from_static("application/json, text/event-stream");
        headers.entry(ACCEPT).or_insert(accept);
        if post {
            headers
                .entry(CONTENT_TYPE)
                .or_insert(HeaderValue::from_static(JSON));
        }
        // A POST's messages, as the client's bytes while they say the same.
        let write = move |inputs: &[ToServer]| {
            let posts = inputs.iter().filter(|i| i.method == Method::POST);
            let mut bytes = posts.map(|i| match i.origin.bytes(NAME, i) {
                Some(bytes) => bytes.clone(),
                None => body(&i.messages, i.batch).to_string().into(),
            });
            Ok::<_, Fault>(bytes.next_back().unwrap_or_default())
        };
        Box::new(Relay::new(write, |_: &Fault| Bytes::new(), false))
    }

    fn response_decoder(
        &self,
        head: &http::response::Parts,
        request: &http::Request<Bytes>,
    ) -> Box<dyn Decoder<ToClient>> {
        if head.status.is_success() && bodiless(request.method(), head.status) {
            return formats::whole::nothing();
        }
        let name = Arc::from(NAME);
        let on_message = move |raw: Bytes, out: &mut Vec<ToClient>| {
            if !raw.is_empty() {
                let mut output = ToClient::new(Vec::new());
                (output.messages, output.batch) = read(&raw).unwrap_or_default();
                output.origin = Relayed::came(&name, raw, output.clone());
                out.push(output);
            }
            Ok(())
        };
        let inner = sse::or_whole(NAME, head, on_message);
        let pending = posted(request).0;
        Box::new(Answered { inner, pending })
    }

    /// Messages as events or one JSON body, each as its bytes while it says
    /// the same, and a failure as a JSON-RPC error per request unanswered:
    /// a refusal under its status, unless a provider's answer in this
    /// format, which goes as it came. A 202, a 204 and a response to a
    /// `HEAD` have no body.
    fn response_encoder(
        &self,
        head: &mut http::response::Parts,
        request: &http::Request<Bytes>,
        refusal: Option<&Fault>,
    ) -> Box<dyn Encoder<ToClient>> {
        if let Some(fault) = refusal {
            let body = respan_gateway_http::answered(NAME, fault, head).unwrap_or_else(|| {
                formats::default_content_type(head, JSON);
                let (pending, batch) = posted(request);
                failure(fault, &pending, false, batch).into()
            });
            let bodiless = bodiless(request.method(), head.status);
            return formats::whole::written(if bodiless { Bytes::new() } else { body });
        }
        if bodiless(request.method(), head.status) || head.status == StatusCode::ACCEPTED {
            return formats::whole::no_body();
        }
        let listen = request.method() == Method::GET;
        if !matches!(head.status, StatusCode::ACCEPTED | StatusCode::NO_CONTENT) {
            let stream = listen && head.status.is_success();
            formats::default_content_type(head, if stream { SSE } else { JSON });
        }
        let stream = sse::is_stream(head);
        let (pending, batch) = posted(request);
        // The requests not yet answered in a stream; all in a JSON body.
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
        let fail = move |f: &Fault| -> Bytes {
            let pending = lock(&pending);
            failure(f, &pending, stream, batch).into()
        };
        let relay = Relay::new(write, fail, stream);
        // A quiet stream opens with a comment, so its head goes at once.
        let open = (stream && listen).then(|| sse::comment("stream"));
        Box::new(relay.with_open(open.unwrap_or_default()))
    }

    fn is_content(&self, output: &ToClient) -> bool {
        !output.messages.is_empty()
    }
}

/// A response's messages, complete once every request it answers is.
struct Answered {
    inner: Box<dyn Decoder<ToClient>>,
    /// The requests not yet answered.
    pending: Vec<Value>,
}

impl Decoder<ToClient> for Answered {
    fn decode(&mut self, bytes: Bytes, out: &mut Vec<ToClient>) -> Result<(), Fault> {
        let from = out.len();
        let decoded = self.inner.decode(bytes, out);
        forget_answered(&mut self.pending, &out[from..]);
        decoded
    }

    fn end(&mut self, out: &mut Vec<ToClient>) -> Result<(), Fault> {
        let from = out.len();
        self.inner.end(out)?;
        forget_answered(&mut self.pending, &out[from..]);
        if !self.pending.is_empty() {
            return Err(Fault::truncated());
        }
        Ok(())
    }
}

/// An output as an event: its bytes while it says the same.
fn event(output: &ToClient) -> Bytes {
    if let Some(bytes) = output.origin.bytes(NAME, output) {
        return bytes.clone();
    } else if output.messages.is_empty() {
        return Bytes::new();
    }
    let like = output.origin.origin(NAME).map(|o| &o.bytes);
    sse::event_as(like, None, body(&output.messages, output.batch)).into()
}

/// Outputs as one JSON body: the one's bytes while it says the same.
fn whole(outputs: &[ToClient]) -> Bytes {
    if let [one] = outputs
        && let Some(bytes) = one.origin.bytes(NAME, one)
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
