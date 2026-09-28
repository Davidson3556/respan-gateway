//! MCP over streamable HTTP as a [`Format`] of the MCP kind.
//!
//! - `POST` carries one message or a batch. A request is answered by JSON
//!   or by an event stream ending with its response; notifications and
//!   responses get `202 Accepted` with no body.
//! - `GET` opens the server's own event stream, which ends with its body.
//! - `DELETE` ends the session named by `Mcp-Session-Id`.

use respan_gateway_core::bytes::Bytes;
use respan_gateway_core::http::header::{ACCEPT, CONTENT_TYPE};
use respan_gateway_core::http::{self, Method, StatusCode};
use respan_gateway_core::serde_json::{Value, json};
use respan_gateway_core::{Decoder, Doc, Encoder, Fault, Format, Item, Part, codes};
use respan_gateway_formats::whole::Relay;
use respan_gateway_formats::{self as formats, sse};

use crate::kind::{Message, ToClient, ToServer, Verb, rpc};

/// The header naming an MCP session.
pub const SESSION: &str = "mcp-session-id";

/// MCP over streamable HTTP, served at `path` and sent to `path` relative
/// to each provider's transport.
#[derive(Debug, Clone, Copy)]
pub struct StreamableHttp {
    name: &'static str,
    path: &'static str,
}

impl Default for StreamableHttp {
    fn default() -> Self {
        Self::new("/mcp")
    }
}

impl StreamableHttp {
    /// The format named `mcp` at `path`.
    pub const fn new(path: &'static str) -> Self {
        Self { name: "mcp", path }
    }

    /// The same protocol under another name, such as for a second endpoint.
    pub const fn named(name: &'static str, path: &'static str) -> Self {
        Self { name, path }
    }
}

/// The verb of an HTTP method; `None` for one MCP does not use.
fn verb(method: &Method) -> Option<Verb> {
    match *method {
        Method::POST => Some(Verb::Send),
        Method::GET => Some(Verb::Listen),
        Method::DELETE => Some(Verb::End),
        _ => None,
    }
}

/// A refusal carrying its JSON-RPC error code, which the encoder writes.
pub fn rpc_fault(status: StatusCode, code: i64, message: impl Into<String>) -> Fault {
    let kernel_code = match status {
        StatusCode::BAD_REQUEST => codes::BAD_REQUEST,
        StatusCode::NOT_FOUND => codes::NOT_FOUND,
        _ => "mcp_error",
    };
    Fault::new(status, kernel_code, message).with_details(json!({ "jsonrpc_code": code }))
}

/// A `POST` body as the kind's input.
pub(crate) fn read_post(body: &Doc) -> Result<ToServer, Fault> {
    let not_json = || rpc_fault(StatusCode::BAD_REQUEST, rpc::PARSE_ERROR, "not JSON");
    let v = body.json().ok_or_else(not_json)?;
    let (items, batch) = match v {
        Value::Array(items) => (items.clone(), true),
        one => (vec![one.clone()], false),
    };
    let invalid = |why| rpc_fault(StatusCode::BAD_REQUEST, rpc::INVALID_REQUEST, why);
    if items.is_empty() {
        return Err(invalid("an empty batch"));
    }
    if items.iter().any(|m| !m.is_object()) {
        return Err(invalid("a message that is not a JSON object"));
    }
    Ok(ToServer {
        verb: Verb::Send,
        messages: items.into_iter().map(Message::new).collect(),
        batch,
    })
}

/// The messages in a whole body or an event's data; none for an empty
/// body or an event with empty data (a server's resumption primer).
fn read_messages(raw: &Bytes) -> Result<Option<Vec<Message>>, Fault> {
    if raw.iter().all(u8::is_ascii_whitespace) || has_empty_data(raw) {
        return Ok(None);
    }
    let v = formats::json(raw).ok_or_else(|| Fault::upstream("not a JSON-RPC message"))?;
    let items = match v {
        Value::Array(items) => items,
        one => vec![one],
    };
    Ok(Some(items.into_iter().map(Message::new).collect()))
}

/// Whether an event's data lines are all empty. `formats::sse` keeps its
/// own reading of `data:` private, so it is repeated here.
fn has_empty_data(raw: &Bytes) -> bool {
    let text = String::from_utf8_lossy(raw);
    let mut data = text
        .lines()
        .filter_map(|l| l.strip_prefix("data:").or((l == "data").then_some("")));
    let mut any = false;
    let empty = data.all(|d| {
        any = true;
        d.trim().is_empty()
    });
    any && empty
}

/// Whether the client takes an event stream.
fn takes_sse(headers: &http::HeaderMap) -> bool {
    headers
        .get_all(ACCEPT)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .any(|v| v.contains("text/event-stream"))
}

/// Items of `messages`, decoded from `raw`.
fn item(messages: Vec<Message>, raw: Bytes) -> Part {
    Item::decoded(messages.into_iter().map(ToClient), raw).into()
}

impl Format for StreamableHttp {
    type Input = ToServer;
    type Output = ToClient;

    fn name(&self) -> &str {
        self.name
    }

    fn accepts(&self, head: &http::request::Parts) -> bool {
        head.uri.path() == self.path && verb(&head.method).is_some()
    }

    fn decode_input(&self, head: &http::request::Parts, body: &Doc) -> Result<ToServer, Fault> {
        match verb(&head.method) {
            Some(Verb::Send) => read_post(body),
            Some(verb) => Ok(ToServer::bare(verb)),
            None => Err(Fault::bad_request("not an MCP method")),
        }
    }

    /// The kind holds every field, so the input is written anew.
    fn encode_input(
        &self,
        input: &ToServer,
        _original: Option<(&Doc, &ToServer)>,
    ) -> Result<Doc, Fault> {
        if input.verb != Verb::Send {
            return Ok(Doc::default());
        }
        let mut all = input.messages.iter().map(|m| m.json().clone());
        let body = match (input.batch, input.messages.len()) {
            (false, 1) => all.next().unwrap_or_default(),
            _ => Value::Array(all.collect()),
        };
        Ok(Doc::from(body))
    }

    fn prepare(
        &self,
        body: Doc,
        input: Option<&ToServer>,
        client: &http::request::Parts,
        relayed: bool,
    ) -> Result<http::Request<Doc>, Fault> {
        // The verb is the input's; a body relayed unread keeps the client's.
        let method = match input {
            Some(input) => input.verb.method(),
            None => client.method.clone(),
        };
        let query = client.uri.query().filter(|q| relayed && !q.is_empty());
        let uri = match query {
            Some(q) => format!("{}?{q}", self.path),
            None => self.path.to_owned(),
        };
        let mut req = http::Request::builder()
            .method(method.clone())
            .uri(uri)
            .header(ACCEPT, "application/json, text/event-stream");
        if method == Method::POST {
            req = req.header(CONTENT_TYPE, "application/json");
        }
        // The session is protocol state: every request of it names it,
        // a call with its own input too.
        if let Some(session) = client.headers.get(SESSION) {
            req = req.header(SESSION, session);
        }
        let mut req = req.body(body)?;
        formats::pass_headers(&mut req, relayed.then_some(client));
        Ok(req)
    }

    fn decoder(&self, head: &http::response::Parts, call: &http::Request<Doc>) -> Box<dyn Decoder> {
        let on_message = |raw: Bytes, out: &mut Vec<Part>| {
            match read_messages(&raw)? {
                Some(messages) => out.push(item(messages, raw)),
                None if raw.is_empty() => {}
                None => out.push(Part::Bytes(raw)),
            }
            Ok(())
        };
        match verb(call.method()) {
            // The server's own stream ends with its body.
            Some(Verb::Listen) => sse::at_eof(sse::or_whole(head, on_message)),
            Some(Verb::End) | None => sse::or_whole(head, on_message),
            // A POST's stream ends with the responses to the requests the
            // call sent.
            Some(Verb::Send) => {
                let body = read_post(call.body()).ok();
                let mut pending: Vec<Value> =
                    body.iter().flat_map(|b| b.request_ids().cloned()).collect();
                let stream = sse::is_stream(head);
                sse::or_whole(head, move |raw, out| {
                    let Some(messages) = read_messages(&raw)? else {
                        if !raw.is_empty() {
                            out.push(Part::Bytes(raw));
                        }
                        return Ok(());
                    };
                    for m in messages.iter().filter(|m| m.is_response()) {
                        pending.retain(|id| Some(id) != m.id());
                    }
                    out.push(item(messages, raw));
                    if stream && pending.is_empty() {
                        out.push(Part::End(Ok(())));
                    }
                    Ok(())
                })
            }
        }
    }

    fn encoder(
        &self,
        head: &mut http::response::Parts,
        client: &http::Request<Doc>,
        relayed: bool,
    ) -> Box<dyn Encoder> {
        let listen = verb(client.method()) == Some(Verb::Listen);
        let input = read_post(client.body()).ok();
        let stream = match relayed {
            true => sse::is_stream(head),
            false if head.headers.contains_key(CONTENT_TYPE) => {
                let ct = head.headers.get(CONTENT_TYPE).and_then(|v| v.to_str().ok());
                ct.is_some_and(|c| c.starts_with("text/event-stream"))
            }
            false if !head.status.is_success() => false,
            false => listen || takes_sse(client.headers()),
        };
        if !relayed && head.status != StatusCode::ACCEPTED {
            let ct = if stream {
                "text/event-stream"
            } else {
                "application/json"
            };
            formats::default_content_type(head, ct);
        }
        let batch = input.as_ref().is_some_and(|i| i.batch);
        let mut ids: Vec<Value> = input
            .iter()
            .flat_map(|i| i.request_ids().cloned())
            .collect();
        if ids.is_empty() {
            ids.push(Value::Null);
        }
        let frame = move |v: &Value, like: Option<&Bytes>| match stream {
            true => sse::event_as(like, None, v),
            false => v.to_string(),
        };
        let write = move |events: &[ToClient], like: Option<&Bytes>| {
            let one = events.len() == 1;
            Ok(match (stream, one && !batch) {
                (true, _) => events
                    .iter()
                    .map(|e| frame(e.0.json(), like.filter(|_| one)))
                    .collect(),
                (false, _) if events.is_empty() => String::new(),
                (false, true) => events[0].0.json().to_string(),
                (false, false) => {
                    Value::Array(events.iter().map(|e| e.0.json().clone()).collect()).to_string()
                }
            })
        };
        let fail = move |f: &Fault| {
            let code = f
                .details()
                .and_then(|d| d["jsonrpc_code"].as_i64())
                .unwrap_or(match f.code() {
                    codes::BAD_REQUEST => rpc::INVALID_REQUEST,
                    codes::NOT_FOUND => rpc::METHOD_NOT_FOUND,
                    _ => rpc::INTERNAL_ERROR,
                });
            let data = Some(json!({ "code": f.code() }));
            let errors: Vec<Value> = ids
                .iter()
                .map(|id| {
                    Message::error(id.clone(), code, f.message(), data.clone())
                        .json()
                        .clone()
                })
                .collect();
            match (stream, errors.len()) {
                (true, _) => errors.iter().map(|e| frame(e, None)).collect(),
                (false, 1) => errors[0].to_string(),
                (false, _) => Value::Array(errors).to_string(),
            }
        };
        // The server's own stream may be quiet for long: open it with a
        // comment, so its head goes out at once, relayed or merged.
        let open = match stream && listen {
            true => sse::comment("stream"),
            false => String::new(),
        };
        Box::new(Relay::new(write, fail, stream).with_open(open))
    }
}
