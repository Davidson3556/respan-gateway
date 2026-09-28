//! The MCP kind: what a client sends to a server ([`ToServer`]) and what a
//! server sends back ([`ToClient`]). Both directions carry JSON-RPC 2.0
//! messages, so each direction gets its own type: the kernel requires a
//! kind's input and output to differ.

use std::sync::Arc;

use respan_gateway_core::serde_json::{Value, json};

/// One JSON-RPC 2.0 message, kept whole as JSON so every field MCP adds
/// survives. Shared behind an `Arc`, as the kernel copies and compares
/// inputs and outputs to tell what a plugin changed.
#[derive(Debug, Clone, PartialEq)]
pub struct Message(Arc<Value>);

impl Message {
    /// A message from its JSON.
    pub fn new(json: Value) -> Self {
        Self(Arc::new(json))
    }

    /// A successful response to `id`.
    pub fn result(id: Value, result: Value) -> Self {
        Self::new(json!({ "jsonrpc": "2.0", "id": id, "result": result }))
    }

    /// An error response to `id` (`null` if unknown), in JSON-RPC's shape.
    pub fn error(id: Value, code: i64, message: &str, data: Option<Value>) -> Self {
        let mut error = json!({ "code": code, "message": message });
        if let Some(data) = data {
            error["data"] = data;
        }
        Self::new(json!({ "jsonrpc": "2.0", "id": id, "error": error }))
    }

    /// The message as JSON.
    pub fn json(&self) -> &Value {
        &self.0
    }

    /// The message as JSON, to change; copied first if shared.
    pub fn json_mut(&mut self) -> &mut Value {
        Arc::make_mut(&mut self.0)
    }

    /// Its id, if it is a request or a response.
    pub fn id(&self) -> Option<&Value> {
        self.0.get("id").filter(|id| !id.is_null())
    }

    /// Its method, if it is a request or a notification.
    pub fn method(&self) -> Option<&str> {
        self.0.get("method")?.as_str()
    }

    /// Its params, or `null`.
    pub fn params(&self) -> &Value {
        self.0.get("params").unwrap_or(&Value::Null)
    }

    /// Its result, if it is a successful response.
    pub fn result_json(&self) -> Option<&Value> {
        self.0.get("result")
    }

    /// Whether it is a request: a method and an id.
    pub fn is_request(&self) -> bool {
        self.method().is_some() && self.id().is_some()
    }

    /// Whether it is a response: an id, no method.
    pub fn is_response(&self) -> bool {
        self.method().is_none() && self.id().is_some()
    }
}

/// What the client's HTTP method asks of a streamable-HTTP MCP server.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verb {
    /// `POST`: messages for the server.
    Send,
    /// `GET`: open the server's own stream of messages.
    Listen,
    /// `DELETE`: end the session.
    End,
}

impl Verb {
    /// The HTTP method of this verb.
    pub fn method(self) -> respan_gateway_core::http::Method {
        use respan_gateway_core::http::Method;
        match self {
            Verb::Send => Method::POST,
            Verb::Listen => Method::GET,
            Verb::End => Method::DELETE,
        }
    }
}

/// The kind's input: one HTTP request from an MCP client. `GET` and
/// `DELETE` carry no messages, so the verb is part of the input.
#[derive(Debug, Clone, PartialEq)]
pub struct ToServer {
    /// What the request asks.
    pub verb: Verb,
    /// The messages of a `POST`, in order.
    pub messages: Vec<Message>,
    /// Whether they came as a JSON array (MCP 2025-03-26 batches).
    pub batch: bool,
}

impl ToServer {
    /// A `POST` of one message.
    pub fn send(message: Message) -> Self {
        Self {
            verb: Verb::Send,
            messages: vec![message],
            batch: false,
        }
    }

    /// A request with no messages, such as `GET` or `DELETE`.
    pub fn bare(verb: Verb) -> Self {
        Self {
            verb,
            messages: Vec::new(),
            batch: false,
        }
    }

    /// The ids of its requests, which the server must answer.
    pub fn request_ids(&self) -> impl Iterator<Item = &Value> {
        self.messages
            .iter()
            .filter(|m| m.is_request())
            .filter_map(Message::id)
    }

    /// Its only request, if it is a `POST` of exactly one message that is
    /// a request.
    pub fn single(&self) -> Option<&Message> {
        match (&self.verb, &self.messages[..], self.batch) {
            (Verb::Send, [m], false) if m.is_request() => Some(m),
            _ => None,
        }
    }
}

/// The kind's output: one message from the server to the client.
#[derive(Debug, Clone, PartialEq)]
pub struct ToClient(pub Message);

/// JSON-RPC error codes.
pub mod rpc {
    /// Invalid JSON.
    pub const PARSE_ERROR: i64 = -32700;
    /// Not a valid request object.
    pub const INVALID_REQUEST: i64 = -32600;
    /// No such method.
    pub const METHOD_NOT_FOUND: i64 = -32601;
    /// Bad params, such as an unknown tool (the MCP spec's choice).
    pub const INVALID_PARAMS: i64 = -32602;
    /// Anything else.
    pub const INTERNAL_ERROR: i64 = -32603;
}
