//! A user-defined, MCP-like kind: JSON-RPC tool calls over `POST .../mcp`.
//! The core and the conversation kind have never seen it, so a chat
//! guardrail reading it by type gets `WRONG_KIND`, and must refuse it
//! rather than pass it silently.

use std::sync::Arc;

use respan_gateway_core::bytes::Bytes;
use respan_gateway_core::http::{self, header::CONTENT_TYPE};
use respan_gateway_core::serde_json::{self, Value, json};
use respan_gateway_core::{Decoder, Doc, Encoder, Fault, Format, Item, Part};
use respan_gateway_formats::{self as formats, whole};

use crate::guard::{Readable, Streamed};

/// The kind's input: one JSON-RPC request, such as `tools/call`.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct McpRequest {
    /// The JSON-RPC method.
    pub method: String,
    /// Its params, shared as they may be large.
    pub params: Arc<Value>,
}

impl McpRequest {
    /// A request of `method` with `params`.
    pub fn new(method: impl Into<String>, params: Value) -> Self {
        Self {
            method: method.into(),
            params: Arc::new(params),
        }
    }
}

/// The kind's output: one JSON-RPC result.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct McpResult {
    /// The result, shared.
    pub result: Arc<Value>,
}

/// A tool call's arguments are what a guard must read.
impl Readable for McpRequest {
    fn text(&self) -> String {
        format!("{} {}", self.method, self.params)
    }
}

impl Streamed for McpResult {
    fn text(&self) -> Option<String> {
        Some(self.result.to_string())
    }

    fn continues(&self) -> bool {
        false
    }
}

/// The MCP-like format: `POST` to a path ending `/mcp`, a whole JSON body
/// each way. It names no model.
#[derive(Debug, Clone, Copy, Default)]
pub struct McpLite;

impl Format for McpLite {
    type Input = McpRequest;
    type Output = McpResult;

    fn name(&self) -> &str {
        "mcp-lite"
    }

    fn accepts(&self, head: &http::request::Parts) -> bool {
        formats::posts_to(head, "/mcp")
    }

    fn decode_input(&self, _head: &http::request::Parts, body: &Doc) -> Result<McpRequest, Fault> {
        let v = body
            .json()
            .ok_or_else(|| Fault::bad_request("not JSON-RPC"))?;
        let method = v["method"]
            .as_str()
            .ok_or_else(|| Fault::bad_request("no method"))?;
        Ok(McpRequest::new(method, v["params"].clone()))
    }

    fn encode_input(
        &self,
        input: &McpRequest,
        original: Option<(&Doc, &McpRequest)>,
    ) -> Result<Doc, Fault> {
        let mut v = original
            .and_then(|(body, _)| body.json().cloned())
            .unwrap_or_else(|| json!({ "jsonrpc": "2.0", "id": 1 }));
        v["method"] = input.method.clone().into();
        v["params"] = (*input.params).clone();
        Ok(Doc::from(v))
    }

    fn prepare(
        &self,
        body: Doc,
        _: Option<&McpRequest>,
        client: &http::request::Parts,
        relayed: bool,
    ) -> Result<http::Request<Doc>, Fault> {
        let mut req = http::Request::post("/mcp")
            .header(CONTENT_TYPE, "application/json")
            .body(body)?;
        formats::pass_headers(&mut req, relayed.then_some(client));
        Ok(req)
    }

    fn decoder(
        &self,
        head: &http::response::Parts,
        _call: &http::Request<Doc>,
    ) -> Box<dyn Decoder> {
        whole::decoder(head, |raw: Bytes, out: &mut Vec<Part>| {
            let v: Value =
                serde_json::from_slice(&raw).map_err(|_| Fault::upstream("not JSON-RPC"))?;
            if let Some(msg) = v["error"]["message"].as_str() {
                out.push(Part::End(Err(Fault::upstream(msg).with_raw(raw.clone()))));
                return Ok(());
            }
            let result = McpResult {
                result: Arc::new(v["result"].clone()),
            };
            out.push(Item::decoded([result], raw).into());
            Ok(())
        })
    }

    fn encoder(
        &self,
        head: &mut http::response::Parts,
        _client: &http::Request<Doc>,
        _relayed: bool,
    ) -> Box<dyn Encoder> {
        formats::default_content_type(head, "application/json");
        let write = |results: &[McpResult], _: Option<&Bytes>| {
            let result = results.last().map(|r| (*r.result).clone());
            Ok(json!({ "jsonrpc": "2.0", "id": 1, "result": result }).to_string())
        };
        let fail = |f: &Fault| {
            json!({ "jsonrpc": "2.0", "id": 1,
                "error": { "code": -32000, "message": f.message(), "data": { "code": f.code() } } })
            .to_string()
        };
        Box::new(whole::Relay::new(write, fail, false))
    }
}
