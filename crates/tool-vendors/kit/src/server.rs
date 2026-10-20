//! A tool vendor as an MCP server in process.

use std::sync::Arc;

use respan_gateway_core::{Extensions, Fault};
use respan_gateway_http::http::{self, Method, StatusCode, header};
use respan_gateway_http::http_body_util::BodyExt;
use respan_gateway_http::{Body, Sent};
use respan_gateway_mcp::RpcCode;
use serde_json::{Value, json};

use crate::{Api, Content, ToolVendor};

/// The key of a `tools/call` result's `_meta` that holds what the call
/// used ([`ToolOutput::usage`](crate::ToolOutput::usage)), by unit:
/// `{"_meta": {"respan.ai/usage": {"credits": 2}}}`.
pub const USAGE: &str = "respan.ai/usage";
/// JSON-RPC's code for a body that is no JSON-RPC.
const PARSE_ERROR: i64 = -32700;
/// JSON-RPC's code for a message that is no request.
const INVALID_REQUEST: i64 = -32600;
/// JSON-RPC's code for a method the server does not have.
const METHOD_NOT_FOUND: i64 = -32601;
/// JSON-RPC's code for parameters it cannot take, such as an unknown tool.
const INVALID_PARAMS: i64 = -32602;

/// Serves a [`ToolVendor`]'s tools as an MCP server, in process.
#[derive(Debug, Clone)]
pub struct ToolServer {
    pub(crate) vendor: &'static ToolVendor,
    pub(crate) api: Api,
}

impl ToolServer {
    /// Serve `vendor`'s tools, calling its API through `api`.
    pub fn new(vendor: &'static ToolVendor, api: Api) -> Self {
        Self { vendor, api }
    }

    /// Fresh outgoing I/O for each call, answered in process. Register it
    /// behind the provider side of the MCP format, as
    /// `Provider::new(name, StreamableHttp::new("/"), server.connect(limit))`.
    pub fn connect(
        self,
        limit: usize,
    ) -> impl Fn(respan_gateway_core::Extensions) -> respan_gateway_http::ProviderIo
    + Clone
    + Send
    + Sync
    + 'static {
        let server = Arc::new(self);
        let connect = respan_gateway_http::connect(
            move |request| {
                let server = server.clone();
                async move { server.send(request).await }
            },
            limit,
        );
        move |extensions: Extensions| connect(extensions)
    }

    /// Answer an MCP request as a stateless streamable HTTP server would:
    /// a `POST`'s requests with their responses as JSON (202 if it held
    /// none), a `GET` with 405, a `DELETE` with 204.
    pub async fn send(&self, call: http::Request<Body>) -> Sent {
        let (head, body) = call.into_parts();
        match head.method {
            Method::POST => {}
            Method::DELETE => return Ok((empty(StatusCode::NO_CONTENT), None)),
            _ => return Ok((empty(StatusCode::METHOD_NOT_ALLOWED), None)),
        }
        let body = body.collect().await?.to_bytes();
        let posted: Value = serde_json::from_slice(&body).map_err(|e| {
            Fault::bad_request("not JSON-RPC")
                .with_source(e)
                .with(RpcCode(PARSE_ERROR))
        })?;
        let answer = match posted {
            Value::Array(messages) => {
                let mut answers = Vec::new();
                for message in messages {
                    answers.extend(self.answer(message).await);
                }
                (!answers.is_empty()).then_some(Value::Array(answers))
            }
            message => self.answer(message).await,
        };
        let Some(answer) = answer else {
            return Ok((empty(StatusCode::ACCEPTED), None));
        };
        let mut resp = http::Response::new(Body::full(answer.to_string()));
        resp.headers_mut().insert(
            header::CONTENT_TYPE,
            http::HeaderValue::from_static("application/json"),
        );
        Ok((resp, None))
    }

    /// The response to `message` if it is a request; notifications and
    /// responses get none.
    async fn answer(&self, message: Value) -> Option<Value> {
        let id = message.get("id").cloned()?;
        let Some(method) = message.get("method").and_then(Value::as_str) else {
            // A response of the client's, or no message at all.
            return message
                .get("result")
                .or_else(|| message.get("error"))
                .is_none()
                .then(|| error(id, INVALID_REQUEST, "not a JSON-RPC request"));
        };
        let params = message.get("params").cloned().unwrap_or(Value::Null);
        let outcome = match method {
            "initialize" => Ok(self.initialize(&params)),
            "ping" => Ok(json!({})),
            "tools/list" => Ok(self.list()),
            "tools/call" => self.call(params).await,
            _ => Err((METHOD_NOT_FOUND, format!("no method {method}"))),
        };
        Some(match outcome {
            Ok(result) => json!({"jsonrpc": "2.0", "id": id, "result": result}),
            Err((code, message)) => error(id, code, &message),
        })
    }

    /// `initialize`'s result: the client's protocol version, or ours.
    fn initialize(&self, params: &Value) -> Value {
        let version = params
            .get("protocolVersion")
            .and_then(Value::as_str)
            .unwrap_or(respan_gateway_mcp::PROTOCOL);
        json!({
            "protocolVersion": version,
            "capabilities": {"tools": {}},
            "serverInfo": {"name": self.vendor.id, "version": env!("CARGO_PKG_VERSION")},
        })
    }

    /// `tools/list`'s result: every tool, on one page.
    fn list(&self) -> Value {
        let tools: Vec<Value> = self
            .vendor
            .tools
            .list()
            .iter()
            .map(|tool| {
                let schema = serde_json::from_str(tool.input_schema)
                    .unwrap_or_else(|_| json!({"type": "object"}));
                json!({
                    "name": tool.name,
                    "description": tool.description,
                    "inputSchema": schema,
                })
            })
            .collect();
        json!({ "tools": tools })
    }

    /// `tools/call`'s result: the tool's output, or its failure as an
    /// error result the model reads.
    async fn call(&self, params: Value) -> Result<Value, (i64, String)> {
        let name = params
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let Some(tool) = self.vendor.tools.list().iter().find(|t| t.name == name) else {
            return Err((INVALID_PARAMS, format!("no tool {name}")));
        };
        let arguments = match params.get("arguments") {
            Some(Value::Null) | None => json!({}),
            Some(arguments) => arguments.clone(),
        };
        let (output, failed) = match self
            .vendor
            .tools
            .call(&self.api, tool.name, arguments)
            .await
        {
            Ok(output) => (output, false),
            Err(e) => (e.0.into(), true),
        };
        let content: Vec<Value> = output.content.iter().map(Content::to_json).collect();
        let mut result = json!({"content": content, "isError": failed});
        if !output.usage.is_empty() {
            result["_meta"] = json!({ USAGE: output.usage });
        }
        Ok(result)
    }
}

/// A JSON-RPC error response.
fn error(id: Value, code: i64, message: &str) -> Value {
    json!({"jsonrpc": "2.0", "id": id, "error": {"code": code, "message": message}})
}

/// A success of `status` with no body, such as a 202.
fn empty(status: StatusCode) -> http::Response<Body> {
    let mut resp = http::Response::new(Body::full(""));
    *resp.status_mut() = status;
    resp
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{KeyIn, ToolError, ToolOutput, ToolSpec, Tools};
    use async_trait::async_trait;

    struct Echo;

    #[async_trait]
    impl Tools for Echo {
        fn list(&self) -> &'static [ToolSpec] {
            &[
                ToolSpec {
                    name: "echo",
                    description: "Says its text back.",
                    input_schema: r#"{"type":"object","properties":{"text":{"type":"string"}}}"#,
                },
                ToolSpec {
                    name: "fail",
                    description: "Fails.",
                    input_schema: r#"{"type":"object"}"#,
                },
            ]
        }

        async fn call(
            &self,
            _: &Api,
            name: &str,
            arguments: Value,
        ) -> Result<ToolOutput, ToolError> {
            match name {
                "echo" => {
                    let text = arguments["text"].as_str().unwrap_or_default();
                    Ok(ToolOutput::from(text).with_usage("characters", text.len() as f64))
                }
                _ => Err(ToolError::new("it broke")),
            }
        }
    }

    static VENDOR: ToolVendor = ToolVendor {
        id: "echoes",
        key_env: &[],
        key_in: KeyIn::Bearer,
        base_url: "http://127.0.0.1:9",
        tools: &Echo,
    };

    fn server() -> ToolServer {
        ToolServer::new(&VENDOR, VENDOR.api("k", None))
    }

    async fn post(body: Value) -> (StatusCode, Option<Value>) {
        let request = http::Request::post("/")
            .body(Body::full(body.to_string()))
            .expect("a request");
        let (resp, _) = server().send(request).await.expect("an answer");
        let status = resp.status();
        let bytes = resp.into_body().collect().await.expect("a body").to_bytes();
        let json = (!bytes.is_empty()).then(|| serde_json::from_slice(&bytes).expect("JSON"));
        (status, json)
    }

    fn request(id: u64, method: &str, params: Value) -> Value {
        json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params})
    }

    #[tokio::test]
    async fn initialize_takes_the_clients_version() {
        let (status, answer) = post(request(
            1,
            "initialize",
            json!({"protocolVersion": "2025-03-26"}),
        ))
        .await;
        assert_eq!(status, StatusCode::OK);
        let result = &answer.expect("a response")["result"];
        assert_eq!(result["protocolVersion"], "2025-03-26");
        assert_eq!(result["serverInfo"]["name"], "echoes");
        assert_eq!(result["capabilities"], json!({"tools": {}}));
    }

    #[tokio::test]
    async fn tools_are_listed_with_their_schemas() {
        let (_, answer) = post(request(2, "tools/list", json!({}))).await;
        let tools = &answer.expect("a response")["result"]["tools"];
        assert_eq!(tools[0]["name"], "echo");
        assert_eq!(
            tools[0]["inputSchema"]["properties"]["text"]["type"],
            "string"
        );
        assert_eq!(tools[1]["name"], "fail");
    }

    #[tokio::test]
    async fn a_call_answers_with_the_tools_text() {
        let params = json!({"name": "echo", "arguments": {"text": "hi"}});
        let (_, answer) = post(request(3, "tools/call", params)).await;
        let answer = answer.expect("a response");
        assert_eq!(answer["id"], 3);
        assert_eq!(
            answer["result"],
            json!({
                "content": [{"type": "text", "text": "hi"}],
                "isError": false,
                "_meta": {"respan.ai/usage": {"characters": 2.0}},
            })
        );
    }

    #[tokio::test]
    async fn a_failed_call_is_an_error_result() {
        let (_, answer) = post(request(4, "tools/call", json!({"name": "fail"}))).await;
        assert_eq!(
            answer.expect("a response")["result"],
            json!({"content": [{"type": "text", "text": "it broke"}], "isError": true})
        );
    }

    #[tokio::test]
    async fn an_unknown_tool_or_method_is_an_rpc_error() {
        let (_, answer) = post(request(5, "tools/call", json!({"name": "nope"}))).await;
        assert_eq!(answer.expect("a response")["error"]["code"], INVALID_PARAMS);
        let (_, answer) = post(request(6, "resources/list", json!({}))).await;
        assert_eq!(
            answer.expect("a response")["error"]["code"],
            METHOD_NOT_FOUND
        );
    }

    #[tokio::test]
    async fn a_notification_gets_202_and_no_body() {
        let note = json!({"jsonrpc": "2.0", "method": "notifications/initialized"});
        assert_eq!(post(note).await, (StatusCode::ACCEPTED, None));
    }

    #[tokio::test]
    async fn a_batch_gets_its_requests_responses() {
        let batch = json!([
            request(7, "ping", json!({})),
            {"jsonrpc": "2.0", "method": "notifications/initialized"},
            request(8, "tools/call", json!({"name": "echo", "arguments": {"text": "x"}})),
        ]);
        let (status, answer) = post(batch).await;
        assert_eq!(status, StatusCode::OK);
        let answer = answer.expect("responses");
        let answers = answer.as_array().expect("an array");
        assert_eq!(answers.len(), 2);
        assert_eq!(answers[0], json!({"jsonrpc": "2.0", "id": 7, "result": {}}));
        assert_eq!(answers[1]["result"]["content"][0]["text"], "x");
    }

    #[tokio::test]
    async fn get_is_refused_and_delete_ends_nothing() {
        let get = http::Request::get("/")
            .body(Body::full(""))
            .expect("a request");
        let (resp, _) = server().send(get).await.expect("an answer");
        assert_eq!(resp.status(), StatusCode::METHOD_NOT_ALLOWED);
        let delete = http::Request::delete("/")
            .body(Body::full(""))
            .expect("a request");
        let (resp, _) = server().send(delete).await.expect("an answer");
        assert_eq!(resp.status(), StatusCode::NO_CONTENT);
    }
}
