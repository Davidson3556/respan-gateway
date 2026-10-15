//! What the vendors' tests share: a gateway serving OpenAI Chat and
//! Anthropic Messages, whose one provider speaks a vendor's format, or Chat
//! Completions with a vendor's plugin, and answers as told.

#![allow(dead_code)]

#[path = "../../../../tests/support/http.rs"]
mod http_support;

use std::sync::{Arc, Mutex};

use respan_gateway_conversation::{Input, Output};
use respan_gateway_conversation_http::anthropic::AnthropicMessages;
use respan_gateway_conversation_http::openai::OpenAiChat;
use respan_gateway_core::{Gateway, Plugin, Provider};
use respan_gateway_http::Body;
use respan_gateway_http::http::{self, header::CONTENT_TYPE};
use respan_gateway_http::http_body_util::BodyExt;
use serde_json::Value;

pub use serde_json::json;

/// A gateway whose provider, with `plugin` as its own, answers every call with `body`
/// of `content_type`, and the requests it was sent.
pub struct Vendor {
    gateway: Gateway,
    sent: Arc<Mutex<Vec<Value>>>,
}

impl Vendor {
    pub fn new(plugin: &'static dyn Plugin, content_type: &'static str, body: String) -> Self {
        Self::speaking(OpenAiChat, plugin, content_type, body)
    }

    /// As [`Vendor::new`], its provider speaking `format`.
    pub fn speaking<F: ProviderFormat>(
        format: F,
        plugin: &'static dyn Plugin,
        content_type: &'static str,
        body: String,
    ) -> Self {
        Self::serving(format, Some(plugin), content_type, body)
    }

    /// A provider speaking `format`, a vendor's own, answering with
    /// `chunks` as a stream, then `[DONE]`.
    pub fn streaming_in<F: ProviderFormat>(format: F, chunks: &[Value]) -> Self {
        Self::serving(format, None, "text/event-stream", stream_of(chunks))
    }

    /// A provider speaking `format`, a vendor's own, answering with `body`,
    /// whole.
    pub fn whole_in<F: ProviderFormat>(format: F, body: Value) -> Self {
        Self::serving(format, None, "application/json", body.to_string())
    }

    fn serving<F: ProviderFormat>(
        format: F,
        plugin: Option<&'static dyn Plugin>,
        content_type: &'static str,
        body: String,
    ) -> Self {
        let sent = Arc::new(Mutex::new(Vec::new()));
        let seen = sent.clone();
        let answer = move |request: http::Request<Body>| {
            let (seen, body) = (seen.clone(), body.clone());
            async move {
                let bytes = request.into_body().collect().await?.to_bytes();
                let json = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
                seen.lock().expect("sent").push(json);
                let answer = http::Response::builder()
                    .header(CONTENT_TYPE, content_type)
                    .body(Body::full(body))
                    .expect("an answer");
                Ok((answer, None))
            }
        };
        let transport = respan_gateway_http::connect(answer, http_support::LIMIT);
        let mut provider = Provider::new("p", format, transport);
        if let Some(plugin) = plugin {
            provider = provider.plugin(plugin);
        }
        let gateway = Gateway::new()
            .provider(provider)
            .and_then(|g| g.client::<Input, _, _>(OpenAiChat))
            .and_then(|g| g.client::<Input, _, _>(AnthropicMessages))
            .and_then(|g| g.target("p"))
            .expect("a gateway");
        Self { gateway, sent }
    }

    /// A provider answering with `chunks` as a stream, then `[DONE]`.
    pub fn streaming(plugin: &'static dyn Plugin, chunks: &[Value]) -> Self {
        Self::new(plugin, "text/event-stream", stream_of(chunks))
    }

    /// A provider answering with `body`, whole.
    pub fn whole(plugin: &'static dyn Plugin, body: Value) -> Self {
        Self::new(plugin, "application/json", body.to_string())
    }

    /// What a client of OpenAI Chat gets for `request`.
    pub async fn openai(&self, request: Value) -> String {
        self.send("/v1/chat/completions", request).await
    }

    /// What a client of Anthropic Messages gets for `request`.
    pub async fn anthropic(&self, request: Value) -> String {
        self.send("/v1/messages", request).await
    }

    /// The requests the provider was sent.
    pub fn sent(&self) -> Vec<Value> {
        self.sent.lock().expect("sent").clone()
    }

    async fn send(&self, path: &str, request: Value) -> String {
        let call = http_support::post(path, request.to_string());
        let (answer, _) = http_support::serve(&self.gateway, call).await;
        let (status, _, body) = http_support::read_whole(answer).await;
        let body = body.expect("a body");
        let body = String::from_utf8_lossy(&body).into_owned();
        assert!(status.is_success(), "{status}: {body}");
        self.gateway.idle().await;
        body
    }
}

/// A provider format of the conversation kind.
pub trait ProviderFormat:
    respan_gateway_core::Format<respan_gateway_http::ProviderIo, Output, Input>
{
}

impl<F: respan_gateway_core::Format<respan_gateway_http::ProviderIo, Output, Input>> ProviderFormat
    for F
{
}

/// `chunks` as a stream, then `[DONE]`.
fn stream_of(chunks: &[Value]) -> String {
    let mut stream: String = chunks.iter().map(|c| format!("data: {c}\n\n")).collect();
    stream.push_str("data: [DONE]\n\n");
    stream
}

/// An OpenAI Chat request, streamed if `stream`, asking for usage.
pub fn chat(stream: bool) -> Value {
    json!({ "model": "m", "stream": stream, "stream_options": { "include_usage": true },
        "messages": [{ "role": "user", "content": "hi" }] })
}

/// An Anthropic Messages request, streamed if `stream`.
pub fn messages(stream: bool) -> Value {
    json!({ "model": "m", "max_tokens": 10, "stream": stream,
        "messages": [{ "role": "user", "content": "hi" }] })
}

/// The JSON of each data line of a stream, but `[DONE]`.
pub fn data(stream: &str) -> Vec<Value> {
    let lines = stream.lines().filter_map(|l| l.strip_prefix("data: "));
    lines.filter_map(|d| serde_json::from_str(d).ok()).collect()
}

/// A server that answers `GET <path>` carrying the key `k` with `body`, and
/// 404 to anything else; its base URL.
pub async fn listing(path: &'static str, body: Value) -> String {
    use axum::response::IntoResponse;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("a port");
    let base = format!("http://{}", listener.local_addr().expect("an address"));
    let app = axum::Router::new().fallback(move |req: axum::extract::Request| {
        let body = body.clone();
        async move {
            let asked = req.uri().path_and_query().map_or("", |p| p.as_str());
            let keyed = req.headers().values().any(|v| v == "Bearer k" || v == "k");
            match asked == path && keyed {
                true => axum::Json(body).into_response(),
                false => http::StatusCode::NOT_FOUND.into_response(),
            }
        }
    });
    tokio::spawn(async move { axum::serve(listener, app).await });
    base
}

/// The ids of `models`.
pub fn ids(models: Vec<respan_gateway_vendor_kit::Model>) -> Vec<String> {
    models.into_iter().map(|m| m.id).collect()
}
