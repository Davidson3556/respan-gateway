//! A conversation the gateway kept, crossing to OpenAI's own Responses (as a
//! fallback's may): sent whole, without the id OpenAI does not know.

#![allow(clippy::unwrap_used)]

use std::time::Duration;

use mockllm::{Answer, FakeOpenAi};
use respan_gateway_conversation::{Input, Output};
use respan_gateway_conversation_http::openai::OpenAiChat;
use respan_gateway_conversation_http::responses::OpenAiResponses;
use respan_gateway_core::{ClientContext, Fault, Gateway, Plugin, Provider, Response, async_trait};
use respan_gateway_http::http::{HeaderMap, HeaderValue};
use respan_gateway_http_transport::HttpTransport;
use respan_gateway_plugin_responses::Responses;
use serde_json::{Value, json};

#[allow(dead_code)]
#[path = "../../../../tests/support/http.rs"]
mod http_support;
use http_support::{post, read_whole, serve};

/// Targets the provider the model names.
struct Target;

#[async_trait]
impl Plugin for Target {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let model = cx.input::<Input>().ok().and_then(|i| i.model.clone());
        cx.set_target(model.unwrap_or_default());
        Ok(None)
    }
}

async fn respond(gw: &Gateway, body: Value) -> Value {
    let (resp, _) = serve(gw, post("/v1/responses", body.to_string())).await;
    let (status, _, body) = read_whole(resp).await;
    gw.idle().await;
    let body: Value = serde_json::from_slice(&body.unwrap()).unwrap();
    assert!(status.is_success(), "{body}");
    body
}

#[tokio::test]
async fn a_kept_conversation_reaches_openai_whole() {
    let openai = FakeOpenAi::start().await;
    let mut headers = HeaderMap::new();
    headers.insert("authorization", HeaderValue::from_static("Bearer sk"));
    let to = || {
        let transport = HttpTransport::new(openai.base_url()).unwrap();
        transport.with_headers(headers.clone()).unwrap().connect()
    };
    let gw = Gateway::new()
        .provider(Provider::new("chat", OpenAiChat, to()))
        .and_then(|g| g.provider(Provider::new("responses", OpenAiResponses, to())))
        .and_then(|g| g.client::<Input, Output, _>(OpenAiResponses))
        .map(|g| {
            g.plugin(Responses::new(Duration::from_secs(60)))
                .plugin(Target)
        })
        .unwrap();
    openai.answer(Answer::text("reply-1"));
    openai.answer(Answer::text("reply-2"));
    let first = respond(&gw, json!({ "model": "chat", "input": "one" })).await;
    let id = first["id"].as_str().unwrap();
    assert!(id.starts_with("resp_"), "{first}");
    let next = json!({ "model": "responses", "input": "two", "previous_response_id": id });
    respond(&gw, next).await;

    let sent = &openai.requests()[1].body;
    assert!(sent.get("previous_response_id").is_none(), "{sent}");
    let input = sent["input"].to_string();
    for said in ["one", "reply-1", "two"] {
        assert!(input.contains(said), "{input}");
    }
}
