use super::*;
use respan_gateway_examples::Meter;
use respan_gateway_http::{Status, http};
use respan_gateway_providers::AnthropicMessages;
use respan_gateway_sdk::conversation::Conversation;
use respan_gateway_sdk::{Plugin, ProviderContext, async_trait};

async fn ask(gw: &Gateway, req: &'static str) -> Result<(http::HeaderMap, String), Fault> {
    let (status, headers, body) =
        read_whole(serve(gw, post("/v1/chat/completions", req)).await.0).await;
    assert_eq!(status, http::StatusCode::OK);
    Ok((headers, String::from_utf8_lossy(&body?).into_owned()))
}

#[tokio::test]
async fn answers_json_or_sse_as_asked() -> Result<(), Fault> {
    let gw = gateway()?;
    let (headers, json) = ask(&gw, UNARY).await?;
    assert_eq!(headers["content-type"], "application/json");
    let v: serde_json::Value =
        serde_json::from_str(&json).map_err(|e| Fault::internal(e.to_string()))?;
    assert_eq!(
        v["choices"][0]["message"]["content"],
        "you said: hello there"
    );
    let (headers, sse) = ask(&gw, STREAM).await?;
    assert_eq!(headers["content-type"], "text/event-stream");
    assert!(sse.contains(r#""content":"hello ""#), "{sse}");
    assert!(sse.ends_with("data: [DONE]\n\n"), "{sse}");
    Ok(())
}

#[tokio::test]
async fn the_same_model_answers_in_anthropics_format() -> Result<(), Fault> {
    let model: Model = Arc::new(echo);
    let gw = Gateway::new()
        .provider::<Conversation, _, _>("local", Local, move |_| model.clone())?
        .client::<Conversation, _, _>(AnthropicMessages)?
        .target("local")?;
    let req = r#"{"model":"echo","max_tokens":9,"stream":true,"messages":[{"role":"user","content":"hi"}]}"#;
    let (_, _, body) = read_whole(serve(&gw, post("/v1/messages", req)).await.0).await;
    let sse = String::from_utf8_lossy(&body?).into_owned();
    assert!(
        sse.contains("event: content_block_delta") && sse.contains(r#""text":"said: ""#),
        "{sse}"
    );
    Ok(())
}

/// Refuses every call to "local", as a limit or guard would.
struct Closed;

#[async_trait]
impl Plugin for Closed {
    async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
        if px.provider() == "local" {
            return Err(Fault::new("limit", "closed").with(Status(429)));
        }
        Ok(())
    }
}

#[tokio::test]
async fn the_provider_edge_sees_and_bills_its_calls() -> Result<(), Fault> {
    let meter = Meter::default();
    let gw = gateway()?.plugin(meter.clone());
    ask(&gw, STREAM).await?;
    gw.idle().await;
    let usage = meter.usage();
    assert_eq!(usage.get(units::INPUT_TOKENS), Some(2.0));
    assert_eq!(usage.get(units::OUTPUT_TOKENS), Some(4.0));

    let gw = gateway()?.plugin(Closed);
    let (status, _, _) = read_whole(serve(&gw, post("/v1/chat/completions", UNARY)).await.0).await;
    assert_eq!(status, http::StatusCode::TOO_MANY_REQUESTS);
    Ok(())
}
