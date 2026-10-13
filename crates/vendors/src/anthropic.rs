//! Anthropic: Messages.

use respan_gateway_conversation_http::anthropic::AnthropicMessages;

use crate::{
    Body, BoxFuture, Model, Models, Speaks, Vendor, VendorProvider, built_wrong, http, json,
};

/// Anthropic. Its base URL leaves out the version.
pub const VENDOR: Vendor = Vendor {
    id: "anthropic",
    base_url: Some("https://api.anthropic.com"),
    key_env: &["ANTHROPIC_API_KEY"],
    formats: &[&Speaks::http(AnthropicMessages).key_in("x-api-key")],
    models: list_models,
    ..Vendor::DEFAULT
};

/// Its models: `GET /v1/models`, which needs its API's version.
fn list_models(p: &VendorProvider) -> BoxFuture<'_, Models> {
    Box::pin(async move {
        let call = http::Request::get("/v1/models?limit=1000")
            .header("anthropic-version", "2023-06-01")
            .body(Body::default());
        let (answer, _) = p.client()?.send(call.map_err(built_wrong)?).await?;
        Ok(Model::openai(&json(answer).await?))
    })
}
