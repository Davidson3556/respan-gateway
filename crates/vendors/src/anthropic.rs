//! Anthropic: Messages.

use respan_gateway_conversation_http::anthropic::AnthropicMessages;

use serde_json::Value;

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
        let answer = json(answer).await?;
        let models = answer["data"].as_array().into_iter().flatten();
        Ok(models.filter_map(model).collect())
    })
}

/// One of its models, as its list says it: `display_name`, its limits
/// (`max_input_tokens`, `max_tokens`), what it reads and can do
/// (`capabilities`), and when it retires.
fn model(m: &Value) -> Option<Model> {
    let mut model = Model::new(m["id"].as_str()?);
    model.display_name = m["display_name"].as_str().map(str::to_owned);
    model.context_window = m["max_input_tokens"].as_u64();
    model.max_output_tokens = m["max_tokens"].as_u64();
    model.retires_at = m["retires_at"].as_str().map(str::to_owned);
    let can = |name: &str| m["capabilities"][name]["supported"] == true;
    for (name, modality) in [("image_input", "image"), ("pdf_input", "pdf")] {
        if can(name) {
            model.input_modalities.push(modality.to_owned());
        }
    }
    // Every model of its Messages API calls tools.
    model.capabilities.push("tools".to_owned());
    for (name, capability) in [
        ("thinking", "reasoning"),
        ("structured_outputs", "structured_outputs"),
    ] {
        if can(name) {
            model.capabilities.push(capability.to_owned());
        }
    }
    Some(model)
}
