//! Cloudflare Workers AI: Chat Completions and embeddings, as OpenAI serves
//! them, at an account's own base URL.

use respan_gateway_conversation_http::openai::OpenAiChat;
use respan_gateway_media_http::openai::OpenAiEmbeddings;

use crate::{
    Base, Body, BoxFuture, Model, Models, Speaks, Vendor, VendorProvider, built_wrong, http, json,
};

/// Cloudflare Workers AI.
pub const VENDOR: Vendor = Vendor {
    id: "cloudflare",
    base_url: Some("https://api.cloudflare.com/client/v4/accounts/{CLOUDFLARE_ACCOUNT_ID}/ai/v1"),
    key_env: &["CLOUDFLARE_API_TOKEN"],
    formats: &[&Speaks::http(OpenAiChat), &Speaks::http(OpenAiEmbeddings)],
    models: list_models,
    ..Vendor::DEFAULT
};

/// Its models: its account API's search, beside its OpenAI-compatible
/// base, in OpenRouter's form, as OpenAI lists them.
fn list_models(p: &VendorProvider) -> BoxFuture<'_, Models> {
    Box::pin(async move {
        let search = "/models/search?format=openrouter&per_page=1000";
        let call = http::Request::get(search).body(Body::default());
        let account = p.client_at(Base::Without("/v1"))?;
        let (answer, _) = account.send(call.map_err(built_wrong)?).await?;
        Ok(Model::openai(&json(answer).await?))
    })
}
