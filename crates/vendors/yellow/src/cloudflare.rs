//! Cloudflare Workers AI: Chat Completions and embeddings, as OpenAI serves
//! them, at an account's own base URL.

use respan_gateway_conversation_http::openai::OpenAiChat;
use respan_gateway_media_http::openai::OpenAiEmbeddings;

use respan_gateway_vendor_kit::{
    Body, BoxFuture, Models, Vendor, VendorProvider, built_wrong, http, json, models,
};

/// Where its API is.
const BASE_URL: &str =
    "https://api.cloudflare.com/client/v4/accounts/{CLOUDFLARE_ACCOUNT_ID}/ai/v1";

/// Cloudflare Workers AI.
pub const VENDOR: Vendor = Vendor {
    id: "cloudflare",
    key_env: &["CLOUDFLARE_API_TOKEN"],
    formats: &[
        (&OpenAiChat, Some(BASE_URL)),
        (&OpenAiEmbeddings, Some(BASE_URL)),
    ],
    models: list_models,
    ..Vendor::DEFAULT
};

/// Its models: its account API's search, its OpenAI-compatible base
/// without `/v1`, in OpenRouter's form, as OpenAI lists them.
fn list_models(p: &VendorProvider) -> BoxFuture<'_, Models> {
    Box::pin(async move {
        let search = "/models/search?format=openrouter&per_page=1000";
        let call = http::Request::get(search).body(Body::default());
        let base = p.base_url_of("openai-chat").unwrap_or(BASE_URL);
        let base = base.trim_end_matches('/');
        let account = p.client_at(base.strip_suffix("/v1").unwrap_or(base))?;
        let (answer, _) = account.send(call.map_err(built_wrong)?).await?;
        Ok(models::listed(&json(answer).await?))
    })
}
