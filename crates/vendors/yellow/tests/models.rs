//! Model lists a vendor lists its own way ([`Vendor::models`]): Cloudflare's
//! account search beside its OpenAI-compatible base.
//!
//! [`Vendor::models`]: respan_gateway_vendor_kit::Vendor::models

#[path = "../../tests/support/mod.rs"]
mod support;

use respan_gateway_vendors_yellow::cloudflare;
use support::{ids, json, listing};

#[tokio::test]
async fn cloudflare_lists_its_models_from_its_account_search() {
    let list = json!({"data": [{"id": "@cf/meta/llama-3.1-8b-instruct", "name": "Llama"}]});
    let path = "/client/v4/accounts/acct/ai/models/search?format=openrouter&per_page=1000";
    let base = listing(path, list).await;
    let base = format!("{base}/client/v4/accounts/acct/ai/v1");
    let provider = cloudflare::VENDOR.provider("cf").base(base).key("k");
    let models = provider.models().await.expect("a list").expect("models");
    assert_eq!(ids(models), ["@cf/meta/llama-3.1-8b-instruct"]);
}
