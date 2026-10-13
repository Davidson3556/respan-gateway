//! Model lists a vendor lists its own way ([`Vendor::models`]): Anthropic's
//! with its API's version, TypeSafe's `{"models": [{"name"}]}`, and
//! Cloudflare's account search beside its OpenAI-compatible base. Gemini's
//! is its quirk's, in `gemini.rs`.
//!
//! [`Vendor::models`]: respan_gateway_vendors::Vendor::models

mod support;

use respan_gateway_vendors::{anthropic, cloudflare, typesafe};
use support::{ids, json, listing};

#[tokio::test]
async fn anthropic_lists_its_models_at_its_versioned_path() {
    let list = json!({"data": [{"id": "claude-sonnet-4-5", "type": "model"}], "has_more": false});
    let base = listing("/v1/models?limit=1000", list).await;
    let provider = anthropic::VENDOR.provider("a").base(base).key("k");
    let models = provider.models().await.expect("a list").expect("models");
    assert_eq!(ids(models), ["claude-sonnet-4-5"]);
}

#[tokio::test]
async fn typesafe_lists_its_models_by_name() {
    let list = json!({"models": [
        {"name": "jev-latest", "description": "newest", "release_date": "2026-09-01"},
        {"name": "jev-1.13.0", "description": "pinned", "release_date": "2026-08-01"},
    ]});
    let base = listing("/v1/models", list).await;
    let provider = typesafe::VENDOR
        .provider("ts")
        .base(format!("{base}/v1"))
        .key("k");
    let models = provider.models().await.expect("a list").expect("models");
    assert_eq!(ids(models), ["jev-latest", "jev-1.13.0"]);
}

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
