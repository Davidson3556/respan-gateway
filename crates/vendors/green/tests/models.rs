//! Model lists a vendor lists its own way ([`Vendor::models`]): Anthropic's
//! with its API's version, and TypeSafe's `{"models": [{"name"}]}`. Gemini's
//! is tested in `gemini.rs`.
//!
//! [`Vendor::models`]: respan_gateway_vendor_kit::Vendor::models

#[path = "../../tests/support/mod.rs"]
mod support;

use respan_gateway_vendor_kit::{Capability, Modality};
use respan_gateway_vendors_green::{anthropic, typesafe};
use support::{ids, json, listing};

#[tokio::test]
async fn anthropic_lists_its_models_at_its_versioned_path() {
    let list = json!({"data": [{"id": "claude-sonnet-4-5", "type": "model",
        "display_name": "Claude Sonnet 4.5", "max_input_tokens": 1000000, "max_tokens": 64000,
        "created_at": "2025-09-29T00:00:00Z", "retires_at": "2026-10-23T00:00:00Z",
        "capabilities": {"image_input": {"supported": true}, "pdf_input": {"supported": true},
            "thinking": {"supported": true}, "structured_outputs": {"supported": false}}}],
        "has_more": false});
    let base = listing("/v1/models?limit=1000", list).await;
    let provider = anthropic::VENDOR.provider("a").base(base).key("k");
    let models = provider.models().await.expect("a list").expect("models");
    let claude = &models[0];
    assert_eq!(claude.display_name.as_deref(), Some("Claude Sonnet 4.5"));
    let at = |t: Option<chrono::DateTime<chrono::Utc>>| t.map(|t| t.to_rfc3339());
    assert_eq!(at(claude.created), Some("2025-09-29T00:00:00+00:00".into()));
    assert_eq!(
        at(claude.retires_at),
        Some("2026-10-23T00:00:00+00:00".into())
    );
    assert_eq!(
        (claude.context_window, claude.max_output_tokens),
        (Some(1000000), Some(64000))
    );
    assert_eq!(
        claude.input_modalities,
        [Modality::Image, Modality::Document]
    );
    assert_eq!(
        claude.capabilities,
        [Capability::Tools, Capability::Reasoning]
    );
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
