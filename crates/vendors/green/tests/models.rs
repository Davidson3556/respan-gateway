//! Model lists a vendor lists its own way ([`Vendor::models`]): Anthropic's
//! with its API's version, TypeSafe's `{"models": [{"name"}]}`, and
//! Respan's catalog, a page at a time. Gemini's is tested in `gemini.rs`.
//!
//! [`Vendor::models`]: respan_gateway_vendor_kit::Vendor::models

#[path = "../../tests/support/mod.rs"]
mod support;

use respan_gateway_vendor_kit::{Capability, Modality, Model};
use respan_gateway_vendors_green::{anthropic, respan, typesafe};
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

#[tokio::test]
async fn respan_lists_its_catalog_a_page_at_a_time_sent_no_key() {
    use std::sync::{Arc, Mutex};
    let asked = Arc::new(Mutex::new(Vec::new()));
    let seen = asked.clone();
    let app = axum::Router::new().fallback(move |req: axum::extract::Request| {
        let seen = seen.clone();
        async move {
            let at = req
                .uri()
                .path_and_query()
                .map_or("", |p| p.as_str())
                .to_owned();
            let keyed = req.headers().contains_key("authorization");
            seen.lock()
                .unwrap_or_else(|e| e.into_inner())
                .push((at.clone(), keyed));
            let page = match at.as_str() {
                "/api/gateway-models/list?page=1&page_size=1000" => json!({"models": [
                    {"model_name": "gpt-6-luna", "display_name": "GPT-6 Luna",
                     "provider": {"provider_id": "openai"}, "model_description": "", "max_context_window": 400000,
                     "input_cost": 0.1, "output_cost": 0.5, "cache_hit_input_cost": 0.01,
                     "metadata": {"release_date": "2026-09-01"},
                     "capability": {"function_call": true, "reasoning_support": true,
                        "supported_parameters": ["tools", "structured_outputs"],
                        "max_completion_tokens": 128000, "images": true}},
                    {"model_name": "gpt-6-luna:batch", "input_cost": 0.05, "output_cost": 0.25},
                ], "hasNext": true}),
                "/api/gateway-models/list?page=2&page_size=1000" => json!({"models": [
                    {"model_name": "span-02", "display_name": "Span-02",
                     "input_cost": 0, "output_cost": 0, "capability": {"function_call": false}},
                    {"model_name": "gemini-x", "provider": {"provider_id": "google_gemini_ai"}},
                ], "hasNext": false}),
                _ => json!({}),
            };
            axum::Json(page)
        }
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("a port");
    let base = format!("http://{}/api", listener.local_addr().expect("an address"));
    tokio::spawn(async move { axum::serve(listener, app).await });

    let provider = respan::VENDOR.provider("r").base(base).key("k");
    let models = provider.models().await.expect("a list").expect("models");
    let luna = &models[0];
    assert_eq!(luna.display_name.as_deref(), Some("GPT-6 Luna"));
    assert_eq!(luna.description, None);
    assert_eq!(
        (luna.context_window, luna.max_output_tokens),
        (Some(400000), Some(128000))
    );
    assert_eq!(
        luna.capabilities,
        [
            Capability::Tools,
            Capability::Reasoning,
            Capability::StructuredOutputs
        ]
    );
    assert_eq!(luna.input_modalities, [Modality::Image]);
    let price = luna.price.expect("a price");
    assert_eq!(
        (price.input, price.output, price.cached_input),
        (0.1, 0.5, Some(0.01))
    );
    assert_eq!(
        luna.created.map(|t| t.to_rfc3339()),
        Some("2026-09-01T00:00:00+00:00".into())
    );
    // A batch-only model is no model a call names; a free one has no price.
    assert_eq!(models[1].price, None);
    let makers: Vec<_> = models.iter().map(|m| m.maker.as_deref()).collect();
    assert_eq!(makers, [Some("openai"), None, Some("google")]);
    // Its own endpoint serves a maker's models; Chat Completions any, and
    // one of no maker, as one it does not list.
    let ask = |format| respan_gateway_vendor_kit::Ask {
        kind: "chat",
        format,
        model: None,
        client: None,
    };
    let format_for = respan::VENDOR.format_for.expect("a format_for");
    let chat = Some("openai-chat");
    assert_eq!(format_for(&ask("gemini-generate"), &models[2]), None);
    assert_eq!(format_for(&ask("anthropic-messages"), &models[2]), chat);
    assert_eq!(format_for(&ask("anthropic-messages"), &models[1]), chat);
    assert_eq!(
        format_for(&ask("anthropic-messages"), &Model::new("x")),
        chat
    );
    // An OpenAI model only Responses serves well goes there from any client.
    assert_eq!(
        format_for(&ask("openai-chat"), &models[0]),
        Some("openai-responses")
    );
    assert_eq!(ids(models), ["gpt-6-luna", "span-02", "gemini-x"]);
    let asked = asked.lock().unwrap_or_else(|e| e.into_inner()).clone();
    assert_eq!(asked.len(), 2, "{asked:?}");
    assert!(asked.iter().all(|(_, keyed)| !keyed), "{asked:?}");
}
