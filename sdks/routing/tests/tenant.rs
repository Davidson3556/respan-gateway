//! Per-tenant provider selection and credentials (use case 13).

mod common;

use std::collections::HashMap;

use common::*;
use respan_gateway_core::Gateway;
use respan_gateway_core::http::{HeaderMap, HeaderName, HeaderValue, StatusCode};
use respan_gateway_formats::{AnthropicMessages, OpenAiChat};
use respan_gateway_routing::fallback::Fallback;
use respan_gateway_routing::retry::Backoff;
use respan_gateway_routing::tenant::{ByEndpoint, Tenant, Tenants};
use respan_gateway_testkit::MockTransport;

const AZURE_A: &str = "http://tenant-a.azure.test/v1";
const AZURE_B: &str = "http://tenant-b.azure.test/v1";

fn header(name: &str, value: &str) -> HeaderMap {
    let mut h = HeaderMap::new();
    h.insert(
        HeaderName::from_bytes(name.as_bytes()).expect("name"),
        HeaderValue::from_str(value).expect("value"),
    );
    h
}

fn gateway(mock: &MockTransport, tenants: &Tenants) -> Gateway {
    let azure = ByEndpoint::new()
        .endpoint("a", mock.at(AZURE_A))
        .endpoint("b", mock.at(AZURE_B));
    Gateway::new()
        .provider("openai", OpenAiChat, mock.at(OPENAI))
        .and_then(|g| g.provider("anthropic", AnthropicMessages, mock.at(ANTHROPIC)))
        .and_then(|g| g.provider("azure", OpenAiChat, azure))
        .and_then(|g| g.serve(OpenAiChat)?.serve(AnthropicMessages))
        .and_then(|g| {
            g.plugin("tenants", tenants.clone())?.plugin(
                "fallback",
                Fallback::new(Backoff::once()).then("anthropic", Some("claude-x")),
            )
        })
        .and_then(|g| g.target("openai"))
        .expect("gateway")
}

fn as_tenant(key: &str) -> respan_gateway_core::http::Request<respan_gateway_core::Body> {
    let mut req = chat("gpt-x", false, "hi");
    let bearer = HeaderValue::from_str(&format!("Bearer {key}")).expect("value");
    req.headers_mut().insert("authorization", bearer);
    req
}

fn tenants() -> Tenants {
    let tenants = Tenants::default();
    tenants.set(
        "gw-a",
        Tenant {
            keys: HashMap::from([("openai".into(), header("authorization", "Bearer sk-a"))]),
            ..Tenant::default()
        },
    );
    tenants.set(
        "gw-b",
        Tenant {
            target: Some("anthropic".into()),
            model: Some("claude-b".into()),
            keys: HashMap::from([("anthropic".into(), header("x-api-key", "ak-b"))]),
            ..Tenant::default()
        },
    );
    tenants
}

#[tokio::test]
async fn each_tenant_gets_its_provider_and_key_and_the_gateway_key_never_leaves() {
    let (mock, tenants) = (MockTransport::new(), tenants());
    let gw = gateway(&mock, &tenants);
    mock.reply(OPENAI, json_reply(StatusCode::OK, &openai_json("a")))
        .reply(ANTHROPIC, json_reply(StatusCode::OK, &anthropic_json("b")));

    let (a, _, _) = send(&gw, as_tenant("gw-a")).await;
    let (b, _, _) = send(&gw, as_tenant("gw-b")).await;

    assert_eq!((a, b), (StatusCode::OK, StatusCode::OK));
    let sent = mock.sent();
    assert_eq!(sent[0].uri().host(), Some("openai.test"));
    assert_eq!(sent[0].headers()["authorization"], "Bearer sk-a");
    assert_eq!(sent[1].uri().host(), Some("anthropic.test"));
    assert_eq!(sent[1].headers()["x-api-key"], "ak-b");
    assert_eq!(sent_json(&sent[1])["model"], "claude-b");
    for req in &sent {
        let all: Vec<_> = req.headers().values().collect();
        assert!(
            !all.iter()
                .any(|v| v.to_str().is_ok_and(|v| v.contains("gw-"))),
            "{req:?}"
        );
    }
}

#[tokio::test]
async fn an_unknown_key_is_refused_in_the_clients_format() {
    let (mock, tenants) = (MockTransport::new(), tenants());
    let gw = gateway(&mock, &tenants);

    let (status, _, body) = send(&gw, as_tenant("gw-nobody")).await;

    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert!(body.expect("whole").contains("\"unauthorized\""));
    assert!(mock.sent().is_empty());
}

#[tokio::test]
async fn tenants_change_while_the_gateway_runs() {
    let (mock, tenants) = (MockTransport::new(), tenants());
    let gw = gateway(&mock, &tenants);
    mock.reply(OPENAI, json_reply(StatusCode::OK, &openai_json("a")));
    let (before, _, _) = send(&gw, as_tenant("gw-c")).await;

    tenants.set(
        "gw-c",
        Tenant {
            keys: HashMap::from([("openai".into(), header("authorization", "Bearer sk-c"))]),
            ..Tenant::default()
        },
    );
    let (after, _, _) = send(&gw, as_tenant("gw-c")).await;
    tenants.remove("gw-c");
    let (removed, _, _) = send(&gw, as_tenant("gw-c")).await;

    assert_eq!(before, StatusCode::UNAUTHORIZED);
    assert_eq!(after, StatusCode::OK);
    assert_eq!(removed, StatusCode::UNAUTHORIZED);
    assert_eq!(mock.sent()[0].headers()["authorization"], "Bearer sk-c");
}

#[tokio::test]
async fn a_fallback_to_a_provider_the_tenant_has_no_key_for_is_refused_before_sending() {
    let (mock, tenants) = (MockTransport::new(), tenants());
    let gw = gateway(&mock, &tenants);
    mock.reply(OPENAI, json_reply(StatusCode::SERVICE_UNAVAILABLE, "{}"));

    let (status, _, body) = send(&gw, as_tenant("gw-a")).await;

    // The hook's refusal wins over the provider's 503 (F11).
    assert_eq!(status, StatusCode::FORBIDDEN, "{body:?}");
    assert_eq!(mock.sent().len(), 1);
}

#[tokio::test]
async fn one_provider_name_reaches_each_tenants_own_deployment() {
    let (mock, tenants) = (MockTransport::new(), Tenants::default());
    for (key, endpoint, secret) in [("gw-a", "a", "az-a"), ("gw-b", "b", "az-b")] {
        tenants.set(
            key,
            Tenant {
                target: Some("azure".into()),
                keys: HashMap::from([("azure".into(), header("api-key", secret))]),
                endpoints: HashMap::from([("azure".into(), endpoint.into())]),
                ..Tenant::default()
            },
        );
    }
    let gw = gateway(&mock, &tenants);
    mock.reply(AZURE_A, json_reply(StatusCode::OK, &openai_json("a")))
        .reply(AZURE_B, json_reply(StatusCode::OK, &openai_json("b")));

    let (_, _, a) = send(&gw, as_tenant("gw-a")).await;
    let (_, _, b) = send(&gw, as_tenant("gw-b")).await;

    assert_eq!(
        (openai_text(&a.expect("a")), openai_text(&b.expect("b"))),
        ("a".into(), "b".into())
    );
    let sent = mock.sent();
    assert_eq!(sent[0].uri().host(), Some("tenant-a.azure.test"));
    assert_eq!(sent[0].headers()["api-key"], "az-a");
    assert_eq!(sent[1].headers()["api-key"], "az-b");
}
