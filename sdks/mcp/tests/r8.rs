//! Probes of the round-8 core: `Format::model` is gone, so a passthrough of
//! no kind has no model. A proxy routing Claude Code's traffic by model
//! routes the typed Messages path by the input and the passthrough by the
//! body, with two plugins where the kernel's model served both.

mod common;

use std::sync::Arc;

use common::{send, whole};
use respan_gateway_core::http::{self, StatusCode};
use respan_gateway_core::{Body, ClientContext, Fault, Gateway, Plugin, Response, async_trait};
use respan_gateway_formats::AnthropicMessages;
use respan_gateway_formats::conversation::Conversation;
use respan_gateway_mcp::Passthrough;
use respan_gateway_plugins::ModelRouter;
use respan_gateway_testkit::{MockTransport, mock};

const ANTHROPIC: &str = "http://anthropic.test";
const BIG: &str = "http://big.test";

fn claude(path: &str, body: &'static str) -> http::Request<Body> {
    http::Request::post(path)
        .header("content-type", "application/json")
        .body(whole(body))
        .unwrap_or_else(|e| panic!("{e}"))
}

/// Routes a request of no kind by the `model` of its JSON body: the
/// protocol's spelling, which a format used to know for every plugin.
struct BodyModel(&'static str, &'static str);

#[async_trait]
impl Plugin for BodyModel {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        if !cx.is_kind::<()>() {
            return Ok(None);
        }
        let model = cx.body().json().and_then(|v| v["model"].as_str());
        if model.is_some_and(|m| m.starts_with(self.0)) {
            cx.set_target(self.1);
        }
        Ok(None)
    }
}

fn proxy(mock: &MockTransport, plugins: Vec<Arc<dyn Plugin>>) -> Gateway {
    let raw = Passthrough::new("anthropic-raw", ["/v1/messages/count_tokens"]);
    let mut gw = Gateway::new()
        .provider("anthropic", AnthropicMessages, mock.at(ANTHROPIC))
        .and_then(|g| g.provider("big", AnthropicMessages, mock.at(BIG)))
        .and_then(|g| g.provider("anthropic-raw", raw.clone(), mock.at(ANTHROPIC)))
        .and_then(|g| g.provider("big-raw", raw.clone(), mock.at(BIG)))
        .and_then(|g| g.serve(AnthropicMessages))
        .and_then(|g| g.serve(raw))
        .and_then(|g| g.target("anthropic"))
        .and_then(|g| g.target("anthropic-raw"))
        .unwrap_or_else(|e| panic!("{e}"));
    for (i, p) in plugins.into_iter().enumerate() {
        gw = gw
            .plugin(format!("p{i}"), p)
            .unwrap_or_else(|e| panic!("{e}"));
    }
    gw
}

fn router() -> Arc<dyn Plugin> {
    Arc::new(ModelRouter::<Conversation>::new(|c| c.model.as_deref()).route("claude-big", "big"))
}

const MESSAGE: &str = r#"{"id":"m","type":"message","role":"assistant","content":[],"model":"claude-big","stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":1}}"#;

/// The typed path routes by the input's model.
#[tokio::test]
async fn the_model_router_routes_the_typed_path() {
    let mock = MockTransport::new();
    mock.reply(BIG, mock::json(StatusCode::OK, MESSAGE));
    let gw = proxy(&mock, vec![router()]);

    let body = r#"{"model":"claude-big","max_tokens":1,"messages":[]}"#;
    let (status, ..) = send(&gw, claude("/v1/messages", body)).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(mock.sent()[0].uri().host(), Some("big.test"));
}

/// R8-C: the same model on the passthrough is not routed: the router reads
/// the kind's input, and traffic of no kind has none. Before M8-1 the
/// passthrough's `Format::model` gave every router the model.
#[tokio::test]
async fn today_the_model_router_leaves_the_passthrough_at_its_default() {
    let mock = MockTransport::new();
    mock.reply(
        ANTHROPIC,
        mock::json(StatusCode::OK, r#"{"input_tokens":1}"#),
    );
    let gw = proxy(&mock, vec![router()]);

    let body = r#"{"model":"claude-big","messages":[]}"#;
    let (status, ..) = send(&gw, claude("/v1/messages/count_tokens", body)).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(mock.sent()[0].uri().host(), Some("anthropic.test"));
}

/// The fix in plugin code: a second router that reads the body.
#[tokio::test]
async fn a_body_router_routes_the_passthrough() {
    let mock = MockTransport::new();
    mock.reply(BIG, mock::json(StatusCode::OK, r#"{"input_tokens":1}"#));
    let gw = proxy(
        &mock,
        vec![router(), Arc::new(BodyModel("claude-big", "big-raw"))],
    );

    let body = r#"{"model":"claude-big","messages":[]}"#;
    let (status, ..) = send(&gw, claude("/v1/messages/count_tokens", body)).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(mock.sent()[0].uri().host(), Some("big.test"));
    assert_eq!(mock.sent()[0].body(), body.as_bytes());
}
