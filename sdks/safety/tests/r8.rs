//! Probes of the round-8 core: the model is part of the conversation input,
//! so a model allowlist is an input filter on the provider edge, and a
//! failed filter's releases are dropped (Z8-1).

mod common;

use std::collections::HashSet;
use std::sync::Arc;

use common::*;
use respan_gateway_core::http::StatusCode;
use respan_gateway_core::{
    ClientContext, Fault, Plugin, ProviderContext, Response, async_trait, codes,
};
use respan_gateway_formats::conversation::{Conversation, set_model};

/// Refuses any call whose final model is not allowed. As a provider-edge
/// input filter it sees the model the call sends: the client's, or one a
/// request filter or the call's own filter set. A call it cannot read the
/// model of (another kind) is refused: it fails closed.
struct Allow(Arc<HashSet<&'static str>>);

#[async_trait]
impl Plugin for Allow {
    async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
        if !px.is_kind::<Conversation>() {
            return Err(Fault::new(
                StatusCode::FORBIDDEN,
                "model_denied",
                "no model to check",
            ));
        }
        let allowed = self.0.clone();
        px.filter(move |chat: &mut Conversation| {
            match chat.model.as_deref().filter(|m| allowed.contains(m)) {
                Some(_) => Ok(()),
                None => Err(Fault::new(
                    StatusCode::FORBIDDEN,
                    "model_denied",
                    "model not allowed",
                )),
            }
        })
    }
}

fn allow(models: &[&'static str]) -> Arc<dyn Plugin> {
    plugin(Allow(Arc::new(models.iter().copied().collect())))
}

/// Calls the target asking for `gpt-big` through the call's own filter.
struct BigCall;

#[async_trait]
impl Plugin for BigCall {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let call = cx.call("openai").with_filter(set_model("gpt-big"));
        call.await.map(Some)
    }
}

#[tokio::test]
async fn an_allowed_model_goes_through() {
    let h = Harness::new(vec![("allow", allow(&["gpt-x"]))]);
    h.reply(OPENAI, oa_whole("ok"));
    let (status, _, _) = h.text(openai_request("hi", false)).await;
    assert_eq!(status, StatusCode::OK);
}

/// A model a call's own filter set is what the allowlist checks, though
/// the allowlist was registered first: a call's filters see the inputs
/// before any plugin's provider edge.
#[tokio::test]
async fn a_model_set_by_a_calls_own_filter_is_checked() {
    let h = Harness::new(vec![("allow", allow(&["gpt-x"])), ("big", plugin(BigCall))]);
    h.reply(OPENAI, oa_whole("ok"));
    let (status, _, _) = h.text(openai_request("hi", false)).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert!(h.requests_to(OPENAI).is_empty());
}

/// Asks every call for `gpt-big` from the provider edge.
struct BigEdge;

#[async_trait]
impl Plugin for BigEdge {
    async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
        px.filter(set_model("gpt-big"))
    }
}

/// Plugin order still decides: a provider-edge filter added after the
/// allowlist's changes the model past it (the F2 pattern, now for models).
#[tokio::test]
async fn a_later_provider_edge_filter_changes_the_model_past_the_allowlist() {
    let h = Harness::new(vec![("allow", allow(&["gpt-x"])), ("big", plugin(BigEdge))]);
    h.reply(OPENAI, oa_whole("ok"));
    let (status, _, _) = h.text(openai_request("hi", false)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(h.sent_to(OPENAI)[0]["model"], "gpt-big");
}

/// Another kind has no model the allowlist can read: MCP calls are refused.
#[tokio::test]
async fn a_call_of_another_kind_is_refused_as_it_names_no_model() {
    let h = Harness::build(vec![("allow", allow(&["gpt-x"]))], |gw| gw.target("mcp"));
    let (status, _, outcome) = h
        .text(mcp_request(
            "search",
            respan_gateway_core::serde_json::json!({}),
        ))
        .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_ne!(
        outcome.end.err().map(|f| f.code().to_owned()),
        Some(codes::INTERNAL.to_owned())
    );
}
