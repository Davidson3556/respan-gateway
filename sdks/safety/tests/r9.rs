//! Probes of the round-9 core: a call's own filters all end when a later
//! one fails to be added (K9-5), and a head edit that fails refuses the
//! request (`edit_response_head` returns `Result`).

mod common;

use std::sync::{Arc, Mutex, PoisonError};

use common::*;
use respan_gateway_core::http::StatusCode;
use respan_gateway_core::{ClientContext, Fault, Filter, Plugin, Response, async_trait, codes};
use respan_gateway_formats::conversation::Reply;

/// Holds every output back until its end, recording each end it is told.
#[derive(Clone, Default)]
struct Holder {
    held: Vec<Reply>,
    ends: Arc<Mutex<Vec<Result<(), String>>>>,
}

impl Filter<Reply> for Holder {
    fn edit(&mut self, value: Reply, _out: &mut Vec<Reply>) -> Result<(), Fault> {
        self.held.push(value);
        Ok(())
    }

    fn end(&mut self, end: &Result<(), Fault>, out: &mut Vec<Reply>) -> Result<(), Fault> {
        let end = end.as_ref().map(drop).map_err(|f| f.code().to_owned());
        self.ends
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(end);
        out.append(&mut self.held);
        Ok(())
    }
}

/// Calls the target with a holder, then a filter of a type that is not the
/// call's kind, so the call fails.
struct BadSecondFilter(Holder);

#[async_trait]
impl Plugin for BadSecondFilter {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let call = cx
            .call("openai")
            .with_filter(self.0.clone())
            .with_filter(|_: &mut String| Ok(()));
        call.await.map(Some)
    }
}

/// K9-5 fixed: the holder, added first, is told the call's end though the
/// filter after it was never added.
#[tokio::test]
async fn a_calls_first_filter_ends_when_its_second_is_refused() {
    let holder = Holder::default();
    let h = Harness::new(vec![("bad", plugin(BadSecondFilter(holder.clone())))]);

    let (status, _, _) = h.text(openai_request("hi", false)).await;

    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert!(h.mock.sent().is_empty());
    let ends = holder.ends.lock().unwrap_or_else(PoisonError::into_inner);
    assert_eq!(*ends, [Err(codes::INTERNAL.to_owned())]);
}

/// Refuses any response whose head lacks a policy header, from the head
/// edit: a response-level check with no body filter.
struct NeedsPolicy;

#[async_trait]
impl Plugin for NeedsPolicy {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        cx.edit_response_head(|head| match head.headers.contains_key("x-policy") {
            true => Ok(()),
            false => Err(Fault::new(
                StatusCode::FORBIDDEN,
                "policy_missing",
                "the provider sent no policy",
            )),
        });
        Ok(None)
    }
}

/// A failing head edit refuses a streamed response before any byte, in the
/// client's framing, and the provider's stream is dropped.
#[tokio::test]
async fn a_failing_head_edit_refuses_a_stream_in_the_clients_format() {
    let h = Harness::new(vec![("policy", plugin(NeedsPolicy))]);
    h.reply(OPENAI, oa_stream(&["Sure"]));

    let (status, body, outcome) = h.text(openai_request("hi", true)).await;

    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    assert!(!body.contains("Sure"), "{body}");
    assert!(body.contains("\"policy_missing\""), "{body}");
    assert_eq!(
        outcome.end.as_ref().map_err(|f| f.code()),
        Err("policy_missing")
    );
}

/// The same refusal for an Anthropic client: its own error framing.
#[tokio::test]
async fn a_failing_head_edit_refuses_in_anthropic_framing() {
    let h = Harness::build(vec![("policy", plugin(NeedsPolicy))], |gw| {
        gw.target("anthropic")
    });
    h.reply(ANTHROPIC, an_stream(&["Sure"]));

    let (status, body, _) = h.text(anthropic_request("hi", true)).await;

    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    assert!(body.contains("\"type\":\"error\""), "{body}");
}
