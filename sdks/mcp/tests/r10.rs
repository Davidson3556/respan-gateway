//! Probes of the round-10 core: a bodiless response read by `ready()` and
//! then dropped ends its call as read, not abandoned (T10-2), and so does
//! one dropped unread.

mod common;

use std::sync::{Arc, Mutex, PoisonError};

use common::*;
use respan_gateway_core::http::{Method, StatusCode};
use respan_gateway_core::{
    ClientContext, Fault, Gateway, Outcome, Plugin, ProviderContext, Response, async_trait, codes,
};
use respan_gateway_mcp::StreamableHttp;
use respan_gateway_testkit::MockTransport;

/// Records how each call ended.
#[derive(Clone, Default)]
struct CallEnds(Arc<Mutex<Vec<Result<(), Fault>>>>);

impl CallEnds {
    fn codes(&self) -> Vec<Option<String>> {
        let ends = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        let code = |e: &Result<(), Fault>| e.as_ref().err().map(|f| f.code().to_owned());
        ends.iter().map(code).collect()
    }
}

#[async_trait]
impl Plugin for CallEnds {
    async fn on_provider_end(&self, _px: &ProviderContext, outcome: &Outcome) {
        let mut ends = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        ends.push(outcome.end.clone());
    }
}

/// Relays the client's request to the server, checks the server took it
/// (`ready()`), then answers with its own 204: the server's is dropped.
struct CheckThenAnswer(bool);

#[async_trait]
impl Plugin for CheckThenAnswer {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let resp = cx.call("a").await?;
        if self.0 {
            let resp = resp.ready().await?;
            if !resp.head().status.is_success() {
                return Err(Fault::upstream("the server refused"));
            }
        }
        Ok(Some(
            Response::stream::<respan_gateway_mcp::ToClient, _>(
                respan_gateway_core::futures::stream::empty(),
            )
            .with_status(StatusCode::NO_CONTENT),
        ))
    }
}

async fn ends_of(ready: bool) -> Vec<Option<String>> {
    let mock = MockTransport::new();
    mock.reply("http://a.test", bare(StatusCode::NO_CONTENT));
    let ends = CallEnds::default();
    let providers =
        Gateway::new().provider("a", StreamableHttp::default(), mock.at("http://a.test"));
    let gw = gateway(
        providers,
        StreamableHttp::default(),
        vec![Arc::new(ends.clone()), Arc::new(CheckThenAnswer(ready))],
        "a",
    );
    let req = mcp(Method::DELETE, "", &[("mcp-session-id", "s1")]);
    let (status, ..) = send(&gw, req).await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    gw.idle().await;
    ends.codes()
}

/// T10-2: the server's 204 to a `DELETE`, read by `ready()` then dropped,
/// ends its call `Ok`: nothing of it was left unread.
#[tokio::test]
async fn a_204_read_by_ready_then_dropped_ends_its_call_ok() {
    assert_eq!(ends_of(true).await, [None]);
}

/// Dropped unread, it ends `Ok` too: a bodiless response's end is decoded
/// as it arrives, so it is read to its end before any plugin sees it and
/// there is nothing to abandon ("dropping it before it is ... read to its
/// end ends its call abandoned").
#[tokio::test]
async fn a_204_dropped_unread_ends_its_call_ok_too() {
    assert_eq!(ends_of(false).await, [None]);
}

/// A 200 with an empty body is not bodiless by its status: dropped unread,
/// its call ends abandoned, as documented.
#[tokio::test]
async fn an_empty_200_dropped_unread_ends_its_call_abandoned() {
    let mock = MockTransport::new();
    mock.reply("http://a.test", bare(StatusCode::OK));
    let ends = CallEnds::default();
    let providers =
        Gateway::new().provider("a", StreamableHttp::default(), mock.at("http://a.test"));
    let gw = gateway(
        providers,
        StreamableHttp::default(),
        vec![Arc::new(ends.clone()), Arc::new(CheckThenAnswer(false))],
        "a",
    );
    let req = mcp(Method::DELETE, "", &[("mcp-session-id", "s1")]);
    let _ = send(&gw, req).await;
    gw.idle().await;
    assert_eq!(ends.codes(), [Some(codes::ABANDONED.to_owned())]);
}
