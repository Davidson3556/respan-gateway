//! The legacy driver's part of the edge contract, on the old toy format: it
//! classifies a provider's answer at its head (rule 3), so a streaming
//! client its provider refuses gets a complete refusal, not a head and then
//! a failing body (W1), and the call says it was refused at its head.
#![allow(clippy::unwrap_used)]

mod toy;

use std::sync::{Arc, Mutex};

use respan_gateway_core::futures::FutureExt;
use respan_gateway_core::*;
use toy::*;

/// Notes whether each call was refused at its head.
struct HeadOnly(Arc<Mutex<Vec<bool>>>);
#[async_trait]
impl Plugin for HeadOnly {
    async fn on_provider_end(&self, px: &ProviderContext, _end: &Result<(), Fault>) {
        lock(&self.0).push(px.is_head_only());
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_streaming_request_refused_at_the_head_gets_a_complete_refusal() {
    // (the provider's status, what the client gets, how it ends, head only)
    let cases = [
        (403u16, (403, "ERR x"), "x", true),
        (200, (200, "a;!"), "ok", false),
    ];
    for (status, (got, body), end_code, head_only) in cases {
        let seen = Arc::default();
        let t = fnt(move |_| {
            async move { Ok(Exchange::new(Ans(status), chunks(vec!["a", "DONE"]))) }.boxed()
        });
        let gw = Gateway::new()
            .serve(Toy)
            .unwrap()
            .provider("a", Toy, t)
            .unwrap()
            .target("a")
            .unwrap()
            .plugin("head", HeadOnly(Arc::clone(&seen)))
            .unwrap();
        let (tx, req) = upload();
        let (res, end) = handle(&gw, req).await;
        drop(tx);
        assert_eq!(read(res).await, Ok((got, body.to_string())), "{status}");
        assert_eq!(code(&end.await), end_code, "{status}");
        gw.idle().await;
        assert_eq!(*lock(&seen), [head_only], "{status}");
    }
}
