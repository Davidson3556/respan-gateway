//! What a provider is sent of a session's later inputs, at the event level
//! (B1): an abandoned or broken upload never looks complete, and what the
//! client edge produced before it failed is not lost. The byte-level cases
//! (an encoder's failure, opening bytes) are HTTP's driver's
//! (`crates/http/tests/driver_toy.rs`). The toy dialer writes each input as
//! its text and a clean end as `<END>`.
#![allow(clippy::unwrap_used)]

mod edge;

use std::sync::{Arc, Mutex};

use edge::*;
use respan_gateway_core::futures::StreamExt;
use respan_gateway_core::*;

fn gateway_of(seen: &Seen, wait: bool, answer: fn() -> Text) -> Gateway {
    Gateway::new()
        .serve(Toy)
        .unwrap()
        .provider("a", Toy, seen.transport(wait, answer))
        .unwrap()
        .target("a")
        .unwrap()
}

fn frames(of: &[Result<&str, &str>]) -> Frames {
    of.iter()
        .map(|f| f.map(str::to_string).map_err(str::to_string))
        .collect()
}

/// B1: the client leaves in the middle of its upload, which is still open.
/// The provider's inputs end with an `Err`, never as complete.
#[tokio::test(flavor = "multi_thread")]
async fn an_abandoned_upload_ends_the_providers_body_with_an_err() {
    let seen = Seen::default();
    let gw = gateway_of(&seen, false, never);
    let (tx, req) = upload();
    send(&tx, "hello");
    let (res, _) = handle(&gw, req).await;
    let mut reply = res.unwrap();
    let _ = reply.body.next().await;
    seen.wait_for(1).await;
    drop(reply);
    let got = seen.finished().await;
    drop(tx);
    assert_eq!(got, frames(&[Ok("hello"), Err("abandoned")]));
}

/// B1: the client's inputs break. The provider's inputs end with that
/// `Err`, and no end follows it.
#[tokio::test(flavor = "multi_thread")]
async fn a_broken_upload_ends_the_providers_body_with_the_err() {
    let seen = Seen::default();
    let rec = Rec::default();
    let gw = gateway_of(&seen, true, done)
        .plugin("rec", rec.clone())
        .unwrap();
    let (tx, req) = upload();
    send(&tx, "hello");
    tx.unbounded_send(Err(Fault::bad_request("client broke")))
        .unwrap();
    let (res, _) = handle(&gw, req).await;
    let _ = read(res).await;
    gw.idle().await;
    assert_eq!(seen.frames(), frames(&[Ok("hello"), Err("bad_request")]));
}

/// A clean upload is written, then its end.
#[tokio::test(flavor = "multi_thread")]
async fn a_complete_upload_ends_cleanly() {
    let seen = Seen::default();
    let gw = gateway_of(&seen, true, done);
    let (tx, req) = upload();
    send(&tx, "one");
    send(&tx, "two");
    drop(tx);
    let (res, _) = handle(&gw, req).await;
    let _ = read(res).await;
    assert_eq!(seen.frames(), frames(&[Ok("one"), Ok("two"), Ok("<END>")]));
}

/// Reads a session's inputs in a task of its own, which may outlive it.
struct Tap(Arc<Mutex<Vec<tokio::task::JoinHandle<Frames>>>>);
#[async_trait]
impl Plugin for Tap {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let mut inputs = cx.take_inputs::<In>();
        let task = tokio::spawn(async move {
            let mut got = vec![];
            while let Some(x) = inputs.next().await {
                got.push(x.map(|i| i.0).map_err(|e| e.code().to_string()));
            }
            got
        });
        lock(&self.0).push(task);
        let z = futures::stream::iter([Ok::<_, Fault>(Out("z".into()))]);
        Ok(Some(Response::stream(z)))
    }
}

/// B1: inputs cut by the request's end, with the upload still open, end
/// with an `Err`.
#[tokio::test(flavor = "multi_thread")]
async fn take_inputs_ends_with_an_err_when_the_request_ends_first() {
    let taps = Arc::default();
    let gw = Gateway::new()
        .serve(Toy)
        .unwrap()
        .plugin("tap", Tap(Arc::clone(&taps)))
        .unwrap();
    let (tx, req) = upload();
    send(&tx, "one");
    let (res, _) = handle(&gw, req).await;
    let _ = read(res).await;
    gw.idle().await;
    let task = lock(&taps).pop().unwrap();
    let got = tokio::time::timeout(std::time::Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap();
    drop(tx);
    assert_eq!(got.last(), Some(&Err("abandoned".to_string())), "{got:?}");
}

/// Reads its inputs before it answers.
struct Collect(Arc<Mutex<Frames>>);
#[async_trait]
impl Plugin for Collect {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let got: Vec<_> = cx.take_inputs::<In>().collect().await;
        let got = got
            .into_iter()
            .map(|x| x.map(|i| i.0).map_err(|e| e.code().to_string()));
        lock(&self.0).extend(got);
        Ok(None)
    }
}

/// What the client edge produced before its `Err` is kept, for a plugin
/// and for a call.
#[tokio::test(flavor = "multi_thread")]
async fn values_decoded_before_a_decoder_err_are_kept() {
    let got = Arc::default();
    let seen = Seen::default();
    let gw = gateway_of(&seen, true, done)
        .plugin("collect", Collect(Arc::clone(&got)))
        .unwrap();
    let (tx, req) = upload();
    send(&tx, "ok");
    send(&tx, "BAD");
    let (res, _) = handle(&gw, req).await;
    let _ = read(res).await;
    gw.idle().await;
    let want = frames(&[Ok("ok"), Ok("before-bad"), Err("bad_request")]);
    assert_eq!(*lock(&got), want, "the plugin's inputs");

    // and a call sent after a plugin that did not read them
    let seen = Seen::default();
    let gw = gateway_of(&seen, true, done);
    let (tx, req) = upload();
    send(&tx, "ok");
    send(&tx, "BAD");
    let (res, _) = handle(&gw, req).await;
    let _ = read(res).await;
    assert_eq!(
        seen.frames(),
        frames(&[Ok("ok"), Ok("before-bad"), Err("bad_request")]),
        "the call's body"
    );
}

/// An empty input is an input.
#[tokio::test(flavor = "multi_thread")]
async fn an_empty_frame_is_still_a_frame() {
    let seen = Seen::default();
    let gw = gateway_of(&seen, true, done);
    let (tx, req) = upload();
    send(&tx, "one");
    send(&tx, "");
    drop(tx);
    let (res, _) = handle(&gw, req).await;
    let _ = read(res).await;
    assert_eq!(seen.frames(), frames(&[Ok("one"), Ok(""), Ok("<END>")]));
}
