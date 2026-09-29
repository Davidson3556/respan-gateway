//! What a provider is sent of a request's body: an abandoned or broken
//! upload never looks complete, opening bytes come first, and what the
//! client's decoder produced before it failed is not lost.
#![allow(clippy::unwrap_used)]

mod toy;

use std::sync::{Arc, Mutex};

use respan_gateway_core::futures::StreamExt;
use respan_gateway_core::*;
use toy::*;

fn gateway_of(
    format: impl Format<Ask = Ask, Answer = Ans, Input = In, Output = Out>,
    seen: &Seen,
    wait: bool,
    answer: fn() -> Body,
) -> Gateway {
    Gateway::new()
        .serve(Toy)
        .unwrap()
        .provider("a", format, seen.transport(wait, answer))
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
/// The provider's body ends with an `Err`, never as a complete one.
#[tokio::test(flavor = "multi_thread")]
async fn an_abandoned_upload_ends_the_providers_body_with_an_err() {
    let seen = Seen::default();
    let gw = gateway_of(Toy, &seen, false, never);
    let (tx, req) = upload();
    send(&tx, "hello");
    let (res, _) = handle(&gw, req).await;
    let mut ex = res.unwrap();
    let _ = ex.body.next().await;
    seen.wait_for(1).await;
    drop(ex);
    let got = seen.finished().await;
    drop(tx);
    assert_eq!(got, frames(&[Ok("hello"), Err("abandoned")]));
}

/// B1: the client's body breaks. The provider's body ends with that `Err`,
/// and no end is written after it.
#[tokio::test(flavor = "multi_thread")]
async fn a_broken_upload_ends_the_providers_body_with_the_err() {
    let seen = Seen::default();
    let rec = Rec::default();
    let gw = gateway_of(Toy, &seen, true, done)
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

/// An encoder that fails what it is given ends its body there: nothing
/// follows the `Err`, not even a clean end.
#[tokio::test(flavor = "multi_thread")]
async fn a_body_ends_at_its_first_failure() {
    let seen = Seen::default();
    let gw = gateway_of(Toy, &seen, true, done);
    let (tx, req) = upload();
    for text in ["one", "ENCPANIC", "two"] {
        send(&tx, text);
    }
    let (res, _) = handle(&gw, req).await;
    let _ = read(res).await;
    assert_eq!(seen.frames(), frames(&[Ok("one"), Err("internal")]));
}

/// A clean upload is written, then its end.
#[tokio::test(flavor = "multi_thread")]
async fn a_complete_upload_ends_cleanly() {
    let seen = Seen::default();
    let gw = gateway_of(Toy, &seen, true, done);
    let (tx, req) = upload();
    send(&tx, "one");
    send(&tx, "two");
    drop(tx);
    let (res, _) = handle(&gw, req).await;
    let _ = read(res).await;
    assert_eq!(seen.frames(), frames(&[Ok("one"), Ok("two"), Ok("<END>")]));
}

/// Reads a request's inputs in a task of its own, which may outlive the
/// request.
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

/// Suspected: a decoder's `Err` after values in the same chunk dropped those
/// values for a streaming request, but not for a response.
#[tokio::test(flavor = "multi_thread")]
async fn values_decoded_before_a_decoder_err_are_kept() {
    let got = Arc::default();
    let seen = Seen::default();
    let gw = gateway_of(Toy, &seen, true, done)
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
    let gw = gateway_of(Toy, &seen, true, done);
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

/// B5: `Encoder::open`'s bytes come before the body, the client's or the
/// written one.
#[tokio::test(flavor = "current_thread")]
async fn a_request_encoders_opening_bytes_come_first() {
    let seen = Seen::default();
    let gw = gateway_of(Opening("<OPEN>", false), &seen, true, done);
    let (res, _) = handle(&gw, req(vec!["q"])).await;
    let _ = read(res).await;
    // a relay's first frame is the client's bytes, unread
    assert_eq!(seen.frames(), frames(&[Ok("<OPEN>q<END>")]));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_streaming_requests_opening_bytes_are_its_first_frame() {
    let seen = Seen::default();
    let gw = gateway_of(Opening("<OPEN>", false), &seen, true, done);
    let (tx, req) = upload();
    send(&tx, "x");
    drop(tx);
    let (res, _) = handle(&gw, req).await;
    let _ = read(res).await;
    assert_eq!(seen.frames(), frames(&[Ok("<OPEN>"), Ok("x"), Ok("<END>")]));
}

/// B5: `Ok(true)` from a request encoder's `open`, as from a response
/// encoder's, means nothing more follows: the body is the opening bytes.
#[tokio::test(flavor = "multi_thread")]
async fn a_request_encoders_open_saying_done_is_the_whole_body() {
    let seen = Seen::default();
    let gw = gateway_of(Opening("<OPEN>", true), &seen, true, done);
    let (res, _) = handle(&gw, req(vec!["q"])).await;
    assert_eq!(read(res).await, Ok((200, "a;!".into())));
    assert_eq!(seen.frames(), frames(&[Ok("<OPEN>")]), "a whole request");

    let seen = Seen::default();
    let gw = gateway_of(Opening("<OPEN>", true), &seen, true, done);
    let (tx, req) = upload();
    send(&tx, "x");
    let (res, _) = handle(&gw, req).await;
    assert_eq!(read(res).await, Ok((200, "a;!".into())));
    drop(tx);
    assert_eq!(
        seen.frames(),
        frames(&[Ok("<OPEN>")]),
        "a streaming request"
    );
}

/// An empty data frame is an input, and reaches the provider as a frame.
#[tokio::test(flavor = "multi_thread")]
async fn an_empty_frame_is_still_a_frame() {
    let seen = Seen::default();
    let gw = gateway_of(Toy, &seen, true, done);
    let (tx, req) = upload();
    send(&tx, "one");
    send(&tx, "");
    drop(tx);
    let (res, _) = handle(&gw, req).await;
    let _ = read(res).await;
    assert_eq!(seen.frames(), frames(&[Ok("one"), Ok(""), Ok("<END>")]));
}
