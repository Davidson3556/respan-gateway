//! A call's own streamed inputs (`Call::with_inputs`, decision 0022): a
//! plugin does async work on each input of a session, here a remote
//! classifier, with no wrapper around the provider's format. On the toy
//! edge, whose provider body shows each input's text, then `<END>` on a
//! clean end or the fault it broke with.
#![allow(clippy::unwrap_used)]

mod edge;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use edge::*;
use respan_gateway_core::futures::stream::BoxStream;
use respan_gateway_core::futures::{FutureExt, StreamExt, TryStreamExt, stream};
use respan_gateway_core::*;

/// What a provider read of each call's body, whether each call relayed,
/// and how many calls it got.
#[derive(Clone, Default)]
struct Body {
    frames: Arc<Mutex<Frames>>,
    relayed: Arc<Mutex<Vec<bool>>>,
    done: Arc<AtomicUsize>,
}

impl Body {
    /// A provider that reads up to `read` frames of the body in its own
    /// task, holding the rest unread, and answers `a` then never ends.
    fn provider(&self, read: usize) -> Dialer {
        let body = self.clone();
        fnt(move |c| {
            let body = body.clone();
            lock(&body.relayed).push(c.relayed);
            tokio::spawn(async move {
                let mut frames = c.body;
                for _ in 0..read {
                    match frames.next().await {
                        Some(f) => lock(&body.frames).push(f.map_err(|e| e.code().into())),
                        None => break,
                    }
                }
                body.done.fetch_add(1, Ordering::SeqCst);
                // Held, unread, until the call ends.
                let _held = frames;
                std::future::pending::<()>().await;
            });
            async { Ok(answer(200, never())) }.boxed()
        })
    }

    async fn wait_for(&self, n: usize) {
        for _ in 0..500 {
            if lock(&self.frames).len() >= n {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("the body never had {n} frames: {:?}", lock(&self.frames));
    }

    async fn finished(&self) -> Frames {
        for _ in 0..500 {
            if self.done.load(Ordering::SeqCst) > 0 {
                return lock(&self.frames).clone();
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("the body never ended: {:?}", lock(&self.frames));
    }
}

/// A classifier: flags an input that says `evil`, and takes 50ms over one
/// that says `slow`.
fn classifier(checked: Arc<AtomicUsize>) -> Dialer {
    fnt(move |c| {
        checked.fetch_add(1, Ordering::SeqCst);
        async move {
            let text: String = c.body.try_collect().await?;
            if text.starts_with("slow") {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            let verdict = if text.starts_with("evil") {
                "FLAG"
            } else {
                "ok"
            };
            Ok(ok_resp(vec![verdict, "DONE"]))
        }
        .boxed()
    })
}

/// Sends each of the client's inputs to the provider once the classifier
/// passed it; the first it flags ends the call's inputs `bad_request`.
struct Guard;

#[async_trait]
impl Plugin for Guard {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let check = cx.call("classifier");
        let inputs = cx.take_inputs::<In>().and_then(move |input| {
            let check = check.clone().with_input(input.clone());
            async move {
                let verdict: Vec<Out> = check.await?.outputs().try_collect().await?;
                match verdict.iter().any(|v| v.0 == "FLAG") {
                    true => Err(Fault::bad_request("the classifier flagged an input")),
                    false => Ok(input),
                }
            }
        });
        Ok(Some(cx.call("p").with_inputs(inputs).await?))
    }
}

fn gateway(body: &Body, read: usize, checked: &Arc<AtomicUsize>, plugin: impl Plugin) -> Gateway {
    Gateway::new()
        .serve(Toy)
        .unwrap()
        .provider("p", Toy, body.provider(read))
        .unwrap()
        .provider("classifier", Toy, classifier(checked.clone()))
        .unwrap()
        .plugin("plugin", plugin)
        .unwrap()
}

/// The first reply frame, the reply kept open.
async fn first(
    res: Result<ToyReply, Fault>,
) -> (String, BoxStream<'static, Result<String, Fault>>) {
    let mut body = res.unwrap().body;
    let frame = body.next().await.unwrap().unwrap();
    (frame, body)
}

#[tokio::test]
async fn a_flagged_input_is_refused_after_an_async_check() {
    let (body, checked) = (Body::default(), Arc::default());
    let gw = gateway(&body, 10, &checked, Guard);
    let (tx, req) = upload();
    for text in ["a", "evil", "b"] {
        send(&tx, text);
    }
    let (res, _end) = handle(&gw, req).await;
    let (_frame, _reply) = first(res).await;
    let frames = body.finished().await;
    assert_eq!(frames, [Ok("a".into()), Err("bad_request".into())]);
    // `b` was never checked, and the call is not a relay.
    assert_eq!(checked.load(Ordering::SeqCst), 2);
    assert_eq!(*lock(&body.relayed), [false]);
}

#[tokio::test]
async fn inputs_keep_their_order_whatever_each_check_takes() {
    let (body, checked) = (Body::default(), Arc::default());
    let gw = gateway(&body, 10, &checked, Guard);
    let (tx, req) = upload();
    for text in ["slow", "b", "c"] {
        send(&tx, text);
    }
    drop(tx);
    let (res, _end) = handle(&gw, req).await;
    let (_frame, _reply) = first(res).await;
    let frames = body.finished().await;
    let texts = ["slow", "b", "c", "<END>"].map(|t| Ok(t.to_string()));
    assert_eq!(frames, texts, "a clean end follows every input");
}

/// Pulled as the provider reads: an input the provider has not asked for is
/// not checked.
#[tokio::test]
async fn an_input_is_checked_only_when_the_provider_reads_it() {
    let (body, checked) = (Body::default(), Arc::default());
    let gw = gateway(&body, 1, &checked, Guard);
    let (tx, req) = upload();
    for text in ["a", "b", "c", "d"] {
        send(&tx, text);
    }
    let (res, _end) = handle(&gw, req).await;
    let (_frame, _reply) = first(res).await;
    assert_eq!(body.finished().await, [Ok("a".into())]);
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(checked.load(Ordering::SeqCst), 1);
}

/// The request ends while the provider reads: the call's inputs end
/// `abandoned`, never as if complete (B1).
#[tokio::test]
async fn a_cut_ends_the_inputs_abandoned_never_cleanly() {
    let (body, checked) = (Body::default(), Arc::default());
    let gw = gateway(&body, 10, &checked, Guard);
    let (tx, req) = upload();
    send(&tx, "a");
    let (res, end) = handle(&gw, req).await;
    let (_frame, reply) = first(res).await;
    body.wait_for(1).await;
    // The client goes away; its upload stays open.
    drop(reply);
    assert_eq!(code(&end.await), "abandoned");
    let frames = body.finished().await;
    assert_eq!(frames, [Ok("a".into()), Err("abandoned".into())]);
    drop(tx);
}

/// The same for inputs a plugin makes of its own, not of the client's.
#[tokio::test]
async fn a_cut_ends_a_plugins_own_stream_abandoned() {
    struct Own;
    #[async_trait]
    impl Plugin for Own {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let inputs = stream::once(async { Ok(In("x".into())) }).chain(stream::pending());
            Ok(Some(cx.call("p").with_inputs(inputs).await?))
        }
    }
    let (body, checked) = (Body::default(), Arc::default());
    let gw = gateway(&body, 10, &checked, Own);
    let (res, end) = handle(&gw, req(vec!["q"])).await;
    let (_frame, reply) = first(res).await;
    body.wait_for(1).await;
    drop(reply);
    assert_eq!(code(&end.await), "abandoned");
    let frames = body.finished().await;
    assert_eq!(frames, [Ok("x".into()), Err("abandoned".into())]);
}

/// A plugin's stream that fails ends the call's inputs with its fault.
#[tokio::test]
async fn a_stream_that_fails_ends_the_inputs_with_its_fault() {
    struct Fails;
    #[async_trait]
    impl Plugin for Fails {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let inputs = stream::iter([Ok(In("x".into())), Err(Fault::upstream("gone"))]);
            Ok(Some(cx.call("p").with_inputs(inputs).await?))
        }
    }
    let (body, checked) = (Body::default(), Arc::default());
    let gw = gateway(&body, 10, &checked, Fails);
    let (res, _end) = handle(&gw, req(vec!["q"])).await;
    let (_frame, _reply) = first(res).await;
    let frames = body.finished().await;
    assert_eq!(frames, [Ok("x".into()), Err("upstream".into())]);
}

/// Inputs of another type than the provider's fail the call.
#[tokio::test]
async fn inputs_of_another_type_fail_the_call() {
    struct Wrong;
    #[async_trait]
    impl Plugin for Wrong {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let inputs = stream::iter([Ok::<_, Fault>("x".to_string())]);
            Ok(Some(cx.call("p").with_inputs(inputs).await?))
        }
    }
    let (body, checked) = (Body::default(), Arc::default());
    let gw = gateway(&body, 10, &checked, Wrong);
    let (res, _end) = handle(&gw, req(vec!["q"])).await;
    assert_eq!(read(res).await, Ok((500, "ERR internal".into())));
}

/// A clone shares the inputs: a call refused before it read any gives
/// them back, so its retry sends them all.
#[tokio::test]
async fn a_retry_gets_inputs_the_refused_call_never_read() {
    struct Retry;
    #[async_trait]
    impl Plugin for Retry {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let inputs = stream::iter(["x", "y"].map(|t| Ok(In(t.into()))));
            let call = cx.call("p").with_inputs(inputs);
            match call.clone().await?.until_content().await {
                Ok(resp) => Ok(Some(resp)),
                Err(_) => Ok(Some(call.await?)),
            }
        }
    }
    let (body, calls) = (Body::default(), Arc::new(AtomicUsize::new(0)));
    let reads = body.provider(10);
    let refuses_first = fnt(move |c| match calls.fetch_add(1, Ordering::SeqCst) {
        0 => async { Ok(answer(503, chunks(vec![]))) }.boxed(),
        _ => (reads.0)(c),
    });
    let gw = Gateway::new()
        .serve(Toy)
        .unwrap()
        .provider("p", Toy, refuses_first)
        .unwrap()
        .plugin("retry", Retry)
        .unwrap();
    let (res, _end) = handle(&gw, req(vec!["q"])).await;
    let (_frame, _reply) = first(res).await;
    let texts = ["x", "y", "<END>"].map(|t| Ok(t.to_string()));
    assert_eq!(body.finished().await, texts);
}
