//! A plugin's wrap of a request's streams (`ClientContext::wrap`, decision
//! 0023): async work on each input of a session, here a remote classifier,
//! and a deadline that cuts the outputs, with no plugin making the call. On
//! the toy edge, whose provider body shows each input's text, then `<END>`
//! on a clean end or the fault it broke with.
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

/// Lets each of the client's inputs on once the classifier passed it; the
/// first it flags fails the inputs `bad_request`. It makes no call itself.
struct Guard;

#[async_trait]
impl Plugin for Guard {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let check = cx.call("classifier");
        cx.wrap(move |inputs: Events<In>| {
            inputs.and_then(move |input| {
                let check = check.clone().with_input(input.clone());
                async move {
                    let verdict: Vec<Out> = check.await?.outputs().try_collect().await?;
                    match verdict.iter().any(|v| v.0 == "FLAG") {
                        true => Err(Fault::bad_request("the classifier flagged an input")),
                        false => Ok(input),
                    }
                }
            })
        })?;
        Ok(None)
    }
}

/// Cuts the outputs sent to the client after `.0`, with a clean end. It
/// makes no call itself.
struct Deadline(Duration);

#[async_trait]
impl Plugin for Deadline {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let limit = self.0;
        cx.wrap(move |outputs: Events<Out>| outputs.take_until(tokio::time::sleep(limit)))?;
        Ok(None)
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
        .target("p")
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

/// The rest of a reply, as `read` gives it.
async fn rest(mut body: BoxStream<'static, Result<String, Fault>>) -> String {
    let mut out = String::new();
    while let Some(frame) = body.next().await {
        out.push_str(&frame.unwrap());
    }
    out
}

/// An input refused after an async check ends what the provider is sent,
/// and the call and the reply end with that refusal, for the client edge to
/// say, not as if the client had left.
#[tokio::test]
async fn a_flagged_input_is_refused_and_ends_the_call_with_that_fault() {
    let (body, checked, rec) = (Body::default(), Arc::default(), Rec::default());
    let gw = Gateway::new()
        .serve(Toy)
        .unwrap()
        .provider("p", Toy, body.provider(10))
        .unwrap()
        .provider("classifier", Toy, classifier(Arc::clone(&checked)))
        .unwrap()
        .plugin("rec", rec.clone())
        .unwrap()
        .plugin("guard", Guard)
        .unwrap()
        .target("p")
        .unwrap();
    let (tx, req) = upload();
    for text in ["a", "evil", "b"] {
        send(&tx, text);
    }
    let (res, end) = handle(&gw, req).await;
    let (frame, reply) = first(res).await;
    assert_eq!(frame, "a;");
    assert_eq!(rest(reply).await, "ERR bad_request");
    assert_eq!(code(&end.await), "bad_request");
    let frames = body.finished().await;
    assert_eq!(frames, [Ok("a".into()), Err("bad_request".into())]);
    // `b` was never checked, and the call still relays.
    assert_eq!(checked.load(Ordering::SeqCst), 2);
    assert_eq!(*lock(&body.relayed), [true]);
    gw.idle().await;
    // The classifier's calls ended ok; the vendor's with the refusal.
    let prov = rec.log().prov_codes();
    assert_eq!(prov.iter().filter(|c| *c == "bad_request").count(), 1);
    assert_eq!(rec.log().client_codes(), ["bad_request"]);
}

/// A filter's refusal of a session's input ends the call with it too.
#[tokio::test]
async fn a_filters_refusal_of_a_sessions_input_ends_the_call_with_it() {
    struct Refuse;
    #[async_trait]
    impl Plugin for Refuse {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            cx.filter(|input: &mut In| match input.0.as_str() {
                "evil" => Err(Fault::bad_request("refused")),
                _ => Ok(()),
            })?;
            Ok(None)
        }
    }
    let (body, checked) = (Body::default(), Arc::default());
    let gw = gateway(&body, 10, &checked, Refuse);
    let (tx, req) = upload();
    send(&tx, "a");
    send(&tx, "evil");
    let (res, end) = handle(&gw, req).await;
    let (_frame, reply) = first(res).await;
    assert_eq!(rest(reply).await, "ERR bad_request");
    assert_eq!(code(&end.await), "bad_request");
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

/// A wrap that turns the cut of what it wraps into a clean end still
/// leaves the provider a cut, never a complete upload.
#[tokio::test]
async fn a_wrap_that_swallows_the_cut_still_ends_abandoned() {
    struct Swallow;
    #[async_trait]
    impl Plugin for Swallow {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            cx.wrap(|inputs: Events<In>| inputs.take_while(|i| std::future::ready(i.is_ok())))?;
            Ok(None)
        }
    }
    let (body, checked) = (Body::default(), Arc::default());
    let gw = gateway(&body, 10, &checked, Swallow);
    let (tx, req) = upload();
    send(&tx, "a");
    let (res, end) = handle(&gw, req).await;
    let (_frame, reply) = first(res).await;
    body.wait_for(1).await;
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
            cx.wrap(|_client: Events<In>| {
                stream::once(async { Ok(In("x".into())) }).chain(stream::pending())
            })?;
            Ok(None)
        }
    }
    let (body, checked) = (Body::default(), Arc::default());
    let gw = gateway(&body, 10, &checked, Own);
    let (_tx, req) = upload();
    let (res, end) = handle(&gw, req).await;
    let (_frame, reply) = first(res).await;
    body.wait_for(1).await;
    drop(reply);
    assert_eq!(code(&end.await), "abandoned");
    let frames = body.finished().await;
    assert_eq!(frames, [Ok("x".into()), Err("abandoned".into())]);
}

/// A plugin's stream that fails ends the call's inputs, and the call, with
/// its fault.
#[tokio::test]
async fn a_stream_that_fails_ends_the_inputs_and_the_call_with_its_fault() {
    struct Fails;
    #[async_trait]
    impl Plugin for Fails {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            cx.wrap(|_client: Events<In>| {
                stream::iter([Ok(In("x".into())), Err(Fault::upstream("gone"))])
            })?;
            Ok(None)
        }
    }
    let (body, checked) = (Body::default(), Arc::default());
    let gw = gateway(&body, 10, &checked, Fails);
    let (_tx, req) = upload();
    let (res, end) = handle(&gw, req).await;
    let (_frame, reply) = first(res).await;
    let frames = body.finished().await;
    assert_eq!(frames, [Ok("x".into()), Err("upstream".into())]);
    assert_eq!(rest(reply).await, "ERR upstream");
    assert_eq!(code(&end.await), "upstream");
}

/// A panic in a plugin's stream is a failure, never a clean end, and every
/// hook ends once.
#[tokio::test]
async fn a_panicking_stream_is_a_failure_and_hooks_end_once() {
    struct Panics;
    #[async_trait]
    impl Plugin for Panics {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            cx.wrap(|_client: Events<In>| {
                stream::iter([1, 2]).map(|i| match i {
                    2 => panic!("a plugin's stream panics"),
                    _ => Ok(In("x".into())),
                })
            })?;
            Ok(None)
        }
    }
    let (seen, rec) = (Seen::default(), Rec::default());
    let gw = Gateway::new()
        .serve(Toy)
        .unwrap()
        .provider("p", Toy, seen.transport(true, done))
        .unwrap()
        .plugin("rec", rec.clone())
        .unwrap()
        .plugin("panics", Panics)
        .unwrap()
        .target("p")
        .unwrap();
    let (_tx, req) = upload();
    let (res, end) = handle(&gw, req).await;
    let _ = read(res).await;
    assert_eq!(code(&end.await), "internal");
    tokio::time::timeout(Duration::from_secs(2), gw.idle())
        .await
        .unwrap();
    assert_eq!(seen.frames(), [Ok("x".into()), Err("internal".into())]);
    let l = rec.log();
    assert_eq!((l.client_start, l.client_end.len()), (1, 1));
    assert_eq!((l.prov_start, l.prov_end.len()), (1, 1));
}

/// A wrap of another type than the client's input or output is refused, as
/// is a wrap of a whole request's one input, which came with its head.
#[tokio::test]
async fn a_wrap_of_another_type_or_of_a_whole_input_is_refused() {
    struct Wrong(bool);
    #[async_trait]
    impl Plugin for Wrong {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            match self.0 {
                true => cx.wrap(|s: Events<String>| s)?,
                false => cx.wrap(|s: Events<In>| s)?,
            }
            Ok(None)
        }
    }
    for (streams, wrong) in [(true, true), (false, false)] {
        let (body, checked) = (Body::default(), Arc::default());
        let gw = gateway(&body, 10, &checked, Wrong(wrong));
        let (res, _end) = handle(&gw, req_of(vec!["q"], streams)).await;
        assert_eq!(read(res).await, Ok((500, "ERR internal".into())));
    }
}

/// A call refused before it read an input gives the wrapped inputs back,
/// so a fallback to another provider sends them all.
#[tokio::test]
async fn a_fallback_gets_the_wrapped_inputs_the_refused_call_never_read() {
    struct Fallback;
    #[async_trait]
    impl Plugin for Fallback {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            cx.wrap(|inputs: Events<In>| inputs.map_ok(|In(text)| In(text.to_uppercase())))?;
            match cx.call("refuses").await?.until_content().await {
                Ok(resp) => Ok(Some(resp)),
                Err(_) => Ok(Some(cx.call("p").await?)),
            }
        }
    }
    let body = Body::default();
    let refuses = fnt(|_| async { Ok(answer(503, chunks(vec![]))) }.boxed());
    let gw = Gateway::new()
        .serve(Toy)
        .unwrap()
        .provider("refuses", Toy, refuses)
        .unwrap()
        .provider("p", Toy, body.provider(10))
        .unwrap()
        .plugin("fallback", Fallback)
        .unwrap();
    let (tx, req) = upload();
    send(&tx, "x");
    send(&tx, "y");
    drop(tx);
    let (res, _end) = handle(&gw, req).await;
    let (_frame, _reply) = first(res).await;
    let texts = ["X", "Y", "<END>"].map(|t| Ok(t.to_string()));
    assert_eq!(body.finished().await, texts);
    assert_eq!(*lock(&body.relayed), [true]);
}

/// A provider that reads the whole call body, records it, and refuses the
/// first call at its head, after reading it; later calls answer `a`.
fn reads_then_refuses_first(bodies: Arc<Mutex<Vec<Frames>>>) -> Dialer {
    let n = Arc::new(AtomicUsize::new(0));
    fnt(move |c| {
        let (bodies, n) = (bodies.clone(), n.clone());
        async move {
            let frames = c.body.map(|f| f.map_err(|e| e.code().to_string()));
            let frames: Frames = frames.collect().await;
            lock(&bodies).push(frames);
            match n.fetch_add(1, Ordering::SeqCst) {
                0 => Ok(answer(503, chunks(vec![]))),
                _ => Ok(answer(200, chunks(vec!["a", "DONE"]))),
            }
        }
        .boxed()
    })
}

/// B1: a retry after the first call read every wrapped input is not sent an
/// empty upload that looks complete (the plain case is in `upload.rs`).
#[tokio::test]
async fn a_retry_after_every_wrapped_input_was_read_is_not_a_clean_empty_upload() {
    struct Retry;
    #[async_trait]
    impl Plugin for Retry {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            cx.wrap(|inputs: Events<In>| inputs)?;
            let call = cx.call("p");
            match call.clone().await?.until_content().await {
                Ok(resp) => Ok(Some(resp)),
                Err(_) => Ok(Some(call.await?)),
            }
        }
    }
    let bodies = Arc::new(Mutex::new(Vec::new()));
    let gw = Gateway::new()
        .serve(Toy)
        .unwrap()
        .provider("p", Toy, reads_then_refuses_first(bodies.clone()))
        .unwrap()
        .plugin("retry", Retry)
        .unwrap();
    let (tx, req) = upload();
    send(&tx, "x");
    send(&tx, "y");
    drop(tx);
    let (res, _end) = handle(&gw, req).await;
    let _ = tokio::time::timeout(Duration::from_secs(2), read(res)).await;
    let bodies = lock(&bodies).clone();
    assert_eq!(bodies[0], ["x", "y", "<END>"].map(|t| Ok(t.to_string())));
    assert_eq!(bodies[1], [Err("internal".to_string())]);
}

/// A gate on the inputs and a deadline on the outputs, from two plugins,
/// neither making the call, both apply to one relay: the provider gets the
/// client's origin (and so its key), and the client the provider's head.
#[tokio::test]
async fn a_gate_and_a_deadline_compose_and_the_relay_is_kept() {
    let (body, checked, rec) = (Body::default(), Arc::default(), Rec::default());
    let gw = Gateway::new()
        .serve(Toy)
        .unwrap()
        .provider("p", Toy, body.provider(10))
        .unwrap()
        .provider("classifier", Toy, classifier(Arc::clone(&checked)))
        .unwrap()
        .plugin("rec", rec.clone())
        .unwrap()
        .plugin("guard", Guard)
        .unwrap()
        .plugin("deadline", Deadline(Duration::from_millis(100)))
        .unwrap()
        .target("p")
        .unwrap();
    let (tx, req) = upload();
    send(&tx, "a");
    let (res, end) = handle(&gw, req).await;
    let res = res.unwrap();
    assert_eq!(
        res.head,
        Some(200),
        "the provider's head reached the client"
    );
    // The provider never ends; the deadline does.
    let started = tokio::time::Instant::now();
    assert_eq!(rest(res.body).await, "a;!");
    assert!(started.elapsed() >= Duration::from_millis(90));
    assert_eq!(code(&end.await), "ok");
    assert_eq!(body.finished().await[0], Ok("a".into()));
    assert_eq!(*lock(&body.relayed), [true]);
    assert_eq!(checked.load(Ordering::SeqCst), 1);
    drop(tx);
    gw.idle().await;
    // The vendor's call was cut, its outputs dropped unread.
    assert!(rec.log().prov_codes().contains(&"abandoned".to_string()));
}

/// A deadline that cuts the outputs with a fault of its own ends the reply
/// and the call with it, as `Ended(Err(f))` ends the reply's call (rule 4),
/// though the wrap dropped the call's outputs unread.
#[tokio::test]
async fn a_cut_with_a_fault_of_its_own_ends_the_call_with_it() {
    struct Cut;
    #[async_trait]
    impl Plugin for Cut {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            cx.wrap(|outputs: Events<Out>| {
                let over = stream::once(async { Err(Fault::new("max_length", "over")) });
                outputs.take(1).chain(over)
            })?;
            Ok(None)
        }
    }
    let (body, rec) = (Body::default(), Rec::default());
    let gw = Gateway::new()
        .serve(Toy)
        .unwrap()
        .provider("p", Toy, body.provider(10))
        .unwrap()
        .plugin("rec", rec.clone())
        .unwrap()
        .plugin("cut", Cut)
        .unwrap()
        .target("p")
        .unwrap();
    let (tx, req) = upload();
    let (res, end) = handle(&gw, req).await;
    assert_eq!(read(res).await, Ok((200, "a;ERR max_length".into())));
    assert_eq!(code(&end.await), "max_length");
    drop(tx);
    gw.idle().await;
    assert_eq!(rec.log().prov_codes(), ["max_length"]);
}

/// Without a wrap, the same relay: the wraps change nothing of it.
#[tokio::test]
async fn a_plain_relay_has_the_same_heads() {
    let (body, checked) = (Body::default(), Arc::default());
    let gw = gateway(&body, 10, &checked, Rec::default());
    let (_tx, req) = upload();
    let (res, _end) = handle(&gw, req).await;
    assert_eq!(res.unwrap().head, Some(200));
    let _ = body.finished().await;
    assert_eq!(*lock(&body.relayed), [true]);
}

/// Wraps and filters apply in plugin order: inputs after the earlier
/// plugins, outputs before them.
#[tokio::test]
async fn wraps_and_filters_apply_in_plugin_order() {
    /// Plugin `.0` tags each input and output with its name, by a wrap if
    /// `.1`, else by a filter.
    struct Tag(&'static str, bool);
    #[async_trait]
    impl Plugin for Tag {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let name = self.0;
            if self.1 {
                cx.wrap(move |s: Events<In>| s.map_ok(move |In(t)| In(format!("{t}{name}"))))?;
                cx.wrap(move |s: Events<Out>| s.map_ok(move |Out(t)| Out(format!("{t}{name}"))))?;
            } else {
                cx.filter(move |i: &mut In| {
                    i.0.push_str(name);
                    Ok(())
                })?;
                cx.filter(move |o: &mut Out| {
                    o.0.push_str(name);
                    Ok(())
                })?;
            }
            Ok(None)
        }

        /// The call's own input filter runs after the request's wraps.
        async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
            let name = self.0;
            px.filter(move |i: &mut In| {
                i.0.push_str(&format!("c{name}"));
                Ok(())
            })
        }
    }
    let seen = Seen::default();
    let gw = Gateway::new()
        .serve(Toy)
        .unwrap()
        .provider("p", Toy, seen.transport(true, done))
        .unwrap()
        .plugin("1", Tag("1", false))
        .unwrap()
        .plugin("2", Tag("2", true))
        .unwrap()
        .plugin("3", Tag("3", false))
        .unwrap()
        .plugin("4", Tag("4", true))
        .unwrap()
        .target("p")
        .unwrap();
    let (res, _end) = handle(&gw, req_of(vec!["x"], true)).await;
    assert_eq!(read(res).await, Ok((200, "a4321;!".into())));
    let frames = seen.finished().await;
    assert_eq!(frames, [Ok("x1234c1c2c3c4".into()), Ok("<END>".into())]);
}

/// A wrap that panics as it wraps the outputs refuses the reply `internal`.
#[tokio::test]
async fn a_wrap_that_panics_fails_the_reply_internal() {
    struct Panics;
    #[async_trait]
    impl Plugin for Panics {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            cx.wrap(|_: Events<Out>| -> Events<Out> { panic!("a buggy wrap") })?;
            Ok(None)
        }
    }
    let (seen, rec) = (Seen::default(), Rec::default());
    let gw = Gateway::new()
        .serve(Toy)
        .unwrap()
        .provider("p", Toy, seen.transport(false, done))
        .unwrap()
        .plugin("rec", rec.clone())
        .unwrap()
        .plugin("panics", Panics)
        .unwrap()
        .target("p")
        .unwrap();
    let (res, end) = handle(&gw, req(vec!["q"])).await;
    assert_eq!(read(res).await, Ok((500, "ERR internal".into())));
    assert_eq!(code(&end.await), "internal");
    gw.idle().await;
    assert_eq!(rec.log().prov_end.len(), 1);
}

/// Drop everything at every await point of a request whose inputs a gate
/// checks and whose outputs a deadline wraps: every hook ends once, `idle`
/// resolves, and the provider never sees a clean end before every input.
#[tokio::test(flavor = "current_thread")]
async fn wrapped_requests_end_once_when_dropped_at_every_await() {
    let classifier = fnt(|c| {
        async move {
            let _: String = c.body.try_collect().await?;
            tokio::task::yield_now().await;
            Ok(answer(200, slow(vec!["ok", "DONE"])))
        }
        .boxed()
    });
    let (mut failures, mut reached) = (vec![], 0);
    for h in 0..30 {
        for b in 0..30 {
            let (rec, seen) = (Rec::default(), Seen::default());
            let c = classifier.0.clone();
            let gw = Gateway::new()
                .serve(Toy)
                .unwrap()
                // Answers once it read the body, so the drops land while
                // checked inputs flow.
                .provider("p", Toy, seen.transport(true, done))
                .unwrap()
                .provider("classifier", Toy, fnt(move |call| c(call)))
                .unwrap()
                .plugin("rec", rec.clone())
                .unwrap()
                .plugin("guard", Guard)
                .unwrap()
                .plugin("deadline", Deadline(Duration::from_secs(5)))
                .unwrap()
                .target("p")
                .unwrap();
            let r = req_of(vec!["one", "two"], true);
            let end = match limit(handle(&gw, r), h).await {
                Some((res, end)) => {
                    if let Ok(reply) = res {
                        let mut body = reply.body;
                        let mut n = 0;
                        while let Some(Some(_)) = limit(body.next(), b).await {
                            n += 1;
                            if n > 50 {
                                break;
                            }
                        }
                    }
                    Some(end)
                }
                None => None,
            };
            let idle = tokio::time::timeout(Duration::from_secs(2), gw.idle()).await;
            let (c, p) = {
                let l = rec.log();
                let c = (l.client_start, l.client_end.len());
                (c, (l.prov_start, l.prov_end.len()))
            };
            if idle.is_err() || c.0 != c.1 || p.0 != p.1 {
                let idle = idle.is_ok();
                failures.push(format!(
                    "h {h} b {b}: idle {idle} client {c:?} provider {p:?}"
                ));
            }
            if let Some(end) = end
                && tokio::time::timeout(Duration::from_secs(1), end)
                    .await
                    .is_err()
            {
                failures.push(format!("h {h} b {b}: the end never resolved"));
            }
            let frames = seen.frames();
            if frames.last() == Some(&Ok("<END>".into())) && frames.len() != 3 {
                failures.push(format!("h {h} b {b}: clean end after {frames:?}"));
            }
            reached += usize::from(frames.first() == Some(&Ok("one".into())));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
    assert!(
        reached > 100,
        "the provider read a checked input {reached} times"
    );
}
