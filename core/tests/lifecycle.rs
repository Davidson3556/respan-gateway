//! What a gateway counts as in flight, and what a request may be given back.
//! On the toy edge, with relay provenance.
#![allow(clippy::unwrap_used)]

mod edge;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use edge::*;
use respan_gateway_core::futures::FutureExt;
use respan_gateway_core::*;

fn gateway_of(rec: &Rec) -> Gateway {
    Gateway::new()
        .client::<In, _, _>(Toy)
        .unwrap()
        .provider::<In, _, _>(
            "a",
            Toy,
            connect(fnt(|_| async { Ok(ok_resp(vec!["a", "DONE"])) }.boxed())),
        )
        .unwrap()
        .target("a")
        .unwrap()
        .plugin("rec", rec.clone())
        .unwrap()
}

/// B4: a request whose one-shot edge is still reading its body is in
/// flight (the core enters `InFlight` before `accept`).
#[tokio::test(flavor = "current_thread")]
async fn idle_waits_for_a_request_still_reading_its_body() {
    let gw = gateway_of(&Rec::default());
    let (_tx, req) = whole_upload();
    let mut handling = Box::pin(handle(&gw, req));
    assert!(futures::poll!(handling.as_mut()).is_pending());
    let idle = tokio::time::timeout(Duration::from_millis(100), gw.idle()).await;
    assert!(
        idle.is_err(),
        "idle resolved while a request was reading its body"
    );
    // once the request is given up, so is its count
    drop(handling);
    tokio::time::timeout(Duration::from_secs(2), gw.idle())
        .await
        .expect("idle after the request was dropped");
}

/// A request whose body fails while read is refused, and no plugin, none
/// having started, hears of it.
#[tokio::test(flavor = "current_thread")]
async fn a_request_whose_body_fails_is_refused_before_any_plugin() {
    let rec = Rec::default();
    let gw = gateway_of(&rec);
    let (tx, req) = whole_upload();
    send(&tx, "half");
    tx.unbounded_send(Err(Fault::bad_request("the client broke")))
        .unwrap();
    let (res, end) = handle(&gw, req).await;
    assert_eq!(read(res).await, Ok((400, "ERR bad_request".into())));
    gw.idle().await;
    assert_eq!(code(&end.await), "bad_request");
    let l = rec.log();
    assert_eq!(
        (l.client_start, l.client_end.len(), l.prov_start),
        (0, 0, 0)
    );
}

/// Building on a gateway while requests run gives it a count of its own.
#[tokio::test(flavor = "current_thread")]
async fn a_rebuilt_gateway_counts_only_its_own_requests() {
    let t = fnt(|_| async { Ok(answer(200, never())) }.boxed());
    let gw = Gateway::new()
        .client::<In, _, _>(Toy)
        .unwrap()
        .provider::<In, _, _>("a", Toy, connect(t))
        .unwrap()
        .target("a")
        .unwrap();
    let (res, _) = handle(&gw, req(vec!["q"])).await;
    let running = res.unwrap();
    let old = gw.clone();
    let rebuilt = gw.plugin("more", Rec::default()).unwrap();
    rebuilt.idle().await;
    let waiting = tokio::time::timeout(Duration::from_millis(100), old.idle()).await;
    assert!(
        waiting.is_err(),
        "the gateway that took the request stopped waiting for it"
    );
    drop(running);
    old.idle().await;
}

/// A response goes only to its own request.
struct Steal(Arc<Mutex<Option<Response>>>);
#[async_trait]
impl Plugin for Steal {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>> {
        let taken = lock(&self.0).take();
        match taken {
            Some(resp) => Ok(Some(resp)),
            None => {
                let mut resp = cx.call("a").await?;
                resp.buffer_until(|out: &Out| !out.0.starts_with("keepalive"))
                    .await?;
                *lock(&self.0) = Some(resp);
                Err(Fault::not_found("stash"))
            }
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn a_response_of_another_request_is_refused() {
    let rec = Rec::default();
    let stash = Arc::default();
    let gw = gateway_of(&rec)
        .plugin("steal", Steal(Arc::clone(&stash)))
        .unwrap();
    let (first, _) = handle(&gw, req(vec!["q"])).await;
    drop(first);
    let (second, _) = handle(&gw, req(vec!["q"])).await;
    assert_eq!(read(second).await, Ok((500, "ERR internal".into())));
    gw.idle().await;
    let l = rec.log();
    assert_eq!((l.client_start, l.client_end.len()), (2, 2));
}

/// Calls two ways: with an input of its own, then relaying the client's.
struct TwoCalls;
#[async_trait]
impl Plugin for TwoCalls {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>> {
        let own = cx.call("a").with_input(In("own".into()));
        own.await?.until_end().await?;
        Ok(Some(cx.call("a").await?))
    }
}

/// A dialer that notes whether each call it is given relays.
fn noting(seen: &Arc<Mutex<Vec<bool>>>) -> Dialer {
    let record = seen.clone();
    fnt(move |c| {
        lock(&record).push(c.relayed);
        async { Ok(ok_resp(vec!["a", "DONE"])) }.boxed()
    })
}

/// The kernel alone says a call relays the client's message.
#[tokio::test(flavor = "current_thread")]
async fn only_a_call_that_relays_the_clients_message_is_relayed() {
    let seen: Arc<Mutex<Vec<bool>>> = Arc::default();
    let gw = Gateway::new()
        .client::<In, _, _>(Toy)
        .unwrap()
        .provider::<In, _, _>("a", Toy, connect(noting(&seen)))
        .unwrap()
        .plugin("two", TwoCalls)
        .unwrap();
    let (res, _) = handle(&gw, req(vec!["q"])).await;
    let _ = read(res).await;
    assert_eq!(*lock(&seen), [false, true]);
}

/// Sends the client's own value, cloned, as a call's own input.
struct Cloned;
#[async_trait]
impl Plugin for Cloned {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>> {
        let copy = cx.input::<In>()?.clone();
        let own = cx.call("a").with_input(copy);
        Ok(Some(own.await?))
    }
}

/// Provenance, not value, decides a relay: a call whose own input is the
/// client's value cloned gets no client origin (no head, no key).
#[tokio::test(flavor = "current_thread")]
async fn a_call_of_the_clients_value_cloned_gets_no_client_origin() {
    let seen: Arc<Mutex<Vec<bool>>> = Arc::default();
    let gw = Gateway::new()
        .client::<In, _, _>(Toy)
        .unwrap()
        .provider::<In, _, _>("a", Toy, connect(noting(&seen)))
        .unwrap()
        .plugin("cloned", Cloned)
        .unwrap();
    let (res, _) = handle(&gw, req(vec!["q"])).await;
    assert_eq!(read(res).await, Ok((200, "a;!".into())));
    assert_eq!(*lock(&seen), [false]);
}

/// A job that outlives its request reaches the gateway through the
/// request's context and hands it a request of its own, which runs every
/// plugin; `idle` waits for it from the moment it is handed.
#[tokio::test(flavor = "current_thread")]
async fn a_job_hands_its_gateway_a_request_of_its_own() {
    #[derive(Clone)]
    struct Mark;

    /// Starts a job for each request that is not a job's.
    struct Detach;

    #[async_trait]
    impl Plugin for Detach {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>> {
            if cx.extensions().get::<Mark>().is_some() {
                return Ok(None);
            }
            let (wire, reply) = Io::accept(req(vec!["job"]));
            let mut marked = Extensions::new();
            marked.insert(Mark);
            // Handed here, awaited in the job's own task.
            let job = cx.gateway().handle(wire, marked);
            tokio::spawn(async move {
                let consume = async {
                    let reply = reply.await.map(|(reply, _)| reply);
                    let _ = read(reply.ok_or_else(|| Fault::internal("none"))).await;
                };
                let _ = futures::future::join(job, consume).await;
            });
            Ok(None)
        }
    }

    let rec = Rec::default();
    let gw = gateway_of(&rec).plugin("detach", Detach).unwrap();
    let (res, _end) = handle(&gw, req(vec!["hi"])).await;
    assert_eq!(read(res).await.map(|(s, _)| s), Ok(200));
    tokio::time::timeout(Duration::from_secs(2), gw.idle())
        .await
        .expect("idle once the job ended");
    let log = rec.log();
    assert_eq!(log.client_start, 2);
    assert_eq!(log.client_codes(), ["ok", "ok"]);
}

/// A request counts for `idle` from the moment `handle` is called, before
/// its future is polled (loop 3, bug 3).
#[tokio::test(flavor = "current_thread")]
async fn a_request_counts_from_the_moment_handle_is_called() {
    let gw = gateway_of(&Rec::default());
    let (wire, _reply) = Io::accept(req(vec!["q"]));
    let handling = gw.handle(wire, Extensions::new());
    assert!(
        gw.idle().now_or_never().is_none(),
        "idle resolved although handle had been called"
    );
    drop(handling);
    assert!(
        gw.idle().now_or_never().is_some(),
        "a dropped request counts no more"
    );
}

/// A job handed to the gateway from an end hook, whose task may run only
/// after the hook's own count is gone: `idle` must not resolve before the
/// job's request ends (loop 3, bug 3). Many rounds on several threads, as
/// the race is timing's.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn idle_waits_for_a_job_started_in_an_end_hook() {
    #[derive(Clone)]
    struct Mark;

    /// Counts each job's end; starts a job from each other request's end.
    struct Detach(Arc<AtomicUsize>);

    #[async_trait]
    impl Plugin for Detach {
        async fn on_client_end(&self, cx: &ClientContext, _end: &Result<()>) {
            if cx.extensions().get::<Mark>().is_some() {
                self.0.fetch_add(1, Ordering::SeqCst);
                return;
            }
            let (wire, reply) = Io::accept(req(vec!["job"]));
            let mut marked = Extensions::new();
            marked.insert(Mark);
            let job = cx.gateway().handle(wire, marked);
            tokio::spawn(async move {
                let consume = async {
                    let Some((reply, end)) = reply.await else {
                        return;
                    };
                    let _ = read(Ok(reply)).await;
                    let _ = end.await;
                };
                let _ = futures::future::join(job, consume).await;
            });
        }
    }

    let rounds = std::env::var("IDLE_ROUNDS").map_or(300, |n| n.parse().unwrap());
    let mut early = 0;
    for _ in 0..rounds {
        let done = Arc::new(AtomicUsize::new(0));
        let gw = gateway_of(&Rec::default())
            .plugin("detach", Detach(done.clone()))
            .unwrap();
        let (res, end) = handle(&gw, req(vec!["hi"])).await;
        let _ = read(res).await;
        let _ = end.await;
        tokio::time::timeout(Duration::from_secs(2), gw.idle())
            .await
            .unwrap();
        if done.load(Ordering::SeqCst) == 0 {
            early += 1;
        }
    }
    assert_eq!(
        early, 0,
        "idle resolved before the job ended in {early}/{rounds}"
    );
}
