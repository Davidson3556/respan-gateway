//! What a gateway counts as in flight, and what a session may be given back.
//! On the toy edge, with relay provenance.
#![allow(clippy::unwrap_used)]

mod edge;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use edge::*;
use respan_gateway_core::futures::FutureExt;
use respan_gateway_core::*;

fn gateway_of(rec: &Rec) -> Gateway {
    Gateway::new()
        .serve(Toy)
        .unwrap()
        .provider(
            "a",
            Toy,
            fnt(|_| async { Ok(ok_resp(vec!["a", "DONE"])) }.boxed()),
        )
        .unwrap()
        .target("a")
        .unwrap()
        .plugin("rec", rec.clone())
        .unwrap()
}

/// B4: a session whose one-shot edge is still reading its body is in
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

/// Building on a gateway while sessions run gives it a count of its own.
#[tokio::test(flavor = "current_thread")]
async fn a_rebuilt_gateway_counts_only_its_own_requests() {
    let t = fnt(|_| async { Ok(answer(200, never())) }.boxed());
    let gw = Gateway::new()
        .serve(Toy)
        .unwrap()
        .provider("a", Toy, t)
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

/// A response goes only to its own session.
struct Steal(Arc<Mutex<Option<Response>>>);
#[async_trait]
impl Plugin for Steal {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let taken = lock(&self.0).take();
        match taken {
            Some(resp) => Ok(Some(resp)),
            None => {
                let resp = cx.call("a").await?.until_content().await?;
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
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let own = cx.call("a").with_input(In("own".into()));
        let _ = own.await?.until_end().await?;
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
        .serve(Toy)
        .unwrap()
        .provider("a", Toy, noting(&seen))
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
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
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
        .serve(Toy)
        .unwrap()
        .provider("a", Toy, noting(&seen))
        .unwrap()
        .plugin("cloned", Cloned)
        .unwrap();
    let (res, _) = handle(&gw, req(vec!["q"])).await;
    assert_eq!(read(res).await, Ok((200, "a;!".into())));
    assert_eq!(*lock(&seen), [false]);
}
