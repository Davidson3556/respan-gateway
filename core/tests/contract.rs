//! The edge contract on the toy edge: the provider edge's staged answer
//! (writer preparation, opening and refusal), a complete refusal before
//! upgrade, and lifecycle behavior when outputs are dropped unread.
#![allow(clippy::unwrap_used)]

mod edge;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use edge::*;
use respan_gateway_core::futures::FutureExt;
use respan_gateway_core::*;

fn gateway_of(rec: &Rec, dialer: Dialer) -> Gateway {
    Gateway::new()
        .client::<In, _, _>(Toy)
        .unwrap()
        .provider::<In, _, _>("a", Toy, connect(dialer))
        .unwrap()
        .target("a")
        .unwrap()
        .plugin(rec.clone())
}

/// A provider that refuses a relayed call at its head, as a 403 does.
fn refusing() -> Dialer {
    fnt(|c| {
        assert!(!c.relayed, "same-name calls have no implicit relay");
        async { Ok(answer(403, chunks(vec!["denied"]))) }.boxed()
    })
}

/// A streaming client (an upgrade: its head goes when the reply
/// resolves) relayed to a provider that refuses at its head gets a
/// complete refusal, not a head and then a failing body.
#[tokio::test(flavor = "multi_thread")]
async fn a_refused_upgrade_is_a_complete_answer() {
    let rec = Rec::default();
    let gw = gateway_of(&rec, refusing());
    let (tx, req) = upload();
    send(&tx, "hello");
    let (res, end) = handle(&gw, req).await;
    assert_eq!(read(res).await, Ok((403, "ERR x".into())));
    gw.idle().await;
    assert_eq!(code(&end.await), "x");
    let l = rec.log();
    assert_eq!(l.prov_codes(), ["x"]);
    assert_eq!(l.head_only, [true], "refused at the head");
    assert!(l.prov_end[0].0, "sent");
    drop(tx);
}

/// The same for a one-shot client, and a plugin that holds the answer:
/// `buffer_until` fails at once, and the plugin's refusal is as clean.
#[tokio::test(flavor = "multi_thread")]
async fn a_refusal_at_the_head_is_clean_whoever_holds_it() {
    for mode in [0u8, 1, 2] {
        let rec = Rec::default();
        let gw = gateway_of(&rec, refusing()).plugin(Mode(mode));
        let (res, _) = handle(&gw, req(vec!["q"])).await;
        assert_eq!(read(res).await, Ok((403, "ERR x".into())), "mode {mode}");
        gw.idle().await;
        assert_eq!(rec.log().head_only, [true], "mode {mode}");
    }
}

/// The contrast: a provider that opens and then fails its first event.
/// A streaming client's head is already out, so the failure ends the body;
/// the call was not refused at its head.
#[tokio::test(flavor = "multi_thread")]
async fn a_failure_after_the_head_ends_the_body() {
    let rec = Rec::default();
    let failing = fnt(|_| async { Ok(answer(200, chunks(vec!["ERR"]))) }.boxed());
    let gw = gateway_of(&rec, failing);
    let (tx, req) = upload();
    send(&tx, "hello");
    let (res, _) = handle(&gw, req).await;
    assert_eq!(read(res).await, Ok((200, "ERR upstream".into())));
    gw.idle().await;
    assert_eq!(rec.log().head_only, [false]);
    drop(tx);
}

/// An edge that writes a head alone and drops the outputs unread
/// (a bodiless answer) ends their call `Ok` if its end was ready, else
/// abandoned; the request ends as the edge says.
#[tokio::test(flavor = "multi_thread")]
async fn outputs_dropped_unread_end_their_call_as_ready() {
    for (body, want) in [(vec!["DONE"], "ok"), (vec!["a", "DONE"], "abandoned")] {
        let rec = Rec::default();
        let body = std::sync::Arc::new(body);
        let t = fnt(move |_| {
            let body = body.clone();
            async move { Ok(answer(200, slow_after_first(&body))) }.boxed()
        });
        let gw = gateway_of(&rec, t);
        let (tx, mut req) = upload();
        req.quirk = 2;
        let (res, end) = handle(&gw, req).await;
        assert_eq!(read(res).await, Ok((200, String::new())));
        gw.idle().await;
        assert_eq!(code(&end.await), "ok");
        assert_eq!(rec.log().prov_codes(), [want]);
        drop(tx);
    }
}

/// `DONE` alone is ready at once; anything else waits a turn first.
fn slow_after_first(body: &[&'static str]) -> Text {
    match body {
        ["DONE"] => chunks(vec!["DONE"]),
        _ => slow(body.to_vec()),
    }
}

/// Fails a call in its hook, or makes the toy writer's preparation fail.
struct Fails(u8);
#[async_trait]
impl Plugin for Fails {
    async fn on_provider(&self, px: &mut ProviderContext) -> Result<()> {
        match self.0 {
            1 => Err(Fault::internal("hook fails")),
            2 => {
                px.extensions_mut().insert(Unsendable);
                Ok(())
            }
            _ => Ok(()),
        }
    }
}

/// Writer stages: a preparation error is not sent; a future error is sent,
/// with no answer; an answer is sent. Mode 2 fails preparation.
#[tokio::test(flavor = "current_thread")]
async fn a_call_is_sent_only_once_handed_to_its_transport() {
    // (how it fails before or at its dialer, whether it was sent)
    for (mode, transport_fails, sent) in [
        (1u8, false, false),
        (2, false, false),
        (0, true, true),
        (0, false, true),
    ] {
        let rec = Rec::default();
        let t = fnt(move |_| {
            async move {
                match transport_fails {
                    true => Err(Fault::upstream("down")),
                    false => Ok(ok_resp(vec!["a", "DONE"])),
                }
            }
            .boxed()
        });
        let gw = gateway_of(&rec, t).plugin(Fails(mode));
        let (res, _) = handle(&gw, req(vec!["q"])).await;
        let _ = read(res).await;
        gw.idle().await;
        let ends: Vec<bool> = rec.log().prov_end.iter().map(|(s, _)| *s).collect();
        assert_eq!(
            ends,
            [sent],
            "mode {mode} transport_fails {transport_fails}"
        );
    }
}

/// A panic in the provider edge's stream while `until_end` or
/// `buffer_until` reads unwinds through the plugin that awaits it, so the
/// request is refused `internal`, and the call ends `internal`, as a
/// decoder's panic does in HTTP's driver.
#[tokio::test(flavor = "current_thread")]
async fn a_provider_stream_panic_while_until_end_reads_refuses_internal() {
    for (mode, chunks) in [(2u8, vec!["a", "PANIC"]), (1, vec!["PANIC"])] {
        let rec = Rec::default();
        let t = fnt(move |_| {
            let chunks = chunks.clone();
            async move { Ok(answer(200, slow(chunks))) }.boxed()
        });
        let gw = Gateway::new()
            .client::<In, _, _>(Toy)
            .unwrap()
            .provider::<In, _, _>("a", Toy, connect(t))
            .unwrap()
            .plugin(rec.clone())
            .plugin(Mode(mode));
        let (res, end) = handle(&gw, req(vec!["q"])).await;
        assert_eq!(
            read(res).await,
            Ok((500, "ERR internal".into())),
            "mode {mode}"
        );
        gw.idle().await;
        assert_eq!(rec.log().prov_codes(), ["internal"], "mode {mode}");
        assert_eq!(rec.log().client_codes(), ["internal"], "mode {mode}");
        assert_eq!(code(&end.await), "internal");
    }
}

/// Answers with outputs that never end, a turn apart.
struct Endless;
#[async_trait]
impl Plugin for Endless {
    async fn on_client(&self, _cx: &mut ClientContext) -> Result<Option<Response>> {
        let endless = futures::stream::unfold((), |()| async {
            tokio::task::yield_now().await;
            Some((Ok::<_, Fault>(Out("x".into())), ()))
        });
        Ok(Some(Response::stream(endless)))
    }
}

/// A writer whose completion channel disappears ends the exchange abandoned,
/// and its outputs end `Err(abandoned)`, however much the answer had left.
#[tokio::test(flavor = "multi_thread")]
async fn outputs_end_abandoned_once_the_writer_loses_completion() {
    for streams in [false, true] {
        let rec = Rec::default();
        let gw = Gateway::new()
            .client::<In, _, _>(Toy)
            .unwrap()
            .plugin(rec.clone())
            .plugin(Endless);
        let mut req = req_of(vec!["q"], streams);
        req.quirk = 1;
        let (res, end) = handle(&gw, req).await;
        let got = tokio::time::timeout(Duration::from_secs(2), read(res)).await;
        let (_, body) = got.expect("the outputs never ended").unwrap();
        assert!(body.ends_with("ERR abandoned"), "streams {streams}: {body}");
        assert_eq!(code(&end.await), "abandoned");
        gw.idle().await;
        assert_eq!(rec.log().client_codes(), ["abandoned"]);
    }
}

/// Refuses an output `bad` on the client edge, and notes every output its
/// calls' filters see.
struct Meter(Arc<Mutex<Vec<String>>>);
#[async_trait]
impl Plugin for Meter {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>> {
        cx.filter(|o: &mut Out| match o.0.as_str() {
            "bad" => Err(Fault::bad_request("blocked")),
            _ => Ok(()),
        })?;
        Ok(None)
    }
    async fn on_provider(&self, px: &mut ProviderContext) -> Result<()> {
        let seen = self.0.clone();
        px.filter(move |o: &mut Out| {
            lock(&seen).push(o.0.clone());
            Ok(())
        })
    }
}

/// What a provider edge has ready at once is one batch: its call's filters
/// see all of it before the client's see its first, so an output that
/// shares a batch with one the client's filter fails on is still seen, as
/// usage sharing a frame with it is metered. What it says `Pending` between
/// is read apart.
#[tokio::test(flavor = "current_thread")]
async fn outputs_ready_together_are_one_batch() {
    let cases = [
        (chunks(vec!["bad", "usage", "DONE"]), vec!["bad", "usage"]),
        (slow(vec!["bad", "usage", "DONE"]), vec!["bad"]),
    ];
    for (body, want) in cases {
        let seen = Arc::default();
        let body = Mutex::new(Some(body));
        let t = fnt(move |_| {
            let body = lock(&body).take().unwrap();
            async move { Ok(answer(200, body)) }.boxed()
        });
        let gw = gateway_of(&Rec::default(), t).plugin(Meter(Arc::clone(&seen)));
        let (res, _) = handle(&gw, req(vec!["q"])).await;
        assert_eq!(read(res).await, Ok((400, "ERR bad_request".into())));
        gw.idle().await;
        assert_eq!(*lock(&seen), want);
    }
}
