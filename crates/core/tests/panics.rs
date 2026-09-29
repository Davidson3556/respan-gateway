//! A panic in user code ends the call or request `internal`, wherever it
//! runs, and never costs anyone an end hook.
#![allow(clippy::unwrap_used)]

mod toy;

use respan_gateway_core::futures::FutureExt;
use respan_gateway_core::*;
use toy::*;

/// B2: the request encoder panics on a later frame while the transport reads
/// the body in a task of its own, as reqwest does, where the kernel cannot
/// catch it by unwinding. The call still ends `internal`, and its body ends
/// with an `Err`.
#[tokio::test(flavor = "multi_thread")]
async fn a_request_encoder_panic_on_a_later_frame_ends_the_call_internal() {
    let seen = Seen::default();
    let rec = Rec::default();
    let gw = Gateway::new()
        .serve(Toy)
        .unwrap()
        .provider("a", Toy, seen.transport(true, done))
        .unwrap()
        .target("a")
        .unwrap()
        .plugin("rec", rec.clone())
        .unwrap();
    let (tx, req) = upload();
    send(&tx, "first");
    send(&tx, "ENCPANIC");
    let (res, end) = handle(&gw, req).await;
    let got = read(res).await;
    drop(tx);
    gw.idle().await;
    assert_eq!(
        seen.frames(),
        vec![Ok("first".to_string()), Err("internal".to_string())]
    );
    assert_eq!(rec.log().prov_codes(), ["internal"], "client got {got:?}");
    assert_eq!(rec.log().client_codes(), ["internal"]);
    assert_eq!(code(&end.await), "internal");
}

/// A panic in a decoder while `until_end` reads unwinds through the plugin
/// that awaits it, so the request is refused `internal` (rather than aborted
/// after its first bytes as a response read directly is), and the call ends
/// `internal`.
#[tokio::test(flavor = "current_thread")]
async fn a_decoder_panic_while_until_end_reads_refuses_the_request_internal() {
    for (mode, chunks) in [(2u8, vec!["a", "PANIC"]), (1, vec!["PANIC"])] {
        let rec = Rec::default();
        let t = fnt(move |_| {
            let chunks = chunks.clone();
            async move { Ok(Exchange::new(Ans(200), slow(chunks))) }.boxed()
        });
        let gw = Gateway::new()
            .serve(Toy)
            .unwrap()
            .provider("a", Toy, t)
            .unwrap()
            .plugin("rec", rec.clone())
            .unwrap()
            .plugin("mode", Mode(mode))
            .unwrap();
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

/// Fails a call in its hooks or its edit, before it is sent.
struct Fails(u8);
#[async_trait]
impl Plugin for Fails {
    async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
        match self.0 {
            1 => Err(Fault::internal("hook fails")),
            2 => {
                px.edit_call(|_: &mut Ask, _| Err(Fault::internal("edit fails")));
                Ok(())
            }
            _ => Ok(()),
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn a_call_is_sent_only_once_handed_to_its_transport() {
    // (how it fails before or at its transport, whether it was sent)
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
        let gw = Gateway::new()
            .serve(Toy)
            .unwrap()
            .provider("a", Toy, t)
            .unwrap()
            .target("a")
            .unwrap()
            .plugin("rec", rec.clone())
            .unwrap()
            .plugin("fails", Fails(mode))
            .unwrap();
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
