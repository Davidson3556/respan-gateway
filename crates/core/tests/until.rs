//! `buffer_until` and `until_end` read a response without changing what it
//! is. The toy dialer answers text events,
//! not bytes.
#![allow(clippy::unwrap_used)]

mod edge;
mod faults;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use edge::*;
use faults::says;
use respan_gateway_core::futures::{FutureExt, StreamExt, stream};
use respan_gateway_core::*;

type Script = Vec<std::result::Result<&'static str, ()>>;

/// What a client and the hooks saw of a provider that answers `script` with
/// `status`, its response read as `mode` says (see [`Mode`]).
async fn run(
    script: Script,
    status: u16,
    mode: u8,
) -> (
    std::result::Result<(u16, String), String>,
    Vec<String>,
    Vec<String>,
) {
    let rec = Rec::default();
    let script = Arc::new(script);
    let t = fnt(move |_| {
        let script = script.clone();
        async move {
            let items: Vec<_> = script
                .iter()
                .map(|s| s.map(str::to_string).map_err(|()| faults::upstream("cut")))
                .collect();
            Ok(answer(status, stream::iter(items).boxed()))
        }
        .boxed()
    });
    let gw = Gateway::new()
        .client::<In, _, _>(Toy)
        .unwrap()
        .provider(Provider::new::<In, _, _>("a", Toy, connect(t)))
        .unwrap()
        .plugin(rec.clone())
        .plugin(Mode(mode));
    let (res, _) = handle(&gw, req(vec!["q"])).await;
    let got = read(res).await;
    gw.idle().await;
    let l = rec.log();
    let ends = |ends: Vec<&Result<()>>| {
        ends.into_iter()
            .map(|e| {
                e.as_ref()
                    .map_err(|f| (faults::code(f).to_string(), edge::number(f)))
                    .map_or_else(|e| format!("{e:?}"), |()| "ok".into())
            })
            .collect()
    };
    let client = ends(l.client_end.iter().collect());
    let provider = ends(l.prov_end.iter().map(|(_, e)| e).collect());
    (got, client, provider)
}

/// Read to its content or its end and sent, a response is as it would have
/// been unread: the same bytes, the same ends.
#[tokio::test(flavor = "current_thread")]
async fn reading_a_response_first_changes_nothing() {
    let scripts: Vec<(&str, Script, u16)> = vec![
        ("complete", vec![Ok("a"), Ok("b"), Ok("DONE")], 200),
        ("err-after-content", vec![Ok("a"), Ok("ERR")], 200),
        ("err-first", vec![Ok("ERR")], 200),
        ("truncated", vec![Ok("a"), Ok("b")], 200),
        ("truncated-empty", vec![], 200),
        ("only-done", vec![Ok("DONE")], 200),
        ("cut-after-content", vec![Ok("a"), Err(())], 200),
        ("cut-first", vec![Err(())], 200),
        ("status-500", vec![Ok("x")], 500),
        (
            "content-after-done",
            vec![Ok("a"), Ok("DONE"), Ok("zzz")],
            200,
        ),
        (
            "keepalives",
            vec![Ok("keepalive"), Ok("keepalive"), Ok("a"), Ok("DONE")],
            200,
        ),
        ("keepalives-only", vec![Ok("keepalive"), Ok("DONE")], 200),
    ];
    let mut bad = vec![];
    for (name, script, status) in scripts {
        let direct = run(script.clone(), status, 0).await;
        for mode in [1u8, 2] {
            let read = run(script.clone(), status, mode).await;
            if direct != read {
                bad.push(format!(
                    "{name} mode {mode}:\n  direct {direct:?}\n  read   {read:?}"
                ));
            }
        }
    }
    assert!(bad.is_empty(), "{}", bad.join("\n"));
}

/// `buffer_until` on a response with no content reads and holds all of it,
/// to its end, and what it held is sent whole.
#[tokio::test(flavor = "current_thread")]
async fn buffer_until_holds_a_response_without_content_to_its_end() {
    let reads = Arc::new(AtomicUsize::new(0));
    let counted = reads.clone();
    let t = fnt(move |_| {
        let counted = counted.clone();
        async move {
            let body = stream::iter(0..1000).map(move |i| {
                counted.fetch_add(1, Ordering::SeqCst);
                Ok((if i == 999 { "DONE" } else { "keepalive" }).to_string())
            });
            Ok(answer(200, body.boxed()))
        }
        .boxed()
    });
    let at_return = Arc::new(AtomicUsize::new(usize::MAX));
    struct Probe(Arc<AtomicUsize>, Arc<AtomicUsize>);
    #[async_trait]
    impl Plugin for Probe {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>> {
            let mut resp = cx.call("a").await?;
            resp.buffer_until(|out: &Out| !out.0.starts_with("keepalive"))
                .await?;
            self.1
                .store(self.0.load(Ordering::SeqCst), Ordering::SeqCst);
            Ok(Some(resp))
        }
    }
    let gw = Gateway::new()
        .client::<In, _, _>(Toy)
        .unwrap()
        .provider(Provider::new::<In, _, _>("a", Toy, connect(t)))
        .unwrap()
        .plugin(Probe(reads.clone(), at_return.clone()));
    let (res, _) = handle(&gw, req(vec!["q"])).await;
    let (_, body) = read(res).await.unwrap();
    assert_eq!(
        at_return.load(Ordering::SeqCst),
        1000,
        "read to the end before it returned"
    );
    assert_eq!(body, format!("{}!", "keepalive;".repeat(999)));
}

#[tokio::test]
async fn a_buffer_predicate_panic_settles_both_edges_as_internal() {
    struct Panic;
    #[async_trait]
    impl Plugin for Panic {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>> {
            let mut response = cx.call("a").await?;
            response
                .buffer_until(|_: &Out| panic!("predicate failed"))
                .await?;
            Ok(Some(response))
        }
    }
    let rec = Rec::default();
    let transport = fnt(|_| async { Ok(answer(200, slow(vec!["a", "DONE"]))) }.boxed());
    let gateway = Gateway::new()
        .client::<In, _, _>(Toy)
        .unwrap()
        .provider(Provider::new::<In, _, _>("a", Toy, connect(transport)))
        .unwrap()
        .plugin(rec.clone())
        .plugin(Panic);
    let (response, end) = handle(&gateway, req(vec!["q"])).await;
    assert_eq!(read(response).await, Ok((500, "ERR internal".into())));
    assert_eq!(code(&end.await), "internal");
    gateway.idle().await;
    assert_eq!(rec.log().prov_codes(), ["internal"]);
    assert_eq!(rec.log().client_codes(), ["internal"]);
}

#[tokio::test]
async fn buffered_filter_end_is_known_before_hooks_and_replayed_once() {
    struct Tail {
        ends: Arc<AtomicUsize>,
        fail: bool,
    }
    impl Filter<Out> for Tail {
        fn edit(&mut self, _: Out, _: &mut Vec<Out>) -> Result<()> {
            Ok(())
        }
        fn end(&mut self, _: &Result<()>, out: &mut Vec<Out>) -> Result<()> {
            self.ends.fetch_add(1, Ordering::SeqCst);
            if self.fail {
                return Err(faults::upstream("failed-at-end"));
            }
            out.push(Out("tail".into()));
            Ok(())
        }
    }
    struct Buffer {
        ends: Arc<AtomicUsize>,
        rec: Rec,
        fail: bool,
        discard: bool,
    }
    #[async_trait]
    impl Plugin for Buffer {
        async fn on_provider(&self, px: &mut ProviderContext) -> Result<()> {
            px.filter(Tail {
                ends: self.ends.clone(),
                fail: self.fail,
            })
        }
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>> {
            let mut response = cx.call("a").await?;
            let end = response.until_end().await;
            let expected = if self.fail {
                Err("failed-at-end".to_owned())
            } else {
                Ok(())
            };
            assert_eq!(says(end.clone()), expected);
            assert_eq!(self.ends.load(Ordering::SeqCst), 1);
            assert!(self.rec.log().prov_end.is_empty(), "hooks stay deferred");
            // Repeated buffering must not run Filter::end again or lose its tail.
            let again = response.until_end().await;
            assert_eq!(says(again), expected);
            if self.discard {
                drop(response);
                Ok(Some(Response::stream(stream::empty::<Result<Out>>())))
            } else {
                Ok(Some(response))
            }
        }
    }
    for fail in [false, true] {
        for discard in [false, true] {
            let ends = Arc::new(AtomicUsize::new(0));
            let rec = Rec::default();
            let transport = fnt(|_| async { Ok(answer(200, slow(vec!["a", "DONE"]))) }.boxed());
            let gateway = Gateway::new()
                .client::<In, _, _>(Toy)
                .unwrap()
                .provider(Provider::new::<In, _, _>("a", Toy, connect(transport)))
                .unwrap()
                .plugin(rec.clone())
                .plugin(Buffer {
                    ends: ends.clone(),
                    rec: rec.clone(),
                    fail,
                    discard,
                });
            let (response, end) = handle(&gateway, req(vec!["q"])).await;
            let (_, body) = read(response).await.unwrap();
            let _ = end.await;
            gateway.idle().await;
            assert_eq!(ends.load(Ordering::SeqCst), 1);
            assert_eq!(
                rec.log().prov_codes(),
                [if fail { "upstream" } else { "ok" }]
            );
            if !fail && !discard {
                assert_eq!(body, "tail;!");
            }
        }
    }
}
