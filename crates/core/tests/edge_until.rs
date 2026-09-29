//! `until_content` and `until_end` read a response without changing what it
//! is, and in time linear in its size. The toy edge's twin of `until.rs`:
//! the dialer answers text events, not bytes.
#![allow(clippy::unwrap_used)]

mod edge;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use edge::*;
use respan_gateway_core::futures::{FutureExt, StreamExt, stream};
use respan_gateway_core::*;

type Script = Vec<Result<&'static str, ()>>;

/// What a client and the hooks saw of a provider that answers `script` with
/// `status`, its response read as `mode` says (see [`Mode`]).
async fn run(
    script: Script,
    status: u16,
    mode: u8,
) -> (Result<(u16, String), String>, Vec<String>, Vec<String>) {
    let rec = Rec::default();
    let script = Arc::new(script);
    let t = fnt(move |_| {
        let script = script.clone();
        async move {
            let items: Vec<_> = script
                .iter()
                .map(|s| s.map(str::to_string).map_err(|()| Fault::upstream("cut")))
                .collect();
            Ok(answer(status, stream::iter(items).boxed()))
        }
        .boxed()
    });
    let gw = Gateway::new()
        .serve_dialect(Toy)
        .unwrap()
        .provider_dialect("a", Toy, t)
        .unwrap()
        .plugin("rec", rec.clone())
        .unwrap()
        .plugin("mode", Mode(mode))
        .unwrap();
    let (res, _) = handle(&gw, req(vec!["q"])).await;
    let got = read(res).await;
    gw.idle().await;
    let l = rec.log();
    let ends = |ends: Vec<&Result<(), Fault>>| {
        ends.into_iter()
            .map(|e| {
                e.as_ref()
                    .map_err(|f| (f.code().to_string(), edge::number(f)))
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

/// The time to read `n` events of a response as `mode` says; the best of a
/// few runs, so a noisy machine cannot fail it.
async fn time_of(mode: u8, lead: &'static str, n: usize) -> Duration {
    let mut best = Duration::MAX;
    for _ in 0..3 {
        let t = fnt(move |_| {
            async move {
                let chunks = stream::iter((0..n).map(move |_| Ok(lead.to_string())))
                    .chain(stream::iter([Ok("x".to_string()), Ok("DONE".to_string())]));
                Ok(answer(200, chunks.boxed()))
            }
            .boxed()
        });
        let gw = Gateway::new()
            .serve_dialect(Toy)
            .unwrap()
            .provider_dialect("a", Toy, t)
            .unwrap()
            .plugin("mode", Mode(mode))
            .unwrap();
        let start = Instant::now();
        let (res, _) = handle(&gw, req(vec!["q"])).await;
        let (_, body) = read(res).await.unwrap();
        best = best.min(start.elapsed());
        assert_eq!(body.len(), n * (lead.len() + 1) + "x;!".len());
    }
    best
}

/// B3: a read to the end, or through a run without content, takes time in
/// proportion to the response, not its square.
#[tokio::test(flavor = "current_thread")]
async fn reading_until_is_linear_in_the_chunks() {
    for (mode, lead) in [(2u8, "c"), (1, "keepalive")] {
        let n = 12_000;
        let (one, two) = (
            time_of(mode, lead, n).await,
            time_of(mode, lead, 2 * n).await,
        );
        assert!(
            two < one * 3,
            "mode {mode}: {n} chunks took {one:?}, {} took {two:?}",
            2 * n
        );
    }
}

/// `until_content` on a response with no content reads and holds all of it,
/// to its end, and what it held is sent whole.
#[tokio::test(flavor = "current_thread")]
async fn until_content_holds_a_response_without_content_to_its_end() {
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
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let resp = cx.call("a").await?.until_content().await?;
            self.1
                .store(self.0.load(Ordering::SeqCst), Ordering::SeqCst);
            Ok(Some(resp))
        }
    }
    let gw = Gateway::new()
        .serve_dialect(Toy)
        .unwrap()
        .provider_dialect("a", Toy, t)
        .unwrap()
        .plugin("probe", Probe(reads.clone(), at_return.clone()))
        .unwrap();
    let (res, _) = handle(&gw, req(vec!["q"])).await;
    let (_, body) = read(res).await.unwrap();
    assert_eq!(
        at_return.load(Ordering::SeqCst),
        1000,
        "read to the end before it returned"
    );
    assert_eq!(body, format!("{}!", "keepalive;".repeat(999)));
}
