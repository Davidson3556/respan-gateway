//! Many sessions at once, and sessions cut at every kind of point: nothing
//! leaks, and every hook runs once. Deterministic: the points come from a
//! fixed sequence, the work is local, and the whole file takes seconds.
#![allow(clippy::unwrap_used)]

mod support;

use std::collections::BTreeMap;
use std::sync::atomic::Ordering;

use support::*;

const PATH: &str = "/v1/realtime";

fn counts<'a>(ends: impl Iterator<Item = &'a str>) -> BTreeMap<String, usize> {
    let mut all = BTreeMap::new();
    for end in ends {
        *all.entry(end.to_owned()).or_default() += 1;
    }
    all
}

fn probe_behaviour() -> Behaviour {
    Behaviour {
        echo: true,
        commands: true,
        ..Behaviour::default()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn many_concurrent_sessions_each_get_their_own_frames_in_order() {
    const SESSIONS: usize = 64;
    const FRAMES: usize = 25;
    let probe = Probe::with(probe_behaviour()).await;
    let (gateway, ends, calls) = relay(&probe.base());
    let server = serve(gateway).await;
    let addr = server.addr;
    let sessions: Vec<_> = (0..SESSIONS)
        .map(|i| {
            tokio::spawn(async move {
                let mut ws = connect(addr, PATH, &[]).await.unwrap();
                for k in 0..FRAMES {
                    say(&mut ws, &format!("{i}-{k}")).await;
                }
                for k in 0..FRAMES {
                    assert_eq!(heard_text(&mut ws).await, format!("echo:{i}-{k}"));
                }
                close(&mut ws, 1000, "").await;
                assert!(matches!(
                    hear(&mut ws).await,
                    Heard::Closed(_) | Heard::Broken
                ));
            })
        })
        .collect();
    for session in sessions {
        session.await.unwrap();
    }
    idle(&server.gateway).await;
    assert_eq!(probe.total.load(Ordering::SeqCst), SESSIONS);
    eventually("every provider connection closed", || {
        probe.live.load(Ordering::SeqCst) == 0
    })
    .await;
    assert_eq!(
        counts(ends.all().iter().map(code)),
        BTreeMap::from([("ok".into(), SESSIONS)])
    );
    assert_eq!(
        counts(calls.all().iter().map(code)),
        BTreeMap::from([("ok".into(), SESSIONS)])
    );
}

/// The next number of a fixed sequence.
fn next(state: &mut u64) -> u64 {
    *state = state
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1442695040888963407);
    *state >> 33
}

// Each session is cut at a point of a fixed pseudo-random sequence: the
// client drops or closes, the provider drops or closes, after 0 to 5 frames.
// Each request and each call ends once, as its cut says.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_sweep_of_cuts_leaks_nothing_and_ends_every_hook_once() {
    const SESSIONS: usize = 96;
    let probe = Probe::with(probe_behaviour()).await;
    let (gateway, ends, calls) = relay(&probe.base());
    let server = serve(gateway).await;
    let addr = server.addr;
    let mut state = 7;
    let mut expected = Vec::new();
    let mut sessions = Vec::new();
    for _ in 0..SESSIONS {
        let (cut, after) = (next(&mut state) % 4, (next(&mut state) % 6) as usize);
        expected.push(["abandoned", "ok", "truncated", "upstream"][cut as usize]);
        sessions.push(tokio::spawn(async move {
            let mut ws = connect(addr, PATH, &[]).await.unwrap();
            for k in 0..after {
                say(&mut ws, &format!("m{k}")).await;
                assert_eq!(heard_text(&mut ws).await, format!("echo:m{k}"));
            }
            match cut {
                // The client drops.
                0 => drop(ws),
                // The client closes.
                1 => {
                    close(&mut ws, 1000, "").await;
                    assert!(matches!(
                        hear(&mut ws).await,
                        Heard::Closed(_) | Heard::Broken
                    ));
                }
                // The provider drops: the client is told 1011.
                2 => {
                    say(&mut ws, "!drop").await;
                    let heard = hear(&mut ws).await;
                    assert_eq!(heard, Heard::Closed(Some((1011, "truncated".into()))));
                }
                // The provider closes with 1001: the client is told 1001.
                _ => {
                    say(&mut ws, "!close").await;
                    let heard = hear(&mut ws).await;
                    assert_eq!(heard, Heard::Closed(Some((1001, "upstream".into()))));
                }
            }
        }));
    }
    for session in sessions {
        session.await.unwrap();
    }
    idle(&server.gateway).await;
    eventually("every provider connection closed", || {
        probe.live.load(Ordering::SeqCst) == 0
    })
    .await;
    // Every end hook has run once by the time the gateway is idle.
    let want = counts(expected.iter().copied());
    assert_eq!(counts(ends.all().iter().map(code)), want);
    assert_eq!(counts(calls.all().iter().map(code)), want);
    assert_eq!(ends.all().len(), SESSIONS);
    assert_eq!(probe.total.load(Ordering::SeqCst), SESSIONS);
}

// The same against a provider's real protocol, cut in the middle of a
// response: the client drops after 0 to 7 events of it, or the provider cuts
// its stream after some. Whatever the cut, each hook ends once and the gateway
// goes idle.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_sweep_of_cuts_in_the_middle_of_a_response_ends_every_hook_once() {
    use mockllm::{Answer, FakeOpenAi};
    const SESSIONS: usize = 24;
    let openai = FakeOpenAi::start().await;
    let mut state = 11;
    let mut cuts = Vec::new();
    for i in 0..SESSIONS {
        let after = (next(&mut state) % 8) as usize;
        cuts.push(after);
        let answer = Answer::text("one two three four");
        // Headed the responses are cut by the provider, without a close frame.
        openai.answer(if i % 2 == 0 {
            answer.cut_after(after + 2)
        } else {
            answer
        });
    }
    let base = openai.base_url().replace("http://", "ws://");
    let (gateway, ends, calls) = relay(base.trim_end_matches("/v1"));
    let server = serve(gateway).await;
    let addr = server.addr;
    let sessions: Vec<_> = cuts
        .into_iter()
        .map(|after| {
            tokio::spawn(async move {
                let key = [("authorization", "Bearer sk-test")];
                let mut ws = connect(addr, "/v1/realtime?model=m", &key).await.unwrap();
                heard_text(&mut ws).await;
                say(&mut ws, r#"{"type":"response.create"}"#).await;
                for _ in 0..after {
                    // A cut provider ends the stream early: stop listening then.
                    if !matches!(hear(&mut ws).await, Heard::Text(_)) {
                        return;
                    }
                }
                drop(ws);
            })
        })
        .collect();
    for session in sessions {
        session.await.unwrap();
    }
    idle(&server.gateway).await;
    assert_eq!(ends.all().len(), SESSIONS);
    assert_eq!(calls.all().len(), SESSIONS);
    let sorted = counts(ends.all().iter().map(code));
    assert!(
        sorted
            .keys()
            .all(|k| ["abandoned", "truncated"].contains(&k.as_str())),
        "{sorted:?}"
    );
}
