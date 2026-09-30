#![cfg(feature = "ws")]
//! A slow reader does not make the gateway buffer without bound: a session's
//! frames are pulled one at a time from a socket, through the kernel, to a
//! socket, so a peer that stops reading stops the frames at their source,
//! once the sockets' own buffers are full.
#![allow(clippy::unwrap_used)]

mod support;

use std::sync::atomic::Ordering;
use std::time::Duration;

use support::*;

const PATH: &str = "/v1/realtime";
const FRAMES: usize = 20_000;
const SIZE: usize = 4096;

/// The frames of a flood, in bytes.
const TOTAL: usize = FRAMES * SIZE;

fn mib(bytes: usize) -> usize {
    bytes / (1024 * 1024)
}

async fn stalled(what: &str, progress: impl Fn() -> usize) -> usize {
    // Wait until the count stops growing.
    let mut last = progress();
    for _ in 0..100 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let now = progress();
        if now == last && now > 0 {
            return now;
        }
        last = now;
    }
    panic!("{what}: never stalled, it reached {last}");
}

// The provider floods and the client does not read: the flood stalls at the
// sockets' buffers, far short of its size, and the client then reads all of
// it, none lost or reordered.
#[tokio::test(flavor = "multi_thread")]
async fn a_client_that_does_not_read_stalls_the_provider() {
    let probe = Probe::with(Behaviour {
        commands: true,
        ..Behaviour::default()
    })
    .await;
    let (gateway, ends, _) = relay(&probe.base());
    let server = serve(gateway).await;
    let mut ws = connect(server.addr, PATH, &[]).await.unwrap();
    say(&mut ws, &format!("!flood:{FRAMES}:{SIZE}")).await;
    let sent = stalled("the provider", || probe.sent.load(Ordering::SeqCst)).await;
    eprintln!(
        "provider sent {} of {FRAMES} frames ({} MiB) before it stalled",
        sent,
        mib(sent * SIZE)
    );
    assert!(
        sent * SIZE < TOTAL / 2,
        "buffered {} of {} MiB",
        mib(sent * SIZE),
        mib(TOTAL)
    );
    // It resumes: everything arrives in order.
    for i in 0..FRAMES {
        let frame = heard_text(&mut ws).await;
        assert!(
            frame.starts_with(&format!("{i:08}:")),
            "{i}: {}",
            &frame[..20]
        );
        assert_eq!(frame.len(), SIZE);
    }
    close(&mut ws, 1000, "").await;
    idle(&server.gateway).await;
    assert_eq!(ends.all().len(), 1);
}

// The client floods and the provider does not read: the client's sends stall
// at the sockets' buffers, and all arrive, in order, once the provider reads.
#[tokio::test(flavor = "multi_thread")]
async fn a_provider_that_does_not_read_stalls_the_client() {
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;

    let probe = Probe::start().await;
    probe.reading.store(false, Ordering::SeqCst);
    let (gateway, ends, _) = relay(&probe.base());
    let server = serve(gateway).await;
    let mut ws = connect(server.addr, PATH, &[]).await.unwrap();
    let sent = Arc::new(AtomicUsize::new(0));
    let counter = sent.clone();
    let sender = tokio::spawn(async move {
        for i in 0..FRAMES {
            let head = format!("{i:08}:");
            say(&mut ws, &format!("{head}{}", "x".repeat(SIZE - head.len()))).await;
            counter.fetch_add(1, Ordering::SeqCst);
        }
        ws
    });
    let done = stalled("the client", || sent.load(Ordering::SeqCst)).await;
    eprintln!(
        "client sent {done} of {FRAMES} frames ({} MiB) before it stalled",
        mib(done * SIZE)
    );
    assert!(
        done * SIZE < TOTAL / 2,
        "buffered {} of {} MiB",
        mib(done * SIZE),
        mib(TOTAL)
    );
    probe.reading.store(true, Ordering::SeqCst);
    let mut ws = sender.await.unwrap();
    eventually("the provider read every frame", || {
        probe.texts().len() == FRAMES
    })
    .await;
    let texts = probe.texts();
    for (i, text) in texts.iter().enumerate() {
        assert!(text.starts_with(&format!("{i:08}:")), "{i}");
    }
    close(&mut ws, 1000, "").await;
    idle(&server.gateway).await;
    assert_eq!(ends.all().len(), 1);
}
