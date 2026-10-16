//! A live encoder writes zero or more messages of each value, a frame each
//! ([`Encoder::encode`]), at both edges, and takes the next value
//! only once its reader has read the last one's.
#![allow(clippy::unwrap_used)]

use std::task::{Context, Poll, Waker};
use std::time::Duration;

use respan_gateway_core::futures::channel::oneshot;
use respan_gateway_core::futures::{FutureExt, SinkExt, StreamExt, stream};
use respan_gateway_core::{EventSink, EventStream, Extensions, Fault};
use respan_gateway_http::bytes::Bytes;
use respan_gateway_http::{Body, ClientIo, Encoder, http};

const LIMIT: usize = 1 << 20;

/// Value `n` as `n` messages: `n.0`, `n.1`, ….
fn messages() -> Box<dyn Encoder<usize>> {
    Box::new(|n: &usize, out: &mut Vec<Bytes>| {
        out.extend((0..*n).map(|i| Bytes::from(format!("{n}.{i}"))));
        Ok(())
    })
}

/// A source the sinks are prepared for: live, so never one value.
fn live() -> EventStream<usize> {
    EventStream::stream(stream::pending())
}

/// Whether `sink` would take another value now.
fn ready(sink: &mut EventSink<usize>) -> bool {
    let mut cx = Context::from_waker(Waker::noop());
    match sink.poll_ready_unpin(&mut cx) {
        Poll::Ready(ready) => ready.is_ok(),
        Poll::Pending => false,
    }
}

/// Feed `sink` zero, one and three messages' values, reading `body` frame by
/// frame: the next value waits until the last one's frames are read.
async fn zero_one_three(sink: &mut EventSink<usize>, body: &mut Body) {
    let deadline = Duration::from_secs(3);
    let mut cx = Context::from_waker(Waker::noop());
    for n in [0, 1, 3] {
        // The reader waits, as a transport pulling the call's body does.
        assert!(body.poll_next_unpin(&mut cx).is_pending());
        let fed = tokio::time::timeout(deadline, sink.feed(Ok(n)));
        fed.await.expect("ready").unwrap();
        for i in 0..n {
            assert!(!ready(sink), "{n}.{i} is unread");
            let frame = tokio::time::timeout(deadline, body.next()).await.unwrap();
            assert_eq!(frame.unwrap().unwrap(), format!("{n}.{i}"));
        }
    }
    assert!(body.poll_next_unpin(&mut cx).is_pending());
    tokio::time::timeout(deadline, sink.feed(Ok(0)))
        .await
        .expect("drained")
        .unwrap();
}

#[tokio::test]
async fn a_clients_live_reply_frames_each_message() {
    let request = http::Request::get("/").body(Body::stream(stream::pending().boxed()));
    let (io, answer) = ClientIo::accept_live(request.unwrap(), LIMIT);
    let (_, mut sink) = io.bind(
        |_| {
            Box::new(move |_: Bytes, out: &mut Vec<_>| {
                out.push(0);
                Ok(())
            })
        },
        |reply| {
            let head = reply.head();
            Ok(reply.open(head, messages()))
        },
    );
    sink.prepare(&mut live()).unwrap();
    let (response, _) = answer.await.expect("opened");
    let mut body = response.into_body();
    zero_one_three(&mut sink, &mut body).await;
}

#[tokio::test]
async fn a_providers_live_call_frames_each_message() {
    let (tx, called) = oneshot::channel();
    let tx = parking_lot::Mutex::new(Some(tx));
    let transport = move |request: http::Request<Body>| {
        let _ = tx.lock().take().unwrap().send(request.into_body());
        let answer = http::Response::new(Body::stream(stream::pending().boxed()));
        async move { Ok((answer, None)) }.boxed()
    };
    let io = respan_gateway_http::connect(transport, LIMIT)(Extensions::new());
    // The answer is held: a call whose answer is dropped is over.
    let (_answer, mut sink) = io.with_refusal(|_| Fault::internal("refused")).bind(
        |_, _: Option<&usize>| Ok(messages()),
        |_, _| Some(Box::new(|_: Bytes, _: &mut Vec<usize>| Ok(()))),
    );
    sink.prepare(&mut live()).unwrap();
    // The call is made once the sink is first polled.
    assert!(!ready(&mut sink));
    let mut body = called.await.unwrap();
    zero_one_three(&mut sink, &mut body).await;
}

/// Messages are one after another where the body is not live, and an
/// encoder's failure fails the body whole.
#[test]
fn messages_concatenate_and_a_failure_fails_the_body() {
    let mut encoder: Box<dyn Encoder<usize>> = Box::new(|n: &usize, out: &mut Vec<Bytes>| {
        out.push(Bytes::from("partial"));
        if *n == 0 {
            return Err(Fault::internal("no"));
        }
        out.push(Bytes::from("whole"));
        Ok(())
    });
    assert_eq!(encoder.encode_all(&[1]).unwrap(), "partialwhole");
    assert!(encoder.encode_all(&[0]).is_err());
}
