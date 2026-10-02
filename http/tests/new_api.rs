use std::pin::Pin;
#[allow(dead_code)]
#[path = "../../tests/support/http.rs"]
mod http_support;
use http_support::LIMIT;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::task::{Context, Poll};
use std::time::Duration;

use h::{
    Body, Encoder,
    bytes::{Bytes, BytesMut},
    http::{Request, Response},
};
use respan_gateway_http as h;
use respan_gateway_sdk::Upstream;
use respan_gateway_sdk::futures::{self, FutureExt, Sink, SinkExt, StreamExt, future, stream};
use respan_gateway_sdk::{EventSink, EventStream, Extensions, Fault};

struct Writes {
    calls: Arc<AtomicUsize>,
    ends: Arc<AtomicUsize>,
    fail_encode: bool,
    fail_end: bool,
}
impl Encoder<u8> for Writes {
    fn encode(&mut self, value: &u8, out: &mut BytesMut) -> Result<(), Fault> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.fail_encode {
            return Err(Fault::config("encode"));
        }
        out.extend_from_slice(&[*value]);
        Ok(())
    }
    fn end(&mut self, end: &Result<(), Fault>, out: &mut BytesMut) -> Result<(), Fault> {
        self.ends.fetch_add(1, Ordering::SeqCst);
        end.clone()?;
        if self.fail_end {
            return Err(Fault::config("end"));
        }
        out.extend_from_slice(b"!");
        Ok(())
    }
}
fn writer(fail_encode: bool, fail_end: bool) -> Writes {
    Writes {
        calls: Arc::new(AtomicUsize::new(0)),
        ends: Arc::new(AtomicUsize::new(0)),
        fail_encode,
        fail_end,
    }
}

/// A body read and discarded, keeping its wire failure.
fn nothing(
    body: Body,
) -> respan_gateway_sdk::futures::stream::BoxStream<'static, Result<u8, Fault>> {
    h::formats::read(body, (), |_, _, _| Ok(()), |_, wire, _| wire)
}

#[tokio::test]
async fn default_request_decodes_encodes_and_finalizes_one_initial_body() {
    let (incoming, _answer) = h::Incoming::accept(Request::new(Body::full("raw")), false, LIMIT);
    let (read, _sink) = incoming.bind(
        |request| {
            Ok(h::messages(request, |_, _| -> Result<u8, Fault> {
                Ok(b'r')
            }))
        },
        |reply, source: &mut EventStream<u8>| {
            Ok(reply.empty(
                respan_gateway_http::http::Response::new(()).into_parts().0,
                source,
            ))
        },
        h::plain_refusal,
    );
    let mut source = read.await.expect("expected value");
    let encode = writer(false, false);
    let (calls, ends) = (encode.calls.clone(), encode.ends.clone());
    let (request, _) = h::request(&mut source, Box::new(encode)).expect("expected value");
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(ends.load(Ordering::SeqCst), 1);
    assert!(
        source.is_single(),
        "stop retains source shape and ownership"
    );
    let mut body = request.into_body();
    assert_eq!(body.full_bytes().expect("expected value"), "r!");
    assert_eq!(
        body.next()
            .await
            .expect("expected value")
            .expect("expected value"),
        "r!"
    );
    assert!(body.next().await.is_none());
}

#[test]
fn value_fails_synchronously_on_encoding_or_end_and_live_consumes_nothing() {
    for (encode, end, message) in [(true, false, "encode"), (false, true, "end")] {
        let mut source = EventStream::single(7);
        let error = h::request(&mut source, Box::new(writer(encode, end)))
            .expect_err("synchronous encoding failure");
        assert_eq!(error.message(), message);
        assert_eq!(
            source
                .value()
                .expect("expected value")
                .expect("expected value"),
            &7
        );
    }
    let polls = Arc::new(AtomicUsize::new(0));
    let seen = polls.clone();
    let mut source = EventStream::stream(stream::poll_fn(move |_| {
        seen.fetch_add(1, Ordering::SeqCst);
        Poll::Ready(Some(Ok(1)))
    }));
    let encode = writer(true, true);
    let calls = encode.calls.clone();
    let (request, _) = h::request(&mut source, Box::new(encode)).expect("expected value");
    assert!(request.body().full_bytes().is_none());
    assert_eq!(polls.load(Ordering::SeqCst), 0);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[test]
fn encode_all_writes_values_end_and_propagates_end_failure() {
    assert_eq!(
        h::encode_all(Box::new(writer(false, false)), &[1, 2]).expect("expected value"),
        Bytes::from_static(b"\x01\x02!")
    );
    assert_eq!(
        h::encode_all(Box::new(writer(false, true)), &[1])
            .expect_err("expected failure")
            .message(),
        "end"
    );
}

#[tokio::test]
async fn empty_does_not_encode_and_open_keeps_prefix_through_decorator() {
    for empty in [true, false] {
        let (incoming, answer) = h::Incoming::accept(Request::new(Body::default()), false, LIMIT);
        let encode = writer(false, false);
        let calls = encode.calls.clone();
        let ends = encode.ends.clone();
        let (read, mut sink) = incoming.bind(
            |request| Ok(h::messages(request, |_, _| Ok(0_u8))),
            move |reply, source| {
                let sink = if empty {
                    reply.empty(
                        respan_gateway_http::http::Response::new(()).into_parts().0,
                        source,
                    )
                } else {
                    reply.open(
                        respan_gateway_http::http::Response::new(()).into_parts().0,
                        Bytes::from_static(b"prefix:"),
                        Box::new(encode),
                    )
                };
                Ok(EventSink::new(sink.sink_map_err(|fault| fault)))
            },
            h::plain_refusal,
        );
        read.await.expect("expected value");
        let mut source = EventStream::single(1_u8);
        sink.prepare(&mut source).expect("expected value");
        let receiving = async {
            let (response, completion) = answer.await.expect("expected value");
            let body = h::http_body_util::BodyExt::collect(response.into_body())
                .await
                .expect("expected value")
                .to_bytes();
            completion.await.expect("expected value");
            body
        };
        let (end, body) = future::join(sink.close(), receiving).await;
        end.expect("expected value");
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(ends.load(Ordering::SeqCst), usize::from(!empty));
        assert_eq!(
            body,
            if empty {
                Bytes::new()
            } else {
                Bytes::from_static(b"prefix:!")
            }
        );
        assert_eq!(
            source
                .value()
                .expect("expected value")
                .expect("expected value"),
            &1
        );
        if empty {
            assert!(source.next().await.is_none());
        }
    }
}

struct Fallible {
    phase: u8,
    inner: Option<EventSink<u8>>,
}
impl Sink<Result<u8, Fault>> for Fallible {
    type Error = Fault;
    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Fault>> {
        if self.phase == 0 {
            return Poll::Ready(Err(Fault::config("custom ready")));
        }
        self.inner
            .as_mut()
            .map_or(Poll::Ready(Ok(())), |sink| Pin::new(sink).poll_ready(cx))
    }
    fn start_send(mut self: Pin<&mut Self>, value: Result<u8, Fault>) -> Result<(), Fault> {
        self.inner
            .as_mut()
            .map_or(Ok(()), |sink| Pin::new(sink).start_send(value))
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Fault>> {
        if self.phase == 1 {
            return Poll::Ready(Err(Fault::config("custom flush")));
        }
        self.inner
            .as_mut()
            .map_or(Poll::Ready(Ok(())), |sink| Pin::new(sink).poll_flush(cx))
    }
    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Fault>> {
        if self.phase == 2 {
            return Poll::Ready(Err(Fault::config("custom close")));
        }
        if self.phase == 3 {
            return Poll::Pending;
        }
        self.inner
            .as_mut()
            .map_or(Poll::Ready(Ok(())), |sink| Pin::new(sink).poll_close(cx))
    }
}

#[tokio::test]
async fn custom_and_decorated_sink_faults_are_not_hidden_by_transport_completion() {
    for wrapped in [false, true] {
        for complete in [false, true] {
            for phase in 0..3 {
                let send = h::connect(
                    move |_| async move {
                        let completion: h::Completion = if complete {
                            future::ready(Ok(())).boxed()
                        } else {
                            future::pending().boxed()
                        };
                        Ok((Response::new(Body::default()), Some(completion)))
                    },
                    LIMIT,
                );
                let (_read, mut sink) = send(Extensions::new()).bind(
                    move |source| {
                        let (request, inner) = if wrapped {
                            let (request, sink) = h::request(
                                source,
                                h::formats::each(|v: &u8| Ok(Bytes::copy_from_slice(&[*v]))),
                            )?;
                            (request, Some(sink))
                        } else {
                            (Request::new(Body::default()), None)
                        };
                        Ok((request, EventSink::new(Fallible { phase, inner })))
                    },
                    |response, _| nothing(response.into_body()),
                    h::success,
                );
                let mut source = EventStream::stream(stream::pending());
                sink.prepare(&mut source).expect("expected value");
                let action = future::poll_fn(|cx| match phase {
                    0 => Pin::new(&mut sink).poll_ready(cx),
                    1 => Pin::new(&mut sink).poll_flush(cx),
                    _ => Pin::new(&mut sink).poll_close(cx),
                });
                let error = tokio::time::timeout(Duration::from_secs(1), action)
                    .await
                    .expect("expected value")
                    .expect_err("expected failure");
                assert_eq!(
                    h::error_code(&error),
                    "config",
                    "wrapped={wrapped} complete={complete} phase={phase}"
                );
            }
        }
    }
}

#[tokio::test]
async fn transport_failure_does_not_wait_for_a_pending_custom_sink_close() {
    let send = h::connect(
        |_| async { Err(Upstream::fault("transport failed")) },
        LIMIT,
    );
    let (_read, mut sink) = send(Extensions::new()).bind(
        |_| {
            Ok((
                Request::new(Body::default()),
                EventSink::new(Fallible {
                    phase: 3,
                    inner: None,
                }),
            ))
        },
        |response, _| nothing(response.into_body()),
        h::success,
    );
    sink.prepare(&mut EventStream::single(0))
        .expect("expected value");
    let error = tokio::time::timeout(Duration::from_secs(1), sink.close())
        .await
        .expect("expected value")
        .expect_err("expected failure");
    assert_eq!(error.message(), "transport failed");
}

#[tokio::test]
async fn custom_request_body_failure_survives_a_successful_transport() {
    let send = h::connect(
        |mut request| async move {
            assert!(
                request
                    .body_mut()
                    .next()
                    .await
                    .expect("expected value")
                    .is_err()
            );
            Ok((Response::new(Body::default()), None))
        },
        LIMIT,
    );
    let (_read, mut sink) = send(Extensions::new()).bind(
        |_| {
            let body = Body::stream(stream::once(future::ready(Err(Fault::config(
                "custom body",
            )))));
            Ok((
                Request::new(body),
                EventSink::new(futures::sink::drain().sink_map_err(|never| match never {})),
            ))
        },
        |response, _| nothing(response.into_body()),
        h::success,
    );
    sink.prepare(&mut EventStream::<u8>::stream(stream::pending()))
        .expect("expected value");
    let error = tokio::time::timeout(Duration::from_secs(1), sink.close())
        .await
        .expect("expected value")
        .expect_err("expected failure");
    assert_eq!(error.message(), "custom body");
}

#[tokio::test]
async fn authoritative_completion_releases_an_unconsumed_encoded_frame() {
    let (finish, finished) = futures::channel::oneshot::channel::<()>();
    let finished = Arc::new(std::sync::Mutex::new(Some(finished)));
    let send = h::connect(
        move |mut request| {
            let finished = finished
                .lock()
                .expect("expected value")
                .take()
                .expect("expected value");
            async move {
                assert!(request.body_mut().next().now_or_never().is_none());
                let response = Response::new(Body::stream(stream::poll_fn(move |_| {
                    let _keep_reader = &request;
                    Poll::Pending
                })));
                let completion: h::Completion = async move {
                    finished.await.expect("expected value");
                    Ok(())
                }
                .boxed();
                Ok((response, Some(completion)))
            }
        },
        LIMIT,
    );
    let (_read, mut sink) = send(Extensions::new()).bind(
        |source| {
            h::request(
                source,
                h::formats::each(|v: &u8| Ok(Bytes::copy_from_slice(&[*v]))),
            )
        },
        |response, _| nothing(response.into_body()),
        h::success,
    );
    sink.prepare(&mut EventStream::stream(stream::pending()))
        .expect("expected value");
    future::poll_fn(|cx| Pin::new(&mut sink).poll_ready(cx))
        .await
        .expect("expected value");
    Pin::new(&mut sink)
        .start_send(Ok(7))
        .expect("expected value");
    assert!(
        sink.flush().now_or_never().is_none(),
        "encoded bytes remain queued"
    );
    finish.send(()).expect("expected value");
    tokio::time::timeout(Duration::from_secs(1), sink.close())
        .await
        .expect("expected value")
        .expect("expected value");
}

/// A success opens at its head: an error its body says later fails the
/// opened answer, however soon it comes; it refuses nothing.
#[tokio::test]
async fn a_late_error_in_a_success_fails_the_opened_answer() {
    let polls = Arc::new(AtomicUsize::new(0));
    let seen = polls.clone();
    let send = h::connect(
        move |_| {
            let seen = seen.clone();
            async move {
                let frames = (0..300).map(move |_| {
                    seen.fetch_add(1, Ordering::SeqCst);
                    Ok(Bytes::new())
                });
                Ok((Response::new(Body::stream(stream::iter(frames))), None))
            }
        },
        LIMIT,
    );
    let (read, mut sink) = send(Extensions::new()).bind(
        |source| h::request(source, h::formats::each(|_: &u8| Ok(Bytes::new()))),
        |response, _| {
            h::formats::read::<u8, _, _, _>(
                response.into_body(),
                (),
                |_, _, _| Ok(()),
                |_, wire, _| {
                    wire?;
                    Err(Upstream::fault("late failure"))
                },
            )
        },
        h::success,
    );
    sink.prepare(&mut EventStream::single(0))
        .expect("expected value");
    let (end, output) = future::join(sink.close(), read).await;
    end.expect("expected value");
    let output = output.expect("expected value");
    assert!(output.refusal().is_none(), "a success opens at its head");
    let read: Vec<_> = output.collect().await;
    assert_eq!(polls.load(Ordering::SeqCst), 300);
    let failure = read
        .into_iter()
        .map(|r| r.err().map(|f| f.message().to_owned()));
    assert_eq!(
        failure.collect::<Vec<_>>(),
        [Some("late failure".to_owned())]
    );
}

#[tokio::test]
async fn empty_error_frames_yield_even_though_the_byte_cap_is_not_reached() {
    let count = Arc::new(AtomicUsize::new(0));
    let observed = count.clone();
    let send = h::connect(
        move |_| {
            let count = count.clone();
            async move {
                let mut response = Response::new(Body::stream(stream::repeat_with(move || {
                    count.fetch_add(1, Ordering::SeqCst);
                    Ok(Bytes::new())
                })));
                *response.status_mut() = h::http::StatusCode::BAD_GATEWAY;
                Ok((response, None))
            }
        },
        LIMIT,
    );
    let (_read, mut sink) = send(Extensions::new()).bind(
        |source| h::request(source, h::formats::each(|_: &u8| Ok(Bytes::new()))),
        |response, _| h::formats::whole::read::<u8>("test", response, |_, _| Ok(())),
        h::success,
    );
    sink.prepare(&mut EventStream::single(0))
        .expect("expected value");
    assert!(sink.close().now_or_never().is_none());
    // Close drives delivery once before and once after closing the request sink.
    assert!((1..=256).contains(&observed.load(Ordering::SeqCst)));
}

#[tokio::test]
async fn ignored_incoming_frames_cannot_spin_in_one_poll() {
    let count = Arc::new(AtomicUsize::new(0));
    let seen = count.clone();
    let body = Body::stream(stream::repeat_with(move || {
        seen.fetch_add(1, Ordering::SeqCst);
        Ok(Bytes::new())
    }));
    let mut read = h::formats::read::<u8, _, _, _>(body, (), |_, _, _| Ok(()), |_, wire, _| wire);
    assert!(read.next().now_or_never().is_none());
    assert_eq!(count.load(Ordering::SeqCst), 128);
}
