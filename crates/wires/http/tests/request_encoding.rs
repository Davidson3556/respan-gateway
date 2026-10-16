use respan_gateway_core::FaultKind;
use std::pin::Pin;
#[allow(dead_code)]
#[path = "../../../../tests/support/http.rs"]
mod http_support;
use http_support::LIMIT;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::task::Poll;
use std::time::Duration;

use h::{
    Body, Encoder,
    bytes::Bytes,
    http::{Request, Response},
};
use respan_gateway_core::futures::{self, FutureExt, Sink, SinkExt, StreamExt, future, stream};
use respan_gateway_core::{EventSink, EventStream, Extensions, Fault};
use respan_gateway_http as h;

struct Writes {
    calls: Arc<AtomicUsize>,
    ends: Arc<AtomicUsize>,
    fail_encode: bool,
    fail_end: bool,
}
impl Encoder<u8> for Writes {
    fn encode(&mut self, value: &u8, frames: &mut Vec<h::bytes::Bytes>) -> Result<(), Fault> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.fail_encode {
            return Err(Fault::config("encode"));
        }
        frames.push(h::bytes::Bytes::copy_from_slice(&[*value]));
        Ok(())
    }

    fn end(
        &mut self,
        end: Result<(), Fault>,
        frames: &mut Vec<h::bytes::Bytes>,
    ) -> Result<(), Fault> {
        self.ends.fetch_add(1, Ordering::SeqCst);
        end?;
        if self.fail_end {
            return Err(Fault::config("end"));
        }
        frames.push(h::bytes::Bytes::from_static(b"!"));
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

/// A decoder that reads a body and discards it, keeping its wire failure.
fn nothing() -> Option<Box<dyn h::Decoder<u8>>> {
    Some(Box::new(|_: Bytes, _: &mut Vec<u8>| Ok(())))
}

/// Reads nothing of a body, and fails once it ended well.
struct LateFailure;

impl h::Decoder<u8> for LateFailure {
    fn decode(&mut self, _: Bytes, _: &mut Vec<u8>) -> Result<(), Fault> {
        Ok(())
    }

    fn end(&mut self, end: Result<(), Fault>, _: &mut Vec<u8>) -> Result<(), Fault> {
        end?;
        Err(Fault::upstream("late failure"))
    }
}

/// Where a transport keeps the request it was sent.
type Sent = Arc<std::sync::Mutex<Option<Request<Body>>>>;

/// An outgoing binding encoding with `encoder`, its transport keeping the
/// request it is sent and never answering.
fn dial<T: Clone + Send + Sync + 'static>(encoder: Box<dyn Encoder<T>>) -> (Sent, EventSink<T>) {
    let sent = Sent::default();
    let kept = sent.clone();
    let send = h::connect(
        move |request| {
            *kept.lock().expect("unpoisoned") = Some(request);
            future::pending::<Result<(Response<Body>, Option<h::Completion>), Fault>>()
        },
        LIMIT,
    );
    let (_read, sink) = send(Extensions::new())
        .with_refusal(|_| Fault::new("refused"))
        .bind(move |_, _| Ok(encoder), |_, _| nothing());
    (sent, sink)
}

/// The request `sink` sends, once its delivery starts.
fn sent<T: 'static>(sent: &Sent, sink: &mut EventSink<T>) -> Request<Body> {
    let _ = sink.flush().now_or_never();
    sent.lock()
        .expect("unpoisoned")
        .take()
        .expect("a sent request")
}

#[tokio::test]
async fn default_request_decodes_encodes_and_finalizes_one_initial_body() {
    let request = Request::new(Body::full("raw"));
    let (incoming, _answer) = h::ClientIo::accept(request, LIMIT);
    let (read, _sink) = incoming.bind(
        |_| {
            Box::new(|_: Bytes, out: &mut Vec<u8>| {
                out.push(b'r');
                Ok(())
            })
        },
        |reply| {
            let head = reply.head();
            Ok(reply.write(
                head,
                Box::new(|_: &u8, out: &mut Vec<Bytes>| {
                    out.push(Bytes::new());
                    Ok(())
                }),
            ))
        },
    );
    let mut source = read.await.expect("expected value");
    let encode = writer(false, false);
    let (calls, ends) = (encode.calls.clone(), encode.ends.clone());
    let (kept, mut sink) = dial(Box::new(encode));
    sink.prepare(&mut source).expect("expected value");
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(ends.load(Ordering::SeqCst), 1);
    assert!(
        source.is_single(),
        "stop retains source shape and ownership"
    );
    let mut body = sent(&kept, &mut sink).into_body();
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
        let (_, mut sink) = dial(Box::new(writer(encode, end)));
        let error = sink
            .prepare(&mut source)
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
    let (kept, mut sink) = dial(Box::new(encode));
    sink.prepare(&mut source).expect("expected value");
    assert!(sent(&kept, &mut sink).body().full_bytes().is_none());
    assert_eq!(polls.load(Ordering::SeqCst), 0);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[test]
fn encode_all_writes_values_end_and_propagates_end_failure() {
    assert_eq!(
        (Box::new(writer(false, false)) as Box<dyn h::Encoder<u8>>)
            .encode_all(&[1, 2])
            .expect("expected value"),
        Bytes::from_static(b"\x01\x02!")
    );
    assert_eq!(
        (Box::new(writer(false, true)) as Box<dyn h::Encoder<u8>>)
            .encode_all(&[1])
            .expect_err("expected failure")
            .message(),
        "end"
    );
}

/// A response HTTP gives no body, to a `HEAD` or a 204, never calls its
/// encoder, and stops its source unread; one that has a body keeps its
/// prefix through a decorating sink.
#[tokio::test]
async fn a_bodiless_response_does_not_encode_and_open_keeps_prefix_through_decorator() {
    for (method, status) in [("HEAD", 200), ("GET", 204), ("GET", 200)] {
        let empty = (method, status) != ("GET", 200);
        let mut request = Request::new(Body::default());
        *request.method_mut() = h::http::Method::from_bytes(method.as_bytes()).expect("method");
        let (incoming, answer) = h::ClientIo::accept(request, LIMIT);
        let encode = writer(false, false);
        let calls = encode.calls.clone();
        let ends = encode.ends.clone();
        let (read, mut sink) = incoming.bind(
            |_| {
                Box::new(move |_: Bytes, out: &mut Vec<_>| {
                    out.push(0_u8);
                    Ok(())
                })
            },
            move |reply| {
                let mut head = reply.head();
                head.status = h::http::StatusCode::from_u16(status).expect("status");
                let prefix = Bytes::from_static(b"prefix:");
                let sink = reply.open(head, Box::new(Prefixed(prefix, Box::new(encode))));
                Ok(EventSink::new(sink.sink_map_err(|fault| fault)))
            },
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

/// An encoder's failure fails the call: a transport that says its write is
/// complete does not hide it.
#[tokio::test]
async fn an_encoders_failure_is_not_hidden_by_transport_completion() {
    let send = h::connect(
        |_| async {
            let completion: h::Completion = future::ready(Ok(())).boxed();
            Ok((Response::new(Body::default()), Some(completion)))
        },
        LIMIT,
    );
    let (_read, mut sink) = send(Extensions::new())
        .with_refusal(|_| Fault::new("refused"))
        .bind(
            |_, _| Ok(Box::new(writer(true, false)) as Box<dyn Encoder<u8>>),
            |_, _| nothing(),
        );
    sink.prepare(&mut EventStream::stream(stream::pending()))
        .expect("expected value");
    let error = tokio::time::timeout(Duration::from_secs(1), sink.send(Ok(1)))
        .await
        .expect("expected value")
        .expect_err("expected failure");
    assert_eq!(error.code(), "config");
}

#[tokio::test]
async fn transport_failure_ends_the_call() {
    let send = h::connect(
        |_| async { Err(Fault::upstream("transport failed")) },
        LIMIT,
    );
    let (_read, mut sink) = send(Extensions::new())
        .with_refusal(|_| Fault::new("refused"))
        .bind(
            |_, _| {
                Ok(Box::new(|v: &u8, out: &mut Vec<Bytes>| {
                    out.push(Bytes::copy_from_slice(&[*v]));
                    Ok(())
                }))
            },
            |_, _| nothing(),
        );
    sink.prepare(&mut EventStream::single(0))
        .expect("expected value");
    let error = tokio::time::timeout(Duration::from_secs(1), sink.close())
        .await
        .expect("expected value")
        .expect_err("expected failure");
    assert_eq!(error.message(), "transport failed");
}

/// A live body an encoder broke off fails the call, though its transport
/// answered.
#[tokio::test]
async fn a_live_body_failure_survives_a_successful_transport() {
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
    let (_read, mut sink) = send(Extensions::new())
        .with_refusal(|_| Fault::new("refused"))
        .bind(
            |_, _| Ok(Box::new(writer(true, false)) as Box<dyn Encoder<u8>>),
            |_, _| nothing(),
        );
    sink.prepare(&mut EventStream::<u8>::stream(stream::pending()))
        .expect("expected value");
    let sent = async {
        sink.feed(Ok(1)).await?;
        sink.close().await
    };
    let error = tokio::time::timeout(Duration::from_secs(1), sent)
        .await
        .expect("expected value")
        .expect_err("expected failure");
    assert_eq!(error.message(), "encode");
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
    let (_read, mut sink) = send(Extensions::new())
        .with_refusal(|_| Fault::new("refused"))
        .bind(
            |_, _| {
                Ok(Box::new(|v: &u8, out: &mut Vec<Bytes>| {
                    out.push(Bytes::copy_from_slice(&[*v]));
                    Ok(())
                }))
            },
            |_, _| nothing(),
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
    let (read, mut sink) = send(Extensions::new())
        .with_refusal(|_| Fault::new("refused"))
        .bind(
            |_, _| {
                Ok(Box::new(|_: &u8, out: &mut Vec<Bytes>| {
                    out.push(Bytes::new());
                    Ok(())
                }))
            },
            |_, _| Some(Box::new(LateFailure)),
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
        |_, _| {
            Ok(Box::new(|_: &u8, out: &mut Vec<Bytes>| {
                out.push(Bytes::new());
                Ok(())
            }))
        },
        |_, _| nothing(),
    );
    sink.prepare(&mut EventStream::single(0))
        .expect("expected value");
    assert!(sink.close().now_or_never().is_none());
    // Close drives delivery once before and once after closing the request sink.
    assert!((1..=256).contains(&observed.load(Ordering::SeqCst)));
}

/// An answer the format takes for no success is read whole by the binding,
/// never by its decoder, into its error reader's fault, which keeps HTTP's
/// extensions of it, even for a 2xx a stricter format refuses: its status, its
/// `Retry-After` and the answer as it came.
#[tokio::test]
async fn a_refused_answer_is_read_whole_into_the_error_readers_fault() {
    let send = h::connect(
        |_| async {
            let frames = [
                Ok(Bytes::from_static(b"not ")),
                Ok(Bytes::from_static(b"up")),
            ];
            let response = Response::builder()
                .header("retry-after", "3")
                .body(Body::stream(stream::iter(frames)))
                .expect("a response");
            Ok((response, None))
        },
        LIMIT,
    );
    let (read, mut sink) = send(Extensions::new())
        .with_success(|answer, _| answer.status == h::http::StatusCode::SWITCHING_PROTOCOLS)
        .bind(
            |_, _| {
                Ok(Box::new(|_: &u8, out: &mut Vec<Bytes>| {
                    out.push(Bytes::new());
                    Ok(())
                }))
            },
            |_, _| -> Option<Box<dyn h::Decoder<u8>>> { panic!("a refusal reached the decoder") },
        );
    sink.prepare(&mut EventStream::single(0))
        .expect("expected value");
    let (_, output) = future::join(sink.close(), read).await;
    let output = output.expect("an answer");
    let fault = output.refusal().expect("refused");
    assert!(fault.is(FaultKind::Provider));
    assert_eq!(
        fault.get(),
        Some(&respan_gateway_http_faults::Status(
            h::http::StatusCode::BAD_GATEWAY
        ))
    );
    assert_eq!(
        fault.get(),
        Some(&respan_gateway_core::RetryAfter(Duration::from_secs(3)))
    );
    let answer = &fault
        .get::<respan_gateway_http_faults::ProviderError>()
        .expect("the answer")
        .0;
    assert_eq!(
        (answer.status().as_u16(), &answer.body()[..]),
        (200, &b"not up"[..])
    );
}

/// A passthrough relays a 101 only to a call that asked to switch: one
/// that did not is refused, the answer never relayed.
#[tokio::test]
async fn a_passthrough_refuses_a_101_no_one_asked_for() {
    use respan_gateway_core::Format;
    let send = h::connect(
        |_| async {
            let mut response = Response::new(Body::default());
            *response.status_mut() = h::http::StatusCode::SWITCHING_PROTOCOLS;
            Ok((response, None))
        },
        LIMIT,
    );
    let (read, mut sink) = respan_gateway_passthrough_http::Passthrough
        .bind(send(Extensions::new()))
        .expect("bound");
    let input = respan_gateway_passthrough_http::Input::new("x");
    sink.prepare(&mut EventStream::single(input))
        .expect("expected value");
    let (_, output) = future::join(sink.close(), read).await;
    let output = output.expect("an answer");
    let fault = output.refusal().expect("refused");
    assert!(fault.is(FaultKind::Provider));
    assert!(
        fault
            .get::<respan_gateway_http_faults::ProviderError>()
            .is_none(),
        "never relayed"
    );
}

/// An encoder that starts with `0`, then encodes as `1` does.
struct Prefixed<T>(Bytes, Box<dyn Encoder<T>>);

impl<T: 'static> Encoder<T> for Prefixed<T> {
    fn start(&mut self, out: &mut Vec<Bytes>) -> Result<(), Fault> {
        out.push(self.0.clone());
        self.1.start(out)
    }

    fn encode(&mut self, value: &T, out: &mut Vec<Bytes>) -> Result<(), Fault> {
        self.1.encode(value, out)
    }

    fn end(&mut self, end: Result<(), Fault>, out: &mut Vec<Bytes>) -> Result<(), Fault> {
        self.1.end(end, out)
    }
}
