use respan_gateway_core::Fault;
use std::time::Duration;

#[allow(dead_code)]
#[path = "../../../../tests/support/http.rs"]
mod http_support;
use h::bytes::Bytes;
use h::http::{Request, StatusCode};
use h::http_body_util::BodyExt;
use http_support::LIMIT;
use respan_gateway_core::futures::{SinkExt, future};
use respan_gateway_core::{EventSink, EventStream};
use respan_gateway_http as h;

#[derive(Clone, Copy, Debug)]
enum Opening {
    Delayed,
    Immediate,
    Empty,
}

/// A response to `request`, of the method `opening` asks.
fn request(opening: Opening) -> Request<h::Body> {
    let mut request = Request::new(h::Body::full(Bytes::new()));
    if let Opening::Empty = opening {
        // A `HEAD` is answered with no body.
        *request.method_mut() = h::http::Method::HEAD;
    }
    request
}

async fn preparation_failure(opening: Opening, panic: bool) {
    let (io, answer) = h::ClientIo::accept(request(opening), LIMIT);
    let (read, mut sink) = io
        .with_error(|fault, _, _| Bytes::from(fault.message().to_owned()))
        .bind(
            |_| {
                Box::new(|_: Bytes, out: &mut Vec<String>| {
                    out.push(String::new());
                    Ok(())
                })
            },
            move |reply| {
                let head = respan_gateway_http::http::Response::new(()).into_parts().0;
                let encoder = Box::new(|_: &u8, out: &mut Vec<Bytes>| {
                    out.push(Bytes::new());
                    Ok(())
                });
                let _sink = match opening {
                    Opening::Delayed | Opening::Empty => reply.write(head, encoder),
                    Opening::Immediate => {
                        let premature = Bytes::from_static(b"premature");
                        reply.open(head, Box::new(Prefixed(premature, encoder)))
                    }
                };
                if panic {
                    panic!("preparation panic after constructing a sink");
                }
                Err::<EventSink<u8>, _>(Fault::bad_request("preparation failed"))
            },
        );
    let _input = read.await.expect("incoming request opens");
    let mut source = EventStream::single(1_u8);
    sink.prepare(&mut source).expect("prepare response sink");

    let receive = async move {
        let (response, completion) = answer.await.expect("response handoff");
        let (head, body) = response.into_parts();
        let bytes = body
            .collect()
            .await
            .expect("collect refusal body")
            .to_bytes();
        (head.status, bytes, completion.await)
    };
    let (delivery, (status, bytes, completion)) =
        tokio::time::timeout(Duration::from_secs(1), future::join(sink.close(), receive))
            .await
            .expect("preparation failure must not leave delivery pending");

    let expected = if panic { "internal" } else { "bad_request" };
    assert_eq!(
        delivery.expect_err("delivery must fail").code(),
        expected,
        "{opening:?}"
    );
    assert_eq!(
        completion.expect_err("delivery must fail").code(),
        expected,
        "{opening:?}"
    );
    if panic {
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "{opening:?}");
        assert!(
            String::from_utf8_lossy(&bytes).contains("panicked"),
            "{opening:?}"
        );
    } else {
        assert_eq!(status, StatusCode::BAD_REQUEST, "{opening:?}");
        // A refusal of a `HEAD` has no body either.
        let written = match opening {
            Opening::Empty => Bytes::new(),
            _ => Bytes::from_static(b"preparation failed"),
        };
        assert_eq!(bytes, written, "{opening:?}");
    }
}

#[tokio::test]
async fn preparation_error_wins_after_constructing_each_response_sink() {
    for opening in [Opening::Delayed, Opening::Immediate, Opening::Empty] {
        preparation_failure(opening, false).await;
    }
}

#[tokio::test]
async fn preparation_panic_wins_after_constructing_each_response_sink() {
    for opening in [Opening::Delayed, Opening::Immediate, Opening::Empty] {
        preparation_failure(opening, true).await;
    }
}

#[tokio::test]
async fn cancelled_response_handoff_cannot_acknowledge_empty_delivery() {
    for opening in [Opening::Delayed, Opening::Empty] {
        let (io, answer) = h::ClientIo::accept(request(opening), LIMIT);
        let (read, mut sink) = io
            .with_error(|fault, _, _| Bytes::from(fault.message().to_owned()))
            .bind(
                |_| {
                    Box::new(|_: Bytes, out: &mut Vec<String>| {
                        out.push(String::new());
                        Ok(())
                    })
                },
                move |reply| {
                    drop(answer);
                    let head = respan_gateway_http::http::Response::new(()).into_parts().0;
                    Ok(reply.write(
                        head,
                        Box::new(|_: &u8, out: &mut Vec<Bytes>| {
                            out.push(Bytes::new());
                            Ok(())
                        }),
                    ))
                },
            );
        let _input = read.await.expect("incoming request opens");
        let mut source = EventStream::empty();
        sink.prepare(&mut source).expect("prepare response sink");
        let result = tokio::time::timeout(Duration::from_secs(1), sink.close())
            .await
            .expect("cancelled response handoff must not hang");
        assert_eq!(result.expect_err("delivery must fail").code(), "abandoned");
    }
}

/// A response prepared before its request was read, which no gateway
/// does, fails `internal` and says so.
#[tokio::test]
async fn a_response_prepared_before_its_request_was_read_fails() {
    let (io, _answer) = h::ClientIo::accept(request(Opening::Delayed), LIMIT);
    let (_read, mut sink) = io.bind(
        |_| {
            Box::new(|_: Bytes, out: &mut Vec<String>| {
                out.push(String::new());
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
    let fault = sink
        .prepare(&mut EventStream::single(1_u8))
        .expect_err("unread request");
    assert_eq!(
        (fault.code(), fault.message()),
        (
            "internal",
            "a response was prepared before its request was read"
        )
    );
}

/// An encoder that starts with `0`, then encodes as `1` does.
struct Prefixed<T>(Bytes, Box<dyn respan_gateway_http::Encoder<T>>);

impl<T: 'static> respan_gateway_http::Encoder<T> for Prefixed<T> {
    fn start(&mut self, out: &mut Vec<Bytes>) -> Result<(), respan_gateway_core::Fault> {
        out.push(self.0.clone());
        self.1.start(out)
    }

    fn encode(
        &mut self,
        value: &T,
        out: &mut Vec<Bytes>,
    ) -> Result<(), respan_gateway_core::Fault> {
        self.1.encode(value, out)
    }

    fn end(
        &mut self,
        end: Result<(), respan_gateway_core::Fault>,
        out: &mut Vec<Bytes>,
    ) -> Result<(), respan_gateway_core::Fault> {
        self.1.end(end, out)
    }
}
