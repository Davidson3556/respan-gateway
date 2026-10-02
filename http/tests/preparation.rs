use std::time::Duration;

use h::bytes::Bytes;
use h::http::{Request, StatusCode};
use h::http_body_util::BodyExt;
use respan_gateway_http as h;
use respan_gateway_sdk::futures::{SinkExt, future};
use respan_gateway_sdk::{EventSink, EventStream, Fault, codes};

#[derive(Clone, Copy, Debug)]
enum Opening {
    Delayed,
    Immediate,
    Empty,
}

async fn preparation_failure(opening: Opening, panic: bool) {
    let request = Request::new(h::Body::full(Bytes::new()));
    let (io, answer) = h::Incoming::accept(request, false);
    let (read, mut sink) = io.bind(
        |_| Ok(EventStream::single(String::new())),
        move |reply, source: &mut EventStream<u8>| {
            let head = respan_gateway_http::http::Response::new(()).into_parts().0;
            let encoder = h::formats::each(|_: &u8| Ok(Bytes::new()));
            let _sink = match opening {
                Opening::Delayed => reply.write(head, encoder),
                Opening::Immediate => reply.open(head, Bytes::from_static(b"premature"), encoder),
                Opening::Empty => reply.empty(head, source),
            };
            if panic {
                panic!("preparation panic after constructing a sink");
            }
            Err::<EventSink<u8>, _>(Fault::bad_request("preparation failed"))
        },
        |fault, _, _| Bytes::from(fault.message().to_owned()),
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

    let expected = if panic {
        codes::INTERNAL
    } else {
        codes::BAD_REQUEST
    };
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
        assert_eq!(
            bytes,
            Bytes::from_static(b"preparation failed"),
            "{opening:?}"
        );
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
    for explicit_empty in [false, true] {
        let (io, answer) = h::Incoming::accept(Request::new(h::Body::full(Bytes::new())), false);
        let (read, mut sink) = io.bind(
            |_| Ok(EventStream::single(String::new())),
            move |reply, source: &mut EventStream<u8>| {
                drop(answer);
                let head = respan_gateway_http::http::Response::new(()).into_parts().0;
                Ok(if explicit_empty {
                    reply.empty(head, source)
                } else {
                    reply.write(head, h::formats::each(|_: &u8| Ok(Bytes::new())))
                })
            },
            |fault, _, _| Bytes::from(fault.message().to_owned()),
        );
        let _input = read.await.expect("incoming request opens");
        let mut source = EventStream::empty();
        sink.prepare(&mut source).expect("prepare response sink");
        let result = tokio::time::timeout(Duration::from_secs(1), sink.close())
            .await
            .expect("cancelled response handoff must not hang");
        assert_eq!(
            result.expect_err("delivery must fail").code(),
            codes::ABANDONED
        );
    }
}
