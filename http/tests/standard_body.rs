use std::sync::Arc;
#[allow(dead_code)]
#[path = "../../tests/support/http.rs"]
mod http_support;
use http_support::LIMIT;
use std::sync::atomic::{AtomicUsize, Ordering};

use h::bytes::Bytes;
use h::http::{HeaderMap, HeaderValue, Request, Response};
use h::http_body::Frame;
use h::http_body_util::{BodyExt, Empty, Full, StreamBody};
use respan_gateway_http as h;
use respan_gateway_sdk::Upstream;
use respan_gateway_sdk::futures::{StreamExt, stream};
use respan_gateway_sdk::{EventStream, Fault};

async fn open(
    request: Request<h::Body>,
    decoded: Arc<AtomicUsize>,
) -> Result<EventStream<Bytes>, Fault> {
    let (incoming, _answer) = h::Incoming::accept(request, false, LIMIT);
    let (opening, _sink) = incoming.bind(
        move |request| {
            Ok(h::messages(request, move |_, bytes| {
                decoded.fetch_add(1, Ordering::SeqCst);
                Ok(bytes)
            }))
        },
        |reply, _: &mut EventStream<()>| {
            Ok(reply.write(
                Response::new(()).into_parts().0,
                h::formats::each(|_: &()| Ok(Bytes::new())),
            ))
        },
        h::plain_refusal,
    );
    opening.await
}

#[tokio::test]
async fn standard_frame_adapter_preserves_data_and_head_but_discards_trailers() {
    let mut trailers = HeaderMap::new();
    trailers.insert("x-trailer", HeaderValue::from_static("ignored"));
    let frames = stream::iter([
        Ok::<_, Fault>(Frame::data(Bytes::from_static(b"one"))),
        Ok(Frame::data(Bytes::from_static(b"two"))),
        Ok(Frame::trailers(trailers)),
    ]);
    let request = Request::builder()
        .method("POST")
        .uri("/native")
        .header("x-kept", "yes")
        .body(StreamBody::new(frames))
        .expect("build native request")
        .map(|body| h::Body::stream(body.into_data_stream()));
    assert_eq!(request.method(), "POST");
    assert_eq!(request.uri(), "/native");
    assert_eq!(request.headers()["x-kept"], "yes");

    let decoded = Arc::new(AtomicUsize::new(0));
    let mut input = open(request, decoded.clone()).await.expect("open request");
    assert!(input.is_single());
    assert_eq!(decoded.load(Ordering::SeqCst), 0);
    assert_eq!(
        input
            .next()
            .await
            .expect("one input")
            .expect("decode input"),
        Bytes::from_static(b"onetwo")
    );
    assert!(input.next().await.is_none());
    assert_eq!(decoded.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn standard_body_collection_keeps_wire_failure_out_of_semantic_decoding() {
    let frames = stream::iter([
        Ok(Frame::data(Bytes::from_static(b"partial"))),
        Err(Upstream::fault("broken native body")),
    ]);
    let body = StreamBody::new(frames);
    let decoded = Arc::new(AtomicUsize::new(0));
    let result = open(
        Request::new(h::Body::stream(body.into_data_stream())),
        decoded.clone(),
    )
    .await;
    let fault = result.expect_err("wire failure must fail opening");
    assert_eq!(h::error_code(&fault), "upstream");
    assert_eq!(fault.message(), "broken native body");
    assert_eq!(decoded.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn incoming_collection_releases_frames_before_requesting_more() {
    use respan_gateway_sdk::futures::channel::oneshot;

    struct PooledFrame(Option<oneshot::Sender<()>>);
    impl AsRef<[u8]> for PooledFrame {
        fn as_ref(&self) -> &[u8] {
            b"first"
        }
    }
    impl Drop for PooledFrame {
        fn drop(&mut self) {
            if let Some(released) = self.0.take() {
                let _ = released.send(());
            }
        }
    }

    let (released, reusable) = oneshot::channel();
    let frames = stream::iter([Ok(Bytes::from_owner(PooledFrame(Some(released))))]).chain(
        stream::once(async move {
            reusable.await.expect("first frame returned to pool");
            Ok(Bytes::from_static(b"second"))
        }),
    );
    let decoded = Arc::new(AtomicUsize::new(0));
    let mut input = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        open(Request::new(h::Body::stream(frames)), decoded.clone()),
    )
    .await
    .expect("collecting must not hold the producer's buffer until EOF")
    .expect("open pooled body");
    assert_eq!(decoded.load(Ordering::SeqCst), 0);
    assert_eq!(
        input.next().await.expect("input").expect("decode"),
        Bytes::from_static(b"firstsecond")
    );
    assert!(input.next().await.is_none());
    assert_eq!(decoded.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn standard_full_and_empty_bodies_decode_lazily() {
    let bodies = [
        h::Body::stream(
            Full::new(Bytes::from_static(b"full"))
                .map_err(Fault::from)
                .into_data_stream(),
        ),
        h::Body::stream(
            Empty::<Bytes>::new()
                .map_err(Fault::from)
                .into_data_stream(),
        ),
    ];
    for (body, expected) in bodies.into_iter().zip([b"full".as_slice(), b"".as_slice()]) {
        let decoded = Arc::new(AtomicUsize::new(0));
        let mut input = open(Request::new(body), decoded.clone())
            .await
            .expect("open standard body");
        assert!(input.is_single());
        assert_eq!(decoded.load(Ordering::SeqCst), 0);
        assert_eq!(
            input.next().await.expect("input").expect("decode"),
            expected
        );
        assert!(input.next().await.is_none());
        assert_eq!(decoded.load(Ordering::SeqCst), 1);
    }
}
