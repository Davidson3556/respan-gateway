use std::time::Duration;

#[allow(dead_code)]
#[path = "../../../../tests/support/http.rs"]
mod http_support;
use h::bytes::Bytes;
use h::http::{Request, Response};
use http_support::LIMIT;
use respan_gateway_core::futures::{SinkExt, StreamExt, TryStreamExt, future, stream};
use respan_gateway_core::{EventStream, Extensions};
use respan_gateway_http as h;

#[tokio::test]
async fn a_live_upload_keeps_feeding_a_retained_reader_without_completion() {
    let connect = h::connect(
        |request: Request<h::Body>| async move { Ok((Response::new(request.into_body()), None)) },
        LIMIT,
    );
    let (read, mut sink) = connect(Extensions::default()).bind(
        |_, _: Option<&u8>| {
            Ok(Box::new(|value: &u8, out: &mut Vec<Bytes>| {
                out.push(Bytes::from(vec![*value]));
                Ok(())
            }))
        },
        |_, _| {
            Some(Box::new(|bytes: Bytes, out: &mut Vec<u8>| {
                out.push(bytes[0]);
                Ok(())
            }))
        },
    );
    let mut source = EventStream::stream(stream::iter([Ok(7_u8), Ok(9_u8)]));
    sink.prepare(&mut source).expect("prepare native upload");

    let send = async move {
        while let Some(item) = source.next().await {
            sink.send(item).await?;
        }
        sink.close().await
    };
    let receive = async move { read.await?.try_collect::<Vec<_>>().await };
    let (delivery, values) =
        tokio::time::timeout(Duration::from_secs(1), future::join(send, receive))
            .await
            .expect("a retained live request body must continue making progress");
    delivery.expect("native upload completes");
    assert_eq!(values.expect("read echoed native bytes"), vec![7, 9]);
}
