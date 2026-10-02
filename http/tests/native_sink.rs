use std::time::Duration;

#[allow(dead_code)]
#[path = "../../tests/support/http.rs"]
mod http_support;
use h::bytes::Bytes;
use h::http::{Request, Response};
use http_support::LIMIT;
use respan_gateway_http as h;
use respan_gateway_sdk::futures::{
    SinkExt, StreamExt, TryStreamExt, channel::mpsc, future, stream,
};
use respan_gateway_sdk::{EventSink, EventStream, Extensions, Fault};

#[tokio::test]
async fn a_custom_body_and_sink_keep_feeding_a_retained_reader_without_completion() {
    let connect = h::connect(
        |request: Request<h::Body>| async move { Ok((Response::new(request.into_body()), None)) },
        LIMIT,
    );
    let (read, mut sink) = connect(Extensions::default()).bind(
        |_: &mut EventStream<u8>| {
            let (sender, receiver) = mpsc::channel::<Result<Bytes, Fault>>(0);
            let sink =
                sender
                    .sink_map_err(|_| Fault::abandoned())
                    .with(|item: Result<u8, Fault>| {
                        future::ready(Ok::<_, Fault>(item.map(|value| Bytes::from(vec![value]))))
                    });
            Ok((
                Request::new(h::Body::stream(receiver)),
                EventSink::new(sink),
            ))
        },
        |response, _| response.into_body().map_ok(|bytes| bytes[0]).boxed(),
        h::success,
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
            .expect("retained custom request body must continue making progress");
    delivery.expect("native upload completes");
    assert_eq!(values.expect("read echoed native bytes"), vec![7, 9]);
}
