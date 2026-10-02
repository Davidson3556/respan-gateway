//! The HTTP relay contract is an application choice, independent of format names.
#[allow(dead_code)]
#[path = "../../tests/support/http.rs"]
mod http_support;
use http_support::{LIMIT, serve};
use respan_gateway_http::{self as h, bytes::Bytes, http};
use respan_gateway_sdk::{
    ClientContext, Connection, EventStream, Fault, Format, Gateway, Plugin, Response, async_trait,
};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

#[derive(Clone)]
struct Text(Arc<AtomicUsize>);
impl Format<h::Incoming, String, Bytes> for Text {
    fn matches(&self, _: &h::Incoming) -> bool {
        true
    }
    fn name(&self) -> &str {
        "text"
    }
    fn bind(&self, io: h::Incoming) -> Result<Connection<String, Bytes>, Fault> {
        let decodes = self.0.clone();
        Ok(io.bind(
            move |request| {
                Ok(h::messages(request, move |_, bytes| {
                    decodes.fetch_add(1, Ordering::SeqCst);
                    Ok(format!("encoded:{}", String::from_utf8_lossy(&bytes)))
                }))
            },
            |reply, _: &mut EventStream<Bytes>| {
                let head = reply.head();
                Ok(reply.write(head, h::formats::each(|value: &Bytes| Ok(value.clone()))))
            },
            h::plain_refusal,
        ))
    }
}
impl Format<h::Outgoing, Bytes, String> for Text {
    fn name(&self) -> &str {
        "text"
    }
    fn bind(&self, io: h::Outgoing) -> Result<Connection<Bytes, String>, Fault> {
        Ok(io.bind(
            |source| {
                h::request(
                    source,
                    h::formats::each(|value: &String| Ok(Bytes::from(value.clone()))),
                )
            },
            |response, _| {
                h::formats::whole::read("text", response, |bytes, out| {
                    out.push(bytes);
                    Ok(())
                })
            },
            h::success,
        ))
    }
}
struct Choose {
    raw: bool,
    key: bool,
    head: bool,
    error: bool,
}
#[async_trait]
impl Plugin for Choose {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let slot = h::ProviderHead::default();
        let mut call = cx.call("p").with_extension(slot.clone());
        if self.raw {
            let wire = cx
                .extensions()
                .get::<h::Wire>()
                .expect("adapter publishes raw request");
            let request = wire.get().expect("body available in hook").clone();
            assert!(!request.headers().contains_key("authorization"));
            call = call.with_extension(h::SendRaw(h::Wire::new(request, false)));
        }
        if self.key {
            call = call.with_extension(h::CallHeaders(
                cx.extensions()
                    .get::<h::Credential>()
                    .expect("key")
                    .headers()
                    .clone(),
            ));
        }
        let mut response = call.await?;
        let head = slot.get().expect("head published before call resolves");
        if self.head {
            cx.extensions()
                .get::<h::ClientHead>()
                .expect("client slot")
                .set(Some(head.clone()));
        }
        if self.error
            && head.status.is_server_error()
            && let Err(fault) = response.until_end().await
        {
            let original = fault
                .get::<h::ProviderError>()
                .expect("inspection hint")
                .response
                .clone();
            return Err(fault.with(h::RawReply(original)));
        }
        Ok(Some(response))
    }
}
async fn exercise(
    choice: Option<Choose>,
    failure: bool,
) -> (http::Request<Bytes>, http::Response<Bytes>, usize) {
    use h::http_body_util::BodyExt;
    let decodes = Arc::new(AtomicUsize::new(0));
    let sent = Arc::new(Mutex::new(None));
    let seen = sent.clone();
    let format = Text(decodes.clone());
    let mut gateway = Gateway::new()
        .client::<String, Bytes, h::Incoming>(format.clone())
        .expect("client")
        .provider::<String, Bytes, _>(
            "p",
            format,
            h::connect(
                move |mut request| {
                    let seen = seen.clone();
                    async move {
                        h::credentials(&mut request, &http::HeaderMap::new());
                        let (head, body) = request.into_parts();
                        *seen.lock().expect("record") = Some(http::Request::from_parts(
                            head,
                            BodyExt::collect(body).await?.to_bytes(),
                        ));
                        let response = http::Response::builder()
                            .status(if failure { 503 } else { 201 })
                            .header("x-provider", "private")
                            .body(h::Body::full(if failure {
                                "secret upstream bytes"
                            } else {
                                "ok"
                            }))
                            .expect("response");
                        Ok((response, None))
                    }
                },
                LIMIT,
            ),
        )
        .expect("provider")
        .target("p")
        .expect("target");
    if let Some(choice) = choice {
        gateway = gateway.plugin(choice);
    }
    let request = http::Request::builder()
        .method("PATCH")
        .uri("/original?x=1")
        .header("authorization", "Bearer client")
        .header("x-client", "custom")
        .header("connection", "x-hop")
        .header("x-hop", "strip")
        .body(h::Body::full("raw unknown bytes"))
        .expect("request");
    let (response, end) = serve(&gateway, request).await;
    let (head, body) = response.into_parts();
    let bytes = BodyExt::collect(body).await.expect("body").to_bytes();
    let _ = end.await;
    gateway.idle().await;
    let request = sent.lock().expect("record").take().expect("sent");
    (
        request,
        http::Response::from_parts(head, bytes),
        decodes.load(Ordering::SeqCst),
    )
}
#[tokio::test]
async fn same_name_default_encodes_without_client_key_or_provider_head() {
    let (request, response, decodes) = exercise(None, false).await;
    assert_eq!(decodes, 1);
    assert_eq!(request.body(), "encoded:raw unknown bytes");
    assert!(!request.headers().contains_key("authorization"));
    assert!(!request.headers().contains_key("x-client"));
    assert_eq!(response.status(), 200);
    assert!(!response.headers().contains_key("x-provider"));
}
#[tokio::test]
async fn explicit_raw_request_skips_decoding_and_keys_require_separate_copy() {
    for key in [false, true] {
        let (request, response, decodes) = exercise(
            Some(Choose {
                raw: true,
                key,
                head: true,
                error: false,
            }),
            false,
        )
        .await;
        assert_eq!(decodes, 0);
        assert_eq!(request.body(), "raw unknown bytes");
        assert_eq!(request.method(), "PATCH");
        assert_eq!(request.uri(), "/original?x=1");
        assert_eq!(request.headers().contains_key("authorization"), key);
        assert!(!request.headers().contains_key("x-hop"));
        assert_eq!(response.status(), 201);
        assert_eq!(response.headers()["x-provider"], "private");
    }
}
#[tokio::test]
async fn call_headers_can_explicitly_supply_a_key_with_typed_input() {
    let (request, _, decodes) = exercise(
        Some(Choose {
            raw: false,
            key: true,
            head: false,
            error: false,
        }),
        false,
    )
    .await;
    assert_eq!(decodes, 1);
    assert_eq!(request.headers()["authorization"], "Bearer client");
}
#[tokio::test]
async fn provider_errors_are_inspectable_but_only_explicit_raw_replies_are_written() {
    for error in [false, true] {
        let (_, response, _) = exercise(
            Some(Choose {
                raw: false,
                key: false,
                head: false,
                error,
            }),
            true,
        )
        .await;
        assert_eq!(response.status(), 503);
        assert_eq!(response.body() == "secret upstream bytes", error);
        assert_eq!(response.headers().contains_key("x-provider"), error);
    }
}
