//! The HTTP relay contract is an application choice, independent of format names.
#[allow(dead_code)]
#[path = "../../../../tests/support/http.rs"]
mod http_support;
use http_support::{LIMIT, serve};
use respan_gateway_core::{
    ClientContext, Connection, Fault, Format, Gateway, Plugin, Provider, Response, async_trait,
};
use respan_gateway_http::{self as h, bytes::Bytes, http};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

#[derive(Clone)]
struct Text(Arc<AtomicUsize>);
impl Format<h::ClientIo, String, Bytes> for Text {
    fn matches(&self, _: &h::ClientIo) -> bool {
        true
    }
    fn name(&self) -> &str {
        "text"
    }
    fn bind(&self, io: h::ClientIo) -> Result<Connection<String, Bytes>, Fault> {
        let decodes = self.0.clone();
        Ok(io.bind(
            move |_request| {
                {
                    Box::new(move |bytes: Bytes, out: &mut Vec<_>| {
                        let value: Result<_, Fault> = {
                            {
                                decodes.fetch_add(1, Ordering::SeqCst);
                                Ok(format!("encoded:{}", String::from_utf8_lossy(&bytes)))
                            }
                        };
                        out.push(value?);
                        Ok(())
                    })
                }
            },
            |reply| {
                let head = reply.head();
                Ok(reply.write(
                    head,
                    Box::new(|value: &Bytes, out: &mut Vec<Bytes>| {
                        out.push(value.clone());
                        Ok(())
                    }),
                ))
            },
        ))
    }
}
impl Format<h::ProviderIo, Bytes, String> for Text {
    fn name(&self) -> &str {
        "text"
    }
    fn bind(&self, io: h::ProviderIo) -> Result<Connection<Bytes, String>, Fault> {
        Ok(io.bind(
            |_, _: Option<&String>| {
                Ok(Box::new(|value: &String, out: &mut Vec<Bytes>| {
                    out.push(Bytes::from(value.clone()));
                    Ok(())
                }))
            },
            |_, _| {
                Some(Box::new(h::Whole::new(
                    |bytes: Bytes, out: &mut Vec<Bytes>| {
                        out.push(bytes);
                        Ok(())
                    },
                )))
            },
        ))
    }
}
struct Choose {
    key: bool,
    error: bool,
}
#[async_trait]
impl Plugin for Choose {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let mut call = cx.call("p");
        if self.key {
            call = call.with_extension(h::call::CallHeaders(
                cx.extensions()
                    .get::<h::Credential>()
                    .expect("key")
                    .headers()
                    .clone(),
            ));
        }
        let mut response = call.await?;
        if self.error
            && let Err(fault) = response.until_end().await
        {
            let original = fault
                .get::<respan_gateway_http_faults::ProviderError>()
                .expect("inspection hint")
                .0
                .clone();
            return Err(fault.with(h::raw::RawReply(original)));
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
    let sent = Arc::new(parking_lot::Mutex::new(None));
    let seen = sent.clone();
    let format = Text(decodes.clone());
    let mut gateway = Gateway::new()
        .client::<String, Bytes, h::ClientIo>(format.clone())
        .expect("client")
        .provider(Provider::new(
            "p",
            format,
            h::connect(
                move |mut request| {
                    let seen = seen.clone();
                    async move {
                        h::call::credentials(&mut request, &http::HeaderMap::new());
                        let (head, body) = request.into_parts();
                        *seen.lock() = Some(http::Request::from_parts(
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
        ))
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
        .header("connection", "x-hop, upgrade")
        .header("x-hop", "strip")
        .header("upgrade", "h2c")
        .header("host", "gateway.example")
        .header("expect", "100-continue")
        .header("if-none-match", "\"v1\"")
        .header("cookie", "session=client")
        .header("x-forwarded-for", "10.0.0.1")
        .body(h::Body::full("raw unknown bytes"))
        .expect("request");
    let (response, end) = serve(&gateway, request).await;
    let (head, body) = response.into_parts();
    let bytes = BodyExt::collect(body).await.expect("body").to_bytes();
    let _ = end.await;
    gateway.idle().await;
    let request = sent.lock().take().expect("sent");
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
async fn call_headers_can_explicitly_supply_a_key_with_typed_input() {
    let (request, _, decodes) = exercise(
        Some(Choose {
            key: true,
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
        let (_, response, _) = exercise(Some(Choose { key: false, error }), true).await;
        assert_eq!(response.status(), 503);
        assert_eq!(response.body() == "secret upstream bytes", error);
        assert_eq!(response.headers().contains_key("x-provider"), error);
    }
}

/// Two relayed calls, one dropped for the other (a race's loser), or one
/// that fails after its head for a translated retry (a fallback).
struct Pick {
    retry: bool,
}
#[async_trait]
impl Plugin for Pick {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let first = cx.call("p").with_extension(h::raw::Relayed).await?;
        if self.retry {
            let mut first = first;
            assert!(first.until_end().await.is_err(), "the first broke off");
            drop(first);
            return Ok(Some(cx.call("p").await?));
        }
        let second = cx.call("p").with_extension(h::raw::Relayed).await?;
        drop(second);
        Ok(Some(first))
    }
}

/// The client's response to `pick`, each call answered `x-call: <n>`, the
/// first breaking off mid-body if `broken`.
async fn picked(pick: Pick, broken: bool) -> http::Response<Bytes> {
    use h::http_body_util::BodyExt;
    let calls = Arc::new(AtomicUsize::new(0));
    let format = Text(Arc::default());
    let gateway = Gateway::new()
        .client::<String, Bytes, h::ClientIo>(format.clone())
        .expect("client")
        .provider(Provider::new(
            "p",
            format,
            h::connect(
                move |_| {
                    let n = calls.fetch_add(1, Ordering::SeqCst);
                    async move {
                        let body = match broken && n == 0 {
                            true => h::Body::stream(futures::stream::iter([
                                Ok(Bytes::from_static(b"half")),
                                Err(Fault::upstream("broke off")),
                            ])),
                            false => h::Body::full(format!("call {n}")),
                        };
                        let response = http::Response::builder()
                            .status(201)
                            .header("x-call", n.to_string())
                            .body(body)
                            .expect("response");
                        Ok((response, None))
                    }
                },
                LIMIT,
            ),
        ))
        .expect("provider")
        .target("p")
        .expect("target")
        .plugin(pick);
    let request = http::Request::new(h::Body::full("q"));
    let (response, end) = serve(&gateway, request).await;
    let (head, body) = response.into_parts();
    let bytes = BodyExt::collect(body).await.expect("body").to_bytes();
    let _ = end.await;
    gateway.idle().await;
    http::Response::from_parts(head, bytes)
}

#[tokio::test]
async fn a_relayed_answer_dropped_for_another_leaves_the_winners_head() {
    let response = picked(Pick { retry: false }, false).await;
    assert_eq!(response.body(), "call 0");
    assert_eq!(response.status(), 201);
    assert_eq!(response.headers()["x-call"], "0");
}

#[tokio::test]
async fn a_relayed_answer_that_broke_off_leaves_no_head_for_its_retry() {
    let response = picked(Pick { retry: true }, true).await;
    assert_eq!(response.body(), "call 1");
    assert_eq!(response.status(), 200, "the translated answer's own head");
    assert!(!response.headers().contains_key("x-call"));
}
