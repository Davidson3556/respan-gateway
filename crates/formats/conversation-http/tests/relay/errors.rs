//! Relay errors behavior.

use super::*;
use respan_gateway_core::Fault;

#[tokio::test]
async fn an_error_page_is_relayed_as_it_is() {
    let h = Harness::new(
        vec![Arc::new(respan_gateway_plugin_relay::Relay)],
        Some("openai"),
    );
    let page = "<html>bad gateway</html>";
    h.sites
        .openai
        .reply(fixture::json(StatusCode::BAD_GATEWAY, page));

    let (status, _, body) = h.send(unary_chat()).await;

    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert_eq!(body.ok().as_deref(), Some(page.as_bytes()));
    // The provider answered with the failure, which the end carries.
    let o = h.log.only();
    assert!(
        matches!(o, Err(f) if respan_gateway_http_faults::status(&f) == StatusCode::BAD_GATEWAY && f.get::<respan_gateway_http_faults::ProviderError>().map(|e| &e.0).is_some())
    );
}

#[tokio::test]
async fn a_refusal_is_an_error_in_the_client_format() {
    let fault = Fault::new("balance too low")
        .with(ErrorCode("insufficient_credits".into()))
        .with(Status(StatusCode::PAYMENT_REQUIRED))
        .with(Details(serde_json::Map::from_iter([(
            "balance".into(),
            json!(0.12),
        )])));
    let h = Harness::new(vec![Arc::new(Refuse(fault.clone()))], Some("openai"));

    let (status, headers, body) = h.send(chat()).await;

    assert_eq!(status, StatusCode::PAYMENT_REQUIRED);
    assert_eq!(headers["content-type"], "application/json");
    let err: serde_json::Value = body
        .ok()
        .and_then(|b| serde_json::from_slice(&b).ok())
        .unwrap_or_default();
    assert_eq!(err["error"]["code"], "insufficient_credits");
    assert_eq!(err["error"]["message"], "balance too low");
    assert_eq!(err["error"]["balance"], 0.12);
    assert!(h.sites.sent.sent().is_empty());
    // The fault carries the status.
    let logged = h.log.only().expect_err("the refusal is logged");
    assert_eq!(logged.message(), fault.message());
    assert_eq!(fault_code(&logged), "insufficient_credits");
    assert_eq!(logged.get::<Status>().map(|s| s.0.as_u16()), Some(402));
    assert_eq!(logged.get::<Details>(), fault.get::<Details>());
}

#[tokio::test]
async fn a_unary_body_cut_before_any_bytes_is_an_error_in_the_client_format() {
    let h = relay();
    let mut reset = fixture::json(StatusCode::OK, UNARY);
    *reset.body_mut() = vec![Err(Fault::upstream("connection reset"))];
    h.sites.openai.reply(reset);

    let (status, _, body) = h.send(unary_chat()).await;

    // Nothing was sent, so the head says what happened.
    assert_eq!(status, StatusCode::BAD_GATEWAY);
    let body = String::from_utf8_lossy(&body.unwrap_or_default()).into_owned();
    assert!(body.contains("\"code\":\"upstream\""), "{body}");
    assert!(matches!(h.log.only(), Err(f) if fault_code(&f) == "upstream"));
}

#[tokio::test]
async fn a_broken_unary_success_is_refused_as_the_providers_failure() {
    let h = relay();
    let cut = r#"{"id":"#;
    h.sites.openai.reply(fixture::json(StatusCode::OK, cut));

    // It failed before any content, as `until_content` would find.
    let (status, body) = h.text(unary_chat()).await;
    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert!(body.contains("\"code\":\"upstream\""), "{body}");
    assert!(matches!(h.log.only(), Err(f) if fault_code(&f) == "upstream"));
}

#[tokio::test]
async fn a_gateway_with_no_target_and_no_plugins_refuses_404_in_the_clients_format() {
    let sites = Sites::default();
    let gw = gateway([provider(&sites.openai, "openai", None)], Vec::new(), None)
        .unwrap_or_else(|e| panic!("gateway: {e}"));

    let (status, headers, body) = read_whole(serve(&gw, unary_chat()).await.0).await;

    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(headers["content-type"], "application/json");
    let body = String::from_utf8(body.unwrap_or_default().to_vec()).unwrap_or_default();
    assert!(body.contains("not_found"), "{body}");
    assert!(sites.sent.sent().is_empty());
}

#[tokio::test]
async fn a_client_body_that_breaks_off_is_refused_in_the_clients_format() {
    let h = relay();
    let broken = futures::stream::iter([Err::<http_body::Frame<bytes::Bytes>, _>(
        Fault::new("request body too large")
            .with(ErrorCode("too_large".into()))
            .with(Status(StatusCode::PAYLOAD_TOO_LARGE)),
    )]);
    let body = Body::stream(http_body_util::BodyExt::into_data_stream(
        http_body_util::StreamBody::new(broken),
    ));

    let mut req = http::Request::new(body);
    *req.method_mut() = http::Method::POST;
    *req.uri_mut() = http::Uri::from_static(CHAT);
    let (status, headers, body) = h.send(req).await;

    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(headers["content-type"], "application/json");
    let body = body.map(|b| String::from_utf8_lossy(&b).into_owned());
    assert!(body.is_ok_and(|b| b.contains("\"code\":\"too_large\"")));
    assert!(h.sites.sent.sent().is_empty());
}

#[tokio::test]
async fn a_refusal_its_format_cannot_write_is_a_plain_500() {
    // OpenAI has no error under a success, so the refusal cannot be written.
    let refuse = Refuse(
        Fault::new("not an error")
            .with(ErrorCode("odd".into()))
            .with(Status(StatusCode::OK)),
    );
    let h = Harness::new(vec![Arc::new(refuse)], Some("openai"));

    let (status, headers, body) = h.send(unary_chat()).await;

    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(headers["content-type"], "text/plain; charset=utf-8");
    assert_eq!(body.ok().as_deref(), Some(&b"not an error"[..]));
    // The adapter wrote the plain 500.
    let o = h.log.only();
    assert!(matches!(o, Err(f) if fault_code(&f) == "odd"));
}

#[tokio::test]
async fn a_reframed_stream_that_fails_is_refused_in_the_clients_words() {
    let h = Harness::new(vec![Arc::new(Flip)], Some("openai"));
    let failing = [
        STREAM[0],
        "\"}}]}\n\ndata: {\"error\":{\"message\":\"busy\",\"type\":\"overloaded\",\"param\":null,\"code\":\"overloaded\"}}\n\n",
    ];
    h.sites.openai.reply(fixture::sse(&failing));

    let (status, headers, body) = h.send(unary_chat()).await;

    assert!(!status.is_success(), "{status}");
    assert_eq!(headers["content-type"], "application/json");
    let body: serde_json::Value =
        serde_json::from_slice(&body.unwrap_or_default()).unwrap_or_default();
    assert_eq!(body["error"]["code"], "overloaded", "{body}");
}

#[tokio::test]
async fn a_relayed_redirect_passes_as_it_came_and_is_no_answer() {
    let h = Harness::new(
        vec![Arc::new(respan_gateway_plugin_relay::Relay)],
        Some("openai"),
    );
    let moved = http::Response::builder()
        .status(StatusCode::FOUND)
        .header("location", "http://elsewhere/")
        .body(Vec::new())
        .unwrap_or_default();
    h.sites.openai.reply(moved);

    let (status, headers, body) = h.send(unary_chat()).await;

    assert_eq!(status, StatusCode::FOUND);
    assert_eq!(headers["location"], "http://elsewhere/");
    // r13 (F12-4): a head without a content type gets the format's, a
    // redirect's too.
    assert_eq!(headers["content-type"], "application/json");
    assert_eq!(body.ok().as_deref(), Some(&b""[..]));
    // The redirect is a fault, a 502 to any other format, that
    // carries the provider's answer.
    let end = h.log.only().err();
    assert_eq!(
        end.as_ref().map(respan_gateway_http_faults::status),
        Some(StatusCode::BAD_GATEWAY)
    );
    let answer = end.as_ref().and_then(|f| {
        f.get::<respan_gateway_http_faults::ProviderError>()
            .map(|e| &e.0)
    });
    assert_eq!(answer.map(|a| a.status()), Some(StatusCode::FOUND));
}

#[tokio::test]
async fn a_failure_labelled_as_a_stream_is_still_a_failure() {
    let h = Harness::new(
        vec![Arc::new(respan_gateway_plugin_relay::Relay)],
        Some("openai"),
    );
    let failed = || {
        http::Response::builder()
            .status(StatusCode::SERVICE_UNAVAILABLE)
            .header("content-type", "text/event-stream")
            .body(
                STREAM
                    .iter()
                    .map(|c| Ok(bytes::Bytes::from(*c)))
                    .collect::<Vec<_>>(),
            )
            .unwrap_or_default()
    };
    h.sites.openai.reply(failed());

    let (status, _, body) = h.send(chat()).await;

    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body.ok().as_deref(), Some(STREAM.concat().as_bytes()));
    let end = h
        .log
        .only()
        .map_err(|f| respan_gateway_http_faults::status(&f));
    assert_eq!(end, Err(StatusCode::SERVICE_UNAVAILABLE));
}

#[tokio::test]
async fn a_success_framed_as_an_event_is_the_providers_failure_on_every_path() {
    let odd = "data: {\"id\":\"c2\",\"choices\":[{\"message\":{\"content\":\"hi\"}}]}\n\n";
    for plugins in [Vec::new(), vec![Arc::new(Shout) as Arc<dyn Plugin>]] {
        let h = Harness::new(plugins, Some("openai"));
        h.sites.openai.reply(fixture::json(StatusCode::OK, odd));

        let (status, body) = h.text(unary_chat()).await;

        // Refused and counted a provider failure, plugin or not.
        assert_eq!(status, StatusCode::BAD_GATEWAY);
        assert!(body.contains("\"code\":\"upstream\""), "{body}");
        let end = h.log.only().map_err(|f| fault_code(&f));
        assert_eq!(end, Err("upstream".to_owned()));
    }
}

#[tokio::test]
async fn a_failure_of_several_events_keeps_the_providers_words() {
    struct Ask;

    #[async_trait]
    impl Plugin for Ask {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let answer = cx
                .call("openai")
                .await?
                .outputs::<Output>()
                .try_collect::<Vec<_>>()
                .await;
            let fault = answer.err();
            assert_eq!(fault.as_ref().map(Fault::message), Some("busy"));
            assert_eq!(
                fault.as_ref().map(fault_code).as_deref(),
                Some("overloaded_error")
            );
            assert_eq!(
                fault.map(|f| respan_gateway_http_faults::status(&f).as_u16()),
                Some(529)
            );
            Ok(None)
        }
    }

    let h = Harness::new(vec![Arc::new(Ask)], Some("openai"));
    let busy = "event: ping\ndata: {\"type\":\"ping\"}\n\nevent: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\",\"message\":\"busy\"}}\n\n";
    let failed = http::Response::builder()
        .status(StatusCode::from_u16(529).unwrap_or_default())
        .header("content-type", "text/event-stream")
        .body(vec![Ok(bytes::Bytes::from(busy))])
        .unwrap_or_default();
    h.sites
        .openai
        .reply(failed)
        .reply(fixture::json(StatusCode::OK, UNARY));

    let (status, _, _) = h.send(unary_chat()).await;

    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn an_error_in_a_success_says_what_the_provider_answered() {
    let h = Harness::new(Vec::new(), Some("openai"));
    h.sites
        .openai
        .reply(fixture::json(StatusCode::OK, r#"{"error":{"code":"x"}}"#))
        .reply(fixture::sse(&[
            "data: {\"error\":\"boom\",\"error_type\":\"overloaded\"}\n\n",
        ]));

    let _ = h.send(unary_chat()).await;
    let _ = h.send(chat()).await;

    let ends: Vec<_> = h
        .log
        .all()
        .into_iter()
        .map(|o| {
            o.map_err(|f| {
                (
                    respan_gateway_http_faults::status(&f),
                    fault_code(&f),
                    f.message().to_owned(),
                )
            })
        })
        .collect();
    assert_eq!(
        ends,
        [
            Err((
                StatusCode::BAD_GATEWAY,
                "x".to_owned(),
                "the provider returned 200".to_owned()
            )),
            Err((
                StatusCode::BAD_GATEWAY,
                "overloaded".to_owned(),
                "boom".to_owned()
            )),
        ]
    );
}

#[tokio::test]
async fn a_providers_error_event_in_a_stream_goes_out_as_it_came() {
    let h = relay();
    let error = "data: {\"error\":{\"message\":\"boom\",\"type\":\"server_error\",\"param\":null,\"code\":null}}\n\n";
    h.sites
        .openai
        .reply(fixture::sse(&[STREAM[0], "lo\"}}]}\n\n", error]));

    let (_, _, body) = h.send(chat()).await;

    let body = String::from_utf8(body.unwrap_or_default().to_vec()).unwrap_or_default();
    assert!(body.ends_with(error), "{body}");
    let end = h.log.only().map_err(|f| fault_code(&f));
    assert_eq!(end, Err("server_error".to_owned()));
}

// A provider's error event mid-stream goes as it came to a client of its
// format, fields its format lacks and all.
#[tokio::test]
async fn a_providers_error_event_mid_stream_goes_as_it_came_to_its_format() {
    let h = Harness::new(
        vec![Arc::new(respan_gateway_plugin_relay::Relay)],
        Some("openai"),
    );
    let error = "data: {\"error\":{\"code\":502,\"message\":\"Provider returned error\",\"metadata\":{\"raw\":\"x\"}}}\n\n";
    let content =
        "data: {\"id\":\"c1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Hi\"}}]}\n\n";
    h.sites.openai.reply(fixture::sse(&[content, error]));

    let (_, _, body) = h.send(chat()).await;

    let body = String::from_utf8(body.unwrap_or_default().to_vec()).unwrap_or_default();
    assert_eq!(gist_text(&sent_chunks(&body)), "Hi");
    assert!(body.ends_with(error), "{body}");
    assert!(h.log.only().is_err());
}

// A provider's error inside a 200 goes as it came to a client of its format.
#[tokio::test]
async fn a_providers_error_in_a_success_goes_as_it_came_to_its_format() {
    let h = Harness::new(
        vec![Arc::new(respan_gateway_plugin_relay::Relay)],
        Some("openai"),
    );
    let error = r#"{"error": {"code": 502, "message": "Provider returned error", "metadata": {"raw": "x"}}}"#;
    h.sites.openai.reply(fixture::json(StatusCode::OK, error));

    let (status, body) = h.text(unary_chat()).await;

    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert_eq!(body, error);
}

#[tokio::test]
async fn a_failed_encode_writes_nothing() {
    let sites = Sites::default();
    let gw = fragile(&sites, Vec::new());
    let stream = [HI, &text_chunk("bad"), "data: [DONE]\n\n"];
    sites.openai.reply(fixture::sse(&stream));

    let (_, _, body) = read_whole(serve(&gw, chat()).await.0).await;

    let body = String::from_utf8_lossy(&body.unwrap_or_default()).into_owned();
    assert!(
        body.contains("\"Hi\"") && body.contains("unwritable"),
        "{body}"
    );
    assert!(!body.contains("GARBAGE"), "{body}");
}

#[tokio::test]
async fn an_empty_stream_of_another_kind_is_refused() {
    struct Wrong;

    #[async_trait]
    impl Plugin for Wrong {
        async fn on_client(&self, _cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let none = futures::stream::empty::<Result<String, Fault>>();
            Ok(Some(Response::stream(none)))
        }
    }

    let h = Harness::new(vec![Arc::new(Wrong)], None);

    let (status, body) = h.text(unary_chat()).await;

    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert!(body.contains("internal"), "{body}");
    assert_eq!(code(&h.log.only()), "internal");
}

// A translated head keeps no status; a 304 is neither a success
// nor an error, so a failure, in the client's words.
#[tokio::test]
async fn a_translated_304_is_a_bad_gateway() {
    let sites = Sites::default();
    let p = add_provider("anthropic", AnthropicMessages, sites.claude.plain());
    let gw = providers([p]).and_then(|g| g.client::<Input, _, _>(OpenAiChat)?.target("anthropic"));
    let gw = gw.unwrap_or_else(|e| panic!("gateway: {e}"));
    let not_modified = http::Response::builder().status(StatusCode::NOT_MODIFIED);
    sites
        .claude
        .reply(not_modified.body(Vec::new()).unwrap_or_default());
    let req = call(
        CHAT,
        r#"{"model":"claude-x","messages":[{"role":"user","content":"hi"}]}"#,
    );

    let (status, _, body) = read_whole(serve(&gw, req).await.0).await;

    assert_eq!(status, StatusCode::BAD_GATEWAY);
    let body = String::from_utf8_lossy(&body.unwrap_or_default()).into_owned();
    assert!(body.contains("\"code\":\"upstream\""), "{body}");
}

// Raw errors (bodies and close codes) are a permission the relay grants
// only on a call to a provider of the client's own format.
#[tokio::test]
async fn the_relay_grants_raw_errors_only_within_one_format() {
    let sites = Sites::default();
    let seen = RawPermission::default();
    let same = providers([provider(&sites.openai, "openai", None)]).and_then(|g| {
        g.client::<Input, _, _>(OpenAiChat)?
            .plugin(respan_gateway_plugin_relay::Relay)
            .plugin(seen.clone())
            .target("openai")
    });
    let same = same.unwrap_or_else(|e| panic!("gateway: {e}"));
    sites.openai.reply(fixture::json(StatusCode::OK, UNARY));
    let _ = read_whole(serve(&same, unary_chat()).await.0).await;
    same.idle().await;

    let other = add_provider("anthropic", AnthropicMessages, sites.claude.plain());
    let other = providers([other]).and_then(|g| {
        g.client::<Input, _, _>(OpenAiChat)?
            .plugin(respan_gateway_plugin_relay::Relay)
            .plugin(seen.clone())
            .target("anthropic")
    });
    let other = other.unwrap_or_else(|e| panic!("gateway: {e}"));
    sites
        .claude
        .reply(fixture::json(StatusCode::BAD_REQUEST, "{}"));
    let _ = read_whole(serve(&other, unary_chat()).await.0).await;
    other.idle().await;

    let seen = seen.0.lock().clone();
    assert_eq!(seen, [true, false]);
}

// Another vendor's head and error body are not the client's protocol: the
// client gets its own format's error.
#[tokio::test]
async fn relayed_heads_and_errors_stay_within_one_format() {
    let sites = Sites::default();
    let gw = translating(&sites, respan_gateway_plugin_relay::Relay);
    let raw = r#"{"type":"error","error":{"type":"invalid_request_error","message":"bad"}}"#;
    let mut refused = fixture::json(StatusCode::BAD_REQUEST, raw);
    refused
        .headers_mut()
        .insert("anthropic-organization-id", HeaderValue::from_static("org"));
    sites.claude.reply(refused);
    let req = call(
        CHAT,
        r#"{"model":"claude-x","messages":[{"role":"user","content":"hi"}]}"#,
    );

    let (_, headers, body) = read_whole(serve(&gw, req).await.0).await;

    let body = String::from_utf8_lossy(&body.unwrap_or_default()).into_owned();
    assert_ne!(body, raw, "the provider's body was relayed raw");
    let error: serde_json::Value = serde_json::from_str(&body).unwrap_or_default();
    assert!(error.get("type").is_none(), "{body}");
    assert!(error["error"]["message"].is_string(), "{body}");
    assert!(!headers.contains_key("anthropic-organization-id"));
}

#[tokio::test]
async fn a_provider_hook_that_fails_refuses_the_call() {
    let unsigned = OnProvider(|_| {
        Err(Fault::new("no key")
            .with(ErrorCode("unsigned".into()))
            .with(Status(StatusCode::FORBIDDEN)))
    });
    let h = Harness::new(vec![Arc::new(unsigned)], Some("openai"));
    h.sites.openai.reply(fixture::json(StatusCode::OK, UNARY));

    let (status, body) = h.text(unary_chat()).await;

    assert_eq!(status, StatusCode::FORBIDDEN);
    assert!(body.contains("unsigned"), "{body}");
    assert!(h.sites.sent.sent().is_empty());
}
