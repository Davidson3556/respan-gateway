//! Relay requests behavior.

use super::*;

#[tokio::test]
async fn requests_no_served_format_accepts_are_404() {
    let h = relay();
    let mut get = call(CHAT, "{}");
    *get.method_mut() = http::Method::GET;
    for req in [call("/nope", "{}"), call("/v1/messages", "{}"), get] {
        let (status, headers, body) = h.send(req).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(headers["content-type"], "text/plain; charset=utf-8");
        let body = String::from_utf8(body.unwrap_or_default().to_vec()).unwrap_or_default();
        assert!(body.starts_with("no format served accepts "), "{body}");
    }
    assert!(h.log.all().is_empty());
    assert!(h.sites.sent.sent().is_empty());
}

#[tokio::test]
async fn plugins_see_the_request_and_change_its_model() {
    #[derive(Clone)]
    struct Seen(&'static str);

    /// Changes the model and records what `on_client_end` finds in the
    /// extensions.
    #[derive(Clone, Default)]
    struct Redirect(Arc<Mutex<Option<&'static str>>>);

    #[async_trait]
    impl Plugin for Redirect {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let asked = cx
                .extensions()
                .get::<Original>()
                .and_then(Original::get)
                .map(|a| a.method().clone());
            assert_eq!(asked, Some(http::Method::POST));
            // The client's request, as it said it.
            let chat = cx.input::<Input>()?;

            let said = chat.messages[0].text();
            cx.extensions_mut()
                .insert(Seen(if said == "hi" { "hi" } else { "other" }));
            cx.filter(set_model("gpt-small"))?;
            Ok(None)
        }

        async fn on_client_end(&self, cx: &ClientContext, _end: &Result<(), Fault>) {
            if let Ok(mut seen) = self.0.lock() {
                *seen = cx.extensions().get::<Seen>().map(|s| s.0);
            }
        }
    }

    let redirect = Redirect::default();
    let h = Harness::new(vec![Arc::new(redirect.clone())], Some("openai"));
    h.sites.openai.reply(fixture::sse(STREAM));
    let _ = h.send(chat()).await;

    let sent: serde_json::Value =
        serde_json::from_slice(h.sites.sent.sent()[0].body()).unwrap_or_default();
    assert_eq!(sent["model"], "gpt-small");
    assert_eq!(says(h.log.only()), Ok(()));
    assert_eq!(redirect.0.lock().map(|s| *s).ok().flatten(), Some("hi"));
}

#[tokio::test(flavor = "multi_thread")]
async fn end_hooks_run_on_the_requests_runtime_wherever_its_response_is_read() {
    /// Notes whether its end hook ran on a tokio runtime.
    #[derive(Clone, Default)]
    struct OnRuntime(Arc<Mutex<Option<bool>>>);

    #[async_trait]
    impl Plugin for OnRuntime {
        async fn on_client_end(&self, _cx: &ClientContext, _end: &Result<(), Fault>) {
            let on = tokio::runtime::Handle::try_current().is_ok();
            self.0.lock().map(|mut s| *s = Some(on)).ok();
        }
    }

    let sites = Sites::default();
    let noted = OnRuntime::default();
    let gw = gateway(
        [provider(&sites.openai, "openai", None)],
        vec![Arc::new(noted.clone())],
        Some("openai"),
    )
    .unwrap_or_else(|e| panic!("{e}"));
    sites.openai.reply(fixture::json(StatusCode::OK, UNARY));

    let resp = serve(&gw, unary_chat()).await.0;
    // Read to its end on a thread with no runtime, which starts the hooks.
    let read = std::thread::spawn(move || futures::executor::block_on(read_whole(resp)));
    let _ = read.join();
    gw.idle().await;

    assert_eq!(noted.0.lock().map(|s| *s).ok().flatten(), Some(true));
}

#[tokio::test]
async fn an_embedder_reads_how_its_request_ended() {
    let h = relay();
    h.sites.openai.reply(fixture::sse(STREAM));
    let req = chat();

    let (resp, ended) = serve(&h.gw, req).await;
    let _ = read_whole(resp).await;

    assert_eq!(says(ended.await), Ok(()));
}

#[tokio::test]
async fn every_response_says_how_its_request_ended() {
    let h = relay();

    let (resp, ended) = serve(&h.gw, post("/nowhere", UNARY_REQ)).await;

    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    // Its end is that of the plain refusal.
    let end = ended.await.map_err(|f| fault_code(&f));
    assert_eq!(end, Err("not_found".to_owned()));
}

#[tokio::test]
async fn a_plugin_knows_which_format_the_client_speaks() {
    struct Seen(Arc<Mutex<Vec<String>>>);

    #[async_trait]
    impl Plugin for Seen {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            if let Ok(mut seen) = self.0.lock() {
                seen.push(cx.format().to_owned());
            }
            Ok(None)
        }
    }

    let seen = Arc::new(Mutex::new(Vec::new()));
    let sites = Sites::default();
    let gw = providers([provider(&sites.openai, "openai", Some("Bearer sk-test"))])
        .and_then(|g| {
            g.client::<Input, _, _>(OpenAiChat)?
                .client::<Input, _, _>(AnthropicMessages)
        })
        .and_then(|g| g.plugin(Seen(seen.clone())).target("openai"))
        .expect("builds");
    let messages = r#"{"model":"gpt-x","max_tokens":10,"messages":[]}"#;
    for req in [post(CHAT, UNARY_REQ), post(MESSAGES, messages)] {
        sites.openai.reply(fixture::json(StatusCode::OK, UNARY));
        let _ = read_whole(serve(&gw, req).await.0).await;
    }

    assert_eq!(
        seen.lock().map(|s| s.clone()).ok(),
        Some(vec![
            "openai-chat".to_owned(),
            "anthropic-messages".to_owned()
        ])
    );
}

#[tokio::test]
async fn a_calls_own_input_its_format_cannot_write_is_refused_as_a_bad_request() {
    /// Asks again with a message of its own the format has no form for.
    struct Own;

    #[async_trait]
    impl Plugin for Own {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let image = json!({"type": "image", "source": {"type": "url", "url": "u"}});
            let message = Message::new(Role::User, Content::native("anthropic-messages", image));
            let own = Input::new([message]);
            cx.call("openai").with_input(own).await.map(Some)
        }
    }

    let h = Harness::new(vec![Arc::new(Own)], None);

    let (status, _, _) = h.send(unary_chat()).await;

    // Without the bytes a request came as, the gateway cannot tell a plugin's
    // message from the client's: a request it cannot write is the client's.
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(matches!(h.log.only(), Err(f) if fault_code(&f) == "bad_request"));
    assert!(h.sites.sent.sent().is_empty());
}

// A call's own input copied from the client's keeps what the kind has no
// place for, in the format's own terms.
#[tokio::test]
async fn a_calls_own_input_copied_from_the_clients_keeps_what_it_said() {
    /// Asks again with a trimmed copy of the client's request.
    struct Trim;

    #[async_trait]
    impl Plugin for Trim {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let mut trimmed = cx.input::<Input>()?.clone();
            trimmed.messages.truncate(1);
            cx.call("openai").with_input(trimmed).await.map(Some)
        }
    }

    let h = Harness::new(vec![Arc::new(Trim)], None);
    h.sites.openai.reply(fixture::json(StatusCode::OK, UNARY));
    let body = json!({"model": "gpt-x", "tools": [{"type": "function", "function": {"name": "f"}}],
        "messages": [{"role": "user", "content": "a"}, {"role": "user", "content": "b"}]});

    let (status, _, _) = h.send(call(CHAT, body.to_string())).await;

    assert_eq!(status, StatusCode::OK);
    let sent: serde_json::Value =
        serde_json::from_slice(h.sites.sent.sent()[0].body()).unwrap_or_default();
    assert_eq!(sent["tools"], body["tools"]);
    assert_eq!(sent["messages"], json!([{"role": "user", "content": "a"}]));
}

#[tokio::test]
async fn a_whole_answer_to_a_calls_own_input_reaches_a_streaming_client_as_a_stream() {
    struct Trim;

    #[async_trait]
    impl Plugin for Trim {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let own = Input::new([Message::new(Role::User, "hi")]);
            Ok(Some(cx.call("openai").with_input(own).await?))
        }
    }

    let h = Harness::new(vec![Arc::new(Trim)], Some("openai"));
    h.sites.openai.reply(fixture::json(StatusCode::OK, UNARY));

    let (status, headers, body) = h.send(chat()).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers["content-type"], "text/event-stream");
    let body = String::from_utf8(body.unwrap_or_default().to_vec()).unwrap_or_default();
    assert!(body.starts_with("data: {"), "{body}");
    assert!(body.contains("\"content\":\"hi\""), "{body}");
    assert!(body.ends_with("data: [DONE]\n\n"), "{body}");
}

#[tokio::test]
async fn a_redirect_answering_a_calls_own_input_is_a_bad_gateway() {
    struct Trim;

    #[async_trait]
    impl Plugin for Trim {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let own = Input::new([Message::new(Role::User, "hi")]);
            // The call's response fails as read, not the call at
            // its head; sent, the provider's words stay off a client whose
            // message the call did not relay.
            Ok(Some(cx.call("openai").with_input(own).await?))
        }
    }

    let h = Harness::new(vec![Arc::new(Trim)], Some("openai"));
    let moved = http::Response::builder()
        .status(StatusCode::FOUND)
        .header("location", "http://elsewhere/")
        .body(Vec::new())
        .unwrap_or_default();
    h.sites.openai.reply(moved);

    let (status, headers, _) = h.send(unary_chat()).await;

    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert!(headers.get("location").is_none());
}

#[tokio::test]
async fn a_failure_answering_a_calls_own_input_keeps_its_status_and_retry_after() {
    struct Trim;

    #[async_trait]
    impl Plugin for Trim {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let own = Input::new([Message::new(Role::User, "hi")]);
            Ok(Some(cx.call("openai").with_input(own).await?))
        }
    }

    let h = Harness::new(vec![Arc::new(Trim)], Some("openai"));
    let mut limited = fixture::json(
        StatusCode::TOO_MANY_REQUESTS,
        r#"{"error":{"message":"slow"}}"#,
    );
    limited
        .headers_mut()
        .insert("retry-after", HeaderValue::from_static("7"));
    limited
        .headers_mut()
        .insert("x-request-id", HeaderValue::from_static("req_1"));
    h.sites.openai.reply(limited);

    let (status, headers, _) = h.send(unary_chat()).await;

    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(headers["retry-after"], "7");
    assert!(headers.get("x-request-id").is_none());
}

#[tokio::test]
async fn a_call_refused_on_the_provider_edge_refuses_the_request_before_sending() {
    struct Deny;

    #[async_trait]
    impl Plugin for Deny {
        async fn on_provider(&self, _px: &mut ProviderContext) -> Result<(), Fault> {
            Err(Fault::new("no calls")
                .with(ErrorCode("denied".into()))
                .with(Status(StatusCode::FORBIDDEN)))
        }
    }

    let bill = Bill::default();
    let h = Harness::new(vec![Arc::new(Deny), Arc::new(bill.clone())], Some("openai"));

    let (status, body) = h.text(unary_chat()).await;

    assert_eq!(status, StatusCode::FORBIDDEN);
    assert!(body.contains(r#""code":"denied""#), "{body}");
    assert!(h.sites.sent.sent().is_empty());
    // Refused before the plugins after it ran on the provider edge.
    assert_eq!(bill.started(), ["client"]);
    assert!(matches!(h.log.only(), Err(f) if fault_code(&f) == "denied"));
}

#[tokio::test]
async fn a_calls_own_copy_of_the_input_changes_only_that_call() {
    /// Cools only calls to `openai`; copies every call's input.
    struct CoolOpenai;

    #[async_trait]
    impl Plugin for CoolOpenai {
        async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
            if px.provider() != "openai" {
                return Ok(());
            }
            px.filter(|chat: &mut Input| {
                chat.options.temperature = Some(0.0);
                Ok(())
            })
        }
    }

    let chain: Vec<Arc<dyn Plugin>> = vec![
        Arc::new(CoolOpenai),
        Arc::new(Fallback(vec!["openai", "backup"])),
    ];
    let h = Harness::new(chain, None);
    h.sites
        .openai
        .reply(fixture::json(StatusCode::SERVICE_UNAVAILABLE, "{}"));
    h.sites.backup.reply(fixture::json(StatusCode::OK, UNARY));

    assert_eq!(h.text(unary_chat()).await.0, StatusCode::OK);

    let sent = h.sites.sent.sent();
    let cooled: serde_json::Value = serde_json::from_slice(sent[0].body()).unwrap_or_default();
    assert_eq!(cooled["temperature"], 0.0);
    // The other call is the request as it said it.
    let plain: serde_json::Value = serde_json::from_slice(sent[1].body()).unwrap_or_default();
    assert_eq!(plain, json!({ "model": "gpt-x", "messages": [] }));
}

#[tokio::test]
async fn a_model_set_on_the_provider_edge_is_that_calls_own() {
    let cheap = OnProvider(|px| {
        if px.provider() == "backup" {
            px.filter(set_model("cheap"))?;
        }
        Ok(())
    });
    let bill = Bill::default();
    let chain: Vec<Arc<dyn Plugin>> = vec![
        Arc::new(cheap),
        Arc::new(bill.clone()),
        Arc::new(Fallback(vec!["openai", "backup"])),
    ];
    let h = Harness::new(chain, None);
    h.sites
        .openai
        .reply(fixture::json(StatusCode::SERVICE_UNAVAILABLE, "{}"));
    h.sites.backup.reply(fixture::json(StatusCode::OK, UNARY));

    assert_eq!(h.text(unary_chat()).await.0, StatusCode::OK);

    let models: Vec<serde_json::Value> = h
        .sites
        .sent
        .sent()
        .iter()
        .map(|r| serde_json::from_slice::<serde_json::Value>(r.body()).unwrap_or_default())
        .map(|v| v["model"].clone())
        .collect();
    assert_eq!(models, [json!("gpt-x"), json!("cheap")]);
    assert_eq!(bill.started(), ["client", "openai/gpt-x", "backup/cheap"]);
}

#[tokio::test]
async fn a_calls_request_is_as_it_was_when_the_call_was_made() {
    /// Calls twice, changing the request's model in between.
    #[derive(Clone, Default)]
    struct Twice(Arc<Mutex<Vec<String>>>);

    #[async_trait]
    impl Plugin for Twice {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            cx.filter(set_model("a"))?;
            drop(cx.call("openai").await?);
            cx.filter(set_model("b"))?;
            cx.call("openai").await.map(Some)
        }

        async fn on_provider_end(&self, px: &ProviderContext, _end: &Result<(), Fault>) {
            let model = px.client().input::<Input>().ok();
            let model = model.and_then(|c| c.model.clone());
            self.0.lock().map(|mut s| s.extend(model)).ok();
        }
    }

    let twice = Twice::default();
    let h = Harness::new(vec![Arc::new(twice.clone())], Some("openai"));
    h.sites.openai.reply(fixture::json(StatusCode::OK, UNARY));
    h.sites.openai.reply(fixture::json(StatusCode::OK, UNARY));

    assert_eq!(h.text(unary_chat()).await.0, StatusCode::OK);
    let mut seen = twice.0.lock().map(|s| s.clone()).unwrap_or_default();
    seen.sort();
    assert_eq!(seen, ["a", "b"]);
}

#[tokio::test]
async fn an_undecodable_input_is_refused_without_explicit_raw_delivery() {
    /// Reads the input on both edges and changes nothing.
    struct Read;

    #[async_trait]
    impl Plugin for Read {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let _ = cx.input::<Input>();
            Ok(None)
        }

        async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
            let _ = px.input::<Input>();
            Ok(())
        }
    }

    let h = Harness::new(vec![Arc::new(Read)], Some("openai"));
    h.sites.openai.reply(fixture::json(StatusCode::OK, UNARY));
    let request = r#"["not an object"]"#;

    assert_eq!(h.text(call(CHAT, request)).await.0, StatusCode::BAD_REQUEST);
    assert!(h.sites.sent.sent().is_empty());
}

#[tokio::test]
async fn extensions_set_in_on_provider_reach_the_transport() {
    let key = OnProvider(|px| {
        px.extensions_mut().insert(TenantKey("k1"));
        Ok(())
    });
    let sites = Sites::default();
    let gw = gateway(
        [add_provider(
            "openai",
            OpenAiChat,
            TenantSender(sites.openai.plain()),
        )],
        vec![Arc::new(key)],
        Some("openai"),
    )
    .unwrap_or_else(|e| panic!("gateway: {e}"));
    sites.openai.reply(fixture::json(StatusCode::OK, UNARY));
    let req = unary_chat();

    assert_eq!(read_whole(serve(&gw, req).await.0).await.0, StatusCode::OK);
    let sent = sites.sent.sent();
    let key = sent[0].headers().get("x-tenant-key");
    assert_eq!(key.and_then(|k| k.to_str().ok()), Some("k1"));
}

#[tokio::test]
async fn a_calls_own_extension_reaches_its_transport() {
    /// Answers with a call carrying a tenant's key.
    struct Tenant;

    #[async_trait]
    impl Plugin for Tenant {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let call = cx.call("openai").with_extension(TenantKey("k2"));
            call.await.map(Some)
        }
    }

    let sites = Sites::default();
    let transport = TenantSender(sites.openai.plain());
    let gw = gateway(
        [add_provider("openai", OpenAiChat, transport)],
        vec![Arc::new(Tenant)],
        None,
    )
    .unwrap_or_else(|e| panic!("gateway: {e}"));
    sites.openai.reply(fixture::json(StatusCode::OK, UNARY));

    assert_eq!(
        read_whole(serve(&gw, unary_chat()).await.0).await.0,
        StatusCode::OK
    );
    let key = sites.sent.sent()[0].headers().get("x-tenant-key").cloned();
    assert_eq!(key, Some(HeaderValue::from_static("k2")));
}

#[tokio::test]
async fn a_response_goes_only_to_the_request_whose_call_it_answers() {
    /// On a request marked `x-keep`, keeps a call's response where the
    /// next request sends it.
    struct Swap(Kept);

    #[async_trait]
    impl Plugin for Swap {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let asked = cx.extensions().get::<Original>().and_then(Original::get);
            match asked.is_some_and(|a| a.headers().contains_key("x-keep")) {
                true => {
                    let resp = cx.call("backup").await?;
                    *self.0.0.lock().map_err(|_| Fault::internal("poisoned"))? = Some(resp);
                    Ok(None)
                }
                _ => Ok(self.0.take()),
            }
        }
    }

    let (ends, swap) = (Ends::default(), Arc::new(Swap(Kept::default())));
    let sites = Sites::default();
    let gw = gateway(
        [
            provider(&sites.openai, "openai", Some("Bearer sk-test")),
            provider(&sites.backup, "backup", None),
        ],
        vec![Arc::new(ends.clone()), swap],
        Some("openai"),
    )
    .expect("builds");
    sites.backup.reply(fixture::sse(STREAM));
    sites.openai.reply(fixture::json(StatusCode::OK, UNARY));

    // Request `a` is still open, its answer unread, when `b` runs.
    let mut a = post(CHAT, UNARY_REQ);
    a.headers_mut()
        .insert("x-keep", HeaderValue::from_static("1"));
    let (a, _) = serve(&gw, a).await;
    let (status, _, body) = read_whole(serve(&gw, post(CHAT, UNARY_REQ)).await.0).await;
    let _ = read_whole(a).await;
    gw.idle().await;

    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    let body = String::from_utf8_lossy(&body.unwrap_or_default()).into_owned();
    assert!(body.contains("internal"), "{body}");
    let mut ends = ends.all();
    ends.sort();
    assert_eq!(ends, ["backup abandoned", "client", "client", "openai ok"]);
}

#[tokio::test]
async fn a_body_that_broke_off_is_refused_without_its_model_read() {
    let sites = Sites::default();
    let gw = fragile(&sites, Vec::new());
    let too_large = Fault::new("too large")
        .with(ErrorCode("too_large".into()))
        .with(Status(StatusCode::PAYLOAD_TOO_LARGE));
    let broken = futures::stream::iter([Err::<http_body::Frame<bytes::Bytes>, _>(too_large)]);
    let mut req = http::Request::new(http_body_util::StreamBody::new(broken));
    *req.method_mut() = http::Method::POST;
    *req.uri_mut() = http::Uri::from_static(CHAT);

    let (resp, ended) = serve(&gw, req).await;
    let (status, headers, _) = read_whole(resp).await;

    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(headers["content-type"], "application/json");
    let end = ended.await.map_err(|f| fault_code(&f));
    assert_eq!(end, Err("too_large".to_owned()));
}

#[tokio::test]
async fn a_call_with_its_own_input_names_only_its_own_model() {
    /// Calls with its own input, with a model and without.
    struct Own;

    #[async_trait]
    impl Plugin for Own {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let chat = || Input::new([Message::new(Role::User, "judge")]);
            let _ = cx.call("openai").with_input(chat()).await?;
            let _ = cx
                .call("openai")
                .with_input(judged(chat(), "judge"))
                .await?;
            Ok(Some(cx.call("openai").await?))
        }
    }

    let h = Harness::new(vec![Arc::new(Own)], None);
    for _ in 0..3 {
        h.sites.openai.reply(fixture::json(StatusCode::OK, UNARY));
    }

    assert_eq!(h.text(unary_chat()).await.0, StatusCode::OK);

    let models: Vec<serde_json::Value> = h
        .sites
        .sent
        .sent()
        .iter()
        .map(|r| serde_json::from_slice::<serde_json::Value>(r.body()).unwrap_or_default())
        .map(|body| body["model"].clone())
        .collect();
    assert_eq!(models, [json!(null), json!("judge"), json!("gpt-x")]);
}
