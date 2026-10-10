//! Relay lifecycle behavior.

use super::*;

#[tokio::test]
async fn a_cut_stream_ends_with_an_error_event_relayed_or_filtered() {
    for (cut, chain) in [
        (2, Vec::<Arc<dyn Plugin>>::new()),
        (2, vec![Arc::new(Shout) as Arc<dyn Plugin>]),
        // Mid-event: the half event is dropped, nothing was sent, so the
        // response is refused.
        (1, Vec::new()),
        (1, vec![Arc::new(Shout) as Arc<dyn Plugin>]),
    ] {
        let filtered = !chain.is_empty();
        let h = Harness::new(chain, Some("openai"));
        h.sites.openai.reply(fixture::sse(&STREAM[..cut]));

        let (status, _, body) = h.send(chat()).await;
        let body = String::from_utf8_lossy(&body.unwrap_or_default()).into_owned();

        if cut == 1 {
            assert_ne!(status, StatusCode::OK, "cut {cut}, filtered {filtered}");
            assert!(body.contains("\"code\":\"truncated\""), "{body}");
        } else {
            assert!(
                body.ends_with(TRUNCATED),
                "cut {cut}, filtered {filtered}: {body}"
            );
        }
        assert!(!body.contains("[DONE]"));
        assert!(matches!(h.log.only(), Err(f) if fault_code(&f) == "truncated"));
    }
}

#[tokio::test]
async fn a_transport_error_mid_stream_ends_with_an_error_event() {
    let h = relay();
    let mut broken = fixture::sse(STREAM);
    broken.body_mut().truncate(2);
    broken
        .body_mut()
        .push(Err(Upstream::fault("connection reset")));
    h.sites.openai.reply(broken);

    let body = h.body(chat()).await;

    let text: String = sent_chunks(&body)
        .iter()
        .filter_map(|c| {
            c["choices"][0]["delta"]["content"]
                .as_str()
                .map(str::to_owned)
        })
        .collect();
    assert_eq!(text, "Hello world", "{body}");
    assert!(body.ends_with("\"type\":\"upstream\",\"param\":null,\"code\":\"upstream\"}}\n\n"));
    assert!(matches!(h.log.only(), Err(f) if fault_code(&f) == "upstream"));
}

#[tokio::test]
async fn a_client_that_leaves_mid_response_ends_once() {
    let h = relay();
    h.sites.openai.reply(fixture::sse(STREAM));

    let (_, mut body) = serve(&h.gw, chat()).await.0.into_parts();
    let _first = http_body_util::BodyExt::frame(&mut body).await;
    drop(body);
    h.gw.idle().await;

    let o = h.log.only();
    assert!(matches!(o, Err(f) if fault_code(&f) == "abandoned"));
}

#[tokio::test]
async fn a_client_that_leaves_before_a_response_ends_once() {
    let hang = Outcomes::default();
    let h = Harness::new(vec![Arc::new(Hang(hang.clone()))], Some("openai"));

    let gave_up = tokio::time::timeout(Duration::from_millis(10), serve(&h.gw, chat())).await;
    assert!(gave_up.is_err());
    h.gw.idle().await;

    let o = h.log.only();
    assert!(matches!(o, Err(f) if fault_code(&f) == "abandoned"));
    assert_eq!(
        hang.all().len(),
        1,
        "the plugin cut short gets its end hook too"
    );
}

#[tokio::test]
async fn only_plugins_that_ran_get_end_hooks() {
    let after = Outcomes::default();
    let h = Harness::new(
        vec![
            Arc::new(Refuse(BadRequest::fault("no"))),
            Arc::new(after.clone()),
        ],
        Some("openai"),
    );

    let _ = h.send(chat()).await;

    assert_eq!(h.log.all().len(), 1);
    assert!(after.all().is_empty());
}

#[tokio::test]
async fn a_panicking_end_hook_does_not_skip_the_others() {
    let after = Outcomes::default();
    let h = Harness::new(
        vec![Arc::new(PanicAtEnd), Arc::new(after.clone())],
        Some("openai"),
    );
    h.sites.openai.reply(fixture::sse(STREAM));

    let _ = h.send(chat()).await;

    assert_eq!(says(after.only()), Ok(()));
}

#[tokio::test]
async fn a_panic_in_a_filter_is_refused_as_internal() {
    struct Explode;

    #[async_trait]
    impl Plugin for Explode {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            cx.filter(|_: &mut Output| panic!("a buggy filter"))?;
            Ok(None)
        }
    }

    let h = Harness::new(vec![Arc::new(Explode)], Some("openai"));
    h.sites.openai.reply(fixture::sse(STREAM));

    let (status, _, body) = h.send(chat()).await;

    // Nothing was sent before the panic, so the client gets an error in its
    // format, not a dropped connection.
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    let body = body.map(|b| String::from_utf8_lossy(&b).into_owned());
    assert!(body.is_ok_and(|b| b.contains("internal")));
    assert!(matches!(h.log.only(), Err(f) if fault_code(&f) == "internal"));
}

#[tokio::test]
async fn a_plugin_that_panics_is_refused_as_internal() {
    struct Explode;

    #[async_trait]
    impl Plugin for Explode {
        async fn on_client(&self, _cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            panic!("a buggy plugin")
        }
    }

    let h = Harness::new(vec![Arc::new(Explode)], Some("openai"));

    let (status, _, body) = h.send(chat()).await;

    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    let body = body.map(|b| String::from_utf8_lossy(&b).into_owned());
    assert!(body.is_ok_and(|b| b.contains("internal")));
    assert!(matches!(h.log.only(), Err(f) if fault_code(&f) == "internal"));
}

#[tokio::test]
async fn a_call_kept_past_its_request_cannot_call() {
    /// Keeps a call, and awaits it once the request ended.
    #[derive(Clone, Default)]
    struct Late(
        Arc<Mutex<Option<respan_gateway_core::Call>>>,
        Arc<Mutex<Option<String>>>,
    );

    #[async_trait]
    impl Plugin for Late {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let call = cx.call("openai");
            self.0.lock().map(|mut kept| *kept = Some(call)).ok();
            Ok(None)
        }

        async fn on_client_end(&self, _cx: &ClientContext, _end: &Result<(), Fault>) {
            let Some(call) = self.0.lock().ok().and_then(|mut kept| kept.take()) else {
                return;
            };
            let err = call.await.err().map(|f| f.message().to_owned());
            self.1.lock().map(|mut seen| *seen = err).ok();
        }
    }

    let late = Late::default();
    let h = Harness::new(vec![Arc::new(late.clone())], Some("openai"));
    h.sites.openai.reply(fixture::sse(STREAM));
    let _ = h.send(chat()).await;
    h.gw.idle().await;

    let err = late.1.lock().map(|e| e.clone()).ok().flatten();
    assert_eq!(
        err.as_deref(),
        Some("providers can be called until the request ends")
    );
    assert_eq!(h.sites.sent.sent().len(), 1);
}

#[tokio::test]
async fn a_client_that_leaves_before_the_head_ends_abandoned() {
    /// A provider that answers 200 and never sends a byte.
    struct Silent;

    impl Silent {
        pub async fn send(
            &self,
            _req: http::Request<Body>,
        ) -> Result<(http::Response<Body>, Option<Completion>), Fault> {
            let never = http_body_util::StreamBody::new(futures::stream::pending());
            let mut resp = http::Response::new(Body::stream(
                http_body_util::BodyExt::into_data_stream(never),
            ));
            resp.headers_mut().insert(
                "content-type",
                HeaderValue::from_static("text/event-stream"),
            );
            Ok((resp, None))
        }
    }
    impl From<Silent> for SendRequest {
        fn from(value: Silent) -> Self {
            let value = std::sync::Arc::new(value);
            sender(move |call| {
                let value = value.clone();
                async move { value.send(call).await }
            })
        }
    }

    let log = Outcomes::default();
    let gw = gateway(
        [add_provider("openai", OpenAiChat::OPENAI, Silent)],
        vec![Arc::new(log.clone())],
        Some("openai"),
    )
    .unwrap_or_else(|e| panic!("{e}"));

    let gave_up = tokio::time::timeout(Duration::from_millis(10), serve(&gw, chat())).await;
    assert!(gave_up.is_err());
    gw.idle().await;

    assert!(matches!(log.only(), Err(f) if fault_code(&f) == "abandoned"));
}

#[tokio::test]
async fn what_the_provider_sent_after_its_finish_is_kept_when_it_breaks_off() {
    let stream = [
        "data: {\"id\":\"c1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Hello\"}}]}\n\n",
        "data: {\"id\":\"c1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\" world\"},\"finish_reason\":\"stop\"}]}\n\n",
    ];
    let h = relay();
    h.sites.openai.reply(fixture::sse(&stream));

    let (_, _, body) = h.send(chat()).await;

    let body = String::from_utf8(body.unwrap_or_default().to_vec()).unwrap_or_default();
    // What came is kept, the finish too, and the failure ends the stream.
    let said = sent_chunks(&body);
    let text: String = said
        .iter()
        .filter_map(|c| c["choices"][0]["delta"]["content"].as_str())
        .collect();
    assert_eq!(text, "Hello world", "{body}");
    assert!(
        said.iter()
            .any(|c| c["choices"][0]["finish_reason"] == "stop")
    );
    assert!(body.ends_with(TRUNCATED), "{body}");
}

#[tokio::test]
async fn a_gateway_dropped_mid_response_serves_what_it_began() {
    let h = Harness::new(Vec::new(), Some("openai"));
    h.sites.openai.reply(fixture::sse(STREAM));

    let req = chat();
    let (resp, ended) = serve(&h.gw, req).await;
    drop(h);
    let (_, _, body) = read_whole(resp).await;

    let body = String::from_utf8(body.unwrap_or_default().to_vec()).unwrap_or_default();
    assert_eq!(gist(&body).items(), gist(&STREAM.concat()).items());
    assert_eq!(says(ended.await), Ok(()));
}

#[tokio::test]
async fn a_providers_error_after_its_finish_keeps_both_and_ends_last() {
    let error = "data: {\"error\":{\"message\":\"boom\",\"type\":\"server_error\",\"param\":null,\"code\":null}}\n\n";
    let content =
        "data: {\"id\":\"c1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Hi\"}}]}\n\n";
    let finish = "data: {\"id\":\"c1\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n";
    for held in [false, true] {
        // A filter that keeps the finish back and adds a tail at the end.
        let h = match held {
            true => filtered(Footer(0, None)),
            false => relay(),
        };
        h.sites
            .openai
            .reply(fixture::sse(&[content, finish, error]));

        let (_, _, body) = h.send(chat()).await;

        let body = String::from_utf8(body.unwrap_or_default().to_vec()).unwrap_or_default();
        assert!(body.contains("\"content\":\"Hi\""), "held {held}: {body}");
        // A provider's finish is written. One a filter kept back past a failed
        // end is not, as it would claim the answer complete.
        let finished = body.contains("\"finish_reason\":\"stop\"");
        assert_eq!(finished, !held, "{body}");
        assert!(body.ends_with(error), "held {held}: {body}");
        assert_eq!(body.contains("[footer]"), held, "{body}");
        let end = h.log.only().map_err(|f| fault_code(&f));
        assert_eq!(end, Err("server_error".to_owned()));
    }
}

#[tokio::test]
async fn a_response_dropped_unsent_cancels_its_provider_body() {
    /// Sets its flag when dropped.
    struct Dropped(Arc<AtomicBool>);

    impl Drop for Dropped {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    /// A provider that answers 200 and streams forever, keeping a guard.
    struct Endless(Arc<AtomicBool>);

    impl Endless {
        pub async fn send(
            &self,
            _req: http::Request<Body>,
        ) -> Result<(http::Response<Body>, Option<Completion>), Fault> {
            let guard = Dropped(self.0.clone());
            let never = futures::StreamExt::inspect(futures::stream::pending(), move |_| {
                let _ = &guard;
            });
            let body = http_body_util::StreamBody::new(never);
            let mut resp = http::Response::new(Body::stream(
                http_body_util::BodyExt::into_data_stream(body),
            ));
            resp.headers_mut().insert(
                "content-type",
                HeaderValue::from_static("text/event-stream"),
            );
            Ok((resp, None))
        }
    }
    impl From<Endless> for SendRequest {
        fn from(value: Endless) -> Self {
            let value = std::sync::Arc::new(value);
            sender(move |call| {
                let value = value.clone();
                async move { value.send(call).await }
            })
        }
    }

    /// Calls the provider, drops its response and answers itself.
    struct Discard;

    #[async_trait]
    impl Plugin for Discard {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            drop(cx.call("openai").await?);
            Cached.on_client(cx).await
        }
    }

    let dropped = Arc::new(AtomicBool::new(false));
    let log = Outcomes::default();
    let gw = gateway(
        [add_provider(
            "openai",
            OpenAiChat::OPENAI,
            Endless(dropped.clone()),
        )],
        vec![Arc::new(log.clone()), Arc::new(Discard)],
        None,
    )
    .unwrap_or_else(|e| panic!("{e}"));

    let req = chat();
    let (status, _, _) = read_whole(serve(&gw, req).await.0).await;
    gw.idle().await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(says(log.only()), Ok(()));
    assert!(dropped.load(Ordering::SeqCst));
}

#[tokio::test]
async fn a_call_dropped_unread_ends_once_as_abandoned() {
    /// Calls a provider and never reads its answer.
    struct Peek;

    #[async_trait]
    impl Plugin for Peek {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            drop(cx.call("backup").await?);
            Ok(None)
        }
    }

    let bill = Bill::default();
    let h = Harness::new(vec![Arc::new(Peek), Arc::new(bill.clone())], Some("openai"));
    h.sites.backup.reply(fixture::sse(STREAM));
    h.sites.openai.reply(fixture::json(StatusCode::OK, UNARY));

    assert_eq!(h.text(unary_chat()).await.0, StatusCode::OK);

    let ended = bill.ended();
    assert_eq!(ended.len(), 2);
    let (call, dropped) = &ended[0];
    assert_eq!(call, "backup/gpt-x");
    assert!(matches!(&dropped, Err(f) if fault_code(f) == "abandoned"));
    assert_eq!(says(ended[1].1.clone()), Ok(()));
}

#[tokio::test]
async fn a_panic_in_on_provider_ends_the_call_and_refuses_the_request() {
    let bill = Bill::default();
    let panics = OnProvider(|_| panic!("a buggy plugin"));
    let h = Harness::new(
        vec![Arc::new(bill.clone()), Arc::new(panics)],
        Some("openai"),
    );

    assert_eq!(h.text(chat()).await.0, StatusCode::INTERNAL_SERVER_ERROR);
    assert!(h.sites.sent.sent().is_empty());
    assert!(ended_with(&bill, "internal"));
}

#[tokio::test]
async fn a_panic_in_a_provider_filter_ends_the_call_as_internal() {
    let bill = Bill::default();
    let panics = OnProvider(|px| px.filter(|_: &mut Output| panic!("a buggy filter")));
    let h = Harness::new(
        vec![Arc::new(bill.clone()), Arc::new(panics)],
        Some("openai"),
    );
    h.sites.openai.reply(fixture::sse(STREAM));

    assert_eq!(h.text(chat()).await.0, StatusCode::INTERNAL_SERVER_ERROR);
    assert!(ended_with(&bill, "internal"));
    assert!(matches!(h.log.only(), Err(f) if fault_code(&f) == "internal"));
}

#[tokio::test]
async fn a_client_that_leaves_mid_stream_ends_the_call_abandoned() {
    let bill = Bill::default();
    let h = Harness::new(vec![Arc::new(bill.clone())], Some("openai"));
    h.sites.openai.reply(fixture::sse(STREAM));

    let req = chat();
    let (_, mut body) = serve(&h.gw, req).await.0.into_parts();
    let _first = http_body_util::BodyExt::frame(&mut body).await;
    drop(body);
    h.gw.idle().await;

    assert!(ended_with(&bill, "abandoned"));
}

#[tokio::test]
async fn a_call_the_transport_fails_ends_with_its_fault() {
    let bill = Bill::default();
    let h = Harness::new(vec![Arc::new(bill.clone())], Some("openai"));

    assert_eq!(h.text(chat()).await.0, StatusCode::BAD_GATEWAY);
    // This one is no answer.
    assert!(ended_with(&bill, "upstream"));
    assert!(bill.ended()[0].1.as_ref().is_err_and(|f| {
        f.get::<respan_gateway_http::ProviderError>()
            .map(|e| &e.response)
            .is_none()
    }));
}

#[tokio::test]
async fn a_refused_call_ends_for_the_refusing_plugin_and_those_before_it() {
    /// Bills the call, then refuses it.
    struct Deny(Bill);

    #[async_trait]
    impl Plugin for Deny {
        async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
            self.0.on_provider(px).await?;
            Err(Fault::new("no calls")
                .with(ErrorCode("denied".into()))
                .with(Status(StatusCode::FORBIDDEN)))
        }

        async fn on_provider_end(&self, px: &ProviderContext, end: &Result<(), Fault>) {
            self.0.on_provider_end(px, end).await;
        }
    }

    let (before, deny, after) = (Bill::default(), Bill::default(), Bill::default());
    let chain: Vec<Arc<dyn Plugin>> = vec![
        Arc::new(before.clone()),
        Arc::new(Deny(deny.clone())),
        Arc::new(after.clone()),
    ];
    let h = Harness::new(chain, Some("openai"));

    assert_eq!(h.text(unary_chat()).await.0, StatusCode::FORBIDDEN);
    assert!(ended_with(&before, "denied"));
    assert!(ended_with(&deny, "denied"));
    assert!(after.ended().is_empty());
}

#[tokio::test]
async fn extensions_set_in_on_provider_reach_that_calls_end_only() {
    /// A reservation made for one call.
    #[derive(Clone)]
    struct Reservation(&'static str);

    #[derive(Clone, Default)]
    struct Reserve(Arc<Mutex<Vec<Option<&'static str>>>>);

    #[async_trait]
    impl Plugin for Reserve {
        async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
            px.extensions_mut().insert(Reservation("r1"));
            // The call's extensions are its copy of the request's.
            let call = &px.extensions();
            assert!(call.get::<Reservation>().is_some());
            Ok(())
        }

        async fn on_provider_end(&self, px: &ProviderContext, _end: &Result<(), Fault>) {
            let held = px.extensions().get::<Reservation>().map(|r| r.0);
            self.0.lock().map(|mut s| s.push(held)).ok();
        }

        async fn on_client_end(&self, cx: &ClientContext, _end: &Result<(), Fault>) {
            let held = cx.extensions().get::<Reservation>().map(|r| r.0);
            self.0.lock().map(|mut s| s.push(held)).ok();
        }
    }

    let reserve = Reserve::default();
    let h = Harness::new(vec![Arc::new(reserve.clone())], Some("openai"));
    h.sites.openai.reply(fixture::json(StatusCode::OK, UNARY));

    assert_eq!(h.text(unary_chat()).await.0, StatusCode::OK);

    let mut seen = reserve.0.lock().map(|s| s.clone()).unwrap_or_default();
    seen.sort();
    assert_eq!(seen, [None, Some("r1")]);
}

#[tokio::test]
async fn idle_waits_for_a_slow_on_provider_end() {
    #[derive(Clone, Default)]
    struct Slow(Arc<AtomicBool>);

    #[async_trait]
    impl Plugin for Slow {
        async fn on_provider_end(&self, _px: &ProviderContext, _end: &Result<(), Fault>) {
            tokio::time::sleep(Duration::from_millis(50)).await;
            self.0.store(true, Ordering::SeqCst);
        }
    }

    let slow = Slow::default();
    let h = Harness::new(vec![Arc::new(slow.clone())], Some("openai"));
    h.sites.openai.reply(fixture::json(StatusCode::OK, UNARY));

    // `send` returns once `idle` does.
    assert_eq!(h.text(unary_chat()).await.0, StatusCode::OK);
    assert!(slow.0.load(Ordering::SeqCst));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_calls_provider_end_runs_before_the_clients_end() {
    /// Records each end hook, the provider edge's slowly.
    #[derive(Clone, Default)]
    struct Order(Arc<Mutex<Vec<&'static str>>>);

    #[async_trait]
    impl Plugin for Order {
        async fn on_provider_end(&self, _px: &ProviderContext, _end: &Result<(), Fault>) {
            tokio::time::sleep(Duration::from_millis(20)).await;
            self.0.lock().map(|mut o| o.push("provider")).ok();
        }

        async fn on_client_end(&self, _cx: &ClientContext, _end: &Result<(), Fault>) {
            self.0.lock().map(|mut o| o.push("client")).ok();
        }
    }

    /// Calls a provider and drops its answer unread.
    struct Peek;

    #[async_trait]
    impl Plugin for Peek {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            drop(cx.call("backup").await?);
            Ok(None)
        }
    }

    let order = Order::default();
    let h = Harness::new(
        vec![Arc::new(order.clone()), Arc::new(Peek)],
        Some("openai"),
    );
    h.sites.backup.reply(fixture::sse(STREAM));
    h.sites.openai.reply(fixture::json(StatusCode::OK, UNARY));

    assert_eq!(h.text(unary_chat()).await.0, StatusCode::OK);
    let order = order.0.lock().map(|o| o.clone()).unwrap_or_default();
    assert_eq!(order, ["provider", "provider", "client"]);
}

#[tokio::test]
async fn a_response_kept_in_the_extensions_ends_as_the_request_closes() {
    /// Calls a provider and keeps its answer, unread, in the request.
    struct Stash;

    #[async_trait]
    impl Plugin for Stash {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let kept = Kept(Arc::new(Mutex::new(Some(cx.call("backup").await?))));
            cx.extensions_mut().insert(kept);
            Ok(None)
        }
    }

    let ends = Ends::default();
    let h = Harness::new(
        vec![Arc::new(ends.clone()), Arc::new(Stash)],
        Some("openai"),
    );
    h.sites.backup.reply(fixture::sse(STREAM));
    h.sites.openai.reply(fixture::json(StatusCode::OK, UNARY));

    assert_eq!(closes(&h, unary_chat()).await, StatusCode::OK);
    let mut ends = ends.all();
    assert_eq!(ends.pop().as_deref(), Some("client"));
    ends.sort();
    assert_eq!(ends, ["backup abandoned", "openai ok"]);
}

#[tokio::test]
async fn a_response_kept_past_its_request_ends_abandoned_and_reads_so() {
    /// Races two providers and answers with the first, keeping the loser.
    struct Hedge(Kept);

    #[async_trait]
    impl Plugin for Hedge {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let (first, loser) = (cx.call("openai"), cx.call("backup"));
            let (first, loser) = futures::join!(first, loser);
            *self.0.0.lock().map_err(|_| Fault::internal("poisoned"))? = Some(loser?);
            first.map(Some)
        }
    }

    let (ends, kept) = (Ends::default(), Kept::default());
    let h = Harness::new(
        vec![Arc::new(ends.clone()), Arc::new(Hedge(kept.clone()))],
        None,
    );
    h.sites.openai.reply(fixture::json(StatusCode::OK, UNARY));
    h.sites.backup.reply(fixture::sse(STREAM));

    assert_eq!(closes(&h, unary_chat()).await, StatusCode::OK);
    let mut ends = ends.all();
    assert_eq!(ends.pop().as_deref(), Some("client"));
    ends.sort();
    assert_eq!(ends, ["backup abandoned", "openai ok"]);
    let read = kept
        .take()
        .expect("kept")
        .outputs::<Output>()
        .try_collect::<Vec<_>>()
        .await;
    assert_eq!(read.err().map(|f| fault_code(&f)), Some("abandoned".into()));
}

#[tokio::test]
async fn a_client_edge_failure_ends_the_call_whose_response_was_sent() {
    let bill = Bill::default();
    let guard =
        Guard(|e| matches!(e, Output::Delta { delta, .. } if delta.as_str().contains("world")));
    let h = Harness::new(
        vec![Arc::new(bill.clone()), Arc::new(guard)],
        Some("openai"),
    );
    let stream = [
        chunk(
            json!({"id": "c1", "choices": [{"index": 0, "delta": {"content": "Hi"}}],
            "usage": {"prompt_tokens": 5, "completion_tokens": 1}}),
        ),
        chunk(json!({"id": "c1", "choices": [{"index": 0, "delta": {"content": " world"}}]})),
        "data: [DONE]\n\n".to_owned(),
    ];
    let chunks: Vec<&str> = stream.iter().map(String::as_str).collect();
    h.sites.openai.reply(fixture::sse(&chunks));

    let _ = h.send(chat()).await;

    let ended = bill.ended();
    assert_eq!(ended.len(), 1);
    let end = &ended[0].1;
    assert_eq!(
        end.as_ref().err().map(fault_code).as_deref(),
        Some("blocked")
    );
    assert_eq!(input_tokens(&h.usage.all()[0]), 5);
}

#[tokio::test]
async fn a_reader_waiting_on_a_kept_response_wakes_as_its_request_closes() {
    type Reading = tokio::task::JoinHandle<Result<Vec<Output>, Fault>>;

    /// Reads a call whose body never ends, in a task of its own.
    struct Watch(Arc<Mutex<Option<Reading>>>);

    #[async_trait]
    impl Plugin for Watch {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let reading = tokio::spawn(
                cx.call("slow")
                    .await?
                    .outputs::<Output>()
                    .try_collect::<Vec<_>>(),
            );
            *self.0.lock().map_err(|_| Fault::internal("poisoned"))? = Some(reading);
            Ok(None)
        }
    }

    let (dropped, reader) = (Arc::new(AtomicBool::new(false)), Arc::default());
    let sites = Sites::default();
    let gw = gateway(
        [
            provider(&sites.openai, "openai", Some("Bearer sk-test")),
            add_provider("slow", OpenAiChat::OPENAI, Endless(dropped.clone())),
        ],
        vec![Arc::new(Watch(Arc::clone(&reader)))],
        Some("openai"),
    )
    .expect("builds");
    sites.openai.reply(fixture::json(StatusCode::OK, UNARY));

    let (status, _, _) = read_whole(serve(&gw, post(CHAT, UNARY_REQ)).await.0).await;
    let reading = reader.lock().ok().and_then(|mut r| r.take()).expect("read");
    let read = tokio::time::timeout(Duration::from_secs(5), reading).await;
    gw.idle().await;

    assert_eq!(status, StatusCode::OK);
    let read = read.expect("the reader woke").expect("the reader ran");
    assert_eq!(read.err().map(|f| fault_code(&f)), Some("abandoned".into()));
    assert!(dropped.load(Ordering::SeqCst));
}

#[tokio::test]
async fn filters_of_a_call_abandoned_at_close_get_their_end() {
    /// Keeps a call's answer, unread, in the request.
    struct Stash;

    #[async_trait]
    impl Plugin for Stash {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let kept = Kept(Arc::new(Mutex::new(Some(cx.call("backup").await?))));
            cx.extensions_mut().insert(kept);
            Ok(None)
        }
    }

    /// Notes how each call to `backup` ended, as its filter learns it.
    #[derive(Clone, Default)]
    struct Note(Arc<Mutex<Vec<String>>>);

    struct Noted(Arc<Mutex<Vec<String>>>);

    impl Filter<Output> for Noted {
        fn edit(&mut self, event: Output, out: &mut Vec<Output>) -> Result<(), Fault> {
            out.push(event);
            Ok(())
        }

        fn end(&mut self, end: &Result<(), Fault>, _out: &mut Vec<Output>) -> Result<(), Fault> {
            let end = end.as_ref().err().map_or("ok".to_owned(), fault_code);
            self.0.lock().map(|mut n| n.push(end.to_owned())).ok();
            Ok(())
        }
    }

    #[async_trait]
    impl Plugin for Note {
        async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
            match px.provider() {
                "backup" => px.filter(Noted(self.0.clone())),
                _ => Ok(()),
            }
        }
    }

    let note = Note::default();
    let h = Harness::new(
        vec![Arc::new(note.clone()), Arc::new(Stash)],
        Some("openai"),
    );
    h.sites.backup.reply(fixture::sse(STREAM));
    h.sites.openai.reply(fixture::json(StatusCode::OK, UNARY));

    assert_eq!(closes(&h, unary_chat()).await, StatusCode::OK);
    let noted = note.0.lock().map(|n| n.clone()).unwrap_or_default();
    assert_eq!(noted, ["abandoned"]);
}

#[tokio::test]
async fn a_response_whose_last_bytes_were_read_ends_complete() {
    let h = relay();
    h.sites.openai.reply(fixture::json(StatusCode::OK, UNARY));

    let (_, mut body) = serve(&h.gw, unary_chat()).await.0.into_parts();
    let frame = http_body_util::BodyExt::frame(&mut body).await;
    let data = frame.and_then(|f| f.ok()?.into_data().ok());
    let data = data
        .map(|d| String::from_utf8_lossy(&d).into_owned())
        .unwrap_or_default();
    assert_eq!(gist(&data).text(), "hi");
    // The host stops at the last bytes, without polling for the end.
    drop(body);
    h.gw.idle().await;

    assert_eq!(says(h.log.only()), Ok(()));
}

#[tokio::test]
async fn a_panic_before_the_head_ends_the_call_that_answered_internal() {
    let bill = Bill::default();
    let chain: Vec<Arc<dyn Plugin>> = vec![Arc::new(bill.clone()), Arc::new(PanicFilter)];
    let h = Harness::new(chain, Some("openai"));
    h.sites.openai.reply(fixture::sse(STREAM));

    let (status, _, _) = h.send(chat()).await;

    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(code(&h.log.only()), "internal");
    // The call ends with the panic that refused its response.
    let ended = bill.ended();
    assert_eq!(code(&ended[0].1), "internal");
}

#[tokio::test]
async fn a_panic_before_any_bytes_is_refused() {
    /// Filters the provider's response through a filter that panics.
    struct Busy;

    #[async_trait]
    impl Plugin for Busy {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            cx.filter(|_: &mut Output| -> Result<(), Fault> { panic!("a buggy filter") })?;
            Ok(None)
        }
    }

    let h = Harness::new(vec![Arc::new(Busy)], Some("openai"));
    h.sites.openai.reply(fixture::json(StatusCode::OK, UNARY));

    let (status, body) = h.text(unary_chat()).await;

    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert!(body.contains("internal"), "{body}");
    assert_eq!(code(&h.log.only()), "internal");
}

#[tokio::test]
async fn a_panic_after_bytes_went_out_keeps_those_written_before_it() {
    let h = Harness::new(
        vec![Arc::new(Guard(|e| {
            if matches!(e, Output::Delta { delta, .. } if delta.as_str() == "boom") {
                panic!("a buggy guard");
            }
            false
        }))],
        Some("openai"),
    );
    let both = format!("{}{}", text_chunk(" world"), text_chunk("boom"));
    h.sites
        .openai
        .reply(fixture::sse(&[HI, &both, "data: [DONE]\n\n"]));

    let (text, fault) = frames(serve(&h.gw, chat()).await.0).await;
    h.gw.idle().await;

    // What was written before the panic in the same batch is sent, and the
    // panic, an internal fault like a filter's `Err`, is written after it.
    assert!(
        text.contains("\"Hi\"") && text.contains("\" world\""),
        "{text}"
    );
    assert!(text.contains("internal"), "{text}");
    assert!(fault.is_none());
}

#[tokio::test]
async fn a_client_edge_failure_ends_the_call_however_its_bytes_were_split() {
    let world = text_chunk(" world");
    let done = "data: [DONE]\n\n";
    let together = format!("{world}{done}");
    // r7, as N5 made it: a call settles at the end of its body, so the
    // failure that stopped its reader ends it, however the bytes fell.
    let each = [
        (vec![HI, &*world, done], "blocked"),
        (vec![HI, &*together], "blocked"),
    ];
    for (chunks, want) in each {
        let bill = Bill::default();
        let guard =
            Guard(|e| matches!(e, Output::Delta { delta, .. } if delta.as_str().contains("world")));
        let h = Harness::new(
            vec![Arc::new(bill.clone()), Arc::new(guard)],
            Some("openai"),
        );
        h.sites.openai.reply(fixture::sse(&chunks));

        let body = h.body(chat()).await;

        assert!(body.contains("blocked"), "{body}");
        let ended = bill.ended();
        let end = ended[0]
            .1
            .as_ref()
            .err()
            .map_or("ok".to_owned(), fault_code);
        assert_eq!(end, want, "{chunks:?}");
    }
}

#[tokio::test]
async fn a_raw_relay_sends_an_unedited_request_as_it_came() {
    let h = Harness::new(vec![Arc::new(explicit::Relay::RAW)], Some("openai"));
    h.sites.openai.reply(fixture::json(StatusCode::OK, UNARY));

    let (status, _, _) = h.send(unary_chat()).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(&h.sites.sent.sent()[0].body()[..], UNARY_REQ.as_bytes());
}

#[tokio::test]
async fn idle_waits_for_a_response_still_streaming() {
    let h = relay();
    h.sites.openai.reply(fixture::sse(STREAM));

    let (resp, _) = serve(&h.gw, chat()).await;
    let waited = tokio::time::timeout(Duration::from_millis(50), h.gw.idle()).await;
    assert!(waited.is_err(), "idle returned with a response unread");

    let _ = read_whole(resp).await;
    h.gw.idle().await;
    assert_eq!(says(h.log.only()), Ok(()));
}

#[tokio::test]
async fn a_gateway_changed_after_cloning_waits_only_for_its_own_requests() {
    let h = Harness::new(vec![Arc::new(Hang(Outcomes::default()))], Some("openai"));
    let changed = h.gw.clone().plugin(Outcomes::default());

    // A request of the original hangs in its plugin.
    let pending = tokio::spawn({
        let gw = h.gw.clone();
        async move { serve(&gw, unary_chat()).await }
    });
    tokio::time::sleep(Duration::from_millis(10)).await;

    let own = tokio::time::timeout(Duration::from_millis(50), h.gw.idle()).await;
    let other = tokio::time::timeout(Duration::from_millis(50), changed.idle()).await;
    assert!(own.is_err() && other.is_ok());
    pending.abort();
}

#[tokio::test]
async fn a_panicking_call_leaves_its_sibling_abandoned() {
    struct Never;

    impl Never {
        pub async fn send(
            &self,
            _req: http::Request<Body>,
        ) -> Result<(http::Response<Body>, Option<Completion>), Fault> {
            std::future::pending().await
        }
    }
    impl From<Never> for SendRequest {
        fn from(value: Never) -> Self {
            let value = std::sync::Arc::new(value);
            sender(move |call| {
                let value = value.clone();
                async move { value.send(call).await }
            })
        }
    }

    /// Panics sending.
    struct Boom;

    impl Boom {
        pub async fn send(
            &self,
            _req: http::Request<Body>,
        ) -> Result<(http::Response<Body>, Option<Completion>), Fault> {
            panic!("a buggy transport")
        }
    }
    impl From<Boom> for SendRequest {
        fn from(value: Boom) -> Self {
            let value = std::sync::Arc::new(value);
            sender(move |call| {
                let value = value.clone();
                async move { value.send(call).await }
            })
        }
    }

    /// Calls both at once.
    struct Race;

    #[async_trait]
    impl Plugin for Race {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let (never, boom) = (cx.call("never"), cx.call("boom"));
            let _ = futures::join!(never, boom);
            Ok(None)
        }
    }

    let ends = Ends::default();
    let list = [
        add_provider("never", OpenAiChat::OPENAI, Never),
        add_provider("boom", OpenAiChat::OPENAI, Boom),
    ];
    let plugins: Vec<Arc<dyn Plugin>> = vec![Arc::new(ends.clone()), Arc::new(Race)];
    let gw = gateway(list, plugins, None).unwrap_or_else(|e| panic!("gateway: {e}"));

    let (status, _, _) = read_whole(serve(&gw, unary_chat()).await.0).await;
    gw.idle().await;

    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    let mut all = ends.all();
    all.sort();
    assert_eq!(all, ["boom internal", "client", "never abandoned"]);
}

#[tokio::test]
async fn a_client_leaving_mid_stream_ends_the_call_and_its_filters() {
    /// Notes each provider-edge filter end.
    #[derive(Clone, Default)]
    struct Note(Arc<Mutex<Vec<String>>>);

    struct Noted(Arc<Mutex<Vec<String>>>);

    impl Filter<Output> for Noted {
        fn edit(&mut self, event: Output, out: &mut Vec<Output>) -> Result<(), Fault> {
            out.push(event);
            Ok(())
        }

        fn end(&mut self, end: &Result<(), Fault>, _out: &mut Vec<Output>) -> Result<(), Fault> {
            let end = code(end).to_owned();
            self.0.lock().map(|mut n| n.push(end)).ok();
            Ok(())
        }
    }

    #[async_trait]
    impl Plugin for Note {
        async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
            px.filter(Noted(self.0.clone()))
        }
    }

    let (note, ends) = (Note::default(), Ends::default());
    let h = Harness::new(
        vec![Arc::new(note.clone()), Arc::new(ends.clone())],
        Some("openai"),
    );
    let world = text_chunk(" world");
    h.sites
        .openai
        .reply(fixture::sse(&[HI, &world, "data: [DONE]\n\n"]));

    let (_, mut body) = serve(&h.gw, chat()).await.0.into_parts();
    let _ = http_body_util::BodyExt::frame(&mut body).await;
    drop(body);
    h.gw.idle().await;

    let noted = note.0.lock().map(|n| n.clone()).unwrap_or_default();
    assert_eq!(noted, ["abandoned"]);
    assert_eq!(ends.all(), ["openai abandoned", "client"]);
}

#[tokio::test]
async fn a_panic_reading_a_calls_events_ends_the_call_internal() {
    /// Reads a judge's verdict through a provider-edge filter that panics.
    struct Judge;

    #[async_trait]
    impl Plugin for Judge {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let verdict = cx.call("backup").await?;
            verdict.outputs::<Output>().try_collect::<Vec<_>>().await?;
            Ok(None)
        }
    }

    let buggy = OnProvider(|px| {
        px.filter(|_: &mut Output| -> Result<(), Fault> { panic!("a buggy filter") })
    });
    let ends = Ends::default();
    let plugins: Vec<Arc<dyn Plugin>> =
        vec![Arc::new(ends.clone()), Arc::new(buggy), Arc::new(Judge)];
    let h = Harness::new(plugins, Some("openai"));
    h.sites.backup.reply(fixture::json(StatusCode::OK, UNARY));

    let (status, _) = h.text(unary_chat()).await;

    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(ends.all(), ["backup internal", "client"]);
}

#[tokio::test]
async fn a_dropped_response_runs_its_provider_filters_end() {
    /// Records each provider-edge filter's `end`, by provider.
    #[derive(Clone, Default)]
    struct FilterEnds(Arc<Mutex<Vec<String>>>);

    struct NoteEnd(FilterEnds, String);

    impl Filter<Output> for NoteEnd {
        fn edit(&mut self, output: Output, out: &mut Vec<Output>) -> Result<(), Fault> {
            out.push(output);
            Ok(())
        }

        fn end(&mut self, end: &Result<(), Fault>, _out: &mut Vec<Output>) -> Result<(), Fault> {
            let end = format!("{} {}", self.1, code(end));
            self.0.0.lock().map(|mut e| e.push(end)).ok();
            Ok(())
        }
    }

    #[async_trait]
    impl Plugin for FilterEnds {
        async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
            let provider = px.provider().to_owned();
            px.filter(NoteEnd(self.clone(), provider))
        }
    }

    /// Reads `openai` to its first content, then prefers `backup` anyway.
    struct Hedge;

    #[async_trait]
    impl Plugin for Hedge {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let mut first = cx.call("openai").await?;
            first.buffer_until(Output::is_content).await?;
            drop(first);
            Ok(Some(cx.call("backup").await?))
        }
    }

    let ends = FilterEnds::default();
    let h = Harness::new(vec![Arc::new(ends.clone()), Arc::new(Hedge)], None);
    h.sites.openai.reply(fixture::sse(STREAM));
    h.sites.backup.reply(fixture::sse(STREAM));

    let _ = h.body(chat()).await;

    let got = ends.0.lock().map(|e| e.clone()).unwrap_or_default();
    assert_eq!(got, ["openai abandoned", "backup ok"]);
}

#[tokio::test]
async fn a_plugins_own_response_sends_its_head_at_its_first_byte() {
    /// Answers with a stream that has nothing yet, or opens with a first
    /// output, as a stream that waits on the client does.
    struct Waiting(bool);

    #[async_trait]
    impl Plugin for Waiting {
        async fn on_client(&self, _cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let opens = self.0.then(|| text(0, "Hi")).into_iter().flatten().map(Ok);
            let pending = futures::stream::pending::<Result<Output, Fault>>();
            let quiet = futures::StreamExt::chain(futures::stream::iter(opens), pending);
            Ok(Some(Response::stream(quiet)))
        }
    }

    /// Answers with a stream that fails before any output.
    struct Failing;

    #[async_trait]
    impl Plugin for Failing {
        async fn on_client(&self, _cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let fails = [Err::<Output, _>(
                Fault::new("no")
                    .with(ErrorCode("no".into()))
                    .with(Status(StatusCode::FORBIDDEN)),
            )];
            Ok(Some(Response::stream(futures::stream::iter(fails))))
        }
    }

    let h = Harness::new(vec![Arc::new(Waiting(false))], None);
    let handled = tokio::time::timeout(Duration::from_millis(100), serve(&h.gw, chat())).await;
    assert!(handled.is_err(), "the head went out before a first byte");
    let h = Harness::new(vec![Arc::new(Waiting(true))], None);
    let handled = tokio::time::timeout(Duration::from_secs(5), serve(&h.gw, chat())).await;
    let (resp, _) = handled.unwrap_or_else(|_| panic!("the head waited past its first byte"));
    assert_eq!(resp.status(), StatusCode::OK);

    // A failure before its first byte is refused with the fault's status.
    let h = Harness::new(vec![Arc::new(Failing)], None);
    let (status, body) = h.text(chat()).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert!(body.contains(r#""code":"no""#), "{body}");

    /// Answers from a cache, its outputs at hand.
    struct Cached;

    #[async_trait]
    impl Plugin for Cached {
        async fn on_client(&self, _cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            // Its first output, which the guard sees before anything is written.
            let cached = [Ok(Output::text(0, "world"))];
            Ok(Some(Response::stream(futures::stream::iter(cached))))
        }
    }

    // So a guard refuses a cached answer as it refuses a provider's.
    let guard =
        Guard(|e| matches!(e, Output::Delta { delta, .. } if delta.as_str().contains("world")));
    let h = Harness::new(vec![Arc::new(guard), Arc::new(Cached)], None);
    let (status, body) = h.text(chat()).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert!(body.contains("blocked"), "{body}");
}

#[tokio::test]
async fn a_calls_response_read_to_its_end_then_dropped_ends_ok() {
    /// Reads a call's whole response with `until_content`, then drops it.
    struct ContentThenDrop;

    #[async_trait]
    impl Plugin for ContentThenDrop {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let mut response = cx.call("openai").await?;
            response.buffer_until(Output::is_content).await?;
            drop(response);
            Err(Fault::not_found("done"))
        }
    }

    let fake = FakeOpenAi::start().await;
    let h = Harness::real(&fake, vec![Arc::new(ContentThenDrop)], None);
    fake.answer(Answer::raw(UNARY));
    let _ = h.send(valid_unary_chat()).await;

    let all = h.calls.all();
    assert_eq!(all.len(), 1);
    assert_eq!(says(all[0].clone()), Ok(()));
    assert_eq!(h.usage.all()[0].input_tokens, 4);
}

#[tokio::test]
async fn a_refusal_before_the_head_ends_a_plugins_response_filters() {
    /// Notes each end it gets.
    #[derive(Clone, Default)]
    struct Ends(Arc<Mutex<Vec<String>>>);

    impl Filter<Output> for Ends {
        fn edit(&mut self, reply: Output, out: &mut Vec<Output>) -> Result<(), Fault> {
            out.push(reply);
            Ok(())
        }

        fn end(&mut self, end: &Result<(), Fault>, _out: &mut Vec<Output>) -> Result<(), Fault> {
            let end = end
                .as_ref()
                .err()
                .map_or("ok".to_owned(), fault_code)
                .to_owned();
            self.0.lock().map(|mut e| e.push(end)).ok();
            Ok(())
        }
    }

    /// Filters its own answer.
    struct Answer(Ends);

    #[async_trait]
    impl Plugin for Answer {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            cx.filter(self.0.clone())?;
            let answer = futures::stream::iter(text(0, "hi").into_iter().map(Ok));
            Ok(Some(Response::stream(answer)))
        }
    }

    // The panicking filter, added first, runs after `Ends`.
    let ends = Ends::default();
    let h = Harness::new(
        vec![Arc::new(PanicFilter), Arc::new(Answer(ends.clone()))],
        None,
    );
    let (status, _, _) = h.send(chat()).await;

    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    let ends = ends.0.lock().map(|e| e.clone()).unwrap_or_default();
    assert_eq!(ends, ["internal"]);
}

/// The same provider bytes and the same client-edge guard must give the
/// call the same end, however the transport split the bytes.
#[tokio::test]
async fn r6_a_calls_outcome_does_not_depend_on_how_its_bytes_were_split() {
    let guard = || -> Vec<Arc<dyn Plugin>> {
        vec![Arc::new(Guard(
            |e| matches!(e, Output::Delta { delta, .. } if delta.as_str() == " world"),
        ))]
    };
    let end = |h: &Harness| {
        let all = h.calls.all();
        let o = &all[0];
        (
            o.as_ref().err().map(fault_code),
            h.usage.all()[0].output_tokens,
        )
    };
    let split = Harness::new(guard(), Some("openai"));
    split.sites.openai.reply(fixture::sse(STREAM));
    let _ = split.send(chat()).await;

    let joined = Harness::new(guard(), Some("openai"));
    joined.sites.openai.reply(fixture::sse(&[&STREAM.concat()]));
    let _ = joined.send(chat()).await;

    // N5: a call settles at the end of its body, so both end as the guard
    // refused; each meter saw what was decoded, joined the usage that came
    // with the bytes refused, as provider-edge filters run first.
    assert_eq!(end(&split), (Some("blocked".to_owned()), 0));
    assert_eq!(end(&joined), (Some("blocked".to_owned()), 2));
}

#[tokio::test]
async fn a_response_read_to_its_end_and_kept_past_its_request_ends_abandoned() {
    /// Reads `backup` to its end and keeps it, unsent, in the request.
    struct Stash;

    #[async_trait]
    impl Plugin for Stash {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let mut read = cx.call("backup").await?;
            read.until_end().await?;
            cx.extensions_mut()
                .insert(Kept(Arc::new(Mutex::new(Some(read)))));
            Ok(None)
        }
    }

    let ends = Ends::default();
    let h = Harness::new(
        vec![Arc::new(ends.clone()), Arc::new(Stash)],
        Some("openai"),
    );
    h.sites.backup.reply(fixture::sse(STREAM));
    h.sites.openai.reply(fixture::json(StatusCode::OK, UNARY));

    assert_eq!(closes(&h, unary_chat()).await, StatusCode::OK);
    let mut ends = ends.all();
    assert_eq!(ends.pop().as_deref(), Some("client"));
    ends.sort();
    // Never read, so never settled: the request's close abandons it.
    assert_eq!(ends, ["backup abandoned", "openai ok"]);
}
