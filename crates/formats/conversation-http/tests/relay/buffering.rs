//! Relay buffering behavior.

use super::*;
use respan_gateway_core::Fault;

#[tokio::test]
async fn a_whole_body_call_ends_only_once_its_until_content_response_is_read() {
    /// Returns `openai`'s answer read to its content; its provider-edge filter notes its end.
    #[derive(Clone, Default)]
    struct Returns(Arc<Mutex<Vec<&'static str>>>);

    impl Returns {
        fn note(&self, what: &'static str) {
            self.0.lock().map(|mut v| v.push(what)).ok();
        }
    }

    impl Filter<Output> for Returns {
        fn edit(&mut self, reply: Output, out: &mut Vec<Output>) -> Result<(), Fault> {
            out.push(reply);
            Ok(())
        }

        fn end(&mut self, _end: &Result<(), Fault>, _out: &mut Vec<Output>) -> Result<(), Fault> {
            self.note("filter ended");
            Ok(())
        }
    }

    #[async_trait]
    impl Plugin for Returns {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let mut resp = cx.call("openai").await?;
            resp.buffer_until(Output::is_content).await?;
            self.note("returned");
            Ok(Some(resp))
        }

        async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
            px.filter(self.clone())
        }

        async fn on_provider_end(&self, _: &ProviderContext, _: &Result<(), Fault>) {
            self.note("call ended");
        }
    }

    let (ends, returns) = (Ends::default(), Returns::default());
    let h = Harness::new(
        vec![Arc::new(ends.clone()), Arc::new(returns.clone())],
        None,
    );
    // A whole body: its content and its end come in one batch.
    h.sites.openai.reply(fixture::json(StatusCode::OK, UNARY));

    let (status, body) = h.text(unary_chat()).await;

    assert_eq!(status, StatusCode::OK);
    let said = gist(&body);
    assert_eq!(said.text(), "hi");
    assert_eq!(said.usage.input_tokens, 4);
    let order = returns.0.lock().map(|v| v.clone()).unwrap_or_default();
    assert_eq!(order, ["filter ended", "returned", "call ended"]);
    assert_eq!(ends.all(), ["openai ok", "client"]);
}

#[tokio::test]
async fn buffer_until_judges_provider_outputs_after_filters() {
    /// Takes every output of `openai`'s response.
    struct Mute;

    impl Filter<Output> for Mute {
        fn edit(&mut self, _output: Output, _out: &mut Vec<Output>) -> Result<(), Fault> {
            Ok(())
        }
    }

    let mute = OnProvider(|px| match px.provider() {
        "openai" => px.filter(Mute),
        _ => Ok(()),
    });
    let ends = Ends::default();
    let chain: Vec<Arc<dyn Plugin>> = vec![
        Arc::new(ends.clone()),
        Arc::new(mute),
        Arc::new(FirstContent),
    ];
    let h = Harness::new(chain, None);
    let overloaded = "data: {\"error\":{\"message\":\"Overloaded\",\"code\":\"overloaded\"}}\n\n";
    h.sites.openai.reply(fixture::sse(&[HI, overloaded]));
    h.sites.backup.reply(fixture::sse(STREAM));

    let (status, _) = h.text(chat()).await;

    // The filter removed the qualifying events, so the policy falls back.
    assert_eq!(status, StatusCode::OK);
    assert_eq!(ends.all(), ["openai overloaded", "backup ok", "client"]);
    assert_eq!(h.sites.sent.sent().len(), 2);
}

#[tokio::test]
async fn buffer_until_rechecks_filtered_values_with_a_new_predicate() {
    /// Replaces every output with a non-content event.
    struct Mute;

    impl Filter<Output> for Mute {
        fn edit(&mut self, _output: Output, out: &mut Vec<Output>) -> Result<(), Fault> {
            out.push(Output::usage(Usage::default()));
            Ok(())
        }
    }

    /// Readies `openai`'s response twice, falling back if the second fails.
    struct UntilContentTwice;

    #[async_trait]
    impl Plugin for UntilContentTwice {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let mut first = cx.call("openai").await?;
            first.buffer_until(|_: &Output| true).await?;
            match first.buffer_until(Output::is_content).await {
                Ok(()) => Ok(Some(first)),
                Err(_) => {
                    drop(first);
                    Ok(Some(cx.call("backup").await?))
                }
            }
        }
    }

    let mute = OnProvider(|px| match px.provider() {
        "openai" => px.filter(Mute),
        _ => Ok(()),
    });
    let ends = Ends::default();
    let chain: Vec<Arc<dyn Plugin>> = vec![
        Arc::new(ends.clone()),
        Arc::new(mute),
        Arc::new(UntilContentTwice),
    ];
    let h = Harness::new(chain, None);
    let overloaded = "data: {\"error\":{\"message\":\"Overloaded\",\"code\":\"overloaded\"}}\n\n";
    h.sites.openai.reply(fixture::sse(&[HI, overloaded]));
    h.sites.backup.reply(fixture::sse(STREAM));

    let (status, _) = h.text(chat()).await;

    // The second predicate rejects the buffered values and finds the failure.
    assert_eq!(status, StatusCode::OK);
    assert_eq!(ends.all(), ["openai overloaded", "backup ok", "client"]);
    assert_eq!(h.sites.sent.sent().len(), 2);
}

#[tokio::test]
async fn a_call_is_a_copy_sent_until_the_request_ends() {
    /// Sends a clone of a sent call, then keeps a call for after the end.
    #[derive(Clone, Default)]
    struct Keep(Arc<Mutex<Option<respan_gateway_core::Call>>>);

    #[async_trait]
    impl Plugin for Keep {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let call = cx.call("openai");
            let copy = call.clone();
            let _ = call.await?;
            self.0.lock().map(|mut k| *k = Some(copy.clone())).ok();
            Ok(Some(copy.await?))
        }
    }

    let keep = Keep::default();
    let h = Harness::new(vec![Arc::new(keep.clone())], None);
    for _ in 0..2 {
        h.sites.openai.reply(fixture::json(StatusCode::OK, UNARY));
    }

    assert_eq!(h.text(unary_chat()).await.0, StatusCode::OK);
    assert_eq!(h.sites.sent.sent().len(), 2);

    let late = keep.0.lock().ok().and_then(|mut k| k.take());
    let late = late.unwrap_or_else(|| panic!("no call kept"));
    let refused = late.await.err().map(|f| fault_code(&f));
    assert_eq!(refused.as_deref(), Some("internal"));
    assert_eq!(h.sites.sent.sent().len(), 2);
}

// A provider's 429 reaches the client as it came, status, `Retry-After` and
// body, whether sent directly or through `until_content`.
#[tokio::test]
async fn r11_1_until_content_on_an_error_status_changes_what_the_client_gets() {
    let sent = Harness::new(vec![Arc::new(Answers(false))], None);
    sent.sites.openai.reply(limited());
    let (s1, h1, b1) = sent.send(unary_chat()).await;

    let readied = Harness::new(vec![Arc::new(Answers(true))], None);
    readied.sites.openai.reply(limited());
    let (s2, h2, b2) = readied.send(unary_chat()).await;

    assert_eq!(s1, s2);
    assert_eq!(h1.get("retry-after"), h2.get("retry-after"));
    assert_eq!(b1.ok(), b2.ok());
}

// The caller chooses whether a synthetic quiet event is enough to commit.
#[tokio::test]
async fn synthetic_outputs_use_the_callers_predicate() {
    struct FallsBack;

    #[async_trait]
    impl Plugin for FallsBack {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let mut one = Usage::default();
            one.input_tokens += 1;
            let quiet = Output::usage(one);
            // A policy accepting any event commits to the quiet reply.
            let made = [Ok(quiet.clone()), Err(Fault::upstream("gone"))];
            let mut unfiltered = Response::stream(futures::stream::iter(made));
            assert!(unfiltered.buffer_until(|_: &Output| true).await.is_ok());
            drop(unfiltered);
            let made = [Ok(quiet), Err(Fault::upstream("gone"))];
            let content = |r: &Output| std::future::ready(r.is_content());
            let made = futures::stream::iter(made).try_filter(content);
            let mut made = Response::stream(made);
            match made.buffer_until(Output::is_content).await {
                Ok(()) => Ok(Some(made)),
                Err(_) => {
                    drop(made);
                    Ok(Some(cx.call("openai").await?))
                }
            }
        }
    }

    let h = Harness::new(vec![Arc::new(FallsBack)], None);
    h.sites.openai.reply(fixture::json(StatusCode::OK, UNARY));

    let (status, body) = h.text(unary_chat()).await;

    assert_eq!(status, StatusCode::OK);
    assert!(body.contains(r#""content":"hi""#), "{body}");
    assert_eq!(h.calls.all().len(), 1);
}

// `outputs` yields every value, keep-alives included, so a bridge
// or merge can show usage; readers filter with the kind's own check.
#[tokio::test]
async fn outputs_yields_values_that_are_no_content_such_as_a_keep_alive() {
    /// Reads the call, noting each output's usage and whether it is
    /// content, and answers with the content.
    #[derive(Clone, Default)]
    struct Reads(Arc<Mutex<Vec<Noted>>>);

    /// Whether an output is content, and its input tokens.
    type Noted = (bool, Option<u64>);

    #[async_trait]
    impl Plugin for Reads {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let read: Vec<Output> = cx.call("openai").await?.outputs().try_collect().await?;
            let noted = read.iter().map(|r| match r {
                Output::Usage(u) => (false, Some(u.input_tokens)),
                r => (r.is_content(), None),
            });
            self.0.lock().map(|mut n| n.extend(noted)).ok();
            let said = read.into_iter().filter(Output::is_content).map(Ok);
            Ok(Some(Response::stream(futures::stream::iter(said))))
        }
    }

    let reads = Reads::default();
    let h = Harness::new(vec![Arc::new(reads.clone())], None);
    let mut stream = STREAM.to_vec();
    // A line that is no chunk, as a server's own keep-alive may be.
    stream.insert(0, "data: keep-alive\n\n");
    h.sites.openai.reply(fixture::sse(&stream));

    assert_eq!(h.send(chat()).await.0, StatusCode::OK);
    let noted = reads.0.lock().map(|n| n.clone()).unwrap_or_default();
    assert_eq!(noted.first(), Some(&(false, None)), "{noted:?}");
    // Usage is shown, though it is no content.
    assert!(noted.contains(&(false, Some(5))), "{noted:?}");
}

// Filters see every value, yet `buffer_until(Output::is_content)` matches neither
// a keep-alive nor usage alone: a stream that sends both, then fails, reaches
// no content, and fallback moves on. The meter still saw the failed call's usage.
#[tokio::test]
async fn until_content_commits_on_neither_a_keep_alive_nor_usage_alone() {
    struct ContentOrBackup;

    #[async_trait]
    impl Plugin for ContentOrBackup {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let mut resp = cx.call("openai").await?;
            match resp.buffer_until(Output::is_content).await {
                Ok(()) => Ok(Some(resp)),
                Err(_) => {
                    drop(resp);
                    Ok(Some(cx.call("backup").await?))
                }
            }
        }
    }

    let h = Harness::new(vec![Arc::new(ContentOrBackup)], None);
    let quiet = [
        ": keep-alive\n\n",
        "data: {\"id\":\"c1\",\"choices\":[],\"usage\":{\"prompt_tokens\":7}}\n\n",
        "data: {\"error\":{\"message\":\"overloaded\"}}\n\n",
    ];
    h.sites.openai.reply(fixture::sse(&quiet));
    h.sites.backup.reply(fixture::sse(STREAM));

    assert_eq!(
        gist(&h.body(chat()).await).items(),
        gist(&STREAM.concat()).items()
    );
    let calls = metered(&h);
    let calls: Vec<_> = calls.iter().map(|(c, n)| (c.as_str(), *n)).collect();
    assert_eq!(calls, [("backup/gpt-x", 5), ("openai/gpt-x", 7)]);
}

#[tokio::test]
async fn until_end_replays_a_stream_and_a_whole_body_alike() {
    let stream = relayed_and_read_to_its_end(|| fixture::sse(STREAM), chat, None).await;
    let whole =
        relayed_and_read_to_its_end(|| fixture::json(StatusCode::OK, UNARY), unary_chat, None)
            .await;

    for [relayed, read] in [stream.clone(), whole.clone()] {
        assert_eq!(read, relayed);
        assert_eq!(read.0, StatusCode::OK);
        assert_eq!(read.3, ["openai ok", "client"]);
    }
    assert_eq!(gist(&stream[1].2).items(), gist(&STREAM.concat()).items());
    assert_eq!(gist(&whole[1].2).items(), gist(UNARY).items());
}

#[tokio::test]
async fn until_end_holds_a_failure_and_it_is_delivered_as_it_would_have_been() {
    // Mid-stream, after content: held and sent after what came before it.
    let broken = || {
        let mut broken = fixture::sse(STREAM);
        broken.body_mut().truncate(2);
        broken
            .body_mut()
            .push(Err(Fault::upstream("connection reset")));
        broken
    };
    let [relayed, read] = relayed_and_read_to_its_end(broken, chat, Some("upstream")).await;
    assert_eq!(read, relayed);
    let text: String = sent_chunks(&read.2)
        .iter()
        .filter_map(|c| {
            c["choices"][0]["delta"]["content"]
                .as_str()
                .map(str::to_owned)
        })
        .collect();
    assert_eq!(text, "Hello world", "{}", read.2);
    assert!(
        read.2.ends_with("\"code\":\"upstream\"}}\n\n"),
        "{}",
        read.2
    );
    assert_eq!(read.3, ["openai upstream", "client"]);

    // Before any content: `Err`, refused as a relay refuses it.
    let [relayed, read] =
        relayed_and_read_to_its_end(|| fixture::sse(&STREAM[..1]), chat, Some("truncated")).await;
    assert_eq!((read.0, &read.2), (relayed.0, &relayed.2));
    assert_eq!(read.0, StatusCode::BAD_GATEWAY);
    assert_eq!(read.3, ["openai truncated", "client"]);
}

#[tokio::test]
async fn until_end_says_how_a_response_ended_and_changes_nothing() {
    /// Records the end `until_end` returns, and answers with the response.
    struct Ending(Arc<Mutex<Vec<String>>>);

    #[async_trait]
    impl Plugin for Ending {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let mut resp = cx.call("openai").await?;
            let end = resp.until_end().await;
            let seen = end
                .as_ref()
                .err()
                .map_or("ok".to_owned(), fault_code)
                .to_owned();
            self.0.lock().map(|mut e| e.push(seen)).ok();
            Ok(Some(resp))
        }
    }

    let broken = || {
        let mut broken = fixture::sse(STREAM);
        broken.body_mut().truncate(2);
        broken.body_mut().push(Err(Fault::upstream("reset")));
        broken
    };
    let clean = || fixture::sse(STREAM);
    let cases: [(&dyn Fn() -> fixture::Reply, &str); 2] = [(&clean, "ok"), (&broken, "upstream")];
    for (reply, seen) in cases {
        let text = |(status, headers, body): (StatusCode, HeaderMap, Result<_, Fault>)| {
            let body: bytes::Bytes = body.unwrap_or_default();
            (status, headers, steady(&String::from_utf8_lossy(&body)))
        };
        let relayed = Harness::new(Vec::new(), Some("openai"));
        relayed.sites.openai.reply(reply());
        let relayed = text(relayed.send(chat()).await);

        let (seen_ends, ends) = (Arc::new(Mutex::new(Vec::new())), Ends::default());
        let plugins: Vec<Arc<dyn Plugin>> =
            vec![Arc::new(ends.clone()), Arc::new(Ending(seen_ends.clone()))];
        let h = Harness::new(plugins, None);
        h.sites.openai.reply(reply());
        let read = text(h.send(chat()).await);

        let seen_ends = seen_ends.lock().map(|e| e.clone()).unwrap_or_default();
        assert_eq!(seen_ends, [seen]);
        // Asking settled nothing: the call ends as its end is sent, as it came.
        assert_eq!(read, relayed);
        assert_eq!(ends.all(), [format!("openai {seen}"), "client".into()]);
    }
}

#[tokio::test]
async fn a_response_dropped_after_until_end_ends_its_call_as_it_ended() {
    /// Reads `openai` to its end, drops it, and answers with `backup`.
    struct ReadAndDrop(bool);

    #[async_trait]
    impl Plugin for ReadAndDrop {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let mut response = cx.call("openai").await?;
            let end = response.until_end().await;
            assert_eq!(
                end.as_ref().err().map(fault_code).as_deref(),
                self.0.then_some("upstream")
            );
            drop(response);
            Ok(Some(cx.call("backup").await?))
        }
    }

    let broken = || {
        let mut broken = fixture::sse(STREAM);
        broken.body_mut().truncate(2);
        broken.body_mut().push(Err(Fault::upstream("reset")));
        broken
    };
    let cases: [(fixture::Reply, &str); 3] = [
        (fixture::sse(STREAM), "openai ok"),
        (fixture::json(StatusCode::OK, UNARY), "openai ok"),
        (broken(), "openai upstream"),
    ];
    for (reply, openai) in cases {
        let ends = Ends::default();
        let h = Harness::new(
            vec![
                Arc::new(ends.clone()),
                Arc::new(ReadAndDrop(openai == "openai upstream")),
            ],
            None,
        );
        h.sites.openai.reply(reply);
        h.sites.backup.reply(fixture::sse(STREAM));

        let body = h.body(chat()).await;
        assert_eq!(gist(&body).items(), gist(&STREAM.concat()).items());
        // Its end was held: dropped, the call ends so, before `backup` starts.
        assert_eq!(ends.all(), [openai, "backup ok", "client"]);
    }
}
