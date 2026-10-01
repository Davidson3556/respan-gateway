use super::*;
use respan_gateway_http::http::StatusCode;

#[tokio::test]
async fn every_call_asks_for_at_most_cap_tokens() -> Result<(), Fault> {
    let fake = FakeOpenAi::start().await;
    fake.answer(Answer::text("hi"));
    let gw = gateway(
        provider_support::openai(&fake.base_url())?.connect(),
        team(),
    )?;
    assert!(ask(&gw, false).await.contains("hi"));
    let sent: serde_json::Value = serde_json::from_slice(&fake.requests()[0].raw)
        .map_err(|e| Fault::internal("json").with_source(e))?;
    assert_eq!(sent["max_tokens"], CAP);
    assert_eq!(fake.requests().len(), 1);
    Ok(())
}

#[tokio::test]
async fn a_runaway_stream_is_cut_and_charged_its_estimate() -> Result<(), Fault> {
    let (fake, ledger) = (FakeOpenAi::start().await, team());
    fake.answer(runaway());
    let gw = gateway(
        provider_support::openai(&fake.base_url())?.connect(),
        ledger.clone(),
    )?;
    let body = ask(&gw, true).await;
    assert!(!body.contains("[DONE]"), "cut: {body}");
    // Cut on the delta that passed 100 tokens: 21 × 20 characters = 105.
    // No usage came, so input is its bound: "hi", 8 for framing and two
    // empty `{}`, 14 bytes.
    assert_eq!(ledger.spent("team"), PRICE.cost(14, 105));
    assert_eq!(fake.requests().len(), 1);
    Ok(())
}

#[tokio::test]
async fn a_call_that_ends_settles_from_its_reported_usage() -> Result<(), Fault> {
    let (fake, ledger) = (FakeOpenAi::start().await, team());
    let answer = r#"{"id":"c1","object":"chat.completion","model":"m","choices":[{"index":0,"finish_reason":"stop","message":{"role":"assistant","content":"hi"}}],"usage":{"prompt_tokens":3,"completion_tokens":7}}"#;
    fake.answer(Answer::raw(answer));
    let gw = gateway(
        provider_support::openai(&fake.base_url())?.connect(),
        ledger.clone(),
    )?;
    ask(&gw, false).await;
    assert_eq!(ledger.spent("team"), PRICE.cost(3, 7));
    assert_eq!(fake.requests().len(), 1);
    Ok(())
}

#[tokio::test]
async fn a_runaway_tool_call_is_cut_and_charged_its_estimate() -> Result<(), Fault> {
    let (fake, ledger) = (FakeOpenAi::start().await, team());
    // 300 deltas of 20 characters of arguments, about 1,500 tokens.
    let first = r#"{"id":"c1","model":"m","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"t","type":"function","function":{"name":"f","arguments":""}}]}}]}"#;
    let delta = r#"{"id":"c1","model":"m","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"twenty characters.. "}}]}}]}"#;
    let mut data = vec![first];
    data.extend(vec![delta; 300]);
    data.push("[DONE]");
    fake.answer(sse(&data));
    let gw = gateway(
        provider_support::openai(&fake.base_url())?.connect(),
        ledger.clone(),
    )?;
    let body = ask(&gw, true).await;
    assert!(!body.contains("[DONE]"), "cut: {body}");
    // No usage came: charged the input bound and at least the cap.
    assert!(
        ledger.spent("team") > PRICE.cost(10, CAP),
        "{}",
        ledger.spent("team")
    );
    assert_eq!(fake.requests().len(), 1);
    Ok(())
}

#[tokio::test]
async fn non_text_input_is_refused_before_it_is_sent() -> Result<(), Fault> {
    let (fake, ledger) = (FakeOpenAi::start().await, team());
    let gw = gateway(
        provider_support::openai(&fake.base_url())?.connect(),
        ledger.clone(),
    )?;
    let image = r#"{"model":"m","messages":[{"role":"user","content":[{"type":"image_url","image_url":{"url":"https://x.test/cat.png"}}]}]}"#;
    let (status, _, _) = read_whole(serve(&gw, post("/v1/chat/completions", image)).await.0).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(fake.requests().is_empty());
    assert_eq!(ledger.spent("team"), 0);
    Ok(())
}

/// `n` answers of up to `CAP` each: the hold covers all of them, so 30
/// parallel requests for three answers never overspend the team.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn n_answers_never_overspend_the_team() -> Result<(), Fault> {
    let (fake, ledger) = (FakeOpenAi::start().await, team());
    let answer = r#"{"id":"c1","object":"chat.completion","model":"m","choices":[{"index":0,"finish_reason":"length","message":{"role":"assistant","content":"a"}},{"index":1,"finish_reason":"length","message":{"role":"assistant","content":"b"}},{"index":2,"finish_reason":"length","message":{"role":"assistant","content":"c"}}],"usage":{"prompt_tokens":3,"completion_tokens":300}}"#;
    for _ in 0..30 {
        fake.answer(Answer::raw(answer));
    }
    let gw = gateway(
        provider_support::openai(&fake.base_url())?.connect(),
        ledger.clone(),
    )?;
    let body =
        r#"{"model":"m","n":3,"max_tokens":5000,"messages":[{"role":"user","content":"hi"}]}"#;
    let all = (0..30).map(|_| {
        let gw = gw.clone();
        tokio::spawn(async move {
            let (status, _, _) =
                read_whole(serve(&gw, post("/v1/chat/completions", body)).await.0).await;
            gw.idle().await;
            status
        })
    });
    let statuses = respan_gateway_sdk::futures::future::join_all(all).await;
    let served = statuses.iter().flatten().filter(|s| s.is_success()).count();
    let spent = ledger.spent("team");
    assert!(spent <= 10_000, "overspent: {spent} of 10,000");
    assert_eq!(spent, served as i64 * PRICE.cost(3, 300));
    assert_eq!(fake.requests().len(), served);
    Ok(())
}

/// A stream cut before its usage and before any text (a reasoning model
/// thinks first, or the upstream resets): the provider may have billed the
/// whole cap, so the team is charged the reserve.
#[tokio::test]
async fn a_stream_cut_before_any_text_is_charged_its_reserve() -> Result<(), Fault> {
    let (fake, ledger) = (FakeOpenAi::start().await, team());
    let role = r#"{"id":"c1","model":"m","choices":[{"index":0,"delta":{"role":"assistant","content":""}}]}"#;
    fake.answer(sse(&[role]).cut_after(1));
    let gw = gateway(
        provider_support::openai(&fake.base_url())?.connect(),
        ledger.clone(),
    )?;
    ask(&gw, true).await;
    assert_eq!(ledger.spent("team"), PRICE.cost(14, CAP));
    assert_eq!(fake.requests().len(), 1);
    Ok(())
}

/// The kind reads the larger of two spellings, so the cap is lower and is
/// written under both: a provider reading either is capped.
#[tokio::test]
async fn two_spellings_of_max_tokens_are_both_capped() -> Result<(), Fault> {
    let (fake, ledger) = (FakeOpenAi::start().await, team());
    let gw = gateway(
        provider_support::openai(&fake.base_url())?.connect(),
        ledger.clone(),
    )?;
    let body = r#"{"model":"m","max_tokens":5000,"max_completion_tokens":100,"messages":[{"role":"user","content":"hi"}]}"#;
    let answer = r#"{"id":"c1","object":"chat.completion","model":"m","choices":[{"index":0,"finish_reason":"stop","message":{"role":"assistant","content":"hi"}}],"usage":{"prompt_tokens":3,"completion_tokens":7}}"#;
    fake.answer(Answer::raw(answer));
    let (status, _, _) = read_whole(serve(&gw, post("/v1/chat/completions", body)).await.0).await;
    assert_eq!(status, StatusCode::OK);
    let sent: serde_json::Value = serde_json::from_slice(&fake.requests()[0].raw)
        .map_err(|e| Fault::internal("json").with_source(e))?;
    assert_eq!(
        (
            sent["max_tokens"].as_u64(),
            sent["max_completion_tokens"].as_u64()
        ),
        (Some(CAP), Some(CAP))
    );
    assert_eq!(fake.requests().len(), 1);
    Ok(())
}
