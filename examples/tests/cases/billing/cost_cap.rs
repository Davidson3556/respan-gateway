use super::*;
use respan_gateway_core::Provider;
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
    // No usage came, so input is its bound: "hi" and 8 for framing,
    // 10 bytes. Absent native fields contribute no bytes.
    assert_eq!(ledger.spent("team"), PRICE.cost(10, 105));
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
    fake.answer(data_events(&data));
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

/// A request for several answers (`n > 1`) is refused before anything is
/// held or sent: the kind has one answer.
#[tokio::test]
async fn a_request_for_several_answers_is_refused_and_costs_nothing() -> Result<(), Fault> {
    let (fake, ledger) = (FakeOpenAi::start().await, team());
    let gw = Gateway::new()
        .provider(Provider::new(
            "openai",
            OpenAiChat,
            provider_support::openai(&fake.base_url())?.connect(),
        ))?
        .client::<Input, _, _>(OpenAiChat)?
        .plugin(Cap)
        .plugin(Accounting::new(ledger.clone(), quote))
        .target("openai")?;
    let body =
        r#"{"model":"m","n":3,"max_tokens":5000,"messages":[{"role":"user","content":"hi"}]}"#;
    let (status, _, _) = read_whole(serve(&gw, post("/v1/chat/completions", body)).await.0).await;
    gw.idle().await;
    assert_eq!(status.as_u16(), 400);
    assert_eq!(ledger.spent("team"), 0);
    assert!(fake.requests().is_empty());
    Ok(())
}

#[tokio::test]
async fn a_stream_cut_before_any_text_is_charged_its_reserve() -> Result<(), Fault> {
    let (fake, ledger) = (FakeOpenAi::start().await, team());
    let role = r#"{"id":"c1","model":"m","choices":[{"index":0,"delta":{"role":"assistant","content":""}}]}"#;
    fake.answer(data_events(&[role]).cut_after(1));
    let gw = gateway(
        provider_support::openai(&fake.base_url())?.connect(),
        ledger.clone(),
    )?;
    ask(&gw, true).await;
    assert_eq!(ledger.spent("team"), PRICE.cost(10, CAP));
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
