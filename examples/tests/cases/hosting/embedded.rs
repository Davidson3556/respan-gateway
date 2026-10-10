use super::*;

#[tokio::test]
async fn handle_returns_the_response_and_its_outcome_with_no_server() -> Result<(), Fault> {
    let openai = FakeOpenAi::start().await;
    openai.answer(Answer::raw(ANSWER));
    let tally = Meter::default();
    let gw = gateway(scripted_provider(&openai)?.connect(), tally.clone())?;
    let (status, body, outcome) = ask(&gw, "Say hi").await?;
    // The answer is re-encoded: the provider's answer, not its bytes (the
    // encoder adds fields of its own, such as `created`).
    let got: serde_json::Value =
        serde_json::from_slice(&body).map_err(|e| Fault::internal(e.to_string()))?;
    let sent: serde_json::Value =
        serde_json::from_str(ANSWER).map_err(|e| Fault::internal(e.to_string()))?;
    for pointer in [
        "/id",
        "/model",
        "/choices/0/message",
        "/choices/0/finish_reason",
        "/usage/prompt_tokens",
        "/usage/completion_tokens",
    ] {
        assert_eq!(got.pointer(pointer), sent.pointer(pointer), "{pointer}");
    }
    assert!(outcome.is_ok());
    // The status is the response's.
    assert_eq!(status, http::StatusCode::OK);
    gw.idle().await;
    let usage = tally.usage();
    assert_eq!(usage.input_tokens, 3);
    assert_eq!(usage.output_tokens, 1);
    assert_eq!(openai.requests().len(), 1);
    Ok(())
}

#[tokio::test]
async fn a_provider_failure_is_an_outcome_not_a_panic() -> Result<(), Fault> {
    // The scripted provider refuses rather than producing a completion.
    let openai = FakeOpenAi::start().await;
    openai.answer(Answer::error(502).message("upstream unavailable"));
    let gw = gateway(scripted_provider(&openai)?.connect(), Meter::default())?;
    let (_, _, outcome) = ask(&gw, "Say hi").await?;
    assert_eq!(
        outcome
            .err()
            .map(|f| respan_gateway_http::error_code(&f).to_owned()),
        Some("server_error".into())
    );
    assert_eq!(openai.requests().len(), 1);
    Ok(())
}
