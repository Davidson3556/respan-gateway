use super::*;
use respan_gateway_sdk::conversation::units;

#[tokio::test]
async fn handle_returns_the_response_and_its_outcome_with_no_server() -> Result<(), Fault> {
    let openai = FakeOpenAi::start().await;
    openai.answer(Answer::raw(ANSWER));
    let tally = Meter::default();
    let gw = gateway(scripted_provider(&openai)?.connect(), tally.clone())?;
    let (status, body, outcome) = ask(&gw, "Say hi").await?;
    assert_eq!(body, ANSWER, "a relay passes the provider's bytes");
    assert!(outcome.is_ok());
    // The status is the response's.
    assert_eq!(status, http::StatusCode::OK);
    gw.idle().await;
    let usage = tally.usage();
    assert_eq!(usage.get(units::INPUT_TOKENS), Some(3.0));
    assert_eq!(usage.get(units::OUTPUT_TOKENS), Some(1.0));
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
            .map(|f| respan_gateway_http::error_code(&f).into_owned()),
        Some("server_error".into())
    );
    assert_eq!(openai.requests().len(), 1);
    Ok(())
}
