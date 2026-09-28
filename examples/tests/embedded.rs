use super::*;
use respan_gateway_formats::conversation::units;

#[tokio::test]
async fn handle_returns_the_response_and_its_outcome_with_no_server() -> Result<(), Fault> {
    let mock = MockTransport::new();
    mock.reply(OPENAI, mock::json(http::StatusCode::OK, ANSWER));
    let tally = Meter::default();
    let gw = gateway(&mock, tally.clone())?;
    let (body, outcome) = ask(&gw, "Say hi").await?;
    assert_eq!(body, ANSWER, "a relay passes the provider's bytes");
    assert!(outcome.end.is_ok());
    assert_eq!(outcome.head.map(|h| h.status), Some(http::StatusCode::OK));
    gw.idle().await;
    let usage = tally.usage();
    assert_eq!(usage.get(units::INPUT_TOKENS), Some(3.0));
    assert_eq!(usage.get(units::OUTPUT_TOKENS), Some(1.0));
    Ok(())
}

#[tokio::test]
async fn a_provider_failure_is_an_outcome_not_a_panic() -> Result<(), Fault> {
    // Nothing queued: the mock answers 502 upstream.
    let mock = MockTransport::new();
    let gw = gateway(&mock, Meter::default())?;
    let (_, outcome) = ask(&gw, "Say hi").await?;
    assert_eq!(
        outcome.end.err().map(|f| f.code().to_owned()),
        Some("upstream".into())
    );
    Ok(())
}
