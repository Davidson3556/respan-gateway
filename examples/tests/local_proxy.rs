use super::*;

#[tokio::test]
async fn credentials_beta_headers_and_unknown_fields_pass_untouched() -> Result<(), Fault> {
    let mock = MockTransport::new();
    mock.reply(ANTHROPIC, mock::sse(STREAM));
    let gw = gateway(mock.at(ANTHROPIC))?;
    let (status, _, body) = read_whole(gw.handle(request()).await.0).await;
    assert_eq!(status, http::StatusCode::OK);
    // The response, unknown event and field included, is the provider's bytes.
    assert_eq!(body?, STREAM.concat());
    let sent = &mock.sent()[0];
    assert_eq!(sent.uri(), "https://api.anthropic.com/v1/messages");
    assert_eq!(
        sent.body(),
        REQUEST,
        "the request body relays byte for byte"
    );
    let header = |name| sent.headers().get(name).and_then(|v| v.to_str().ok());
    assert_eq!(
        header("authorization"),
        Some("Bearer sk-ant-oat-subscription")
    );
    assert_eq!(
        header("anthropic-beta"),
        Some("interleaved-thinking-2025-05-14")
    );
    Ok(())
}

#[tokio::test]
async fn a_provider_error_reaches_the_client_as_the_provider_wrote_it() -> Result<(), Fault> {
    let error = r#"{"type":"error","error":{"type":"overloaded_error","message":"Overloaded"}}"#;
    let mock = MockTransport::new();
    mock.reply(
        ANTHROPIC,
        mock::json(
            http::StatusCode::from_u16(529).map_err(http::Error::from)?,
            error,
        ),
    );
    let gw = gateway(mock.at(ANTHROPIC))?;
    let (status, _, body) = read_whole(gw.handle(request()).await.0).await;
    assert_eq!((status.as_u16(), body?), (529, error.into()));
    Ok(())
}
