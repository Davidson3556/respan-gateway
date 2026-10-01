use super::*;

#[tokio::test]
async fn credentials_beta_headers_and_unknown_fields_pass_untouched() -> Result<(), Fault> {
    let sent = Sent::default();
    let gw = gateway(fixture(http::StatusCode::OK, STREAM.concat(), sent.clone()))?;
    let (status, _, body) = read_whole(serve(&gw, request()).await.0).await;
    assert_eq!(status, http::StatusCode::OK);
    // The response, unknown event and field included, is the provider's bytes.
    assert_eq!(body?, STREAM.concat());
    let requests = sent.lock().unwrap_or_else(PoisonError::into_inner);
    assert_eq!(requests.len(), 1);
    let (uri, headers, body) = &requests[0];
    assert_eq!(uri.to_string(), "/v1/messages");
    assert_eq!(
        body.as_ref(),
        REQUEST.as_bytes(),
        "the request body relays byte for byte"
    );
    let header = |name| headers.get(name).and_then(|v| v.to_str().ok());
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
    let sent = Sent::default();
    let status =
        http::StatusCode::from_u16(529).map_err(|e| respan_gateway_http::built_wrong(e.into()))?;
    let gw = gateway(fixture(status, error, sent.clone()))?;
    let (status, _, body) = read_whole(serve(&gw, request()).await.0).await;
    assert_eq!((status.as_u16(), body?), (529, error.into()));
    assert_eq!(sent.lock().unwrap_or_else(PoisonError::into_inner).len(), 1);
    Ok(())
}
