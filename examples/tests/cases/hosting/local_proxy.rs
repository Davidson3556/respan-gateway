use super::*;

/// Whether `got` says everything `sent` does: its fields are there, equal
/// (objects field by field). A re-encoded response may say more.
fn says_all(got: &serde_json::Value, sent: &serde_json::Value) -> bool {
    match (got, sent) {
        (serde_json::Value::Object(got), serde_json::Value::Object(sent)) => sent
            .iter()
            .all(|(key, value)| got.get(key).is_some_and(|g| says_all(g, value))),
        _ => got == sent,
    }
}

/// The JSON of each `data:` line of an event stream.
fn data_events(sse: &[u8]) -> Vec<serde_json::Value> {
    let text = String::from_utf8_lossy(sse).into_owned();
    let lines: Vec<_> = text
        .lines()
        .filter_map(|l| l.strip_prefix("data: "))
        .collect();
    lines
        .iter()
        .filter_map(|d| serde_json::from_str(d).ok())
        .collect()
}

#[test]
fn invalid_serve_arguments_fail_instead_of_running_the_offline_demo() {
    let args = |parts: &[&str]| {
        parts
            .iter()
            .map(|part| (*part).to_owned())
            .collect::<Vec<_>>()
    };
    assert_eq!(serve_address(&[]), Ok(None));
    assert_eq!(
        serve_address(&args(&["--serve", "127.0.0.1:8080"])),
        Ok(Some("127.0.0.1:8080"))
    );
    for parts in [
        vec!["--serve"],
        vec!["--serve", ""],
        vec!["--serve", "127.0.0.1:8080", "extra"],
        vec!["--unknown"],
        vec!["127.0.0.1:8080"],
    ] {
        assert!(serve_address(&args(&parts)).is_err(), "{parts:?}");
    }
}

#[test]
fn malformed_upstream_is_rejected_before_binding_or_announcing_readiness() {
    let Err(error) = upstream_gateway("not-a-url") else {
        panic!("malformed upstream must fail");
    };
    assert_eq!(error.code(), "config");
}

#[tokio::test]
async fn serve_mode_uses_local_upstream_and_relays_client_credentials()
-> Result<(), Box<dyn std::error::Error>> {
    use mockllm::{Answer, FakeAnthropic};
    let provider = FakeAnthropic::start().await;
    let reply = r#"{"id":"m1","type":"message","role":"assistant","model":"test","content":[{"type":"text","text":"local reply"}],"stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":1}}"#;
    provider.answer(Answer::raw(reply));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let gw = upstream_gateway(&provider.base_url())?;
    let server = tokio::spawn(async move { serve_upstream(listener, gw).await });
    let request = r#"{"model":"test","max_tokens":32,"messages":[{"role":"user","content":"hi"}]}"#;
    let response = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(3))
        .build()?
        .post(format!("http://{addr}/v1/messages"))
        .header("x-api-key", "sk-ant-test")
        .header("anthropic-version", "2023-06-01")
        .header("content-type", "application/json")
        .body(request)
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    // The response is re-encoded: everything the provider said is there.
    let got: serde_json::Value = serde_json::from_str(&response.text().await?)?;
    assert!(says_all(&got, &serde_json::from_str(reply)?), "{got}");
    let requests = provider.requests();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].path, "/v1/messages");
    let json = |raw: &[u8]| serde_json::from_slice::<serde_json::Value>(raw).ok();
    assert_eq!(
        json(&requests[0].raw),
        json(request.as_bytes()),
        "round-tripped"
    );
    assert_eq!(
        requests[0]
            .headers
            .get("x-api-key")
            .and_then(|v| v.to_str().ok()),
        Some("sk-ant-test")
    );
    server.abort();
    Ok(())
}

#[tokio::test]
async fn credentials_beta_headers_and_unknown_fields_round_trip() -> Result<(), Fault> {
    let sent = Sent::default();
    let gw = gateway(fixture(http::StatusCode::OK, STREAM.concat(), sent.clone()))?;
    let (status, _, body) = read_whole(serve(&gw, request()).await.0).await;
    assert_eq!(status, http::StatusCode::OK);
    // The response is re-encoded, not the provider's bytes: each event it
    // sent comes back in order and says all it said, the unknown event and
    // a block's unknown field included.
    let (got, said) = (data_events(&body?), data_events(STREAM.concat().as_bytes()));
    assert_eq!(got.len(), said.len(), "{got:?}");
    for (got, sent) in got.iter().zip(&said) {
        assert!(says_all(got, sent), "{got} vs {sent}");
    }
    assert!(got.contains(&serde_json::json!({ "type": "brand_new_event", "x": 1 })));
    let requests = sent.lock().unwrap_or_else(PoisonError::into_inner);
    assert_eq!(requests.len(), 1);
    let (uri, headers, body) = &requests[0];
    assert_eq!(
        uri.to_string(),
        "/v1/messages?beta=true",
        "its spec's query"
    );
    let json = |raw: &[u8]| serde_json::from_slice::<serde_json::Value>(raw).ok();
    assert_eq!(
        json(body),
        json(REQUEST.as_bytes()),
        "the request, written again, says what the client's did"
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
    let status = http::StatusCode::from_u16(529)
        .map_err(|e| respan_gateway_http_faults::built_wrong(e.into()))?;
    let gw = gateway(fixture(status, error, sent.clone()))?;
    let (status, _, body) = read_whole(serve(&gw, request()).await.0).await;
    assert_eq!((status.as_u16(), body?), (529, error.into()));
    assert_eq!(sent.lock().unwrap_or_else(PoisonError::into_inner).len(), 1);
    Ok(())
}
