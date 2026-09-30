use super::*;

async fn ask(gw: &Gateway) -> (StatusCode, http::HeaderMap, Value) {
    let (status, headers, body) = read_whole(serve(gw, request()).await.0).await;
    (
        status,
        headers,
        serde_json::from_slice(&body.unwrap_or_default()).unwrap_or_default(),
    )
}

fn ids(body: &Value) -> Vec<&str> {
    body["data"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|m| m["id"].as_str())
        .collect()
}

#[tokio::test]
async fn every_providers_models_come_back_as_one_list() -> Result<(), Fault> {
    let mock = MockTransport::new();
    mock.reply(A, list(&["gpt-x", "shared"]))
        .reply(B, list(&["llama", "shared"]));
    let (status, headers, body) = ask(&gateway(&mock)?).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        headers.get("content-type").and_then(|v| v.to_str().ok()),
        Some("application/json")
    );
    assert_eq!(ids(&body), ["gpt-x", "shared", "llama"], "one entry per id");
    assert_eq!(body["object"], "list");
    Ok(())
}

#[tokio::test]
async fn each_provider_is_asked_under_its_own_key_never_the_clients() -> Result<(), Fault> {
    let mock = MockTransport::new();
    mock.reply(A, list(&["a"])).reply(B, list(&["b"]));
    ask(&gateway(&mock)?).await;
    let sent = mock.sent();
    let mut keys: Vec<(String, String)> = sent
        .iter()
        .map(|r| {
            (
                r.uri().host().unwrap_or_default().to_owned(),
                format!("{:?}", r.headers().get("authorization")),
            )
        })
        .collect();
    keys.sort();
    assert_eq!(keys[0].1, r#"Some("Bearer key-a")"#);
    assert_eq!(keys[1].1, r#"Some("Bearer key-b")"#);
    assert!(
        sent.iter()
            .all(|r| r.method() == http::Method::GET && r.uri().path() == "/v1/models")
    );
    Ok(())
}

#[tokio::test]
async fn a_provider_that_fails_is_left_out_and_all_failing_fails() -> Result<(), Fault> {
    let mock = MockTransport::new();
    mock.reply(A, mock::json(StatusCode::SERVICE_UNAVAILABLE, "{}"))
        .reply(B, list(&["llama"]));
    let (status, _, body) = ask(&gateway(&mock)?).await;
    assert_eq!((status, ids(&body)), (StatusCode::OK, vec!["llama"]));
    let mock = MockTransport::new();
    mock.reply(A, mock::json(StatusCode::SERVICE_UNAVAILABLE, "{}"))
        .reply(B, mock::json(StatusCode::SERVICE_UNAVAILABLE, "{}"));
    let (status, ..) = ask(&gateway(&mock)?).await;
    assert_ne!(status, StatusCode::OK);
    Ok(())
}

#[tokio::test]
async fn malformed_lists_are_failures_not_empty_successes() -> Result<(), Fault> {
    for bad in [r#"{}"#, r#"{"data":[{}]}"#, r#"{"data":[{"id":42}]}"#] {
        let mock = MockTransport::new();
        mock.reply(A, mock::json(StatusCode::OK, bad))
            .reply(B, list(&["valid"]));
        let (status, _, body) = ask(&gateway(&mock)?).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(ids(&body), ["valid"]);
        mock.reply(A, mock::json(StatusCode::OK, bad))
            .reply(B, mock::json(StatusCode::SERVICE_UNAVAILABLE, "{}"));
        let (status, _, _) = ask(&gateway(&mock)?).await;
        assert_ne!(status, StatusCode::OK);
    }
    Ok(())
}
