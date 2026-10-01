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
    let (a, b) = (LocalModels::start().await, LocalModels::start().await);
    a.answer(list(&["gpt-x", "shared"]));
    b.answer(list(&["llama", "shared"]));
    let (status, headers, body) = ask(&gateway(&a, &b)?).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        headers.get("content-type").and_then(|v| v.to_str().ok()),
        Some("application/json")
    );
    assert_eq!(ids(&body), ["gpt-x", "shared", "llama"], "one entry per id");
    assert_eq!(body["object"], "list");
    assert_eq!((a.requests().len(), b.requests().len()), (1, 1));
    Ok(())
}

#[tokio::test]
async fn each_provider_is_asked_under_its_own_key_never_the_clients() -> Result<(), Fault> {
    let (a, b) = (LocalModels::start().await, LocalModels::start().await);
    a.answer(list(&["a"]));
    b.answer(list(&["b"]));
    ask(&gateway(&a, &b)?).await;
    assert_eq!(
        a.requests(),
        [("GET".into(), "/v1/models".into(), "Bearer key-a".into())]
    );
    assert_eq!(
        b.requests(),
        [("GET".into(), "/v1/models".into(), "Bearer key-b".into())]
    );
    Ok(())
}

#[tokio::test]
async fn a_provider_that_fails_is_left_out_and_all_failing_fails() -> Result<(), Fault> {
    let (a, b) = (LocalModels::start().await, LocalModels::start().await);
    a.answer((StatusCode::SERVICE_UNAVAILABLE, "{}".into()));
    b.answer(list(&["llama"]));
    let (status, _, body) = ask(&gateway(&a, &b)?).await;
    assert_eq!((status, ids(&body)), (StatusCode::OK, vec!["llama"]));
    assert_eq!((a.requests().len(), b.requests().len()), (1, 1));
    a.answer((StatusCode::SERVICE_UNAVAILABLE, "{}".into()));
    b.answer((StatusCode::SERVICE_UNAVAILABLE, "{}".into()));
    let (status, ..) = ask(&gateway(&a, &b)?).await;
    assert_ne!(status, StatusCode::OK);
    assert_eq!((a.requests().len(), b.requests().len()), (2, 2));
    Ok(())
}

#[tokio::test]
async fn malformed_lists_are_failures_not_empty_successes() -> Result<(), Fault> {
    for bad in [r#"{}"#, r#"{"data":[{}]}"#, r#"{"data":[{"id":42}]}"#] {
        let (a, b) = (LocalModels::start().await, LocalModels::start().await);
        a.answer((StatusCode::OK, bad.into()));
        b.answer(list(&["valid"]));
        let (status, _, body) = ask(&gateway(&a, &b)?).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(ids(&body), ["valid"]);
        a.answer((StatusCode::OK, bad.into()));
        b.answer((StatusCode::SERVICE_UNAVAILABLE, "{}".into()));
        let (status, _, _) = ask(&gateway(&a, &b)?).await;
        assert_ne!(status, StatusCode::OK);
        assert_eq!((a.requests().len(), b.requests().len()), (2, 2));
    }
    Ok(())
}
