use super::*;
use serde_json::{Value, json};

async fn embed(gw: &Gateway, body: &str) -> (StatusCode, Result<Value, Fault>) {
    let (status, _, answered) =
        read_whole(serve(gw, post("/v1/embeddings", body.to_owned())).await.0).await;
    let parsed =
        answered.and_then(|b| serde_json::from_slice(&b).map_err(|_| Fault::internal("not JSON")));
    (status, parsed)
}

async fn provider() -> Result<(FakeOpenAi, respan_gateway_http::HttpTransport), Fault> {
    let openai = FakeOpenAi::start().await;
    let transport = provider_support::openai(&openai.base_url())?;
    Ok((openai, transport))
}

#[tokio::test]
async fn a_batch_over_the_cap_is_split_and_merged_in_order_with_usage_summed() -> Result<(), Fault>
{
    let (openai, transport) = provider().await?;
    for scripted in [
        answer(&[0.0, 1.0], 2),
        answer(&[2.0, 3.0], 2),
        answer(&[4.0], 1),
    ] {
        openai.answer(scripted);
    }
    let gw = gateway(transport.connect(), 2)?;
    let body = r#"{"model":"text-embedding-3-small","dimensions":1,"input":["a","b","c","d","e"]}"#;
    let (status, answered) = embed(&gw, body).await;
    assert_eq!(status, StatusCode::OK, "{:?}", openai.requests());
    let answered = answered?;
    let firsts: Vec<_> = answered["data"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|d| (d["index"].clone(), d["embedding"][0].clone()))
        .collect();
    let want: Vec<_> = (0..5).map(|i| (json!(i), json!(f64::from(i)))).collect();
    assert_eq!(firsts, want, "the client's order, indexed anew");
    assert_eq!(
        answered["usage"]["prompt_tokens"],
        json!(5),
        "every call's usage"
    );
    assert_eq!(answered["object"], "list");
    assert_eq!(answered["model"], MODEL);
    // Three calls of at most two inputs, each keeping the client's other fields.
    let sent: Vec<Value> = openai.requests().into_iter().map(|r| r.body).collect();
    assert_eq!(sent.len(), 3);
    let inputs: Vec<&Value> = sent.iter().map(|s| &s["input"]).collect();
    assert_eq!(
        inputs,
        [&json!(["a", "b"]), &json!(["c", "d"]), &json!(["e"])]
    );
    assert!(
        sent.iter()
            .all(|s| s["dimensions"] == 1 && s["model"] == MODEL)
    );
    Ok(())
}

#[tokio::test]
async fn a_batch_within_the_cap_is_sent_whole_with_what_it_said() -> Result<(), Fault> {
    let (openai, transport) = provider().await?;
    openai.answer(answer(&[0.5, 1.5], 2));
    let gw = gateway(transport.connect(), 2)?;
    let body = r#"{"model": "text-embedding-3-small",  "input": ["a","b"]}"#;
    let (status, _) = embed(&gw, body).await;
    assert_eq!(status, StatusCode::OK);
    let requests = openai.requests();
    assert_eq!(requests.len(), 1);
    let sent: Value = serde_json::from_slice(&requests[0].raw).unwrap_or_default();
    assert_eq!(sent, json!({ "model": MODEL, "input": ["a", "b"] }));
    Ok(())
}

#[tokio::test]
async fn a_chunk_that_fails_fails_the_request() -> Result<(), Fault> {
    let (openai, transport) = provider().await?;
    openai.answer(answer(&[0.0, 1.0], 2));
    for _ in 0..3 {
        openai.answer(Answer::error(503).message("busy"));
    }
    let gw = gateway(transport.connect(), 2)?;
    let (status, _) = embed(
        &gw,
        &format!(r#"{{"model":"{MODEL}","input":["a","b","c"]}}"#),
    )
    .await;
    assert_ne!(status, StatusCode::OK);
    assert_eq!(
        openai.requests().len(),
        4,
        "the failed chunk exhausts three tries"
    );
    Ok(())
}

#[tokio::test]
async fn a_zero_batch_cap_is_rejected() -> Result<(), Fault> {
    let (_, transport) = provider().await?;
    assert!(gateway(transport.connect(), 0).is_err());
    Ok(())
}

#[tokio::test]
async fn out_of_order_vectors_are_merged_by_their_input_index() -> Result<(), Fault> {
    let (openai, transport) = provider().await?;
    openai.answer(Answer::raw(
        r#"{"data":[{"index":1,"embedding":[1.0]},{"index":0,"embedding":[0.0]}]}"#,
    ));
    openai.answer(answer(&[2.0], 1));
    let (status, body) = embed(
        &gateway(transport.connect(), 2)?,
        &format!(r#"{{"model":"{MODEL}","input":["a","b","c"]}}"#),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let body = body?;
    for i in 0..3 {
        assert_eq!(body["data"][i]["embedding"][0], json!(i as f64));
    }
    assert_eq!(openai.requests().len(), 2);
    Ok(())
}

#[tokio::test]
async fn missing_or_duplicate_vectors_fail_the_split_request() -> Result<(), Fault> {
    for bad in [
        r#"{"data":[{"index":0,"embedding":[0.0]}]}"#,
        r#"{"data":[{"index":0,"embedding":[0.0]},{"index":0,"embedding":[1.0]}]}"#,
        r#"{"data":[{"index":0,"embedding":"base64"},{"index":1,"embedding":[1.0]}]}"#,
    ] {
        let (openai, transport) = provider().await?;
        openai.answer(Answer::raw(bad));
        openai.answer(answer(&[2.0], 1));
        // If decoding the malformed answer is classified retryable, each
        // retry must still receive malformed data, never a fake default.
        openai.answer(Answer::raw(bad));
        openai.answer(Answer::raw(bad));
        let (status, _) = embed(
            &gateway(transport.connect(), 2)?,
            &format!(r#"{{"model":"{MODEL}","input":["a","b","c"]}}"#),
        )
        .await;
        assert_ne!(
            status,
            StatusCode::OK,
            "{bad}; requests: {:?}",
            openai.requests()
        );
        assert!(
            (2..=4).contains(&openai.requests().len()),
            "each chunk was called, with at most three attempts per bad chunk"
        );
    }
    Ok(())
}

#[tokio::test]
async fn only_a_failed_chunk_is_retried_before_the_merged_answer() -> Result<(), Fault> {
    let (openai, transport) = provider().await?;
    openai.answer(answer(&[0.0, 1.0], 2));
    openai.answer(Answer::error(503).message("busy"));
    openai.answer(answer(&[2.0], 1));
    let (status, body) = embed(
        &gateway(transport.connect(), 2)?,
        &format!(r#"{{"model":"{MODEL}","input":["a","b","c"]}}"#),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body?["data"][2]["embedding"][0], json!(2.0));
    let sent = openai.requests();
    assert_eq!(sent.len(), 3);
    assert_eq!(sent[1].raw, sent[2].raw);
    assert_ne!(sent[0].raw, sent[1].raw);
    Ok(())
}
