use super::*;
use serde_json::{Value, json};

async fn embed(gw: &Gateway, body: &str) -> (StatusCode, Result<Value, Fault>) {
    let (status, _, answered) =
        read_whole(serve(gw, post("/v1/embeddings", body.to_owned())).await.0).await;
    let parsed =
        answered.and_then(|b| serde_json::from_slice(&b).map_err(|_| Fault::internal("not JSON")));
    (status, parsed)
}

#[tokio::test]
async fn a_batch_over_the_cap_is_split_and_merged_in_order_with_usage_summed() -> Result<(), Fault>
{
    let mock = MockTransport::new();
    mock.reply(AT, answer(&[0.0, 1.0], 2))
        .reply(AT, answer(&[2.0, 3.0], 2))
        .reply(AT, answer(&[4.0], 1));
    let gw = gateway(&mock, 2)?;
    let body = r#"{"model":"m","dimensions":1,"input":["a","b","c","d","e"]}"#;
    let (status, answered) = embed(&gw, body).await;
    assert_eq!(status, StatusCode::OK);
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
        json!(5.0),
        "every call's usage"
    );
    // Three calls of at most two inputs, each keeping the client's other fields.
    let sent: Vec<Value> = mock
        .sent()
        .iter()
        .filter_map(|r| serde_json::from_slice(r.body()).ok())
        .collect();
    let inputs: Vec<&Value> = sent.iter().map(|s| &s["input"]).collect();
    assert_eq!(
        inputs,
        [&json!(["a", "b"]), &json!(["c", "d"]), &json!(["e"])]
    );
    assert!(
        sent.iter()
            .all(|s| s["dimensions"] == 1 && s["model"] == "m")
    );
    Ok(())
}

#[tokio::test]
async fn a_batch_within_the_cap_is_relayed_byte_for_byte() -> Result<(), Fault> {
    let mock = MockTransport::new();
    mock.reply(AT, answer(&[0.5, 1.5], 2));
    let gw = gateway(&mock, 2)?;
    let body = r#"{"model": "m",  "input": ["a","b"]}"#;
    let (status, _) = embed(&gw, body).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(mock.sent().len(), 1);
    assert_eq!(mock.sent()[0].body(), body.as_bytes());
    Ok(())
}

#[tokio::test]
async fn a_chunk_that_fails_fails_the_request() -> Result<(), Fault> {
    let mock = MockTransport::new();
    mock.reply(AT, answer(&[0.0, 1.0], 2)).reply(
        AT,
        mock::json(
            StatusCode::SERVICE_UNAVAILABLE,
            r#"{"error":{"message":"busy"}}"#,
        ),
    );
    let gw = gateway(&mock, 2)?;
    let (status, _) = embed(&gw, r#"{"model":"m","input":["a","b","c"]}"#).await;
    assert_ne!(status, StatusCode::OK);
    Ok(())
}
