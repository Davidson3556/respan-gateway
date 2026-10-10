use super::*;

#[tokio::test]
async fn unedited_messages_relay_what_they_say_and_a_filter_reads_their_usage() -> Result<(), Fault>
{
    // Already on the pinned model, so nothing changes it. The spacing of the
    // client's body is not kept: the format writes the value.
    let pinned = r#"{"model": "text-embedding-3-small",  "input": ["hi"], "dimensions": 2}"#;
    let (sent, status, answered, usage) = embed(pinned).await?;
    let json = |b: &[u8]| serde_json::from_slice::<Value>(b).ok();
    assert_eq!(json(&sent), json(pinned.as_bytes()));
    assert_eq!(status, StatusCode::OK);
    // The answer's vectors, model and usage are what it said.
    let answered = json(&answered).unwrap_or_default();
    assert_eq!(answered["model"], "text-embedding-3-small");
    assert_eq!(answered["data"][0]["embedding"], json!([0.5, -0.5]));
    assert_eq!(answered["usage"]["prompt_tokens"].as_f64(), Some(1.0));
    assert_eq!(usage.input_tokens, 1);
    Ok(())
}

#[test]
fn an_input_this_kind_cannot_read_is_refused_before_any_call() {
    for body in [
        r#"{"model":"m"}"#,
        r#"{"model":"m","input":[1,2,3]}"#,
        r#"{"model":"m","input":[[1,2],[3]]}"#,
        r#"{"model":"m","input":["a"],"encoding_format":"base64"}"#,
    ] {
        let refused = read_request(Bytes::from(body)).expect_err(body);
        assert!(refused.is::<BadRequest>(), "{body}");
    }
    let float = r#"{"model":"m","input":"a","encoding_format":"float"}"#;
    assert!(read_request(Bytes::from(float)).is_ok());
}

#[test]
fn each_written_embedding_names_its_object() {
    let one = Embeddings {
        vectors: vec![vec![0.5]],
        ..Embeddings::default()
    };
    let written = write_embeddings(&[one]).expect("a body");
    let written: Value = serde_json::from_slice(&written).expect("JSON");
    assert_eq!(written["data"][0]["object"], "embedding");
}

#[tokio::test]
async fn an_edit_is_written_with_the_clients_other_fields() -> Result<(), Fault> {
    let (sent, ..) = embed(ASKED).await?;
    let edited = json!({ "model": PINNED, "input": ["hi"], "dimensions": 2 });
    assert_eq!(serde_json::from_slice::<Value>(&sent).ok(), Some(edited));
    Ok(())
}
