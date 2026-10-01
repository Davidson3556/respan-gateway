use super::*;

#[tokio::test]
async fn unedited_messages_relay_byte_exact_and_a_filter_reads_their_usage() -> Result<(), Fault> {
    // Already on the pinned model, so nothing changes it.
    let pinned = r#"{"model": "text-embedding-3-small",  "input": ["hi"], "dimensions": 2}"#;
    let (sent, status, answered, usage) = embed(pinned).await?;
    assert_eq!(&sent[..], pinned.as_bytes());
    assert_eq!((status, &answered[..]), (StatusCode::OK, ANSWER.as_bytes()));
    assert_eq!(usage.get(INPUT_TOKENS), Some(1.0));
    Ok(())
}

#[tokio::test]
async fn an_edit_is_written_into_the_clients_json() -> Result<(), Fault> {
    let (sent, ..) = embed(ASKED).await?;
    let edited = json!({ "model": PINNED, "input": ["hi"], "dimensions": 2 });
    assert_eq!(serde_json::from_slice::<Value>(&sent).ok(), Some(edited));
    Ok(())
}
