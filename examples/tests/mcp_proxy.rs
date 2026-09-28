use super::*;

#[tokio::test]
async fn clients_see_and_call_only_the_allowed_tools() -> Result<(), Fault> {
    let mock = mock_server();
    let gw = gateway(mock.at(AT))?;
    let answers = session(&gw).await?;
    let [_, (_, list), (_, called), (status, refused)] = &answers[..] else {
        panic!("{answers:?}")
    };
    assert_eq!(
        list["result"]["tools"],
        json!([{ "name": "echo", "inputSchema": {} }])
    );
    assert_eq!(called["result"]["content"][0]["text"], "hi");
    assert_eq!(
        (*status, &refused["id"], &refused["error"]["code"]),
        (StatusCode::OK, &json!(4), &json!(-32602))
    );
    // The refused call never reached the server; the others kept the session.
    let sent = mock.sent();
    assert_eq!(sent.len(), 3);
    assert_eq!(sent[2].headers()[SESSION], "s1");
    Ok(())
}
