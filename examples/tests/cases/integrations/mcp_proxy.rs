use super::*;

#[tokio::test]
async fn clients_see_and_call_only_the_allowed_tools() -> Result<(), Fault> {
    let provider = McpServer::start().await;
    let gw = gateway(HttpTransport::new(&provider.url)?.connect())?;
    let answers = session(&gw).await?;
    let [_, (_, list), (_, called), (status, refused)] = &answers[..] else {
        panic!("{answers:?}")
    };
    assert_eq!(
        list["result"]["tools"],
        json!([{ "name": "echo", "inputSchema": {"type":"object", "properties":{"message":{"type":"string"}}, "required":["message"]} }])
    );
    assert_eq!(called["result"]["content"][0]["text"], "hi");
    assert_eq!(
        (*status, &refused["id"], &refused["error"]["code"]),
        (StatusCode::OK, &json!(4), &json!(-32602))
    );
    // The refused call never reached the server; the others kept the session.
    let sent = provider.requests();
    assert_eq!(sent.len(), 4);
    assert_eq!(sent[0].1, None);
    assert_eq!(sent[1].1.as_deref(), Some("s1"));
    assert_eq!(sent[2].1.as_deref(), Some("s1"));
    assert_eq!(sent[1].0["method"], "notifications/initialized");
    assert!(sent[1].0.get("id").is_none());
    assert_eq!(sent[3].0["params"]["name"], "echo");
    Ok(())
}

#[tokio::test]
async fn a_mock_llm_tool_round_trip_uses_mcp_and_keeps_the_provider_call_id() -> Result<(), Fault> {
    use mockllm::{Answer, FakeAnthropic};
    use respan_gateway_conversation::Input;
    use respan_gateway_conversation::anthropic::AnthropicMessages;
    use respan_gateway_conversation::openai::OpenAiChat;
    let mcp = McpServer::start().await;
    let tools = gateway(HttpTransport::new(&mcp.url)?.connect())?;
    let (_, session, _) = initialize(&tools).await?;
    let (_, _, list) = send(&tools, session.as_deref(), Some(2), "tools/list", json!({})).await?;
    let offered: Vec<_> = list["result"]["tools"].as_array().expect("tool list").iter()
        .map(|t| json!({"type":"function","function":{"name":t["name"],"parameters":t["inputSchema"]}})).collect();

    let llm = FakeAnthropic::start().await;
    let text = "雪😀\nquote: \" and backslash: \\";
    llm.answer(Answer::tool_call("echo", json!({"message":text})));
    llm.answer(Answer::text("tool received"));
    let mut headers = respan_gateway_http::http::HeaderMap::new();
    headers.insert("x-api-key", HeaderValue::from_static("sk-ant-test"));
    let chat = Gateway::new()
        .client::<Input, _, _>(OpenAiChat::OPENAI)?
        .provider::<Input, _, _>(
            "llm",
            AnthropicMessages,
            HttpTransport::new(llm.base_url())?
                .with_headers(headers)?
                .connect(),
        )?
        .target("llm")?;
    let mut body = json!({"model":"claude-haiku-4-5","max_tokens":128,"messages":[{"role":"user","content":"Echo this string using the tool."}],"tools":offered});
    let (response, ended) = serve(&chat, post("/v1/chat/completions", body.to_string())).await;
    let (status, _, answer) = read_whole(response).await;
    assert_eq!(status, StatusCode::OK);
    ended.await?;
    let answer: Value = serde_json::from_slice(&answer?).expect("chat reply");
    let assistant = &answer["choices"][0]["message"];
    let call = &assistant["tool_calls"][0];
    assert_eq!(call["function"]["name"], "echo");
    let arguments: Value = serde_json::from_str(
        call["function"]["arguments"]
            .as_str()
            .expect("JSON arguments"),
    )
    .expect("tool arguments");
    assert_eq!(arguments["message"], text);
    let (_, _, result) = send(
        &tools,
        session.as_deref(),
        Some(3),
        "tools/call",
        json!({"name":call["function"]["name"],"arguments":arguments}),
    )
    .await?;
    assert_eq!(result["result"]["content"][0]["text"], text);
    let messages = body["messages"].as_array_mut().expect("messages");
    messages.push(assistant.clone());
    messages.push(json!({"role":"tool","tool_call_id":call["id"],"content":text}));
    let (response, ended) = serve(&chat, post("/v1/chat/completions", body.to_string())).await;
    let (status, _, answer) = read_whole(response).await;
    assert_eq!(status, StatusCode::OK);
    ended.await?;
    let answer: Value = serde_json::from_slice(&answer?).expect("final reply");
    assert_eq!(answer["choices"][0]["message"]["content"], "tool received");
    let sent = llm.requests();
    let tool_use = &sent[1].body["messages"][1]["content"][0];
    let tool_result = &sent[1].body["messages"][2]["content"][0];
    assert_eq!(tool_use["id"], call["id"]);
    assert_eq!(tool_result["tool_use_id"], call["id"]);
    assert_eq!(tool_result["content"], text);
    assert_eq!(mcp.requests()[3].0["params"]["arguments"]["message"], text);
    chat.idle().await;
    tools.idle().await;
    Ok(())
}
