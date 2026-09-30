use super::*;
use respan_gateway_messages::conversation::Conversation;
use respan_gateway_messages::conversation::{Message, Role};
use respan_gateway_providers::AnthropicMessages;

#[test]
fn hides_addresses_only() {
    let vault = Placeholders::default();
    let text = "bob@x.io, (a@b.co) or 42; not a@b, @x.io or x.io.";
    assert_eq!(
        hide_emails(text, &vault),
        "<EMAIL_1>, (<EMAIL_2>) or 42; not a@b, @x.io or x.io."
    );
}

#[tokio::test]
async fn a_placeholder_split_across_three_deltas_is_restored() -> Result<(), Fault> {
    let mock = MockTransport::new();
    mock.reply(OPENAI, deltas(&["Sure: <EMA", "IL_", "1> and <EMAIL_2>."]));
    let (status, text, sent) = ask(&mock, chat(PROMPT.into(), json!({}))).await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(text, "Sure: bob@x.io and alice@y.org.");
    assert!(sent.contains("Mail <EMAIL_1>, or <<EMAIL_2>>."), "{sent}");
    assert!(!sent.contains('@'), "{sent}");
    Ok(())
}

#[tokio::test]
async fn stop_sequences_and_the_user_id_are_redacted_too() -> Result<(), Fault> {
    let mock = MockTransport::new();
    mock.reply(OPENAI, deltas(&["ok"]));
    let more = json!({ "stop": ["bob@x.io"], "user": "carol@z.io" });
    let (status, _, sent) = ask(&mock, chat("hi".into(), more)).await?;
    assert_eq!(status, StatusCode::OK);
    let sent: serde_json::Value = serde_json::from_str(&sent).unwrap_or_default();
    assert_eq!(
        (&sent["stop"], &sent["user"]),
        (&json!(["<EMAIL_1>"]), &json!("<EMAIL_2>"))
    );
    Ok(())
}

#[tokio::test]
async fn what_it_cannot_read_is_refused() -> Result<(), Fault> {
    let mock = MockTransport::new();
    let image = json!([{ "type": "image_url", "image_url": { "url": "data:," } }]);
    let (status, body, _) = ask(&mock, chat(image, json!({}))).await?;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert!(mock.sent().is_empty());
    Ok(())
}

/// A plugin registered before `pii-input` that adds to each call's input, as a RAG
/// or memory plugin does.
struct Rag;

#[async_trait]
impl Plugin for Rag {
    async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
        px.filter(|chat: &mut Conversation| {
            let context = "the user is carol@z.io";
            chat.messages.push(Message::new(Role::User, context));
            Ok(())
        })
    }
}

#[tokio::test]
async fn what_an_earlier_plugin_adds_is_redacted_too() -> Result<(), Fault> {
    let mock = MockTransport::new();
    mock.reply(OPENAI, deltas(&["ok"]));
    let gw = gateway_with(&mock, |gw| gw.plugin("rag", Rag))?;
    let (status, _, sent) = ask_via(&gw, &mock, chat("hi".into(), json!({}))).await?;
    assert_eq!(status, StatusCode::OK);
    assert!(
        sent.contains("the user is <EMAIL_1>") && !sent.contains('@'),
        "{sent}"
    );
    Ok(())
}

#[tokio::test]
async fn an_agent_request_is_redacted_and_its_tool_call_restored() -> Result<(), Fault> {
    let mock = MockTransport::new();
    let answer = json!({ "id": "c2", "model": "gpt", "choices": [{ "index": 0,
        "message": { "role": "assistant", "content": null, "tool_calls": [{ "id": "t2",
            "type": "function", "function": { "name": "mail",
                "arguments": r#"{"to":"<EMAIL_1>"}"# } }] },
        "finish_reason": "tool_calls" }] });
    mock.reply(OPENAI, mock::json(StatusCode::OK, &answer.to_string()));
    let call = json!({ "id": "t1", "type": "function",
        "function": { "name": "mail", "arguments": r#"{"to":"bob@x.io"}"# } });
    let tools = json!([{ "type": "function", "function": { "name": "mail",
        "description": "Sends mail; the admin is root@x.io" } }]);
    let more = json!({ "stream": false, "tools": tools, "messages": [
        { "role": "user", "content": "mail bob@x.io" },
        { "role": "assistant", "content": null, "tool_calls": [call] },
        { "role": "tool", "tool_call_id": "t1", "content": "sent" },
    ] });
    let req = chat("".into(), more);
    let (status, _, body) = read_whole(serve(&gateway(&mock)?, req).await.0).await;
    assert_eq!(status, StatusCode::OK);
    let sent = mock
        .sent()
        .last()
        .map(|r| r.body().to_vec())
        .unwrap_or_default();
    let sent: serde_json::Value = serde_json::from_slice(&sent).unwrap_or_default();
    assert!(!sent.to_string().contains('@'), "{sent}");
    let arguments = &sent["messages"][1]["tool_calls"][0]["function"]["arguments"];
    assert_eq!(arguments, r#"{"to":"<EMAIL_1>"}"#);
    // The client gets the answer's tool call restored.
    let body: serde_json::Value = serde_json::from_slice(&body?).unwrap_or_default();
    let arguments = &body["choices"][0]["message"]["tool_calls"][0]["function"]["arguments"];
    assert_eq!(arguments, r#"{"to":"bob@x.io"}"#, "{body}");
    Ok(())
}

#[tokio::test]
async fn several_choices_restore_independently_whole_or_streamed() -> Result<(), Fault> {
    let mock = MockTransport::new();
    let events = [
        json!([{"index":0,"delta":{"content":"a <EMA"}},{"index":1,"delta":{"content":"b <EM"}}]),
        json!([{"index":1,"delta":{"content":"AIL_2>"}},{"index":0,"delta":{"content":"IL_"}}]),
        json!([{"index":0,"delta":{"content":"1>"},"finish_reason":"stop"}]),
        json!([{"index":1,"delta":{"content":" and <EMA"}}]),
        json!([{"index":1,"delta":{"content":"IL_1>"},"finish_reason":"stop"}]),
    ]
    .map(|choices| {
        format!(
            "data: {}\n\n",
            json!({"id":"c1","object":"chat.completion.chunk","model":"gpt","choices":choices})
        )
    });
    let mut chunks: Vec<_> = events.iter().map(String::as_str).collect();
    chunks.push("data: [DONE]\n\n");
    mock.reply(OPENAI, mock::sse(&chunks));
    let (status, _, body) = read_whole(
        serve(&gateway(&mock)?, chat(PROMPT.into(), json!({"n":2})))
            .await
            .0,
    )
    .await;
    let body = String::from_utf8_lossy(&body?).into_owned();
    assert_eq!(status, StatusCode::OK, "{body}");
    let mut texts = [String::new(), String::new()];
    let mut finishes = [0, 0];
    for data in body.lines().filter_map(|l| l.strip_prefix("data: ")) {
        let Ok(value) = serde_json::from_str::<serde_json::Value>(data) else {
            continue;
        };
        for choice in value["choices"].as_array().into_iter().flatten() {
            let index = choice["index"].as_u64().unwrap_or(0) as usize;
            texts[index].push_str(choice["delta"]["content"].as_str().unwrap_or_default());
            finishes[index] += usize::from(choice["finish_reason"].is_string());
        }
    }
    assert_eq!(
        texts,
        ["a bob@x.io", "b alice@y.org and bob@x.io"],
        "{body}"
    );
    assert_eq!(finishes, [1, 1], "{body}");
    let answer = json!({ "id": "c1", "model": "gpt", "choices": [
        { "index": 0, "message": { "role": "assistant", "content": "a <EMAIL_1>" } },
        { "index": 1, "message": { "role": "assistant", "content": "b <EMAIL_1>" } }] });
    mock.reply(OPENAI, mock::json(StatusCode::OK, &answer.to_string()));
    let whole = chat(PROMPT.into(), json!({ "n": 2, "stream": false }));
    let (status, _, body) = read_whole(serve(&gateway(&mock)?, whole).await.0).await;
    let body = String::from_utf8_lossy(&body?).into_owned();
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(
        body.contains("a bob@x.io") && body.contains("b bob@x.io"),
        "{body}"
    );
    Ok(())
}

#[tokio::test]
async fn a_tool_description_with_quoted_emails_is_redacted() -> Result<(), Fault> {
    let mock = MockTransport::new();
    mock.reply(OPENAI, deltas(&["ok"]));
    let tools = json!({ "tools": [{ "type": "function", "function": {
        "name": "mail", "description": "Mail \"ops@x.io\" or \"help@x.io\"",
        "parameters": { "type": "object" } } }] });
    let (status, text, sent) = ask(&mock, chat("hi".into(), tools)).await?;
    assert_eq!(status, StatusCode::OK, "{text}");
    assert!(!sent.contains("ops@x.io"), "{sent}");
    Ok(())
}

#[tokio::test]
async fn an_earlier_tool_call_with_quoted_emails_is_redacted() -> Result<(), Fault> {
    let mock = MockTransport::new();
    mock.reply(OPENAI, deltas(&["ok"]));
    let args = json!({ "body": "Cc \"ops@x.io\" and \"help@x.io\"" }).to_string();
    let body = json!({ "model": "gpt", "stream": true, "messages": [
        { "role": "user", "content": "send it" },
        { "role": "assistant", "content": null, "tool_calls": [{ "id": "c1", "type": "function",
            "function": { "name": "mail", "arguments": args } }] },
        { "role": "tool", "tool_call_id": "c1", "content": "sent" } ] });
    let (status, text, sent) = ask(&mock, post("/v1/chat/completions", body.to_string())).await?;
    assert_eq!(status, StatusCode::OK, "{text}");
    assert!(
        !sent.contains("ops@x.io") && !sent.contains("help@x.io"),
        "{sent}"
    );
    Ok(())
}

#[tokio::test]
async fn a_placeholder_split_across_streamed_tool_arguments_is_restored() -> Result<(), Fault> {
    let mock = MockTransport::new();
    let call = |args: &str, first: bool| {
        let mut call = json!({ "index": 0, "function": { "arguments": args } });
        if first {
            call["id"] = "call_1".into();
            call["type"] = "function".into();
            call["function"]["name"] = "send".into();
        }
        let chunk = json!({ "id": "c1", "model": "gpt", "choices": [{ "index": 0, "delta": { "tool_calls": [call] } }] });
        format!("data: {chunk}\n\n")
    };
    let finish = json!({ "id": "c1", "choices": [{ "index": 0, "delta": {}, "finish_reason": "tool_calls" }] });
    let chunks = [
        call("{\"to\":\"<EMA", true),
        call("IL_", false),
        call("1>\"}", false),
        format!("data: {finish}\n\n"),
        "data: [DONE]\n\n".to_owned(),
    ];
    mock.reply(
        OPENAI,
        mock::sse(&chunks.iter().map(String::as_str).collect::<Vec<_>>()),
    );
    let gw = gateway(&mock)?;
    let (status, _, body) = read_whole(serve(&gw, chat(PROMPT.into(), json!({}))).await.0).await;
    let body = String::from_utf8_lossy(&body?).into_owned();
    assert_eq!(status, StatusCode::OK);
    let mut args = String::new();
    for data in body.lines().filter_map(|l| l.strip_prefix("data: ")) {
        let Ok(v) = serde_json::from_str::<serde_json::Value>(data) else {
            continue;
        };
        for c in v["choices"][0]["delta"]["tool_calls"]
            .as_array()
            .into_iter()
            .flatten()
        {
            args.push_str(c["function"]["arguments"].as_str().unwrap_or_default());
        }
    }
    assert_eq!(args, r#"{"to":"bob@x.io"}"#, "{body}");
    Ok(())
}

fn sse_call(args: &str, first: bool, index: u64) -> String {
    let mut call = json!({ "index": index, "function": { "arguments": args } });
    if first {
        call["id"] = format!("call_{index}").into();
        call["type"] = "function".into();
        call["function"]["name"] = "send".into();
    }
    let chunk = json!({ "id": "c1", "object": "chat.completion.chunk", "model": "gpt", "choices": [{ "index": 0, "delta": { "tool_calls": [call] }, "finish_reason": null }] });
    format!("data: {chunk}\n\n")
}

async fn stream_of(chunks: Vec<String>) -> Result<(StatusCode, String), Fault> {
    let mock = MockTransport::new();
    mock.reply(
        OPENAI,
        mock::sse(&chunks.iter().map(String::as_str).collect::<Vec<_>>()),
    );
    let gw = gateway(&mock)?;
    let (status, _, body) = read_whole(serve(&gw, chat(PROMPT.into(), json!({}))).await.0).await;
    Ok((status, String::from_utf8_lossy(&body?).into_owned()))
}

fn joined_args(body: &str, index: u64) -> (String, Vec<serde_json::Value>) {
    let mut args = String::new();
    let mut all = Vec::new();
    for data in body.lines().filter_map(|l| l.strip_prefix("data: ")) {
        let Ok(v) = serde_json::from_str::<serde_json::Value>(data) else {
            continue;
        };
        for c in v["choices"][0]["delta"]["tool_calls"]
            .as_array()
            .into_iter()
            .flatten()
        {
            if c["index"].as_u64() == Some(index) {
                args.push_str(c["function"]["arguments"].as_str().unwrap_or_default());
            }
        }
        all.push(v);
    }
    (args, all)
}

/// B-e2e-1: a placeholder held at the finish chunk (the call's JSON left
/// open, as a `length` cut does) goes out restored, as a well-formed chunk
/// before the finish.
#[tokio::test]
async fn b_a_tail_held_at_the_finish_goes_out_before_it() -> Result<(), Fault> {
    let finish = json!({ "id": "c1", "object": "chat.completion.chunk", "model": "gpt", "choices": [{ "index": 0, "delta": {}, "finish_reason": "length" }] });
    let chunks = vec![
        sse_call("{\"to\":\"<EMA", true, 0),
        sse_call("IL_1>", false, 0),
        format!("data: {finish}\n\n"),
        "data: [DONE]\n\n".to_owned(),
    ];
    let (status, body) = stream_of(chunks).await?;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (args, all) = joined_args(&body, 0);
    assert_eq!(args, "{\"to\":\"bob@x.io", "{body}");
    // Every chunk is a chat.completion.chunk with an id; the finish is last.
    for v in &all {
        assert!(
            v["id"].is_string() && v["object"] == "chat.completion.chunk",
            "{v}\n{body}"
        );
    }
    let last_finish = all
        .iter()
        .rposition(|v| v["choices"][0]["finish_reason"].is_string());
    let last_args = all
        .iter()
        .rposition(|v| !v["choices"][0]["delta"]["tool_calls"].is_null());
    assert!(last_args < last_finish, "{body}");
    Ok(())
}

/// B-e2e-2: the split `\\` escape end to end.
#[tokio::test]
async fn b_a_split_backslash_before_a_placeholder_is_restored_end_to_end() -> Result<(), Fault> {
    let finish = json!({ "id": "c1", "choices": [{ "index": 0, "delta": {}, "finish_reason": "tool_calls" }] });
    let chunks = vec![
        sse_call("{\"p\":\"C:\\", true, 0),
        sse_call("\\<EMAIL_1>\"}", false, 0),
        format!("data: {finish}\n\n"),
        "data: [DONE]\n\n".to_owned(),
    ];
    let (status, body) = stream_of(chunks).await?;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (args, _) = joined_args(&body, 0);
    let parsed: serde_json::Value = serde_json::from_str(&args).unwrap_or_default();
    assert_eq!(parsed["p"], "C:\\bob@x.io", "{args}");
    Ok(())
}

/// B-e2e-3: two calls interleaved, each split, through the real encoder.
#[tokio::test]
async fn b_two_interleaved_calls_are_restored_end_to_end() -> Result<(), Fault> {
    let finish = json!({ "id": "c1", "choices": [{ "index": 0, "delta": {}, "finish_reason": "tool_calls" }] });
    let chunks = vec![
        sse_call("{\"to\":\"<EM", true, 0),
        sse_call("{\"to\":\"<EMAIL", true, 1),
        sse_call("AIL_1>\"}", false, 0),
        sse_call("_2>", false, 1),
        format!("data: {finish}\n\n"),
        "data: [DONE]\n\n".to_owned(),
    ];
    let (status, body) = stream_of(chunks).await?;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(joined_args(&body, 0).0, "{\"to\":\"bob@x.io\"}", "{body}");
    assert_eq!(joined_args(&body, 1).0, "{\"to\":\"alice@y.org", "{body}");
    Ok(())
}

/// An OpenAI client served by an Anthropic provider: the placeholder split
/// across `partial_json` fragments is restored in the OpenAI arguments.
#[tokio::test]
async fn a_placeholder_split_across_anthropic_tool_input_is_restored() -> Result<(), Fault> {
    const ANTHROPIC: &str = "http://anthropic.test/v1";
    let event = |name: &str, data: serde_json::Value| format!("event: {name}\ndata: {data}\n\n");
    let partial = |json: &str| {
        let delta = json!({ "type": "input_json_delta", "partial_json": json });
        event(
            "content_block_delta",
            json!({ "type": "content_block_delta", "index": 0, "delta": delta }),
        )
    };
    let start = json!({ "type": "content_block_start", "index": 0,
        "content_block": { "type": "tool_use", "id": "toolu_1", "name": "send", "input": {} } });
    let message = json!({ "id": "m1", "type": "message", "role": "assistant", "model": "c",
        "content": [], "usage": { "input_tokens": 5, "output_tokens": 1 } });
    let stop = json!({ "type": "message_delta", "usage": { "output_tokens": 9 },
        "delta": { "stop_reason": "tool_use", "stop_sequence": null } });
    let chunks = [
        event(
            "message_start",
            json!({ "type": "message_start", "message": message }),
        ),
        event("content_block_start", start),
        partial("{\"to\":\"<EMA"),
        partial("IL_"),
        partial("1>\"}"),
        event(
            "content_block_stop",
            json!({ "type": "content_block_stop", "index": 0 }),
        ),
        event("message_delta", stop),
        event("message_stop", json!({ "type": "message_stop" })),
    ];
    let mock = MockTransport::new();
    mock.reply(
        ANTHROPIC,
        mock::sse(&chunks.iter().map(String::as_str).collect::<Vec<_>>()),
    );
    let gw = Gateway::new()
        .provider::<Conversation, _, _>(
            "anthropic",
            AnthropicMessages,
            respan_gateway_http::connect(mock.at(ANTHROPIC)),
        )?
        .client::<Conversation, _, _>(OpenAiChat)?
        .plugin("pii", Pii)?
        .plugin("pii-input", PiiInput)?
        .target("anthropic")?;
    let (status, _, body) = read_whole(serve(&gw, chat(PROMPT.into(), json!({}))).await.0).await;
    let body = String::from_utf8_lossy(&body?).into_owned();
    assert_eq!(status, StatusCode::OK, "{body}");
    let (args, _) = joined_args(&body, 0);
    assert_eq!(args, r#"{"to":"bob@x.io"}"#, "{body}");
    Ok(())
}

#[tokio::test]
async fn anthropic_tool_history_is_redacted_and_its_reply_restored() -> Result<(), Fault> {
    const AT: &str = "http://anthropic.test";
    let mock = MockTransport::new();
    mock.reply(AT, mock::json(StatusCode::OK, r#"{"id":"m","type":"message","role":"assistant","model":"c","content":[{"type":"text","text":"<EMAIL_1>"}],"stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":1}}"#));
    let gateway = Gateway::new()
        .client::<Conversation, _, _>(AnthropicMessages)?
        .provider::<Conversation, _, _>(
            "p",
            AnthropicMessages,
            respan_gateway_http::connect(mock.at(AT)),
        )?
        .plugin("pii", Pii)?
        .plugin("pii-input", PiiInput)?
        .target("p")?;
    let body = json!({"model":"c","max_tokens":10,"messages":[
        {"role":"assistant","content":[{"type":"tool_use","id":"call1","name":"send","input":{"to":"bob@x.io"}}]},
        {"role":"user","content":[{"type":"tool_result","tool_use_id":"call1","content":"sent to bob@x.io"}]}
    ]});
    let (status, _, body) = read_whole(
        serve(&gateway, post("/v1/messages", body.to_string()))
            .await
            .0,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(String::from_utf8_lossy(&body?).contains("bob@x.io"));
    let sent: serde_json::Value =
        serde_json::from_slice(mock.sent()[0].body()).map_err(|_| Fault::internal("test JSON"))?;
    assert_eq!(
        sent["messages"][0]["content"][0]["input"]["to"],
        "<EMAIL_1>"
    );
    assert_eq!(
        sent["messages"][1]["content"][0]["content"],
        "sent to <EMAIL_1>"
    );
    assert_eq!(sent["messages"][0]["content"][0]["id"], "call1");
    assert_eq!(sent["messages"][1]["content"][0]["tool_use_id"], "call1");
    Ok(())
}

#[tokio::test]
async fn finishing_one_choice_does_not_release_another_choices_tool_arguments() -> Result<(), Fault>
{
    let chunks = [
        json!([{"index":1,"delta":{"tool_calls":[{"index":0,"id":"t","type":"function","function":{"name":"send","arguments":"{\"to\":\"<EMA"}}]}}]),
        json!([{"index":0,"delta":{},"finish_reason":"stop"}]),
        json!([{"index":1,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"IL_1>"}}]}}]),
        json!([{"index":1,"delta":{},"finish_reason":"length"}]),
    ].map(|choices| format!("data: {}\n\n", json!({"id":"c1","object":"chat.completion.chunk","model":"gpt","choices":choices})));
    let mut chunks = chunks.to_vec();
    chunks.push("data: [DONE]\n\n".into());
    let (status, body) = stream_of(chunks).await?;
    assert_eq!(status, StatusCode::OK, "{body}");
    let mut arguments = String::new();
    let mut finished = false;
    for data in body.lines().filter_map(|line| line.strip_prefix("data: ")) {
        let Ok(value) = serde_json::from_str::<serde_json::Value>(data) else {
            continue;
        };
        for choice in value["choices"]
            .as_array()
            .into_iter()
            .flatten()
            .filter(|c| c["index"] == 1)
        {
            for call in choice["delta"]["tool_calls"]
                .as_array()
                .into_iter()
                .flatten()
            {
                let text = call["function"]["arguments"].as_str().unwrap_or_default();
                assert!(
                    !finished || text.is_empty(),
                    "arguments after finish: {body}"
                );
                arguments.push_str(text);
            }
            finished |= choice["finish_reason"].is_string();
        }
    }
    assert!(finished, "{body}");
    assert_eq!(arguments, "{\"to\":\"bob@x.io", "{body}");
    Ok(())
}
