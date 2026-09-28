use super::*;
use respan_gateway_formats::conversation::{Message, Role};

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

/// A plugin registered later that adds to each call's input, as a RAG
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
async fn what_a_later_plugin_adds_is_redacted_too() -> Result<(), Fault> {
    let mock = MockTransport::new();
    mock.reply(OPENAI, deltas(&["ok"]));
    let gw = gateway(&mock)?
        .plugin("rag", Rag)?
        .after("pii-input", ["rag"])?;
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
    let (status, _, body) = read_whole(gateway(&mock)?.handle(req).await.0).await;
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
