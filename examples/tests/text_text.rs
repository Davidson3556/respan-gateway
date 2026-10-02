#[allow(dead_code)] // The shared helper also has request shapes these tests do not use.
#[path = "../../tests/support/http.rs"]
mod http_support;
#[path = "text_support/mod.rs"]
mod support;

#[path = "../support/text.rs"]
mod text;

use http_support::{post, read_whole, serve};
use mockllm::{Answer, FakeAnthropic, FakeOpenAi};
use respan_gateway_http::bytes::Bytes;
use respan_gateway_http::http::{self, HeaderMap, HeaderValue, StatusCode};
use respan_gateway_http::{HttpClient, Status};
use respan_gateway_providers::{AnthropicMessages, OpenAiChat};
use respan_gateway_sdk::conversation::{
    Block, Content, Conversation, Finish, Message, Reply, Role, Tool,
};
use respan_gateway_sdk::{ClientContext, Fault, Filter, Gateway, Plugin, Response, async_trait};
use respan_gateway_sdk::{ErrorCode, Upstream, Usage};
use serde_json::{self, json};
use support::{opened, run_filter as run};
use text::{Holdback, Placeholders, Readable, Rewrite, UNREADABLE, partial_match};

/// Refuses text holding "bomb".
struct Terms;

impl Rewrite for Terms {
    fn pending(&self, text: &str) -> usize {
        partial_match(text, ["bomb"])
    }

    fn rewrite(&mut self, text: &str) -> Result<String, Fault> {
        if text.contains("bomb") {
            return Err(Fault::new("blocked")
                .with(ErrorCode("blocked".into()))
                .with(Status(400)));
        }
        Ok(text.to_owned())
    }
}

/// The text of every delta in `replies`.
fn text(replies: &[Reply]) -> String {
    replies
        .iter()
        .filter_map(|r| match r {
            Reply::Delta { delta, .. } => Some(delta.as_str()),
            _ => None,
        })
        .collect()
}

/// Text block `block` opens and says each of `deltas`.
fn said(block: u32, deltas: &[&str]) -> Vec<Reply> {
    let open = Reply::block_start(block, Block::Text);
    let deltas = deltas.iter().map(|d| Reply::delta(block, *d));
    std::iter::once(open).chain(deltas).collect()
}

fn keep_alive() -> Reply {
    Reply::other("anthropic-messages", json!({ "type": "ping" }))
}

fn email() -> Placeholders {
    let vault = Placeholders::default();
    assert_eq!(vault.hide("EMAIL", "bob@x.io"), "<EMAIL_1>");
    assert_eq!(vault.hide("EMAIL", "bob@x.io"), "<EMAIL_1>");
    vault
}

#[test]
fn a_placeholder_split_across_three_deltas_is_restored() {
    let deltas = said(0, &["Mail <EMA", "IL_", "1> now"]);
    let out = run(&mut Holdback::new(email()), deltas);
    assert_eq!(text(&out.unwrap_or_default()), "Mail bob@x.io now");
}

#[test]
fn nothing_is_held_past_the_finish() {
    let mut holdback = Holdback::new(email());
    let mut out = Vec::new();
    for event in said(0, &["Hi <EMA"]) {
        assert!(holdback.edit(event, &mut out).is_ok());
    }
    assert_eq!(text(&out), "Hi ");
    assert!(
        holdback
            .edit(Reply::finished(Finish::Stop), &mut out)
            .is_ok()
    );
    assert_eq!(text(&out), "Hi <EMA");
    assert_eq!(out.last(), Some(&Reply::finished(Finish::Stop)));
    let mut tail = Vec::new();
    assert!(holdback.end(&Ok(()), &mut tail).is_ok());
    assert!(tail.is_empty(), "{tail:?}");
}

#[test]
fn held_text_goes_before_other_content_and_at_the_end() {
    let mut holdback = Holdback::new(email());
    let tool_use = json!({ "type": "tool_use", "name": "f", "input": {} });
    let mut replies = said(0, &["a <EMAIL_"]);
    replies.push(Reply::other("anthropic-messages", tool_use.clone()));
    replies.extend(said(1, &["b <EM"]));
    let out = run(&mut holdback, replies).unwrap_or_default();
    assert_eq!(
        out,
        [
            Reply::block_start(0, Block::Text),
            Reply::delta(0, "a "),
            Reply::delta(0, "<EMAIL_"),
            Reply::other("anthropic-messages", tool_use),
            Reply::block_start(1, Block::Text),
            Reply::delta(1, "b "),
            Reply::delta(1, "<EM"),
        ]
    );
}

#[test]
fn an_event_left_showing_nothing_waits_for_one_that_does() {
    let mut holdback = Holdback::new(Terms);
    let mut out = Vec::new();
    for event in [Reply::start("c1", "m"), Reply::block_start(0, Block::Text)] {
        assert!(holdback.edit(event, &mut out).is_ok());
    }
    assert!(holdback.edit(Reply::delta(0, "bo"), &mut out).is_ok());
    assert!(out.is_empty(), "{out:?}");
    assert!(holdback.edit(Reply::delta(0, "at"), &mut out).is_ok());
    assert_eq!(text(&out), "boat");
    assert_eq!(out[0], Reply::start("c1", "m"));
}

// Every value reaches a filter: a keep-alive or usage alone passes
// at once when nothing waits, even while text is held, and is never refused.
#[test]
fn a_keep_alive_or_usage_alone_passes_at_once_unless_events_wait() {
    let mut holdback = Holdback::new(Terms);
    let mut out = Vec::new();
    let mut counts = Usage::default();
    counts.add("output_tokens", 3.0);
    let usage = Reply::usage(counts);
    assert!(holdback.edit(keep_alive(), &mut out).is_ok());
    assert_eq!(out, [keep_alive()]);
    // "bo" may start a match, so it is held; usage alone still passes.
    for event in said(0, &["a bo"]) {
        assert!(holdback.edit(event, &mut out).is_ok());
    }
    assert!(holdback.edit(usage.clone(), &mut out).is_ok());
    assert_eq!(out.last(), Some(&usage));
    // Behind an event that waits, it waits too, in order.
    let (mut holdback, mut out) = (Holdback::new(Terms), Vec::new());
    for event in [Reply::start("c1", "m"), Reply::block_start(1, Block::Text)] {
        assert!(holdback.edit(event, &mut out).is_ok());
    }
    assert!(holdback.edit(keep_alive(), &mut out).is_ok());
    assert!(out.is_empty(), "{out:?}");
    assert!(holdback.edit(Reply::delta(1, "at"), &mut out).is_ok());
    assert_eq!(out[0], Reply::start("c1", "m"));
    assert_eq!(out[2], keep_alive(), "{out:?}");
    assert_eq!(out[3], Reply::delta(1, "at"), "{out:?}");
}

#[test]
fn a_match_ending_in_its_own_start_is_seen_whole() {
    let out = run(&mut Holdback::new(Terms), said(0, &["a bomb"]));
    assert!(out.is_err(), "{out:?}");
}

#[test]
fn a_match_across_deltas_is_seen_whole() {
    let out = run(&mut Holdback::new(Terms), said(0, &["a bo", "m", "b!"]));
    assert_eq!(
        out.err()
            .map(|f| respan_gateway_http::error_code(&f).into_owned())
            .as_deref(),
        Some("blocked")
    );
}

#[test]
fn a_label_matches_exactly_and_counts_its_own() {
    let vault = Placeholders::default();
    assert_eq!(vault.hide("EMAIL_WORK", "a@x.io"), "<EMAIL_WORK_1>");
    assert_eq!(vault.hide("EMAIL", "a@x.io"), "<EMAIL_1>");
    assert_eq!(vault.hide("EMAIL", "b@x.io"), "<EMAIL_2>");
    assert_eq!(vault.hide("EMAIL_WORK", "b@x.io"), "<EMAIL_WORK_2>");
    let out = run(
        &mut Holdback::new(vault),
        said(0, &["<EMAIL_2> <EMAIL_WORK_1>"]),
    );
    assert_eq!(text(&out.unwrap_or_default()), "b@x.io a@x.io");
}

#[test]
fn a_restored_value_is_not_restored_again() {
    let vault = Placeholders::default();
    // Someone literally wrote a placeholder's shape.
    assert_eq!(vault.hide("NAME", "<NAME_2>"), "<NAME_1>");
    assert_eq!(vault.hide("NAME", "Alice"), "<NAME_2>");
    let out = run(&mut Holdback::new(vault), said(0, &["<NAME_1>. <"]));
    assert_eq!(text(&out.unwrap_or_default()), "<NAME_2>. <");
}

#[test]
fn partial_match_is_the_longest_unfinished_start() {
    assert_eq!(partial_match("a <EMA", ["<EMAIL_1>", "<PHONE_1>"]), 4);
    assert_eq!(partial_match("a <EMAIL_1>", ["<EMAIL_1>"]), 0);
    assert_eq!(partial_match("a < b", ["<EMAIL_1>"]), 0);
    assert_eq!(partial_match("é", ["éa"]), 2);
    // The `b` ending a whole "bomb" may start another, but is not held
    // apart from it.
    assert_eq!(partial_match("a bomb", ["bomb"]), 0);
    assert_eq!(partial_match("a bob", ["bomb", "bob"]), 0);
    assert_eq!(partial_match("a bomb, b", ["bomb"]), 1);
}

#[test]
fn content_that_cannot_be_read_is_refused() {
    let image = Content::other("openai-chat", json!({ "type": "image_url" }));
    let mut message = Message::new(Role::User, "look");
    message.content.push(image);
    let chat = Conversation::new([message]);
    assert_eq!(
        chat.read_all()
            .err()
            .map(|f| respan_gateway_http::error_code(&f).into_owned())
            .as_deref(),
        Some(UNREADABLE)
    );
    let mut plain = Conversation::new([Message::new(Role::User, "hi")]);
    assert_eq!(plain.read_all().ok().as_deref(), Some("hi\n"));
    // The stop sequences and the end user's id are text too.
    plain.stop = vec!["bob@x.io".into()];
    plain.user = Some("bob@y.io".into());
    assert_eq!(
        plain.read_all().ok().as_deref(),
        Some("hi\nbob@x.io\nbob@y.io\n")
    );
    let upper = plain.edit_all(|text| {
        *text = text.to_uppercase();
        Ok(())
    });
    assert!(upper.is_ok());
    assert_eq!(
        (&plain.stop[..], plain.user.as_deref()),
        (&["BOB@X.IO".to_owned()][..], Some("BOB@Y.IO"))
    );
    assert_eq!(plain.messages[0].text(), "HI");
    assert_eq!(
        Reply::delta(0, "hi").read_all().ok().as_deref(),
        Some("hi\n")
    );
}

/// An agent's request: tools, an assistant's tool call and the tool's
/// answer.
fn agent_chat() -> Conversation {
    let arguments = json!({ "to": "bob@x.io", "cc": ["a@y.io"] }).to_string();
    let mut call = Message::new(Role::Assistant, "");
    call.content = vec![Content::tool_call("c1", "mail", arguments)];
    let mut answer = Message::new(Role::User, "");
    let said = vec![Content::from("sent to bob@x.io")];
    answer.content = vec![Content::tool_result("c1", said, false)];
    let mut chat = Conversation::new([Message::new(Role::User, "mail bob"), call, answer]);
    let parameters = json!({ "type": "object", "properties": { "to": { "type": "string" } } });
    let mut tool = Tool::new("mail", parameters);
    tool.description = Some("Mail; CEO is c@z.io".into());
    chat.tools.push(tool);
    chat
}

/// The text of the first part of the tool result in `message`.
fn result_text(message: &Message) -> String {
    match message.content.first() {
        Some(Content::ToolResult { content, .. }) => match content.first() {
            Some(Content::Text { text, .. }) => text.clone(),
            _ => String::new(),
        },
        _ => String::new(),
    }
}

#[test]
fn tools_and_tool_calls_are_read_and_edited_as_text() -> Result<(), Fault> {
    let mut chat = agent_chat();
    let all = chat.read_all()?;
    // Names inside the JSON are texts too, as a term may hide in one.
    for text in [
        "\nto\n",
        "\nbob@x.io\n",
        "\na@y.io\n",
        "sent to bob@x.io\n",
        "CEO is c@z.io\n",
    ] {
        assert!(all.contains(text), "{all}");
    }
    let vault = Placeholders::default();
    chat.edit_all(|text| {
        if text.contains('@') {
            *text = text.replace("bob@x.io", &vault.hide("EMAIL", "bob@x.io"));
        }
        Ok(())
    })?;
    let Some(Content::ToolCall { arguments, .. }) = chat.messages[1].content.first() else {
        panic!("a tool call")
    };
    let arguments: serde_json::Value =
        serde_json::from_str(arguments).map_err(|e| Fault::internal(e.to_string()))?;
    // Still JSON, the value hidden inside it.
    assert_eq!(arguments, json!({ "to": "<EMAIL_1>", "cc": ["a@y.io"] }));
    assert_eq!(result_text(&chat.messages[2]), "sent to <EMAIL_1>");
    Ok(())
}

#[test]
fn numbers_in_json_text_are_read_but_not_the_requests_own() -> Result<(), Fault> {
    let arguments = json!({ "card": 4111111111111111u64, "ok": true }).to_string();
    let mut call = Message::new(Role::Assistant, "");
    call.content = vec![Content::tool_call("c1", "pay", arguments)];
    let mut chat = Conversation::new([call]);
    chat.extra
        .fields
        .insert("seed".into(), json!(4111111111111111u64));
    assert!(chat.read_all()?.contains("\n4111111111111111\n"));
    chat.edit_all(|text| {
        *text = text.replace("4111111111111111", "<CARD_1>");
        Ok(())
    })?;
    let Some(Content::ToolCall { arguments, .. }) = chat.messages[0].content.first() else {
        panic!("a tool call")
    };
    let arguments: serde_json::Value =
        serde_json::from_str(arguments).map_err(|e| Fault::internal(e.to_string()))?;
    assert_eq!(arguments, json!({ "card": "<CARD_1>", "ok": true }));
    // The request's own numbers are not texts.
    assert_eq!(chat.extra.fields["seed"], json!(4111111111111111u64));
    Ok(())
}

fn call_delta(block: u32, arguments: &str) -> Reply {
    Reply::delta(block, arguments)
}

/// The arguments of each streamed call in `replies`, by call.
fn arguments_by_call(replies: &[Reply]) -> Vec<String> {
    let mut calls: Vec<String> = Vec::new();
    for reply in replies {
        let Reply::Delta { block, delta } = reply else {
            continue;
        };
        let at = *block as usize;
        calls.resize(calls.len().max(at + 1), String::new());
        calls[at].push_str(delta);
    }
    calls
}

#[test]
fn a_placeholder_split_across_three_argument_deltas_is_restored_by_call() {
    let deltas = opened(vec![
        call_delta(0, r#"{"to":"<EMA"#),
        call_delta(1, r#"{"cc":"<EMAIL_1>","n":"#),
        call_delta(0, "IL_"),
        call_delta(0, r#"1>"}"#),
        call_delta(1, "3}"),
        Reply::finished(Finish::ToolUse),
    ]);
    let out = run(&mut Holdback::new(email()), deltas).unwrap_or_default();
    assert_eq!(
        arguments_by_call(&out),
        [r#"{"to":"bob@x.io"}"#, r#"{"cc":"bob@x.io","n":3}"#]
    );
    assert_eq!(out.last(), Some(&Reply::finished(Finish::ToolUse)));
}

#[test]
fn a_tail_held_at_the_end_is_sent_then_and_never_lost() {
    // The stream ends (cut) inside a placeholder: what was held goes out.
    let out = run(
        &mut Holdback::new(email()),
        opened(vec![call_delta(0, r#"{"to":"<EMA"#)]),
    );
    assert_eq!(
        arguments_by_call(&out.unwrap_or_default()),
        [r#"{"to":"<EMA"#]
    );
}

/// A guard: inputs must be readable, outputs go through `Terms`.
struct Guard;

#[async_trait]
impl Plugin for Guard {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        cx.input::<Conversation>()?.read_all()?;
        cx.filter(Holdback::new(Terms))?;
        Ok(None)
    }
}

fn http_transport(
    base: &str,
    header: &'static str,
    key: &'static str,
) -> Result<
    impl Fn(respan_gateway_sdk::Extensions) -> respan_gateway_http::Outgoing + Send + Sync + 'static,
    Fault,
> {
    let mut headers = HeaderMap::new();
    headers.insert(header, HeaderValue::from_static(key));
    Ok(HttpClient::new(base)?.with_headers(headers)?.connect())
}

fn gateway_with(
    openai: impl Fn(respan_gateway_sdk::Extensions) -> respan_gateway_http::Outgoing
    + Send
    + Sync
    + 'static,
    anthropic: impl Fn(respan_gateway_sdk::Extensions) -> respan_gateway_http::Outgoing
    + Send
    + Sync
    + 'static,
    target: &str,
    plugin: impl Plugin,
) -> Result<Gateway, Fault> {
    Gateway::new()
        .provider::<Conversation, _, _>("openai", OpenAiChat, openai)?
        .provider::<Conversation, _, _>("anthropic", AnthropicMessages, anthropic)?
        .client::<Conversation, _, _>(OpenAiChat)?
        .client::<Conversation, _, _>(AnthropicMessages)?
        .plugin(plugin)
        .target(target)
}

fn gateway(
    openai: &FakeOpenAi,
    anthropic: &FakeAnthropic,
    target: &str,
    plugin: impl Plugin,
) -> Result<Gateway, Fault> {
    gateway_with(
        http_transport(&openai.base_url(), "authorization", "Bearer sk-test")?,
        http_transport(&anthropic.base_url(), "x-api-key", "sk-ant-test")?,
        target,
        plugin,
    )
}

fn openai_request(content: serde_json::Value) -> http::Request<respan_gateway_http::Body> {
    let body = json!({ "model": "gpt-4.1-mini", "stream": true, "messages": [{ "role": "user", "content": content }] });
    post("/v1/chat/completions", body.to_string())
}

fn anthropic_request() -> http::Request<respan_gateway_http::Body> {
    let body = json!({ "model": "claude-sonnet-4-5", "max_tokens": 9, "stream": true,
        "messages": [{ "role": "user", "content": "hi" }] });
    let mut request = post("/v1/messages", body.to_string());
    request
        .headers_mut()
        .insert("anthropic-version", HeaderValue::from_static("2023-06-01"));
    request
}

fn script(events: Vec<String>) -> Answer {
    Answer::events(
        events
            .into_iter()
            .map(|e| json!(e.trim_end_matches('\n')))
            .collect(),
    )
}

fn openai_stream(deltas: &[&str]) -> Answer {
    let mut events = vec![
        r#"data: {"id":"c1","model":"gpt-4.1-mini","choices":[{"index":0,"delta":{"role":"assistant","content":""}}]}"#.to_owned(),
    ];
    for d in deltas {
        let delta = json!({ "id": "c1", "model": "gpt-4.1-mini", "choices": [{ "index": 0, "delta": { "content": d } }] });
        events.push(format!("data: {delta}"));
    }
    events.push(
        r#"data: {"id":"c1","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}"#.into(),
    );
    events.push("data: [DONE]".into());
    script(events)
}

fn anthropic_stream(deltas: &[&str]) -> Answer {
    let event = |name: &str, data: serde_json::Value| format!("event: {name}\ndata: {data}");
    let mut events = vec![
        event(
            "message_start",
            json!({ "type": "message_start", "message": { "id": "m1", "model": "claude-sonnet-4-5", "usage": { "input_tokens": 1 } } }),
        ),
        event(
            "content_block_start",
            json!({ "type": "content_block_start", "index": 0, "content_block": { "type": "text", "text": "" } }),
        ),
    ];
    for d in deltas {
        events.push(event("content_block_delta", json!({ "type": "content_block_delta", "index": 0, "delta": { "type": "text_delta", "text": d } })));
    }
    events.push(event(
        "content_block_stop",
        json!({ "type": "content_block_stop", "index": 0 }),
    ));
    events.push(event("message_delta", json!({ "type": "message_delta", "delta": { "stop_reason": "end_turn" }, "usage": { "output_tokens": 3 } })));
    events.push(event("message_stop", json!({ "type": "message_stop" })));
    script(events)
}

/// The text of every OpenAI chunk or Anthropic delta in an event stream.
fn streamed_text(body: &str) -> String {
    let data = body.lines().filter_map(|l| l.strip_prefix("data: "));
    let json = data.filter_map(|d| serde_json::from_str::<serde_json::Value>(d).ok());
    json.filter_map(|v| {
        let text = v
            .pointer("/choices/0/delta/content")
            .or(v.pointer("/delta/text"));
        text.and_then(|t| t.as_str()).map(str::to_owned)
    })
    .collect()
}

/// Restores placeholders in every response.
struct Restore(Placeholders);

#[async_trait]
impl Plugin for Restore {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        cx.filter(Holdback::new(self.0.clone()))?;
        Ok(None)
    }
}

#[tokio::test]
async fn a_placeholder_split_across_three_deltas_is_restored_in_either_stream() -> Result<(), Fault>
{
    let (openai, anthropic) = (FakeOpenAi::start().await, FakeAnthropic::start().await);
    openai.answer(openai_stream(&["Mail <EMA", "IL_", "1> now"]));
    let gw = gateway(&openai, &anthropic, "openai", Restore(email()))?;
    let (status, _, body) = read_whole(serve(&gw, openai_request(json!("hi"))).await.0).await;
    let body = String::from_utf8_lossy(&body?).into_owned();
    assert_eq!(status, StatusCode::OK);
    assert_eq!(streamed_text(&body), "Mail bob@x.io now", "{body}");
    assert!(body.ends_with("data: [DONE]\n\n"), "{body}");

    anthropic.answer(anthropic_stream(&["Mail <EMA", "IL_", "1> now"]));
    let gw = gateway(&openai, &anthropic, "anthropic", Restore(email()))?;
    let (status, _, body) = read_whole(serve(&gw, anthropic_request()).await.0).await;
    let body = String::from_utf8_lossy(&body?).into_owned();
    assert_eq!(status, StatusCode::OK);
    assert_eq!(streamed_text(&body), "Mail bob@x.io now", "{body}");
    let (text, stop) = (body.find("now"), body.find("content_block_stop"));
    assert!(
        text < stop && body.ends_with("{\"type\":\"message_stop\"}\n\n"),
        "{body}"
    );
    Ok(())
}

#[tokio::test]
async fn a_placeholder_split_across_three_streamed_argument_deltas_reaches_the_client_restored()
-> Result<(), Fault> {
    let call = |args: &str, first: bool| {
        let mut call = json!({ "index": 0, "function": { "arguments": args } });
        if first {
            call["id"] = "call_1".into();
            call["type"] = "function".into();
            call["function"]["name"] = "send".into();
        }
        let chunk = json!({ "id": "c1", "model": "m", "choices": [{ "index": 0, "delta": { "tool_calls": [call] } }] });
        format!("data: {chunk}\n\n")
    };
    let finish = json!({ "id": "c1", "choices": [{ "index": 0, "delta": {}, "finish_reason": "tool_calls" }] });
    let chunks = [
        call(r#"{"to":"<EMA"#, true),
        call("IL_", false),
        call(r#"1>"}"#, false),
        format!("data: {finish}\n\n"),
        "data: [DONE]\n\n".to_owned(),
    ];
    let (openai, anthropic) = (FakeOpenAi::start().await, FakeAnthropic::start().await);
    openai.answer(script(chunks.to_vec()));
    let gw = gateway(&openai, &anthropic, "openai", Restore(email()))?;
    let (status, _, body) = read_whole(serve(&gw, openai_request(json!("hi"))).await.0).await;
    let body = String::from_utf8_lossy(&body?).into_owned();
    assert_eq!(status, StatusCode::OK);
    let events = body.lines().filter_map(|l| l.strip_prefix("data: "));
    let events: Vec<serde_json::Value> = events
        .filter_map(|d| serde_json::from_str(d).ok())
        .collect();
    let mut arguments = String::new();
    let mut names = 0;
    for call in events.iter().flat_map(|e| {
        e["choices"][0]["delta"]["tool_calls"]
            .as_array()
            .into_iter()
            .flatten()
    }) {
        arguments.push_str(call["function"]["arguments"].as_str().unwrap_or_default());
        names += usize::from(call["function"]["name"].is_string());
    }
    assert_eq!(arguments, r#"{"to":"bob@x.io"}"#, "{body}");
    assert_eq!(names, 1, "the name is said once: {body}");
    // The held tail went out before the finish, and the stream still ends.
    let last_args = body.rfind("bob@x.io");
    assert!(
        last_args < body.find("finish_reason\":\"tool_calls"),
        "{body}"
    );
    assert!(body.ends_with("data: [DONE]\n\n"), "{body}");
    Ok(())
}

#[tokio::test]
async fn a_violation_in_the_held_start_is_refused_before_any_byte() -> Result<(), Fault> {
    for (target, reply) in [
        ("openai", openai_stream(&["bo", "mb, then more"])),
        ("anthropic", anthropic_stream(&["bo", "mb, then more"])),
    ] {
        let (openai, anthropic) = (FakeOpenAi::start().await, FakeAnthropic::start().await);
        if target == "openai" {
            openai.answer(reply);
        } else {
            anthropic.answer(reply);
        }
        let gw = gateway(&openai, &anthropic, target, Guard)?;
        let (resp, outcome) = serve(&gw, openai_request(json!("hi"))).await;
        let (status, _, body) = read_whole(resp).await;
        let body = String::from_utf8_lossy(&body?).into_owned();
        assert_eq!(status, StatusCode::BAD_REQUEST, "{target}: {body}");
        assert!(
            !body.contains("data:") && body.contains("blocked"),
            "{target}: {body}"
        );
        assert_eq!(
            outcome
                .await
                .err()
                .map(|f| respan_gateway_http::error_code(&f).into_owned())
                .as_deref(),
            Some("blocked")
        );
    }
    Ok(())
}

#[tokio::test]
async fn an_unreadable_request_is_refused_before_any_call() -> Result<(), Fault> {
    let (openai, anthropic) = (FakeOpenAi::start().await, FakeAnthropic::start().await);
    let gw = gateway(&openai, &anthropic, "openai", Guard)?;
    let image = json!([{ "type": "image_url", "image_url": { "url": "data:," } }]);
    let (status, _, body) = read_whole(serve(&gw, openai_request(image)).await.0).await;
    let body = String::from_utf8_lossy(&body?).into_owned();
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert!(body.contains(UNREADABLE), "{body}");
    assert!(openai.requests().is_empty());
    assert!(anthropic.requests().is_empty());
    Ok(())
}

#[tokio::test]
async fn a_clean_stream_passes_unchanged() -> Result<(), Fault> {
    let (openai, anthropic) = (FakeOpenAi::start().await, FakeAnthropic::start().await);
    openai.answer(openai_stream(&["bo", "at trip ", "is b", "o"]));
    let gw = gateway(&openai, &anthropic, "openai", Guard)?;
    let (status, _, body) = read_whole(serve(&gw, openai_request(json!("hi"))).await.0).await;
    let body = String::from_utf8_lossy(&body?).into_owned();
    assert_eq!(status, StatusCode::OK);
    assert_eq!(streamed_text(&body), "boat trip is bo", "{body}");
    assert!(body.ends_with("data: [DONE]\n\n"), "{body}");
    Ok(())
}

/// Only the byte-fragment and injected-fault tests need control below HTTP
/// event framing. Provider protocol tests use mockllm and real HttpClient.
fn local(
    chunks: Vec<Result<Bytes, Fault>>,
) -> impl Fn(respan_gateway_sdk::Extensions) -> respan_gateway_http::Outgoing + Send + Sync + 'static
{
    use respan_gateway_sdk::futures::{StreamExt, TryStreamExt, stream};
    respan_gateway_http::connect(move |call| {
        let chunks = chunks.clone();
        async move {
            call.into_body().try_collect::<Vec<_>>().await?;
            let mut head = respan_gateway_http::http::Response::new(()).into_parts().0;
            head.headers.insert(
                "content-type",
                HeaderValue::from_static("text/event-stream"),
            );
            Ok((
                http::Response::from_parts(
                    head,
                    respan_gateway_http::Body::stream(stream::iter(chunks).boxed()),
                ),
                None,
            ))
        }
    })
}

/// A body of OpenAI chunks, cut into frames of `size` bytes.
fn cut(chunks: &[&str], size: usize) -> Vec<Result<Bytes, Fault>> {
    chunks
        .concat()
        .as_bytes()
        .chunks(size)
        .map(|c| Ok(Bytes::copy_from_slice(c)))
        .collect()
}

fn chunk(text: &str) -> String {
    let delta = json!({ "id": "c1", "choices": [{ "index": 0, "delta": { "content": text } }] });
    format!("data: {delta}\n\n")
}

#[tokio::test]
async fn text_split_inside_a_character_by_the_transport_is_restored() -> Result<(), Fault> {
    let anthropic = FakeAnthropic::start().await;
    let chunk = chunk("h\u{e9}llo <EMAIL_1> \u{1F600}!");
    let gw = gateway_with(
        local(cut(&[&chunk, "data: [DONE]\n\n"], 3)),
        http_transport(&anthropic.base_url(), "x-api-key", "sk-ant-test")?,
        "openai",
        Restore(email()),
    )?;
    let (status, _, body) = read_whole(serve(&gw, openai_request(json!("hi"))).await.0).await;
    let body = String::from_utf8_lossy(&body?).into_owned();
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        streamed_text(&body),
        "h\u{e9}llo bob@x.io \u{1F600}!",
        "{body}"
    );
    Ok(())
}

#[tokio::test]
async fn a_stream_cut_while_text_is_held_sends_it_then_the_error() -> Result<(), Fault> {
    let anthropic = FakeAnthropic::start().await;
    let gw = gateway_with(
        local(vec![
            Ok(Bytes::from(chunk("Mail <EMA"))),
            Err(Upstream::fault("reset")),
        ]),
        http_transport(&anthropic.base_url(), "x-api-key", "sk-ant-test")?,
        "openai",
        Restore(email()),
    )?;
    let (_, _, body) = read_whole(serve(&gw, openai_request(json!("hi"))).await.0).await;
    let body = String::from_utf8_lossy(&body.unwrap_or_default()).into_owned();
    assert_eq!(streamed_text(&body), "Mail <EMA", "{body}");
    assert!(body.contains("error"), "{body}");
    Ok(())
}

#[test]
fn a_value_restored_into_a_streamed_tool_fragment_is_escaped() {
    let vault = Placeholders::default();
    let ph = vault.hide("NAME", "Robert \"Bob\" Smith");
    let frags = [format!("{{\"to\":\"{ph}"), "\"}".to_owned()];
    let out = run(
        &mut Holdback::new(vault),
        opened(frags.iter().map(|f| call_delta(0, f)).collect()),
    )
    .unwrap_or_default();
    let joined = arguments_by_call(&out).concat();
    let parsed: serde_json::Value = serde_json::from_str(&joined).unwrap_or_default();
    assert_eq!(parsed["to"], "Robert \"Bob\" Smith", "{joined}");
}

// A keep-alive and the usage chunk pass a guard's Holdback: the client still
// gets the text, the finish, the usage it asked for and `[DONE]`. (The
// keep-alive comment says nothing, so it is not an event and is not resent.)
#[tokio::test]
async fn a_keep_alive_and_usage_pass_a_holdback_in_place() -> Result<(), Fault> {
    let chunks = [
        "data: {\"id\":\"c1\",\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi\"}}]}\n\n",
        ": keep-alive\n\n",
        "data: {\"id\":\"c1\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
        "data: {\"id\":\"c1\",\"choices\":[],\"usage\":{\"prompt_tokens\":3,\"completion_tokens\":1}}\n\n",
        "data: [DONE]\n\n",
    ];
    let (openai, anthropic) = (FakeOpenAi::start().await, FakeAnthropic::start().await);
    openai.answer(script(
        chunks.iter().map(|chunk| (*chunk).to_owned()).collect(),
    ));
    let gw = gateway(&openai, &anthropic, "openai", Guard)?;
    let body = json!({ "model": "gpt-4.1-mini", "stream": true, "stream_options": { "include_usage": true },
        "messages": [{ "role": "user", "content": "hi" }] });
    let req = post("/v1/chat/completions", body.to_string());
    let (status, _, body) = read_whole(serve(&gw, req).await.0).await;
    assert_eq!(status, StatusCode::OK);
    let body = String::from_utf8_lossy(&body?).into_owned();
    let events = body.lines().filter_map(|l| l.strip_prefix("data: "));
    let events: Vec<serde_json::Value> = events
        .filter_map(|d| serde_json::from_str(d).ok())
        .collect();
    assert_eq!(streamed_text(&body), "hi", "{body}");
    let finish = |e: &&serde_json::Value| e["choices"][0]["finish_reason"] == "stop";
    assert!(events.iter().any(|e| finish(&e)), "{body}");
    let usage = events.iter().find(|e| e["usage"].is_object());
    let usage = usage.map(|e| {
        (
            e["usage"]["prompt_tokens"].clone(),
            e["usage"]["completion_tokens"].clone(),
        )
    });
    assert_eq!(usage, Some((json!(3), json!(1))), "{body}");
    assert!(body.ends_with("data: [DONE]\n\n"), "{body}");
    Ok(())
}
