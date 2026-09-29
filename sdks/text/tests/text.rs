use respan_gateway_core::bytes::Bytes;
use respan_gateway_core::serde_json::{self, json};
use respan_gateway_core::{ClientContext, Fault, Filter, Gateway, Plugin, Response, async_trait};
use respan_gateway_formats::conversation::{
    Content, Conversation, Finish, Message, Piece, Reply, Role,
};
use respan_gateway_formats::{AnthropicMessages, OpenAiChat};
use respan_gateway_http::http::{self, StatusCode};
use respan_gateway_testkit::{MockTransport, mock, post, read_whole, run_filter as run, serve};
use respan_gateway_text::{Holdback, Placeholders, Readable, Rewrite, UNREADABLE, partial_match};

const OPENAI: &str = "http://openai.test/v1";
const ANTHROPIC: &str = "http://anthropic.test/v1";

/// Refuses text holding "bomb".
struct Terms;

impl Rewrite for Terms {
    fn pending(&self, text: &str) -> usize {
        partial_match(text, ["bomb"])
    }

    fn rewrite(&mut self, text: &str) -> Result<String, Fault> {
        if text.contains("bomb") {
            return Err(Fault::new(StatusCode::BAD_REQUEST, "blocked", "blocked"));
        }
        Ok(text.to_owned())
    }
}

fn text(replies: &[Reply]) -> String {
    replies.iter().flat_map(Reply::texts).collect()
}

fn email() -> Placeholders {
    let vault = Placeholders::default();
    assert_eq!(vault.hide("EMAIL", "bob@x.io"), "<EMAIL_1>");
    assert_eq!(vault.hide("EMAIL", "bob@x.io"), "<EMAIL_1>");
    vault
}

#[test]
fn a_placeholder_split_across_three_deltas_is_restored() {
    let deltas = ["Mail <EMA", "IL_", "1> now"].map(|d| Reply::text(0, d));
    let out = run(&mut Holdback::new(email()), deltas.into());
    assert_eq!(text(&out.unwrap_or_default()), "Mail bob@x.io now");
}

#[test]
fn nothing_is_held_past_the_finish() {
    let mut holdback = Holdback::new(email());
    let mut out = Vec::new();
    assert!(holdback.edit(Reply::text(0, "Hi <EMA"), &mut out).is_ok());
    assert_eq!(text(&out), "Hi ");
    assert!(
        holdback
            .edit(Reply::finished(Finish::Stop), &mut out)
            .is_ok()
    );
    assert_eq!(text(&out), "Hi <EMA");
    assert_eq!(
        out.last().and_then(|r| r.finish.clone()),
        Some(Finish::Stop)
    );
    let mut tail = Vec::new();
    assert!(holdback.end(&Ok(()), &mut tail).is_ok());
    assert!(tail.is_empty(), "{tail:?}");
}

#[test]
fn held_text_goes_before_other_content_and_at_the_end() {
    let mut holdback = Holdback::new(email());
    let replies = vec![
        Reply::text(0, "a <EMAIL_"),
        Reply::other("f", json!({ "tool": 1 })),
        Reply::text(1, "b <EM"),
    ];
    let out = run(&mut holdback, replies).unwrap_or_default();
    let pieces: Vec<Piece> = out.into_iter().flat_map(|r| r.content).collect();
    assert_eq!(
        pieces[..3],
        [
            Piece::text(0, "a "),
            Piece::text(0, "<EMAIL_"),
            Piece::other("f", json!({ "tool": 1 })),
        ]
    );
    assert_eq!(pieces[3..], [Piece::text(1, "b "), Piece::text(1, "<EM")]);
}

#[test]
fn a_reply_left_showing_nothing_waits_for_one_that_does() {
    let mut holdback = Holdback::new(Terms);
    let mut out = Vec::new();
    let mut start = Reply::start("c1", "m");
    start.content.push(Piece::text(0, "bo"));
    assert!(holdback.edit(start, &mut out).is_ok());
    assert!(out.is_empty());
    assert!(holdback.edit(Reply::text(0, "at"), &mut out).is_ok());
    assert_eq!(text(&out), "boat");
    assert!(out[0].start.is_some());
}

// Every value reaches a filter (0010): a keep-alive or usage alone passes
// at once when nothing waits, even while text is held, and is never refused.
#[test]
fn a_keep_alive_or_usage_alone_passes_at_once_unless_replies_wait() {
    let mut holdback = Holdback::new(Terms);
    let mut out = Vec::new();
    let keep_alive = Reply::default();
    let mut usage = Reply::default();
    usage.usage.add("output_tokens", 3.0);
    assert!(holdback.edit(keep_alive.clone(), &mut out).is_ok());
    assert_eq!(out.len(), 1);
    assert_eq!(out[0], keep_alive);
    // "bo" may start a match, so it is held; usage alone still passes.
    assert!(holdback.edit(Reply::text(0, "a bo"), &mut out).is_ok());
    assert!(holdback.edit(usage.clone(), &mut out).is_ok());
    assert_eq!(out.last(), Some(&usage));
    // Behind a reply that waits, it waits too, in order.
    let (mut holdback, mut out) = (Holdback::new(Terms), Vec::new());
    let mut start = Reply::start("c1", "m");
    start.content.push(Piece::text(1, ""));
    assert!(holdback.edit(start, &mut out).is_ok());
    assert!(holdback.edit(keep_alive.clone(), &mut out).is_ok());
    assert!(out.is_empty(), "{out:?}");
    assert!(holdback.edit(Reply::text(1, "at"), &mut out).is_ok());
    assert!(out[0].start.is_some() && out[1] == keep_alive, "{out:?}");
}

#[test]
fn a_match_ending_in_its_own_start_is_seen_whole() {
    let out = run(&mut Holdback::new(Terms), vec![Reply::text(0, "a bomb")]);
    assert!(out.is_err(), "{out:?}");
}

#[test]
fn a_match_across_deltas_is_seen_whole() {
    let replies = ["a bo", "m", "b!"].map(|d| Reply::text(0, d));
    let out = run(&mut Holdback::new(Terms), replies.into());
    assert_eq!(
        out.err().map(|f| f.code().to_owned()).as_deref(),
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
        vec![Reply::text(0, "<EMAIL_2> <EMAIL_WORK_1>")],
    );
    assert_eq!(text(&out.unwrap_or_default()), "b@x.io a@x.io");
}

#[test]
fn a_restored_value_is_not_restored_again() {
    let vault = Placeholders::default();
    // Someone literally wrote a placeholder's shape.
    assert_eq!(vault.hide("NAME", "<NAME_2>"), "<NAME_1>");
    assert_eq!(vault.hide("NAME", "Alice"), "<NAME_2>");
    let out = run(
        &mut Holdback::new(vault),
        vec![Reply::text(0, "<NAME_1>. <")],
    );
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
    let image = Content::other(json!({ "type": "image_url" }));
    let mut message = Message::new(Role::User, "look");
    message.content.push(image);
    let chat = Conversation::new([message]);
    assert_eq!(
        chat.read_all()
            .err()
            .map(|f| f.code().to_owned())
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
        Reply::text(0, "hi").read_all().ok().as_deref(),
        Some("hi\n")
    );
}

/// An agent's request: tools, an assistant's tool call and the tool's
/// answer.
fn agent_chat() -> Conversation {
    let mut call = Message::new(Role::Assistant, "");
    let arguments = json!({ "to": "bob@x.io", "cc": ["a@y.io"] }).to_string();
    let calls = json!([{ "id": "c1", "function": { "name": "mail", "arguments": arguments } }]);
    call.unsupported.insert("tool_calls".into(), calls);
    let mut answer = Message::new(Role::Other("tool".into()), "sent to bob@x.io");
    answer
        .unsupported
        .insert("tool_call_id".into(), "c1".into());
    let mut chat = Conversation::new([Message::new(Role::User, "mail bob"), call, answer]);
    let tools = json!([{ "function": { "name": "mail", "description": "Mail; CEO is c@z.io",
        "parameters": { "type": "object", "properties": { "to": { "type": "string" } } } } }]);
    chat.unsupported.insert("tools".into(), tools);
    chat
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
    let arguments = &chat.messages[1].unsupported["tool_calls"][0]["function"]["arguments"];
    let arguments: serde_json::Value = serde_json::from_str(arguments.as_str().unwrap_or(""))
        .map_err(|e| Fault::internal(e.to_string()))?;
    // Still JSON, the value hidden inside it.
    assert_eq!(arguments, json!({ "to": "<EMAIL_1>", "cc": ["a@y.io"] }));
    assert_eq!(chat.messages[2].text(), "sent to <EMAIL_1>");
    Ok(())
}

#[test]
fn numbers_in_json_text_are_read_but_not_the_requests_own() -> Result<(), Fault> {
    let arguments = json!({ "card": 4111111111111111u64, "ok": true }).to_string();
    let calls = json!([{ "function": { "arguments": arguments } }]);
    let mut call = Message::new(Role::Assistant, "");
    call.unsupported.insert("tool_calls".into(), calls);
    let mut chat = Conversation::new([call]);
    chat.unsupported
        .insert("seed".into(), json!(4111111111111111u64));
    assert!(chat.read_all()?.contains("\n4111111111111111\n"));
    chat.edit_all(|text| {
        *text = text.replace("4111111111111111", "<CARD_1>");
        Ok(())
    })?;
    let arguments = &chat.messages[0].unsupported["tool_calls"][0]["function"]["arguments"];
    let arguments: serde_json::Value = serde_json::from_str(arguments.as_str().unwrap_or(""))
        .map_err(|e| Fault::internal(e.to_string()))?;
    assert_eq!(arguments, json!({ "card": "<CARD_1>", "ok": true }));
    // The request's own numbers are not texts.
    assert_eq!(chat.unsupported["seed"], json!(4111111111111111u64));
    Ok(())
}

#[test]
fn a_whole_tool_call_is_restored_but_not_one_split_across_fragments() {
    let call = |arguments: &str| {
        let raw = json!([{ "index": 0, "delta": { "tool_calls": [{ "function": { "arguments": arguments } }] } }]);
        Reply::other("openai", raw)
    };
    let arguments = |reply: &Reply| match &reply.content[..] {
        [Piece::Other { raw, .. }] => {
            raw[0]["delta"]["tool_calls"][0]["function"]["arguments"].clone()
        }
        other => panic!("{other:?}"),
    };
    let whole = run(
        &mut Holdback::new(email()),
        vec![call(r#"{"to":"<EMAIL_1>"}"#)],
    );
    let whole = whole.unwrap_or_default();
    assert_eq!(arguments(&whole[0]), r#"{"to":"bob@x.io"}"#);
    // Streamed: each fragment on its own, so one split is not restored.
    let fragments = vec![call(r#"{"to":"<EMA"#), call(r#"IL_1>"}"#)];
    let split = run(&mut Holdback::new(email()), fragments).unwrap_or_default();
    assert_eq!(arguments(&split[0]), r#"{"to":"<EMA"#);
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

fn gateway(mock: &MockTransport, target: &str, plugin: impl Plugin) -> Result<Gateway, Fault> {
    Gateway::new()
        .provider("openai", OpenAiChat, mock.at(OPENAI))?
        .provider("anthropic", AnthropicMessages, mock.at(ANTHROPIC))?
        .serve(OpenAiChat)?
        .serve(AnthropicMessages)?
        .plugin("guard", plugin)?
        .target(target)
}

fn openai_request(content: serde_json::Value) -> http::Request<respan_gateway_http::Body> {
    let body = json!({ "model": "m", "stream": true, "messages": [{ "role": "user", "content": content }] });
    post("/v1/chat/completions", body.to_string())
}

fn anthropic_request() -> http::Request<respan_gateway_http::Body> {
    let body = json!({ "model": "c", "max_tokens": 9, "stream": true,
        "messages": [{ "role": "user", "content": "hi" }] });
    post("/v1/messages", body.to_string())
}

fn openai_stream(deltas: &[&str]) -> mock::Reply {
    let mut chunks = vec![
        r#"data: {"id":"c1","model":"m","choices":[{"index":0,"delta":{"role":"assistant","content":""}}]}"#.to_owned(),
    ];
    for d in deltas {
        let delta = json!({ "id": "c1", "model": "m", "choices": [{ "index": 0, "delta": { "content": d } }] });
        chunks.push(format!("data: {delta}"));
    }
    chunks.push(
        r#"data: {"id":"c1","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}"#.into(),
    );
    chunks.push("data: [DONE]".into());
    let chunks: Vec<String> = chunks.into_iter().map(|c| c + "\n\n").collect();
    mock::sse(&chunks.iter().map(String::as_str).collect::<Vec<_>>())
}

fn anthropic_stream(deltas: &[&str]) -> mock::Reply {
    let event = |name: &str, data: serde_json::Value| format!("event: {name}\ndata: {data}\n\n");
    let mut events = vec![
        event(
            "message_start",
            json!({ "type": "message_start", "message": { "id": "m1", "model": "c", "usage": { "input_tokens": 1 } } }),
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
    mock::sse(&events.iter().map(String::as_str).collect::<Vec<_>>())
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
    let mock = MockTransport::new();
    mock.reply(OPENAI, openai_stream(&["Mail <EMA", "IL_", "1> now"]));
    let gw = gateway(&mock, "openai", Restore(email()))?;
    let (status, _, body) = read_whole(serve(&gw, openai_request(json!("hi"))).await.0).await;
    let body = String::from_utf8_lossy(&body?).into_owned();
    assert_eq!(status, StatusCode::OK);
    assert_eq!(streamed_text(&body), "Mail bob@x.io now", "{body}");
    assert!(body.ends_with("data: [DONE]\n\n"), "{body}");

    mock.reply(ANTHROPIC, anthropic_stream(&["Mail <EMA", "IL_", "1> now"]));
    let gw = gateway(&mock, "anthropic", Restore(email()))?;
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
async fn a_violation_in_the_held_start_is_refused_before_any_byte() -> Result<(), Fault> {
    for (target, reply) in [
        ("openai", openai_stream(&["bo", "mb, then more"])),
        ("anthropic", anthropic_stream(&["bo", "mb, then more"])),
    ] {
        let mock = MockTransport::new();
        mock.reply(
            if target == "openai" {
                OPENAI
            } else {
                ANTHROPIC
            },
            reply,
        );
        let gw = gateway(&mock, target, Guard)?;
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
                .end
                .err()
                .map(|f| f.code().to_owned())
                .as_deref(),
            Some("blocked")
        );
    }
    Ok(())
}

#[tokio::test]
async fn an_unreadable_request_is_refused_before_any_call() -> Result<(), Fault> {
    let mock = MockTransport::new();
    let gw = gateway(&mock, "openai", Guard)?;
    let image = json!([{ "type": "image_url", "image_url": { "url": "data:," } }]);
    let (status, _, body) = read_whole(serve(&gw, openai_request(image)).await.0).await;
    let body = String::from_utf8_lossy(&body?).into_owned();
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert!(body.contains(UNREADABLE), "{body}");
    assert!(mock.sent().is_empty());
    Ok(())
}

#[tokio::test]
async fn a_clean_stream_passes_unchanged() -> Result<(), Fault> {
    let mock = MockTransport::new();
    mock.reply(OPENAI, openai_stream(&["bo", "at trip ", "is b", "o"]));
    let gw = gateway(&mock, "openai", Guard)?;
    let (status, _, body) = read_whole(serve(&gw, openai_request(json!("hi"))).await.0).await;
    let body = String::from_utf8_lossy(&body?).into_owned();
    assert_eq!(status, StatusCode::OK);
    assert_eq!(streamed_text(&body), "boat trip is bo", "{body}");
    assert!(body.ends_with("data: [DONE]\n\n"), "{body}");
    Ok(())
}

/// A body of OpenAI chunks, cut into frames of `size` bytes.
fn cut(chunks: &[&str], size: usize) -> mock::Reply {
    let all: String = chunks.concat();
    let mut reply = mock::sse(&[]);
    let frames = all.as_bytes().chunks(size);
    *reply.body_mut() = frames.map(|c| Ok(Bytes::copy_from_slice(c))).collect();
    reply
}

fn chunk(text: &str) -> String {
    let delta = json!({ "id": "c1", "choices": [{ "index": 0, "delta": { "content": text } }] });
    format!("data: {delta}\n\n")
}

#[tokio::test]
async fn text_split_inside_a_character_by_the_transport_is_restored() -> Result<(), Fault> {
    let mock = MockTransport::new();
    let chunk = chunk("h\u{e9}llo <EMAIL_1> \u{1F600}!");
    mock.reply(OPENAI, cut(&[&chunk, "data: [DONE]\n\n"], 3));
    let gw = gateway(&mock, "openai", Restore(email()))?;
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
    let mock = MockTransport::new();
    let mut reply = mock::sse(&[&chunk("Mail <EMA")]);
    reply.body_mut().push(Err(Fault::upstream("reset")));
    mock.reply(OPENAI, reply);
    let gw = gateway(&mock, "openai", Restore(email()))?;
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
    let call = |arguments: &str| {
        let raw = json!([{ "index": 0, "delta": { "tool_calls": [{ "function": { "arguments": arguments } }] } }]);
        Reply::other("openai", raw)
    };
    let frags = [format!("{{\"to\":\"{ph}"), "\"}".to_owned()];
    let out = run(
        &mut Holdback::new(vault),
        frags.iter().map(|f| call(f)).collect(),
    )
    .unwrap_or_default();
    let joined: String = out
        .iter()
        .filter_map(|r| match &r.content[..] {
            [Piece::Other { raw, .. }] => raw[0]["delta"]["tool_calls"][0]["function"]["arguments"]
                .as_str()
                .map(str::to_owned),
            _ => None,
        })
        .collect();
    let parsed: serde_json::Value = serde_json::from_str(&joined).unwrap_or_default();
    assert_eq!(parsed["to"], "Robert \"Bob\" Smith", "{joined}");
}

// A keep-alive and the usage chunk pass a guard's Holdback in place, so a
// client of the provider's format gets the provider's bytes.
#[tokio::test]
async fn a_keep_alive_and_usage_pass_a_holdback_in_place() -> Result<(), Fault> {
    let chunks = [
        "data: {\"id\":\"c1\",\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi\"}}]}\n\n",
        ": keep-alive\n\n",
        "data: {\"id\":\"c1\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
        "data: {\"id\":\"c1\",\"choices\":[],\"usage\":{\"prompt_tokens\":3,\"completion_tokens\":1}}\n\n",
        "data: [DONE]\n\n",
    ];
    let mock = MockTransport::new();
    mock.reply(OPENAI, mock::sse(&chunks));
    let gw = gateway(&mock, "openai", Guard)?;
    let body = json!({ "model": "m", "stream": true, "stream_options": { "include_usage": true },
        "messages": [{ "role": "user", "content": "hi" }] });
    let req = post("/v1/chat/completions", body.to_string());
    let (status, _, body) = read_whole(serve(&gw, req).await.0).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body?, Bytes::from(chunks.concat()));
    Ok(())
}
