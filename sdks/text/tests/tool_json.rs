//! Restoring and redacting inside tool calls keeps their JSON whole.

use respan_gateway_core::Fault;
use respan_gateway_formats::conversation::{Conversation, Message, Piece, Reply, Role};
use respan_gateway_testkit::run_filter as run;
use respan_gateway_text::{Holdback, Placeholders, Readable, Rewrite};
use serde_json::{self, Value, json};

fn call(arguments: &str) -> Reply {
    Reply::other(
        "openai",
        json!([{ "index": 0, "delta": { "tool_calls": [{ "function": { "arguments": arguments } }] } }]),
    )
}
fn args(out: &[Reply]) -> String {
    out.iter()
        .flat_map(|r| &r.content)
        .filter_map(|p| match p {
            Piece::Other { raw, .. } => raw[0]["delta"]["tool_calls"][0]["function"]["arguments"]
                .as_str()
                .map(str::to_owned),
            _ => None,
        })
        .collect()
}

#[test]
fn a_value_restored_in_a_whole_tool_call_is_escaped_once() {
    let vault = Placeholders::default();
    let ph = vault.hide("NAME", "Robert \"Bob\" Smith");
    let whole = json!({ "note": format!("[vip] {ph}") }).to_string();
    let out = run(&mut Holdback::new(vault), vec![call(&whole)]).unwrap_or_default();
    let parsed: Value = serde_json::from_str(&args(&out)).unwrap_or_default();
    assert_eq!(
        parsed["note"],
        "[vip] Robert \"Bob\" Smith",
        "{}",
        args(&out)
    );
}

#[test]
fn a_fragment_that_is_only_a_placeholder_is_escaped() {
    let vault = Placeholders::default();
    let ph = vault.hide("NAME", "Robert \"Bob\" Smith");
    let frags = ["{\"to\":\"".to_owned(), ph, "\"}".to_owned()];
    let out = run(
        &mut Holdback::new(vault),
        frags.iter().map(|f| call(f)).collect(),
    )
    .unwrap_or_default();
    let joined = args(&out);
    let parsed: Value = serde_json::from_str(&joined).unwrap_or_default();
    assert_eq!(parsed["to"], "Robert \"Bob\" Smith", "{joined}");
}

#[test]
fn a_tool_description_with_two_quoted_values_is_edited() {
    let vault = Placeholders::default();
    let mut chat = Conversation::new([Message::new(Role::User, "hi")]);
    chat.unsupported.insert(
        "tools".into(),
        json!([{ "type": "function", "function": {
        "name": "mail", "description": "Send to \"a@b.io\" or \"c@d.io\"" } }]),
    );
    let r = chat.edit_all(|t| {
        for e in ["a@b.io", "c@d.io"] {
            if t.contains(e) {
                *t = t.replace(e, &vault.hide("EMAIL", e));
            }
        }
        Ok(())
    });
    assert!(r.is_ok(), "{r:?}");
}

#[test]
fn a_whole_tool_call_with_two_quoted_placeholders_is_restored() {
    let vault = Placeholders::default();
    let a = vault.hide("EMAIL", "a@b.io");
    let b = vault.hide("EMAIL", "c@d.io");
    let whole = json!({ "body": format!("Write \"{a}\" and \"{b}\"") }).to_string();
    let out = run(&mut Holdback::new(vault), vec![call(&whole)]);
    assert!(out.is_ok(), "{:?}", out.err());
}

fn restored(raw_json: Value, vault: Placeholders) -> Value {
    let out = run(
        &mut Holdback::new(vault),
        vec![Reply::other("openai", raw_json)],
    )
    .unwrap_or_default();
    match out.iter().flat_map(|r| &r.content).next() {
        Some(Piece::Other { raw, .. }) => (**raw).clone(),
        _ => Value::Null,
    }
}

#[test]
fn prose_in_other_content_is_restored_unescaped() {
    let vault = Placeholders::default();
    let name = vault.hide("NAME", "Robert \"Bob\" Smith");
    let refusal =
        json!([{ "index": 0, "message": { "refusal": format!("I won't write to {name}.") } }]);
    let v = restored(refusal, vault.clone());
    assert_eq!(
        v[0]["message"]["refusal"],
        "I won't write to Robert \"Bob\" Smith."
    );
    let path = vault.hide("PATH", "C:\\Users\\bob");
    let second = json!([{ "index": 1, "message": { "role": "assistant", "content": format!("See {path}") } }]);
    let v = restored(second, vault);
    assert_eq!(v[0]["message"]["content"], "See C:\\Users\\bob");
}

#[test]
fn an_anthropic_partial_json_piece_is_escaped() {
    let vault = Placeholders::default();
    let name = vault.hide("NAME", "Robert \"Bob\" Smith");
    let delta = json!({ "type": "input_json_delta", "partial_json": name });
    let v = restored(delta, vault);
    assert_eq!(v["partial_json"], "Robert \\\"Bob\\\" Smith");
}

/// A tool's own parameter named `arguments` is a decoded value, not JSON text.
#[test]
fn a_parameter_named_arguments_is_restored_once() {
    let vault = Placeholders::default();
    let name = vault.hide("NAME", "Robert \"Bob\" Smith");
    let args = json!({ "program": "greet", "arguments": name }).to_string();
    let raw = json!([{ "index": 0, "message": { "tool_calls": [{ "id": "t", "type": "function",
        "function": { "name": "run", "arguments": args } }] } }]);
    let v = restored(raw, vault.clone());
    let text = v[0]["message"]["tool_calls"][0]["function"]["arguments"].as_str();
    let a: Value = serde_json::from_str(text.unwrap_or_default()).unwrap_or_default();
    assert_eq!(a["arguments"], "Robert \"Bob\" Smith");
    let name = vault.hide("NAME", "Robert \"Bob\" Smith");
    let block =
        json!({ "type": "tool_use", "id": "t", "name": "run", "input": { "arguments": name } });
    assert_eq!(
        restored(block, vault)["input"]["arguments"],
        "Robert \"Bob\" Smith"
    );
}

/// A request's text after a backslash is read whole: nothing is skipped.
#[test]
fn text_after_a_backslash_in_a_tool_call_is_read() {
    let mut call = Message::new(Role::Assistant, "");
    call.content.clear();
    let args = json!({ "arguments": "C:\\dir\\bob@x.io" }).to_string();
    call.unsupported.insert(
        "tool_calls".into(),
        json!([{ "id": "t", "type": "function",
        "function": { "name": "run", "arguments": args } }]),
    );
    let mut chat = Conversation::new([Message::new(Role::User, "hi"), call]);
    let mut seen = Vec::new();
    let read = chat.edit_all(|t| {
        seen.push(t.clone());
        Ok(())
    });
    assert!(read.is_ok());
    assert!(seen.iter().any(|t| t.contains("bob@x.io")), "{seen:?}");
}

/// OpenAI's older `functions` stream: `delta.function_call.arguments`.
#[test]
fn a_legacy_function_call_fragment_is_escaped() {
    let vault = Placeholders::default();
    let path = vault.hide("PATH", "C:\\Users\\bob");
    let frags = ["{\"dir\":\"".to_owned(), path, "\"}".to_owned()];
    let delta =
        |f: &String| json!([{ "index": 0, "delta": { "function_call": { "arguments": f } } }]);
    let out = run(
        &mut Holdback::new(vault),
        frags
            .iter()
            .map(|f| Reply::other("openai", delta(f)))
            .collect(),
    )
    .unwrap_or_default();
    let joined: String = out
        .iter()
        .flat_map(|r| &r.content)
        .filter_map(|p| match p {
            Piece::Other { raw, .. } => raw[0]["delta"]["function_call"]["arguments"]
                .as_str()
                .map(str::to_owned),
            _ => None,
        })
        .collect();
    let parsed: Value = serde_json::from_str(&joined).unwrap_or_default();
    assert_eq!(parsed["dir"], "C:\\Users\\bob", "{joined}");
}

/// A streamed piece that happens to parse is still a piece, not a value.
#[test]
fn a_streamed_piece_that_parses_is_edited_as_text() {
    let mut joined = String::new();
    for f in ["{\"note\":\"ids ", "[4111111111111111]", "\"}"] {
        let raw = json!([{ "index": 0, "delta": { "tool_calls": [{ "index": 0, "function": { "arguments": f } }] } }]);
        let mut reply = Reply::other("openai", raw);
        let edited = reply.edit_all(|t| {
            *t = t.replace("4111111111111111", "[card]");
            Ok(())
        });
        assert!(edited.is_ok());
        if let Some(Piece::Other { raw, .. }) = reply.content.first() {
            let piece = raw[0]["delta"]["tool_calls"][0]["function"]["arguments"].as_str();
            joined.push_str(piece.unwrap_or_default());
        }
    }
    let parsed: Value = serde_json::from_str(&joined).unwrap_or_default();
    assert_eq!(parsed["note"], "ids [[card]]", "{joined}");
}

/// A reply's tool call is read alike in both formats, numbers included.
#[test]
fn a_tool_calls_numbers_are_read_in_both_formats() {
    let anthropic = json!({ "type": "tool_use", "id": "t", "name": "pay", "input": { "card": 4111111111111111u64 } });
    let openai = json!([{ "index": 0, "message": { "tool_calls": [{ "function": {
        "name": "pay", "arguments": "{\"card\":4111111111111111}" } }] } }]);
    for (format, raw) in [("anthropic", anthropic), ("openai", openai)] {
        let all = Reply::other(format, raw).read_all().unwrap_or_default();
        assert!(all.contains("4111111111111111"), "{format}: {all:?}");
    }
}

/// Streamed reasoning that happens to be whole JSON keeps its own spelling.
#[test]
fn streamed_reasoning_is_plain_text() {
    let vault = Placeholders::default();
    let name = vault.hide("NAME", "bob");
    let text = format!("{{\"a\": \"{name}\",  \"b\": 1.0}}");
    let openai = json!([{ "index": 0, "delta": { "reasoning_content": text } }]);
    let v = restored(openai, vault.clone());
    assert_eq!(
        v[0]["delta"]["reasoning_content"],
        "{\"a\": \"bob\",  \"b\": 1.0}"
    );
    let name = vault.hide("NAME", "bob");
    let thinking = json!({ "type": "thinking_delta", "thinking": format!("[\"{name}\", 1.0]") });
    assert_eq!(restored(thinking, vault)["thinking"], "[\"bob\", 1.0]");
}

/// Masks every run of four or more digits, as a card or phone redactor would.
struct MaskDigits;

impl Rewrite for MaskDigits {
    fn pending(&self, text: &str) -> usize {
        text.len() - text.trim_end_matches(|c: char| c.is_ascii_digit()).len()
    }

    fn rewrite(&mut self, text: &str) -> Result<String, Fault> {
        let (mut out, mut run) = (String::new(), String::new());
        for c in text.chars().map(Some).chain([None]) {
            match c {
                Some(d) if d.is_ascii_digit() => run.push(d),
                _ => {
                    out.push_str(if run.len() >= 4 { "****" } else { &run });
                    run.clear();
                    out.extend(c);
                }
            }
        }
        Ok(out)
    }
}

#[test]
fn a_unicode_escape_in_streamed_arguments_is_kept_whole() {
    let piece = "{\"note\":\"don\\u2019t call 5551234\"}";
    let out = run(&mut Holdback::new(MaskDigits), vec![call(piece)]).unwrap_or_default();
    let parsed: Value = serde_json::from_str(&args(&out)).unwrap_or_default();
    assert_eq!(parsed["note"], "don\u{2019}t call ****", "{}", args(&out));
}

#[test]
fn anthropic_request_tools_are_read_and_edited_without_changing_link_ids() -> Result<(), Fault> {
    use respan_gateway_formats::conversation::Content;
    let mut chat = Conversation::default();
    let mut message = Message::new(Role::Assistant, "");
    message.content = vec![
        Content::other(
            "anthropic-messages",
            json!({"type":"tool_use","id":"tool-1","name":"lookup","input":{"email":"alice@example.com","nested":[{"note":"alice@example.com"}]}}),
        ),
        Content::other(
            "anthropic-messages",
            json!({"type":"tool_result","tool_use_id":"tool-1","content":[{"type":"text","text":"alice@example.com"}]}),
        ),
    ];
    chat.messages.push(message);
    chat.edit_all(|text| {
        *text = text.replace("alice@example.com", "<EMAIL_1>");
        Ok(())
    })?;
    let Content::Other { raw, .. } = &chat.messages[0].content[0] else {
        panic!("tool input")
    };
    assert_eq!(raw["input"]["email"], "<EMAIL_1>");
    assert_eq!(raw["input"]["nested"][0]["note"], "<EMAIL_1>");
    assert_eq!(raw["id"], "tool-1");
    let Content::Other { raw, .. } = &chat.messages[0].content[1] else {
        panic!("tool result")
    };
    assert_eq!(raw["content"][0]["text"], "<EMAIL_1>");
    assert_eq!(raw["tool_use_id"], "tool-1");
    Ok(())
}

#[test]
fn an_image_inside_a_tool_result_is_refused_before_any_edit() {
    use respan_gateway_formats::conversation::Content;
    let mut chat = Conversation::default();
    let mut message = Message::new(Role::User, "keep this");
    message.content.push(Content::other("anthropic-messages", json!({
        "type":"tool_result","tool_use_id":"tool-1","content":[{"type":"image","source":{"type":"base64","data":"opaque"}}]
    })));
    chat.messages.push(message);
    let before = chat.clone();
    let result = chat.edit_all(|text| {
        text.clear();
        Ok(())
    });
    assert!(result.is_err());
    assert_eq!(chat, before);
}

#[test]
fn tool_indexes_do_not_delay_argument_tails_until_after_the_finish() -> Result<(), Fault> {
    use respan_gateway_core::Filter;
    use respan_gateway_formats::conversation::Finish;

    for (format, raw) in [
        (
            "anthropic-messages",
            json!({"type":"content_block_delta","index":2,"delta":{"type":"input_json_delta","partial_json":"{\"to\":\"<NA"}}),
        ),
        (
            "openai",
            json!([{"delta":{"tool_calls":[{"index":2,"function":{"arguments":"{\"to\":\"<NA"}}]}}]),
        ),
    ] {
        let vault = Placeholders::default();
        vault.hide("NAME", "alice");
        let mut filter = Holdback::new(vault);
        let mut out = Vec::new();
        filter.edit(Reply::other(format, raw), &mut out)?;
        filter.edit(Reply::finished(Finish::Length), &mut out)?;
        let finished = out.last().expect("finish is forwarded");
        assert!(finished.finish.is_some());
        assert!(
            finished.content.iter().any(|piece| match piece {
                Piece::Other { raw, .. } => raw.to_string().contains("<NA"),
                _ => false,
            }),
            "{format} must flush the tool argument tail with the finish"
        );
        let mut after = Vec::new();
        filter.end(&Ok(()), &mut after)?;
        assert!(
            after.is_empty(),
            "{format} emitted arguments after the finish"
        );
    }
    Ok(())
}
