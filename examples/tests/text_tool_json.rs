//! Restoring and redacting inside tool calls keeps their JSON whole.

#[path = "../support/text.rs"]
mod text;

use respan_gateway_conversation::{Block, Content, Delta, Input, Message, Output, Role, Tool};
use respan_gateway_sdk::{Fault, Filter};
#[path = "support/text.rs"]
mod support;

use serde_json::{self, Value, json};
use support::{arguments_of, run_filter as run};
use text::{Holdback, Placeholders, Readable, Rewrite, UNREADABLE};

/// The next part of the arguments of the tool call in block 0.
fn call(arguments: &str) -> Output {
    Output::arguments(0, arguments)
}

fn args(out: &[Output]) -> String {
    arguments_of(out, 0)
}

/// A tool call whose arguments come whole, as a body that was not streamed
/// decodes.
fn whole_call(arguments: &str) -> Vec<Output> {
    let kind = Block::ToolCall {
        id: "t".into(),
        name: "f".into(),
    };
    vec![
        Output::block_start(0, kind),
        Output::arguments(0, arguments),
        Output::block_end(0),
    ]
}

#[test]
fn a_value_restored_in_a_whole_tool_call_is_escaped_once() {
    let vault = Placeholders::default();
    let ph = vault.hide("NAME", "Robert \"Bob\" Smith");
    let whole = json!({ "note": format!("[vip] {ph}") }).to_string();
    let out = run(&mut Holdback::new(vault), whole_call(&whole)).unwrap_or_default();
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
    let mut chat = Input::new([Message::new(Role::User, "hi")]);
    let mut tool = Tool::new("mail", json!({ "type": "object" }));
    tool.description = Some("Send to \"a@b.io\" or \"c@d.io\"".into());
    chat.tools.push(tool);
    let r = chat.edit_all(|t| {
        for e in ["a@b.io", "c@d.io"] {
            if t.contains(e) {
                *t = t.replace(e, &vault.hide("EMAIL", e));
            }
        }
        Ok(())
    });
    assert!(r.is_ok(), "{r:?}");
    let description = chat.tools[0].description.clone().unwrap_or_default();
    assert_eq!(description, "Send to \"<EMAIL_1>\" or \"<EMAIL_2>\"");
}

#[test]
fn a_whole_tool_call_with_two_quoted_placeholders_is_restored() {
    let vault = Placeholders::default();
    let a = vault.hide("EMAIL", "a@b.io");
    let b = vault.hide("EMAIL", "c@d.io");
    let whole = json!({ "body": format!("Write \"{a}\" and \"{b}\"") }).to_string();
    let out = run(&mut Holdback::new(vault), whole_call(&whole));
    assert!(out.is_ok(), "{:?}", out.err());
}

/// The first event the kind has no form for in `out`, as `raw`.
fn first_other(out: &[Output]) -> Value {
    out.iter()
        .find_map(|r| match r {
            Output::Native(respan_gateway_sdk::NativeValue { value: raw, .. }) => {
                Some((**raw).clone())
            }
            _ => None,
        })
        .unwrap_or(Value::Null)
}

fn restored(raw_json: Value, vault: Placeholders) -> Value {
    let out = run(
        &mut Holdback::new(vault),
        vec![Output::native("openai-chat", raw_json)],
    )
    .unwrap_or_default();
    first_other(&out)
}

/// The text a reasoning block says, restored.
fn reasoning(text: &str, vault: Placeholders) -> String {
    let events = vec![
        Output::block_start(0, Block::Reasoning),
        Output::reasoning(0, text),
        Output::block_end(0),
    ];
    let out = run(&mut Holdback::new(vault), events).unwrap_or_default();
    out.iter()
        .filter_map(|e| match e {
            Output::Delta {
                delta: Delta::Reasoning(text),
                ..
            } => Some(text.as_str()),
            _ => None,
        })
        .collect()
}

#[test]
fn prose_in_other_content_is_restored_unescaped() {
    let vault = Placeholders::default();
    let name = vault.hide("NAME", "Robert \"Bob\" Smith");
    let refusal = json!({ "choices": [{ "index": 0, "delta": { "refusal": format!("I won't write to {name}.") } }] });
    let v = restored(refusal, vault.clone());
    assert_eq!(
        v["choices"][0]["delta"]["refusal"],
        "I won't write to Robert \"Bob\" Smith."
    );
    let path = vault.hide("PATH", "C:\\Users\\bob");
    let second = json!({ "choices": [{ "index": 0, "delta": { "role": "assistant", "content": format!("See {path}") } }] });
    let v = restored(second, vault);
    assert_eq!(v["choices"][0]["delta"]["content"], "See C:\\Users\\bob");
}

/// Anthropic's `partial_json` pieces decode as deltas of a tool call.
#[test]
fn an_anthropic_partial_json_piece_is_escaped() {
    let vault = Placeholders::default();
    let name = vault.hide("NAME", "Robert \"Bob\" Smith");
    let out = run(&mut Holdback::new(vault), vec![call(&name)]).unwrap_or_default();
    assert_eq!(args(&out), "Robert \\\"Bob\\\" Smith");
}

/// A tool's own parameter named `arguments` is a decoded value, not JSON text.
#[test]
fn a_parameter_named_arguments_is_restored_once() {
    let vault = Placeholders::default();
    let name = vault.hide("NAME", "Robert \"Bob\" Smith");
    let given = json!({ "program": "greet", "arguments": name }).to_string();
    let out = run(&mut Holdback::new(vault.clone()), whole_call(&given)).unwrap_or_default();
    let a: Value = serde_json::from_str(&args(&out)).unwrap_or_default();
    assert_eq!(a["arguments"], "Robert \"Bob\" Smith");
    // An Anthropic request's tool input is a value in its own format's JSON.
    let name = vault.hide("NAME", "Robert \"Bob\" Smith");
    let block =
        json!({ "type": "tool_use", "id": "t", "name": "run", "input": { "arguments": name } });
    let mut chat = Input::new([Message::new(Role::Assistant, "")]);
    chat.messages[0].content = vec![Content::native("anthropic-messages", block)];
    let edited = chat.edit_all(|t| {
        *t = vault.clone().rewrite(t)?;
        Ok(())
    });
    assert!(edited.is_ok());
    let Content::Native(respan_gateway_sdk::NativeValue { value: raw, .. }) =
        &chat.messages[0].content[0]
    else {
        panic!("tool input")
    };
    assert_eq!(raw["input"]["arguments"], "Robert \"Bob\" Smith");
}

/// A request's text after a backslash is read whole: nothing is skipped.
#[test]
fn text_after_a_backslash_in_a_tool_call_is_read() {
    let args = json!({ "arguments": "C:\\dir\\bob@x.io" }).to_string();
    let mut call = Message::new(Role::Assistant, "");
    call.content = vec![Content::tool_call("t", "run", args)];
    let mut chat = Input::new([Message::new(Role::User, "hi"), call]);
    let mut seen = Vec::new();
    let read = chat.edit_all(|t| {
        seen.push(t.clone());
        Ok(())
    });
    assert!(read.is_ok());
    assert!(seen.iter().any(|t| t.contains("bob@x.io")), "{seen:?}");
}

/// OpenAI's older `functions` stream (`delta.function_call.arguments`)
/// decodes as the deltas of a tool call, like any other.
#[test]
fn a_legacy_function_call_fragment_is_escaped() {
    let vault = Placeholders::default();
    let path = vault.hide("PATH", "C:\\Users\\bob");
    let frags = ["{\"dir\":\"".to_owned(), path, "\"}".to_owned()];
    let out = run(
        &mut Holdback::new(vault),
        frags.iter().map(|f| call(f)).collect(),
    )
    .unwrap_or_default();
    let joined = args(&out);
    let parsed: Value = serde_json::from_str(&joined).unwrap_or_default();
    assert_eq!(parsed["dir"], "C:\\Users\\bob", "{joined}");
}

/// A streamed piece that happens to parse is still a piece, not a value.
#[test]
fn a_streamed_piece_that_parses_is_edited_as_text() {
    let mut joined = String::new();
    for f in ["{\"note\":\"ids ", "[4111111111111111]", "\"}"] {
        let mut reply = call(f);
        let edited = reply.edit_all(|t| {
            *t = t.replace("4111111111111111", "[card]");
            Ok(())
        });
        assert!(edited.is_ok());
        if let Output::Delta { delta, .. } = &reply {
            joined.push_str(delta.as_str());
        }
    }
    let parsed: Value = serde_json::from_str(&joined).unwrap_or_default();
    assert_eq!(parsed["note"], "ids [[card]]", "{joined}");
}

/// Remembers every text it is asked to rewrite.
struct Record(std::sync::Arc<std::sync::Mutex<Vec<String>>>);

impl Rewrite for Record {
    fn pending(&self, _text: &str) -> usize {
        0
    }

    fn rewrite(&mut self, text: &str) -> Result<String, Fault> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(text.to_owned());
        Ok(text.to_owned())
    }
}

/// A tool call's whole arguments are read as JSON, numbers included.
#[test]
fn a_tool_calls_numbers_are_read() {
    let seen = std::sync::Arc::default();
    let whole = whole_call("{\"card\":4111111111111111}");
    let out = run(
        &mut Holdback::new(Record(std::sync::Arc::clone(&seen))),
        whole,
    );
    assert!(out.is_ok());
    let seen = seen
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    assert!(seen.iter().any(|t| t == "4111111111111111"), "{seen:?}");
}

/// Streamed reasoning that happens to be whole JSON keeps its own spelling.
#[test]
fn streamed_reasoning_is_plain_text() {
    let vault = Placeholders::default();
    let name = vault.hide("NAME", "bob");
    let text = format!("{{\"a\": \"{name}\",  \"b\": 1.0}}");
    assert_eq!(
        reasoning(&text, vault.clone()),
        "{\"a\": \"bob\",  \"b\": 1.0}"
    );
    let name = vault.hide("NAME", "bob");
    let thinking = format!("[\"{name}\", 1.0]");
    assert_eq!(reasoning(&thinking, vault), "[\"bob\", 1.0]");
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
    use respan_gateway_conversation::Content;
    let mut chat = Input::default();
    let mut message = Message::new(Role::Assistant, "");
    message.content = vec![
        Content::native(
            "anthropic-messages",
            json!({"type":"tool_use","id":"tool-1","name":"lookup","input":{"email":"alice@example.com","nested":[{"note":"alice@example.com"}]}}),
        ),
        Content::native(
            "anthropic-messages",
            json!({"type":"tool_result","tool_use_id":"tool-1","content":[{"type":"text","text":"alice@example.com"}]}),
        ),
    ];
    chat.messages.push(message);
    chat.edit_all(|text| {
        *text = text.replace("alice@example.com", "<EMAIL_1>");
        Ok(())
    })?;
    let Content::Native(respan_gateway_sdk::NativeValue { value: raw, .. }) =
        &chat.messages[0].content[0]
    else {
        panic!("tool input")
    };
    assert_eq!(raw["input"]["email"], "<EMAIL_1>");
    assert_eq!(raw["input"]["nested"][0]["note"], "<EMAIL_1>");
    assert_eq!(raw["id"], "tool-1");
    let Content::Native(respan_gateway_sdk::NativeValue { value: raw, .. }) =
        &chat.messages[0].content[1]
    else {
        panic!("tool result")
    };
    assert_eq!(raw["content"][0]["text"], "<EMAIL_1>");
    assert_eq!(raw["tool_use_id"], "tool-1");
    Ok(())
}

#[test]
fn an_image_inside_a_tool_result_is_refused_before_any_edit() {
    use respan_gateway_conversation::Content;
    let mut chat = Input::default();
    let mut message = Message::new(Role::User, "keep this");
    message.content.push(Content::native("anthropic-messages", json!({
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
fn unknown_request_tool_siblings_refuse_before_any_callback_or_edit() {
    use respan_gateway_conversation::Content;

    for raw in [
        json!({"type":"tool_use","id":"tool-1","name":"lookup","input":{"query":"safe"},
            "vendor_payload":{"image":{"data":"opaque"}}}),
        json!({"type":"tool_result","tool_use_id":"tool-1","content":"safe",
            "vendor_payload":{"audio":{"data":"opaque"}}}),
    ] {
        let mut message = Message::new(Role::User, "keep me");
        message
            .content
            .push(Content::native("anthropic-messages", raw));
        let mut chat = Input::new([message]);
        let before = chat.clone();
        assert_eq!(
            chat.read_all()
                .err()
                .map(|f| respan_gateway_http::error_code(&f).to_owned())
                .as_deref(),
            Some(UNREADABLE)
        );
        let mut calls = 0;
        let result = chat.edit_all(|text| {
            calls += 1;
            text.clear();
            Ok(())
        });
        assert_eq!(
            result
                .err()
                .map(|f| respan_gateway_http::error_code(&f).to_owned())
                .as_deref(),
            Some(UNREADABLE)
        );
        assert_eq!(calls, 0, "no text before an opaque sibling may be edited");
        assert_eq!(chat, before);
    }
}

#[test]
fn tool_indexes_do_not_delay_argument_tails_until_after_the_finish() -> Result<(), Fault> {
    use respan_gateway_conversation::Finish;
    use respan_gateway_sdk::Filter;

    let vault = Placeholders::default();
    vault.hide("NAME", "alice");
    let mut filter = Holdback::new(vault);
    let mut out = Vec::new();
    let kind = Block::ToolCall {
        id: "t".into(),
        name: "f".into(),
    };
    filter.edit(Output::block_start(2, kind), &mut out)?;
    filter.edit(Output::arguments(2, "{\"to\":\"<NA"), &mut out)?;
    filter.edit(Output::finished(Finish::Length), &mut out)?;
    assert_eq!(out.last(), Some(&Output::finished(Finish::Length)));
    assert!(
        arguments_of(&out, 2).contains("<NA"),
        "the tool argument tail must flush before the finish: {out:?}"
    );
    let mut after = Vec::new();
    filter.end(&Ok(()), &mut after)?;
    assert!(after.is_empty(), "arguments emitted after the finish");
    Ok(())
}

/// Replaces the text `42` with `.0`.
struct Replace42(String);

impl Rewrite for Replace42 {
    fn pending(&self, _text: &str) -> usize {
        0
    }

    fn rewrite(&mut self, text: &str) -> Result<String, Fault> {
        Ok(if text == "42" {
            self.0.clone()
        } else {
            text.to_owned()
        })
    }
}

#[test]
fn edited_tool_argument_numbers_are_strings_even_when_replacement_looks_like_json() {
    for replacement in [r#"{"admin":true}"#, "null", "[1,2]", "123"] {
        let mut request = Input::new([Message::new(Role::Assistant, "")]);
        request.messages[0].content =
            vec![Content::tool_call("t", "f", r#"{"n":42,"unchanged":7}"#)];
        request
            .edit_all(|text| {
                if text == "42" {
                    *text = replacement.to_owned();
                }
                Ok(())
            })
            .expect("request edit");
        let Content::ToolCall { arguments, .. } = &request.messages[0].content[0] else {
            panic!("tool call")
        };
        let parsed: Value = serde_json::from_str(arguments).expect("valid request arguments");
        assert_eq!(parsed["n"], json!(replacement));
        assert_eq!(parsed["unchanged"], json!(7));

        // A response's tool call, in either format, is the same events.
        let whole = whole_call(r#"{"n":42,"unchanged":7}"#);
        let out =
            run(&mut Holdback::new(Replace42(replacement.to_owned())), whole).expect("reply edit");
        let parsed: Value = serde_json::from_str(&args(&out)).expect("valid reply arguments");
        assert_eq!(parsed["n"], json!(replacement));
        assert_eq!(parsed["unchanged"], json!(7));
    }
}

#[test]
fn unreadable_reply_content_refuses_before_editing_any_text() {
    for (format, opaque) in [
        (
            "openai-chat",
            json!({"choices": [{"index": 0, "delta": {"audio": {"data": "opaque"}}}]}),
        ),
        (
            "anthropic-messages",
            json!({"type": "image", "source": {"data": "opaque"}}),
        ),
        ("openai-chat", json!({"unknown": "opaque"})),
        (
            "openai-chat",
            json!({"choices": [{"index": 0, "delta": {"tool_calls": [
                {"function": {"name": "f", "arguments": "{}", "audio": {"data": "opaque"}}}
            ]}}]}),
        ),
        (
            "anthropic-messages",
            json!({"type": "tool_result", "content": [
                {"type": "text", "text": "hello", "image": {"source": "opaque"}}
            ]}),
        ),
    ] {
        let mut reply = Output::native(format, opaque);
        let before = reply.clone();
        let mut calls = 0;
        let result = reply.edit_all(|text| {
            calls += 1;
            text.clear();
            Ok(())
        });
        assert_eq!(
            result
                .err()
                .map(|f| respan_gateway_http::error_code(&f).to_owned())
                .as_deref(),
            Some(UNREADABLE)
        );
        assert_eq!(calls, 0, "the callback must not see partial content");
        assert_eq!(reply, before);
    }
}

#[test]
fn holdback_refuses_unreadable_reply_before_rewriting_or_emitting_text() {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    struct Count(Arc<AtomicUsize>);
    impl Rewrite for Count {
        fn pending(&self, _text: &str) -> usize {
            0
        }
        fn rewrite(&mut self, text: &str) -> Result<String, Fault> {
            self.0.fetch_add(1, Ordering::Relaxed);
            Ok(text.to_owned())
        }
    }
    for (format, opaque) in [
        (
            "openai-chat",
            json!({"choices": [{"index": 0, "delta": {"content": [{"type": "image_url", "image_url": {"url": "data:,"}}]}}]}),
        ),
        (
            "anthropic-messages",
            json!({"type": "audio", "data": "opaque"}),
        ),
    ] {
        let calls = Arc::new(AtomicUsize::new(0));
        let mut filter = Holdback::new(Count(calls.clone()));
        let mut out = Vec::new();
        let result = filter.edit(Output::native(format, opaque), &mut out);
        assert_eq!(
            result
                .err()
                .map(|f| respan_gateway_http::error_code(&f).to_owned())
                .as_deref(),
            Some(UNREADABLE)
        );
        assert!(out.is_empty(), "nothing from the rejected event is emitted");
        assert_eq!(calls.load(Ordering::Relaxed), 0);
    }
}
