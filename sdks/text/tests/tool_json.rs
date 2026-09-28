//! Restoring and redacting inside tool calls keeps their JSON whole.

use respan_gateway_core::serde_json::{self, Value, json};
use respan_gateway_core::{Fault, Filter};
use respan_gateway_formats::conversation::{Conversation, Message, Piece, Reply, Role};
use respan_gateway_text::{Holdback, Placeholders, Readable};

fn run(filter: &mut impl Filter<Reply>, replies: Vec<Reply>) -> Result<Vec<Reply>, Fault> {
    let mut out = Vec::new();
    for reply in replies {
        filter.edit(reply, &mut out)?;
    }
    filter.end(&Ok(()), &mut out)?;
    Ok(out)
}
fn call(arguments: &str) -> Reply {
    Reply::other(
        "openai",
        json!([{ "index": 0, "tool_calls": [{ "function": { "arguments": arguments } }] }]),
    )
}
fn args(out: &[Reply]) -> String {
    out.iter()
        .flat_map(|r| &r.content)
        .filter_map(|p| match p {
            Piece::Other { raw, .. } => raw[0]["tool_calls"][0]["function"]["arguments"]
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
