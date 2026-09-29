//! Holdback over Anthropic `partial_json` fragments: escapes and split matches.

use respan_gateway_core::Fault;
use respan_gateway_formats::conversation::{Piece, Reply};
use respan_gateway_testkit::run_filter as run;
use respan_gateway_text::{Holdback, Placeholders, Rewrite, partial_match};
use serde_json::{Value, json};

/// One Anthropic `input_json_delta` of a streamed tool call, as the format
/// decodes it.
fn partial(json_text: &str) -> Reply {
    Reply::other(
        "anthropic-messages",
        json!({ "type": "input_json_delta", "partial_json": json_text }),
    )
}

fn joined_partial(out: &[Reply]) -> String {
    out.iter()
        .flat_map(|r| &r.content)
        .filter_map(|p| match p {
            Piece::Other { raw, .. } => raw["partial_json"].as_str().map(str::to_owned),
            _ => None,
        })
        .collect()
}

struct Refuse;
impl Rewrite for Refuse {
    fn pending(&self, text: &str) -> usize {
        partial_match(text, ["bomb"])
    }
    fn rewrite(&mut self, text: &str) -> Result<String, Fault> {
        match text.contains("bomb") {
            true => Err(Fault::new("blocked", "blocked")),
            false => Ok(text.to_owned()),
        }
    }
}

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

/// Anthropic's streamed arguments: a backslash split from its pair makes the
/// next fragment's `\b` read as an escape, so `bomb` (whole in the second
/// fragment's bytes after the `\`) is read as `\u{8}omb`. The loop-2 fix
/// covers OpenAI's arguments only.
#[test]
fn a_split_backslash_hides_a_term_in_anthropic_partial_json() {
    let unsplit = run(
        &mut Holdback::new(Refuse),
        vec![partial("{\"s\":\"a\\\\bomb\"}")],
    );
    assert!(unsplit.is_err(), "control");
    let split = run(
        &mut Holdback::new(Refuse),
        vec![partial("{\"s\":\"a\\"), partial("\\bomb\"}")],
    );
    assert!(
        split.is_err(),
        "split passed: {:?}",
        split.map(|o| joined_partial(&o))
    );
}

/// A term split across two Anthropic fragments passes a refusing check.
#[test]
fn a_term_split_across_anthropic_partial_json_passes_a_check() {
    let split = run(
        &mut Holdback::new(Refuse),
        vec![partial("{\"s\":\"bo"), partial("mb\"}")],
    );
    assert!(
        split.is_err(),
        "split passed: {:?}",
        split.map(|o| joined_partial(&o))
    );
}

/// A placeholder split across Anthropic fragments is not restored.
#[test]
fn a_placeholder_split_across_anthropic_partial_json_is_restored() {
    let vault = Placeholders::default();
    let ph = vault.hide("EMAIL", "a@x.io");
    let (a, b) = ph.split_at(4);
    let out = run(
        &mut Holdback::new(vault),
        vec![
            partial(&format!("{{\"to\":\"{a}")),
            partial(&format!("{b}\"}}")),
        ],
    )
    .unwrap_or_default();
    let joined = joined_partial(&out);
    let v: Value = serde_json::from_str(&joined).unwrap_or_default();
    assert_eq!(v["to"], "a@x.io", "{joined}");
}

/// A masked number before a split escaped quote: the edit drops the
/// fragment's trailing `\`, so the next fragment's `"` ends the string and
/// the arguments are no longer JSON.
#[test]
fn an_edit_before_a_split_escape_keeps_anthropic_partial_json_valid() {
    let out = run(
        &mut Holdback::new(MaskDigits),
        vec![
            partial("{\"n\":\"card 4111111111111111\\"),
            partial("\"x\"}"),
        ],
    )
    .unwrap_or_default();
    let joined = joined_partial(&out);
    assert!(
        serde_json::from_str::<Value>(&joined).is_ok(),
        "invalid JSON: {joined}"
    );
}
