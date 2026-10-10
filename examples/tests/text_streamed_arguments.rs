//! Holdback over a streamed tool call's arguments, as Anthropic's
//! `partial_json` fragments decode: escapes and split matches.

#[path = "../support/text.rs"]
mod text;

use respan_gateway_conversation::Output;
use respan_gateway_core::ErrorCode;
use respan_gateway_core::Fault;
#[path = "support/text.rs"]
mod support;

use serde_json::Value;
use support::{arguments_of, run_filter as run};
use text::{Holdback, Placeholders, Rewrite, partial_match};

/// One Anthropic `input_json_delta` of a streamed tool call, as the format
/// decodes it: the next part of the arguments of the call in block 1.
fn partial(json_text: &str) -> Output {
    Output::arguments(1, json_text)
}

fn joined_partial(out: &[Output]) -> String {
    arguments_of(out, 1)
}

struct Refuse;
impl Rewrite for Refuse {
    fn pending(&self, text: &str) -> usize {
        partial_match(text, ["bomb"])
    }
    fn rewrite(&mut self, text: &str) -> Result<String, Fault> {
        match text.contains("bomb") {
            true => Err(Fault::new("blocked").with(ErrorCode("blocked".into()))),
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

/// Streamed arguments: a backslash split from its pair makes the next
/// fragment's `\b` read as an escape, so `bomb` (whole in the second
/// fragment's bytes after the `\`) is read as `\u{8}omb`.
#[test]
fn a_split_backslash_does_not_hide_a_term_in_streamed_arguments() {
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

/// A term split across two fragments is still refused.
#[test]
fn a_term_split_across_streamed_arguments_is_refused() {
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

/// A placeholder split across fragments is restored.
#[test]
fn a_placeholder_split_across_streamed_arguments_is_restored() {
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
fn an_edit_before_a_split_escape_keeps_streamed_arguments_valid() {
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
