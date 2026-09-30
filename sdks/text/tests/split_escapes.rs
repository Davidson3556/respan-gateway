//! Loop 2 adversarial tests (fork B).

use respan_gateway_messages::conversation::{Finish, Piece, Reply};
use respan_gateway_testkit::run_filter as run;
use respan_gateway_text::{Holdback, Placeholders};
use serde_json::{Value, json};

fn call_at(index: u64, arguments: &str) -> Reply {
    Reply::other(
        "openai",
        json!([{ "index": 0, "delta": { "tool_calls": [{ "index": index, "function": { "arguments": arguments } }] } }]),
    )
}

fn args_of(out: &[Reply], index: u64) -> String {
    let mut s = String::new();
    for p in out.iter().flat_map(|r| &r.content) {
        if let Piece::Other { raw, .. } = p {
            for c in raw[0]["delta"]["tool_calls"]
                .as_array()
                .into_iter()
                .flatten()
            {
                if c["index"].as_u64().unwrap_or(0) == index {
                    s.push_str(c["function"]["arguments"].as_str().unwrap_or_default());
                }
            }
        }
    }
    s
}

/// JSON `"C:\\<NAME_1>"`: a literal backslash, then the placeholder. The
/// vendor splits between the two characters of the `\\` escape.
#[test]
fn a_split_backslash_escape_before_a_placeholder_is_restored() {
    let vault = Placeholders::default();
    let ph = vault.hide("NAME", "bob");
    let frags = ["{\"p\":\"C:\\".to_owned(), format!("\\{ph}\"}}")];
    let out = run(
        &mut Holdback::new(vault),
        frags.iter().map(|f| call_at(0, f)).collect(),
    )
    .unwrap_or_default();
    let joined = args_of(&out, 0);
    let parsed: Value = serde_json::from_str(&joined).unwrap_or_default();
    assert_eq!(parsed["p"], "C:\\bob", "{joined}");
}

/// Unsplit, the same text is restored: the split alone breaks it.
#[test]
fn control_unsplit_backslash_before_a_placeholder_is_restored() {
    let vault = Placeholders::default();
    let ph = vault.hide("NAME", "bob");
    let whole = format!("{{\"p\":\"C:\\\\{ph}\"}}");
    let out = run(&mut Holdback::new(vault), vec![call_at(0, &whole)]).unwrap_or_default();
    let joined = args_of(&out, 0);
    let parsed: Value = serde_json::from_str(&joined).unwrap_or_default();
    assert_eq!(parsed["p"], "C:\\bob", "{joined}");
}

/// A split escape before a quote: `\` then `"<NAME_1>` inside a string, i.e.
/// JSON text `"say \"<NAME_1>\""`. After the split the second fragment's
/// `"` is really escaped, so the value restored after it must be escaped.
#[test]
fn a_split_escaped_quote_then_placeholder_keeps_json_valid() {
    let vault = Placeholders::default();
    let ph = vault.hide("NAME", "a\"b");
    let frags = ["{\"s\":\"say \\".to_owned(), format!("\"{ph}\\\"\"}}")];
    let out = run(
        &mut Holdback::new(vault),
        frags.iter().map(|f| call_at(0, f)).collect(),
    )
    .unwrap_or_default();
    let joined = args_of(&out, 0);
    let parsed: Value = serde_json::from_str(&joined).unwrap_or_default();
    assert_eq!(parsed["s"], "say \"a\"b\"", "{joined}");
}

/// Two calls interleaved, each with a placeholder split across fragments.
#[test]
fn two_interleaved_calls_with_split_placeholders_are_restored() {
    let vault = Placeholders::default();
    let a = vault.hide("EMAIL", "a@x.io");
    let b = vault.hide("EMAIL", "b@x.io");
    let (a1, a2) = a.split_at(4);
    let (b1, b2) = b.split_at(3);
    let replies = vec![
        call_at(0, &format!("{{\"to\":\"{a1}")),
        call_at(1, &format!("{{\"to\":\"{b1}")),
        call_at(0, &format!("{a2}\"}}")),
        call_at(1, &format!("{b2}\"}}")),
    ];
    let out = run(&mut Holdback::new(vault), replies).unwrap_or_default();
    let (ja, jb) = (args_of(&out, 0), args_of(&out, 1));
    let pa: Value = serde_json::from_str(&ja).unwrap_or_default();
    let pb: Value = serde_json::from_str(&jb).unwrap_or_default();
    assert_eq!(
        (pa["to"].clone(), pb["to"].clone()),
        (json!("a@x.io"), json!("b@x.io")),
        "{ja} {jb}"
    );
}

/// A placeholder held at the end of a truncated stream (no finish) is
/// released at `end` restored, in a chunk of the same call.
#[test]
fn a_tail_at_the_end_is_released_restored() {
    let vault = Placeholders::default();
    let ph = vault.hide("EMAIL", "a@x.io");
    let partial = &ph[..ph.len() - 1];
    let out = run(
        &mut Holdback::new(vault),
        vec![call_at(0, &format!("{{\"to\":\"{partial}"))],
    )
    .unwrap_or_default();
    let joined = args_of(&out, 0);
    assert!(joined.ends_with(partial), "{joined}");
}

/// A tail held at a finish reply: the finish reply carries it.
#[test]
fn a_tail_at_a_finish_is_released_with_its_index() {
    let vault = Placeholders::default();
    let ph = vault.hide("EMAIL", "a@x.io");
    let (p1, p2) = ph.split_at(4);
    let mut finish = Reply::finished(Finish::ToolUse);
    finish.content.clear();
    let out = run(
        &mut Holdback::new(vault),
        vec![
            call_at(2, &format!("{{\"to\":\"{p1}")),
            call_at(2, p2),
            finish,
        ],
    )
    .unwrap_or_default();
    let joined = args_of(&out, 2);
    assert_eq!(joined, "{\"to\":\"a@x.io", "{out:?}");
}

/// A whole (not streamed) reply's arguments that are not valid JSON (a
/// truncated call, finish `length`): the restored value is written into JSON
/// text unescaped.
#[test]
fn a_restore_into_malformed_whole_arguments_stays_escaped() {
    let vault = Placeholders::default();
    let ph = vault.hide("NAME", "x\",\"admin\":true,\"y\":\"");
    let args = format!("{{\"name\":\"{ph}\",\"note\":\"trunc");
    let raw = json!([{ "index": 0, "message": { "tool_calls": [{ "id": "t", "type": "function",
        "function": { "name": "f", "arguments": args } }] } }]);
    let out = run(&mut Holdback::new(vault), vec![Reply::other("openai", raw)]).unwrap_or_default();
    let text = match out.iter().flat_map(|r| &r.content).next() {
        Some(Piece::Other { raw, .. }) => {
            raw[0]["message"]["tool_calls"][0]["function"]["arguments"]
                .as_str()
                .unwrap_or_default()
                .to_owned()
        }
        _ => String::new(),
    };
    assert!(!text.contains("\"admin\":true"), "injected: {text}");
}

/// A refusing check (as a guardrail's): refuses any text containing `bomb`.
struct Refuse;
impl respan_gateway_text::Rewrite for Refuse {
    fn pending(&self, text: &str) -> usize {
        respan_gateway_text::partial_match(text, ["bomb"])
    }
    fn rewrite(&mut self, text: &str) -> Result<String, respan_gateway_core::Fault> {
        if text.contains("bomb") {
            Err(respan_gateway_core::Fault::new("blocked", "blocked"))
        } else {
            Ok(text.to_owned())
        }
    }
}

/// JSON text `"a\\bomb"` (a backslash, then `bomb`), split inside the `\\`.
/// Unsplit it is refused; split, the second fragment's `\b` is taken for an
/// escape and the check sees `omb`.
#[test]
fn a_split_backslash_hides_a_term_from_a_refusing_check() {
    let unsplit = run(
        &mut Holdback::new(Refuse),
        vec![call_at(0, "{\"s\":\"a\\\\bomb\"}")],
    );
    assert!(unsplit.is_err(), "control: unsplit is refused");
    let split = run(
        &mut Holdback::new(Refuse),
        vec![call_at(0, "{\"s\":\"a\\"), call_at(0, "\\bomb\"}")],
    );
    assert!(
        split.is_err(),
        "split passed: {:?}",
        split.map(|o| args_of(&o, 0))
    );
}

struct MaskDigits;
impl respan_gateway_text::Rewrite for MaskDigits {
    fn pending(&self, text: &str) -> usize {
        text.len() - text.trim_end_matches(|c: char| c.is_ascii_digit()).len()
    }
    fn rewrite(&mut self, text: &str) -> Result<String, respan_gateway_core::Fault> {
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

/// `’` split after `\u`: the digits are rewritten as text, breaking the
/// JSON (unsplit, the same escape is kept whole).
#[test]
fn a_split_unicode_escape_stays_valid_json() {
    let out = run(
        &mut Holdback::new(MaskDigits),
        vec![call_at(0, "{\"n\":\"don\\u"), call_at(0, "2019t\"}")],
    )
    .unwrap_or_default();
    let joined = args_of(&out, 0);
    assert!(
        serde_json::from_str::<Value>(&joined).is_ok(),
        "invalid JSON: {joined}"
    );
}

/// A term spelled with a JSON escape (`\u0062omb` is `bomb`) in one fragment.
#[test]
fn an_escaped_letter_hides_a_term_from_a_refusing_check() {
    let out = run(
        &mut Holdback::new(Refuse),
        vec![call_at(0, "{\"s\":\"\\u0062omb\"}")],
    );
    assert!(out.is_err(), "passed: {:?}", out.map(|o| args_of(&o, 0)));
}

/// An astral character spelled as a surrogate pair, split between its two
/// escapes and inside the second, comes out as the one character.
#[test]
fn a_split_surrogate_pair_is_one_character() {
    let vault = Placeholders::default();
    let ph = vault.hide("NAME", "bob");
    let frags = ["{\"s\":\"\\ud83d", "\\ude", "00 ", &ph, "\"}"];
    let calls = frags.iter().map(|f| call_at(0, f)).collect();
    let out = run(&mut Holdback::new(vault), calls).unwrap_or_default();
    let joined = args_of(&out, 0);
    let parsed: Value = serde_json::from_str(&joined).unwrap_or_default();
    assert_eq!(parsed["s"], "\u{1F600} bob", "{joined}");
}
