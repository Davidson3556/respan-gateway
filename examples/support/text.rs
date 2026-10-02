#![warn(missing_docs)]
// Each example selects the helpers it needs from this private source module.
#![allow(dead_code)]
//! Read and rewrite the chat kind's text safely across stream deltas; refuse
//! what cannot be read.
//!
//! - [`Readable`]: all the text of a [`Conversation`] or a [`Reply`], tools
//!   and tool calls included, to read or edit, or a refusal if it carries
//!   content with no text form, such as an image.
//! - [`Holdback`]: a [`Reply`] filter that holds back trailing text that may
//!   start a match, so a [`Rewrite`] sees each match whole, in text and in a
//!   streamed tool call's arguments.
//! - [`Placeholders`]: hide values behind `<LABEL_n>` and restore them, a
//!   placeholder split across deltas included.

use respan_gateway_sdk::ErrorCode;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, PoisonError};

use respan_gateway_http::Status;
use respan_gateway_http::http::StatusCode;
use respan_gateway_sdk::conversation::{Content, Conversation, Piece, Reply, ToolChoice};
use respan_gateway_sdk::{Fault, Filter};
use serde_json::{self, Map, Value, json};

/// The code of a refusal of content that cannot be read.
pub const UNREADABLE: &str = "unreadable_content";

fn unreadable(what: &str) -> Fault {
    let message = format!("the gateway cannot read {what}, so it refuses it");
    Fault::new(message)
        .with(ErrorCode(UNREADABLE.into()))
        .with(Status(StatusCode::UNPROCESSABLE_ENTITY.as_u16()))
}

/// An edit of one text.
type Edit<'a> = dyn FnMut(&mut String) -> Result<(), Fault> + 'a;

/// A value whose text a check must see all of. Its texts are:
///
/// - its text parts, and the strings and names inside the JSON the kind
///   has no form for (tools, a tool call), so an edit may rename a key;
/// - inside a string holding a JSON object or array, such as a tool call's
///   arguments, each string, name and number (so a card number is read; a
///   changed number is written back as the edit's text);
/// - in a reply's streamed delta, a tool call's arguments as pieces of JSON
///   text, read as the text they stand for (`\u0062` is `b`): what an edit
///   writes is escaped, but a bare number there is not quoted; the delta's
///   other strings as plain text;
/// - a whole tool call's arguments that do not parse (cut off by a token
///   limit) as that JSON text, not as one text, so a value an edit writes
///   into them cannot add a key.
///
/// Booleans, and the request's own numbers and field names, are not texts.
pub trait Readable: Clone {
    /// Runs `edit` on each of its texts, or `Err` with [`UNREADABLE`],
    /// before any edit, if it carries content with no text form, such as an
    /// image.
    fn edit_all(&mut self, edit: impl FnMut(&mut String) -> Result<(), Fault>)
    -> Result<(), Fault>;

    /// All its texts, one per line, or `Err` as
    /// [`edit_all`](Readable::edit_all).
    fn read_all(&self) -> Result<String, Fault> {
        let mut all = String::new();
        self.clone().edit_all(|text| {
            all.push_str(text);
            all.push('\n');
            Ok(())
        })?;
        Ok(all)
    }
}

/// Runs `edit` on every string and name in `v`, and on every number if
/// `numbers`.
fn edit_json(v: &mut Value, numbers: bool, edit: &mut Edit) -> Result<(), Fault> {
    match v {
        Value::String(s) => edit_string(s, edit),
        Value::Number(n) if numbers => {
            let mut text = n.to_string();
            edit(&mut text)?;
            if text != n.to_string() {
                *v = Value::String(text);
            }
            Ok(())
        }
        Value::Array(a) => a.iter_mut().try_for_each(|v| edit_json(v, numbers, edit)),
        Value::Object(o) => {
            for (mut name, mut value) in std::mem::take(o) {
                edit(&mut name)?;
                edit_json(&mut value, numbers, edit)?;
                o.insert(name, value);
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

/// Runs `edit` on content a format kept as it came. In a stream's delta
/// (OpenAI's `delta`, an Anthropic `*_delta` event), a tool call's
/// arguments (`function.arguments`, `function_call.arguments`,
/// `partial_json`) are pieces of JSON text, see [`edit_json_text`], and
/// every other string is plain text. Outside a delta, content is whole:
/// read as [`edit_json`] reads it, a tool call's `input` numbers included.
fn edit_raw(v: &mut Value, parent: &str, delta: bool, edit: &mut Edit) -> Result<(), Fault> {
    match v {
        Value::Array(a) => a
            .iter_mut()
            .try_for_each(|v| edit_raw(v, parent, delta, edit)),
        Value::Object(o) => {
            for (mut name, mut value) in std::mem::take(o) {
                let arguments = name == "partial_json"
                    || (name == "arguments" && ["function", "function_call"].contains(&parent));
                match &mut value {
                    // Arguments that do not parse (cut off by a token limit)
                    // are JSON text too: a value written there is escaped.
                    Value::String(s)
                        if arguments && (delta || serde_json::from_str::<Value>(s).is_err()) =>
                    {
                        edit_json_text(s, edit)?
                    }
                    Value::String(s) if delta => edit(s)?,
                    _ if name == "input" && !delta => edit_json(&mut value, true, edit)?,
                    _ => edit_raw(&mut value, &name, delta || name == "delta", edit)?,
                }
                edit(&mut name)?;
                o.insert(name, value);
            }
            Ok(())
        }
        _ if delta => Ok(()),
        _ => edit_json(v, false, edit),
    }
}

/// Runs `edit` on the values of `fields`, not their names.
fn edit_fields(fields: &mut Map<String, Value>, edit: &mut Edit) -> Result<(), Fault> {
    fields
        .values_mut()
        .try_for_each(|v| edit_json(v, false, edit))
}

/// Runs `edit` on `text`, or inside it, numbers included, if it holds a
/// JSON object or array, written back only if changed.
fn edit_string(text: &mut String, edit: &mut Edit) -> Result<(), Fault> {
    match serde_json::from_str::<Value>(text) {
        Ok(mut json @ (Value::Object(_) | Value::Array(_))) => {
            let before = json.clone();
            edit_json(&mut json, true, edit)?;
            if json != before {
                *text = json.to_string();
            }
            Ok(())
        }
        _ => edit(text),
    }
}

/// The steps of JSON text: each a character, or an escape with the
/// character it stands for, `None` if the text ends inside it (a `\`, a
/// partial `\uXXXX`, or a high surrogate whose pair may follow).
fn steps(mut raw: &str) -> impl Iterator<Item = (&str, Option<char>)> {
    std::iter::from_fn(move || {
        let first = raw.chars().next()?;
        let (len, meaning) = match first {
            '\\' => escape(raw),
            c => (c.len_utf8(), Some(c)),
        };
        let (step, rest) = raw.split_at(len);
        raw = rest;
        Some((step, meaning))
    })
}

/// The length and meaning of the escape `raw` starts with.
fn escape(raw: &str) -> (usize, Option<char>) {
    let hex = |s: &str| s.bytes().all(|b| b.is_ascii_hexdigit());
    // The code unit of the `\uXXXX` `s` starts with.
    let unit = |s: &str| {
        let digits = s.strip_prefix("\\u")?.get(..4)?;
        u32::from_str_radix(digits, 16).ok().filter(|_| hex(digits))
    };
    let lone = Some('\u{FFFD}');
    match raw[1..].chars().next() {
        None => (1, None),
        Some('u') => match unit(raw) {
            None if raw.len() < 6 && hex(&raw[2..]) => (raw.len(), None),
            None => (1, Some('\\')),
            Some(high @ 0xD800..0xDC00) => {
                let rest = &raw[6..];
                let started = rest.bytes().zip(*b"\\u").all(|(a, b)| a == b)
                    && hex(rest.get(2..).unwrap_or(""));
                match unit(rest) {
                    Some(low @ 0xDC00..0xE000) => {
                        let pair = 0x10000 + ((high - 0xD800) << 10) + (low - 0xDC00);
                        (12, char::from_u32(pair))
                    }
                    None if rest.len() < 6 && started => (raw.len(), None),
                    _ => (6, lone),
                }
            }
            Some(unit) => (6, char::from_u32(unit).or(lone)),
        },
        Some(c) => {
            let named = [
                ('n', '\n'),
                ('t', '\t'),
                ('r', '\r'),
                ('b', '\u{8}'),
                ('f', '\u{c}'),
            ];
            let meaning = named.iter().find(|(k, _)| *k == c).map_or(c, |(_, v)| *v);
            (1 + c.len_utf8(), Some(meaning))
        }
    }
}

/// The text `raw` (JSON text with no cut-off escape) stands for.
fn unescape(raw: &str) -> String {
    steps(raw).filter_map(|(_, c)| c).collect()
}

/// `text` as JSON text, between quotes.
fn escaped(text: &str) -> String {
    let quoted = Value::String(text.to_owned()).to_string();
    quoted[1..quoted.len() - 1].to_owned()
}

/// The run of JSON text before its first quote that is not escaped, and the
/// rest, from that quote.
fn run(raw: &str) -> (&str, &str) {
    let steps = steps(raw).take_while(|(s, _)| *s != "\"");
    raw.split_at(steps.map(|(s, _)| s.len()).sum())
}

/// Runs `edit` on each run of a piece of JSON text between its quotes, as
/// the text it stands for (`b` reads as `b`, so a check sees a term
/// however it is spelled). A run is written escaped if changed and as it
/// came otherwise, so the JSON stays whole.
fn edit_json_text(text: &mut String, edit: &mut Edit) -> Result<(), Fault> {
    let (mut out, mut rest) = (String::with_capacity(text.len()), text.as_str());
    while !rest.is_empty() {
        let (before, after) = run(rest);
        let (mut plain, quote) = (unescape(before), after.get(..1).unwrap_or(""));
        let was = plain.clone();
        if !plain.is_empty() {
            edit(&mut plain)?;
        }
        out.push_str(&if plain == was {
            before.to_owned()
        } else {
            // A cut-off escape at the end stays as it came.
            let cut = steps(before).last().filter(|(_, m)| m.is_none());
            escaped(&plain) + cut.map_or("", |(step, _)| step)
        });
        out.push_str(quote);
        rest = &after[quote.len()..];
    }
    *text = out;
    Ok(())
}

/// Tool results may contain text blocks, but images and other opaque payloads
/// must be refused before any edits occur.
fn text_result(value: &Value) -> bool {
    value.is_string()
        || value.as_array().is_some_and(|parts| {
            parts.iter().all(|part| {
                fields_are(part, &["type", "text"])
                    && part["type"] == "text"
                    && part["text"].is_string()
            })
        })
}

fn readable_part(part: &Content) -> bool {
    match part {
        Content::Text { .. } | Content::ToolCall { .. } => true,
        Content::ToolResult { content, .. } => content.iter().all(readable_part),
        Content::Other { format, raw, .. } if format.as_ref() == "anthropic-messages" => {
            matches!(raw["type"].as_str(), Some("tool_use" | "tool_result"))
                && readable_anthropic(raw)
        }
        _ => false,
    }
}

fn edit_content(part: &mut Content, edit: &mut Edit) -> Result<(), Fault> {
    match part {
        Content::Text { text, .. } => edit(text),
        Content::ToolCall {
            name, arguments, ..
        } => {
            edit(name)?;
            edit_arguments(arguments, edit)
        }
        Content::ToolResult { content, .. } => content
            .iter_mut()
            .try_for_each(|part| edit_content(part, edit)),
        Content::Other { raw, .. } => {
            let raw = Arc::make_mut(raw);
            if raw["type"] == "tool_use" {
                if let Some(Value::String(name)) = raw.get_mut("name") {
                    edit(name)?;
                }
                edit_json(&mut raw["input"], true, edit)
            } else {
                match &mut raw["content"] {
                    Value::String(text) => edit_string(text, edit),
                    Value::Array(parts) => {
                        parts
                            .iter_mut()
                            .try_for_each(|part| match &mut part["text"] {
                                Value::String(text) => edit(text),
                                _ => Err(unreadable("a tool result")),
                            })
                    }
                    _ => Err(unreadable("a tool result")),
                }
            }
        }
        _ => Err(unreadable("a part with no text form")),
    }
}

/// Every text part of every message, the system prompt included, the
/// messages' and the request's fields the kind has no place for (a tool
/// call, tools), Anthropic tool inputs and text results, the stop sequences
/// and the end user's id. Content with no text form, such as an image, is refused.
impl Readable for Conversation {
    fn edit_all(
        &mut self,
        mut edit: impl FnMut(&mut String) -> Result<(), Fault>,
    ) -> Result<(), Fault> {
        let parts = self.messages.iter().flat_map(|m| &m.content);
        if !parts.clone().all(readable_part) {
            return Err(unreadable("a part other than text, such as an image"));
        }
        for message in &mut self.messages {
            for part in &mut message.content {
                edit_content(part, &mut edit)?;
            }
            edit_fields(&mut message.unsupported, &mut edit)?;
        }
        for text in self.stop.iter_mut().chain(&mut self.user) {
            edit(text)?;
        }
        for tool in &mut self.tools {
            edit(&mut tool.name)?;
            if let Some(text) = &mut tool.description {
                edit(text)?;
            }
            edit_json(&mut tool.parameters, false, &mut edit)?;
            edit_fields(&mut tool.unsupported, &mut edit)?;
        }
        if let Some(ToolChoice::Tool(name)) = &mut self.tool_choice {
            edit(name)?;
        }
        edit_fields(&mut self.unsupported, &mut edit)
    }
}

/// Only known wire fields may be read as text. An unknown field can contain
/// opaque content even when the fields beside it are readable.
fn fields_are(value: &Value, names: &[&str]) -> bool {
    value
        .as_object()
        .is_some_and(|fields| fields.keys().all(|name| names.contains(&name.as_str())))
}

fn text_or_null(value: &Value) -> bool {
    value.is_null() || value.is_string()
}

fn openai_call(call: &Value) -> bool {
    fields_are(call, &["index", "id", "type", "function"])
        && call.get("index").is_none_or(Value::is_number)
        && ["id", "type"].iter().all(|key| text_or_null(&call[*key]))
        && call.get("function").is_none_or(|f| {
            fields_are(f, &["name", "arguments"])
                && text_or_null(&f["name"])
                && text_or_null(&f["arguments"])
        })
}

fn openai_body(body: &Value) -> bool {
    fields_are(
        body,
        &[
            "role",
            "content",
            "refusal",
            "reasoning_content",
            "function_call",
            "tool_calls",
        ],
    ) && ["role", "content", "refusal", "reasoning_content"]
        .iter()
        .all(|key| text_or_null(&body[*key]))
        && body.get("function_call").is_none_or(|call| {
            fields_are(call, &["name", "arguments"])
                && text_or_null(&call["name"])
                && text_or_null(&call["arguments"])
        })
        && body.get("tool_calls").is_none_or(|calls| {
            calls
                .as_array()
                .is_some_and(|calls| calls.iter().all(openai_call))
        })
}

fn openai_choice(choice: &Value) -> bool {
    fields_are(
        choice,
        &[
            "index",
            "delta",
            "message",
            "finish_reason",
            "logprobs",
            "native_finish_reason",
            "stop_reason",
            "content_filter_results",
            "content_filter_offsets",
        ],
    ) && choice.get("index").is_none_or(Value::is_number)
        && ["finish_reason", "native_finish_reason", "stop_reason"]
            .iter()
            .all(|key| text_or_null(&choice[*key]))
        && [
            "logprobs",
            "content_filter_results",
            "content_filter_offsets",
        ]
        .iter()
        .all(|key| choice[*key].is_null())
        && (choice.get("delta").is_some_and(openai_body)
            || choice.get("message").is_some_and(openai_body)
            || choice["finish_reason"].is_string())
        && ["delta", "message"]
            .iter()
            .all(|key| choice.get(*key).is_none_or(openai_body))
}

fn readable_anthropic(raw: &Value) -> bool {
    match raw["type"].as_str() {
        Some("tool_use") => {
            fields_are(raw, &["type", "id", "name", "input"])
                && text_or_null(&raw["id"])
                && raw["name"].is_string()
                && raw["input"].is_object()
        }
        Some("tool_result") => {
            fields_are(raw, &["type", "tool_use_id", "content", "is_error"])
                && text_or_null(&raw["tool_use_id"])
                && (raw["is_error"].is_null() || raw["is_error"].is_boolean())
                && text_result(&raw["content"])
        }
        Some("input_json_delta") => {
            fields_are(raw, &["type", "partial_json"]) && raw["partial_json"].is_string()
        }
        Some("text") => {
            fields_are(raw, &["type", "text", "citations"])
                && raw["text"].is_string()
                && raw["citations"].is_null()
        }
        Some("text_delta") => fields_are(raw, &["type", "text"]) && raw["text"].is_string(),
        Some("thinking" | "thinking_delta") => {
            fields_are(raw, &["type", "thinking", "signature"])
                && raw["thinking"].is_string()
                && text_or_null(&raw["signature"])
        }
        Some("content_block_delta") => {
            fields_are(raw, &["type", "index", "delta"])
                && raw["index"].is_number()
                && matches!(
                    raw["delta"]["type"].as_str(),
                    Some("input_json_delta" | "text_delta" | "thinking_delta")
                )
                && readable_anthropic(&raw["delta"])
        }
        _ => false,
    }
}

fn readable_piece(piece: &Piece) -> bool {
    match piece {
        Piece::Text { .. } | Piece::ToolCall { .. } => true,
        Piece::Other { format, raw, .. } if matches!(format.as_ref(), "openai-chat" | "openai") => {
            raw.as_array()
                .is_some_and(|choices| !choices.is_empty() && choices.iter().all(openai_choice))
        }
        Piece::Other { format, raw, .. } if format.as_ref() == "anthropic-messages" => {
            readable_anthropic(raw)
        }
        _ => false,
    }
}

/// Its texts and other content, such as a tool call. In a stream, a tool
/// call's arguments come in fragments, each read on its own: a match split
/// across fragments is not seen whole, unless a [`Holdback`] holds them.
impl Readable for Reply {
    fn edit_all(
        &mut self,
        mut edit: impl FnMut(&mut String) -> Result<(), Fault>,
    ) -> Result<(), Fault> {
        if !self.content.iter().all(readable_piece) {
            return Err(unreadable("content this reader does not know"));
        }
        self.content
            .iter_mut()
            .try_for_each(|p| edit_piece(p, &mut edit))
    }
}

/// Whether `raw` is an Anthropic `*_delta` event.
fn is_delta(raw: &Value) -> bool {
    let event = raw.get("type").and_then(Value::as_str);
    event.is_some_and(|t| t.ends_with("_delta"))
}

/// Runs `edit` on a tool call's arguments, JSON text: whole arguments that
/// parse as an object or array as [`edit_json`] reads them, numbers
/// included; a streamed part, or arguments cut off, as [`edit_json_text`].
fn edit_arguments(arguments: &mut String, edit: &mut Edit) -> Result<(), Fault> {
    match serde_json::from_str::<Value>(arguments) {
        Ok(Value::Object(_) | Value::Array(_)) => edit_string(arguments, edit),
        _ => edit_json_text(arguments, edit),
    }
}

fn edit_piece(piece: &mut Piece, edit: &mut Edit) -> Result<(), Fault> {
    match piece {
        Piece::Text { text, .. } => edit(text),
        Piece::ToolCall {
            name, arguments, ..
        } => {
            if let Some(name) = name {
                edit(name)?;
            }
            edit_arguments(arguments, edit)
        }
        Piece::Other { raw, .. } => {
            let delta = is_delta(raw);
            edit_raw(Arc::make_mut(raw), "", delta, edit)
        }
        _ => Ok(()),
    }
}

/// What a [`Holdback`] does with the text it releases. A check returns
/// `text` unchanged, or `Err`.
pub trait Rewrite: Send + 'static {
    /// How many trailing bytes of `text` may start a match: they are held
    /// until more text or the block's end says.
    fn pending(&self, text: &str) -> usize;

    /// Released `text`, never cut inside a match, rewritten; `Err` refuses
    /// the response, before any byte if none was sent.
    fn rewrite(&mut self, text: &str) -> Result<String, Fault>;
}

/// How many trailing bytes of `text` begin one of `terms` without completing
/// it, never cutting a whole term in `text`: [`Rewrite::pending`] for
/// literal terms.
pub fn partial_match<'a>(text: &str, terms: impl IntoIterator<Item = &'a str>) -> usize {
    let terms: Vec<&str> = terms.into_iter().collect();
    // Whether a whole term spans byte `at`, as the `b` ending "a bomb" does.
    let cuts = |at: usize| {
        let spans = |t: &&str| {
            text.match_indices(*t)
                .any(|(i, m)| i < at && at < i + m.len())
        };
        terms.iter().any(spans)
    };
    let mut longest = 0;
    for term in &terms {
        for n in (1..term.len()).filter(|&n| term.is_char_boundary(n)) {
            if text.ends_with(&term[..n]) && !cuts(text.len() - n) {
                longest = longest.max(n);
            }
        }
    }
    longest
}

/// A [`Reply`] filter that runs every text through a [`Rewrite`], holding
/// back what may start a match. Text is held per block: text of another
/// block, other content, a finish or the end releases it, so a match split
/// across two text blocks (Anthropic's citations make many) is not seen
/// whole; holding across would move text into a block that does not say
/// it. A reply left showing nothing waits, whole, until one that shows
/// something, so nothing is sent while the start is held; one that is no
/// content ([`Reply::is_content`]), such as a keep-alive or usage alone,
/// passes at once unless replies wait before it. Other content, such as a
/// tool call, is rewritten as [`Readable`] reads it. A streamed tool call's
/// arguments (`function.arguments` in a chunk's `delta`, as OpenAI sends
/// them) are held per call like text, so a match split across their
/// fragments is seen whole and what replaces it is escaped as JSON text; the
/// tail held at a finish or the end is released as a chunk of just that call's
/// arguments. Anthropic's `partial_json` is held the same way. OpenAI
/// choices retain independent text and argument tails, including when their
/// chunks interleave or one choice finishes before the others.
#[derive(Debug)]
pub struct Holdback<R> {
    rewrite: R,
    /// The block whose trailing text is held, and that text.
    held: Option<(u32, String)>,
    /// The tails of typed tool calls' arguments held, by call.
    calls: BTreeMap<u32, String>,
    /// The tails of tool-call arguments held in other content, by place.
    arguments: Vec<Tail>,
    /// Text tails of OpenAI choices other than choice zero (the typed text).
    choices: BTreeMap<u64, (Arc<str>, String)>,
    /// Replies that show nothing yet, in order.
    waiting: Vec<Reply>,
}

impl<R: Rewrite> Holdback<R> {
    /// A filter running texts through `rewrite`.
    pub fn new(rewrite: R) -> Self {
        Self {
            rewrite,
            held: None,
            calls: BTreeMap::new(),
            arguments: Vec::new(),
            choices: BTreeMap::new(),
            waiting: Vec::new(),
        }
    }

    fn rewritten(&mut self, text: &str) -> Result<String, Fault> {
        if text.is_empty() {
            return Ok(String::new());
        }
        self.rewrite.rewrite(text)
    }

    /// The held text's block and the text, rewritten, if any.
    fn release(&mut self) -> Result<Option<(u32, String)>, Fault> {
        match self.held.take() {
            Some((block, text)) if !text.is_empty() => Ok(Some((block, self.rewritten(&text)?))),
            _ => Ok(None),
        }
    }

    /// Release the held text into `reply`.
    fn release_into(&mut self, reply: &mut Reply) -> Result<(), Fault> {
        if let Some((block, text)) = self.release()? {
            reply.content.push(Piece::text(block, text));
        }
        Ok(())
    }

    /// Choice zero may arrive as typed text or inside a later array entry.
    /// Both share `held`; other choices retain independent tails.
    fn hold_choices(&mut self, format: &Arc<str>, raw: &mut Arc<Value>) {
        let Some(choices) = Arc::make_mut(raw).as_array_mut() else {
            return;
        };
        for choice in choices {
            let index = choice["index"].as_u64().unwrap_or(0);
            let finished = choice["finish_reason"].is_string();
            if !choice["delta"]["content"].is_string() && !finished {
                continue;
            }
            let before = if index == 0 {
                self.held.take().map(|(_, text)| text)
            } else {
                self.choices.remove(&index).map(|(_, text)| text)
            }
            .unwrap_or_default();
            let text = choice["delta"]["content"].as_str().unwrap_or_default();
            let mut text = before + text;
            let keep = if finished {
                0
            } else {
                self.rewrite.pending(&text)
            };
            let mut cut = text.len().saturating_sub(keep);
            while !text.is_char_boundary(cut) {
                cut -= 1;
            }
            let tail = text.split_off(cut);
            if !tail.is_empty() {
                if index == 0 {
                    self.held = Some((0, tail));
                } else {
                    self.choices.insert(index, (format.clone(), tail));
                }
            }
            if !text.is_empty() || choice["delta"]["content"].is_string() {
                choice["delta"]["content"] = text.into();
            }
        }
    }

    /// Hold back the tail of each tool call's arguments in `raw` that may
    /// start a match or is a cut-off escape, with what was held of the call
    /// before in front. With `open` false nothing is held: the message ends
    /// the response.
    fn hold_arguments(&mut self, format: &Arc<str>, raw: &mut Arc<Value>, open: bool) {
        let finished: Vec<_> = raw
            .as_array()
            .into_iter()
            .flatten()
            .filter(|choice| choice["finish_reason"].is_string())
            .filter_map(|choice| choice["index"].as_u64())
            .collect();
        let (rewrite, tails) = (&self.rewrite, &mut self.arguments);
        for_arguments(Arc::make_mut(raw), &mut |path, arguments| {
            let at = tails.iter().position(|t| t.path == path);
            let before = at.map(|i| tails.remove(i).text).unwrap_or_default();
            let mut text = before + arguments;
            let choice = choice_index(path);
            let open = (open || choice != 0) && !finished.contains(&choice);
            let cut = if open {
                held_from(&text, |t| rewrite.pending(t))
            } else {
                text.len()
            };
            let tail = text.split_off(cut);
            if !tail.is_empty() {
                let (format, path) = (format.clone(), path.to_vec());
                tails.push(Tail {
                    format,
                    path,
                    text: tail,
                });
            }
            *arguments = text;
        });
    }

    /// Hold back the tail of tool call `call`'s `arguments` that may start a
    /// match or is a cut-off escape, with what was held of the call before
    /// in front, unless `open` is false; rewrite the rest.
    fn hold_call(&mut self, call: u32, arguments: &mut String, open: bool) -> Result<(), Fault> {
        let mut text = self.calls.remove(&call).unwrap_or_default() + arguments.as_str();
        if open {
            let cut = held_from(&text, |t| self.rewrite.pending(t));
            let tail = text.split_off(cut);
            if !tail.is_empty() {
                self.calls.insert(call, tail);
            }
        }
        edit_arguments(&mut text, &mut |t| {
            *t = self.rewritten(t)?;
            Ok(())
        })?;
        *arguments = text;
        Ok(())
    }

    /// Release the typed calls' arguments held, rewritten.
    fn release_calls(&mut self) -> Result<Vec<Piece>, Fault> {
        let mut pieces = Vec::new();
        for (call, mut text) in std::mem::take(&mut self.calls) {
            edit_json_text(&mut text, &mut |t| {
                *t = self.rewritten(t)?;
                Ok(())
            })?;
            pieces.push(Piece::tool_arguments(call, text));
        }
        Ok(pieces)
    }

    /// Release the arguments held, rewritten, as one piece for each call.
    fn release_arguments(&mut self, choice: Option<u64>) -> Result<Vec<Piece>, Fault> {
        let (tails, kept): (Vec<_>, Vec<_>) = std::mem::take(&mut self.arguments)
            .into_iter()
            .partition(|tail| choice.is_none_or(|index| choice_index(&tail.path) == index));
        self.arguments = kept;
        let mut pieces = Vec::with_capacity(tails.len());
        for Tail {
            format,
            path,
            mut text,
        } in tails
        {
            edit_json_text(&mut text, &mut |t| {
                *t = self.rewritten(t)?;
                Ok(())
            })?;
            pieces.push(Piece::other(format, skeleton(&path, text)));
        }
        Ok(pieces)
    }
}

/// Where the tail of `text`, JSON text, that a [`Holdback`] holds begins: a
/// cut-off escape, and the text `pending` says may start a match, read as
/// the text it stands for, in the last run between quotes.
fn held_from(text: &str, pending: impl Fn(&str) -> usize) -> usize {
    let mut unfinished = text.len();
    let mut last = 0;
    let mut at = 0;
    for (step, meaning) in steps(text) {
        match (step, meaning) {
            (_, None) => {
                unfinished = at;
                break;
            }
            ("\"", _) => last = at + 1,
            _ => {}
        }
        at += step.len();
    }
    let run = &text[last..unfinished];
    let mut keep = pending(&unescape(run));
    let mut cut = run.len();
    for (step, meaning) in steps(run).collect::<Vec<_>>().into_iter().rev() {
        if keep == 0 {
            break;
        }
        keep = keep.saturating_sub(meaning.map_or(0, char::len_utf8));
        cut -= step.len();
    }
    last + cut
}

/// The tail of a tool call's arguments a [`Holdback`] holds, as JSON text.
#[derive(Debug)]
struct Tail {
    /// The format of the chunks the call is in.
    format: Arc<str>,
    /// Where the arguments are in a chunk.
    path: Vec<Step>,
    text: String,
}

/// A step from a message's content to a value in it: what makes the same
/// tool call's arguments in another chunk the same place.
#[derive(Debug, Clone, PartialEq)]
enum Step {
    Key(String),
    /// An element of an array.
    Element,
    /// The `index` the object being entered names itself by.
    Index(u64),
}

fn choice_index(path: &[Step]) -> u64 {
    // Only the outer choices array identifies a choice; deeper indexes name tools.
    match path {
        [Step::Element, Step::Index(index), ..] => *index,
        _ => 0,
    }
}

/// Runs `visit` on each streamed tool call's arguments in `v` (a chunk's
/// choices), with their place in it.
fn for_arguments(v: &mut Value, visit: &mut impl FnMut(&[Step], &mut String)) {
    fn walk(
        v: &mut Value,
        parent: &str,
        delta: bool,
        path: &mut Vec<Step>,
        visit: &mut impl FnMut(&[Step], &mut String),
    ) {
        match v {
            Value::Array(items) => {
                for item in items {
                    path.push(Step::Element);
                    walk(item, parent, delta, path, visit);
                    path.pop();
                }
            }
            Value::Object(fields) => {
                let index = fields.get("index").and_then(Value::as_u64);
                path.extend(index.map(Step::Index));
                for (name, value) in fields.iter_mut() {
                    path.push(Step::Key(name.clone()));
                    let calls = ["function", "function_call"].contains(&parent);
                    let arguments = (calls && name == "arguments") || name == "partial_json";
                    match value {
                        Value::String(s) if delta && arguments => visit(path, s),
                        _ => walk(value, name, delta || name == "delta", path, visit),
                    }
                    path.pop();
                }
                if index.is_some() {
                    path.pop();
                }
            }
            _ => {}
        }
    }
    walk(v, "", is_delta(v), &mut Vec::new(), visit);
}

/// The chunk that holds only `text` at `path`.
fn skeleton(path: &[Step], text: String) -> Value {
    match path {
        [] => text.into(),
        [Step::Key(name), rest @ ..] => {
            let mut object = json!({ name.as_str(): skeleton(rest, text) });
            if name == "partial_json" {
                object["type"] = "input_json_delta".into();
            }
            object
        }
        [Step::Element, rest @ ..] => Value::Array(vec![skeleton(rest, text)]),
        [Step::Index(n), rest @ ..] => {
            let mut object = skeleton(rest, text);
            object["index"] = (*n).into();
            object
        }
    }
}

/// Whether a reader sees something in it: a finish or content that is not
/// empty text.
fn shows(reply: &Reply) -> bool {
    let empty = |p: &Piece| matches!(p, Piece::Text { text, .. } if text.is_empty());
    reply.finish.is_some() || reply.content.iter().any(|p| !empty(p))
}

impl<R: Rewrite> Filter<Reply> for Holdback<R> {
    fn edit(&mut self, mut reply: Reply, out: &mut Vec<Reply>) -> Result<(), Fault> {
        if !reply.content.iter().all(readable_piece) {
            return Err(unreadable("content this reader does not know"));
        }
        let pieces = std::mem::take(&mut reply.content);
        let (last, open) = (pieces.len(), reply.finish.is_none());
        for (i, piece) in pieces.into_iter().enumerate() {
            let (block, text) = match piece {
                Piece::Text { block, text, .. } => (block, text),
                Piece::ToolCall {
                    call,
                    id,
                    mut name,
                    mut arguments,
                    ..
                } => {
                    self.release_into(&mut reply)?;
                    if let Some(name) = &mut name {
                        *name = self.rewritten(name)?;
                    }
                    self.hold_call(call, &mut arguments, open)?;
                    let mut piece = Piece::tool_arguments(call, arguments);
                    if let Piece::ToolCall { id: i, name: n, .. } = &mut piece {
                        (*i, *n) = (id, name);
                    }
                    reply.content.push(piece);
                    continue;
                }
                mut other => {
                    let choices = matches!(&other, Piece::Other { raw, .. } if raw.is_array());
                    if !choices {
                        self.release_into(&mut reply)?;
                    }
                    if let Piece::Other { format, raw, .. } = &mut other {
                        self.hold_choices(format, raw);
                        self.hold_arguments(format, raw, open);
                    }
                    edit_piece(&mut other, &mut |text| {
                        *text = self.rewritten(text)?;
                        Ok(())
                    })?;
                    if let Piece::Other { raw, .. } = &other {
                        let finished: Vec<_> = raw
                            .as_array()
                            .into_iter()
                            .flatten()
                            .filter(|choice| choice["finish_reason"].is_string())
                            .filter_map(|choice| choice["index"].as_u64())
                            .collect();
                        for index in finished {
                            let content = self.release_arguments(Some(index))?;
                            if !content.is_empty() {
                                out.append(&mut self.waiting);
                                let mut tail = Reply::default();
                                tail.content = content;
                                out.push(tail);
                            }
                        }
                    }
                    reply.content.push(other);
                    continue;
                }
            };
            if self.held.as_ref().is_some_and(|(b, _)| *b != block) {
                self.release_into(&mut reply)?;
            }
            let mut text = self.held.take().map(|(_, t)| t).unwrap_or_default() + &text;
            // Only the reply's last text can continue in the next reply.
            let keep = if open && (i + 1 == last || block == 0) {
                self.rewrite.pending(&text)
            } else {
                0
            };
            let mut at = text.len().saturating_sub(keep);
            while !text.is_char_boundary(at) {
                at -= 1;
            }
            self.held = Some((block, text.split_off(at)));
            reply
                .content
                .push(Piece::text(block, self.rewritten(&text)?));
        }
        if !open {
            self.release_into(&mut reply)?;
            reply.content.extend(self.release_calls()?);
            reply.content.extend(self.release_arguments(Some(0))?);
        }
        // No content, such as a keep-alive or usage alone, is not delayed
        // unless replies wait before it.
        if shows(&reply) || (!reply.is_content() && self.waiting.is_empty()) {
            out.append(&mut self.waiting);
            out.push(reply);
        } else {
            self.waiting.push(reply);
        }
        Ok(())
    }

    fn end(&mut self, _end: &Result<(), Fault>, out: &mut Vec<Reply>) -> Result<(), Fault> {
        out.append(&mut self.waiting);
        if let Some((block, text)) = self.release()? {
            out.push(Reply::text(block, text));
        }
        let mut arguments = Reply::default();
        arguments.content = self.release_calls()?;
        arguments.content.extend(self.release_arguments(None)?);
        if !arguments.content.is_empty() {
            out.push(arguments);
        }
        for (index, (format, text)) in std::mem::take(&mut self.choices) {
            let text = self.rewritten(&text)?;
            out.push(Reply::other(
                format,
                json!([{ "index": index, "delta": { "content": text } }]),
            ));
        }
        Ok(())
    }
}

/// Values hidden behind placeholders, `<LABEL_n>`, for one request; clones
/// share them. As a [`Rewrite`] it restores them, in one pass:
/// `cx.filter(Holdback::new(placeholders.clone()))`.
#[derive(Debug, Clone, Default)]
pub struct Placeholders(Arc<Mutex<Vec<Hidden>>>);

#[derive(Debug)]
struct Hidden {
    label: String,
    value: String,
    placeholder: String,
}

impl Placeholders {
    /// The placeholder of `value`, made on first sight: `<LABEL_n>`, the
    /// `n`th value hidden under `label`, a plain word such as `EMAIL`.
    pub fn hide(&self, label: &str, value: &str) -> String {
        let mut all = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        let mine = || all.iter().filter(|h| h.label == label);
        if let Some(h) = mine().find(|h| h.value == value) {
            return h.placeholder.clone();
        }
        let placeholder = format!("<{label}_{}>", mine().count() + 1);
        all.push(Hidden {
            label: label.to_owned(),
            value: value.to_owned(),
            placeholder: placeholder.clone(),
        });
        placeholder
    }
}

impl Rewrite for Placeholders {
    fn pending(&self, text: &str) -> usize {
        let all = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        partial_match(text, all.iter().map(|h| h.placeholder.as_str()))
    }

    /// Each placeholder replaced once, so a restored value is never read
    /// again.
    fn rewrite(&mut self, text: &str) -> Result<String, Fault> {
        let all = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        let (mut out, mut rest) = (String::with_capacity(text.len()), text);
        while let Some(at) = rest.find('<') {
            out.push_str(&rest[..at]);
            rest = &rest[at..];
            match all.iter().find(|h| rest.starts_with(&h.placeholder)) {
                Some(h) => {
                    out.push_str(&h.value);
                    rest = &rest[h.placeholder.len()..];
                }
                None => {
                    out.push('<');
                    rest = &rest[1..];
                }
            }
        }
        out.push_str(rest);
        Ok(out)
    }
}
