#![warn(missing_docs)]
//! Read and rewrite the chat kind's text safely across stream deltas; refuse
//! what cannot be read.
//!
//! - [`Readable`]: all the text of a [`Conversation`] or a [`Reply`], tools
//!   and tool calls included, to read or edit, or a refusal if it carries
//!   content with no text form, such as an image.
//! - [`Holdback`]: a [`Reply`] filter that holds back trailing text that may
//!   start a match, so a [`Rewrite`] sees each match whole.
//! - [`Placeholders`]: hide values behind `<LABEL_n>` and restore them, a
//!   placeholder split across deltas included.

use std::sync::{Arc, Mutex, PoisonError};

use respan_gateway_core::serde_json::{self, Map, Value};
use respan_gateway_core::{Fault, Filter};
use respan_gateway_formats::conversation::{Content, Conversation, Piece, Reply};
use respan_gateway_http::http::StatusCode;

/// The code of a refusal of content that cannot be read.
pub const UNREADABLE: &str = "unreadable_content";

fn unreadable(what: &str) -> Fault {
    let message = format!("the gateway cannot read {what}, so it refuses it");
    Fault::new(StatusCode::UNPROCESSABLE_ENTITY, UNREADABLE, message)
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
///   text: what an edit writes is escaped, but a bare number there is not
///   quoted; the delta's other strings as plain text.
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
                *v = serde_json::from_str(&text).unwrap_or(Value::String(text));
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
                    Value::String(s) if delta && arguments => edit_json_text(s, edit)?,
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

/// Runs `edit` on a piece of JSON text between its quotes and escapes,
/// escaping what it writes, so the JSON stays whole however it is split.
/// Escaping text that needs none leaves it as it is.
fn edit_json_text(text: &mut String, edit: &mut Edit) -> Result<(), Fault> {
    let mut out = String::with_capacity(text.len());
    let mut rest = text.as_str();
    while !rest.is_empty() {
        let plain = rest.find(['"', '\\']).unwrap_or(rest.len());
        let mut part = rest[..plain].to_owned();
        edit(&mut part)?;
        if part == rest[..plain] {
            out.push_str(&part);
        } else {
            let quoted = Value::String(part).to_string();
            out.push_str(&quoted[1..quoted.len() - 1]);
        }
        rest = &rest[plain..];
        // A quote, or an escape (`\uXXXX` with its four digits), as it is.
        let mark = match rest.strip_prefix('\\').and_then(|r| r.chars().next()) {
            Some('u') => {
                2 + rest[2..]
                    .bytes()
                    .take(4)
                    .take_while(u8::is_ascii_hexdigit)
                    .count()
            }
            Some(c) => 1 + c.len_utf8(),
            None => rest.chars().next().map_or(0, char::len_utf8),
        };
        out.push_str(&rest[..mark]);
        rest = &rest[mark..];
    }
    *text = out;
    Ok(())
}

/// Every text part of every message, the system prompt included, the
/// messages' and the request's fields the kind has no place for (a tool
/// call, tools), the stop sequences and the end user's id. A part other
/// than text, such as an image, is refused.
impl Readable for Conversation {
    fn edit_all(
        &mut self,
        mut edit: impl FnMut(&mut String) -> Result<(), Fault>,
    ) -> Result<(), Fault> {
        let parts = self.messages.iter().flat_map(|m| &m.content);
        if !parts.clone().all(|c| matches!(c, Content::Text { .. })) {
            return Err(unreadable("a part other than text, such as an image"));
        }
        for message in &mut self.messages {
            for part in &mut message.content {
                if let Content::Text { text, .. } = part {
                    edit(text)?;
                }
            }
            edit_fields(&mut message.unsupported, &mut edit)?;
        }
        for text in self.stop.iter_mut().chain(&mut self.user) {
            edit(text)?;
        }
        edit_fields(&mut self.unsupported, &mut edit)
    }
}

/// Its texts and other content, such as a tool call. In a stream, a tool
/// call's arguments come in fragments, each read on its own: a match split
/// across fragments is not seen whole.
impl Readable for Reply {
    fn edit_all(
        &mut self,
        mut edit: impl FnMut(&mut String) -> Result<(), Fault>,
    ) -> Result<(), Fault> {
        let known = |p: &Piece| matches!(p, Piece::Text { .. } | Piece::Other { .. });
        if !self.content.iter().all(known) {
            return Err(unreadable("content this reader does not know"));
        }
        self.content
            .iter_mut()
            .try_for_each(|p| edit_piece(p, &mut edit))
    }
}

fn edit_piece(piece: &mut Piece, edit: &mut Edit) -> Result<(), Fault> {
    match piece {
        Piece::Text { text, .. } => edit(text),
        Piece::Other { raw, .. } => {
            let event = raw.get("type").and_then(Value::as_str);
            let delta = event.is_some_and(|t| t.ends_with("_delta"));
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
/// tool call, is
/// rewritten as [`Readable`] reads it, nothing held: a match split across a
/// streamed tool call's fragments is not seen whole.
#[derive(Debug)]
pub struct Holdback<R> {
    rewrite: R,
    /// The block whose trailing text is held, and that text.
    held: Option<(u32, String)>,
    /// Replies that show nothing yet, in order.
    waiting: Vec<Reply>,
}

impl<R: Rewrite> Holdback<R> {
    /// A filter running texts through `rewrite`.
    pub fn new(rewrite: R) -> Self {
        Self {
            rewrite,
            held: None,
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
}

/// Whether a reader sees something in it: a finish or content that is not
/// empty text.
fn shows(reply: &Reply) -> bool {
    let empty = |p: &Piece| matches!(p, Piece::Text { text, .. } if text.is_empty());
    reply.finish.is_some() || reply.content.iter().any(|p| !empty(p))
}

impl<R: Rewrite> Filter<Reply> for Holdback<R> {
    fn edit(&mut self, mut reply: Reply, out: &mut Vec<Reply>) -> Result<(), Fault> {
        let pieces = std::mem::take(&mut reply.content);
        let (last, open) = (pieces.len(), reply.finish.is_none());
        for (i, piece) in pieces.into_iter().enumerate() {
            let (block, text) = match piece {
                Piece::Text { block, text, .. } => (block, text),
                mut other => {
                    self.release_into(&mut reply)?;
                    edit_piece(&mut other, &mut |text| {
                        *text = self.rewritten(text)?;
                        Ok(())
                    })?;
                    reply.content.push(other);
                    continue;
                }
            };
            if self.held.as_ref().is_some_and(|(b, _)| *b != block) {
                self.release_into(&mut reply)?;
            }
            let mut text = self.held.take().map(|(_, t)| t).unwrap_or_default() + &text;
            // Only the reply's last text can continue in the next reply.
            let keep = if open && i + 1 == last {
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
