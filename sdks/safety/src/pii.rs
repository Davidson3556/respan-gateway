//! PII redaction on the provider edge and restoration in the response.
//!
//! Redaction runs in `on_provider`, so it covers every call a request makes:
//! the target's, a fallback's second attempt, and a judge's call with its
//! own input, whatever order the plugins run in. Each call gets its own
//! vault and its own restoring filter, added with `px.filter`, which runs
//! before the client edge's filters, so client-edge plugins (an output
//! guardrail, a log) see the restored text.
//!
//! What the kind does not hold (OpenAI's `user`, Anthropic's `metadata`, a
//! header) is scrubbed from the call as sent, with `px.edit_call`, into the
//! same vault.
//!
//! PII goes on the provider edge (`docs/ARCHITECTURE.md`); on the client
//! edge it would leak to a judge ordered before it (FINDINGS F2).

use std::sync::{Arc, Mutex, PoisonError};

use respan_gateway_core::Doc;
use respan_gateway_core::http::{self, HeaderValue};

use respan_gateway_core::serde_json::{Value, json};
use respan_gateway_core::{Fault, Filter, Plugin, ProviderContext, async_trait};
use respan_gateway_formats::conversation::{Content, Conversation, Reply};

/// The code of a refusal for PII the kind cannot redact.
pub const UNREDACTABLE: &str = "pii_unredactable";

/// Finds PII in text: e-mail addresses, and the names it was given.
#[derive(Debug, Clone, Default)]
pub struct Redactor {
    names: Vec<String>,
}

impl Redactor {
    /// A redactor of e-mail addresses and of `names`, matched as whole
    /// words, case-sensitively.
    pub fn new(names: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            names: names.into_iter().map(Into::into).collect(),
        }
    }

    /// `text` with each PII span replaced by its placeholder in `vault`.
    pub fn redact(&self, text: &str, vault: &mut Vault) -> String {
        let mut out = String::with_capacity(text.len());
        for word in split_keep(text) {
            match self.kind_of(word) {
                Some(kind) => out.push_str(&vault.placeholder(kind, word)),
                None => out.push_str(word),
            }
        }
        out
    }

    /// Whether `text` holds any PII.
    pub fn finds(&self, text: &str) -> bool {
        split_keep(text).any(|w| self.kind_of(w).is_some())
    }

    fn kind_of(&self, word: &str) -> Option<&'static str> {
        if is_email(word) {
            Some("EMAIL")
        } else if self.names.iter().any(|n| n == word) {
            Some("PERSON")
        } else {
            None
        }
    }
}

/// Splits `text` into runs of word characters and runs of the rest, so
/// joining them gives `text` back.
fn split_keep(text: &str) -> impl Iterator<Item = &str> {
    let word = |c: char| c.is_alphanumeric() || matches!(c, '@' | '.' | '_' | '-' | '+');
    let mut rest = text;
    std::iter::from_fn(move || {
        let first = rest.chars().next()?;
        let kind = word(first);
        let end = rest
            .char_indices()
            .find(|&(_, c)| word(c) != kind)
            .map_or(rest.len(), |(i, _)| i);
        // A trailing dot ends a sentence, not an address.
        let end = match kind && rest[..end].ends_with('.') && end > 1 {
            true => end - 1,
            false => end,
        };
        let (run, tail) = rest.split_at(end);
        rest = tail;
        Some(run)
    })
}

fn is_email(word: &str) -> bool {
    let Some((user, domain)) = word.split_once('@') else {
        return false;
    };
    !user.is_empty() && domain.contains('.') && !domain.starts_with('.') && !domain.ends_with('.')
}

/// Placeholders and the PII they stand for, for one call.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Vault {
    entries: Vec<(String, String)>,
}

impl Vault {
    /// The placeholder of `original`, made on first sight: `<KIND_n>`.
    pub fn placeholder(&mut self, kind: &str, original: &str) -> String {
        if let Some((p, _)) = self.entries.iter().find(|(_, o)| o == original) {
            return p.clone();
        }
        let n = self
            .entries
            .iter()
            .filter(|(p, _)| p.starts_with(&format!("<{kind}_")))
            .count()
            + 1;
        let p = format!("<{kind}_{n}>");
        self.entries.push((p.clone(), original.to_owned()));
        p
    }

    /// `text` with every whole placeholder restored.
    pub fn restore(&self, text: &str) -> String {
        let mut text = text.to_owned();
        for (p, o) in &self.entries {
            text = text.replace(p.as_str(), o);
        }
        text
    }

    /// Whether nothing was redacted.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// The longest placeholder, which bounds how much text is held back.
    fn longest(&self) -> usize {
        self.entries.iter().map(|(p, _)| p.len()).max().unwrap_or(0)
    }

    /// Where in `text` a placeholder may begin but has not ended.
    fn open_at(&self, text: &str) -> Option<usize> {
        let i = text.rfind('<')?;
        let tail = &text[i..];
        let could_be = !tail.contains('>')
            && tail.len() < self.longest()
            && self.entries.iter().any(|(p, _)| p.starts_with(tail));
        could_be.then_some(i)
    }
}

/// Redacts conversations before every call and restores the response.
///
/// Text parts are redacted. PII in what the kind has no text form for (an
/// image part's JSON, a tool call's arguments, a request field such as
/// `tools`) cannot be redacted through the kind, so the call is refused
/// with [`UNREDACTABLE`] (FINDINGS F5). Traffic of another kind is refused
/// unless [`Pii::pass_other_kinds`] is set.
#[derive(Debug, Clone)]
pub struct Pii {
    redactor: Arc<Redactor>,
    pass_other_kinds: bool,
    restore_tool_arguments: bool,
}

impl Pii {
    /// Redaction with `redactor`, refusing other kinds.
    pub fn new(redactor: Redactor) -> Self {
        Self {
            redactor: Arc::new(redactor),
            pass_other_kinds: false,
            restore_tool_arguments: false,
        }
    }

    /// Let traffic of kinds other than conversations pass unredacted.
    pub fn pass_other_kinds(mut self) -> Self {
        self.pass_other_kinds = true;
        self
    }

    /// Also restore whole placeholders inside tool-call arguments, which
    /// the kind carries as `Reply::Other` JSON (FINDINGS F4).
    pub fn restore_tool_arguments(mut self) -> Self {
        self.restore_tool_arguments = true;
        self
    }

    /// Redact `chat` in place into `vault`. `Err` for PII outside text
    /// parts.
    pub fn redact(&self, chat: &mut Conversation, vault: &mut Vault) -> Result<(), Fault> {
        if chat.unsupported.values().any(|v| self.finds_in(v)) {
            return Err(unredactable("a request field"));
        }
        for message in &mut chat.messages {
            if message.unsupported.values().any(|v| self.finds_in(v)) {
                return Err(unredactable("a message field such as a tool call"));
            }
            for content in &mut message.content {
                match content {
                    Content::Text { text, .. } => *text = self.redactor.redact(text, vault),
                    Content::Other { raw, .. } if self.finds_in(raw) => {
                        return Err(unredactable("a content part other than text"));
                    }
                    _ => {}
                }
            }
        }
        Ok(())
    }

    fn finds_in(&self, v: &Value) -> bool {
        finds_json(&self.redactor, v)
    }
}

#[async_trait]
impl Plugin for Pii {
    async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
        if !px.is_kind::<Conversation>() {
            return match self.pass_other_kinds {
                true => Ok(()),
                false => Err(Fault::new(
                    http::StatusCode::FORBIDDEN,
                    UNREDACTABLE,
                    "this kind of request cannot be redacted",
                )),
            };
        }
        let vault = Arc::new(Mutex::new(Vault::default()));
        // Every input the call sends: the one that came with the head, and
        // each of an upgrading request's later ones.
        let (this, into) = (self.clone(), vault.clone());
        px.filter(move |chat: &mut Conversation| this.redact(chat, &mut lock(&into)))?;
        let (redactor, into) = (self.redactor.clone(), vault.clone());
        px.edit_call(move |call| scrub(&redactor, call, &mut lock(&into)));
        px.filter(Restore::shared(vault, self.restore_tool_arguments))
    }
}

fn lock(vault: &Mutex<Vault>) -> std::sync::MutexGuard<'_, Vault> {
    vault.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Redact PII left in the call as sent: its headers and every JSON string
/// of its body, such as fields the kind does not hold. `Err` refuses the
/// call if any is left, such as in a body that is not JSON.
fn scrub(
    redactor: &Redactor,
    call: &mut http::Request<Doc>,
    vault: &mut Vault,
) -> Result<(), Fault> {
    for value in call.headers_mut().values_mut() {
        if let Ok(text) = value.to_str()
            && redactor.finds(text)
            && let Ok(redacted) = HeaderValue::from_str(&redactor.redact(text, vault))
        {
            *value = redacted;
        }
    }
    let finds = call.body().json().is_some_and(|v| finds_json(redactor, v));
    if let Some(body) = call.body_mut().json_mut().filter(|_| finds) {
        redact_json(redactor, body, vault);
    }
    let body = call.body().to_bytes();
    let headers = call.headers().values().filter_map(|v| v.to_str().ok());
    let mut left = headers.chain(std::str::from_utf8(&body).ok());
    match left.any(|text| redactor.finds(text)) {
        true => Err(unredactable("the call as sent")),
        false => Ok(()),
    }
}

fn unredactable(what: &str) -> Fault {
    Fault::new(
        http::StatusCode::BAD_REQUEST,
        UNREDACTABLE,
        format!("personal data in {what} cannot be redacted"),
    )
}

fn finds_json(redactor: &Redactor, v: &Value) -> bool {
    match v {
        Value::String(s) => redactor.finds(s),
        Value::Array(a) => a.iter().any(|v| finds_json(redactor, v)),
        Value::Object(o) => o.values().any(|v| finds_json(redactor, v)),
        _ => false,
    }
}

fn redact_json(redactor: &Redactor, v: &mut Value, vault: &mut Vault) {
    match v {
        Value::String(s) => *s = redactor.redact(s, vault),
        Value::Array(a) => a.iter_mut().for_each(|v| redact_json(redactor, v, vault)),
        Value::Object(o) => o.values_mut().for_each(|v| redact_json(redactor, v, vault)),
        _ => {}
    }
}

/// Restores placeholders in a response's text, holding back text that may
/// be the start of a placeholder until the next delta says whether it is.
/// Held text is released before any other content, at a change of block,
/// and at the end, a failed end included.
#[derive(Debug)]
pub struct Restore {
    /// Shared with the call's scrub, which may add to it as the call is
    /// sent.
    vault: Arc<Mutex<Vault>>,
    held: String,
    block: u32,
    tool_arguments: bool,
    /// Held tool-call argument text, by OpenAI tool-call index.
    held_args: std::collections::BTreeMap<u64, String>,
}

impl Restore {
    /// A filter restoring `vault`'s placeholders.
    pub fn new(vault: Vault, tool_arguments: bool) -> Self {
        Self::shared(Arc::new(Mutex::new(vault)), tool_arguments)
    }

    /// A filter restoring the placeholders of `vault`, which a redacting
    /// filter fills.
    pub fn shared(vault: Arc<Mutex<Vault>>, tool_arguments: bool) -> Self {
        Self {
            vault,
            held: String::new(),
            block: 0,
            tool_arguments,
            held_args: Default::default(),
        }
    }

    fn release_text(&mut self, out: &mut Vec<Reply>) {
        if !self.held.is_empty() {
            let text = std::mem::take(&mut self.held);
            out.push(Reply::text(self.block, lock(&self.vault).restore(&text)));
        }
    }

    fn release(&mut self, out: &mut Vec<Reply>) {
        self.release_text(out);
        // Held argument text goes out as a delta of its tool call, in
        // OpenAI's shape: the kind has no neutral form for it (F4).
        for (index, tail) in std::mem::take(&mut self.held_args) {
            let calls = json!([{ "index": index, "function": { "arguments": tail } }]);
            let choices = json!([{ "index": 0, "delta": { "tool_calls": calls } }]);
            out.push(Reply::other(OPENAI_CHAT, choices));
        }
    }

    /// Restore OpenAI tool-call argument deltas in `choices`, holding a
    /// placeholder's start until its next delta.
    fn openai_arguments(&mut self, mut choices: Value) -> Value {
        let calls = choices
            .as_array_mut()
            .into_iter()
            .flatten()
            .filter_map(|c| c.pointer_mut("/delta/tool_calls")?.as_array_mut())
            .flatten();
        for call in calls {
            let index = call["index"].as_u64().unwrap_or(0);
            let Some(Value::String(args)) = call.pointer_mut("/function/arguments") else {
                continue;
            };
            let mut text = self.held_args.remove(&index).unwrap_or_default() + args;
            if let Some(i) = lock(&self.vault).open_at(&text) {
                self.held_args.insert(index, text.split_off(i));
            }
            *args = lock(&self.vault).restore(&text);
        }
        choices
    }
}

/// The OpenAI Chat format's name, which its `Reply::Other` events carry.
const OPENAI_CHAT: &str = "openai-chat";

impl Filter<Reply> for Restore {
    fn edit(&mut self, event: Reply, out: &mut Vec<Reply>) -> Result<(), Fault> {
        match event {
            Reply::Text { block, text, .. } => {
                if block != self.block {
                    self.release_text(out);
                    self.block = block;
                }
                let mut text = std::mem::take(&mut self.held) + &text;
                if let Some(i) = lock(&self.vault).open_at(&text) {
                    self.held = text.split_off(i);
                }
                if !text.is_empty() {
                    out.push(Reply::text(block, lock(&self.vault).restore(&text)));
                }
            }
            // A start repeats in every OpenAI chunk; it bounds no text.
            e @ Reply::Start { .. } => out.push(e),
            Reply::Other { format, raw, .. } if self.tool_arguments && &*format == OPENAI_CHAT => {
                self.release_text(out);
                let choices = self.openai_arguments((*raw).clone());
                out.push(Reply::other(format, choices));
            }
            // Another format's own content: only whole placeholders.
            Reply::Other { format, raw, .. } if self.tool_arguments => {
                self.release(out);
                out.push(Reply::other(format, restore_json(&lock(&self.vault), &raw)));
            }
            e => {
                self.release(out);
                out.push(e);
            }
        }
        Ok(())
    }

    fn end(&mut self, _end: &Result<(), Fault>, out: &mut Vec<Reply>) -> Result<(), Fault> {
        self.release(out);
        Ok(())
    }
}

/// `v` with whole placeholders restored in every string.
fn restore_json(vault: &Vault, v: &Value) -> Value {
    match v {
        Value::String(s) => Value::String(vault.restore(s)),
        Value::Array(a) => a.iter().map(|v| restore_json(vault, v)).collect(),
        Value::Object(o) => Value::Object(
            o.iter()
                .map(|(k, v)| (k.clone(), restore_json(vault, v)))
                .collect(),
        ),
        v => v.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redacts_names_and_addresses_and_restores_them() {
        let r = Redactor::new(["Alice"]);
        let mut v = Vault::default();
        let text = r.redact("Alice <alice@example.com> met Alice.", &mut v);
        assert_eq!(text, "<PERSON_1> <<EMAIL_1>> met <PERSON_1>.");
        assert_eq!(v.restore(&text), "Alice <alice@example.com> met Alice.");
    }

    #[test]
    fn a_placeholder_split_across_deltas_is_held_then_restored() {
        let r = Redactor::new(["Alice"]);
        let mut v = Vault::default();
        r.redact("Alice", &mut v);
        let mut f = Restore::new(v, false);
        let mut out = Vec::new();
        for d in ["Hi <PER", "SON_", "1>!"] {
            f.edit(Reply::text(0, d), &mut out).expect("filters");
        }
        let text: String = out
            .iter()
            .filter_map(|e| match e {
                Reply::Text { text, .. } => Some(text.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(text, "Hi Alice!");
    }

    #[test]
    fn a_lone_angle_bracket_is_not_held() {
        let mut v = Vault::default();
        v.placeholder("PERSON", "Alice");
        assert_eq!(v.open_at("a < b"), None);
        assert_eq!(v.open_at("x <PE"), Some(2));
    }
}
