#![warn(missing_docs)]
//! Read and rewrite the chat kind's text safely across stream deltas; refuse
//! what cannot be read.
//!
//! - [`Readable::read_all`]: all the text of a [`Conversation`] or a
//!   [`Reply`], or a refusal if it carries anything else.
//! - [`Holdback`]: a [`Reply`] filter that holds back trailing text that may
//!   start a match, so a [`Rewrite`] sees each match whole.
//! - [`Placeholders`]: hide values behind `<LABEL_n>` and restore them, a
//!   placeholder split across deltas included.

use std::sync::{Arc, Mutex, PoisonError};

use respan_gateway_core::http::StatusCode;
use respan_gateway_core::{Fault, Filter};
use respan_gateway_formats::conversation::{Content, Conversation, Piece, Reply};

/// The code of a refusal of content that cannot be read.
pub const UNREADABLE: &str = "unreadable_content";

fn unreadable(what: &str) -> Fault {
    let message = format!("the gateway cannot read {what}, so it refuses it");
    Fault::new(StatusCode::UNPROCESSABLE_ENTITY, UNREADABLE, message)
}

/// A value whose text a check must see all of.
pub trait Readable {
    /// All its text, one text per line, or `Err` with [`UNREADABLE`] if it
    /// carries content the kind has no text form for, such as an image, a
    /// tool call or a field the kind has no place for.
    fn read_all(&self) -> Result<String, Fault>;

    /// All its text, to change in place, or `Err` as
    /// [`read_all`](Readable::read_all).
    fn read_all_mut(&mut self) -> Result<Vec<&mut String>, Fault>;
}

/// Refuses what [`Readable`] cannot read in `chat`.
fn readable(chat: &Conversation) -> Result<(), Fault> {
    if let Some(field) = chat.unsupported.keys().next() {
        return Err(unreadable(&format!("the request's field '{field}'")));
    }
    match chat.messages.iter().all(|m| m.is_plain()) {
        true => Ok(()),
        false => Err(unreadable("a message other than text")),
    }
}

/// Every text part of every message, the system prompt included, then the
/// stop sequences and the end user's id.
impl Readable for Conversation {
    fn read_all(&self) -> Result<String, Fault> {
        readable(self)?;
        let parts = self.messages.iter().flat_map(|m| &m.content);
        let texts = parts.filter_map(|c| match c {
            Content::Text { text, .. } => Some(text),
            _ => None,
        });
        let all = texts.chain(&self.stop).chain(&self.user);
        Ok(all.map(|t| format!("{t}\n")).collect())
    }

    fn read_all_mut(&mut self) -> Result<Vec<&mut String>, Fault> {
        readable(self)?;
        let parts = self.messages.iter_mut().flat_map(|m| &mut m.content);
        let texts = parts.filter_map(|c| match c {
            Content::Text { text, .. } => Some(text),
            _ => None,
        });
        Ok(texts.chain(&mut self.stop).chain(&mut self.user).collect())
    }
}

/// Its texts.
impl Readable for Reply {
    fn read_all(&self) -> Result<String, Fault> {
        readable_reply(self)?;
        Ok(self.texts().collect())
    }

    fn read_all_mut(&mut self) -> Result<Vec<&mut String>, Fault> {
        readable_reply(self)?;
        Ok(self.texts_mut().collect())
    }
}

fn readable_reply(reply: &Reply) -> Result<(), Fault> {
    match reply
        .content
        .iter()
        .all(|p| matches!(p, Piece::Text { .. }))
    {
        true => Ok(()),
        false => Err(unreadable("content other than text, such as a tool call")),
    }
}

/// What a [`Holdback`] does with the text it releases.
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
    let starts = terms.iter().flat_map(|term| {
        let n = (1..term.len()).filter(|&n| term.is_char_boundary(n));
        n.filter(|&n| text.ends_with(&term[..n]))
    });
    let mut starts: Vec<usize> = starts.collect();
    starts.sort_unstable();
    let whole = starts.into_iter().rev().find(|&n| !cuts(text.len() - n));
    whole.unwrap_or(0)
}

/// A [`Reply`] filter that runs every text through a [`Rewrite`], holding
/// back what may start a match. Text is held per block: text of another
/// block, other content, a finish or the end releases it. A reply left
/// showing nothing waits, whole, until one that shows something, so nothing
/// is sent while the start is held. Other content passes unread; refuse it
/// with [`Readable::read_all`] if it must be seen.
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
        match text.is_empty() {
            true => Ok(String::new()),
            false => self.rewrite.rewrite(text),
        }
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
                other => {
                    self.release_into(&mut reply)?;
                    reply.content.push(other);
                    continue;
                }
            };
            if self.held.as_ref().is_some_and(|(b, _)| *b != block) {
                self.release_into(&mut reply)?;
            }
            let mut text = self.held.take().map(|(_, t)| t).unwrap_or_default() + &text;
            // Only the reply's last text can continue in the next reply.
            let keep = match open && i + 1 == last {
                true => self.rewrite.pending(&text),
                false => 0,
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
        match shows(&reply) {
            true => {
                out.append(&mut self.waiting);
                out.push(reply);
            }
            false => self.waiting.push(reply),
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
