//! Where the plugin keeps what it keeps ([`Store`]): in this gateway's
//! memory ([`Memory`]), or wherever a deployment of several gateways shares
//! it.

use std::collections::HashMap;
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use respan_gateway_conversation::{Content, MediaSource, Message};

/// What is kept under a key: a response's turn of a conversation (the
/// messages its request added, and the reply) under its id, beside the
/// response it followed; or the reasoning an assistant's tool calls came
/// of, under each call's id.
#[derive(Debug, Clone)]
pub struct Turn {
    /// The response it followed.
    pub parent: Option<String>,
    /// Its messages.
    pub messages: Vec<Message>,
}

/// Where turns are kept. A store answers at once, as it is read and written
/// while a request's events pass; one shared by several gateways answers
/// from what it holds near.
pub trait Store: Send + Sync + 'static {
    /// Keep `turn` under `key`, in place of any there.
    fn put(&self, key: String, turn: Turn);
    /// The turn under `key`, if it is kept; kept afresh, as it was read.
    fn get(&self, key: &str) -> Option<Turn>;
}

/// Turns kept in this gateway's memory, each for `keep` after it was last
/// written or read, up to [`Memory::BUDGET`] bytes of them, roughly, the
/// stalest dropped first: not across a restart, nor shared between
/// gateways.
#[derive(Debug)]
pub struct Memory {
    keep: Duration,
    kept: Mutex<Kept>,
}

impl Memory {
    /// The bytes of messages kept at most, roughly.
    pub const BUDGET: usize = 256 << 20;

    /// Turns kept for `keep`.
    pub fn new(keep: Duration) -> Self {
        Self {
            keep,
            kept: Mutex::default(),
        }
    }

    fn lock(&self) -> MutexGuard<'_, Kept> {
        self.kept.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl Store for Memory {
    fn put(&self, key: String, turn: Turn) {
        let mut kept = self.lock();
        let bytes = turn.messages.iter().map(bytes_of).sum();
        let held = Held {
            turn,
            bytes,
            at: Instant::now(),
        };
        kept.bytes += bytes;
        if let Some(had) = kept.turns.insert(key, held) {
            kept.bytes -= had.bytes;
        }
        let expired: Vec<String> = kept
            .turns
            .iter()
            .filter(|(_, h)| h.at.elapsed() > self.keep)
            .map(|(key, _)| key.clone())
            .collect();
        expired.iter().for_each(|key| kept.remove(key));
        while kept.bytes > Self::BUDGET {
            let stalest = kept.turns.iter().min_by_key(|(_, h)| h.at);
            match stalest.map(|(key, _)| key.clone()) {
                Some(key) => kept.remove(&key),
                None => break,
            }
        }
    }

    fn get(&self, key: &str) -> Option<Turn> {
        let mut kept = self.lock();
        let held = kept.turns.get_mut(key)?;
        if held.at.elapsed() > self.keep {
            kept.remove(key);
            return None;
        }
        held.at = Instant::now();
        Some(held.turn.clone())
    }
}

/// The turns kept, by key, and their bytes.
#[derive(Debug, Default)]
struct Kept {
    turns: HashMap<String, Held>,
    bytes: usize,
}

impl Kept {
    fn remove(&mut self, key: &str) {
        if let Some(held) = self.turns.remove(key) {
            self.bytes -= held.bytes;
        }
    }
}

/// A turn held, its bytes, and when it was last written or read.
#[derive(Debug)]
struct Held {
    turn: Turn,
    bytes: usize,
    at: Instant,
}

/// Roughly the bytes `message` holds: its text and media.
fn bytes_of(message: &Message) -> usize {
    fn part(content: &Content) -> usize {
        match content {
            Content::Text { text, .. } => text.len(),
            // Reasoning carries its encrypted content in its native fields.
            Content::Reasoning { text, native } => {
                let fields = native.iter().flat_map(|n| n.fields.values());
                text.len() + fields.map(|v| v.to_string().len()).sum::<usize>()
            }
            Content::Image(media) | Content::Audio(media) | Content::Document(media) => {
                match &media.source {
                    MediaSource::Bytes(bytes) => bytes.len(),
                    MediaSource::Url(url) => url.len(),
                }
            }
            Content::ToolCall {
                arguments, name, ..
            } => arguments.len() + name.len(),
            Content::ToolResult { content, .. } => content.iter().map(part).sum(),
            _ => 256,
        }
    }
    64 + message.content.iter().map(part).sum::<usize>()
}
