//! Where the plugin keeps what it keeps ([`Store`]): in this gateway's
//! memory ([`Memory`]), or wherever a deployment of several gateways shares
//! it.

use std::time::Duration;

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
/// least recently used dropped first: not across a restart, nor shared
/// between gateways.
#[derive(Debug)]
pub struct Memory {
    turns: moka::sync::Cache<String, Turn>,
}

impl Memory {
    /// The bytes of messages kept at most, roughly.
    pub const BUDGET: u64 = 256 << 20;

    /// Turns kept for `keep`.
    pub fn new(keep: Duration) -> Self {
        let bytes = |_: &String, turn: &Turn| {
            let bytes: usize = turn.messages.iter().map(bytes_of).sum();
            u32::try_from(bytes).unwrap_or(u32::MAX)
        };
        let turns = moka::sync::Cache::builder()
            .time_to_idle(keep)
            .weigher(bytes)
            .max_capacity(Self::BUDGET)
            .eviction_policy(moka::policy::EvictionPolicy::lru())
            .build();
        Self { turns }
    }
}

impl Store for Memory {
    fn put(&self, key: String, turn: Turn) {
        self.turns.insert(key, turn);
    }

    fn get(&self, key: &str) -> Option<Turn> {
        self.turns.get(key)
    }
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
