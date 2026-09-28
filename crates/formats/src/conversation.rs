//! The conversation kind: chats with a language model. Formats translate
//! through these types, and plugins read any conversation through them:
//! `cx.filter(|c: &mut Conversation| ..)` for the input and
//! `cx.filter(|r: &mut Reply| ..)` for the output, one [`Reply`] per
//! provider message.
//!
//! What the kind has no form for stays as the client's format has it
//! ([`Conversation::unsupported`], [`Message::unsupported`],
//! [`Content::Other`]); translating it is refused if it can change the
//! answer.

use respan_gateway_core::bytes::Bytes;
use respan_gateway_core::{Fault, serde_json};
use std::sync::Arc;

use serde_json::{Map, Value};

use crate::Usage;
use crate::relayed::{Origin, Relayed};

/// Usage units every conversation format reports alike.
pub mod units {
    /// Every input token, cached or not.
    pub const INPUT_TOKENS: &str = "input_tokens";
    /// Every output token.
    pub const OUTPUT_TOKENS: &str = "output_tokens";
    /// The input tokens read from a cache.
    pub const CACHED_INPUT_TOKENS: &str = "cached_input_tokens";
    /// The input tokens written to a cache.
    pub const CACHE_WRITE_INPUT_TOKENS: &str = "cache_write_input_tokens";
    /// The output tokens spent on reasoning.
    pub const REASONING_TOKENS: &str = "reasoning_tokens";
}

/// The kind's input: a conversation request.
#[derive(Debug, Clone, Default, PartialEq)]
#[non_exhaustive]
pub struct Conversation {
    /// The model asked for, if any.
    pub model: Option<String>,
    /// The messages, in order.
    pub messages: Vec<Message>,
    /// Whether the request asks for a stream. The client gets the framing
    /// it asked for.
    pub stream: bool,
    /// The most tokens the answer may have.
    pub max_tokens: Option<u64>,
    /// The sampling temperature.
    pub temperature: Option<f64>,
    /// The nucleus sampling cutoff.
    pub top_p: Option<f64>,
    /// Sequences that stop the answer.
    pub stop: Vec<String>,
    /// The end user's id, as the client names them to the provider.
    pub user: Option<String>,
    /// The request's fields the kind has no place for, such as its tools.
    pub unsupported: Map<String, Value>,
    /// The client's body it was decoded from.
    pub(crate) origin: Relayed<Conversation>,
}

impl Conversation {
    /// A request of these `messages` alone, such as a judge's own prompt.
    pub fn new(messages: impl IntoIterator<Item = Message>) -> Self {
        Self {
            messages: messages.into_iter().collect(),
            ..Self::default()
        }
    }

    /// For formats: decoded by `format` from `bytes`.
    pub fn came(mut self, format: &Arc<str>, bytes: Bytes) -> Self {
        self.origin = Relayed::default();
        self.origin = Relayed::came(format, bytes, self.clone());
        self
    }

    /// The body it was decoded from, if `format` decoded it.
    pub fn origin(&self, format: &str) -> Option<&Origin<Conversation>> {
        self.origin.origin(format)
    }
}

/// A filter that asks for `model`, such as `cx.filter(set_model(m))`.
pub fn set_model(
    model: impl Into<String>,
) -> impl FnMut(&mut Conversation) -> Result<(), Fault> + Clone + Send + 'static {
    let model = model.into();
    move |chat: &mut Conversation| {
        chat.model = Some(model.clone());
        Ok(())
    }
}

/// One message of a conversation: its author and what it says.
#[derive(Debug, Clone)]
pub struct Message {
    /// Its author.
    pub role: Role,
    /// What it says, part by part.
    pub content: Vec<Content>,
    /// The message's fields the kind has no place for, such as a tool call.
    pub unsupported: Map<String, Value>,
    /// The client's message it was decoded from; shared, as it may be large.
    source: Option<Arc<Value>>,
}

/// Compares what messages say, not their source.
impl PartialEq for Message {
    fn eq(&self, other: &Self) -> bool {
        (&self.role, &self.content, &self.unsupported)
            == (&other.role, &other.content, &other.unsupported)
    }
}

impl Message {
    /// A message of one part.
    pub fn new(role: Role, content: impl Into<Content>) -> Self {
        Self {
            role,
            content: vec![content.into()],
            unsupported: Map::new(),
            source: None,
        }
    }

    /// For formats: the client's message this was decoded from, sent as it
    /// is while unchanged, and in its shape once changed.
    pub fn with_source(mut self, source: Value) -> Self {
        self.source = Some(Arc::new(source));
        self
    }

    /// The client's message this was decoded from, if any.
    pub fn source(&self) -> Option<&Value> {
        self.source.as_deref()
    }

    /// Whether both were decoded from the same client message.
    pub fn same_source(&self, other: &Message) -> bool {
        matches!((&self.source, &other.source), (Some(a), Some(b)) if Arc::ptr_eq(a, b))
    }

    /// The message's text parts, joined; other content is skipped.
    pub fn text(&self) -> String {
        self.content
            .iter()
            .map(|c| match c {
                Content::Text { text } => text.as_str(),
                Content::Other { .. } => "",
            })
            .collect()
    }

    /// Whether the kind has a place for all of it: a known role and text.
    pub fn is_plain(&self) -> bool {
        let text = |c: &Content| matches!(c, Content::Text { .. });
        !matches!(self.role, Role::Other(_))
            && self.unsupported.is_empty()
            && self.content.iter().all(text)
    }
}

/// Who wrote a message.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Role {
    /// Instructions for the model.
    System,
    /// The person or program using the model.
    User,
    /// The model.
    Assistant,
    /// A role the kind has no form for, by the client format's name.
    Other(String),
}

/// A part of a message; a plugin that must see all of it refuses variants
/// it does not know.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum Content {
    /// Text.
    #[non_exhaustive]
    Text {
        /// The text.
        text: String,
    },
    /// A part the kind has no form for, such as an image.
    #[non_exhaustive]
    Other {
        /// The part as the client's format has it.
        raw: Arc<Value>,
    },
}

impl Content {
    /// A part the kind has no form for.
    pub fn other(raw: impl Into<Value>) -> Self {
        Content::Other {
            raw: Arc::new(raw.into()),
        }
    }
}

impl From<&str> for Content {
    fn from(text: &str) -> Self {
        Content::Text { text: text.into() }
    }
}

impl From<String> for Content {
    fn from(text: String) -> Self {
        Content::Text { text }
    }
}

/// The kind's output: one message of a response, such as an OpenAI chunk,
/// an Anthropic event or a whole body. Filters see every reply, those that
/// are no content ([`Reply::is_content`]) included, such as a keep-alive or
/// usage alone.
#[derive(Debug, Clone, Default, PartialEq)]
#[non_exhaustive]
pub struct Reply {
    /// Which response this is, if the message names it.
    pub start: Option<Start>,
    /// What it says, in order.
    pub content: Vec<Piece>,
    /// Why the model stopped, if it says.
    pub finish: Option<Finish>,
    /// The usage the provider reports in it, as an increment. A plugin
    /// that meters a call sums it in a provider-edge filter, which sees
    /// every reply, usage alone included.
    pub usage: Usage,
    /// The provider's message it was decoded from.
    pub(crate) origin: Relayed<Reply>,
}

/// Which response a reply belongs to.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Start {
    /// The response's id.
    pub id: String,
    /// The model that replied.
    pub model: String,
}

/// A part of what a reply says.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum Piece {
    /// Text of content block `block`.
    #[non_exhaustive]
    Text {
        /// The content block, numbered in the order blocks open.
        block: u32,
        /// The text, or a delta of it in a stream.
        text: String,
    },
    /// Content the kind has no form for, such as a tool call, as `format`
    /// has it; translating it is refused.
    #[non_exhaustive]
    Other {
        /// The name of the format `raw` is in.
        format: Arc<str>,
        /// The content as that format has it.
        raw: Arc<Value>,
    },
}

impl Piece {
    /// Text of content block `block`.
    pub fn text(block: u32, text: impl Into<String>) -> Self {
        Piece::Text {
            block,
            text: text.into(),
        }
    }

    /// Content of `format` the kind has no form for.
    pub fn other(format: impl Into<Arc<str>>, raw: impl Into<Value>) -> Self {
        Piece::Other {
            format: format.into(),
            raw: Arc::new(raw.into()),
        }
    }
}

impl Reply {
    /// A reply naming its response.
    pub fn start(id: impl Into<String>, model: impl Into<String>) -> Self {
        Self {
            start: Some(Start {
                id: id.into(),
                model: model.into(),
            }),
            ..Self::default()
        }
    }

    /// A reply of text of content block `block`.
    pub fn text(block: u32, text: impl Into<String>) -> Self {
        Self {
            content: vec![Piece::text(block, text)],
            ..Self::default()
        }
    }

    /// A reply of content of `format` the kind has no form for.
    pub fn other(format: impl Into<Arc<str>>, raw: impl Into<Value>) -> Self {
        Self {
            content: vec![Piece::other(format, raw)],
            ..Self::default()
        }
    }

    /// A reply saying why the model stopped.
    pub fn finished(finish: Finish) -> Self {
        Self {
            finish: Some(finish),
            ..Self::default()
        }
    }

    /// Its texts, to change in place.
    pub fn texts_mut(&mut self) -> impl Iterator<Item = &mut String> {
        self.content.iter_mut().filter_map(|piece| match piece {
            Piece::Text { text, .. } => Some(text),
            Piece::Other { .. } => None,
        })
    }

    /// Its texts, in order; other content is skipped.
    pub fn texts(&self) -> impl Iterator<Item = &str> {
        self.content.iter().filter_map(|piece| match piece {
            Piece::Text { text, .. } => Some(text.as_str()),
            Piece::Other { .. } => None,
        })
    }

    /// Whether it is content, what `until_content` commits on: a start, content or
    /// a finish. A keep-alive or usage alone is not.
    pub fn is_content(&self) -> bool {
        self.start.is_some() || !self.content.is_empty() || self.finish.is_some()
    }

    /// For formats: decoded by `format` from `bytes`.
    pub fn came(mut self, format: &Arc<str>, bytes: Bytes) -> Self {
        self.origin = Relayed::default();
        self.origin = Relayed::came(format, bytes, self.clone());
        self
    }

    /// The message it was decoded from, if `format` decoded it.
    pub fn origin(&self, format: &str) -> Option<&Origin<Reply>> {
        self.origin.origin(format)
    }
}

/// Why the model stopped.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Finish {
    /// A natural end or a stop sequence.
    Stop,
    /// The token limit.
    Length,
    /// The model wants a tool called.
    ToolUse,
    /// A safety filter.
    Filtered,
    /// A reason the kind has no form for, by the format's name.
    Other(String),
}
