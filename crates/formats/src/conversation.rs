//! The conversation kind: chats with a language model. OpenAI Chat
//! Completions and Anthropic Messages translate to each other through these
//! types, and so do plugins for any conversation, such as PII redaction:
//! `cx.input_mut::<Conversation>()` for the input and
//! `cx.filter(|r: &mut Reply| ..)` for the output, streamed or not.
//!
//! What the kind has no form for stays as the client's format has it: a
//! request's fields in [`Conversation::unsupported`], a message's in
//! [`Message::unsupported`], a content part as [`Content::Other`]. A
//! provider of the client's format gets it as it is. For another format the
//! request is refused, unless the field cannot change the answer: a cache
//! hint, a user id, a field at its default.

use respan_gateway_core::serde_json;
use std::sync::Arc;

use serde_json::{Map, Value};

/// Usage units conversation formats report in the same sense, so usage
/// survives translation.
pub mod units {
    /// Every input token, cached or not.
    pub const INPUT_TOKENS: &str = "input_tokens";
    /// Every output token.
    pub const OUTPUT_TOKENS: &str = "output_tokens";
    /// The input tokens read from a cache.
    pub const CACHED_INPUT_TOKENS: &str = "cached_input_tokens";
    /// The input tokens written to a cache, which some providers charge
    /// separately.
    pub const CACHE_WRITE_INPUT_TOKENS: &str = "cache_write_input_tokens";
    /// The output tokens spent on reasoning.
    pub const REASONING_TOKENS: &str = "reasoning_tokens";
}

/// The kind's input: a conversation request. The kernel holds its model,
/// as [`Context::model`](respan_gateway_core::Context::model).
#[derive(Debug, Clone, Default, PartialEq)]
#[non_exhaustive]
pub struct Conversation {
    /// The messages, in order.
    pub messages: Vec<Message>,
    /// Whether the request asks for a stream. The client always gets the
    /// framing its own request asked for; a response framed otherwise, such
    /// as after a plugin changed this, is written anew from its events.
    pub stream: bool,
    /// The most tokens the answer may have.
    pub max_tokens: Option<u64>,
    /// The sampling temperature.
    pub temperature: Option<f64>,
    /// The nucleus sampling cutoff.
    pub top_p: Option<f64>,
    /// Sequences that stop the answer.
    pub stop: Vec<String>,
    /// The request's fields the kind has no place for, such as its tools.
    /// A field a plugin removes is not sent.
    pub unsupported: Map<String, Value>,
}

impl Conversation {
    /// A request of these `messages` alone, such as a judge's own prompt.
    pub fn new(messages: impl IntoIterator<Item = Message>) -> Self {
        Self {
            messages: messages.into_iter().collect(),
            ..Self::default()
        }
    }
}

/// One message of a conversation: who said it and what.
#[derive(Debug, Clone)]
pub struct Message {
    /// Who said it.
    pub role: Role,
    /// What it says, part by part.
    pub content: Vec<Content>,
    /// The message's fields the kind has no place for, such as a tool call
    /// or a name.
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

    /// Records `source`, the client's message this was decoded from; for
    /// formats, not plugins. An unchanged message is sent as `source`, a
    /// changed one written in its shape. A message is the client's if it
    /// shares the source ([`Message::same_source`]), not if its content is
    /// equal, so one given a new source is written anew.
    pub fn with_source(mut self, source: Value) -> Self {
        self.source = Some(Arc::new(source));
        self
    }

    /// The client's message this was decoded from, if any.
    pub fn source(&self) -> Option<&Value> {
        self.source.as_deref()
    }

    /// Whether `self` and `other` were decoded from the same client
    /// message, wherever plugins moved them: a client message, edited or
    /// not, rather than a new one.
    pub fn same_source(&self, other: &Message) -> bool {
        matches!((&self.source, &other.source), (Some(a), Some(b)) if Arc::ptr_eq(a, b))
    }

    /// The message's text parts, joined; other content is skipped. A plugin
    /// that must see all of a message checks [`Message::is_plain`].
    pub fn text(&self) -> String {
        self.content
            .iter()
            .map(|c| match c {
                Content::Text { text } => text.as_str(),
                Content::Other { .. } => "",
            })
            .collect()
    }

    /// Whether the kind holds all of the message: a known role, text parts
    /// and nothing else. A guard refuses any other message as unreadable.
    pub fn is_plain(&self) -> bool {
        let text = |c: &Content| matches!(c, Content::Text { .. });
        !matches!(self.role, Role::Other(_))
            && self.unsupported.is_empty()
            && self.content.iter().all(text)
    }
}

/// Who said a message.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Role {
    /// Instructions for the model, which some formats allow only before the
    /// conversation.
    System,
    /// The person or program using the model.
    User,
    /// The model.
    Assistant,
    /// A role the kind has no form for, such as a tool result's, by the name
    /// the client's format gives it.
    Other(String),
}

/// A part of a message. New forms, such as files, become new variants, so
/// a plugin that must see all content handles the variants it knows and
/// refuses the rest.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum Content {
    /// Text.
    #[non_exhaustive]
    Text {
        /// The text.
        text: String,
    },
    /// A part the kind has no form for, such as an image, as the client's
    /// format has it. Shared, as it may be large.
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

/// The kind's output: one event of a response, streamed or whole.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum Reply {
    /// Which response this is: the provider's id and model, even after
    /// translation, or a plugin's own. A stream may repeat it, as OpenAI's
    /// chunks do, so it marks no boundary in the content.
    #[non_exhaustive]
    Start {
        /// The response's id.
        id: String,
        /// The model that answered.
        model: String,
    },
    /// Text of content block `block`.
    #[non_exhaustive]
    Text {
        /// The content block, numbered in the order blocks open.
        block: u32,
        /// The text, or a delta of it in a stream.
        text: String,
    },
    /// Content the kind has no form for, such as a tool call or reasoning,
    /// as the provider's `format` has it. It reaches clients of that format;
    /// translating it is refused. A plugin that must see all content
    /// handles the variants it knows and refuses the rest. Shared, as it may
    /// be large.
    #[non_exhaustive]
    Other {
        /// The name of the format `raw` is in.
        format: Arc<str>,
        /// The content as that format has it.
        raw: Arc<Value>,
    },
    /// Why the model stopped.
    Finish(Finish),
}

impl Reply {
    /// Which response this is.
    pub fn start(id: impl Into<String>, model: impl Into<String>) -> Self {
        Reply::Start {
            id: id.into(),
            model: model.into(),
        }
    }

    /// Content of `format` the kind has no form for.
    pub fn other(format: impl Into<Arc<str>>, raw: impl Into<Value>) -> Self {
        Reply::Other {
            format: format.into(),
            raw: Arc::new(raw.into()),
        }
    }

    /// Text of content block `block`.
    pub fn text(block: u32, text: impl Into<String>) -> Self {
        Reply::Text {
            block,
            text: text.into(),
        }
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
    /// A reason the kind has no form for, by the name the format gives it.
    Other(String),
}
