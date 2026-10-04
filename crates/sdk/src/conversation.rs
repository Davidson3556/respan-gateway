//! The conversation kind: chats with a language model. Formats translate
//! through these types, and plugins read any conversation through them:
//! `cx.filter(|c: &mut Conversation| ..)` for the input and
//! `cx.filter(|r: &mut Reply| ..)` for the output, one [`Reply`] per event of
//! the reply: a `Reply` is one event, not a whole reply.
//!
//! A reply is a sequence of events that follows the lifecycle of a
//! stream: [`Reply::Start`], then for each content block a
//! [`Reply::BlockStart`], its [`Reply::Delta`]s (each a [`Delta`] of its
//! block's own kind) and a [`Reply::BlockEnd`],
//! with [`Reply::Usage`], [`Reply::Finish`] and [`Reply::Other`] among them.
//! A response that was not streamed decodes into the same sequence, and an
//! encoder collects the events into one body or writes them as a stream, so a
//! plugin handles one shape. [`Assembled`] folds the events into the whole
//! reply for plugins that need it.
//!
//! Tools are the kind's too: the tools a request offers ([`Tool`],
//! [`ToolChoice`]), the calls an assistant made and their results
//! ([`Content::ToolCall`], [`Content::ToolResult`]), and the calls a
//! response makes ([`Block::ToolCall`]), whose arguments stream as JSON text.
//!
//! What the kind has no form for stays as the format that decoded it has it
//! ([`Extra`], [`Content::Other`], [`Reply::Other`]); it is sent only in that
//! format, and translating it is refused if it can change the answer.
//!
//! Values are plain data: they say what they hold and compare by it. They
//! keep nothing of the bytes they were decoded from.

use respan_gateway_core::Fault;
use std::sync::Arc;

use serde_json::{Map, Value};

use crate::{Upstream, Usage};

/// The conversation kind's [`Usage`] unit names, which every conversation
/// format reports alike.
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

/// Fields a format keeps that the kind has no place for, in that format's
/// terms. An encoder writes them only when `format` is its own name;
/// translating to another format drops them unless doing so could change the
/// answer, in which case it is refused. A format that decodes a value sets
/// `format` whenever `fields` says something, or the value needs it, as a
/// [`Role::Other`] does.
///
/// `fields` may also hold a field's own spelling, such as the key a limit
/// was written under: the format that decoded it honours it when it writes
/// the value again, and any other format writes its own spelling and drops
/// it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Extra {
    /// The name of the format `fields` are in.
    pub format: Option<Arc<str>>,
    /// The fields, as that format has them.
    pub fields: Map<String, Value>,
}

impl Extra {
    /// These `fields` of `format`.
    pub fn new(format: impl Into<Arc<str>>, fields: Map<String, Value>) -> Self {
        Self {
            format: Some(format.into()),
            fields,
        }
    }

    /// Whether it holds no field.
    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }
}

/// The kind's input: a conversation request.
#[derive(Debug, Clone, Default, PartialEq)]
#[non_exhaustive]
pub struct Conversation {
    /// The model asked for, if any.
    pub model: Option<String>,
    /// The messages, in order.
    pub messages: Vec<Message>,
    /// Whether the request asks for a stream: the framing the client asked
    /// for, which the value carries so that a translated call asks the
    /// provider for the same.
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
    /// The tools the model may call.
    pub tools: Vec<Tool>,
    /// Whether, or which, tool the model must call, if the request says.
    pub tool_choice: Option<ToolChoice>,
    /// Whether the model may call several tools at once, if the request
    /// says.
    pub parallel_tool_calls: Option<bool>,
    /// The request's fields the kind has no place for, such as tools a
    /// provider runs itself.
    pub extra: Extra,
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
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct Message {
    /// Its author.
    pub role: Role,
    /// What it says, part by part.
    pub content: Vec<Content>,
    /// The message's fields the kind has no place for, such as a name. A
    /// [`Role::Other`] is in the format of `extra`.
    pub extra: Extra,
}

impl Message {
    /// A message of one part.
    pub fn new(role: Role, content: impl Into<Content>) -> Self {
        Self {
            role,
            content: vec![content.into()],
            extra: Extra::default(),
        }
    }

    /// The message's text parts, joined; tool calls, results and other
    /// content are skipped.
    pub fn text(&self) -> String {
        self.content
            .iter()
            .map(|c| match c {
                Content::Text { text, .. } => text.as_str(),
                _ => "",
            })
            .collect()
    }

    /// Whether the kind has a place for all of it: a known role, no extra
    /// fields on the message, and text parts (whose own extra fields, such as
    /// a cache marker, do not change what they say).
    pub fn is_plain(&self) -> bool {
        let text = |c: &Content| matches!(c, Content::Text { .. });
        !matches!(self.role, Role::Other { .. })
            && self.extra.is_empty()
            && self.content.iter().all(text)
    }
}

/// Who wrote a message.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Role {
    /// Instructions for the model.
    System,
    /// Instructions for the model with a developer's authority, as OpenAI
    /// defines them; a format that has no such role writes them as system.
    Developer,
    /// The person or program using the model.
    User,
    /// The model.
    Assistant,
    /// A role the kind has no form for, as `format` names it; only a call
    /// in that format can send it.
    #[non_exhaustive]
    Other {
        /// The name of the format `name` is in.
        format: Arc<str>,
        /// The role, as that format names it.
        name: String,
    },
}

impl Role {
    /// The role `name` of `format`, which the kind has no form for.
    pub fn other(format: impl Into<Arc<str>>, name: impl Into<String>) -> Self {
        Role::Other {
            format: format.into(),
            name: name.into(),
        }
    }
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
        /// The part's fields the kind has no place for, such as a cache
        /// marker.
        extra: Extra,
    },
    /// A tool call an assistant made.
    #[non_exhaustive]
    ToolCall {
        /// The call's id, which its result names.
        id: String,
        /// The tool's name.
        name: String,
        /// Its arguments, as JSON text.
        arguments: String,
        /// The part's fields the kind has no place for.
        extra: Extra,
    },
    /// What a tool answered a call.
    #[non_exhaustive]
    ToolResult {
        /// The id of the call it answers.
        id: String,
        /// What the tool said.
        content: Vec<Content>,
        /// Whether the call failed.
        is_error: bool,
        /// The part's fields the kind has no place for.
        extra: Extra,
    },
    /// A part the kind has no form for, such as an image or a file, as
    /// `format` has it; only a call in that format can send it.
    #[non_exhaustive]
    Other {
        /// The name of the format `raw` is in.
        format: Arc<str>,
        /// The part as that format has it.
        raw: Arc<Value>,
    },
}

impl Content {
    /// A call of tool `name`, with its `id` and its `arguments` as JSON text.
    pub fn tool_call(
        id: impl Into<String>,
        name: impl Into<String>,
        arguments: impl Into<String>,
    ) -> Self {
        Content::ToolCall {
            id: id.into(),
            name: name.into(),
            arguments: arguments.into(),
            extra: Extra::default(),
        }
    }

    /// The answer to call `id`: what the tool said, and whether it failed.
    pub fn tool_result(id: impl Into<String>, content: Vec<Content>, is_error: bool) -> Self {
        Content::ToolResult {
            id: id.into(),
            content,
            is_error,
            extra: Extra::default(),
        }
    }

    /// A part of `format` the kind has no form for.
    pub fn other(format: impl Into<Arc<str>>, raw: impl Into<Value>) -> Self {
        Content::Other {
            format: format.into(),
            raw: Arc::new(raw.into()),
        }
    }
}

/// A tool a request offers the model.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct Tool {
    /// Its name, which a call names.
    pub name: String,
    /// What it does, for the model.
    pub description: Option<String>,
    /// The JSON schema of its arguments.
    pub parameters: Value,
    /// Its fields the kind has no place for, such as a cache marker.
    pub extra: Extra,
}

impl Tool {
    /// A tool `name` whose arguments `parameters`, a JSON schema, describes.
    pub fn new(name: impl Into<String>, parameters: Value) -> Self {
        Self {
            name: name.into(),
            description: None,
            parameters,
            extra: Extra::default(),
        }
    }
}

/// Whether, or which, tool the model must call.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ToolChoice {
    /// The model decides.
    Auto,
    /// No tool.
    None,
    /// Some tool.
    Required,
    /// The tool of this name.
    Tool(String),
}

impl From<&str> for Content {
    fn from(text: &str) -> Self {
        Content::Text {
            text: text.into(),
            extra: Extra::default(),
        }
    }
}

impl From<String> for Content {
    fn from(text: String) -> Self {
        Content::Text {
            text,
            extra: Extra::default(),
        }
    }
}

/// The kind's output: one event of a reply, streamed or not (a `Reply` is
/// not a whole reply; [`Assembled`] is the whole reply, folded from its
/// events). Filters see every event, those that are no content ([`Reply::is_content`]) included,
/// such as a usage report.
///
/// Blocks are numbered by the format that decodes them, and may be open
/// together; an event names its block.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum Reply {
    /// The response begins: its id and model.
    Start(Start),
    /// Content block `block` opens.
    BlockStart {
        /// The block, numbered in the order blocks open.
        block: u32,
        /// What it holds.
        kind: Block,
    },
    /// The next part of block `block`, of the block's own kind.
    Delta {
        /// The block it belongs to.
        block: u32,
        /// The part, which says what it holds.
        delta: Delta,
    },
    /// Block `block` is complete.
    BlockEnd {
        /// The block that is complete.
        block: u32,
    },
    /// Usage the provider reports, as an increment. A plugin that meters a
    /// call sums it in a provider-edge filter, which sees every event.
    Usage(Usage),
    /// Why the model stopped.
    Finish(Finish),
    /// An event the kind has no form for, as `format` has it (pings,
    /// signatures, provider-specific events). Sent only in that format.
    Other {
        /// The name of the format `raw` is in.
        format: Arc<str>,
        /// The event as that format has it.
        raw: Arc<Value>,
    },
}

/// Which response the events belong to.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Start {
    /// The response's id.
    pub id: String,
    /// The model that replied.
    pub model: String,
    /// The response's fields the kind has no place for, such as a
    /// fingerprint.
    pub extra: Extra,
}

/// What a content block holds.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Block {
    /// Text.
    Text,
    /// A tool call the model makes; its [`Delta::Arguments`] are its
    /// arguments, as JSON text.
    ToolCall {
        /// The call's id, which its result names.
        id: String,
        /// The tool's name.
        name: String,
    },
    /// The model's reasoning.
    Reasoning,
}

/// The next part of a block, of the block's own kind.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Delta {
    /// Text of a [`Block::Text`].
    Text(String),
    /// The next part of a [`Block::ToolCall`]'s arguments, as JSON text.
    Arguments(String),
    /// Reasoning of a [`Block::Reasoning`].
    Reasoning(String),
}

impl Delta {
    /// The text it holds, whatever its kind.
    pub fn as_str(&self) -> &str {
        match self {
            Delta::Text(s) | Delta::Arguments(s) | Delta::Reasoning(s) => s,
        }
    }

    /// The text it holds, to append to.
    pub fn as_mut_string(&mut self) -> &mut String {
        match self {
            Delta::Text(s) | Delta::Arguments(s) | Delta::Reasoning(s) => s,
        }
    }
}

impl Start {
    /// The start of a response of this `id` and `model`.
    pub fn new(id: impl Into<String>, model: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            model: model.into(),
            extra: Extra::default(),
        }
    }
}

impl Reply {
    /// The start of a response of this `id` and `model`.
    pub fn start(id: impl Into<String>, model: impl Into<String>) -> Self {
        Reply::Start(Start::new(id, model))
    }

    /// Block `block` opens as `kind`.
    pub fn block_start(block: u32, kind: Block) -> Self {
        Reply::BlockStart { block, kind }
    }

    /// The next text of the [`Block::Text`] `block`.
    pub fn text(block: u32, text: impl Into<String>) -> Self {
        Reply::Delta {
            block,
            delta: Delta::Text(text.into()),
        }
    }

    /// The next part of the arguments of the [`Block::ToolCall`] `block`.
    pub fn arguments(block: u32, json: impl Into<String>) -> Self {
        Reply::Delta {
            block,
            delta: Delta::Arguments(json.into()),
        }
    }

    /// The next reasoning of the [`Block::Reasoning`] `block`.
    pub fn reasoning(block: u32, text: impl Into<String>) -> Self {
        Reply::Delta {
            block,
            delta: Delta::Reasoning(text.into()),
        }
    }

    /// Block `block` is complete.
    pub fn block_end(block: u32) -> Self {
        Reply::BlockEnd { block }
    }

    /// Usage the provider reports, as an increment.
    pub fn usage(usage: Usage) -> Self {
        Reply::Usage(usage)
    }

    /// The model stopped for this reason.
    pub fn finished(finish: Finish) -> Self {
        Reply::Finish(finish)
    }

    /// An event of `format` the kind has no form for.
    pub fn other(format: impl Into<Arc<str>>, raw: impl Into<Value>) -> Self {
        Reply::Other {
            format: format.into(),
            raw: Arc::new(raw.into()),
        }
    }

    /// Whether it is content: a start, a block event or a finish. Usage and
    /// [`Reply::Other`] are not.
    pub fn is_content(&self) -> bool {
        !matches!(self, Reply::Usage(_) | Reply::Other { .. })
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
    /// A reason the kind has no form for, as `format` names it; only a
    /// response in that format can say it.
    #[non_exhaustive]
    Other {
        /// The name of the format `reason` is in.
        format: Arc<str>,
        /// The reason, as that format names it.
        reason: String,
    },
}

impl Finish {
    /// The reason `reason` of `format`, which the kind has no form for.
    pub fn other(format: impl Into<Arc<str>>, reason: impl Into<String>) -> Self {
        Finish::Other {
            format: format.into(),
            reason: reason.into(),
        }
    }
}

/// The events of a response folded into the whole answer, for plugins that
/// need it (a judge, a cache, a job store). Unary and streamed responses
/// fold alike.
#[derive(Debug, Clone, PartialEq)]
pub struct Assembled {
    /// The response's start, if one came.
    pub start: Option<Start>,
    /// The answer: the assistant's text and tool calls, one part per block,
    /// in the order the blocks opened.
    pub message: Message,
    /// The reasoning of each reasoning block, in the order they opened.
    pub reasoning: Vec<String>,
    /// Why the model stopped, if it said.
    pub finish: Option<Finish>,
    /// All the usage reported.
    pub usage: Usage,
    /// The [`Reply::Other`] events, in order.
    pub other: Vec<Reply>,
    blocks: Vec<Folding>,
}

/// A block being folded: where its content goes.
#[derive(Debug, Clone, PartialEq)]
struct Folding {
    block: u32,
    place: Place,
    ended: bool,
}

#[derive(Debug, Clone, PartialEq)]
enum Place {
    /// An index in the message's content.
    Content(usize),
    /// An index in the reasoning.
    Reasoning(usize),
}

/// An empty answer: an assistant's message with no content yet.
impl Default for Assembled {
    fn default() -> Self {
        Self {
            start: None,
            message: Message {
                role: Role::Assistant,
                content: Vec::new(),
                extra: Extra::default(),
            },
            reasoning: Vec::new(),
            finish: None,
            usage: Usage::default(),
            other: Vec::new(),
            blocks: Vec::new(),
        }
    }
}

impl Assembled {
    /// Fold the next event of the response in. A sequence that cannot be a
    /// response is an `Err`: a start twice, a block that opens as another
    /// kind, a delta of another kind than its block, a delta or an end for a block that is not open.
    pub fn push(&mut self, reply: &Reply) -> Result<(), Fault> {
        match reply {
            Reply::Start(start) => match self.start {
                Some(_) => return Err(malformed("the response starts twice")),
                None => self.start = Some(start.clone()),
            },
            Reply::BlockStart { block, kind } => self.open(*block, kind)?,
            Reply::Delta { block, delta } => {
                let place = self.open_place(*block)?;
                let (held, part) = match (place, delta) {
                    (Place::Reasoning(at), Delta::Reasoning(part)) => {
                        (&mut self.reasoning[at], part)
                    }
                    (Place::Content(at), delta) => match (&mut self.message.content[at], delta) {
                        (Content::Text { text, .. }, Delta::Text(part)) => (text, part),
                        (Content::ToolCall { arguments, .. }, Delta::Arguments(part)) => {
                            (arguments, part)
                        }
                        _ => return Err(malformed("a delta is not of its block's kind")),
                    },
                    _ => return Err(malformed("a delta is not of its block's kind")),
                };
                held.push_str(part);
            }
            Reply::BlockEnd { block } => {
                self.open_place(*block)?;
                if let Some(folding) = self.blocks.iter_mut().find(|f| f.block == *block) {
                    folding.ended = true;
                }
            }
            Reply::Usage(usage) => self.usage += usage,
            Reply::Finish(finish) => self.finish = Some(finish.clone()),
            Reply::Other { .. } => self.other.push(reply.clone()),
        }
        Ok(())
    }

    fn open(&mut self, block: u32, kind: &Block) -> Result<(), Fault> {
        if let Some(had) = self.blocks.iter().find(|f| f.block == block) {
            // A start repeated for a block, as a filter that adds to it may
            // write, opens nothing more.
            let same = match (&had.place, kind) {
                (Place::Reasoning(_), Block::Reasoning) => true,
                (Place::Content(at), kind) => match (&self.message.content[*at], kind) {
                    (Content::Text { .. }, Block::Text) => true,
                    (Content::ToolCall { id, name, .. }, Block::ToolCall { id: i, name: n }) => {
                        id == i && name == n
                    }
                    _ => false,
                },
                _ => false,
            };
            return match (same, had.ended) {
                (true, false) => Ok(()),
                _ => Err(malformed("a block opens twice")),
            };
        }
        let place = match kind {
            Block::Text => {
                self.message.content.push(Content::from(String::new()));
                Place::Content(self.message.content.len() - 1)
            }
            Block::ToolCall { id, name } => {
                self.message.content.push(Content::tool_call(id, name, ""));
                Place::Content(self.message.content.len() - 1)
            }
            Block::Reasoning => {
                self.reasoning.push(String::new());
                Place::Reasoning(self.reasoning.len() - 1)
            }
        };
        self.blocks.push(Folding {
            block,
            place,
            ended: false,
        });
        Ok(())
    }

    fn open_place(&self, block: u32) -> Result<Place, Fault> {
        match self.blocks.iter().find(|f| f.block == block) {
            Some(Folding {
                place,
                ended: false,
                ..
            }) => Ok(place.clone()),
            Some(_) => Err(malformed("an event names a block that has ended")),
            None => Err(malformed("an event names a block that never opened")),
        }
    }
}

/// A sequence of events that is no response.
fn malformed(what: &str) -> Fault {
    Upstream::fault(format!("the response is malformed: {what}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn fold(events: &[Reply]) -> Result<Assembled, Fault> {
        let mut all = Assembled::default();
        events.iter().try_for_each(|e| all.push(e))?;
        Ok(all)
    }

    #[test]
    fn plain_values_compare_by_what_they_say() {
        let a = Message::new(Role::User, "hi");
        let mut b = a.clone();
        assert_eq!(a, b);
        b.extra = Extra::new(
            "chat",
            json!({"name": "x"})
                .as_object()
                .cloned()
                .unwrap_or_default(),
        );
        assert_ne!(a, b);
        assert!(a.is_plain() && !b.is_plain());
        let tagged = Message::new(Role::other("chat", "tool"), "x");
        assert!(!tagged.is_plain());
    }

    #[test]
    fn parts_roles_and_finishes_carry_their_format_and_extra() {
        let marker = Extra::new(
            "chat",
            json!({"cache_control": 1})
                .as_object()
                .cloned()
                .unwrap_or_default(),
        );
        let mut cached = Message::new(Role::User, "hi");
        if let Content::Text { extra, .. } = &mut cached.content[0] {
            *extra = marker;
        }
        // A part's extra does not change what it says.
        assert!(cached.is_plain());
        assert_ne!(cached, Message::new(Role::User, "hi"));
        assert_eq!(Role::other("chat", "critic"), Role::other("chat", "critic"));
        assert_ne!(
            Role::other("chat", "critic"),
            Role::other("other", "critic")
        );
        assert_ne!(Finish::other("a", "x"), Finish::other("b", "x"));
    }

    #[test]
    fn events_fold_into_the_whole_answer() {
        let mut usage = Usage::default();
        usage.add(units::INPUT_TOKENS, 3);
        let all = fold(&[
            Reply::start("r1", "m"),
            Reply::block_start(0, Block::Reasoning),
            Reply::reasoning(0, "hm"),
            Reply::block_end(0),
            Reply::block_start(1, Block::Text),
            Reply::text(1, "Hel"),
            Reply::text(1, "lo"),
            Reply::block_end(1),
            Reply::block_start(
                2,
                Block::ToolCall {
                    id: "t".into(),
                    name: "f".into(),
                },
            ),
            Reply::arguments(2, "{\"a\":"),
            Reply::arguments(2, "1}"),
            Reply::block_end(2),
            Reply::other("chat", json!({"type": "ping"})),
            Reply::usage(usage.clone()),
            Reply::usage(usage.clone()),
            Reply::finished(Finish::ToolUse),
        ])
        .expect("a response");
        assert_eq!(all.start.as_ref().map(|s| s.id.as_str()), Some("r1"));
        assert_eq!(all.reasoning, ["hm"]);
        assert_eq!(
            all.message.content,
            [
                Content::from("Hello"),
                Content::tool_call("t", "f", "{\"a\":1}")
            ]
        );
        assert_eq!(all.message.role, Role::Assistant);
        assert_eq!(all.finish, Some(Finish::ToolUse));
        assert_eq!(all.usage.get(units::INPUT_TOKENS), Some(6));
        assert_eq!(all.other, [Reply::other("chat", json!({"type": "ping"}))]);
    }

    #[test]
    fn a_sequence_that_is_no_response_is_refused() {
        let text = || Reply::block_start(0, Block::Text);
        for events in [
            vec![Reply::text(0, "x")],
            vec![Reply::block_end(0)],
            vec![text(), Reply::block_end(0), Reply::text(0, "x")],
            vec![text(), Reply::block_start(0, Block::Reasoning)],
            vec![text(), Reply::reasoning(0, "x")],
            vec![text(), Reply::arguments(0, "{}")],
            vec![Reply::block_start(0, Block::Reasoning), Reply::text(0, "x")],
            vec![Reply::start("a", "m"), Reply::start("b", "m")],
        ] {
            let end = fold(&events).err();
            assert!(end.is_some_and(|f| f.is::<Upstream>()), "{events:?}");
        }
        // A start repeated for an open block, as a filter adding to it writes.
        assert!(fold(&[text(), Reply::text(0, "a"), text(), Reply::text(0, "b")]).is_ok());
    }

    #[test]
    fn a_delta_of_another_kind_than_its_block_is_refused() {
        let call = Block::ToolCall {
            id: "t".into(),
            name: "f".into(),
        };
        for (kind, wrong) in [
            (Block::Text, Reply::arguments(0, "{}")),
            (Block::Text, Reply::reasoning(0, "x")),
            (call.clone(), Reply::text(0, "x")),
            (call, Reply::reasoning(0, "x")),
            (Block::Reasoning, Reply::text(0, "x")),
            (Block::Reasoning, Reply::arguments(0, "{}")),
        ] {
            let mut all = Assembled::default();
            all.push(&Reply::block_start(0, kind.clone()))
                .expect("opens");
            let end = all.push(&wrong).err();
            assert!(
                end.is_some_and(|f| f.is::<Upstream>()),
                "{kind:?} {wrong:?}"
            );
        }
    }

    #[test]
    fn only_usage_and_other_events_are_no_content() {
        assert!(Reply::start("a", "m").is_content());
        assert!(Reply::text(0, "x").is_content());
        assert!(Reply::finished(Finish::Stop).is_content());
        assert!(!Reply::usage(Usage::default()).is_content());
        assert!(!Reply::other("f", json!(1)).is_content());
    }
}
