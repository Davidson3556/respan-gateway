//! Responses the gateway keeps (`plugins.responses`), so a Responses client's
//! `previous_response_id` works with a provider of another format, which
//! keeps nothing: the conversation it names is sent in its place.
//!
//! Only a response translated from Responses is kept or expanded; OpenAI's
//! own Responses keep their conversations themselves. A kept response is the
//! one its client was given, whatever the calls behind it (a tool loop's
//! several, or a retry's), under an id the gateway mints, `resp_…`: random,
//! so no provider's ids collide or are guessed. It holds its turn alone (the
//! messages its request added and the reply), beside the response it
//! followed, and a conversation is read back by following them; reading it
//! keeps every turn of it as fresh as its last. It is kept for the client
//! key that made it, for `keep`, in memory within [`BUDGET`]: not across a
//! restart, nor shared between gateways. A request that says `store: false`
//! is not kept.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use respan_gateway_conversation::{Assembled, Content, Input, Message, Output, Role};
use respan_gateway_conversation_http::Schema;
use respan_gateway_conversation_http::responses::OpenAiResponses;
use respan_gateway_core::{
    ClientContext, Fault, Filter, Plugin, ProviderContext, Result, async_trait,
};
use respan_gateway_http::raw::Original;
use respan_gateway_mcp_http::Owner;
use serde_json::Value;

/// The bytes of messages kept at most, roughly; the stalest go first.
const BUDGET: usize = 256 << 20;
/// How many responses a conversation follows back at most.
const MAX_DEPTH: usize = 1_000;

const RESPONSES: &str = OpenAiResponses::NAME;

pub struct Responses {
    keep: Duration,
    kept: Arc<Mutex<Kept>>,
}

impl std::fmt::Debug for Responses {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Responses")
            .field("keep", &self.keep)
            .finish_non_exhaustive()
    }
}

impl Responses {
    /// Responses kept for `keep`.
    pub fn new(keep: Duration) -> Self {
        Self {
            keep,
            kept: Arc::default(),
        }
    }
}

/// The responses kept, by id, and their bytes.
#[derive(Default)]
struct Kept {
    turns: HashMap<String, Turn>,
    bytes: usize,
}

/// One response's turn.
struct Turn {
    /// The client key's id that made it.
    owner: Option<String>,
    /// The response it followed.
    parent: Option<String>,
    /// The messages its request added, and the reply.
    messages: Vec<Message>,
    bytes: usize,
    /// When it was made or last read.
    at: Instant,
}

impl Kept {
    /// The conversation up to and including response `id`, oldest first, if
    /// it was kept for `owner` and none of it expired; each turn of it is
    /// kept afresh.
    fn conversation(
        &mut self,
        keep: Duration,
        owner: &Option<String>,
        id: &str,
    ) -> Option<Vec<Message>> {
        let mut ids = Vec::new();
        let mut next = Some(id.to_owned());
        while let Some(id) = next.take() {
            let turn = self.turns.get(&id)?;
            if &turn.owner != owner || turn.at.elapsed() > keep || ids.len() == MAX_DEPTH {
                return None;
            }
            next = turn.parent.clone();
            ids.push(id);
        }
        let now = Instant::now();
        let mut messages = Vec::new();
        for id in ids.iter().rev() {
            let turn = self.turns.get_mut(id)?;
            turn.at = now;
            messages.extend(turn.messages.iter().cloned());
        }
        Some(messages)
    }

    /// Keeps `turn` as response `id`, dropping what expired, then the
    /// stalest beyond [`BUDGET`].
    fn keep(&mut self, keep: Duration, id: String, turn: Turn) {
        self.bytes += turn.bytes;
        if let Some(had) = self.turns.insert(id, turn) {
            self.bytes -= had.bytes;
        }
        let expired: Vec<String> = self
            .turns
            .iter()
            .filter(|(_, t)| t.at.elapsed() > keep)
            .map(|(id, _)| id.clone())
            .collect();
        expired.iter().for_each(|id| self.drop_turn(id));
        while self.bytes > BUDGET {
            let stalest = self
                .turns
                .iter()
                .min_by_key(|(_, t)| t.at)
                .map(|(id, _)| id.clone());
            match stalest {
                Some(id) => self.drop_turn(&id),
                None => break,
            }
        }
    }

    fn drop_turn(&mut self, id: &str) {
        if let Some(turn) = self.turns.remove(id) {
            self.bytes -= turn.bytes;
        }
    }
}

fn lock(kept: &Mutex<Kept>) -> MutexGuard<'_, Kept> {
    kept.lock().unwrap_or_else(PoisonError::into_inner)
}

/// What a client's request adds to its conversation, once a call of it is
/// translated; shared by the request's client edge and its calls.
#[derive(Clone, Default)]
struct Pending(Arc<Mutex<Shared>>);

#[derive(Default)]
struct Shared {
    /// What the first translated call added.
    request: Option<Request>,
    /// Whether the request named `instructions`, a leading system message
    /// apart from its input.
    instructed: bool,
    /// Whether a call went to OpenAI's own Responses, which keeps its
    /// response itself: the response then is not the gateway's to keep, as
    /// which call answered (a hedge's race) is not known here.
    openai: bool,
}

struct Request {
    parent: Option<String>,
    added: Vec<Message>,
    store: bool,
}

impl Pending {
    fn lock(&self) -> MutexGuard<'_, Shared> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

#[async_trait]
impl Plugin for Responses {
    async fn on_client(
        &self,
        cx: &mut ClientContext,
    ) -> Result<Option<respan_gateway_core::Response>> {
        if cx.format() != RESPONSES || !cx.is_kind::<Input>() {
            return Ok(None);
        }
        let pending = Pending::default();
        let original = cx.extensions().get::<Original>().and_then(Original::get);
        pending.lock().instructed = original.is_some_and(|r| {
            let v: Value = serde_json::from_slice(r.body()).unwrap_or_default();
            v["instructions"].as_str().is_some_and(|i| !i.is_empty())
        });
        cx.extensions_mut().insert(pending.clone());
        cx.filter(Keep {
            kept: self.kept.clone(),
            keep: self.keep,
            owner: cx.extensions().get::<Owner>().map(|Owner(id)| id.clone()),
            pending,
            id: None,
            said: Assembled::default(),
        })?;
        Ok(None)
    }

    async fn on_provider(&self, px: &mut ProviderContext) -> Result<()> {
        // A call of another kind, such as a tool loop's to its tool server,
        // carries the request's extensions too.
        let pending = px.extensions().get::<Pending>().cloned();
        let Some(pending) = pending.filter(|_| px.is_kind::<Input>()) else {
            return Ok(());
        };
        if px.format() == RESPONSES {
            pending.lock().openai = true;
            return Ok(());
        }
        let owner = px.extensions().get::<Owner>().map(|Owner(id)| id.clone());
        let (kept, keep, target) = (self.kept.clone(), self.keep, px.format().to_owned());
        px.filter(move |input: &mut Input| {
            let ours = input.native.as_mut().filter(|n| &*n.format == RESPONSES);
            let (parent, store) = match ours {
                Some(native) => {
                    let parent = native.fields.shift_remove("previous_response_id");
                    let unkept = native.fields.get("inert").map(|i| &i["store"])
                        == Some(&Value::Bool(false));
                    (parent.and_then(|p| p.as_str().map(str::to_owned)), !unkept)
                }
                None => (None, true),
            };
            let leading = input
                .messages
                .first()
                .is_some_and(|m| m.role == Role::System);
            // The instructions are the request's own, not the conversation's.
            let instructions = usize::from(pending.lock().instructed && leading);
            let added = input.messages[instructions..].to_vec();
            if let Some(id) = &parent {
                let before = lock(&kept).conversation(keep, &owner, id).ok_or_else(|| {
                    Fault::bad_request(format!(
                        "previous_response_id '{id}' is no response this gateway keeps: it keeps \
                         a response of a provider of another format for {}s, for the client key \
                         that made it, in a conversation of up to {MAX_DEPTH} responses",
                        keep.as_secs()
                    ))
                })?;
                let before = before.into_iter().map(|m| writable(m, &target));
                input.messages.splice(instructions..instructions, before);
            }
            // A tool loop's later calls add its own steps: the client's
            // request is what its first call added.
            pending.lock().request.get_or_insert(Request {
                parent,
                added,
                store,
            });
            Ok(())
        })
    }
}

/// `message` without what `format` cannot write: reasoning or a native part
/// of another format, which only its own provider reads back.
fn writable(mut message: Message, format: &str) -> Message {
    message.content.retain(|part| match part {
        Content::Reasoning { native, .. } => native.as_ref().is_some_and(|n| &*n.format == format),
        Content::Native(native) => &*native.format == format,
        _ => true,
    });
    message
}

/// The response its client is given, named anew and kept once it ended well.
struct Keep {
    kept: Arc<Mutex<Kept>>,
    keep: Duration,
    owner: Option<String>,
    pending: Pending,
    /// Its id, once named.
    id: Option<String>,
    said: Assembled,
}

impl Filter<Output> for Keep {
    fn edit(&mut self, mut output: Output, out: &mut Vec<Output>) -> Result<()> {
        if let Output::Start(start) = &mut output {
            let shared = self.pending.lock();
            if !shared.openai && shared.request.as_ref().is_some_and(|r| r.store) {
                let id = new_id()?;
                start.id.clone_from(&id);
                self.id = Some(id);
            }
        }
        if self.id.is_some() {
            // What cannot be folded is only not kept; the response goes on.
            let _ = self.said.push(&output);
        }
        out.push(output);
        Ok(())
    }

    fn end(&mut self, end: &Result<()>, _out: &mut Vec<Output>) -> Result<()> {
        let shared = std::mem::take(&mut *self.pending.lock());
        let (Ok(()), Some(id), Some(request), false) =
            (end, self.id.take(), shared.request, shared.openai)
        else {
            return Ok(());
        };
        let mut reply = Message::new(Role::Assistant, "");
        reply.content = self.said.replay();
        let mut messages = request.added;
        messages.push(reply);
        let turn = Turn {
            owner: self.owner.clone(),
            parent: request.parent,
            bytes: messages.iter().map(bytes_of).sum(),
            messages,
            at: Instant::now(),
        };
        lock(&self.kept).keep(self.keep, id, turn);
        Ok(())
    }
}

/// A response id of the gateway's: `resp_` and 24 random hex digits.
fn new_id() -> Result<String> {
    let mut bytes = [0u8; 12];
    getrandom::fill(&mut bytes).map_err(|e| Fault::internal(format!("no randomness: {e}")))?;
    let hex: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
    Ok(format!("resp_{hex}"))
}

/// Roughly the bytes `message` holds: its text and media.
fn bytes_of(message: &Message) -> usize {
    fn part(content: &Content) -> usize {
        match content {
            Content::Text { text, .. } | Content::Reasoning { text, .. } => text.len(),
            Content::Image(media) | Content::Audio(media) | Content::Document(media) => {
                match &media.source {
                    respan_gateway_conversation::MediaSource::Bytes(bytes) => bytes.len(),
                    respan_gateway_conversation::MediaSource::Url(url) => url.len(),
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
