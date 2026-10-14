//! Responses the gateway keeps (`plugins.responses`), so a Responses client's
//! `previous_response_id` works with a provider of another format, which
//! keeps nothing: the conversation it names is sent in its place.
//!
//! Only a call translated from Responses is kept or expanded; OpenAI's own
//! Responses keep their conversations themselves, so a chain stays with one
//! side. Each response holds its turn alone (the messages its request added
//! and the reply), beside the response it followed, so a conversation is
//! read back by following them; it is kept for the client key that made it,
//! for `keep`, in memory: not across a restart, nor shared between gateways.
//! A request that says `store: false` is not kept.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use respan_gateway_conversation::{Assembled, Input, Message, Output, Role};
use respan_gateway_conversation_http::Schema;
use respan_gateway_conversation_http::responses::OpenAiResponses;
use respan_gateway_core::{Fault, Filter, Plugin, ProviderContext, Result, async_trait};
use respan_gateway_mcp_http::Owner;
use serde_json::Value;

/// How many responses are kept at most; the oldest go first.
const MAX_KEPT: usize = 10_000;
/// How many responses a conversation follows back at most.
const MAX_DEPTH: usize = 1_000;

/// One response's turn.
struct Kept {
    /// The client key's id that made it.
    owner: Option<String>,
    /// The response it followed.
    parent: Option<String>,
    /// The messages its request added, and the reply.
    turn: Vec<Message>,
    at: Instant,
}

/// The responses kept, by id.
type Store = Arc<Mutex<HashMap<String, Kept>>>;

pub struct Responses {
    keep: Duration,
    kept: Store,
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
            kept: Store::default(),
        }
    }
}

fn lock(kept: &Store) -> MutexGuard<'_, HashMap<String, Kept>> {
    kept.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The conversation up to and including response `id`, oldest first, if it
/// was kept for `owner` and none of it has expired.
fn conversation(
    kept: &Store,
    keep: Duration,
    owner: &Option<String>,
    id: &str,
) -> Option<Vec<Message>> {
    let kept = lock(kept);
    let mut turns = Vec::new();
    let mut next = Some(id.to_owned());
    while let Some(id) = next.take() {
        let turn = kept.get(&id)?;
        if &turn.owner != owner || turn.at.elapsed() > keep || turns.len() == MAX_DEPTH {
            return None;
        }
        turns.push(&turn.turn);
        next = turn.parent.clone();
    }
    Some(turns.into_iter().rev().flatten().cloned().collect())
}

/// Keeps response `id`, dropping what expired, and the oldest beyond
/// [`MAX_KEPT`].
fn remember(kept: &Store, keep: Duration, id: String, turn: Kept) {
    let mut kept = lock(kept);
    kept.retain(|_, k| k.at.elapsed() <= keep);
    if kept.len() >= MAX_KEPT
        && let Some(oldest) = kept
            .iter()
            .min_by_key(|(_, k)| k.at)
            .map(|(id, _)| id.clone())
    {
        kept.remove(&oldest);
    }
    kept.insert(id, turn);
}

#[async_trait]
impl Plugin for Responses {
    async fn on_provider(&self, px: &mut ProviderContext) -> Result<()> {
        let name = OpenAiResponses::NAME;
        if px.client().format() != name || px.format() == name || !px.is_kind::<Input>() {
            return Ok(());
        }
        let owner = px.extensions().get::<Owner>().map(|Owner(id)| id.clone());
        let turn: Arc<Mutex<Option<Turn>>> = Arc::default();
        let (kept, keep, asked, said) = (self.kept.clone(), self.keep, owner.clone(), turn.clone());
        px.filter(move |input: &mut Input| {
            let ours = input.native.as_mut().filter(|n| &*n.format == name);
            let (previous, store) = match ours {
                Some(native) => {
                    let previous = native.fields.shift_remove("previous_response_id");
                    let store = native.fields.get("inert").map(|i| &i["store"])
                        != Some(&Value::Bool(false));
                    (previous.and_then(|p| p.as_str().map(str::to_owned)), store)
                }
                None => (None, true),
            };
            let added: Vec<Message> = input
                .messages
                .iter()
                .filter(|m| m.role != Role::System)
                .cloned()
                .collect();
            if let Some(id) = &previous {
                let before = conversation(&kept, keep, &asked, id).ok_or_else(|| {
                    Fault::bad_request(format!(
                        "previous_response_id '{id}' is no response this gateway keeps: it keeps \
                         a response of a provider of another format for {}s, for the client key \
                         that made it",
                        keep.as_secs()
                    ))
                })?;
                // After the instructions, which a response does not carry on.
                let at = input
                    .messages
                    .iter()
                    .take_while(|m| m.role == Role::System)
                    .count();
                input.messages.splice(at..at, before);
            }
            *said.lock().unwrap_or_else(PoisonError::into_inner) = Some(Turn {
                parent: previous,
                added,
                store,
            });
            Ok(())
        })?;
        px.filter(Capture {
            kept: self.kept.clone(),
            keep: self.keep,
            owner,
            turn,
            said: Assembled::default(),
        })
    }
}

/// What a request added to its conversation.
struct Turn {
    parent: Option<String>,
    added: Vec<Message>,
    store: bool,
}

/// Keeps a response that ended well, under the id its client is given.
struct Capture {
    kept: Store,
    keep: Duration,
    owner: Option<String>,
    turn: Arc<Mutex<Option<Turn>>>,
    said: Assembled,
}

impl Filter<Output> for Capture {
    fn edit(&mut self, output: Output, out: &mut Vec<Output>) -> Result<()> {
        // What cannot be folded is only not kept; the response goes on.
        let _ = self.said.push(&output);
        out.push(output);
        Ok(())
    }

    fn end(&mut self, end: &Result<()>, _out: &mut Vec<Output>) -> Result<()> {
        let turn = self
            .turn
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        let (Ok(()), Some(start), Some(turn)) = (end, &self.said.start, turn) else {
            return Ok(());
        };
        if !turn.store || start.id.is_empty() {
            return Ok(());
        }
        let mut reply = Message::new(Role::Assistant, "");
        reply.content = self.said.replay();
        let mut messages = turn.added;
        messages.push(reply);
        let kept = Kept {
            owner: self.owner.clone(),
            parent: turn.parent,
            turn: messages,
            at: Instant::now(),
        };
        remember(&self.kept, self.keep, start.id.clone(), kept);
        Ok(())
    }
}
