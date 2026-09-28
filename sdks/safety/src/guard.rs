//! An input guardrail and a streaming output guardrail, over any kind that
//! says how to read it.
//!
//! [`Guard`] checks the input on both edges: on the client edge, so a
//! plugin answering from a cache after it is covered, and on the provider
//! edge, so every call is covered, a judge's own input included. It adds
//! its output filter on the client edge, so every response the client gets
//! is covered, whoever answers, provided the guard runs before any plugin
//! that answers (FINDINGS F2). [`Guard::calls_output`] also filters each
//! call's response, so a judge's verdict read with `events` is covered.
//!
//! Traffic of a kind the guard was not told how to read is refused, never
//! passed silently, unless [`Guard::allow_uncovered`] is set.

use std::marker::PhantomData;
use std::sync::Arc;

use respan_gateway_core::http::StatusCode;
use respan_gateway_core::{
    ClientContext, Fault, Filter, Plugin, ProviderContext, Response, async_trait,
};
use respan_gateway_formats::conversation::{Content, Conversation, Reply};

/// The code of a guardrail's refusal or failure.
pub const BLOCKED: &str = "content_blocked";

/// The code of a refusal for traffic the guard cannot read.
pub const UNCOVERED: &str = "guard_uncovered";

/// Blocked terms, matched case-insensitively anywhere in text.
#[derive(Debug, Clone, Default)]
pub struct Policy {
    terms: Vec<String>,
}

impl Policy {
    /// A policy blocking `terms`.
    pub fn new(terms: impl IntoIterator<Item = impl Into<String>>) -> Self {
        let terms = terms.into_iter().map(|t| t.into().to_lowercase());
        Self {
            terms: terms.filter(|t| !t.is_empty()).collect(),
        }
    }

    /// `Err` with [`BLOCKED`] if `text` holds a blocked term.
    pub fn check(&self, text: &str) -> Result<(), Fault> {
        let text = text.to_lowercase();
        match self.terms.iter().find(|t| text.contains(t.as_str())) {
            Some(_) => Err(Fault::new(
                StatusCode::BAD_REQUEST,
                BLOCKED,
                "the content was blocked by a guardrail",
            )),
            None => Ok(()),
        }
    }

    /// Whether some blocked term may continue from the end of `text`.
    fn may_continue(&self, text: &str) -> bool {
        let text = text.to_lowercase();
        self.terms.iter().any(|t| {
            (1..t.len())
                .filter(|&n| t.is_char_boundary(n))
                .any(|n| text.ends_with(&t[..n]))
        })
    }

    fn longest(&self) -> usize {
        self.terms.iter().map(String::len).max().unwrap_or(0)
    }
}

/// An input a guard can read: all the text it carries, including what its
/// kind has no text form for.
pub trait Readable: Clone + PartialEq + Send + Sync + 'static {
    /// Everything in the input a policy should see.
    fn text(&self) -> String;
}

/// An output event a guard can read as it streams.
pub trait Streamed: Clone + PartialEq + Send + Sync + 'static {
    /// The text the event carries, if any.
    fn text(&self) -> Option<String>;
    /// Whether text may continue across this event, such as another delta
    /// of the same block; anything else ends a term.
    fn continues(&self) -> bool;
    /// Whether the event only names the response, such as a start, and
    /// passes at once: relayed, a provider message cannot take several.
    fn names(&self) -> bool {
        false
    }
}

/// A conversation, with the JSON of tool calls, images and unknown fields
/// read as text, since the kind has no text form for them (FINDINGS F4).
impl Readable for Conversation {
    fn text(&self) -> String {
        let mut text = String::new();
        for v in self.unsupported.values() {
            text.push_str(&v.to_string());
            text.push('\n');
        }
        for m in &self.messages {
            for v in m.unsupported.values() {
                text.push_str(&v.to_string());
                text.push('\n');
            }
            for c in &m.content {
                match c {
                    Content::Text { text: t, .. } => text.push_str(t),
                    Content::Other { raw, .. } => text.push_str(&raw.to_string()),
                    // A new form of content is refused by never matching.
                    _ => text.push_str("\u{0}unreadable\u{0}"),
                }
                text.push('\n');
            }
        }
        text
    }
}

impl Streamed for Reply {
    fn text(&self) -> Option<String> {
        match self {
            Reply::Text { text, .. } => Some(text.clone()),
            // A tool call's argument delta as its JSON: a term split across
            // two deltas is missed (FINDINGS F4).
            Reply::Other { raw, .. } => Some(raw.to_string()),
            _ => None,
        }
    }

    fn continues(&self) -> bool {
        matches!(self, Reply::Text { .. } | Reply::Start { .. })
    }

    fn names(&self) -> bool {
        matches!(self, Reply::Start { .. })
    }
}

/// How the guard reads one kind, its types erased.
trait Kind: Send + Sync {
    /// `None` if the request is not of this kind.
    fn client(&self, cx: &mut ClientContext, guard: &Guard) -> Option<Result<(), Fault>>;
    fn provider(&self, px: &mut ProviderContext, guard: &Guard) -> Option<Result<(), Fault>>;
}

struct KindOf<I, O>(PhantomData<fn() -> (I, O)>);

impl<I: Readable, O: Streamed> Kind for KindOf<I, O> {
    fn client(&self, cx: &mut ClientContext, guard: &Guard) -> Option<Result<(), Fault>> {
        if !cx.is_kind::<I>() {
            return None;
        }
        let output = OutputGuard::<O>::new(guard.policy.clone(), guard.hold_first);
        Some(
            cx.filter(guard.inputs::<I>())
                .and_then(|()| cx.filter(output)),
        )
    }

    fn provider(&self, px: &mut ProviderContext, guard: &Guard) -> Option<Result<(), Fault>> {
        if !px.is_kind::<I>() {
            return None;
        }
        let output = OutputGuard::<O>::new(guard.policy.clone(), 0);
        Some(
            px.filter(guard.inputs::<I>())
                .and_then(|()| match guard.calls_output {
                    true => px.filter(output),
                    false => Ok(()),
                }),
        )
    }
}

/// A guardrail on input and output, for the kinds it is told to read.
#[derive(Clone)]
pub struct Guard {
    policy: Arc<Policy>,
    kinds: Vec<Arc<dyn Kind>>,
    hold_first: usize,
    allow_uncovered: bool,
    calls_output: bool,
}

impl std::fmt::Debug for Guard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Guard")
            .field("policy", &self.policy)
            .field("kinds", &self.kinds.len())
            .finish_non_exhaustive()
    }
}

impl Guard {
    /// A guard for `policy`, reading no kind yet.
    pub fn new(policy: Policy) -> Self {
        Self {
            policy: Arc::new(policy),
            kinds: Vec::new(),
            hold_first: 0,
            allow_uncovered: false,
            calls_output: false,
        }
    }

    /// Read requests whose input is `I` and whose responses are of `O`.
    pub fn kind<I: Readable, O: Streamed>(mut self) -> Self {
        self.kinds.push(Arc::new(KindOf::<I, O>(PhantomData)));
        self
    }

    /// The conversation kind.
    pub fn conversations(self) -> Self {
        self.kind::<Conversation, Reply>()
    }

    /// Hold the response until `chars` of text are checked, so a violation
    /// in them refuses the request before any byte is sent.
    pub fn hold_first(mut self, chars: usize) -> Self {
        self.hold_first = chars;
        self
    }

    /// Let traffic of kinds the guard cannot read pass unchecked.
    pub fn allow_uncovered(mut self) -> Self {
        self.allow_uncovered = true;
        self
    }

    /// Also filter every call's response, so plugins reading one (a judge)
    /// get only checked output.
    pub fn calls_output(mut self) -> Self {
        self.calls_output = true;
        self
    }

    /// Checks every input, the one that came with the head at once.
    fn inputs<I: Readable>(&self) -> impl Filter<I> + use<I> {
        let policy = self.policy.clone();
        move |input: &mut I| policy.check(&input.text())
    }

    fn uncovered(&self) -> Result<(), Fault> {
        match self.allow_uncovered {
            true => Ok(()),
            false => Err(Fault::new(
                StatusCode::FORBIDDEN,
                UNCOVERED,
                "no guardrail covers this kind of request",
            )),
        }
    }
}

#[async_trait]
impl Plugin for Guard {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        for kind in &self.kinds {
            if let Some(checked) = kind.client(cx, self) {
                return checked.map(|()| None);
            }
        }
        self.uncovered().map(|()| None)
    }

    async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
        for kind in &self.kinds {
            if let Some(checked) = kind.provider(px, self) {
                return checked;
            }
        }
        self.uncovered()
    }
}

/// A streaming output guardrail. It holds events while their text may be
/// the start of a blocked term, and at first until `hold_first` chars of
/// text are checked; a violation fails the response: a refusal if nothing
/// was sent, else an error in the client's framing.
#[derive(Debug)]
pub struct OutputGuard<O> {
    policy: Arc<Policy>,
    /// Events held, in order, and their text since the last release.
    held: Vec<O>,
    pending: String,
    /// The end of the text already released, for terms across a release.
    window: String,
    /// Text still to check before the first release.
    hold_first: usize,
}

impl<O: Streamed> OutputGuard<O> {
    /// A guard for `policy` holding its first `hold_first` chars.
    pub fn new(policy: Arc<Policy>, hold_first: usize) -> Self {
        Self {
            policy,
            held: Vec::new(),
            pending: String::new(),
            window: String::new(),
            hold_first,
        }
    }

    fn release(&mut self, out: &mut Vec<O>) {
        out.append(&mut self.held);
        let seen = std::mem::take(&mut self.window) + &std::mem::take(&mut self.pending);
        let keep = self.policy.longest().saturating_sub(1);
        let start = seen
            .char_indices()
            .map(|(i, _)| i)
            .find(|&i| seen.len() - i <= keep)
            .unwrap_or(seen.len());
        self.window = seen[start..].to_owned();
    }
}

impl<O: Streamed> Filter<O> for OutputGuard<O> {
    fn edit(&mut self, event: O, out: &mut Vec<O>) -> Result<(), Fault> {
        if event.names() {
            out.push(event);
            return Ok(());
        }
        let text = event.text();
        let continues = event.continues();
        if !continues {
            // Nothing continues a term across it: what is held is whole.
            self.window.clear();
        }
        if let Some(text) = &text {
            self.pending.push_str(text);
            self.hold_first = self.hold_first.saturating_sub(text.chars().count());
        }
        self.policy
            .check(&format!("{}{}", self.window, self.pending))?;
        self.held.push(event);
        let open = continues && self.policy.may_continue(&self.pending);
        if self.hold_first == 0 && !open {
            self.release(out);
        }
        Ok(())
    }

    fn end(&mut self, _end: &Result<(), Fault>, out: &mut Vec<O>) -> Result<(), Fault> {
        // Everything held was checked as it came, so it is whole and goes
        // out. After this guard's own failure the kernel drops what it
        // releases here (Z8-1), the start of the blocked term included.
        self.release(out);
        Ok(())
    }
}
