//! The agent loop: the gateway runs tools inside a chat request.
//!
//! [`Agent`] is a plugin over the conversation kind, so it serves every chat
//! format and provider, translated or not. It offers the model the tools of a
//! [`Tools`] runner beside the client's own, calls the provider, and while the
//! reply calls only the runner's tools it runs them (in parallel), appends the
//! calls and their results to the conversation and calls again. The client
//! sees one reply.
//!
//! It stops at a reply that calls no runner tool, which goes to the client;
//! at a reply that calls a *client* tool (the client runs its own tools; the
//! runner's calls in that reply are left out of what it sees); at
//! [`max_steps`](Agent::max_steps), where the last call forbids tools and a
//! reply that still calls one is refused; and at the wall-clock
//! [`timeout`](Agent::timeout).
//!
//! The reply is one stream: a step's text and client tool calls flow to the
//! client as they arrive, while the runner's tool calls are held back and run;
//! the next step's blocks follow, renumbered, in the same reply. Usage is
//! summed over the steps and sent once, before the finish.
//!
//! What the model said is replayed to it as the conversation kind holds it:
//! the text, the reasoning and the calls of a step, in order, each with the
//! fields its format kept beside it, so a signed `thinking` block of
//! Anthropic or a `thoughtSignature` of Gemini goes back in the assistant
//! message the next step starts from, and the provider accepts the step. A
//! block's fields are those of the native events that name it by `index` or
//! `block` with a `fields` object, and a `signature` delta (Anthropic's
//! `signature_delta`); a `redacted_thinking` block, which only natives carry,
//! is replayed whole. Other blocks only natives carry (a provider-run tool's)
//! are not replayed.
//!
//! Native events of a step reach the client as its blocks do. One that names
//! a block (by `index` or `block`) is renumbered with it, and dropped with it
//! if the runner holds the block; one that names a block no event opens (a
//! provider-run tool's) gets a number of its own. Events about the step as a
//! whole (`ping`, a stop sequence, response fields) are those of the first
//! step; the client has no use for those of the steps after.
//!
//! A tool's failure is the model's to read, not the gateway's: a result with
//! `is_error` set. Only the provider's failure, a limit, or a malformed reply
//! fails the request.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use respan_gateway_conversation::{
    Assembled, AssembledItem, Block, Content, Finish, Input, Message, Output, Role, Tool,
    ToolChoice, Usage,
};
use respan_gateway_core::futures::StreamExt;
use respan_gateway_core::futures::stream::{self, BoxStream};
use respan_gateway_core::{
    Call, ClientContext, ErrorCode, Fault, NativeFields, NativeValue, Plugin, Response, Upstream,
    async_trait,
};
use respan_gateway_http::Status;
use serde_json::{Map, Value};
use tokio::time::{Instant, timeout, timeout_at};

/// What a tool said.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Outcome {
    /// Its answer, as the model reads it.
    pub text: String,
    /// Whether the call failed; the model sees it and may try again.
    pub is_error: bool,
}

impl Outcome {
    /// A tool's answer.
    pub fn ok(text: impl Into<String>) -> Self {
        Self {
            text: text.into(),
            is_error: false,
        }
    }

    /// A failure, as the model reads it.
    pub fn error(text: impl Into<String>) -> Self {
        Self {
            text: text.into(),
            is_error: true,
        }
    }
}

/// The tools the gateway runs, such as an MCP client's. It is asked once per
/// request for that request's [`Toolbox`].
pub trait Tools: Send + Sync + 'static {
    /// The tools for the request `cx`; it may keep what it needs of `cx`,
    /// such as calls to make.
    fn open(&self, cx: &mut ClientContext) -> Arc<dyn Toolbox>;
}

/// The tools one request may use.
#[async_trait]
pub trait Toolbox: Send + Sync + 'static {
    /// The tools to offer, with names unique among themselves and unlikely to
    /// clash with a client's (such as `github__search`). A tool the client
    /// also names is not offered. Tools it cannot list are left out.
    async fn list(&self) -> Vec<Tool>;

    /// Runs the tool `name` of [`list`](Self::list) with `arguments`. A
    /// failure is an [`Outcome::error`]. Bounded by the agent's
    /// [`tool_timeout`](Agent::tool_timeout).
    async fn call(&self, name: &str, arguments: Value) -> Outcome;
}

/// Put on every call the agent makes to the model, as the extension
/// `px.extensions().get::<Step>()`: the step, from 1.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Step(pub u32);

/// A mark for the calls a [`Toolbox`] makes through the gateway to run a
/// tool (put it on them with [`Call::with_extension`]), so a plugin can tell
/// them from the request's own provider calls.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ToolHop;

/// One tool call, as it went.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolReport {
    /// The tool, as the model named it.
    pub name: String,
    /// How long it took.
    pub took: Duration,
    /// Whether it failed (a timeout included).
    pub is_error: bool,
    /// The bytes of its answer the model saw.
    pub bytes: usize,
}

/// One step of the loop: a call to the model and the tools it asked for.
#[derive(Debug, Clone, PartialEq)]
pub struct StepReport {
    /// The step, from 1.
    pub index: u32,
    /// How long the model took, to the end of its reply.
    pub took: Duration,
    /// What the model reported for this step alone.
    pub usage: Usage,
    /// Why the model stopped, if it said.
    pub finish: Option<Finish>,
    /// The tools it asked the gateway to run, in the order it asked.
    pub tools: Vec<ToolReport>,
    /// How long running them took, together.
    pub tools_took: Duration,
}

/// Where a request's steps are reported.
pub type Sink = Arc<dyn Fn(StepReport) + Send + Sync>;

type Scope = dyn Fn(&ClientContext, &Input) -> bool + Send + Sync;
type Flags = dyn Fn(&str) -> bool + Send + Sync;
type Observe = dyn Fn(&ClientContext) -> Option<Sink> + Send + Sync;

#[derive(Debug, Clone, Copy)]
struct Limits {
    max_steps: u32,
    timeout: Duration,
    tool_timeout: Duration,
    max_result: usize,
    parallel: usize,
    max_results: usize,
}

/// The plugin: see the [module](self). Register it after the plugins that
/// route and edit requests, since it answers the request itself.
pub struct Agent {
    tools: Arc<dyn Tools>,
    limits: Limits,
    scope: Option<Arc<Scope>>,
    observe: Option<Arc<Observe>>,
    flags: Option<Arc<Flags>>,
}

impl std::fmt::Debug for Agent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Agent")
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}

impl Agent {
    /// An agent running `tools` for every chat request that has a target:
    /// 8 steps, 5 minutes, 60 seconds a tool, 64 KiB an answer, 4 calls at
    /// once, 256 KiB of answers in all.
    pub fn new(tools: impl Tools) -> Self {
        Self {
            tools: Arc::new(tools),
            limits: Limits {
                max_steps: 8,
                timeout: Duration::from_secs(300),
                tool_timeout: Duration::from_secs(60),
                max_result: 64 << 10,
                parallel: 4,
                max_results: 256 << 10,
            },
            scope: None,
            observe: None,
            flags: None,
        }
    }

    /// At most `steps` calls to the model per request (at least 1). The last
    /// of several forbids tools; a reply that still calls the runner's fails
    /// the request.
    #[must_use]
    pub fn max_steps(mut self, steps: u32) -> Self {
        self.limits.max_steps = steps.max(1);
        self
    }

    /// The whole loop's wall-clock limit; past it the request fails with a 504
    /// (`agent_timeout`).
    #[must_use]
    pub fn timeout(mut self, limit: Duration) -> Self {
        self.limits.timeout = limit;
        self
    }

    /// The limit of one tool call; past it the model gets an error result.
    #[must_use]
    pub fn tool_timeout(mut self, limit: Duration) -> Self {
        self.limits.tool_timeout = limit;
        self
    }

    /// The most bytes of one tool's answer the model sees; the rest is cut
    /// off, saying so.
    #[must_use]
    pub fn max_result(mut self, bytes: usize) -> Self {
        self.limits.max_result = bytes;
        self
    }

    /// At most `calls` tool calls of one step run at once (at least 1); the
    /// rest wait their turn. The answers keep the order asked.
    #[must_use]
    pub fn parallel(mut self, calls: usize) -> Self {
        self.limits.parallel = calls.max(1);
        self
    }

    /// The most bytes of tool answers the model sees in one request, over
    /// all its steps; once spent, a call is answered that its result was
    /// left out. Tool output is untrusted model input, and this bounds how
    /// much of it one request can pile into the conversation.
    #[must_use]
    pub fn max_results(mut self, bytes: usize) -> Self {
        self.limits.max_results = bytes;
        self
    }

    /// Runs the loop only for requests `scope` accepts, which see the
    /// request and its conversation as routing left it. Others pass.
    #[must_use]
    pub fn when(
        mut self,
        scope: impl Fn(&ClientContext, &Input) -> bool + Send + Sync + 'static,
    ) -> Self {
        self.scope = Some(Arc::new(scope));
        self
    }

    /// Says which providers can be sent a tool result flagged as a failure.
    /// Most formats have no such flag, and refuse a flagged result rather than
    /// lose it; so by default a failed result is sent as text beginning
    /// `Error: `, and flagged as well only for a provider `flags` accepts.
    #[must_use]
    pub fn flag_errors_for(mut self, flags: impl Fn(&str) -> bool + Send + Sync + 'static) -> Self {
        self.flags = Some(Arc::new(flags));
        self
    }

    /// Reports each step of a request to the sink `observe` gives for it.
    #[must_use]
    pub fn observe(
        mut self,
        observe: impl Fn(&ClientContext) -> Option<Sink> + Send + Sync + 'static,
    ) -> Self {
        self.observe = Some(Arc::new(observe));
        self
    }
}

#[async_trait]
impl Plugin for Agent {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        if !cx.is_kind::<Input>() {
            return Ok(None);
        }
        let Ok(input) = cx.input::<Input>().cloned() else {
            return Ok(None);
        };
        let Some(target) = cx.target().map(str::to_owned) else {
            return Ok(None);
        };
        let no_tools = input.options.tool_choice == Some(ToolChoice::None);
        let in_scope = self.scope.as_ref().is_none_or(|scope| scope(cx, &input));
        if no_tools || !in_scope {
            return Ok(None);
        }
        let toolbox = self.tools.open(cx);
        let listed = timeout(self.limits.tool_timeout, toolbox.list()).await;
        let (mut input, mut gateway) = (input, HashSet::new());
        let client: HashSet<String> = input.tools.iter().map(|t| t.name.clone()).collect();
        for tool in listed.unwrap_or_default() {
            if !client.contains(&tool.name) && gateway.insert(tool.name.clone()) {
                input.tools.push(tool);
            }
        }
        if gateway.is_empty() {
            return Ok(None);
        }
        let flagged = self.flags.as_ref().is_some_and(|flags| flags(&target));
        let run = Run {
            flagged,
            call: cx.call(target),
            toolbox,
            gateway,
            input,
            limits: self.limits,
            deadline: Instant::now() + self.limits.timeout,
            report: self.observe.as_ref().and_then(|observe| observe(cx)),
            steps: 0,
            next_block: 0,
            usage: Usage::default(),
            budget: self.limits.max_results,
            phase: Phase::Open,
        };
        let events = stream::unfold(run, advance).flat_map(stream::iter);
        Ok(Some(Response::stream(events)))
    }
}

struct Run {
    /// Whether the provider takes a tool result flagged as a failure.
    flagged: bool,
    call: Call,
    toolbox: Arc<dyn Toolbox>,
    /// The names of the tools the runner added.
    gateway: HashSet<String>,
    /// The conversation so far, with the runner's tools.
    input: Input,
    limits: Limits,
    deadline: Instant,
    report: Option<Sink>,
    /// The calls to the model made.
    steps: u32,
    /// The number the next step's first block gets.
    next_block: u32,
    /// The usage of the steps that ended.
    usage: Usage,
    /// The bytes of tool answers the request may still add.
    budget: usize,
    phase: Phase,
}

enum Phase {
    /// The next step is yet to be called.
    Open,
    Reading(Box<Reading>),
    Done,
}

/// A step's reply, as it is read.
struct Reading {
    events: BoxStream<'static, Result<Output, Fault>>,
    /// What the model said, whole, for the conversation.
    said: Assembled,
    /// The blocks of the runner's tool calls, which the client never sees.
    held: HashSet<u32>,
    /// Whether the model called a tool the client must run.
    client_calls: bool,
    /// The number each block the client sees has in its reply, where blocks
    /// are numbered from 0 without gaps, over all the steps.
    numbers: HashMap<u32, u32>,
    /// The number the next block the client sees gets.
    next: u32,
    /// The native events that name a block before it opens, until it does.
    pending: HashMap<u32, Vec<NativeValue>>,
    finish: Option<Finish>,
    usage: Usage,
    started: Instant,
}

type Chunk = Vec<Result<Output, Fault>>;

/// The next events for the client, until the loop is done.
async fn advance(mut run: Run) -> Option<(Chunk, Run)> {
    loop {
        match std::mem::replace(&mut run.phase, Phase::Done) {
            Phase::Done => return None,
            Phase::Open => match run.open().await {
                Ok(reading) => run.phase = Phase::Reading(Box::new(reading)),
                Err(fault) => return Some((vec![Err(fault)], run)),
            },
            Phase::Reading(mut reading) => {
                let next = timeout_at(run.deadline, reading.events.next()).await;
                let event = match next {
                    Err(_) => return Some((vec![Err(run.timed_out())], run)),
                    Ok(Some(Err(fault))) => return Some((vec![Err(fault)], run)),
                    Ok(Some(Ok(event))) => event,
                    Ok(None) => match run.end_step(*reading).await {
                        Err(fault) => return Some((vec![Err(fault)], run)),
                        Ok(Some(last)) => return Some((last, run)),
                        Ok(None) => continue,
                    },
                };
                let first = run.steps == 1;
                match reading.take(event, first, &run.gateway) {
                    Err(fault) => return Some((vec![Err(fault)], run)),
                    Ok(out) => {
                        run.phase = Phase::Reading(reading);
                        if !out.is_empty() {
                            return Some((out.into_iter().map(Ok).collect(), run));
                        }
                    }
                }
            }
        }
    }
}

impl Reading {
    /// The number the client's reply gives the provider's `block`.
    fn number(&mut self, block: u32) -> u32 {
        let next = &mut self.next;
        *self.numbers.entry(block).or_insert_with(|| {
            *next += 1;
            *next - 1
        })
    }

    /// What the client is sent for `event`, of the step's reply. The runner's
    /// tool calls, usage and the finish are held: they are decided at the
    /// step's end.
    fn take(
        &mut self,
        event: Output,
        first: bool,
        gateway: &HashSet<String>,
    ) -> Result<Vec<Output>, Fault> {
        self.said.push(&event)?;
        Ok(match event {
            Output::Start(start) if first => vec![Output::Start(start)],
            Output::Start(_) => Vec::new(),
            Output::BlockStart { block, kind } => {
                let pending = self.pending.remove(&block).unwrap_or_default();
                if let Block::ToolCall { name, .. } = &kind {
                    if gateway.contains(name) {
                        self.held.insert(block);
                        return Ok(Vec::new());
                    }
                    self.client_calls = true;
                }
                let number = self.number(block);
                let mut out: Vec<Output> = pending
                    .iter()
                    .map(|n| renumbered(n, block, number))
                    .collect();
                out.push(Output::block_start(number, kind));
                out
            }
            Output::Delta { block, delta } if !self.held.contains(&block) => {
                let block = self.numbers.get(&block).copied().unwrap_or(block);
                vec![Output::Delta { block, delta }]
            }
            Output::BlockEnd { block } if !self.held.contains(&block) => {
                let block = self.numbers.get(&block).copied().unwrap_or(block);
                vec![Output::block_end(block)]
            }
            Output::Delta { .. } | Output::BlockEnd { .. } => Vec::new(),
            Output::Usage(usage) => {
                self.usage += usage;
                Vec::new()
            }
            Output::Finish(finish) => {
                self.finish = Some(finish);
                Vec::new()
            }
            Output::Native(native) => match block_of(&native.value) {
                Some(block) if self.held.contains(&block) => Vec::new(),
                Some(block) if self.numbers.contains_key(&block) => {
                    vec![renumbered(&native, block, self.number(block))]
                }
                // Fields of a block that is yet to open.
                Some(block) if native.value["fields"].is_object() => {
                    self.pending.entry(block).or_default().push(native);
                    Vec::new()
                }
                // A block only natives carry.
                Some(block) => vec![renumbered(&native, block, self.number(block))],
                None if first => vec![Output::Native(native)],
                None => Vec::new(),
            },
            // An event a later kind adds is the client's to see.
            other => vec![other],
        })
    }
}

impl Run {
    fn timed_out(&self) -> Fault {
        let message = format!(
            "the agent loop ran past its {} s limit",
            self.limits.timeout.as_secs()
        );
        Fault::new(message)
            .with(ErrorCode("agent_timeout".into()))
            .with(Status(
                respan_gateway_http::http::StatusCode::GATEWAY_TIMEOUT,
            ))
    }

    /// Calls the model for the next step.
    async fn open(&mut self) -> Result<Reading, Fault> {
        if Instant::now() >= self.deadline {
            return Err(self.timed_out());
        }
        self.steps += 1;
        let started = Instant::now();
        let mut input = self.input.clone();
        if self.steps == self.limits.max_steps && self.steps > 1 {
            input.options.tool_choice = Some(ToolChoice::None);
        }
        let call = self.call.clone().with_input(input);
        let call = call.with_extension(Step(self.steps));
        let response = match timeout_at(self.deadline, call).await {
            Ok(response) => response?,
            Err(_) => return Err(self.timed_out()),
        };
        Ok(Reading {
            events: response.outputs::<Output>(),
            said: Assembled::default(),
            held: HashSet::new(),
            client_calls: false,
            numbers: HashMap::new(),
            next: self.next_block,
            pending: HashMap::new(),
            finish: None,
            usage: Usage::default(),
            started,
        })
    }

    /// A step's reply ended: the last events for the client if the loop is
    /// done, else `None` after the tools ran and the next step is ready.
    async fn end_step(&mut self, reading: Reading) -> Result<Option<Chunk>, Fault> {
        self.usage += reading.usage;
        let calls: Vec<(String, String, String)> = reading
            .said
            .blocks()
            .filter_map(|b| match &b.kind {
                Block::ToolCall { id, name } if self.gateway.contains(name) => {
                    Some((id.clone(), name.clone(), b.text.clone()))
                }
                _ => None,
            })
            .collect();
        let complete = matches!(
            reading.finish,
            None | Some(Finish::ToolUse) | Some(Finish::Stop)
        );
        let goes_on = !calls.is_empty() && !reading.client_calls && complete;
        let took = reading.started.elapsed();
        if !goes_on {
            self.say(StepReport {
                index: self.steps,
                took,
                usage: reading.usage,
                finish: reading.finish.clone(),
                tools: Vec::new(),
                tools_took: Duration::ZERO,
            });
            let mut last = Vec::new();
            if !self.usage.is_empty() {
                last.push(Ok(Output::Usage(self.usage)));
            }
            last.extend(reading.finish.map(|finish| Ok(Output::Finish(finish))));
            return Ok(Some(last));
        }
        if self.steps >= self.limits.max_steps {
            let message = format!(
                "the model was still calling tools after {} steps",
                self.limits.max_steps
            );
            return Err(limit("agent_max_steps", message));
        }
        let ran = Instant::now();
        let mut results = run_tools(&self.toolbox, self.limits, self.deadline, calls).await;
        spend(&mut self.budget, &mut results);
        let mut said = replay(&reading.said);
        let mut assistant = Message::new(Role::Assistant, said.remove(0));
        assistant.content.append(&mut said);
        let flagged = self.flagged;
        let mut answers = results.iter().map(|(id, outcome, _)| {
            let failed = outcome.is_error;
            let text = match (failed, flagged) {
                (true, false) => format!("Error: {}", outcome.text),
                _ => outcome.text.clone(),
            };
            Content::tool_result(id, vec![Content::from(text)], failed && flagged)
        });
        let mut user = Message::new(
            Role::User,
            answers.next().unwrap_or_else(|| Content::from("")),
        );
        user.content.extend(answers);
        self.input.messages.push(assistant);
        self.input.messages.push(user);
        // A forced call was for the first word; after it the model chooses.
        if matches!(
            self.input.options.tool_choice,
            Some(ToolChoice::Required | ToolChoice::Tool(_))
        ) {
            self.input.options.tool_choice = Some(ToolChoice::Auto);
        }
        self.next_block = reading.next;
        self.say(StepReport {
            index: self.steps,
            took,
            usage: reading.usage,
            finish: reading.finish,
            tools: results.into_iter().map(|(_, _, report)| report).collect(),
            tools_took: ran.elapsed(),
        });
        self.phase = Phase::Open;
        Ok(None)
    }

    fn say(&self, report: StepReport) {
        if let Some(sink) = &self.report {
            sink(report);
        }
    }
}

/// The block an event names, by `index` (Anthropic's) or `block` (the
/// others' natives).
fn block_of(raw: &Value) -> Option<u32> {
    let named = raw["index"].as_u64().or_else(|| raw["block"].as_u64());
    u32::try_from(named?).ok()
}

/// `native`, which names `block`, naming `number`.
fn renumbered(native: &NativeValue, block: u32, number: u32) -> Output {
    let mut value = (*native.value).clone();
    if let Some(object) = value.as_object_mut() {
        let key = if object.contains_key("index") && object["index"] == block {
            "index"
        } else {
            "block"
        };
        object.insert(key.into(), number.into());
    }
    Output::Native(NativeValue::new(native.format.clone(), value))
}

/// What the model said, as the assistant message holds it: its text,
/// reasoning and tool calls in order, each with the fields its format kept
/// beside it.
fn replay(said: &Assembled) -> Vec<Content> {
    let mut kept: HashMap<u32, (NativeValue, Map<String, Value>)> = HashMap::new();
    for item in said.items() {
        let AssembledItem::Native(native) = item else {
            continue;
        };
        let Some(block) = block_of(&native.value) else {
            continue;
        };
        let raw = &native.value;
        let entry = kept
            .entry(block)
            .or_insert_with(|| (native.clone(), Map::new()));
        if let Some(fields) = raw["fields"].as_object() {
            let fields = fields.iter().map(|(k, v)| (k.clone(), v.clone()));
            entry.1.extend(fields);
        }
        if let Some(signature) = raw["delta"]["signature"].as_str() {
            entry.1.insert("signature".into(), signature.into());
        }
    }
    let native_of = |block: u32| {
        let (from, fields) = kept.get(&block)?;
        Some(NativeFields::new(from.format.clone(), fields.clone())).filter(|n| !n.is_empty())
    };
    let mut parts = Vec::new();
    for item in said.items() {
        match item {
            AssembledItem::Block(b) => {
                let native = native_of(b.block);
                match &b.kind {
                    Block::Text if !b.text.is_empty() => {
                        let mut part = Content::from(b.text.clone());
                        if let Content::Text { native: n, .. } = &mut part {
                            *n = native;
                        }
                        parts.push(part);
                    }
                    Block::Reasoning if !b.text.is_empty() || native.is_some() => {
                        parts.push(Content::Reasoning {
                            text: b.text.clone(),
                            native,
                        });
                    }
                    Block::ToolCall { id, name } => {
                        let arguments = Some(b.text.as_str()).filter(|t| !t.trim().is_empty());
                        let mut part = Content::tool_call(id, name, arguments.unwrap_or("{}"));
                        if let Content::ToolCall { native: n, .. } = &mut part {
                            *n = native;
                        }
                        parts.push(part);
                    }
                    _ => {}
                }
            }
            // Thinking the provider redacted is only a native block; it must
            // be sent back as it came.
            AssembledItem::Native(native) => {
                let block = &native.value["content_block"];
                if native.value["type"] == "content_block_start"
                    && block["type"] == "redacted_thinking"
                {
                    parts.push(Content::native(native.format.clone(), block.clone()));
                }
            }
        }
    }
    parts
}

/// Runs `calls` at once; their answers in the order asked.
async fn run_tools(
    toolbox: &Arc<dyn Toolbox>,
    limits: Limits,
    deadline: Instant,
    calls: Vec<(String, String, String)>,
) -> Vec<(String, Outcome, ToolReport)> {
    let each = calls.into_iter().map(|(id, name, arguments)| {
        let toolbox = toolbox.clone();
        async move {
            let started = Instant::now();
            let left = deadline.saturating_duration_since(started);
            let allowed = limits.tool_timeout.min(left);
            let parsed = match arguments.trim() {
                "" => Ok(Value::Object(Default::default())),
                text => serde_json::from_str::<Value>(text),
            };
            let outcome = match parsed {
                Err(e) => Outcome::error(format!("the arguments are not valid JSON: {e}")),
                Ok(arguments) => match timeout(allowed, toolbox.call(&name, arguments)).await {
                    Ok(outcome) => outcome,
                    Err(_) => Outcome::error(format!(
                        "the tool did not answer within {} s",
                        allowed.as_secs().max(1)
                    )),
                },
            };
            let outcome = Outcome {
                text: cut(outcome.text, limits.max_result),
                ..outcome
            };
            let report = ToolReport {
                name,
                took: started.elapsed(),
                is_error: outcome.is_error,
                bytes: outcome.text.len(),
            };
            (id, outcome, report)
        }
    });
    stream::iter(each)
        .buffered(limits.parallel.max(1))
        .collect()
        .await
}

/// What a request has left for tool answers: each of `results`, in order,
/// is cut to what is left of `budget`, which it then takes from.
fn spend(budget: &mut usize, results: &mut [(String, Outcome, ToolReport)]) {
    for (_, outcome, report) in results {
        let text = std::mem::take(&mut outcome.text);
        outcome.text = match *budget {
            0 => "[left out: the tool answers of this request reached their limit]".to_owned(),
            left => cut(text, left),
        };
        report.bytes = outcome.text.len();
        *budget = budget.saturating_sub(outcome.text.len());
    }
}

/// `text` of at most `max` bytes, saying what was cut off.
fn cut(text: String, max: usize) -> String {
    if text.len() <= max {
        return text;
    }
    let mut end = max;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    let omitted = text.len() - end;
    format!("{}\n[cut: {omitted} more bytes]", &text[..end])
}

fn limit(code: &'static str, message: String) -> Fault {
    Upstream::fault(message).with(ErrorCode(code.into()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_answer_is_cut_at_a_character() {
        assert_eq!(cut("abc".into(), 3), "abc");
        assert_eq!(cut("aéb".into(), 2), "a\n[cut: 3 more bytes]");
        assert_eq!(cut("abcdef".into(), 4), "abcd\n[cut: 2 more bytes]");
    }

    #[test]
    fn answers_share_a_budget() {
        let result = |text: &str| {
            let report = ToolReport {
                name: "t".into(),
                took: Duration::ZERO,
                is_error: false,
                bytes: text.len(),
            };
            ("id".to_owned(), Outcome::ok(text), report)
        };
        let mut results = vec![result("aaaa"), result("bbbb"), result("cccc")];
        let mut budget = 6;
        spend(&mut budget, &mut results);
        assert_eq!(results[0].1.text, "aaaa");
        assert!(
            results[1].1.text.starts_with("bb\n[cut"),
            "{}",
            results[1].1.text
        );
        assert!(results[2].1.text.contains("left out"));
        assert_eq!(results[1].2.bytes, results[1].1.text.len());
        assert_eq!(budget, 0);
    }
}
