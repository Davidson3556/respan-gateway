//! The tool loop against strict provider fakes: a fake tool runner, OpenAI
//! and Anthropic providers, and clients of both formats.

#![allow(clippy::unwrap_used)]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use mockllm::{Answer, FakeAnthropic, FakeGemini, FakeOpenAi};
use respan_gateway_conversation::{Input, Output, Tool, Usage};
use respan_gateway_conversation_http::anthropic::AnthropicMessages;
use respan_gateway_conversation_http::gemini::GeminiGenerate;
use respan_gateway_conversation_http::openai::OpenAiChat;
use respan_gateway_core::{
    ClientContext, Fault, Gateway, Plugin, Provider, ProviderContext, Response, async_trait,
};
use respan_gateway_http::http::{self, HeaderMap, HeaderValue, StatusCode};
use respan_gateway_http_faults::error_code;
use respan_gateway_http_transport::HttpTransport;
use respan_gateway_plugin_tool_loop::{Outcome, Sink, Step, StepReport, ToolLoop, Toolbox, Tools};
use serde_json::{Value, json};
use tokio::time::sleep;

#[allow(dead_code)]
#[path = "../../../../tests/support/http.rs"]
mod http_support;
use http_support::{LIMIT, post, read_whole, serve};

/// A runner with scripted answers that notes every call.
#[derive(Default)]
struct Fake {
    calls: Mutex<Vec<(String, Value)>>,
    answers: Mutex<Vec<(String, Outcome)>>,
    delay: Duration,
    active: AtomicUsize,
    peak: AtomicUsize,
    tools: Vec<&'static str>,
}

impl Fake {
    fn new() -> Self {
        Self {
            tools: vec!["files__read", "gh__search"],
            ..Self::default()
        }
    }

    fn answering(self, tool: &str, outcome: Outcome) -> Self {
        self.answers
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push((tool.to_owned(), outcome));
        self
    }

    fn slow(mut self, delay: Duration) -> Self {
        self.delay = delay;
        self
    }

    fn calls(&self) -> Vec<(String, Value)> {
        self.calls
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

#[async_trait]
impl Toolbox for Fake {
    async fn list(&self) -> Vec<Tool> {
        let tool = |name: &&str| {
            let mut tool = Tool::new(
                *name,
                json!({"type": "object", "properties": {"path": {"type": "string"}}}),
            );
            tool.description = Some(format!("the {name} tool"));
            tool
        };
        self.tools.iter().map(tool).collect()
    }

    async fn call(&self, name: &str, arguments: Value) -> Outcome {
        let now = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(now, Ordering::SeqCst);
        self.calls
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push((name.to_owned(), arguments));
        sleep(self.delay).await;
        self.active.fetch_sub(1, Ordering::SeqCst);
        let answers = self.answers.lock().unwrap_or_else(PoisonError::into_inner);
        let said = answers.iter().find(|(tool, _)| tool == name);
        said.map_or_else(|| Outcome::ok(format!("{name} ok")), |(_, o)| o.clone())
    }
}

struct Offer(Arc<Fake>);

impl Tools for Offer {
    fn open(&self, _: &mut ClientContext) -> Arc<dyn Toolbox> {
        self.0.clone()
    }
}

/// Targets provider `.0` unless the client's model names the other.
struct Target;

#[async_trait]
impl Plugin for Target {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let model = cx.input::<Input>().ok().and_then(|i| i.model.clone());
        let model = model.unwrap_or_default();
        cx.set_target(match model {
            m if m.starts_with("claude") => "anthropic",
            m if m.starts_with("gemini") => "gemini",
            _ => "openai",
        });
        Ok(None)
    }
}

/// Sums the usage each provider call reports, and notes the step marks.
#[derive(Clone, Default)]
struct Meter {
    usage: Arc<Mutex<Usage>>,
    steps: Arc<Mutex<Vec<u32>>>,
}

#[async_trait]
impl Plugin for Meter {
    async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
        if let Some(Step(n)) = px.extensions().get::<Step>() {
            self.steps
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(*n);
        }
        if !px.is_kind::<Input>() {
            return Ok(());
        }
        let total = self.usage.clone();
        px.filter(move |output: &mut Output| {
            if let Output::Usage(usage) = output {
                *total.lock().unwrap_or_else(PoisonError::into_inner) += *usage;
            }
            Ok(())
        })
    }
}

fn http_to(
    base: &str,
    header: &'static str,
    key: &'static str,
) -> impl Fn(
    http::Request<respan_gateway_http::Body>,
) -> respan_gateway_core::futures::future::BoxFuture<'static, Sent>
+ Clone
+ Send
+ Sync
+ 'static {
    use respan_gateway_core::futures::FutureExt;
    let mut headers = HeaderMap::new();
    headers.insert(header, HeaderValue::from_static(key));
    let http = HttpTransport::new(base)
        .and_then(|t| t.with_headers(headers))
        .unwrap();
    move |call| {
        let http = http.clone();
        async move { http.send(call).await }.boxed()
    }
}

type Sent = Result<
    (
        http::Response<respan_gateway_http::Body>,
        Option<respan_gateway_http::Completion>,
    ),
    Fault,
>;

struct Rig {
    gw: Gateway,
    openai: FakeOpenAi,
    anthropic: FakeAnthropic,
    gemini: FakeGemini,
    fake: Arc<Fake>,
    meter: Meter,
    reports: Arc<Mutex<Vec<StepReport>>>,
}

impl Rig {
    async fn new(fake: Fake) -> Self {
        Self::with(fake, |tool_loop| tool_loop).await
    }

    async fn with(fake: Fake, configure: impl FnOnce(ToolLoop) -> ToolLoop) -> Self {
        let (openai, anthropic, gemini) = tokio::join!(
            FakeOpenAi::start(),
            FakeAnthropic::start(),
            FakeGemini::start()
        );
        let fake = Arc::new(fake);
        let meter = Meter::default();
        let reports: Arc<Mutex<Vec<StepReport>>> = Arc::default();
        let sink: Sink = {
            let reports = reports.clone();
            Arc::new(move |r| {
                reports
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .push(r)
            })
        };
        let tool_loop = ToolLoop::new(Offer(fake.clone()))
            .observe(move |_| Some(sink.clone()))
            .flag_errors_for(|provider| provider == "anthropic");
        let tool_loop = configure(tool_loop);
        let oa = http_to(&openai.base_url(), "authorization", "Bearer sk-test");
        let an = http_to(&anthropic.base_url(), "x-api-key", "sk-ant-test");
        let gm = http_to(
            &format!("{}/v1beta", gemini.base_url()),
            "x-goog-api-key",
            "gm-test",
        );
        let gw = Gateway::new()
            .provider(Provider::new(
                "openai",
                OpenAiChat,
                respan_gateway_http::connect(oa, LIMIT),
            ))
            .and_then(|g| {
                g.provider(Provider::new(
                    "anthropic",
                    AnthropicMessages,
                    respan_gateway_http::connect(an, LIMIT),
                ))
            })
            .and_then(|g| {
                g.provider(Provider::new(
                    "gemini",
                    GeminiGenerate,
                    respan_gateway_http::connect(gm, LIMIT),
                ))
            })
            .and_then(|g| g.client::<Input, Output, _>(OpenAiChat))
            .and_then(|g| g.client::<Input, Output, _>(AnthropicMessages))
            .map(|g| g.plugin(meter.clone()).plugin(Target).plugin(tool_loop))
            .unwrap();
        Self {
            gw,
            openai,
            anthropic,
            gemini,
            fake,
            meter,
            reports,
        }
    }

    async fn ask(&self, path: &str, body: Value) -> (StatusCode, String) {
        let (resp, _) = serve(&self.gw, post(path, body.to_string())).await;
        let (status, _, body) = read_whole(resp).await;
        self.gw.idle().await;
        let body = body.unwrap_or_else(|f| panic!("aborted: {f}"));
        (status, String::from_utf8_lossy(&body).into_owned())
    }

    async fn chat(&self, body: Value) -> (StatusCode, Value) {
        let (status, text) = self.ask("/v1/chat/completions", body).await;
        (
            status,
            serde_json::from_str(&text).unwrap_or(Value::String(text)),
        )
    }

    fn total(&self) -> Usage {
        *self
            .meter
            .usage
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    fn reports(&self) -> Vec<StepReport> {
        self.reports
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

fn ask_gpt() -> Value {
    json!({"model": "gpt-4o", "messages": [{"role": "user", "content": "read a.txt"}]})
}

fn call(id: &str, name: &str, arguments: Value) -> Value {
    json!({"id": id, "type": "function",
           "function": {"name": name, "arguments": arguments.to_string()}})
}

/// An OpenAI completion that calls `calls`, with usage `(input, output)`.
fn calls(calls: Vec<Value>, usage: (u64, u64)) -> Answer {
    Answer::body(json!({
        "id": "chatcmpl-1", "object": "chat.completion", "created": 1, "model": "gpt-4o",
        "choices": [{"index": 0, "finish_reason": "tool_calls",
                     "message": {"role": "assistant", "content": null, "tool_calls": calls}}],
        "usage": {"prompt_tokens": usage.0, "completion_tokens": usage.1, "total_tokens": usage.0 + usage.1}
    }))
}

fn says(text: &str, usage: (u64, u64)) -> Answer {
    Answer::body(json!({
        "id": "chatcmpl-2", "object": "chat.completion", "created": 2, "model": "gpt-4o",
        "choices": [{"index": 0, "finish_reason": "stop",
                     "message": {"role": "assistant", "content": text}}],
        "usage": {"prompt_tokens": usage.0, "completion_tokens": usage.1, "total_tokens": usage.0 + usage.1}
    }))
}

fn read_a() -> Value {
    call("call_a", "files__read", json!({"path": "a.txt"}))
}

#[tokio::test]
async fn a_two_step_loop_answers_the_client_once_with_summed_usage() {
    let rig = Rig::new(Fake::new().answering("files__read", Outcome::ok("alpha"))).await;
    rig.openai.answer(calls(vec![read_a()], (10, 5)));
    rig.openai.answer(says("It says alpha.", (20, 7)));
    let (status, body) = rig.chat(ask_gpt()).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["choices"][0]["message"]["content"], "It says alpha.");
    assert_eq!(body["choices"][0]["finish_reason"], "stop");
    assert!(
        body["choices"][0]["message"]["tool_calls"].is_null(),
        "{body}"
    );
    assert_eq!(body["usage"]["prompt_tokens"], 30, "{body}");
    assert_eq!(body["usage"]["completion_tokens"], 12, "{body}");
    assert_eq!(rig.total(), Usage::new(30, 12));
    assert_eq!(
        rig.fake.calls(),
        [("files__read".to_owned(), json!({"path": "a.txt"}))]
    );

    let sent = rig.openai.requests();
    assert_eq!(sent.len(), 2);
    let names = |r: &mockllm::Request| -> Vec<String> {
        let tools = r.body["tools"].as_array().cloned().unwrap_or_default();
        tools
            .iter()
            .map(|t| t["function"]["name"].as_str().unwrap().to_owned())
            .collect()
    };
    assert_eq!(names(&sent[0]), ["files__read", "gh__search"]);
    let messages = sent[1].body["messages"].as_array().unwrap();
    assert_eq!(messages.len(), 3, "{messages:?}");
    assert_eq!(messages[1]["tool_calls"][0]["id"], "call_a");
    assert_eq!(
        messages[1]["tool_calls"][0]["function"]["name"],
        "files__read"
    );
    assert_eq!(messages[2]["role"], "tool");
    assert_eq!(messages[2]["tool_call_id"], "call_a");
    assert_eq!(messages[2]["content"], "alpha");
    assert_eq!(*rig.meter.steps.lock().unwrap(), [1, 2]);

    let reports = rig.reports();
    assert_eq!(reports.len(), 2);
    assert_eq!((reports[0].index, reports[0].usage), (1, Usage::new(10, 5)));
    assert_eq!(reports[0].tools.len(), 1);
    assert_eq!(reports[0].tools[0].name, "files__read");
    assert!(!reports[0].tools[0].is_error);
    assert!(reports[1].tools.is_empty());
}

#[tokio::test]
async fn parallel_tool_calls_run_at_once_and_all_answer_in_order() {
    let rig = Rig::new(Fake::new().slow(Duration::from_millis(150))).await;
    let both = vec![read_a(), call("call_b", "gh__search", json!({"q": "x"}))];
    rig.openai.answer(calls(both, (1, 1)));
    rig.openai.answer(says("done", (1, 1)));
    let (status, body) = rig.chat(ask_gpt()).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(rig.fake.peak.load(Ordering::SeqCst), 2, "run in parallel");
    let sent = rig.openai.requests();
    let messages = sent[1].body["messages"].as_array().unwrap();
    assert_eq!(messages[1]["tool_calls"].as_array().unwrap().len(), 2);
    assert_eq!(messages[2]["tool_call_id"], "call_a");
    assert_eq!(messages[2]["content"], "files__read ok");
    assert_eq!(messages[3]["tool_call_id"], "call_b");
    assert_eq!(messages[3]["content"], "gh__search ok");
}

fn with_client_tool(mut body: Value) -> Value {
    body["tools"] = json!([{"type": "function", "function": {
        "name": "local", "description": "client side", "parameters": {"type": "object"}}}]);
    body
}

#[tokio::test]
async fn a_client_tool_call_goes_back_to_the_client_untouched() {
    let rig = Rig::new(Fake::new()).await;
    rig.openai.answer(calls(
        vec![call("call_l", "local", json!({"x": 1}))],
        (4, 2),
    ));
    let (status, body) = rig.chat(with_client_tool(ask_gpt())).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let message = &body["choices"][0]["message"];
    assert_eq!(message["tool_calls"][0]["function"]["name"], "local");
    assert_eq!(body["choices"][0]["finish_reason"], "tool_calls");
    assert_eq!(body["usage"]["prompt_tokens"], 4);
    assert!(rig.fake.calls().is_empty());
    assert_eq!(rig.openai.requests().len(), 1);
    // The model was offered both kinds.
    let tools = rig.openai.requests()[0].body["tools"]
        .as_array()
        .unwrap()
        .len();
    assert_eq!(tools, 3);
}

#[tokio::test]
async fn a_reply_mixing_both_kinds_shows_the_client_only_its_own() {
    let rig = Rig::new(Fake::new()).await;
    let mixed = vec![read_a(), call("call_l", "local", json!({}))];
    rig.openai.answer(calls(mixed, (4, 2)));
    let (status, body) = rig.chat(with_client_tool(ask_gpt())).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let held = body["choices"][0]["message"]["tool_calls"]
        .as_array()
        .unwrap();
    assert_eq!(held.len(), 1, "{body}");
    assert_eq!(held[0]["function"]["name"], "local");
    assert!(rig.fake.calls().is_empty());
}

#[tokio::test]
async fn a_clients_tool_of_the_same_name_is_the_clients() {
    let rig = Rig::new(Fake::new()).await;
    let mut body = ask_gpt();
    body["tools"] = json!([{"type": "function", "function": {
        "name": "files__read", "parameters": {"type": "object"}}}]);
    rig.openai.answer(calls(vec![read_a()], (1, 1)));
    let (_, reply) = rig.chat(body).await;
    assert_eq!(
        reply["choices"][0]["message"]["tool_calls"][0]["id"],
        "call_a"
    );
    assert!(rig.fake.calls().is_empty());
    let tools = rig.openai.requests()[0].body["tools"]
        .as_array()
        .unwrap()
        .len();
    assert_eq!(tools, 2, "the client's files__read, and gh__search");
}

#[tokio::test]
async fn the_last_step_forbids_tools_and_a_model_that_ignores_it_fails_the_request() {
    let rig = Rig::with(Fake::new(), |a| a.max_steps(2)).await;
    rig.openai.answer(calls(vec![read_a()], (1, 1)));
    rig.openai.answer(calls(
        vec![call("call_b", "files__read", json!({}))],
        (1, 1),
    ));
    let (status, text) = rig.ask("/v1/chat/completions", ask_gpt()).await;
    assert_eq!(status, StatusCode::BAD_GATEWAY, "{text}");
    assert!(text.contains("after 2 steps"), "{text}");
    assert!(text.contains("tool_loop_max_steps"), "{text}");
    let sent = rig.openai.requests();
    assert!(sent[0].body["tool_choice"].is_null());
    assert_eq!(sent[1].body["tool_choice"], "none");
    assert_eq!(rig.fake.calls().len(), 1);
}

#[tokio::test]
async fn a_forced_tool_choice_is_for_the_first_step_only() {
    let rig = Rig::new(Fake::new()).await;
    let mut body = ask_gpt();
    body["tool_choice"] = json!("required");
    rig.openai.answer(calls(vec![read_a()], (1, 1)));
    rig.openai.answer(says("ok", (1, 1)));
    let (status, _) = rig.chat(body).await;
    assert_eq!(status, StatusCode::OK);
    let sent = rig.openai.requests();
    assert_eq!(sent[0].body["tool_choice"], "required");
    assert_eq!(sent[1].body["tool_choice"], "auto");
}

#[tokio::test]
async fn a_tool_that_fails_tells_the_model_not_the_client() {
    let fake = Fake::new().answering("files__read", Outcome::error("no such file"));
    let rig = Rig::new(fake).await;
    rig.openai.answer(calls(vec![read_a()], (1, 1)));
    rig.openai.answer(says("It does not exist.", (1, 1)));
    let (status, body) = rig.chat(ask_gpt()).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body["choices"][0]["message"]["content"],
        "It does not exist."
    );
    let sent = rig.openai.requests();
    assert_eq!(
        sent[1].body["messages"][2]["content"],
        "Error: no such file"
    );
    assert!(rig.reports()[0].tools[0].is_error);
}

#[tokio::test]
async fn an_error_result_reaches_an_anthropic_provider_flagged() {
    let fake = Fake::new().answering("files__read", Outcome::error("no such file"));
    let rig = Rig::new(fake).await;
    rig.anthropic
        .answer(Answer::tool_call("files__read", json!({"path": "a.txt"})));
    rig.anthropic.answer(Answer::text("Nope."));
    let body =
        json!({"model": "claude-sonnet-4-5", "messages": [{"role": "user", "content": "read"}]});
    let (status, reply) = rig.chat(body).await;
    assert_eq!(status, StatusCode::OK, "{reply}");
    assert_eq!(reply["choices"][0]["message"]["content"], "Nope.");
    let sent = rig.anthropic.requests();
    assert_eq!(sent.len(), 2);
    let result = &sent[1].body["messages"][2]["content"][0];
    assert_eq!(result["type"], "tool_result", "{}", sent[1].body);
    assert_eq!(result["is_error"], true);
    assert_eq!(sent[0].body["tools"][0]["name"], "files__read");
}

#[tokio::test]
async fn an_anthropic_client_is_served_by_an_openai_provider_through_the_loop() {
    let rig = Rig::new(Fake::new().answering("files__read", Outcome::ok("alpha"))).await;
    rig.openai.answer(calls(vec![read_a()], (10, 5)));
    rig.openai.answer(says("It says alpha.", (20, 7)));
    let body = json!({"model": "gpt-4o", "max_tokens": 100,
                      "messages": [{"role": "user", "content": "read a.txt"}]});
    let (status, text) = rig.ask("/v1/messages", body).await;
    assert_eq!(status, StatusCode::OK, "{text}");
    let reply: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(reply["content"][0]["text"], "It says alpha.", "{text}");
    assert_eq!(reply["stop_reason"], "end_turn");
    assert_eq!(reply["usage"]["input_tokens"], 30, "{text}");
    assert_eq!(reply["usage"]["output_tokens"], 12, "{text}");
    assert_eq!(rig.openai.requests().len(), 2);
}

#[tokio::test]
async fn the_final_step_streams_to_a_streaming_client() {
    let rig = Rig::new(Fake::new().answering("files__read", Outcome::ok("alpha"))).await;
    rig.openai
        .answer(Answer::tool_call("files__read", json!({"path": "a.txt"})));
    rig.openai.answer(Answer::text("It really says alpha"));
    let mut body = ask_gpt();
    body["stream"] = json!(true);
    body["stream_options"] = json!({"include_usage": true});
    let (status, text) = rig.ask("/v1/chat/completions", body).await;
    assert_eq!(status, StatusCode::OK, "{text}");
    let chunks: Vec<Value> = text
        .split("\n\n")
        .filter_map(|f| f.strip_prefix("data: "))
        .filter_map(|d| serde_json::from_str(d).ok())
        .collect();
    let said: String = chunks
        .iter()
        .filter_map(|c| c["choices"][0]["delta"]["content"].as_str())
        .collect();
    assert_eq!(said, "It really says alpha", "{text}");
    assert!(
        chunks
            .iter()
            .filter(|c| c["choices"][0]["delta"]["content"].is_string())
            .count()
            > 1,
        "the text arrives in pieces: {text}"
    );
    assert!(!text.contains("tool_calls"), "{text}");
    assert!(text.contains("\"finish_reason\":\"stop\""), "{text}");
    assert!(text.trim_end().ends_with("[DONE]"), "{text}");
    assert_eq!(rig.openai.requests().len(), 2);
}

#[tokio::test]
async fn a_tool_that_hangs_is_cut_off_and_an_oversized_answer_is_trimmed() {
    let fake = Fake::new()
        .slow(Duration::from_secs(5))
        .answering("files__read", Outcome::ok("x".repeat(100)));
    let rig = Rig::with(fake, |a| a.tool_timeout(Duration::from_millis(100))).await;
    rig.openai.answer(calls(vec![read_a()], (1, 1)));
    rig.openai.answer(says("sorry", (1, 1)));
    let (status, _) = rig.chat(ask_gpt()).await;
    assert_eq!(status, StatusCode::OK);
    let sent = rig.openai.requests();
    let content = sent[1].body["messages"][2]["content"]
        .as_str()
        .unwrap()
        .to_owned();
    assert!(content.contains("did not answer"), "{content}");

    let rig = Rig::with(
        Fake::new().answering("files__read", Outcome::ok("x".repeat(100))),
        |a| a.max_result(10),
    )
    .await;
    rig.openai.answer(calls(vec![read_a()], (1, 1)));
    rig.openai.answer(says("ok", (1, 1)));
    let _ = rig.chat(ask_gpt()).await;
    let content = rig.openai.requests()[1].body["messages"][2]["content"]
        .as_str()
        .unwrap()
        .to_owned();
    assert!(
        content.starts_with("xxxxxxxxxx\n[cut: 90 more bytes]"),
        "{content}"
    );
}

#[tokio::test]
async fn the_whole_loop_has_a_wall_clock_limit() {
    let fake = Fake::new().slow(Duration::from_millis(300));
    let rig = Rig::with(fake, |a| a.timeout(Duration::from_millis(100))).await;
    rig.openai.answer(calls(vec![read_a()], (1, 1)));
    rig.openai.answer(says("late", (1, 1)));
    let (status, text) = rig.ask("/v1/chat/completions", ask_gpt()).await;
    assert_eq!(status, StatusCode::GATEWAY_TIMEOUT, "{text}");
    assert!(text.contains("tool_loop_timeout"), "{text}");
}

#[tokio::test]
async fn a_request_out_of_scope_or_with_no_tools_to_offer_is_untouched() {
    let rig = Rig::with(Fake::new(), |a| {
        a.when(|_, input| input.model() == Some("o1"))
    })
    .await;
    rig.openai.answer(says("plain", (1, 1)));
    let (status, body) = rig.chat(ask_gpt()).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["choices"][0]["message"]["content"], "plain");
    assert!(rig.openai.requests()[0].body["tools"].is_null());

    let mut empty = Fake::new();
    empty.tools = Vec::new();
    let rig = Rig::new(empty).await;
    rig.openai.answer(says("plain", (1, 1)));
    let (_, body) = rig.chat(ask_gpt()).await;
    assert_eq!(body["choices"][0]["message"]["content"], "plain");
    assert!(rig.openai.requests()[0].body["tools"].is_null());

    let rig = Rig::new(Fake::new()).await;
    let mut none = ask_gpt();
    none["tool_choice"] = json!("none");
    none["tools"] = json!([{"type": "function", "function": {"name": "x", "parameters": {}}}]);
    rig.openai.answer(says("plain", (1, 1)));
    let _ = rig.chat(none).await;
    assert_eq!(
        rig.openai.requests()[0].body["tools"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn a_provider_refusal_in_the_first_step_is_the_clients_error() {
    let rig = Rig::new(Fake::new()).await;
    rig.openai.answer(Answer::error(429));
    let (status, text) = rig.ask("/v1/chat/completions", ask_gpt()).await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS, "{text}");
    let _ = error_code;
}

fn anthropic_stream(frames: &[&str]) -> Answer {
    Answer::events(frames.iter().map(|f| f.trim_end().into()).collect())
}

#[tokio::test]
async fn text_before_a_held_tool_call_streams_and_the_next_steps_blocks_follow_without_gaps() {
    let rig = Rig::new(Fake::new().answering("files__read", Outcome::ok("alpha"))).await;
    rig.anthropic.answer(anthropic_stream(&[
        "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_1\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"claude-x\",\"content\":[],\"usage\":{\"input_tokens\":5,\"output_tokens\":1}}}\n\n",
        "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n",
        "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"Reading.\"}}\n\n",
        "event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
        "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":1,\"content_block\":{\"type\":\"tool_use\",\"id\":\"toolu_1\",\"name\":\"files__read\",\"input\":{}}}\n\n",
        "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":1,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"{\\\"path\\\":\\\"a.txt\\\"}\"}}\n\n",
        "event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":1}\n\n",
        "event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"tool_use\",\"stop_sequence\":null},\"usage\":{\"output_tokens\":9}}\n\n",
        "event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n",
    ]));
    rig.anthropic.answer(Answer::text("Alpha."));
    let body = json!({"model": "claude-x", "max_tokens": 100, "stream": true,
                      "messages": [{"role": "user", "content": "read"}]});
    let (status, text) = rig.ask("/v1/messages", body).await;
    assert_eq!(status, StatusCode::OK, "{text}");
    let events: Vec<Value> = text
        .split("\n\n")
        .filter_map(|f| f.lines().find_map(|l| l.strip_prefix("data: ")))
        .filter_map(|d| serde_json::from_str(d).ok())
        .collect();
    let kinds: Vec<(String, Option<u64>)> = events
        .iter()
        .map(|e| (e["type"].as_str().unwrap().to_owned(), e["index"].as_u64()))
        .collect();
    let starts: Vec<_> = kinds
        .iter()
        .filter(|(k, _)| k == "content_block_start")
        .collect();
    assert_eq!(
        starts.iter().map(|(_, i)| *i).collect::<Vec<_>>(),
        [Some(0), Some(1)],
        "{text}"
    );
    assert!(!text.contains("tool_use"), "{text}");
    assert_eq!(
        kinds.iter().filter(|(k, _)| k == "message_start").count(),
        1
    );
    assert_eq!(kinds.iter().filter(|(k, _)| k == "message_stop").count(), 1);
    let said: String = events
        .iter()
        .filter_map(|e| e["delta"]["text"].as_str())
        .collect();
    assert_eq!(said, "Reading.Alpha.", "{text}");
    let stop = events
        .iter()
        .find(|e| e["type"] == "message_delta")
        .unwrap();
    assert_eq!(stop["delta"]["stop_reason"], "end_turn", "{text}");
    // The held call and its result were in the second request.
    let sent = rig.anthropic.requests();
    let history = sent[1].body["messages"].as_array().unwrap();
    assert_eq!(history[1]["content"][0]["text"], "Reading.");
    assert_eq!(history[1]["content"][1]["type"], "tool_use");
    assert_eq!(history[2]["content"][0]["content"], "alpha");
}

/// An Anthropic client with extended thinking on.
fn thinking_ask(stream: bool) -> Value {
    json!({"model": "claude-sonnet-4-5", "max_tokens": 4096, "stream": stream,
           "thinking": {"type": "enabled", "budget_tokens": 2048},
           "messages": [{"role": "user", "content": "read a.txt"}]})
}

#[tokio::test]
async fn signed_thinking_goes_back_before_the_tool_call_so_anthropic_accepts_the_next_step() {
    // The fake refuses a thinking-enabled request whose last assistant turn
    // does not begin with its thinking block, as Anthropic does.
    for stream in [false, true] {
        let rig = Rig::new(Fake::new().answering("files__read", Outcome::ok("alpha"))).await;
        let step = Answer::tool_call("files__read", json!({"path": "a.txt"})).reasoning("hmm now");
        rig.anthropic.answer(step);
        rig.anthropic
            .answer(Answer::text("Alpha.").reasoning("fine then"));
        let (status, text) = rig.ask("/v1/messages", thinking_ask(stream)).await;
        assert_eq!(status, StatusCode::OK, "{text}");
        let sent = rig.anthropic.requests();
        assert_eq!(sent.len(), 2, "{text}");
        let said = &sent[1].body["messages"][1]["content"];
        assert_eq!(said[0]["type"], "thinking", "{said}");
        assert_eq!(said[0]["thinking"], "hmm now", "{said}");
        let signature = said[0]["signature"].as_str().unwrap();
        assert!(signature.starts_with("sig_fake"), "{said}");
        assert_eq!(said[1]["type"], "tool_use", "{said}");
        // The client sees one reply, the thinking of both steps in it, with
        // the blocks numbered without gaps and the signatures with them.
        if stream {
            let events: Vec<Value> = text
                .split("\n\n")
                .filter_map(|f| f.lines().find_map(|l| l.strip_prefix("data: ")))
                .filter_map(|d| serde_json::from_str(d).ok())
                .collect();
            let signed: Vec<(u64, &str)> = events
                .iter()
                .filter(|e| e["delta"]["type"] == "signature_delta")
                .filter(|e| e["delta"]["signature"] != "")
                .map(|e| {
                    (
                        e["index"].as_u64().unwrap(),
                        e["delta"]["signature"].as_str().unwrap(),
                    )
                })
                .collect();
            assert_eq!(signed.len(), 2, "{text}");
            assert_eq!(signed[0].0, 0, "{text}");
            assert_eq!(signed[1].0, 1, "{text}");
            assert_eq!(signed[0].1, signature, "{text}");
            let starts: Vec<u64> = events
                .iter()
                .filter(|e| e["type"] == "content_block_start")
                .map(|e| e["index"].as_u64().unwrap())
                .collect();
            assert_eq!(starts, [0, 1, 2], "{text}");
        }
    }
}

#[tokio::test]
async fn a_gemini_thought_signature_goes_back_on_its_function_call() {
    let rig = Rig::new(Fake::new().answering("files__read", Outcome::ok("alpha"))).await;
    let reply = |parts: Value, reason: &str| {
        let part = json!({"candidates": [{"content": {"role": "model", "parts": parts},
                                          "finishReason": reason, "index": 0}],
                          "usageMetadata": {"promptTokenCount": 3, "candidatesTokenCount": 2,
                                            "totalTokenCount": 5},
                          "modelVersion": "gemini-3-flash-preview", "responseId": "r1"});
        Answer::body(part)
    };
    let call = json!({"functionCall": {"name": "files__read", "args": {"path": "a.txt"}},
                      "thoughtSignature": "sigG1"});
    rig.gemini.answer(reply(json!([call]), "STOP"));
    rig.gemini
        .answer(reply(json!([{"text": "Alpha."}]), "STOP"));
    let body = json!({"model": "gemini-3-flash-preview",
                      "messages": [{"role": "user", "content": "read a.txt"}]});
    let (status, reply) = rig.chat(body).await;
    assert_eq!(status, StatusCode::OK, "{reply}");
    assert_eq!(reply["choices"][0]["message"]["content"], "Alpha.");
    let sent = rig.gemini.requests();
    assert_eq!(sent.len(), 2);
    // Gemini 3 refuses a function call without the signature it sent; the
    // fake does not check, so this does.
    let model = &sent[1].body["contents"][1];
    assert_eq!(model["role"], "model", "{}", sent[1].body);
    assert_eq!(model["parts"][0]["thoughtSignature"], "sigG1", "{model}");
    assert_eq!(model["parts"][0]["functionCall"]["name"], "files__read");
}

#[tokio::test]
async fn redacted_thinking_goes_back_whole() {
    let rig = Rig::new(Fake::new()).await;
    rig.anthropic.answer(anthropic_stream(&[
        "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"m\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"claude-x\",\"content\":[],\"usage\":{\"input_tokens\":5,\"output_tokens\":1}}}\n\n",
        "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"redacted_thinking\",\"data\":\"opaque\"}}\n\n",
        "event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
        "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":1,\"content_block\":{\"type\":\"tool_use\",\"id\":\"toolu_1\",\"name\":\"files__read\",\"input\":{}}}\n\n",
        "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":1,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"{}\"}}\n\n",
        "event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":1}\n\n",
        "event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"tool_use\",\"stop_sequence\":null},\"usage\":{\"output_tokens\":9}}\n\n",
        "event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n",
    ]));
    rig.anthropic.answer(Answer::text("Done."));
    let body = json!({"model": "claude-x", "max_tokens": 100, "stream": true,
                      "messages": [{"role": "user", "content": "read"}]});
    let (status, text) = rig.ask("/v1/messages", body).await;
    assert_eq!(status, StatusCode::OK, "{text}");
    let sent = rig.anthropic.requests();
    let said = &sent[1].body["messages"][1]["content"];
    assert_eq!(
        said[0],
        json!({"type": "redacted_thinking", "data": "opaque"})
    );
    assert_eq!(said[1]["type"], "tool_use");
    // The client sees the redacted block once, as block 0, and the answer.
    assert!(text.contains("redacted_thinking"), "{text}");
    assert!(text.contains("Done."), "{text}");
}

fn read_n(n: usize) -> Vec<Value> {
    (0..n)
        .map(|i| call(&format!("call_{i}"), "files__read", json!({"path": i})))
        .collect()
}

#[tokio::test]
async fn at_most_parallel_calls_of_a_step_run_at_once() {
    let fake = Fake::new().slow(Duration::from_millis(50));
    let rig = Rig::with(fake, |a| a.parallel(2)).await;
    rig.openai.answer(calls(read_n(5), (1, 1)));
    rig.openai.answer(says("done", (1, 1)));
    let (status, _) = rig.chat(ask_gpt()).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(rig.fake.calls().len(), 5);
    assert_eq!(rig.fake.peak.load(Ordering::SeqCst), 2);
    // The answers keep the order asked.
    let sent = rig.openai.requests();
    let ids: Vec<&str> = sent[1].body["messages"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|m| m["role"] == "tool")
        .map(|m| m["tool_call_id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, ["call_0", "call_1", "call_2", "call_3", "call_4"]);
}

#[tokio::test]
async fn the_answers_of_one_request_share_a_budget() {
    let fake = Fake::new().answering("files__read", Outcome::ok("x".repeat(100)));
    let rig = Rig::with(fake, |a| a.max_results(150)).await;
    rig.openai.answer(calls(read_n(3), (1, 1)));
    rig.openai.answer(says("done", (1, 1)));
    let (status, _) = rig.chat(ask_gpt()).await;
    assert_eq!(status, StatusCode::OK);
    let sent = rig.openai.requests();
    let tools: Vec<String> = sent[1].body["messages"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|m| m["role"] == "tool")
        .map(|m| m["content"].as_str().unwrap().to_owned())
        .collect();
    assert_eq!(tools[0], "x".repeat(100));
    assert!(tools[1].starts_with(&"x".repeat(50)), "{}", tools[1]);
    assert!(tools[1].contains("[cut: 50 more bytes]"), "{}", tools[1]);
    assert!(tools[2].contains("left out"), "{}", tools[2]);
}
