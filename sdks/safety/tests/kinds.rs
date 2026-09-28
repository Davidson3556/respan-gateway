//! Traffic of another kind: an MCP-like user format a chat guardrail must
//! not pass silently.

mod common;

use common::*;
use respan_gateway_core::futures::TryStreamExt;
use respan_gateway_core::http::StatusCode;
use respan_gateway_core::serde_json::{Value, json};
use respan_gateway_core::{
    Call, ClientContext, Fault, Plugin, ProviderContext, Response, async_trait, codes,
};
use respan_gateway_formats::conversation::{Conversation, Message, Reply, Role};
use respan_safety::guard::{BLOCKED, Guard, Policy, UNCOVERED};
use respan_safety::mcp::{McpRequest, McpResult};

fn policy() -> Policy {
    Policy::new(["rm -rf"])
}

/// A gateway with default targets `openai` and `mcp`: each request goes to
/// the first it can use, so MCP-like requests need no router.
fn harness(plugins: Vec<(&str, std::sync::Arc<dyn Plugin>)>) -> Harness {
    Harness::build(plugins, |gw| gw.target("openai")?.target("mcp"))
}

fn json_of(body: &str) -> Value {
    respan_gateway_core::serde_json::from_str(body).unwrap_or(Value::Null)
}

#[tokio::test]
async fn a_chat_guard_refuses_a_kind_it_cannot_read() {
    let h = harness(vec![(
        "guard",
        plugin(Guard::new(policy()).conversations()),
    )]);
    let (status, body, _) = h
        .text(mcp_request("shell", json!({ "cmd": "rm -rf /" })))
        .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    // In the MCP-like format's own framing.
    assert_eq!(json_of(&body)["error"]["data"]["code"], UNCOVERED, "{body}");
    assert!(h.mock.sent().is_empty());
}

#[tokio::test]
async fn a_guard_told_the_kind_blocks_tool_call_arguments() {
    let guard = Guard::new(policy())
        .conversations()
        .kind::<McpRequest, McpResult>();
    let h = harness(vec![("guard", plugin(guard))]);
    let (status, body, _) = h
        .text(mcp_request("shell", json!({ "cmd": "rm -rf /" })))
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(json_of(&body)["error"]["data"]["code"], BLOCKED, "{body}");
    assert!(h.mock.sent().is_empty());
}

#[tokio::test]
async fn a_guard_told_the_kind_blocks_a_tool_result() {
    let guard = Guard::new(policy()).kind::<McpRequest, McpResult>();
    let h = harness(vec![("guard", plugin(guard))]);
    h.reply(MCP, mcp_result(json!({ "suggest": "run rm -rf ~" })));
    let (status, body, _) = h.text(mcp_request("advise", json!({}))).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(json_of(&body)["error"]["data"]["code"], BLOCKED, "{body}");
}

#[tokio::test]
async fn a_clean_tool_call_passes_untouched() {
    let guard = Guard::new(policy()).kind::<McpRequest, McpResult>();
    let h = harness(vec![("guard", plugin(guard))]);
    h.reply(MCP, mcp_result(json!({ "files": 3 })));
    let req = mcp_request("ls", json!({ "dir": "/tmp" }));
    let (status, body, _) = h.text(req).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json_of(&body)["result"]["files"], 3);
    // Unchanged, the client's body is relayed as it came, its id included.
    assert_eq!(h.sent_to(MCP)[0]["id"], 7);
}

/// A guard written the easy way: it reads chat if it can.
struct Lenient;

#[async_trait]
impl Plugin for Lenient {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        if let Ok(chat) = cx.input::<Conversation>() {
            policy().check(&chat.messages.iter().map(Message::text).collect::<String>())?;
        }
        Ok(None)
    }
}

/// F9: the core fails closed only for plugins that use `?`; `if let Ok`
/// silently passes other kinds, and a malformed chat body too.
#[tokio::test]
async fn a_guard_that_ignores_the_error_passes_other_kinds_silently() {
    let h = harness(vec![("lenient", plugin(Lenient))]);
    h.reply(MCP, mcp_result(json!({})));
    let (status, _, _) = h
        .text(mcp_request("shell", json!({ "cmd": "rm -rf /" })))
        .await;
    assert_eq!(status, StatusCode::OK);
}

/// N1 (fixed by r2 #11): a request whose kind no default target can take
/// is the gateway's misconfiguration, a 500 `config`, not the client's 404.
#[tokio::test]
async fn a_kind_no_target_can_take_is_a_config_fault() {
    let guard = Guard::new(policy()).kind::<McpRequest, McpResult>();
    let h = Harness::new(vec![("guard", plugin(guard))]);
    let (status, _, outcome) = h.text(mcp_request("ls", json!({}))).await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(outcome.end.expect_err("refused").code(), codes::CONFIG);
}

/// Judges MCP tool calls with a chat model.
struct ModelJudge {
    judge: String,
}

#[async_trait]
impl Plugin for ModelJudge {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        if !cx.is_kind::<McpRequest>() {
            return Ok(None);
        }
        let call = cx.input::<McpRequest>()?;
        let prompt = Conversation::new([Message::new(
            Role::User,
            format!("Is this tool call safe? {}", call.params),
        )]);
        let verdict = cx.call(&self.judge).with_input(prompt).await?;
        let _: Vec<Reply> = verdict.outputs().try_collect().await?;
        Ok(None)
    }
}

/// F12 (fixed by 0006): a call with its own input may go to a provider of
/// another kind, so a chat model can judge an MCP tool call.
#[tokio::test]
async fn a_chat_model_can_judge_an_mcp_tool_call() {
    let h = harness(vec![(
        "judge",
        plugin(ModelJudge {
            judge: "judge".into(),
        }),
    )]);
    h.reply(JUDGE, oa_whole("safe"));
    h.reply(MCP, mcp_result(json!({})));
    let (status, _, outcome) = h.text(mcp_request("ls", json!({}))).await;
    assert!(outcome.end.is_ok(), "{outcome:?}");
    assert_eq!(status, StatusCode::OK);
    assert!(
        h.sent_to(JUDGE)[0]
            .to_string()
            .contains("Is this tool call safe?")
    );
}

// F13: a guardrail that must cover every call lives on the provider edge,
// where `px.client().call` is now a compile error (`call` takes `&mut
// self`), so the ignored test and its run-time pin are gone; the stash below
// is the way.

/// F13 workaround: a `Call` is 'static and Clone, so one made in
/// `on_client` can be sent from every call's `on_provider`.
struct StashedJudge;

#[async_trait]
impl Plugin for StashedJudge {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let judge = cx.call("judge");
        cx.head_mut().extensions.insert(judge);
        Ok(None)
    }

    async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
        // The judge's own call was made before the stash, so it has none.
        let Some(judge) = px.extensions().get::<Call>().cloned() else {
            return Ok(());
        };
        let prompt = Conversation::new([Message::new(Role::User, "safe?")]);
        let verdict = judge.with_input(prompt).await?;
        let _: Vec<Reply> = verdict.outputs().try_collect().await?;
        Ok(())
    }
}

#[tokio::test]
async fn a_provider_edge_guard_asks_a_model_through_a_call_made_on_the_client_edge() {
    let h = Harness::new(vec![("judge", plugin(StashedJudge))]);
    h.reply(JUDGE, oa_whole("yes"));
    h.reply(OPENAI, oa_stream(&["hi"]));
    let (status, _, outcome) = h.text(openai_request("hi", true)).await;
    assert!(outcome.end.is_ok(), "{outcome:?}");
    assert_eq!(status, StatusCode::OK);
    assert_eq!(h.sent_to(JUDGE).len(), 1);
}
