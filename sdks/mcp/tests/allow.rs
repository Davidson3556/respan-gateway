//! The tool allow-list, and the traffic it cannot see.

mod common;

use std::sync::Arc;

use common::*;
use respan_gateway_core::http::StatusCode;
use respan_gateway_core::serde_json::{Value, json};
use respan_gateway_core::{ClientContext, Fault, Gateway, Plugin, Response, async_trait, codes};
use respan_gateway_mcp::{Passthrough, StreamableHttp, ToServer, ToolAllowList};
use respan_gateway_testkit::{MockTransport, Outcomes, mock};

fn guarded(mock: &MockTransport, log: &Outcomes) -> Gateway {
    let p = Gateway::new().provider("mcp", StreamableHttp::default(), mock.at(UPSTREAM));
    let plugins: Vec<Arc<dyn Plugin>> = vec![
        Arc::new(log.clone()),
        Arc::new(ToolAllowList::new(["echo"])),
    ];
    gateway(p, StreamableHttp::default(), plugins, "mcp")
}

fn tools() -> Value {
    json!({"jsonrpc":"2.0","id":1,"result":{"tools":[{"name":"echo"},{"name":"wipe"}]}})
}

fn names(message: &Value) -> Vec<&str> {
    let tools = message["result"]["tools"].as_array();
    tools
        .into_iter()
        .flatten()
        .filter_map(|t| t["name"].as_str())
        .collect()
}

#[tokio::test]
async fn tools_list_shows_only_allowed_tools_in_json() {
    let mock = MockTransport::new();
    mock.reply(UPSTREAM, mock::json(StatusCode::OK, &tools().to_string()));
    let gw = guarded(&mock, &Outcomes::default());

    let (_, _, body, _) = send(&gw, post(&rpc(1, "tools/list", json!({})))).await;

    assert_eq!(names(&messages(&body)[0]), ["echo"]);
}

#[tokio::test]
async fn tools_list_shows_only_allowed_tools_in_a_stream_keeping_event_fields() {
    let mock = MockTransport::new();
    let event = format!("id: 9\nevent: message\ndata: {}\n\n", tools());
    mock.reply(UPSTREAM, mock::sse(&[&event]));
    let gw = guarded(&mock, &Outcomes::default());

    let (_, _, body, _) = send(&gw, post(&rpc(1, "tools/list", json!({})))).await;

    assert!(body.starts_with("id: 9\nevent: message\n"), "{body}");
    assert_eq!(names(&messages(&body)[0]), ["echo"]);
}

#[tokio::test]
async fn a_forbidden_call_is_refused_in_json_rpc_shape_and_never_sent() {
    let mock = MockTransport::new();
    let log = Outcomes::default();
    let gw = guarded(&mock, &log);

    let (status, _, body, _) = send(&gw, post(&call_tool(5, "wipe"))).await;

    assert_eq!(status, StatusCode::OK);
    let error = &messages(&body)[0];
    assert_eq!(
        (error["id"].clone(), error["error"]["code"].clone()),
        (json!(5), json!(-32602))
    );
    assert!(mock.sent().is_empty());
    // Refused, keeping JSON-RPC's 200 (F6, fixed by A11): other plugins
    // see a failure.
    assert!(log.only().end.is_err());
}

#[tokio::test]
async fn an_allowed_call_passes_untouched() {
    let mock = MockTransport::new();
    let reply = r#"{"jsonrpc":"2.0","id":5,"result":{"content":[]}}"#;
    mock.reply(UPSTREAM, mock::json(StatusCode::OK, reply));
    let gw = guarded(&mock, &Outcomes::default());

    let request = call_tool(5, "echo");
    let (_, _, body, _) = send(&gw, post(&request)).await;

    assert_eq!(body, reply);
    assert_eq!(mock.sent()[0].body(), request.as_bytes());
}

#[tokio::test]
async fn a_batch_loses_its_forbidden_call_which_is_answered_at_the_end() {
    let mock = MockTransport::new();
    let pong = json!({"jsonrpc":"2.0","id":1,"result":{}});
    mock.reply(UPSTREAM, mock::json(StatusCode::OK, &format!("[{pong}]")));
    let gw = guarded(&mock, &Outcomes::default());

    let batch = format!("[{},{}]", rpc(1, "ping", json!({})), call_tool(2, "wipe"));
    let (_, _, body, _) = send(&gw, post(&batch)).await;

    let sent: Value = serde_json_from(mock.sent()[0].body());
    assert_eq!(sent.as_array().map(Vec::len), Some(1));
    let got = messages(&body);
    assert_eq!(got[0], pong);
    assert_eq!(
        (got[1]["id"].clone(), got[1]["error"]["code"].clone()),
        (json!(2), json!(-32602))
    );
}

/// FINDINGS F4 (fixed by A2): the decoder reads the call as sent, so a
/// stream ends once every request the call sent is answered.
#[tokio::test]
async fn a_streamed_batch_reply_ends_once_the_calls_requests_are_answered() {
    let mock = MockTransport::new();
    let pong = json!({"jsonrpc":"2.0","id":1,"result":{}});
    mock.reply(UPSTREAM, sse(std::slice::from_ref(&pong)));
    let log = Outcomes::default();
    let gw = guarded(&mock, &log);

    let batch = format!("[{},{}]", rpc(1, "ping", json!({})), call_tool(2, "wipe"));
    let (_, _, body, _) = send(&gw, post(&batch)).await;

    assert_eq!(messages(&body).len(), 2);
    assert!(log.only().end.is_ok(), "{:?}", log.only());
}

fn serde_json_from(bytes: &[u8]) -> Value {
    respan_gateway_core::serde_json::from_slice(bytes).unwrap_or_default()
}

/// A refusal that keeps JSON-RPC's convention of a 200 (FINDINGS F6).
struct RefuseWith200;

#[async_trait]
impl Plugin for RefuseWith200 {
    async fn on_client(&self, _cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        Err(Fault::new(
            StatusCode::OK,
            "tool_forbidden",
            "tool 'wipe' is not allowed",
        ))
    }
}

#[tokio::test]
async fn a_refusal_keeps_json_rpcs_200() {
    let mock = MockTransport::new();
    let p = Gateway::new().provider("mcp", StreamableHttp::default(), mock.at(UPSTREAM));
    let gw = gateway(
        p,
        StreamableHttp::default(),
        vec![Arc::new(RefuseWith200)],
        "mcp",
    );

    let (status, _, body, outcome) = send(&gw, post(&call_tool(5, "wipe"))).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(messages(&body)[0]["id"], 5);
    assert!(outcome.is_some_and(|o| o.end.is_err()));
}

/// Passthrough served beside the MCP format, and a PII-style plugin that
/// reads only MCP.
fn mixed(mock: &MockTransport, catch_all_first: bool, guard: Arc<dyn Plugin>) -> Gateway {
    let raw = Passthrough::new("raw", [""]);
    let providers = Gateway::new()
        .provider("mcp", StreamableHttp::default(), mock.at(UPSTREAM))
        .and_then(|g| g.provider("raw", raw.clone(), mock.at(UPSTREAM)));
    let gw = providers.unwrap_or_else(|e| panic!("{e}"));
    let gw = match catch_all_first {
        true => gw
            .serve(raw)
            .and_then(|g| g.serve(StreamableHttp::default())),
        false => gw
            .serve(StreamableHttp::default())
            .and_then(|g| g.serve(raw)),
    };
    gw.and_then(|g| g.plugin("guard", guard))
        .and_then(|g| g.target("mcp"))
        .and_then(|g| g.target("raw"))
        .unwrap_or_else(|e| panic!("{e}"))
}

/// R1 (fixed by L-7): the allow-list refuses traffic of no kind, which it
/// cannot read, while other kinds pass: `is_kind::<()>()` is true exactly
/// for a format of no kind.
#[tokio::test]
async fn traffic_of_no_kind_is_refused_by_the_allow_list() {
    let mock = MockTransport::new();
    mock.reply(UPSTREAM, mock::json(StatusCode::OK, "{}"));
    let gw = mixed(&mock, false, Arc::new(ToolAllowList::new(["echo"])));

    let other = respan_gateway_testkit::post("/v1/other", "{}");
    let (status, _, _, _) = send(&gw, other).await;

    assert_eq!(status, StatusCode::FORBIDDEN);
    assert!(mock.sent().is_empty());
}

/// R1 (fixed): a catch-all of no kind served first takes MCP's requests,
/// and the allow-list refuses them rather than letting them bypass it.
#[tokio::test]
async fn a_catch_all_served_first_does_not_bypass_the_allow_list() {
    let mock = MockTransport::new();
    mock.reply(UPSTREAM, mock::json(StatusCode::OK, "{}"));
    let gw = mixed(&mock, true, Arc::new(ToolAllowList::new(["echo"])));

    let (status, _, _, _) = send(&gw, post(&call_tool(5, "wipe"))).await;

    assert_eq!(status, StatusCode::FORBIDDEN);
    assert!(mock.sent().is_empty());
}

/// A guard written the short way, `cx.input::<ToServer>()?`.
struct Strict;

#[async_trait]
impl Plugin for Strict {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        cx.input::<ToServer>()?;
        Ok(None)
    }
}

#[tokio::test]
async fn a_guard_that_fails_closed_refuses_traffic_of_no_kind_as_a_500() {
    let mock = MockTransport::new();
    let gw = mixed(&mock, false, Arc::new(Strict));

    let other = respan_gateway_testkit::post("/v1/other", "{}");
    let (status, _, body, outcome) = send(&gw, other).await;

    // The passthrough's error is plain text, with the kernel's message.
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert!(body.contains("another kind"), "{body}");
    let end = outcome.map(|o| o.end);
    assert!(matches!(end, Some(Err(f)) if f.code() == codes::INTERNAL));
    assert!(mock.sent().is_empty());
}

/// R1 (fixed): a plugin tells traffic of no kind from another kind's by
/// `is_kind::<()>()`; `input::<()>` stays an `Err`, as no kind has no input.
#[tokio::test]
async fn traffic_of_no_kind_is_told_from_another_kinds() {
    type Seen = Arc<std::sync::Mutex<Vec<(bool, bool, String)>>>;
    struct Probe(Seen);

    #[async_trait]
    impl Plugin for Probe {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let unit = cx
                .input::<()>()
                .map(|_| ())
                .map_err(|f| f.code().to_owned());
            let seen = (
                cx.is_kind::<()>(),
                cx.is_kind::<ToServer>(),
                unit.err().unwrap_or_default(),
            );
            let mut all = self
                .0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            all.push(seen);
            Ok(None)
        }
    }

    let mock = MockTransport::new();
    let seen = Seen::default();
    let providers = Gateway::new()
        .provider(
            "raw",
            Passthrough::new("raw", ["/v1/other"]),
            mock.at(UPSTREAM),
        )
        .and_then(|g| {
            g.provider(
                "chat",
                respan_gateway_formats::OpenAiChat,
                mock.at(UPSTREAM),
            )
        });
    let gw = providers
        .and_then(|g| g.serve(Passthrough::new("raw", ["/v1/other"])))
        .and_then(|g| g.serve(respan_gateway_formats::OpenAiChat))
        .and_then(|g| g.plugin("probe", Arc::new(Probe(seen.clone()))))
        .and_then(|g| g.target("raw"))
        .and_then(|g| g.target("chat"))
        .unwrap_or_else(|e| panic!("{e}"));
    mock.reply(UPSTREAM, mock::json(StatusCode::OK, "{}"));
    mock.reply(UPSTREAM, mock::json(StatusCode::OK, "{}"));

    let _ = send(&gw, respan_gateway_testkit::post("/v1/other", "{}")).await;
    let chat = r#"{"model":"m","messages":[{"role":"user","content":"hi"}]}"#;
    let _ = send(
        &gw,
        respan_gateway_testkit::post("/v1/chat/completions", chat),
    )
    .await;

    let seen = seen
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone();
    assert_eq!(seen.len(), 2);
    assert_eq!(seen[0], (true, false, "internal".into()));
    assert_eq!(seen[1], (false, false, "internal".into()));
}
