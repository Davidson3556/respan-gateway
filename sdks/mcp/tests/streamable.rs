//! MCP over streamable HTTP relayed through the kernel to one server.

mod common;

use common::*;
use respan_gateway_core::Gateway;
use respan_gateway_core::bytes::Bytes;
use respan_gateway_core::codes;
use respan_gateway_core::http::{Method, StatusCode};
use respan_gateway_core::serde_json::{Value, json};
use respan_gateway_mcp::StreamableHttp;
use respan_gateway_testkit::{MockTransport, mock};

fn relay(mock: &MockTransport) -> respan_gateway_core::Gateway {
    let p = Gateway::new().provider("mcp", StreamableHttp::default(), mock.at(UPSTREAM));
    gateway(p, StreamableHttp::default(), Vec::new(), "mcp")
}

#[tokio::test]
async fn an_initialize_is_relayed_byte_for_byte_with_its_session_header() {
    let mock = MockTransport::new();
    let reply = r#"{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":"2025-06-18"}}"#;
    let mut r = mock::json(StatusCode::OK, reply);
    r.headers_mut().insert(
        "mcp-session-id",
        "s-1".parse().unwrap_or_else(|e| panic!("{e}")),
    );
    mock.reply(UPSTREAM, r);
    let gw = relay(&mock);

    let request = r#"{"jsonrpc":"2.0", "id":1, "method":"initialize", "params":{"x-new":1}}"#;
    let (status, headers, body, outcome) = send(&gw, post(request)).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        headers.get("mcp-session-id").map(|v| v.as_bytes()),
        Some(&b"s-1"[..])
    );
    assert_eq!(body, reply);
    let sent = &mock.sent()[0];
    assert_eq!(sent.method(), Method::POST);
    assert_eq!(sent.uri(), "http://mcp.test/mcp");
    // The unchanged input reaches the server as the client's own bytes.
    assert_eq!(sent.body(), request.as_bytes());
    assert!(outcome.is_some_and(|o| o.end.is_ok()));
}

#[tokio::test]
async fn a_streamed_reply_ends_at_the_response_to_its_request() {
    let mock = MockTransport::new();
    let progress =
        json!({"jsonrpc":"2.0","method":"notifications/progress","params":{"progress":1}});
    let result = json!({"jsonrpc":"2.0","id":7,"result":{"content":[]}});
    let stray = json!({"jsonrpc":"2.0","method":"notifications/message","params":{}});
    mock.reply(UPSTREAM, sse(&[progress.clone(), result.clone(), stray]));
    let gw = relay(&mock);

    let (status, headers, body, outcome) = send(&gw, post(&call_tool(7, "echo"))).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        headers.get("content-type").map(|v| v.as_bytes()),
        Some(&b"text/event-stream"[..])
    );
    // What the server sent after the response is not the request's.
    assert_eq!(messages(&body), vec![progress, result]);
    assert!(outcome.is_some_and(|o| o.end.is_ok()));
}

#[tokio::test]
async fn a_stream_cut_before_the_response_ends_truncated_in_json_rpc_shape() {
    let mock = MockTransport::new();
    let progress =
        json!({"jsonrpc":"2.0","method":"notifications/progress","params":{"progress":1}});
    mock.reply(UPSTREAM, sse(std::slice::from_ref(&progress)));
    let gw = relay(&mock);

    let (_, _, body, outcome) = send(&gw, post(&call_tool(7, "echo"))).await;

    let got = messages(&body);
    assert_eq!(got[0], progress);
    assert_eq!(got[1]["id"], 7);
    assert_eq!(got[1]["error"]["data"]["code"], codes::TRUNCATED);
    let end = outcome.map(|o| o.end);
    assert!(matches!(end, Some(Err(f)) if f.code() == codes::TRUNCATED));
}

#[tokio::test]
async fn a_notification_gets_202_with_no_body() {
    let mock = MockTransport::new();
    mock.reply(UPSTREAM, bare(StatusCode::ACCEPTED));
    let gw = relay(&mock);

    let note = r#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#;
    let (status, headers, body, outcome) = send(&gw, post(note)).await;

    assert_eq!((status, body.as_str()), (StatusCode::ACCEPTED, ""));
    assert!(headers.get("content-type").is_none());
    let outcome = outcome.unwrap_or_default();
    assert_eq!(outcome.head.map(|h| h.status), Some(StatusCode::ACCEPTED));
    assert!(outcome.end.is_ok());
}

#[tokio::test]
async fn a_get_opens_the_servers_stream_with_no_body_sent() {
    let mock = MockTransport::new();
    let a = json!({"jsonrpc":"2.0","method":"notifications/tools/list_changed"});
    let ping = json!({"jsonrpc":"2.0","id":"s1","method":"ping"});
    mock.reply(UPSTREAM, sse(&[a.clone(), ping.clone()]));
    let gw = relay(&mock);

    let get = mcp(
        Method::GET,
        "",
        &[("mcp-session-id", "s-1"), ("last-event-id", "41")],
    );
    let (status, _, body, outcome) = send(&gw, get).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(messages(&body), vec![a, ping]);
    assert!(outcome.is_some_and(|o| o.end.is_ok()));
    let sent = &mock.sent()[0];
    assert_eq!(sent.method(), Method::GET);
    assert!(sent.body().is_empty());
    assert!(sent.headers().get("content-type").is_none());
    assert_eq!(
        sent.headers().get("mcp-session-id").map(|v| v.as_bytes()),
        Some(&b"s-1"[..])
    );
    // Resumption passes on.
    assert_eq!(
        sent.headers().get("last-event-id").map(|v| v.as_bytes()),
        Some(&b"41"[..])
    );
}

#[tokio::test]
async fn a_delete_ends_the_session_upstream() {
    let mock = MockTransport::new();
    mock.reply(UPSTREAM, bare(StatusCode::OK));
    let gw = relay(&mock);

    let delete = mcp(Method::DELETE, "", &[("mcp-session-id", "s-1")]);
    let (status, _, body, _) = send(&gw, delete).await;

    assert_eq!((status, body.as_str()), (StatusCode::OK, ""));
    let sent = &mock.sent()[0];
    assert_eq!(sent.method(), Method::DELETE);
    assert_eq!(
        sent.headers().get("mcp-session-id").map(|v| v.as_bytes()),
        Some(&b"s-1"[..])
    );
}

#[tokio::test]
async fn a_json_rpc_error_inside_a_200_is_content_not_a_failure() {
    let mock = MockTransport::new();
    let reply = r#"{"jsonrpc":"2.0","id":3,"error":{"code":-32601,"message":"no such method"}}"#;
    mock.reply(UPSTREAM, mock::json(StatusCode::OK, reply));
    let gw = relay(&mock);

    let (status, _, body, outcome) = send(&gw, post(&rpc(3, "nope", Value::Null))).await;

    assert_eq!((status, body.as_str()), (StatusCode::OK, reply));
    // The Outcome cannot say that the call failed (FINDINGS F12).
    assert!(
        outcome
            .is_some_and(|o| o.end.is_ok() && o.head.is_some_and(|h| h.status == StatusCode::OK))
    );
}

fn guarded(mock: &MockTransport) -> respan_gateway_core::Gateway {
    let p = Gateway::new().provider("mcp", StreamableHttp::default(), mock.at(UPSTREAM));
    let guard: std::sync::Arc<dyn respan_gateway_core::Plugin> =
        std::sync::Arc::new(respan_gateway_mcp::ToolAllowList::new(["echo"]));
    gateway(p, StreamableHttp::default(), vec![guard], "mcp")
}

/// R3 (fixed by Q-5): a whole request always carries one input, decoded
/// by the format, so an empty POST is the format's JSON-RPC parse error
/// (-32700).
#[tokio::test]
async fn an_empty_post_is_refused_as_a_json_rpc_parse_error_once_a_plugin_reads_it() {
    let mock = MockTransport::new();
    let p = Gateway::new().provider("mcp", StreamableHttp::default(), mock.at(UPSTREAM));
    let guard: std::sync::Arc<dyn respan_gateway_core::Plugin> =
        std::sync::Arc::new(respan_gateway_mcp::ToolAllowList::new(["echo"]));
    let gw = gateway(p, StreamableHttp::default(), vec![guard], "mcp");

    let (status, _, body, _) = send(&gw, post("")).await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    let error = &messages(&body)[0];
    assert_eq!(error["id"], Value::Null);
    assert_eq!(error["error"]["code"], -32700);
    assert!(mock.sent().is_empty());
}

/// R3 (fixed by Q-5): a GET's empty body is its input, which the format
/// decodes to its verb, so a plugin reading `cx.input()` lets it pass.
#[tokio::test]
async fn a_plugin_reading_the_input_sees_a_gets_verb() {
    struct Reads;

    #[respan_gateway_core::async_trait]
    impl respan_gateway_core::Plugin for Reads {
        async fn on_client(
            &self,
            cx: &mut respan_gateway_core::ClientContext,
        ) -> Result<Option<respan_gateway_core::Response>, respan_gateway_core::Fault> {
            let input = cx.input::<respan_gateway_mcp::ToServer>()?;
            assert_eq!(input.verb, respan_gateway_mcp::Verb::Listen);
            Ok(None)
        }
    }
    let mock = MockTransport::new();
    let note = json!({"jsonrpc":"2.0","method":"notifications/message"});
    mock.reply(UPSTREAM, sse(std::slice::from_ref(&note)));
    let p = Gateway::new().provider("mcp", StreamableHttp::default(), mock.at(UPSTREAM));
    let gw = gateway(
        p,
        StreamableHttp::default(),
        vec![std::sync::Arc::new(Reads)],
        "mcp",
    );

    let get = mcp(Method::GET, "", &[("mcp-session-id", "s-1")]);
    let (status, _, body, _) = send(&gw, get).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(messages(&body), vec![note]);
}

#[tokio::test]
async fn the_allow_list_lets_a_get_pass() {
    let mock = MockTransport::new();
    let note = json!({"jsonrpc":"2.0","method":"notifications/message"});
    mock.reply(UPSTREAM, sse(std::slice::from_ref(&note)));
    let gw = guarded(&mock);

    let get = mcp(Method::GET, "", &[("mcp-session-id", "s-1")]);
    let (status, _, body, _) = send(&gw, get).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(messages(&body), vec![note]);
}

#[tokio::test]
async fn with_no_plugin_reading_it_a_body_that_is_not_mcp_is_relayed() {
    let mock = MockTransport::new();
    mock.reply(UPSTREAM, bare(StatusCode::BAD_REQUEST));
    let gw = relay(&mock);

    let (status, _, _, _) = send(&gw, post("")).await;

    // Decoding is lazy: a plain relay never judges the body.
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(mock.sent().len(), 1);
}

#[tokio::test]
async fn an_expired_session_is_relayed_as_404_so_the_client_reinitializes() {
    let mock = MockTransport::new();
    let reply =
        r#"{"jsonrpc":"2.0","id":null,"error":{"code":-32600,"message":"session expired"}}"#;
    mock.reply(UPSTREAM, mock::json(StatusCode::NOT_FOUND, reply));
    let gw = relay(&mock);

    let (status, _, body, _) = send(&gw, post(&rpc(1, "tools/list", json!({})))).await;

    assert_eq!((status, body.as_str()), (StatusCode::NOT_FOUND, reply));
}

#[tokio::test]
async fn a_batch_stream_ends_once_every_request_is_answered() {
    let mock = MockTransport::new();
    let one = json!({"jsonrpc":"2.0","id":1,"result":{}});
    let two = json!({"jsonrpc":"2.0","id":2,"result":{}});
    mock.reply(UPSTREAM, sse(&[two.clone(), one.clone()]));
    let gw = relay(&mock);

    let batch = format!(
        "[{},{}]",
        rpc(1, "ping", json!({})),
        rpc(2, "ping", json!({}))
    );
    let (_, _, body, outcome) = send(&gw, post(&batch)).await;

    assert_eq!(messages(&body), vec![two, one]);
    assert!(outcome.is_some_and(|o| o.end.is_ok()));
}

#[tokio::test]
async fn a_huge_result_split_into_small_chunks_arrives_intact() {
    let mock = MockTransport::new();
    let text = "x".repeat(16 * 1024 * 1024);
    let result = json!({"jsonrpc":"2.0","id":1,"result":{"content":[{"type":"text","text":text}]}});
    let event = format!("data: {result}\n\n");
    let chunks: Vec<_> = event
        .as_bytes()
        .chunks(64 * 1024)
        .map(|c| Ok(Bytes::copy_from_slice(c)))
        .collect();
    let mut reply = mock::sse(&[]);
    *reply.body_mut() = chunks;
    mock.reply(UPSTREAM, reply);
    let gw = relay(&mock);

    let started = std::time::Instant::now();
    let (_, _, body, outcome) = send(&gw, post(&call_tool(1, "echo"))).await;

    assert_eq!(body, event);
    assert!(outcome.is_some_and(|o| o.end.is_ok()));
    assert!(started.elapsed() < std::time::Duration::from_secs(20));
}

/// F1, fixed in the format by `Encoder::open`: a `GET` stream from a
/// server with nothing to say yet gets its `200` at once, as the format
/// opens it with an SSE comment.
#[tokio::test]
async fn a_quiet_servers_stream_opens_at_once() {
    let (fed, _feed) = Fed::new("text/event-stream");
    let p = Gateway::new().provider("mcp", StreamableHttp::default(), fed);
    let gw = gateway(p, StreamableHttp::default(), Vec::new(), "mcp");

    let get = mcp(Method::GET, "", &[("mcp-session-id", "s-1")]);
    let head = tokio::time::timeout(PROMPTLY, gw.handle(get)).await;

    assert!(head.is_ok_and(|(r, _)| r.status() == StatusCode::OK));
}

/// The comment comes first, then the server's own events as it sent them.
#[tokio::test]
async fn a_relayed_get_stream_is_the_comment_then_the_servers_bytes() {
    use respan_gateway_core::http_body_util::BodyExt;
    let (fed, feed) = Fed::new("text/event-stream");
    let p = Gateway::new().provider("mcp", StreamableHttp::default(), fed);
    let gw = gateway(p, StreamableHttp::default(), Vec::new(), "mcp");

    let get = mcp(Method::GET, "", &[("mcp-session-id", "s-1")]);
    let (resp, _) = gw.handle(get).await;
    let mut body = resp.into_body();
    let note = json!({"jsonrpc":"2.0","method":"notifications/message"});
    let event = format!("id: 7\ndata: {note}\n\n");
    let _ = feed.send(Ok(Bytes::from(event.clone())));
    let mut seen = Vec::new();
    while seen.len() < 2 {
        let frame = tokio::time::timeout(PROMPTLY, body.frame()).await;
        let data = frame.ok().flatten().and_then(|f| f.ok()?.into_data().ok());
        seen.push(data.map(|d| String::from_utf8_lossy(&d).into_owned()));
    }

    assert_eq!(seen, [Some(": stream\n\n".to_owned()), Some(event)]);
}

/// F1's neighbour: `Gateway::idle`, the only shutdown hook today, waits
/// for every open stream, so one idle MCP `GET` stream holds a config swap
/// open forever (the drain with a deadline is promised, not there).
#[tokio::test]
async fn today_idle_never_resolves_while_a_get_stream_is_open() {
    let (fed, feed) = Fed::new("text/event-stream");
    let p = Gateway::new().provider("mcp", StreamableHttp::default(), fed);
    let gw = gateway(p, StreamableHttp::default(), Vec::new(), "mcp");
    let note = json!({"jsonrpc":"2.0","method":"notifications/message"});
    let _ = feed.send(Ok(Bytes::from(format!("data: {note}\n\n"))));

    let get = mcp(Method::GET, "", &[("mcp-session-id", "s-1")]);
    let (resp, _) = gw.handle(get).await;

    let idle = tokio::time::timeout(std::time::Duration::from_millis(300), gw.idle()).await;
    assert!(idle.is_err());
    drop(resp);
    assert!(tokio::time::timeout(PROMPTLY, gw.idle()).await.is_ok());
}
