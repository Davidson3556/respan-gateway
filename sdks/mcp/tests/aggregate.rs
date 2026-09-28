//! Two MCP servers behind one endpoint.

mod common;

use std::sync::Arc;

use common::*;
use respan_gateway_core::futures::StreamExt;
use respan_gateway_core::http::{self, Method, StatusCode};
use respan_gateway_core::serde_json::{Value, json};
use respan_gateway_core::{
    ClientContext, Decoder, Doc, Encoder, Fault, Format, Gateway, Outcome, Plugin, ProviderContext,
    Response, async_trait, codes,
};
use respan_gateway_mcp::{Aggregate, StdioTransport, StreamableHttp, ToClient, ToServer, Verb};
use respan_gateway_testkit::{MockTransport, mock};

const A: &str = "http://a.test";
const B: &str = "http://b.test";

fn two(mock: &MockTransport, plugin: Arc<dyn Plugin>) -> Gateway {
    let providers = Gateway::new()
        .provider("a", StreamableHttp::default(), mock.at(A))
        .and_then(|g| g.provider("b", StreamableHttp::default(), mock.at(B)));
    gateway(providers, StreamableHttp::default(), vec![plugin], "a")
}

fn with_session(reply: mock::Reply, id: &str) -> mock::Reply {
    let mut reply = reply;
    let id = id.parse().unwrap_or_else(|e| panic!("{e}"));
    reply.headers_mut().insert("mcp-session-id", id);
    reply
}

fn init_reply(caps: Value) -> String {
    json!({"jsonrpc":"2.0","id":1,"result":{"protocolVersion":"2025-06-18","capabilities":caps}})
        .to_string()
}

/// Initializes both servers and returns the gateway's session id.
async fn initialize(gw: &Gateway, mock: &MockTransport) -> String {
    mock.reply(
        A,
        with_session(
            mock::json(StatusCode::OK, &init_reply(json!({"tools":{}}))),
            "sa",
        ),
    );
    let b = init_reply(json!({"resources":{}}));
    mock.reply(
        B,
        with_session(mock::sse(&[&format!("data: {b}\n\n")]), "sb"),
    );
    let init = rpc(1, "initialize", json!({"protocolVersion":"2025-06-18"}));
    let (status, headers, body, _) = send(gw, post(&init)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let caps = &messages(&body)[0]["result"]["capabilities"];
    assert_eq!(caps, &json!({"tools":{}, "resources":{}}));
    let id = headers.get("mcp-session-id").and_then(|v| v.to_str().ok());
    id.unwrap_or_else(|| panic!("no session")).to_owned()
}

fn session_of(req: &http::Request<respan_gateway_core::bytes::Bytes>) -> &str {
    let id = req
        .headers()
        .get("mcp-session-id")
        .and_then(|v| v.to_str().ok());
    id.unwrap_or_default()
}

#[tokio::test]
async fn initialize_merges_both_servers_under_one_session_of_the_gateways() {
    let mock = MockTransport::new();
    let gw = two(&mock, Arc::new(Aggregate::new(["a", "b"])));
    let session = initialize(&gw, &mock).await;
    assert!(session.starts_with("agg-"));
}

#[tokio::test]
async fn tools_list_merges_under_prefixes_each_server_with_its_own_session() {
    let mock = MockTransport::new();
    let gw = two(&mock, Arc::new(Aggregate::new(["a", "b"])));
    let session = initialize(&gw, &mock).await;
    let tools = |names: &[&str]| {
        let tools: Vec<Value> = names.iter().map(|n| json!({"name": n})).collect();
        json!({"jsonrpc":"2.0","id":2,"result":{"tools":tools}}).to_string()
    };
    mock.reply(A, mock::json(StatusCode::OK, &tools(&["echo"])));
    mock.reply(B, mock::json(StatusCode::OK, &tools(&["search", "fetch"])));

    let list = mcp(
        Method::POST,
        &rpc(2, "tools/list", json!({})),
        &[("mcp-session-id", &session)],
    );
    let (_, _, body, _) = send(&gw, list).await;

    let names: Vec<String> = messages(&body)[0]["result"]["tools"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|t| t["name"].as_str().map(str::to_owned))
        .collect();
    assert_eq!(names, ["a__echo", "b__search", "b__fetch"]);
    let sent = mock.sent();
    assert_eq!((session_of(&sent[2]), session_of(&sent[3])), ("sa", "sb"));
}

#[tokio::test]
async fn tools_call_goes_to_the_server_its_prefix_names_streamed_as_it_comes() {
    let mock = MockTransport::new();
    let gw = two(&mock, Arc::new(Aggregate::new(["a", "b"])));
    let session = initialize(&gw, &mock).await;
    let progress =
        json!({"jsonrpc":"2.0","method":"notifications/progress","params":{"progress":1}});
    let result = json!({"jsonrpc":"2.0","id":3,"result":{"content":[]}});
    mock.reply(B, sse(&[progress.clone(), result.clone()]));

    let call = mcp(
        Method::POST,
        &call_tool(3, "b__search"),
        &[("mcp-session-id", &session)],
    );
    let (_, _, body, _) = send(&gw, call).await;

    assert_eq!(messages(&body), vec![progress, result]);
    let sent = mock.sent();
    let to_b = &sent[2];
    assert_eq!(to_b.uri(), "http://b.test/mcp");
    assert_eq!(session_of(to_b), "sb");
    let v: Value = respan_gateway_core::serde_json::from_slice(to_b.body()).unwrap_or_default();
    assert_eq!(v["params"]["name"], "search");
}

#[tokio::test]
async fn a_notification_and_a_delete_reach_both_servers() {
    let mock = MockTransport::new();
    let gw = two(&mock, Arc::new(Aggregate::new(["a", "b"])));
    let session = initialize(&gw, &mock).await;
    let with = [("mcp-session-id", session.as_str())];
    for server in [A, B] {
        mock.reply(server, bare(StatusCode::ACCEPTED));
    }
    let note = r#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#;
    let (status, _, body, _) = send(&gw, mcp(Method::POST, note, &with)).await;
    assert_eq!((status, body.as_str()), (StatusCode::ACCEPTED, ""));

    for server in [A, B] {
        mock.reply(server, bare(StatusCode::OK));
    }
    let (status, _, _, _) = send(&gw, mcp(Method::DELETE, "", &with)).await;
    assert_eq!(status, StatusCode::OK);
    let methods: Vec<_> = mock
        .sent()
        .iter()
        .skip(4)
        .map(|r| r.method().clone())
        .collect();
    assert_eq!(methods, [Method::DELETE, Method::DELETE]);

    let (status, _, body, _) =
        send(&gw, mcp(Method::POST, &rpc(9, "ping", json!({})), &with)).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(messages(&body)[0]["id"], 9);
}

#[tokio::test]
async fn a_stdio_server_and_an_http_one_aggregate_alike() {
    let mock = MockTransport::new();
    let stdio = StdioTransport::new(env!("CARGO_BIN_EXE_mcp-echo"), []);
    let providers = Gateway::new()
        .provider("a", StreamableHttp::default(), stdio)
        .and_then(|g| g.provider("b", StreamableHttp::default(), mock.at(B)));
    let gw = gateway(
        providers,
        StreamableHttp::default(),
        vec![Arc::new(Aggregate::new(["a", "b"]))],
        "a",
    );
    mock.reply(
        B,
        with_session(mock::json(StatusCode::OK, &init_reply(json!({}))), "sb"),
    );
    let init = rpc(1, "initialize", json!({"protocolVersion":"2025-06-18"}));
    let (_, headers, _, _) = send(&gw, post(&init)).await;
    let session = headers
        .get("mcp-session-id")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_owned();

    let call = mcp(
        Method::POST,
        &call_tool(2, "a__echo"),
        &[("mcp-session-id", &session)],
    );
    let (_, _, body, _) = send(&gw, call).await;

    assert_eq!(messages(&body)[1]["result"]["content"][0]["text"], "hi");
}

/// FINDINGS F2 (fixed by C2): a GET merges the servers' own streams, read
/// with `events` as the answer streams.
#[tokio::test]
async fn a_get_merges_both_servers_streams() {
    let mock = MockTransport::new();
    let gw = two(&mock, Arc::new(Aggregate::new(["a", "b"])));
    let session = initialize(&gw, &mock).await;
    let from =
        |s: &str| json!({"jsonrpc":"2.0","method":"notifications/message","params":{"from":s}});
    mock.reply(A, sse(&[from("a")]));
    mock.reply(B, sse(&[from("b")]));

    let get = mcp(Method::GET, "", &[("mcp-session-id", &session)]);
    let (status, _, body, _) = send(&gw, get).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(messages(&body).len(), 2);
}

/// Answers with both calls' events, read as the answer streams.
struct LazyMerge;

#[async_trait]
impl Plugin for LazyMerge {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let (a, b) = (cx.call("a").await?, cx.call("b").await?);
        let events = a.outputs::<ToClient>().chain(b.outputs());
        Ok(Some(Response::stream(events)))
    }
}

/// F2 (fixed by C2): a call's response is read from the answer's stream.
#[tokio::test]
async fn a_call_is_read_while_the_answer_streams() {
    let mock = MockTransport::new();
    let gw = two(&mock, Arc::new(LazyMerge));
    for server in [A, B] {
        mock.reply(
            server,
            sse(&[json!({"jsonrpc":"2.0","method":"notifications/message"})]),
        );
    }

    let get = mcp(Method::GET, "", &[("mcp-session-id", "s")]);
    let (status, _, body, outcome) = send(&gw, get).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(messages(&body).len(), 2, "{body}");
    assert!(outcome.is_some_and(|o| o.end.is_ok()));
}

/// Ends the session on server `a` with a call of its own, whatever the
/// client sent.
struct EndOnA;

#[async_trait]
impl Plugin for EndOnA {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let resp = cx.call("a").with_input(ToServer::bare(Verb::End)).await?;
        Ok(Some(resp))
    }
}

/// F8, fixed: `prepare` sees the call's own input, so its verb reaches the
/// request line: a `DELETE` with no body.
#[tokio::test]
async fn a_call_with_its_own_input_uses_its_own_verb() {
    let mock = MockTransport::new();
    mock.reply(A, bare(StatusCode::OK));
    let gw = two(&mock, Arc::new(EndOnA));

    let (status, _, _, _) = send(&gw, post(&rpc(1, "ping", json!({})))).await;

    assert_eq!(status, StatusCode::OK);
    let sent = &mock.sent()[0];
    assert_eq!(
        (sent.method().clone(), sent.body().len()),
        (Method::DELETE, 0)
    );
}

/// Reads the input, changing nothing.
struct Reads;

#[async_trait]
impl Plugin for Reads {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        cx.input::<ToServer>()?;
        Ok(None)
    }
}

/// Does nothing.
struct Nothing;

#[async_trait]
impl Plugin for Nothing {}

/// `StreamableHttp`, noting whether each `prepare` got the input.
#[derive(Clone, Default)]
struct Noting(StreamableHttp, Arc<std::sync::Mutex<Vec<bool>>>);

impl Format for Noting {
    type Input = ToServer;
    type Output = ToClient;

    fn name(&self) -> &str {
        self.0.name()
    }

    fn accepts(&self, head: &http::request::Parts) -> bool {
        self.0.accepts(head)
    }

    fn decode_input(&self, head: &http::request::Parts, body: &Doc) -> Result<ToServer, Fault> {
        self.0.decode_input(head, body)
    }

    fn encode_input(
        &self,
        input: &ToServer,
        original: Option<(&Doc, &ToServer)>,
    ) -> Result<Doc, Fault> {
        self.0.encode_input(input, original)
    }

    fn prepare(
        &self,
        body: Doc,
        input: Option<&ToServer>,
        client: &http::request::Parts,
        relayed: bool,
    ) -> Result<http::Request<Doc>, Fault> {
        let mut seen = self
            .1
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        seen.push(input.is_some());
        self.0.prepare(body, input, client, relayed)
    }

    fn decoder(&self, head: &http::response::Parts, call: &http::Request<Doc>) -> Box<dyn Decoder> {
        self.0.decoder(head, call)
    }

    fn encoder(
        &self,
        head: &mut http::response::Parts,
        client: &http::Request<Doc>,
        relayed: bool,
    ) -> Box<dyn Encoder> {
        self.0.encoder(head, client, relayed)
    }
}

/// A client's `DELETE`, relayed, keeps its verb whether or not a plugin
/// read the input. Since T10-6 both are one path: `prepare` gets `None`, as
/// the body is the client's own, and the verb is the client's method. The
/// two-path trap noted in round 9 is gone.
#[tokio::test]
async fn a_relayed_delete_keeps_its_verb_read_or_unread() {
    for plugin in [Arc::new(Reads) as Arc<dyn Plugin>, Arc::new(Nothing)] {
        let (mock, noting) = (MockTransport::new(), Noting::default());
        mock.reply(A, bare(StatusCode::OK));
        let providers = Gateway::new().provider("a", noting.clone(), mock.at(A));
        let gw = gateway(providers, StreamableHttp::default(), vec![plugin], "a");

        let req = mcp(Method::DELETE, "", &[("mcp-session-id", "s1")]);
        let _ = send(&gw, req).await;

        assert_eq!(
            mock.sent().first().map(|r| r.method().clone()),
            Some(Method::DELETE)
        );
        let seen = noting.1.lock().map(|s| s.clone()).unwrap_or_default();
        assert_eq!(seen, [false]);
    }
}

/// Merges both calls' events as they come, like `Aggregate`'s GET.
struct SelectMerge;

#[async_trait]
impl Plugin for SelectMerge {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        use respan_gateway_core::futures::stream;
        let (a, b) = (cx.call("a").await?, cx.call("b").await?);
        let merged = stream::select(a.outputs::<ToClient>(), b.outputs::<ToClient>());
        Ok(Some(Response::stream(merged)))
    }
}

/// Records how each call ended.
#[derive(Clone, Default)]
struct CallEnds(Arc<std::sync::Mutex<Vec<Result<(), Fault>>>>);

#[async_trait]
impl Plugin for CallEnds {
    async fn on_provider_end(&self, _px: &ProviderContext, outcome: &Outcome) {
        let mut ends = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        ends.push(outcome.end.clone());
    }
}

/// C2: a client leaving a merged stream ends every merged call abandoned,
/// and nothing waits on them.
#[tokio::test]
async fn a_client_leaving_a_merged_stream_ends_both_calls_abandoned() {
    let (fa, feed_a) = Fed::new("text/event-stream");
    let (fb, _feed_b) = Fed::new("text/event-stream");
    let providers = Gateway::new()
        .provider("a", StreamableHttp::default(), fa)
        .and_then(|g| g.provider("b", StreamableHttp::default(), fb));
    let ends = CallEnds::default();
    let gw = gateway(
        providers,
        StreamableHttp::default(),
        vec![Arc::new(ends.clone()), Arc::new(SelectMerge)],
        "a",
    );
    let note = json!({"jsonrpc":"2.0","method":"notifications/message"});
    let _ = feed_a.send(Ok(respan_gateway_core::bytes::Bytes::from(format!(
        "data: {note}\n\n"
    ))));

    let get = mcp(Method::GET, "", &[("mcp-session-id", "s")]);
    let (resp, ended) = tokio::time::timeout(PROMPTLY, gw.handle(get))
        .await
        .unwrap_or_else(|_| panic!("the head comes with the first event"));
    let mut body = resp.into_body();
    use respan_gateway_core::http_body_util::BodyExt;
    assert!(body.frame().await.is_some_and(|f| f.is_ok()));
    drop(body);

    let outcome = tokio::time::timeout(PROMPTLY, ended).await.ok();
    assert!(outcome.is_some_and(|o| o.end.is_err_and(|f| f.code() == codes::ABANDONED)));
    assert!(tokio::time::timeout(PROMPTLY, gw.idle()).await.is_ok());
    let ends = ends
        .0
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone();
    assert_eq!(ends.len(), 2);
    assert!(
        ends.iter()
            .all(|e| e.as_ref().is_err_and(|f| f.code() == codes::ABANDONED)),
        "{ends:?}"
    );
}

/// N1 (fixed by M-1): a call with its own input stays in the client's
/// session, as `prepare` always sees the client's head.
#[tokio::test]
async fn a_call_with_its_own_input_stays_in_the_clients_session() {
    let mock = MockTransport::new();
    mock.reply(A, bare(StatusCode::OK));
    let gw = two(&mock, Arc::new(EndOnA));

    let with = [("mcp-session-id", "s-1")];
    let _ = send(&gw, mcp(Method::POST, &rpc(1, "ping", json!({})), &with)).await;

    let sent = &mock.sent()[0];
    assert_eq!(session_of(sent), "s-1");
    // Only the session: nothing else of the client's passes.
    assert!(sent.headers().get("accept").is_some());
}

/// R4 (fixed by `Response::with_status`): the client's encoder sees the
/// aggregate's own `202 Accepted`, so it writes a bare 202 with no content
/// type and no head edit.
#[tokio::test]
async fn an_aggregated_notification_gets_a_bare_202() {
    let mock = MockTransport::new();
    let gw = two(&mock, Arc::new(Aggregate::new(["a", "b"])));
    let session = initialize(&gw, &mock).await;
    for server in [A, B] {
        mock.reply(server, bare(StatusCode::ACCEPTED));
    }
    let note = r#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#;
    let with = [("mcp-session-id", session.as_str())];
    let (status, headers, _, _) = send(&gw, mcp(Method::POST, note, &with)).await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert!(headers.get("content-type").is_none(), "{headers:?}");
}

/// F1 for a plugin's stream (MC-1 fixed by `Encoder::open`): a merge of
/// servers that have nothing to say yet opens at once, as the format opens
/// a `GET` stream with an SSE comment.
#[tokio::test]
async fn an_aggregated_quiet_get_opens_at_once() {
    let (fa, _feed_a) = Fed::new("text/event-stream");
    let (fb, _feed_b) = Fed::new("text/event-stream");
    let providers = Gateway::new()
        .provider("a", StreamableHttp::default(), fa)
        .and_then(|g| g.provider("b", StreamableHttp::default(), fb));
    let gw = gateway(
        providers,
        StreamableHttp::default(),
        vec![Arc::new(SelectMerge)],
        "a",
    );
    let get = mcp(Method::GET, "", &[("mcp-session-id", "s")]);
    let head = tokio::time::timeout(PROMPTLY, gw.handle(get)).await;
    let Ok((resp, _)) = head else {
        panic!("the head waits for a quiet server")
    };
    assert_eq!(resp.status(), StatusCode::OK);
    use respan_gateway_core::http_body_util::BodyExt;
    let first = resp.into_body().frame().await;
    let first = first.and_then(|f| f.ok()).and_then(|f| f.into_data().ok());
    assert_eq!(first.as_deref(), Some(&b": stream\n\n"[..]));
}
