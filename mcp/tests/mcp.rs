//! The promises of the MCP kind, format and stdio transport.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use respan_gateway_http::bytes::Bytes;
use respan_gateway_http::http::header::CONTENT_TYPE;
use respan_gateway_http::http::{self, HeaderMap, Method, StatusCode};
use respan_gateway_http::http_body_util::BodyExt;
use respan_gateway_http::{
    Body, Completion, Credential, Incoming, Outgoing, credentials, proxy_call, serve,
    strip_client_origin_headers, strip_origin_headers,
};
use respan_gateway_mcp::{
    Owner, SESSION, StdioTransport, StreamableHttp, TIMED_OUT, ToClient, ToServer,
};
use respan_gateway_sdk::futures::{StreamExt, stream};
use respan_gateway_sdk::{ClientContext, Extensions, Fault, Format, Gateway, Plugin, Response};
use respan_gateway_sdk::{async_trait, codes};
use serde_json::{Value, json};

use respan_gateway_http::relay;

const AT: &str = "http://mcp.test";

/// An MCP server in `sh`: answers each request with an empty result.
const ECHO: &str = r#"while IFS= read -r line; do
  id=$(printf '%s' "$line" | sed -n 's/.*"id":\([0-9]*\).*/\1/p')
  [ -n "$id" ] && printf '{"jsonrpc":"2.0","id":%s,"result":{}}\n' "$id"
done"#;

fn typed_gateway(
    transport: impl Fn(Extensions) -> Outgoing + Send + Sync + 'static,
    plugin: Option<Touch>,
) -> Gateway {
    let gw = Gateway::new().provider::<ToServer, _, _>("mcp", StreamableHttp::default(), transport);
    let gw = gw.and_then(|g| g.client::<ToServer, _, _>(StreamableHttp::default()));
    let gw = match plugin {
        Some(p) => gw.map(|g| g.plugin(p)),
        None => gw,
    };
    gw.and_then(|g| g.target("mcp"))
        .unwrap_or_else(|e| panic!("{e}"))
}

fn gateway(
    transport: impl Fn(Extensions) -> Outgoing + Send + Sync + 'static,
    plugin: Option<Touch>,
) -> Gateway {
    typed_gateway(transport, plugin).plugin(relay::Relay::RAW)
}

/// Filters every input and output, changing them only if `edit`.
struct Touch {
    edit: bool,
}

#[async_trait]
impl Plugin for Touch {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        cx.filter(|_: &mut ToServer| Ok(()))?;
        let edit = self.edit;
        cx.filter(move |out: &mut ToClient| {
            if edit && let Some(m) = out.messages.first_mut() {
                m["result"] = json!({ "edited": true });
            }
            Ok(())
        })?;
        Ok(None)
    }
}

fn request(method: Method, body: &str, headers: &[(&str, &str)]) -> http::Request<Body> {
    let mut req = http::Request::builder().method(method).uri("/mcp?x=1");
    for (k, v) in headers {
        req = req.header(*k, *v);
    }
    let body = Body::full(body.to_owned());
    req.body(body).unwrap_or_else(|e| panic!("{e}"))
}

async fn send(
    gw: &Gateway,
    req: http::Request<Body>,
) -> (StatusCode, HeaderMap, String, Result<(), Fault>) {
    let (resp, outcome) = serve(gw, req).await;
    let (head, body) = resp.into_parts();
    let body = BodyExt::collect(body).await.map(|b| b.to_bytes());
    let body = body.unwrap_or_else(|f| panic!("aborted: {f}"));
    let text = String::from_utf8_lossy(&body).into_owned();
    (head.status, head.headers, text, outcome.await)
}

/// The messages of a JSON body or of each event.
fn messages(body: &str) -> Vec<Value> {
    let each = body.split_inclusive("\n\n");
    let all = each
        .filter_map(respan_gateway_http::formats::json)
        .flat_map(|v| match v {
            Value::Array(all) => all,
            one => vec![one],
        });
    all.collect()
}

type Reply = http::Response<Vec<Result<Bytes, Fault>>>;

fn typed(status: StatusCode, content_type: Option<&'static str>, chunks: &[&str]) -> Reply {
    let mut reply = http::Response::new(
        chunks
            .iter()
            .map(|s| Ok(Bytes::copy_from_slice(s.as_bytes())))
            .collect(),
    );
    *reply.status_mut() = status;
    if let Some(content_type) = content_type {
        reply
            .headers_mut()
            .insert(CONTENT_TYPE, http::HeaderValue::from_static(content_type));
    }
    reply
}

fn sse(chunks: &[&str]) -> Reply {
    typed(StatusCode::OK, Some("text/event-stream"), chunks)
}

fn json_reply(status: StatusCode, body: &str) -> Reply {
    typed(status, Some("application/json"), &[body])
}

fn bare(status: StatusCode) -> Reply {
    typed(status, None, &[])
}

/// Script only MCP replies in arrival order; retain the actual sent heads and
/// bytes to assert exact unedited relay. This is local to the MCP scenarios.
#[derive(Default, Clone)]
struct ScriptedMcp {
    replies: Arc<Mutex<VecDeque<Reply>>>,
    sent: Arc<Mutex<Vec<http::Request<Bytes>>>>,
}

impl ScriptedMcp {
    fn new() -> Self {
        Self::default()
    }

    fn reply(&self, response: Reply) {
        self.replies
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push_back(response);
    }

    fn sent(&self) -> Vec<http::Request<Bytes>> {
        self.sent
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    async fn send(
        &self,
        call: http::Request<Body>,
    ) -> Result<(http::Response<Body>, Option<Completion>), Fault> {
        let (mut head, body) = call.into_parts();
        proxy_call(&mut head);
        strip_client_origin_headers(&mut head.headers);
        let mut request = http::Request::from_parts(head, body);
        credentials(&mut request, &HeaderMap::new());
        let (mut head, body) = request.into_parts();
        let path = head.uri.path_and_query().map_or("/", |p| p.as_str());
        head.uri = format!("{AT}{path}")
            .parse()
            .map_err(|e| Fault::config("bad MCP test URI").with_source(e))?;
        let body = BodyExt::collect(body).await?.to_bytes();
        head.extensions = http::Extensions::new();
        self.sent
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(http::Request::from_parts(head, body));
        let reply = self
            .replies
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .pop_front()
            .ok_or_else(|| Fault::upstream("no MCP test reply"))?;
        let (mut head, chunks) = reply.into_parts();
        strip_origin_headers(&mut head.headers);
        Ok((
            http::Response::from_parts(head, Body::stream(stream::iter(chunks))),
            None,
        ))
    }
}

impl ScriptedMcp {
    fn connect(self) -> impl Fn(Extensions) -> Outgoing + Send + Sync {
        respan_gateway_http::connect(move |call| {
            let value = self.clone();
            async move { value.send(call).await }
        })
    }
}

#[tokio::test]
async fn an_unedited_message_relays_byte_exact_and_an_edited_one_into_its_event() {
    let body = r#"{ "jsonrpc":"2.0",  "id":1, "method":"tools/list" }"#;
    let events = [
        ": hi\n\nid: 7\n",
        "data: {\"jsonrpc\":\"2.0\", \"id\":1,\"result\":{}}\n\n",
    ];
    let json = r#"{"jsonrpc":"2.0" , "id":1,"result":{}}"#;
    for plugin in [None, Some(false)] {
        let mock = ScriptedMcp::new();
        mock.reply(sse(&events));
        mock.reply(json_reply(StatusCode::OK, json));
        let gw = gateway(mock.clone().connect(), plugin.map(|edit| Touch { edit }));
        let posted = || request(Method::POST, body, &[(SESSION, "s1")]);
        let (status, headers, sse, outcome) = send(&gw, posted()).await;
        assert_eq!((status, &sse[..]), (StatusCode::OK, &events.concat()[..]));
        assert_eq!(headers["content-type"], "text/event-stream");
        assert!(outcome.is_ok());
        let (_, _, whole, _) = send(&gw, posted()).await;
        assert_eq!(whole, json);
        let sent = &mock.sent()[0];
        assert_eq!(
            (sent.uri().to_string(), &sent.body()[..]),
            (format!("{AT}/mcp?x=1"), body.as_bytes())
        );
        assert_eq!(sent.headers()[SESSION], "s1");
    }
    let mock = ScriptedMcp::new();
    mock.reply(sse(&events));
    let gw = gateway(mock.clone().connect(), Some(Touch { edit: true }));
    let (_, _, sse, _) = send(&gw, request(Method::POST, body, &[])).await;
    let edited = r#"id: 7
data: {"jsonrpc":"2.0","id":1,"result":{"edited":true}}

"#;
    assert_eq!(sse, format!(": hi\n\n{edited}"));
}

/// A plugin-created response has no provider response head; the format must
/// choose the client's framing rather than copying the value's original wire.
struct Rebuild;

#[async_trait]
impl Plugin for Rebuild {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let outputs = cx
            .call("mcp")
            .await?
            .outputs::<ToClient>()
            .collect::<Vec<_>>()
            .await;
        Ok(Some(Response::stream(stream::iter(outputs))))
    }
}

#[tokio::test]
async fn a_rebuilt_sse_answer_is_json_when_posted() {
    let mock = ScriptedMcp::new();
    let event = "id: 7\ndata: {\"jsonrpc\":\"2.0\", \"id\":1,\"result\":{}}\n\n";
    mock.reply(sse(&[event]));
    let gw = typed_gateway(mock.connect(), None).plugin(Rebuild);
    let (status, headers, body, outcome) = send(&gw, request(Method::POST, INIT, &[])).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers[CONTENT_TYPE], "application/json");
    assert_eq!(
        messages(&body),
        [json!({"jsonrpc":"2.0","id":1,"result":{}})]
    );
    assert!(!body.contains("data:"), "{body}");
    assert!(outcome.is_ok());
}

#[tokio::test]
async fn a_rebuilt_json_answer_is_framed_for_get() {
    let mock = ScriptedMcp::new();
    mock.reply(json_reply(
        StatusCode::OK,
        r#"{"jsonrpc":"2.0", "method":"notifications/message"}"#,
    ));
    let gw = typed_gateway(mock.connect(), None).plugin(Rebuild);
    let (status, headers, body, outcome) = send(&gw, request(Method::GET, "", &[])).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers[CONTENT_TYPE], "text/event-stream");
    assert_eq!(
        messages(&body),
        [json!({"jsonrpc":"2.0","method":"notifications/message"})]
    );
    assert!(body.starts_with(": stream\n\ndata: "), "{body}");
    assert!(outcome.is_ok());
}

#[tokio::test]
async fn a_batch_answer_is_complete_only_once_every_id_is_answered() {
    let batch = r#"[{"jsonrpc":"2.0","id":1,"method":"a"},{"jsonrpc":"2.0","id":2,"method":"b"}]"#;
    let answer = |id| {
        format!(
            "data: {}\n\n",
            json!({ "jsonrpc": "2.0", "id": id, "result": {} })
        )
    };
    let note = "data: {\"jsonrpc\":\"2.0\",\"method\":\"notifications/progress\"}\n\n";
    let mock = ScriptedMcp::new();
    mock.reply(sse(&[&answer(2)]));
    mock.reply(sse(&[&answer(2), note, &answer(1)]));
    let gw = gateway(mock.clone().connect(), None);

    let (status, _, body, outcome) = send(&gw, request(Method::POST, batch, &[])).await;
    assert_eq!(status, StatusCode::OK);
    let [answered, unanswered] = &messages(&body)[..] else {
        panic!("{body}")
    };
    assert_eq!(
        (&answered["id"], &answered["result"]),
        (&json!(2), &json!({}))
    );
    assert_eq!(
        (&unanswered["id"], &unanswered["error"]["data"]["code"]),
        (&json!(1), &json!(codes::TRUNCATED))
    );
    assert_eq!(
        outcome.map_err(|f| f.code().to_owned()),
        Err(codes::TRUNCATED.to_owned())
    );

    let (_, _, body, outcome) = send(&gw, request(Method::POST, batch, &[])).await;
    assert_eq!(messages(&body).len(), 3);
    assert!(outcome.is_ok());
}

#[tokio::test]
async fn delete_and_202_are_handled() {
    let note = r#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#;
    for plugin in [None, Some(false)] {
        let mock = ScriptedMcp::new();
        mock.reply(bare(StatusCode::NO_CONTENT));
        mock.reply(bare(StatusCode::ACCEPTED));
        let gw = gateway(mock.clone().connect(), plugin.map(|edit| Touch { edit }));

        let (status, _, body, outcome) =
            send(&gw, request(Method::DELETE, "", &[(SESSION, "s1")])).await;
        assert_eq!((status, &body[..]), (StatusCode::NO_CONTENT, ""));
        assert!(outcome.is_ok());
        let (status, headers, body, outcome) =
            send(&gw, request(Method::POST, note, &[(SESSION, "s1")])).await;
        assert_eq!((status, &body[..]), (StatusCode::ACCEPTED, ""));
        assert!(!headers.contains_key("content-type"));
        assert!(outcome.is_ok());

        let sent = mock.sent();
        assert_eq!(
            (sent[0].method(), &sent[0].body()[..]),
            (&Method::DELETE, &b""[..])
        );
        assert_eq!(sent[0].headers()[SESSION], "s1");
        assert_eq!(
            (sent[1].method(), &sent[1].body()[..]),
            (&Method::POST, note.as_bytes())
        );
    }
}

#[tokio::test]
async fn a_stdio_child_answers_a_request_in_its_session() {
    let gw = gateway(StdioTransport::new("sh", ["-c", ECHO]).connect(), None);
    let init = r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}"#;
    let (status, headers, body, _) = send(&gw, request(Method::POST, init, &[])).await;
    assert_eq!(
        (status, &messages(&body)[0]["id"]),
        (StatusCode::OK, &json!(1))
    );
    let session = headers[SESSION].to_str().unwrap_or_default().to_owned();
    let with = [(SESSION, &session[..])];

    let list = r#"{"jsonrpc":"2.0","id":2,"method":"tools/list"}"#;
    let (_, _, body, outcome) = send(&gw, request(Method::POST, list, &with)).await;
    assert_eq!(
        messages(&body),
        [json!({ "jsonrpc": "2.0", "id": 2, "result": {} })]
    );
    assert!(outcome.is_ok());
    let note = r#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#;
    assert_eq!(
        send(&gw, request(Method::POST, note, &with)).await.0,
        StatusCode::ACCEPTED
    );
    assert_eq!(
        send(&gw, request(Method::DELETE, "", &with)).await.0,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        send(&gw, request(Method::POST, list, &with)).await.0,
        StatusCode::NOT_FOUND
    );
}

#[tokio::test]
async fn an_mcp_servers_error_is_written_as_a_json_rpc_error() {
    let init = r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}"#;
    // A child that exits unanswered, and one that cannot start.
    for (program, args) in [
        ("sh", vec!["-c", "read line"]),
        ("/no/such/mcp-server", vec![]),
    ] {
        let gw = gateway(StdioTransport::new(program, args).connect(), None);
        let (status, headers, body, _) = send(&gw, request(Method::POST, init, &[])).await;
        assert_eq!(status, StatusCode::BAD_GATEWAY, "{program}");
        assert_eq!(headers["content-type"], "application/json");
        let error: Value = serde_json::from_str(&body).unwrap_or_default();
        assert_eq!(
            (&error["id"], &error["error"]["code"]),
            (&json!(1), &json!(-32603)),
            "{body}"
        );
    }
    // A server's own error body goes as it came.
    let own = r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"boom"}}"#;
    let mock = ScriptedMcp::new();
    mock.reply(json_reply(StatusCode::INTERNAL_SERVER_ERROR, own));
    let gw = gateway(mock.clone().connect(), None);
    let (status, _, body, _) = send(&gw, request(Method::POST, init, &[])).await;
    assert_eq!(
        (status, &body[..]),
        (StatusCode::INTERNAL_SERVER_ERROR, own)
    );
}

type RefusalEntry = (String, bool, bool, String, Option<Duration>);

#[derive(Clone, Default)]
struct RefusalProbe(Arc<Mutex<Vec<RefusalEntry>>>);

#[async_trait]
impl Plugin for RefusalProbe {
    async fn on_provider_end(
        &self,
        px: &respan_gateway_sdk::ProviderContext,
        end: &Result<(), Fault>,
    ) {
        self.0.lock().unwrap_or_else(PoisonError::into_inner).push((
            px.provider().to_owned(),
            px.is_sent(),
            px.is_refused(),
            end.as_ref().err().map_or("ok", Fault::code).to_owned(),
            end.as_ref().err().and_then(Fault::retry_after),
        ));
    }
}

struct Fallback;

#[async_trait]
impl Plugin for Fallback {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let mut first = cx.call("first").await?;
        if first.until_end().await.is_err() {
            return cx.call("second").await.map(Some);
        }
        Ok(Some(first))
    }
}

#[tokio::test]
async fn a_non_success_event_stream_is_refused_with_retry_after_and_can_fall_back() {
    let failed = ScriptedMcp::new();
    let event = "data: {\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{}}\n\n";
    let mut refusal = sse(&[event]);
    *refusal.status_mut() = StatusCode::TOO_MANY_REQUESTS;
    refusal
        .headers_mut()
        .insert("retry-after", http::HeaderValue::from_static("7"));
    failed.reply(refusal);
    let probe = RefusalProbe::default();
    let gw = gateway(failed.clone().connect(), None).plugin(probe.clone());
    let (status, headers, body, outcome) = send(&gw, request(Method::POST, INIT, &[])).await;
    gw.idle().await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS, "{body}");
    assert_eq!(headers["retry-after"], "7");
    assert_eq!(body, event);
    assert_eq!(
        outcome.expect_err("provider refusal").code(),
        codes::UPSTREAM
    );
    assert_eq!(failed.sent().len(), 1);
    assert_eq!(
        *probe.0.lock().unwrap_or_else(PoisonError::into_inner),
        [(
            "mcp".to_owned(),
            true,
            true,
            codes::UPSTREAM.to_owned(),
            Some(Duration::from_secs(7))
        )]
    );

    let first = ScriptedMcp::new();
    let second = ScriptedMcp::new();
    let mut refusal = sse(&[event]);
    *refusal.status_mut() = StatusCode::TOO_MANY_REQUESTS;
    refusal
        .headers_mut()
        .insert("retry-after", http::HeaderValue::from_static("7"));
    first.reply(refusal);
    second.reply(json_reply(
        StatusCode::OK,
        r#"{"jsonrpc":"2.0","id":1,"result":{}}"#,
    ));
    let probe = RefusalProbe::default();
    let gw = Gateway::new()
        .client::<ToServer, _, _>(StreamableHttp::default())
        .expect("client")
        .provider::<ToServer, _, _>("first", StreamableHttp::default(), first.clone().connect())
        .expect("first")
        .provider::<ToServer, _, _>(
            "second",
            StreamableHttp::default(),
            second.clone().connect(),
        )
        .expect("second")
        .plugin(Fallback)
        .plugin(probe.clone());
    let (status, _, body, outcome) = send(&gw, request(Method::POST, INIT, &[])).await;
    gw.idle().await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        messages(&body),
        [json!({"jsonrpc":"2.0","id":1,"result":{}})]
    );
    assert!(outcome.is_ok());
    assert_eq!((first.sent().len(), second.sent().len()), (1, 1));
    let events = probe
        .0
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .clone();
    assert!(
        events.contains(&(
            "first".to_owned(),
            true,
            true,
            codes::UPSTREAM.to_owned(),
            Some(Duration::from_secs(7))
        )),
        "{events:?}"
    );
    assert!(
        events.contains(&("second".to_owned(), true, false, "ok".to_owned(), None)),
        "{events:?}"
    );
}

/// A server whose `GET` stream never says anything.
struct Quiet;

impl Quiet {
    fn connect(self) -> impl Fn(Extensions) -> Outgoing + Send + Sync {
        respan_gateway_http::connect(|_call| async {
            let resp = http::Response::builder()
                .header("content-type", "text/event-stream")
                .body(Body::stream(stream::pending()))
                .map_err(respan_gateway_http::built_wrong)?;
            Ok((resp, None))
        })
    }
}

#[tokio::test]
async fn a_quiet_get_stream_opens_at_once() {
    let gw = gateway(Quiet.connect(), None);
    let get = serve(&gw, request(Method::GET, "", &[(SESSION, "s1")]));
    let (resp, _) = tokio::time::timeout(Duration::from_secs(2), get)
        .await
        .unwrap_or_else(|_| panic!("the head waited"));
    assert_eq!(resp.status(), StatusCode::OK);
    let first = resp.into_body().frame().await;
    let first = first.and_then(|f| f.ok()).and_then(|f| f.into_data().ok());
    assert_eq!(first.as_deref(), Some(&b": stream\n\n"[..]));
}

const INIT: &str = r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}"#;
const LIST: &str = r#"{"jsonrpc":"2.0","id":2,"method":"tools/list"}"#;

/// A new session's id, started with `headers`.
async fn init(gw: &Gateway, headers: &[(&str, &str)]) -> String {
    let (status, headers, body, _) = send(gw, request(Method::POST, INIT, headers)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    headers[SESSION].to_str().unwrap_or_default().to_owned()
}

/// The status of a `POST` of `tools/list` in `session`.
async fn list(gw: &Gateway, session: &str) -> StatusCode {
    let (status, ..) = send(gw, request(Method::POST, LIST, &[(SESSION, session)])).await;
    status
}

#[tokio::test]
async fn a_child_that_exits_is_reaped_and_its_session_forgotten() {
    let script = r#"read l; echo '{"jsonrpc":"2.0","id":1,"result":{}}'; exit 0"#;
    let gw = gateway(StdioTransport::new("sh", ["-c", script]).connect(), None);
    let session = init(&gw, &[]).await;
    // The session is forgotten only once its child is waited: no zombie.
    for _ in 0..50 {
        if list(&gw, &session).await == StatusCode::NOT_FOUND {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("the session of an exited child is still served");
}

#[tokio::test]
async fn sessions_are_capped() {
    let gw = gateway(
        StdioTransport::new("sh", ["-c", ECHO])
            .max_sessions(2)
            .connect(),
        None,
    );
    let first = init(&gw, &[]).await;
    init(&gw, &[]).await;
    let (status, ..) = send(&gw, request(Method::POST, INIT, &[])).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    drop(send(&gw, request(Method::DELETE, "", &[(SESSION, &first)])).await);
    init(&gw, &[]).await;
}

#[tokio::test]
async fn an_idle_session_ends() {
    let transport = StdioTransport::new("sh", ["-c", ECHO]).max_sessions(1);
    let gw = gateway(transport.idle(Duration::from_secs(60)).connect(), None);
    let session = init(&gw, &[]).await;
    assert_eq!(list(&gw, &session).await, StatusCode::OK);
    let (status, ..) = send(&gw, request(Method::POST, INIT, &[])).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    // Paused only now: a paused clock would jump past a waiting POST's
    // timeout.
    tokio::time::pause();
    tokio::time::sleep(Duration::from_secs(600)).await;
    // Its child is killed and reaped in real time; a new session, which
    // does not touch the old one, fits once it is gone.
    tokio::time::resume();
    for _ in 0..50 {
        if send(&gw, request(Method::POST, INIT, &[])).await.0 == StatusCode::OK {
            return assert_eq!(list(&gw, &session).await, StatusCode::NOT_FOUND);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("an idle session is still served");
}

#[tokio::test]
async fn a_notification_reaches_the_get_stream_when_the_posts_client_left() {
    let script = r#"read a; echo '{"jsonrpc":"2.0","id":1,"result":{}}'; read b; sleep 0.3
echo '{"jsonrpc":"2.0","method":"notifications/message"}'; sleep 3"#;
    let gw = gateway(StdioTransport::new("sh", ["-c", script]).connect(), None);
    let session = init(&gw, &[]).await;
    let with = [(SESSION, &session[..])];
    let (listen, _) = serve(&gw, request(Method::GET, "", &with)).await;
    let call = r#"{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"slow"}}"#;
    // The client leaves before the server says anything.
    let posted = serve(&gw, request(Method::POST, call, &with));
    assert!(
        tokio::time::timeout(Duration::from_millis(100), posted)
            .await
            .is_err()
    );
    let mut body = listen.into_body();
    let mut seen = String::new();
    let heard = tokio::time::timeout(Duration::from_secs(3), async {
        while let Some(Ok(frame)) = body.frame().await {
            let data = frame.into_data().unwrap_or_default();
            seen.push_str(&String::from_utf8_lossy(&data));
            if seen.contains("notifications/message") {
                return;
            }
        }
    });
    assert!(heard.await.is_ok(), "the GET stream got {seen:?}");
}

#[tokio::test]
async fn a_session_belongs_to_the_credential_that_started_it() {
    let gw = gateway(StdioTransport::new("sh", ["-c", ECHO]).connect(), None);
    let session = init(&gw, &[("authorization", "Bearer a")]).await;
    let (a, b) = (
        [(SESSION, &session[..]), ("authorization", "Bearer a")],
        [(SESSION, &session[..]), ("authorization", "Bearer b")],
    );
    for method in [Method::POST, Method::GET, Method::DELETE] {
        let (status, ..) = send(&gw, request(method, LIST, &b)).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }
    assert_eq!(list(&gw, &session).await, StatusCode::NOT_FOUND);
    let (status, ..) = send(&gw, request(Method::POST, LIST, &a)).await;
    assert_eq!(status, StatusCode::OK);
    // The session's stream is not taken over by a second GET.
    let (first, _) = serve(&gw, request(Method::GET, "", &a)).await;
    assert_eq!(first.status(), StatusCode::OK);
    let (second, _) = serve(&gw, request(Method::GET, "", &a)).await;
    assert_eq!(second.status(), StatusCode::CONFLICT);
}

#[tokio::test]
async fn the_session_cap_is_per_owner() {
    let gw = gateway(
        StdioTransport::new("sh", ["-c", ECHO])
            .max_sessions(3)
            .connect(),
        None,
    );
    for _ in 0..3 {
        init(&gw, &[("authorization", "Bearer mallory")]).await;
    }
    let mallory = [("authorization", "Bearer mallory")];
    let (status, ..) = send(&gw, request(Method::POST, INIT, &mallory)).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    init(&gw, &[("authorization", "Bearer alice")]).await;
}

#[tokio::test]
async fn a_flood_from_many_owners_hits_the_total() {
    let transport = StdioTransport::new("sh", ["-c", ECHO]).max_sessions(2);
    let gw = gateway(transport.max_total(4).connect(), None);
    for i in 0..4 {
        init(&gw, &[("authorization", &format!("Bearer {i}"))]).await;
    }
    let fresh = [("authorization", "Bearer 4")];
    let (status, ..) = send(&gw, request(Method::POST, INIT, &fresh)).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn an_answer_to_no_open_request_ends_the_oldest_post() {
    // The server answers `tools/list` with a parse error, id null.
    let script = r#"read a; echo '{"jsonrpc":"2.0","id":1,"result":{}}'; read b
echo '{"jsonrpc":"2.0","id":null,"error":{"code":-32700,"message":"parse"}}'; exec sleep 100"#;
    let gw = gateway(StdioTransport::new("sh", ["-c", script]).connect(), None);
    let session = init(&gw, &[]).await;
    let posted = send(&gw, request(Method::POST, LIST, &[(SESSION, &session)]));
    let (status, _, body, outcome) = tokio::time::timeout(Duration::from_secs(3), posted)
        .await
        .unwrap_or_else(|_| panic!("the POST is still waiting"));
    let ids: Vec<_> = messages(&body).iter().map(|m| m["id"].clone()).collect();
    assert_eq!((status, ids), (StatusCode::OK, vec![Value::Null, json!(2)]));
    assert!(outcome.is_err());
}

/// Moves the caller's key into an [`Owner`], as an auth plugin would.
struct Authenticates;

#[async_trait]
impl Plugin for Authenticates {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let credential = cx.extensions_mut().remove::<Credential>();
        let key = credential.and_then(|c| c.get("authorization").cloned());
        let key = key.and_then(|k| k.to_str().ok().map(String::from));
        cx.extensions_mut().insert(Owner(key.unwrap_or_default()));
        Ok(None)
    }
}

#[tokio::test]
async fn a_session_belongs_to_the_owner_a_plugin_sets() -> Result<(), Fault> {
    let gw = Gateway::new()
        .provider::<ToServer, _, _>(
            "mcp",
            StreamableHttp::default(),
            StdioTransport::new("sh", ["-c", ECHO]).connect(),
        )?
        .client::<ToServer, _, _>(StreamableHttp::default())?
        .plugin(Authenticates)
        .plugin(relay::Relay::RAW)
        .target("mcp")?;
    let session = init(&gw, &[("authorization", "Bearer alice")]).await;
    for (key, status) in [
        ("Bearer bob", StatusCode::NOT_FOUND),
        ("Bearer alice", StatusCode::OK),
    ] {
        let headers = [(SESSION, &session[..]), ("authorization", key)];
        let (got, ..) = send(&gw, request(Method::POST, LIST, &headers)).await;
        assert_eq!(got, status, "{key}");
    }
    Ok(())
}

#[tokio::test]
async fn a_get_stream_that_falls_behind_ends() {
    let script = r#"read a; echo '{"jsonrpc":"2.0","id":1,"result":{}}'; read b
for i in $(seq 500); do echo '{"jsonrpc":"2.0","method":"notifications/message"}'; done; exec sleep 100"#;
    let gw = gateway(StdioTransport::new("sh", ["-c", script]).connect(), None);
    let session = init(&gw, &[]).await;
    let with = [(SESSION, &session[..])];
    let (listen, _) = serve(&gw, request(Method::GET, "", &with)).await;
    let note = r#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#;
    drop(send(&gw, request(Method::POST, note, &with)).await);
    // The client reads nothing until the server has said everything.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let body = tokio::time::timeout(Duration::from_secs(3), BodyExt::collect(listen.into_body()))
        .await
        .unwrap_or_else(|_| panic!("the stream is still open"));
    let body =
        String::from_utf8_lossy(&body.map(|b| b.to_bytes()).unwrap_or_default()).into_owned();
    assert!(messages(&body).len() < 500, "{}", messages(&body).len());
}

#[tokio::test]
async fn a_late_answer_to_a_post_the_client_left_reaches_no_other_post() {
    let script = r#"read a; echo '{"jsonrpc":"2.0","id":1,"result":{}}'
read b; read c; sleep 0.5
echo '{"jsonrpc":"2.0","id":2,"result":{"slow":true}}'; sleep 0.5
echo '{"jsonrpc":"2.0","id":3,"result":{"fast":true}}'; exec sleep 100"#;
    let gw = gateway(StdioTransport::new("sh", ["-c", script]).connect(), None);
    let session = init(&gw, &[]).await;
    let with = [(SESSION, &session[..])];
    let slow = r#"{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"slow"}}"#;
    // The client gives up on id 2; the server answers it anyway, later.
    let posted = serve(&gw, request(Method::POST, slow, &with));
    let gave_up = tokio::time::timeout(Duration::from_millis(100), posted).await;
    assert!(gave_up.is_err());
    let other = r#"{"jsonrpc":"2.0","id":3,"method":"tools/list"}"#;
    let posted = send(&gw, request(Method::POST, other, &with));
    let (_, _, body, outcome) = tokio::time::timeout(Duration::from_secs(5), posted)
        .await
        .unwrap_or_else(|_| panic!("the POST is still waiting"));
    let fast = json!({"jsonrpc":"2.0","id":3,"result":{"fast":true}});
    assert_eq!(messages(&body), [fast], "{body}");
    assert!(outcome.is_ok());
}

#[tokio::test]
async fn a_post_dropped_while_written_leaves_whole_lines() {
    // Busy for a second after `initialize`, then answers each whole line;
    // a line with two messages glued is a parse error.
    let script = r#"read a; echo '{"jsonrpc":"2.0","id":1,"result":{}}'; sleep 1
sed -u -n -e '/"jsonrpc".*"jsonrpc"/{s/.*/{"jsonrpc":"2.0","id":null,"error":{"code":-32700,"message":"parse"}}/p;d;}' \
  -e 's/.*"id":\([0-9]*\).*/{"jsonrpc":"2.0","id":\1,"result":{"ok":true}}/p'"#;
    let gw = gateway(StdioTransport::new("sh", ["-c", script]).connect(), None);
    let session = init(&gw, &[]).await;
    let with = [(SESSION, &session[..])];
    // Far past a pipe's buffer, so writing it waits on the busy server.
    let big = json!({"jsonrpc":"2.0","id":3,"method":"tools/call",
        "params":{"name":"echo","arguments":{"text":"x".repeat(1 << 20)}}});
    let posted = serve(&gw, request(Method::POST, &big.to_string(), &with));
    let gave_up = tokio::time::timeout(Duration::from_millis(200), posted).await;
    assert!(
        gave_up.is_err(),
        "the POST was answered before the client left"
    );
    let posted = send(&gw, request(Method::POST, LIST, &with));
    let (_, _, body, _) = tokio::time::timeout(Duration::from_secs(5), posted)
        .await
        .unwrap_or_else(|_| panic!("the POST is still waiting"));
    let ok = json!({"jsonrpc":"2.0","id":2,"result":{"ok":true}});
    assert_eq!(messages(&body), [ok], "{body}");
}

#[tokio::test]
async fn a_post_behind_a_child_that_stopped_reading_still_times_out() {
    // Answers `initialize`, then never reads stdin again.
    let script = r#"read a; echo '{"jsonrpc":"2.0","id":1,"result":{}}'; exec sleep 100"#;
    let transport = StdioTransport::new("sh", ["-c", script]);
    let gw = gateway(
        transport.timeout(Duration::from_millis(100)).connect(),
        None,
    );
    let session = init(&gw, &[]).await;
    // Fill the pipe and the queue, each POST left once its time is up.
    let big = json!({"jsonrpc":"2.0","id":2,"method":"tools/call",
        "params":{"name":"echo","arguments":{"text":"x".repeat(1 << 20)}}});
    let with = [(SESSION, &session[..])];
    let posted = send(&gw, request(Method::POST, &big.to_string(), &with));
    let _ = tokio::time::timeout(Duration::from_secs(3), posted).await;
    // More than the write queue holds.
    for id in 3..75 {
        let ask = json!({"jsonrpc":"2.0","id":id,"method":"tools/list"}).to_string();
        let posted = send(&gw, request(Method::POST, &ask, &with));
        let (status, ..) = tokio::time::timeout(Duration::from_secs(3), posted)
            .await
            .unwrap_or_else(|_| panic!("POST {id} waited past its timeout"));
        assert_eq!(status, StatusCode::GATEWAY_TIMEOUT, "POST {id}");
    }
}

#[tokio::test]
async fn a_post_the_server_never_answers_times_out_and_frees_its_session() {
    // Answers `initialize` only.
    let script = r#"read a; echo '{"jsonrpc":"2.0","id":1,"result":{}}'; exec sleep 100"#;
    let transport = StdioTransport::new("sh", ["-c", script]).max_sessions(1);
    let transport = transport.idle(Duration::from_secs(60));
    let gw = gateway(transport.timeout(Duration::from_secs(30)).connect(), None);
    let session = init(&gw, &[]).await;
    tokio::time::pause();
    let (status, _, body, outcome) =
        send(&gw, request(Method::POST, LIST, &[(SESSION, &session)])).await;
    let [error] = &messages(&body)[..] else {
        panic!("{body}")
    };
    assert_eq!(
        (status, &error["id"]),
        (StatusCode::GATEWAY_TIMEOUT, &json!(2))
    );
    assert_eq!(error["error"]["data"]["code"], TIMED_OUT);
    assert_eq!(
        outcome.err().map(|f| f.code().to_owned()),
        Some(TIMED_OUT.into())
    );
    // With nothing waiting, the session idles out and a new one fits.
    tokio::time::sleep(Duration::from_secs(600)).await;
    tokio::time::resume();
    for _ in 0..50 {
        if send(&gw, request(Method::POST, INIT, &[])).await.0 == StatusCode::OK {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("the session of a timed-out POST is still held");
}

#[tokio::test]
async fn a_session_whose_initialize_is_never_answered_idles_out() {
    // Never answers, never exits.
    let transport = StdioTransport::new("sh", ["-c", "exec sleep 100"])
        .timeout(Duration::from_millis(100))
        .idle(Duration::from_millis(200))
        .max_total(1);
    let gw = gateway(transport.connect(), None);
    let (first, ..) = send(&gw, request(Method::POST, INIT, &[])).await;
    assert_eq!(first, StatusCode::GATEWAY_TIMEOUT);
    // Its slot frees once it idles out, for any owner.
    tokio::time::sleep(Duration::from_secs(1)).await;
    let other = [("authorization", "Bearer someone-else")];
    let (second, _, body, _) = send(&gw, request(Method::POST, INIT, &other)).await;
    assert_ne!(
        second,
        StatusCode::SERVICE_UNAVAILABLE,
        "never reaped: {body}"
    );
}

// MCP is matched however its path is spelled, so what is served after it
// never takes a call that reaches it.
#[test]
fn mcp_is_matched_however_its_path_is_spelled() {
    let mcp = StreamableHttp::default();
    let head = |method: Method, uri: &str| {
        let req = http::Request::builder()
            .method(method)
            .uri(uri)
            .body(Body::full(""));
        Incoming::accept(req.expect("valid request"), false).0
    };
    let same = [
        "/mcp",
        "/mcp/",
        "/mc%70",
        "/mcp%2F",
        "//mcp",
        "/./mcp",
        "/x/../mcp",
        "/mcp/.",
    ];
    for path in same {
        assert!(mcp.matches(&head(Method::POST, path)), "{path}");
    }
    for path in ["/mcpx", "/x/mcp", "/mcp/x"] {
        assert!(!mcp.matches(&head(Method::POST, path)), "{path}");
    }
    assert!(!mcp.matches(&head(Method::PUT, "/mcp")));
}

/// Response correlation follows rewritten bytes, while client failures retain
/// the caller's original JSON-RPC ID.
#[tokio::test]
async fn rewritten_ids_correlate_with_sent_request_but_refuse_original_request() {
    struct Rewrite;
    #[async_trait]
    impl Plugin for Rewrite {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            cx.filter(|input: &mut ToServer| {
                input.messages[0]["id"] = json!(9);
                Ok(())
            })?;
            Ok(None)
        }
    }
    for answered in [true, false] {
        let mock = ScriptedMcp::new();
        let answer = if answered {
            r#"{"jsonrpc":"2.0","id":9,"result":{}}"#
        } else {
            ""
        };
        mock.reply(json_reply(StatusCode::OK, answer));
        let gw = typed_gateway(mock.clone().connect(), None).plugin(Rewrite);
        let (status, _, body, ended) = send(&gw, request(Method::POST, INIT, &[])).await;
        let sent: Value = serde_json::from_slice(mock.sent()[0].body()).expect("sent JSON");
        assert_eq!(sent["id"], 9);
        let output = messages(&body);
        assert_eq!(output.len(), 1, "{body}");
        if answered {
            assert_eq!(status, StatusCode::OK);
            assert_eq!(output[0]["id"], 9);
            assert!(ended.is_ok(), "correlation must not wait for original ID 1");
        } else {
            assert_eq!(status, StatusCode::BAD_GATEWAY);
            assert_eq!(output[0]["id"], 1, "refusal addresses original caller");
            assert_eq!(ended.expect_err("unanswered request").code(), "truncated");
        }
    }
}
