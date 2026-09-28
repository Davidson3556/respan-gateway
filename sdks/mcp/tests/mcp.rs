//! The promises of the MCP kind, format and stdio transport.

use std::time::Duration;

use respan_gateway_core::http::{self, HeaderMap, Method, StatusCode};
use respan_gateway_core::http_body_util::{self, BodyExt, Full};
use respan_gateway_core::serde_json::{Value, json};
use respan_gateway_core::{Body, ClientContext, Fault, Gateway, Outcome, Plugin, Response};
use respan_gateway_core::{Transport, async_trait, codes};
use respan_gateway_mcp::{SESSION, StdioTransport, StreamableHttp, ToClient, ToServer};
use respan_gateway_testkit::{MockTransport, mock};

const AT: &str = "http://mcp.test";

/// An MCP server in `sh`: answers each request with an empty result.
const ECHO: &str = r#"while IFS= read -r line; do
  id=$(printf '%s' "$line" | sed -n 's/.*"id":\([0-9]*\).*/\1/p')
  [ -n "$id" ] && printf '{"jsonrpc":"2.0","id":%s,"result":{}}\n' "$id"
done"#;

fn gateway(transport: impl Transport, plugin: Option<Touch>) -> Gateway {
    let gw = Gateway::new().provider("mcp", StreamableHttp::default(), transport);
    let gw = gw.and_then(|g| g.serve(StreamableHttp::default()));
    let gw = match plugin {
        Some(p) => gw.and_then(|g| g.plugin("touch", p)),
        None => gw,
    };
    gw.and_then(|g| g.target("mcp"))
        .unwrap_or_else(|e| panic!("{e}"))
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
    let body = Full::from(body.to_owned())
        .map_err(Fault::from)
        .boxed_unsync();
    req.body(body).unwrap_or_else(|e| panic!("{e}"))
}

async fn send(gw: &Gateway, req: http::Request<Body>) -> (StatusCode, HeaderMap, String, Outcome) {
    let (resp, outcome) = gw.handle(req).await;
    let (head, body) = resp.into_parts();
    let body = body.collect().await.map(|b| b.to_bytes());
    let body = body.unwrap_or_else(|f| panic!("aborted: {f}"));
    let text = String::from_utf8_lossy(&body).into_owned();
    (head.status, head.headers, text, outcome.await)
}

/// The messages of a JSON body or of each event.
fn messages(body: &str) -> Vec<Value> {
    let each = body.split_inclusive("\n\n");
    let all = each
        .filter_map(respan_gateway_formats::json)
        .flat_map(|v| match v {
            Value::Array(all) => all,
            one => vec![one],
        });
    all.collect()
}

fn bare(status: StatusCode) -> mock::Reply {
    let mut reply = http::Response::new(Vec::new());
    *reply.status_mut() = status;
    reply
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
        let mock = MockTransport::new();
        mock.reply(AT, mock::sse(&events));
        mock.reply(AT, mock::json(StatusCode::OK, json));
        let gw = gateway(mock.at(AT), plugin.map(|edit| Touch { edit }));
        let posted = || request(Method::POST, body, &[(SESSION, "s1")]);
        let (status, headers, sse, outcome) = send(&gw, posted()).await;
        assert_eq!((status, &sse[..]), (StatusCode::OK, &events.concat()[..]));
        assert_eq!(headers["content-type"], "text/event-stream");
        assert!(outcome.end.is_ok());
        let (_, _, whole, _) = send(&gw, posted()).await;
        assert_eq!(whole, json);
        let sent = &mock.sent()[0];
        assert_eq!(
            (sent.uri().to_string(), &sent.body()[..]),
            (format!("{AT}/mcp?x=1"), body.as_bytes())
        );
        assert_eq!(sent.headers()[SESSION], "s1");
    }
    let mock = MockTransport::new();
    mock.reply(AT, mock::sse(&events));
    let gw = gateway(mock.at(AT), Some(Touch { edit: true }));
    let (_, _, sse, _) = send(&gw, request(Method::POST, body, &[])).await;
    let edited = r#"id: 7
data: {"jsonrpc":"2.0","id":1,"result":{"edited":true}}

"#;
    assert_eq!(sse, format!(": hi\n\n{edited}"));
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
    let mock = MockTransport::new();
    mock.reply(AT, mock::sse(&[&answer(2)]));
    mock.reply(AT, mock::sse(&[&answer(2), note, &answer(1)]));
    let gw = gateway(mock.at(AT), None);

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
        outcome.end.map_err(|f| f.code().to_owned()),
        Err(codes::TRUNCATED.to_owned())
    );

    let (_, _, body, outcome) = send(&gw, request(Method::POST, batch, &[])).await;
    assert_eq!(messages(&body).len(), 3);
    assert!(outcome.end.is_ok());
}

#[tokio::test]
async fn delete_and_202_are_handled() {
    let note = r#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#;
    for plugin in [None, Some(false)] {
        let mock = MockTransport::new();
        mock.reply(AT, bare(StatusCode::NO_CONTENT));
        mock.reply(AT, bare(StatusCode::ACCEPTED));
        let gw = gateway(mock.at(AT), plugin.map(|edit| Touch { edit }));

        let (status, _, body, outcome) =
            send(&gw, request(Method::DELETE, "", &[(SESSION, "s1")])).await;
        assert_eq!((status, &body[..]), (StatusCode::NO_CONTENT, ""));
        assert!(outcome.end.is_ok());
        let (status, headers, body, outcome) =
            send(&gw, request(Method::POST, note, &[(SESSION, "s1")])).await;
        assert_eq!((status, &body[..]), (StatusCode::ACCEPTED, ""));
        assert!(!headers.contains_key("content-type"));
        assert!(outcome.end.is_ok());

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
    let gw = gateway(StdioTransport::new("sh", ["-c", ECHO]), None);
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
    assert!(outcome.end.is_ok());
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
        let gw = gateway(StdioTransport::new(program, args), None);
        let (status, headers, body, _) = send(&gw, request(Method::POST, init, &[])).await;
        assert_eq!(status, StatusCode::BAD_GATEWAY, "{program}");
        assert_eq!(headers["content-type"], "application/json");
        let error: Value = respan_gateway_core::serde_json::from_str(&body).unwrap_or_default();
        assert_eq!(
            (&error["id"], &error["error"]["code"]),
            (&json!(1), &json!(-32603)),
            "{body}"
        );
    }
    // A server's own error body goes as it came.
    let own = r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"boom"}}"#;
    let mock = MockTransport::new();
    mock.reply(AT, mock::json(StatusCode::INTERNAL_SERVER_ERROR, own));
    let gw = gateway(mock.at(AT), None);
    let (status, _, body, _) = send(&gw, request(Method::POST, init, &[])).await;
    assert_eq!(
        (status, &body[..]),
        (StatusCode::INTERNAL_SERVER_ERROR, own)
    );
}

/// A server whose `GET` stream never says anything.
struct Quiet;

#[async_trait]
impl Transport for Quiet {
    async fn send(&self, _call: http::Request<Body>) -> Result<http::Response<Body>, Fault> {
        let never = respan_gateway_core::futures::stream::pending();
        let body = http_body_util::StreamBody::new(never).boxed_unsync();
        let resp = http::Response::builder().header("content-type", "text/event-stream");
        resp.body(body)
            .map_err(|e| Fault::internal("a head").with_source(e))
    }
}

#[tokio::test]
async fn a_quiet_get_stream_opens_at_once() {
    let gw = gateway(Quiet, None);
    let get = gw.handle(request(Method::GET, "", &[(SESSION, "s1")]));
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
    let gw = gateway(StdioTransport::new("sh", ["-c", script]), None);
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
        StdioTransport::new("sh", ["-c", ECHO]).max_sessions(2),
        None,
    );
    let first = init(&gw, &[]).await;
    init(&gw, &[]).await;
    let (status, ..) = send(&gw, request(Method::POST, INIT, &[])).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    send(&gw, request(Method::DELETE, "", &[(SESSION, &first)])).await;
    init(&gw, &[]).await;
}

#[tokio::test(start_paused = true)]
async fn an_idle_session_ends() {
    let transport = StdioTransport::new("sh", ["-c", ECHO]).max_sessions(1);
    let gw = gateway(transport.idle(Duration::from_secs(60)), None);
    let session = init(&gw, &[]).await;
    assert_eq!(list(&gw, &session).await, StatusCode::OK);
    let (status, ..) = send(&gw, request(Method::POST, INIT, &[])).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
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
    let gw = gateway(StdioTransport::new("sh", ["-c", script]), None);
    let session = init(&gw, &[]).await;
    let with = [(SESSION, &session[..])];
    let (listen, _) = gw.handle(request(Method::GET, "", &with)).await;
    let call = r#"{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"slow"}}"#;
    // The client leaves before the server says anything.
    let posted = gw.handle(request(Method::POST, call, &with));
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
    let gw = gateway(StdioTransport::new("sh", ["-c", ECHO]), None);
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
    let (first, _) = gw.handle(request(Method::GET, "", &a)).await;
    assert_eq!(first.status(), StatusCode::OK);
    let (second, _) = gw.handle(request(Method::GET, "", &a)).await;
    assert_eq!(second.status(), StatusCode::CONFLICT);
}
