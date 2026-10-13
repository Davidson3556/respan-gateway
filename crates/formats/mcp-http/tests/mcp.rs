//! The promises of the MCP kind, format and stdio transport.

#[allow(dead_code)]
#[path = "../../../../tests/support/http.rs"]
mod http_support;
use http_support::{LIMIT, serve};
use respan_gateway_core::FaultKind;

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use respan_gateway_core::futures::{StreamExt, stream};
use respan_gateway_core::{
    ClientContext, Extensions, Fault, Format, Gateway, Plugin, Provider, Response,
};
use respan_gateway_core::{RetryAfter, async_trait};
use respan_gateway_http::bytes::Bytes;
use respan_gateway_http::http::header::CONTENT_TYPE;
use respan_gateway_http::http::{self, HeaderMap, Method, StatusCode};
use respan_gateway_http::http_body_util::BodyExt;
use respan_gateway_http::{
    Body, ClientIo, Completion, Credential, ProviderIo, call::credentials,
    call::strip_origin_headers,
};
use respan_gateway_mcp::{Input, Output};
#[cfg(unix)]
use respan_gateway_mcp_http::StdioTransport;
use respan_gateway_mcp_http::{Owner, SESSION, StreamableHttp};
use serde_json::{Value, json};

use respan_gateway_plugin_relay::Relay;

const AT: &str = "http://mcp.test";

/// An MCP server in `sh`: answers each request with an empty result.
#[cfg(unix)]
const ECHO: &str = r#"while IFS= read -r line; do
  id=$(printf '%s' "$line" | sed -n 's/.*"id":\([0-9]*\).*/\1/p')
  [ -n "$id" ] && printf '{"jsonrpc":"2.0","id":%s,"result":{}}\n' "$id"
done"#;

/// A gateway to an MCP provider: typed, with no relay.
fn gateway(
    transport: impl Fn(Extensions) -> ProviderIo + Send + Sync + 'static,
    plugin: Option<Touch>,
) -> Gateway {
    let gw = Gateway::new().provider(Provider::new("mcp", StreamableHttp::default(), transport));
    let gw = gw.and_then(|g| g.client::<Input, _, _>(StreamableHttp::default()));
    let gw = match plugin {
        Some(p) => gw.map(|g| g.plugin(p)),
        None => gw,
    };
    gw.and_then(|g| g.target("mcp"))
        .unwrap_or_else(|e| panic!("{e}"))
}

/// [`gateway`], relaying the client's request, head and errors as they
/// came.
fn relayed(
    transport: impl Fn(Extensions) -> ProviderIo + Send + Sync + 'static,
    plugin: Option<Touch>,
) -> Gateway {
    gateway(transport, plugin).plugin(Relay)
}

/// Filters every input and output, changing them only if `edit`.
struct Touch {
    edit: bool,
}

#[async_trait]
impl Plugin for Touch {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        cx.filter(|_: &mut Input| Ok(()))?;
        let edit = self.edit;
        cx.filter(move |out: &mut Output| {
            if edit
                && let Some(m) = out.messages.first_mut()
                && let Some(result) = m.result_mut()
            {
                *result = json!({ "edited": true });
            }
            Ok(())
        })?;
        Ok(None)
    }
}

fn request(method: Method, body: &str, headers: &[(&str, &str)]) -> http::Request<Body> {
    request_at("/mcp?x=1", method, body, headers)
}

fn request_at(
    uri: &str,
    method: Method,
    body: &str,
    headers: &[(&str, &str)],
) -> http::Request<Body> {
    let mut req = http::Request::builder().method(method).uri(uri);
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
        .filter_map(respan_gateway_http::content::json)
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
/// bytes to assert what was sent. This is local to the MCP scenarios.
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
        let mut request = call;
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
    fn connect(self) -> impl Fn(Extensions) -> ProviderIo + Send + Sync {
        respan_gateway_http::connect(
            move |call| {
                let value = self.clone();
                async move { value.send(call).await }
            },
            LIMIT,
        )
    }
}

/// The JSON of each `data:` line of an event stream.
fn events_json(sse: &str) -> Vec<serde_json::Value> {
    let data = sse.lines().filter_map(|l| l.strip_prefix("data: "));
    data.filter_map(|d| serde_json::from_str(d).ok()).collect()
}

#[tokio::test]
async fn an_answer_is_written_from_its_messages_typed_or_relayed() {
    let body = r#"{ "jsonrpc":"2.0",  "id":1, "method":"tools/list" }"#;
    let events = [
        ": hi\n\nid: 7\n",
        "data: {\"jsonrpc\":\"2.0\", \"id\":1,\"result\":{}}\n\n",
    ];
    let json = r#"{"jsonrpc":"2.0" , "id":1,"result":{}}"#;
    let result = serde_json::json!({ "jsonrpc": "2.0", "id": 1, "result": {} });
    let parse = |b: &[u8]| serde_json::from_slice::<serde_json::Value>(b).ok();
    for raw in [false, true] {
        for plugin in [None, Some(false)] {
            let mock = ScriptedMcp::new();
            mock.reply(sse(&events));
            mock.reply(json_reply(StatusCode::OK, json));
            let plugin = plugin.map(|edit| Touch { edit });
            let gw = match raw {
                true => relayed(mock.clone().connect(), plugin),
                false => gateway(mock.clone().connect(), plugin),
            };
            let posted = |accept| {
                let with = [(SESSION, "s1"), ("accept", accept)];
                request(Method::POST, body, &with)
            };
            let (status, headers, sse, outcome) = send(&gw, posted("*/*")).await;
            // The event is written from the message: its `id:` is kept, the
            // provider's comment and its spacing are not.
            assert_eq!(status, StatusCode::OK);
            assert!(sse.starts_with("id: 7\ndata: {"), "{sse}");
            assert_eq!(events_json(&sse), std::slice::from_ref(&result));
            assert_eq!(headers["content-type"], "text/event-stream");
            assert!(outcome.is_ok());
            let (_, headers, whole, _) = send(&gw, posted("application/json")).await;
            assert_eq!(headers["content-type"], "application/json");
            assert_eq!(parse(whole.as_bytes()), Some(result.clone()));
            let sent = &mock.sent()[0];
            // A relay sends the client's path and query; a typed call, the
            // format's path and the kind's query.
            assert_eq!(sent.uri().to_string(), format!("{AT}/mcp?x=1"));
            assert_eq!(parse(sent.body()), parse(body.as_bytes()));
            assert_eq!(sent.headers()[SESSION], "s1");
        }
    }
    let mock = ScriptedMcp::new();
    mock.reply(sse(&events));
    let gw = gateway(mock.clone().connect(), Some(Touch { edit: true }));
    let (_, _, sse, _) = send(&gw, request(Method::POST, body, &[])).await;
    let edited = serde_json::json!({ "jsonrpc": "2.0", "id": 1, "result": { "edited": true } });
    assert_eq!(events_json(&sse), [edited]);
    assert!(sse.starts_with("id: 7\ndata: {"), "{sse}");
}

/// Moves a client's session to the provider's name for it, and back.
struct Rename;

#[async_trait]
impl Plugin for Rename {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        cx.filter(|input: &mut Input| {
            if input.session.as_deref() == Some("public") {
                input.session = Some("private".into());
            }
            Ok(())
        })?;
        cx.filter(|output: &mut Output| {
            if output.session.as_deref() == Some("private") {
                output.session = Some("public".into());
            }
            Ok(())
        })?;
        Ok(None)
    }
}

#[tokio::test]
async fn the_session_version_and_last_event_travel_in_the_kind_through_filters() {
    let mock = ScriptedMcp::new();
    mock.reply(sse(&[]));
    let mut assigned = json_reply(StatusCode::OK, r#"{"jsonrpc":"2.0","id":1,"result":{}}"#);
    let private = http::HeaderValue::from_static("private");
    assigned.headers_mut().insert(SESSION, private);
    mock.reply(assigned);
    let gw = gateway(mock.clone().connect(), None).plugin(Rename);
    let resumed = [
        (SESSION, "public"),
        ("mcp-protocol-version", "2025-06-18"),
        ("last-event-id", "41"),
    ];
    drop(send(&gw, request(Method::GET, "", &resumed)).await);
    let (status, headers, _, outcome) = send(&gw, request(Method::POST, INIT, &[])).await;
    assert_eq!(status, StatusCode::OK);
    assert!(outcome.is_ok());
    // The session the server named, renamed by the filter, heads the answer.
    assert_eq!(headers[SESSION], "public");
    let sent = mock.sent();
    let get = sent[0].headers();
    for (name, value) in [
        (SESSION, "private"),
        ("mcp-protocol-version", "2025-06-18"),
        ("last-event-id", "41"),
    ] {
        assert_eq!(get[name], value, "{name}");
    }
    assert!(!sent[1].headers().contains_key(SESSION));
}

#[tokio::test]
async fn a_unicode_resume_cursor_returns_in_last_event_id() {
    let mock = ScriptedMcp::new();
    mock.reply(sse(&[]));
    let gw = gateway(mock.clone().connect(), None);
    let cursor = "resume-雪😀";
    let (status, _, _, outcome) =
        send(&gw, request(Method::GET, "", &[("last-event-id", cursor)])).await;
    assert_eq!(status, StatusCode::OK);
    assert!(outcome.is_ok());
    assert_eq!(
        mock.sent()[0].headers()["last-event-id"].as_bytes(),
        cursor.as_bytes()
    );
}

#[tokio::test]
async fn invalid_or_ambiguous_session_headers_are_refused_before_forwarding() {
    let mock = ScriptedMcp::new();
    let gw = gateway(mock.clone().connect(), None);
    for session in ["", " ", "has space", "雪", "tab\tid"] {
        let (status, _, _, outcome) =
            send(&gw, request(Method::POST, LIST, &[(SESSION, session)])).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{session:?}");
        assert!(outcome.is_err());
    }
    for name in [SESSION, "mcp-protocol-version", "last-event-id"] {
        let mut req = request(Method::POST, LIST, &[(name, "one")]);
        req.headers_mut()
            .append(name, http::HeaderValue::from_static("two"));
        let (status, _, _, outcome) = send(&gw, req).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{name}");
        assert!(outcome.is_err());
    }
    assert!(mock.sent().is_empty());
}

#[tokio::test]
async fn an_invalid_provider_session_is_an_upstream_fault() {
    for session in ["", " ", "has space", "雪", "tab\tid", "duplicate"] {
        let mock = ScriptedMcp::new();
        let mut reply = json_reply(StatusCode::OK, r#"{"jsonrpc":"2.0","id":1,"result":{}}"#);
        reply
            .headers_mut()
            .insert(SESSION, session.parse().expect("HTTP header"));
        if session == "duplicate" {
            reply
                .headers_mut()
                .append(SESSION, http::HeaderValue::from_static("other"));
        }
        mock.reply(reply);
        let gw = gateway(mock.connect(), None);
        let (status, headers, _, outcome) = send(&gw, request(Method::POST, INIT, &[])).await;
        assert_eq!(status, StatusCode::BAD_GATEWAY, "{session:?}");
        assert!(!headers.contains_key(SESSION));
        assert!(outcome.is_err_and(|f| f.is(FaultKind::Provider)));
    }
}

#[tokio::test]
async fn sse_id_resets_and_ignored_nul_values_preserve_valid_messages() {
    for (fields, expected) in [
        ("id: old\nid\n", "id: \n"),
        ("id: old\nid: bad\0cursor\n", "id: old\n"),
        ("event: custom\0name\n", "event: custom\0name\n"),
    ] {
        let mock = ScriptedMcp::new();
        let wire = format!(
            "{fields}data: {{\"jsonrpc\":\"2.0\",\"method\":\"notifications/message\"}}\n\n"
        );
        mock.reply(sse(&[&wire]));
        let gw = gateway(mock.connect(), None);
        let (_, _, body, outcome) = send(&gw, request(Method::GET, "", &[])).await;
        assert!(outcome.is_ok(), "{body}");
        assert!(body.contains(expected), "{body:?}");
        assert_eq!(messages(&body).len(), 1);
    }
}

/// Reads the ids of events and sets one.
struct Resume(Arc<Mutex<Vec<Option<String>>>>);

#[async_trait]
impl Plugin for Resume {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let seen = self.0.clone();
        cx.filter(move |out: &mut Output| {
            seen.lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(out.event_id.clone());
            if out.event_id.as_deref() == Some("2") {
                out.event_id = Some("2b".into());
            }
            Ok(())
        })?;
        Ok(None)
    }
}

#[tokio::test]
async fn an_sse_event_id_and_name_round_trip_and_a_plugin_may_edit_them() {
    let events = [
        "id: 1\ndata\n\n",
        "id: 2\nevent: message\ndata: {\"jsonrpc\":\"2.0\",\"method\":\"n\"}\n\n",
        "data: {\"jsonrpc\":\"2.0\",\"method\":\"m\"}\n\n",
    ];
    let mock = ScriptedMcp::new();
    mock.reply(sse(&events));
    let seen = Arc::new(Mutex::new(Vec::new()));
    let gw = gateway(mock.connect(), None).plugin(Resume(seen.clone()));
    let (_, headers, body, _) = send(&gw, request(Method::GET, "", &[])).await;
    assert_eq!(headers[CONTENT_TYPE], "text/event-stream");
    // The priming event, a named one with its id edited, and one with none.
    let want = concat!(
        ": stream\n\n",
        "id: 1\ndata: \n\n",
        "id: 2b\nevent: message\ndata: {\"jsonrpc\":\"2.0\",\"method\":\"n\"}\n\n",
        "data: {\"jsonrpc\":\"2.0\",\"method\":\"m\"}\n\n",
    );
    assert_eq!(body, want);
    let ids = seen.lock().unwrap_or_else(PoisonError::into_inner).clone();
    assert_eq!(ids, [Some("1".into()), Some("2".into()), None]);
}

/// Edits an event id into one that would add lines to the stream.
struct Inject;

#[async_trait]
impl Plugin for Inject {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        cx.filter(|out: &mut Output| {
            if out.event_id.is_some() {
                out.event_id = Some("2\ndata: injected".into());
            }
            Ok(())
        })?;
        Ok(None)
    }
}

#[tokio::test]
async fn an_event_id_that_would_break_its_line_is_refused_not_written() {
    let mock = ScriptedMcp::new();
    mock.reply(sse(&[
        "id: 2\ndata: {\"jsonrpc\":\"2.0\",\"method\":\"n\"}\n\n",
    ]));
    let gw = gateway(mock.connect(), None).plugin(Inject);
    let (_, _, body, outcome) = send(&gw, request(Method::GET, "", &[])).await;
    assert!(!body.contains("injected"), "{body}");
    assert!(outcome.is_err_and(|f| f.code() == "internal"));
}

/// A plugin-created response has no provider response head; the format must
/// choose the client's framing rather than any wire the value came in.
struct Rebuild;

#[async_trait]
impl Plugin for Rebuild {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let outputs = cx
            .call("mcp")
            .await?
            .outputs::<Output>()
            .collect::<Vec<_>>()
            .await;
        Ok(Some(Response::stream(stream::iter(outputs))))
    }
}

#[tokio::test]
async fn a_posted_answer_is_framed_as_the_clients_accept_asks() {
    let event = "id: 7\ndata: {\"jsonrpc\":\"2.0\", \"id\":1,\"result\":{}}\n\n";
    for (accept, framing) in [
        (None, "text/event-stream"),
        (
            Some("application/json, text/event-stream"),
            "text/event-stream",
        ),
        (Some("text/*;q=0.5"), "text/event-stream"),
        (Some("application/json"), "application/json"),
        (Some("text/event-stream;q=0, */*"), "application/json"),
    ] {
        let mock = ScriptedMcp::new();
        mock.reply(sse(&[event]));
        let gw = gateway(mock.connect(), None).plugin(Rebuild);
        let headers: Vec<_> = accept.map(|a| ("accept", a)).into_iter().collect();
        let (status, headers, body, outcome) =
            send(&gw, request(Method::POST, INIT, &headers)).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(headers[CONTENT_TYPE], framing, "{accept:?}");
        assert_eq!(
            messages(&body),
            [json!({"jsonrpc":"2.0","id":1,"result":{}})]
        );
        assert_eq!(body.contains("data:"), framing == "text/event-stream");
        assert!(outcome.is_ok());
    }
}

#[tokio::test]
async fn a_rebuilt_json_answer_is_framed_for_get() {
    let mock = ScriptedMcp::new();
    mock.reply(json_reply(
        StatusCode::OK,
        r#"{"jsonrpc":"2.0", "method":"notifications/message"}"#,
    ));
    let gw = gateway(mock.connect(), None).plugin(Rebuild);
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
        (&json!(1), &json!("truncated"))
    );
    assert_eq!(
        outcome.map_err(|f| (f.code().to_owned(), f.code() == "truncated")),
        Err(("truncated".to_owned(), true))
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
        // A server's 200 to a `DELETE`: the client's answer is still 204.
        mock.reply(bare(StatusCode::OK));
        mock.reply(bare(StatusCode::ACCEPTED));
        let gw = gateway(mock.clone().connect(), plugin.map(|edit| Touch { edit }));

        let (status, headers, body, outcome) =
            send(&gw, request(Method::DELETE, "", &[(SESSION, "s1")])).await;
        assert_eq!((status, &body[..]), (StatusCode::NO_CONTENT, ""));
        assert!(!headers.contains_key("content-type"));
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
async fn a_servers_refusal_of_a_notification_or_delete_reaches_the_client() {
    let note = r#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#;
    let mock = ScriptedMcp::new();
    // The session expired on the server.
    mock.reply(bare(StatusCode::NOT_FOUND));
    mock.reply(bare(StatusCode::NOT_FOUND));
    let gw = gateway(mock.connect(), None);
    let gone = [(SESSION, "gone")];
    for asked in [
        request(Method::POST, note, &gone),
        request(Method::DELETE, "", &gone),
    ] {
        let (status, _, body, outcome) = send(&gw, asked).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
        assert!(outcome.is_err());
    }
}

#[cfg(unix)]
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

#[cfg(unix)]
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
    // A server's own error body goes as it came if relayed; else its
    // status goes with the client format's error, and its code.
    let own = r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"boom"}}"#;
    let mock = ScriptedMcp::new();
    mock.reply(json_reply(StatusCode::INTERNAL_SERVER_ERROR, own));
    mock.reply(json_reply(StatusCode::INTERNAL_SERVER_ERROR, own));
    let (status, _, body, _) = send(
        &relayed(mock.clone().connect(), None),
        request(Method::POST, init, &[]),
    )
    .await;
    assert_eq!(
        (status, &body[..]),
        (StatusCode::INTERNAL_SERVER_ERROR, own)
    );
    let (status, _, body, _) = send(
        &gateway(mock.connect(), None),
        request(Method::POST, init, &[]),
    )
    .await;
    let error: Value = serde_json::from_str(&body).unwrap_or_default();
    assert_eq!(
        (status, &error["id"], &error["error"]["code"]),
        (StatusCode::INTERNAL_SERVER_ERROR, &json!(1), &json!(-32000))
    );
}

type RefusalEntry = (String, bool, bool, String, Option<Duration>);

#[derive(Clone, Default)]
struct RefusalProbe(Arc<Mutex<Vec<RefusalEntry>>>);

#[async_trait]
impl Plugin for RefusalProbe {
    async fn on_provider_end(
        &self,
        px: &respan_gateway_core::ProviderContext,
        end: &Result<(), Fault>,
    ) {
        self.0.lock().unwrap_or_else(PoisonError::into_inner).push((
            px.provider().to_owned(),
            px.is_sent(),
            px.is_refused(),
            end.as_ref()
                .err()
                .map_or("ok".into(), |f| f.code().to_owned()),
            end.as_ref()
                .err()
                .and_then(|f| f.get::<RetryAfter>())
                .map(|r| r.0),
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
    let error: Value = serde_json::from_str(&body).unwrap_or_default();
    assert_eq!(
        (&error["id"], &error["error"]["code"]),
        (&json!(1), &json!(-32603))
    );
    assert!(
        outcome
            .expect_err("provider refusal")
            .is(FaultKind::Provider)
    );
    assert_eq!(failed.sent().len(), 1);
    assert_eq!(
        *probe.0.lock().unwrap_or_else(PoisonError::into_inner),
        [(
            "mcp".to_owned(),
            true,
            true,
            "upstream".to_owned(),
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
        .client::<Input, _, _>(StreamableHttp::default())
        .expect("client")
        .provider(Provider::new(
            "first",
            StreamableHttp::default(),
            first.clone().connect(),
        ))
        .expect("first")
        .provider(Provider::new(
            "second",
            StreamableHttp::default(),
            second.clone().connect(),
        ))
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
            "upstream".to_owned(),
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
    fn connect(self) -> impl Fn(Extensions) -> ProviderIo + Send + Sync {
        respan_gateway_http::connect(
            |_call| async {
                let resp = http::Response::builder()
                    .header("content-type", "text/event-stream")
                    .body(Body::stream(stream::pending()))
                    .map_err(respan_gateway_http_faults::built_wrong)?;
                Ok((resp, None))
            },
            LIMIT,
        )
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
#[cfg_attr(not(unix), allow(dead_code))]
async fn init(gw: &Gateway, headers: &[(&str, &str)]) -> String {
    let (status, headers, body, _) = send(gw, request(Method::POST, INIT, headers)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    headers[SESSION].to_str().unwrap_or_default().to_owned()
}

/// The status of a `POST` of `tools/list` in `session`.
#[cfg_attr(not(unix), allow(dead_code))]
async fn list(gw: &Gateway, session: &str) -> StatusCode {
    let (status, ..) = send(gw, request(Method::POST, LIST, &[(SESSION, session)])).await;
    status
}

#[cfg(unix)]
#[tokio::test]
async fn a_child_that_exits_is_reaped_and_its_session_forgotten() {
    // What it leaves behind holds its stdout open.
    let script = r#"sleep 100 & read l; echo '{"jsonrpc":"2.0","id":1,"result":{}}'; exit 0"#;
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

#[cfg(unix)]
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

#[cfg(unix)]
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

#[cfg(unix)]
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

#[cfg(unix)]
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

#[cfg(unix)]
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

#[cfg(unix)]
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

#[cfg(unix)]
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
#[cfg_attr(not(unix), allow(dead_code))]
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

#[cfg(unix)]
#[tokio::test]
async fn a_session_belongs_to_the_owner_a_plugin_sets() -> Result<(), Fault> {
    let gw = Gateway::new()
        .provider(Provider::new(
            "mcp",
            StreamableHttp::default(),
            StdioTransport::new("sh", ["-c", ECHO]).connect(),
        ))?
        .client::<Input, _, _>(StreamableHttp::default())?
        .plugin(Authenticates)
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

#[cfg(unix)]
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

#[cfg(unix)]
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

#[cfg(unix)]
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

#[cfg(unix)]
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

#[cfg(unix)]
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
    assert_eq!(
        (&error["error"]["code"], &error["error"]["data"]["code"]),
        (&json!(-32001), &json!("timed_out"))
    );
    assert_eq!(
        outcome.err().map(|f| f.code().to_owned()),
        Some("timed_out".into())
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

#[cfg(unix)]
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

/// Whether the process `pid` is still running.
#[cfg_attr(not(unix), allow(dead_code))]
fn running(pid: &Value) -> bool {
    let probe = std::process::Command::new("sh")
        .args(["-c", &format!("kill -0 {pid} 2>/dev/null")])
        .status();
    probe.is_ok_and(|s| s.success())
}

#[cfg(unix)]
#[tokio::test]
async fn a_child_deaf_to_eof_and_sigterm_is_killed_with_all_it_started() {
    // Its background process, like itself, ignores SIGTERM.
    let script = r#"trap '' TERM; sleep 100 & read a
echo '{"jsonrpc":"2.0","id":1,"result":{"pid":'$!'}}'; exec sleep 100"#;
    let gw = gateway(StdioTransport::new("sh", ["-c", script]).connect(), None);
    let (_, headers, body, _) = send(&gw, request(Method::POST, INIT, &[])).await;
    let pid = messages(&body)[0]["result"]["pid"].clone();
    assert!(running(&pid), "{body}");
    let with = [(SESSION, headers[SESSION].to_str().unwrap_or_default())];
    drop(send(&gw, request(Method::DELETE, "", &with)).await);
    for _ in 0..80 {
        if !running(&pid) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("what the child started outlived its session");
}

#[cfg(unix)]
#[tokio::test]
async fn a_child_sees_only_path_and_what_it_is_given() {
    assert!(std::env::var_os("CARGO_MANIFEST_DIR").is_some());
    let script = r#"read a; printf '{"jsonrpc":"2.0","id":1,"result":{"seen":"%s %s %s %s"}}\n' \
  "${CARGO_MANIFEST_DIR-unset}" "${GIVEN-unset}" "$(pwd)" "$(command -v sed)""#;
    let transport = StdioTransport::new("sh", ["-c", script]).env("GIVEN", "yes");
    let gw = gateway(transport.current_dir("/").connect(), None);
    let (_, _, body, _) = send(&gw, request(Method::POST, INIT, &[])).await;
    let seen = messages(&body)[0]["result"]["seen"].clone();
    let seen = seen.as_str().unwrap_or_default().to_owned();
    assert!(seen.starts_with("unset yes / /"), "{seen}");
}

#[cfg(unix)]
#[tokio::test]
async fn an_initialize_whose_client_left_frees_its_slot() {
    let script = r#"read a; sleep 1; echo '{"jsonrpc":"2.0","id":1,"result":{}}'; exec sleep 100"#;
    let transport = StdioTransport::new("sh", ["-c", script]).max_total(1);
    let gw = gateway(transport.connect(), None);
    let posted = serve(&gw, request(Method::POST, INIT, &[]));
    let gave_up = tokio::time::timeout(Duration::from_millis(100), posted).await;
    assert!(gave_up.is_err(), "answered before the client left");
    init(&gw, &[]).await;
}

#[cfg(unix)]
#[tokio::test]
async fn a_request_the_client_cancels_ends_its_post() {
    // Answers `initialize` only.
    let script = r#"read a; echo '{"jsonrpc":"2.0","id":1,"result":{}}'; exec sleep 100"#;
    let gw = gateway(StdioTransport::new("sh", ["-c", script]).connect(), None);
    let session = init(&gw, &[]).await;
    let with = [(SESSION, &session[..])];
    let call = r#"{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"slow"}}"#;
    let cancel = r#"{"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":2}}"#;
    let cancelled = async {
        tokio::time::sleep(Duration::from_millis(200)).await;
        send(&gw, request(Method::POST, cancel, &with)).await.0
    };
    let both = async { tokio::join!(send(&gw, request(Method::POST, call, &with)), cancelled) };
    let ((_, _, body, outcome), cancelled) = tokio::time::timeout(Duration::from_secs(3), both)
        .await
        .unwrap_or_else(|_| panic!("the cancelled POST is still waiting"));
    assert_eq!(cancelled, StatusCode::ACCEPTED);
    assert_eq!(messages(&body)[0]["id"], json!(2), "{body}");
    assert!(outcome.is_err());
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
        ClientIo::accept(req.expect("valid request"), LIMIT).0
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
async fn a_post_a_plugin_emptied_is_a_configuration_fault_not_an_empty_batch() {
    struct Empty;
    #[async_trait]
    impl Plugin for Empty {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            cx.filter(|input: &mut Input| {
                input.messages.clear();
                Ok(())
            })?;
            Ok(None)
        }
    }
    let mock = ScriptedMcp::new();
    let gw = gateway(mock.clone().connect(), None).plugin(Empty);
    let (status, ..) = send(&gw, request(Method::POST, INIT, &[])).await;
    assert!(status.is_server_error(), "{status}");
    assert!(mock.sent().is_empty());
}

#[test]
fn a_new_input_is_a_batch_only_when_it_is_set_to_be() {
    let one = respan_gateway_mcp::Message::try_from(json!({ "jsonrpc": "2.0", "method": "ping" }));
    let one = one.expect("a notification");
    assert!(!Input::new(Method::POST, vec![one.clone(), one]).batch);
    assert!(!Input::new(Method::POST, Vec::new()).batch);
}

#[tokio::test]
async fn rewritten_ids_correlate_with_sent_request_but_refuse_original_request() {
    struct Rewrite;
    #[async_trait]
    impl Plugin for Rewrite {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            cx.filter(|input: &mut Input| {
                if let respan_gateway_mcp::Message::Request { id, .. } = &mut input.messages[0] {
                    *id = respan_gateway_mcp::Id::Number(9.into());
                }
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
        let gw = gateway(mock.clone().connect(), None).plugin(Rewrite);
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
            assert!(ended.expect_err("unanswered request").code() == "truncated");
        }
    }
}

#[tokio::test]
async fn a_failed_get_stream_writes_no_json_rpc_answer() {
    let mock = ScriptedMcp::new();
    let mut broken = sse(&["data: {\"jsonrpc\":\"2.0\",\"method\":\"n\"}\n\n"]);
    broken.body_mut().push(Err(Fault::upstream("cut")));
    mock.reply(broken);
    let gw = gateway(mock.connect(), None);
    let (status, _, body, outcome) = send(&gw, request(Method::GET, "", &[])).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        messages(&body),
        [json!({"jsonrpc":"2.0","method":"n"})],
        "{body}"
    );
    assert!(outcome.is_err_and(|f| f.is(FaultKind::Provider)));
}

#[tokio::test]
async fn a_json_answer_that_fails_midway_keeps_its_answers_and_errs_the_rest() {
    let batch = r#"[{"jsonrpc":"2.0","id":1,"method":"a"},{"jsonrpc":"2.0","id":2,"method":"b"}]"#;
    let mock = ScriptedMcp::new();
    mock.reply(sse(&[
        "data: {\"jsonrpc\":\"2.0\",\"id\":2,\"result\":{}}\n\n",
    ]));
    let gw = gateway(mock.connect(), None);
    let json = [("accept", "application/json")];
    let (status, headers, body, outcome) = send(&gw, request(Method::POST, batch, &json)).await;
    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert_eq!(headers[CONTENT_TYPE], "application/json");
    let all: Value = serde_json::from_str(&body).unwrap_or_default();
    assert_eq!(
        all[0],
        json!({"jsonrpc":"2.0","id":2,"result":{}}),
        "{body}"
    );
    assert_eq!(
        (&all[1]["id"], &all[1]["error"]["data"]["code"], &all[2]),
        (&json!(1), &json!("truncated"), &Value::Null),
        "{body}"
    );
    assert!(outcome.is_err_and(|f| f.code() == "truncated"));
}

#[tokio::test]
async fn a_body_that_is_no_json_is_a_parse_error_and_no_message_an_invalid_request() {
    let gw = gateway(ScriptedMcp::new().connect(), None);
    for (body, code) in [("{", -32700), ("data: {}", -32700), ("[1]", -32600)] {
        let (status, _, answer, _) = send(&gw, request(Method::POST, body, &[])).await;
        let error: Value = serde_json::from_str(&answer).unwrap_or_default();
        assert_eq!(
            (status, &error["id"], &error["error"]["code"]),
            (StatusCode::BAD_REQUEST, &Value::Null, &json!(code)),
            "{body}"
        );
    }
}

#[tokio::test]
async fn invalid_rpc_envelopes_are_refused_before_contacting_the_server() {
    let mock = ScriptedMcp::new();
    let gw = gateway(mock.clone().connect(), None);
    for body in [
        "{}",
        "[]",
        r#"{"jsonrpc":"1.0","id":1,"method":"tools/list"}"#,
        r#"{"jsonrpc":"2.0","id":{},"method":"tools/list"}"#,
        r#"{"jsonrpc":"2.0","id":null,"method":"tools/list"}"#,
        r#"{"jsonrpc":"2.0","id":1.5,"method":"tools/list"}"#,
        r#"{"jsonrpc":"2.0","id":1,"method":false}"#,
        r#"{"jsonrpc":"2.0","id":1,"method":"tools/list","params":true}"#,
        r#"{"jsonrpc":"2.0","id":1}"#,
        r#"{"jsonrpc":"2.0","id":1,"result":{},"error":{"code":-1,"message":"bad"}}"#,
        r#"[{"jsonrpc":"2.0","id":1,"method":"a"},{"jsonrpc":"2.0","id":1,"method":"b"}]"#,
    ] {
        let (status, _, answer, outcome) = send(&gw, request(Method::POST, body, &[])).await;
        let answer: Value = serde_json::from_str(&answer).expect("error response");
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert_eq!(answer["error"]["code"], -32600, "{body}");
        assert!(outcome.is_err());
    }
    assert!(mock.sent().is_empty());
}

#[cfg(unix)]
#[tokio::test]
async fn a_zero_stdio_backlog_is_a_configuration_fault() {
    let gw = gateway(
        StdioTransport::new("sh", ["-c", ECHO]).backlog(0).connect(),
        None,
    );
    let (status, _, _, outcome) = send(&gw, request(Method::POST, INIT, &[])).await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(outcome.expect_err("invalid backlog").code(), "config");
}

#[cfg(unix)]
#[tokio::test]
async fn overlapping_request_ids_are_refused_without_disrupting_the_first_post() {
    let script = r#"read a; echo '{"jsonrpc":"2.0","id":1,"result":{}}'; read b
echo '{"jsonrpc":"2.0","method":"notifications/message","params":{"waiting":true}}'
read c; echo '{"jsonrpc":"2.0","id":2,"result":{}}'
echo '{"jsonrpc":"2.0","id":3,"result":{}}'; exec sleep 100"#;
    let transport = StdioTransport::new("sh", ["-c", script]).timeout(Duration::from_secs(1));
    let gw = gateway(transport.connect(), None);
    let session = init(&gw, &[]).await;
    let (first, finished) = serve(&gw, request(Method::POST, LIST, &[(SESSION, &session)])).await;
    let (status, _, _, _) = send(&gw, request(Method::POST, LIST, &[(SESSION, &session)])).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let third = r#"{"jsonrpc":"2.0","id":3,"method":"tools/list"}"#;
    let (status, _, _, outcome) =
        send(&gw, request(Method::POST, third, &[(SESSION, &session)])).await;
    assert_eq!(status, StatusCode::OK);
    assert!(outcome.is_ok());
    let bytes = BodyExt::collect(first.into_body())
        .await
        .expect("first response")
        .to_bytes();
    assert_eq!(
        messages(std::str::from_utf8(&bytes).expect("UTF-8"))[1]["id"],
        2
    );
    assert!(finished.await.is_ok());
}

#[tokio::test]
async fn a_bodiless_success_cannot_settle_an_unanswered_request() {
    for status in [
        StatusCode::ACCEPTED,
        StatusCode::NO_CONTENT,
        StatusCode::RESET_CONTENT,
    ] {
        let mock = ScriptedMcp::new();
        mock.reply(bare(status));
        let gw = gateway(mock.connect(), None);
        let (status, _, body, outcome) = send(&gw, request(Method::POST, LIST, &[])).await;
        assert_eq!(status, StatusCode::BAD_GATEWAY, "{body}");
        let answer = messages(&body).pop().expect("unanswered request error");
        assert_eq!(answer["id"], 2);
        assert_eq!(answer["error"]["data"]["code"], "truncated");
        assert!(outcome.is_err_and(|f| f.code() == "truncated"));
    }
}

#[tokio::test]
async fn malformed_provider_messages_never_count_as_answers_in_json_or_sse() {
    for bad in [
        "not json",
        "null",
        "[]",
        "{}",
        r#"{"jsonrpc":"2.0","id":2}"#,
        r#"{"jsonrpc":"2.0","id":null,"result":{}}"#,
        r#"{"jsonrpc":"2.0","id":2.5,"result":{}}"#,
        r#"{"jsonrpc":"1.0","id":2,"result":{}}"#,
        r#"{"jsonrpc":"2.0","id":2,"error":{"code":"bad","message":"oops"}}"#,
        r#"{"jsonrpc":"2.0","id":2,"result":{},"error":{"code":-1,"message":"oops"}}"#,
    ] {
        for events in [false, true] {
            let mock = ScriptedMcp::new();
            mock.reply(if events {
                sse(&[&format!("data: {bad}\n\n")])
            } else {
                json_reply(StatusCode::OK, bad)
            });
            let gw = gateway(mock.connect(), None);
            let (status, _, body, outcome) = send(&gw, request(Method::POST, LIST, &[])).await;
            assert_eq!(status, StatusCode::BAD_GATEWAY, "{bad}: {body}");
            let answer = messages(&body).pop().expect("protocol error");
            assert_eq!(answer["id"], 2, "{bad}: {body}");
            assert_eq!(answer["error"]["data"]["code"], "upstream", "{bad}: {body}");
            assert!(outcome.is_err_and(|f| f.is(FaultKind::Provider)));
        }
    }
}

#[tokio::test]
async fn a_foreign_reply_cannot_turn_one_request_into_a_response_batch() {
    for events in [false, true] {
        let mock = ScriptedMcp::new();
        let foreign = r#"{"jsonrpc":"2.0","id":99,"result":{}}"#;
        let matching = r#"{"jsonrpc":"2.0","id":2,"result":{}}"#;
        mock.reply(if events {
            sse(&[
                &format!("data: {foreign}\n\n"),
                &format!("data: {matching}\n\n"),
            ])
        } else {
            json_reply(StatusCode::OK, &format!("[{foreign},{matching}]"))
        });
        let gw = gateway(mock.connect(), None);
        let (status, _, body, outcome) = send(
            &gw,
            request(Method::POST, LIST, &[("accept", "application/json")]),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_GATEWAY, "{body}");
        let reply: Value = serde_json::from_str(&body).expect("single refusal");
        assert_eq!(reply["id"], 2);
        assert_eq!(reply["error"]["data"]["code"], "upstream");
        assert!(outcome.is_err_and(|f| f.is(FaultKind::Provider)));
    }
}

#[tokio::test]
async fn a_post_finishes_after_its_last_reply_even_if_the_server_keeps_sse_open() {
    let connect = respan_gateway_http::connect(
        |_| async {
            let frame =
                Bytes::from_static(b"data: {\"jsonrpc\":\"2.0\",\"id\":2,\"result\":{}}\n\n");
            let frames = stream::once(async { Ok(frame) }).chain(stream::pending());
            let reply = http::Response::builder()
                .header(CONTENT_TYPE, "text/event-stream")
                .body(Body::stream(frames))
                .expect("SSE response");
            Ok((reply, None))
        },
        LIMIT,
    );
    let gw = gateway(connect, None);
    let (status, _, body, outcome) = tokio::time::timeout(
        Duration::from_secs(3),
        send(
            &gw,
            request(Method::POST, LIST, &[("accept", "application/json")]),
        ),
    )
    .await
    .expect("answered POST must finish without waiting for socket EOF");
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        serde_json::from_str::<Value>(&body).expect("single reply")["id"],
        2
    );
    assert!(outcome.is_ok());
    tokio::time::timeout(Duration::from_secs(3), gw.idle())
        .await
        .expect("gateway settled");
}

#[tokio::test]
async fn a_duplicate_reply_cannot_settle_a_different_pending_request() {
    let mock = ScriptedMcp::new();
    mock.reply(sse(&[
        "data: {\"jsonrpc\":\"2.0\",\"id\":2,\"result\":{}}\n\n",
        "data: {\"jsonrpc\":\"2.0\",\"id\":2,\"result\":{}}\n\n",
    ]));
    let gw = gateway(mock.connect(), None);
    let batch = r#"[{"jsonrpc":"2.0","id":1,"method":"a"},{"jsonrpc":"2.0","id":2,"method":"b"}]"#;
    let (status, _, body, outcome) = send(
        &gw,
        request(Method::POST, batch, &[("accept", "application/json")]),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_GATEWAY);
    let replies = messages(&body);
    assert_eq!(replies.len(), 2, "{body}");
    assert_eq!(replies[0]["id"], 2);
    assert_eq!(replies[1]["id"], 1);
    assert_eq!(replies[1]["error"]["data"]["code"], "upstream");
    assert!(outcome.is_err_and(|f| f.is(FaultKind::Provider)));
}

#[tokio::test]
async fn a_resumed_get_can_receive_replies_from_earlier_posts() {
    let mock = ScriptedMcp::new();
    mock.reply(sse(&[
        "id: old-2\ndata: {\"jsonrpc\":\"2.0\",\"id\":2,\"result\":{}}\n\n",
        "id: old-3\ndata: {\"jsonrpc\":\"2.0\",\"id\":3,\"result\":{}}\n\n",
    ]));
    let gw = gateway(mock.connect(), None);
    let (status, _, body, outcome) =
        send(&gw, request(Method::GET, "", &[("last-event-id", "old-1")])).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        messages(&body)
            .iter()
            .map(|m| m["id"].clone())
            .collect::<Vec<_>>(),
        [json!(2), json!(3)]
    );
    assert!(body.contains("id: old-2\n") && body.contains("id: old-3\n"));
    assert!(outcome.is_ok());
}

#[tokio::test]
async fn split_unicode_multiline_events_and_typed_ids_survive_out_of_order_answers() {
    let ask = json!([
        {"jsonrpc":"2.0","id":7,"method":"tools/call"},
        {"jsonrpc":"2.0","id":"7","method":"tools/call"}
    ])
    .to_string();
    let text = concat!(
        ": keepalive\r\n\r\n",
        "id: resume-α\r\nevent: message\r\n",
        "data: {\"jsonrpc\":\"2.0\",\"id\":\"7\",\r\n",
        "data: \"result\":{\"text\":\"雪😀\",\"future\":true}}\r\n\r\n",
        "data: {\"jsonrpc\":\"2.0\",\"method\":\"notifications/progress\",\"params\":{\"progressToken\":0,\"progress\":1}}\n\n",
        "data: {\"jsonrpc\":\"2.0\",\"id\":7,\"result\":null}\n\n"
    );
    for cut in 0..=text.len() {
        let mut reply = typed(StatusCode::OK, Some("text/event-stream"), &[]);
        *reply.body_mut() = [&text.as_bytes()[..cut], &text.as_bytes()[cut..]]
            .into_iter()
            .map(|b| Ok(Bytes::copy_from_slice(b)))
            .collect();
        let mock = ScriptedMcp::new();
        mock.reply(reply);
        let gw = gateway(mock.connect(), None);
        let (_, _, body, outcome) = send(&gw, request(Method::POST, &ask, &[])).await;
        assert!(outcome.is_ok(), "cut {cut}: {body}");
        let all = messages(&body);
        assert_eq!(all.len(), 3, "cut {cut}: {body}");
        assert_eq!(all[0]["id"], "7");
        assert_eq!(all[0]["result"]["text"], "雪😀");
        assert_eq!(all[2]["id"], 7);
        assert!(body.contains("id: resume-α\n"));
    }
}

#[cfg(unix)]
#[tokio::test]
async fn stray_json_on_stdio_cannot_complete_a_post() {
    let script = r#"read a; echo '{"jsonrpc":"2.0","id":1,"result":{}}'; read b
echo 'null'; echo '42'; echo '{"id":2}'; echo '{"jsonrpc":"2.0","id":2}'
echo '{"jsonrpc":"2.0","id":null,"result":{}}'
echo '{"jsonrpc":"2.0","error":{"code":-32700,"message":"not a stdio response"}}'
echo '{"jsonrpc":"2.0","id":2,"result":{"text":"valid"}}'; exec sleep 100"#;
    let gw = gateway(StdioTransport::new("sh", ["-c", script]).connect(), None);
    let session = init(&gw, &[]).await;
    let (_, _, body, outcome) =
        send(&gw, request(Method::POST, LIST, &[(SESSION, &session)])).await;
    assert!(outcome.is_ok(), "{body}");
    assert_eq!(
        messages(&body),
        [json!({"jsonrpc":"2.0","id":2,"result":{"text":"valid"}})]
    );
}

#[tokio::test]
async fn json_answers_do_not_become_batches_when_a_server_notifies_before_replying() {
    for events in [false, true] {
        let mock = ScriptedMcp::new();
        mock.reply(sse(&[
            "data: {\"jsonrpc\":\"2.0\",\"method\":\"notifications/tools/list_changed\"}\n\n",
            "data: {\"jsonrpc\":\"2.0\",\"id\":2,\"result\":{}}\n\n",
        ]));
        let gw = gateway(mock.connect(), None);
        let accept = if events {
            "text/event-stream"
        } else {
            "application/json"
        };
        let (_, _, body, outcome) =
            send(&gw, request(Method::POST, LIST, &[("accept", accept)])).await;
        assert!(outcome.is_ok(), "{body}");
        if events {
            assert_eq!(messages(&body).len(), 2);
            assert_eq!(
                messages(&body)[0]["method"],
                "notifications/tools/list_changed"
            );
        } else {
            assert_eq!(
                serde_json::from_str::<Value>(&body).expect("single response"),
                json!({"jsonrpc":"2.0","id":2,"result":{}})
            );
        }
    }
}

#[tokio::test]
async fn server_requests_are_refused_in_json_framing_and_preserved_in_sse() {
    for events in [false, true] {
        let mock = ScriptedMcp::new();
        mock.reply(sse(&[
            "data: {\"jsonrpc\":\"2.0\",\"id\":\"server\",\"method\":\"sampling/createMessage\",\"params\":{}}\n\n",
            "data: {\"jsonrpc\":\"2.0\",\"id\":2,\"result\":{}}\n\n",
        ]));
        let gw = gateway(mock.connect(), None);
        let accept = if events {
            "text/event-stream"
        } else {
            "application/json"
        };
        let (status, _, body, outcome) =
            send(&gw, request(Method::POST, LIST, &[("accept", accept)])).await;
        if events {
            assert_eq!(status, StatusCode::OK);
            assert!(outcome.is_ok());
            assert_eq!(messages(&body)[0]["id"], "server");
        } else {
            assert_eq!(status, StatusCode::BAD_GATEWAY);
            assert!(outcome.is_err_and(|f| f.is(FaultKind::Provider)));
            assert_eq!(messages(&body)[0]["id"], 2);
            assert_eq!(messages(&body)[0]["error"]["data"]["code"], "upstream");
        }
    }
}

#[cfg(unix)]
#[tokio::test]
async fn a_request_naming_no_session_but_initialize_is_a_400() {
    let gw = gateway(StdioTransport::new("sh", ["-c", ECHO]).connect(), None);
    for (method, body) in [
        (Method::POST, LIST),
        (Method::GET, ""),
        (Method::DELETE, ""),
    ] {
        let (status, _, answer, _) = send(&gw, request(method.clone(), body, &[])).await;
        let error = messages(&answer).pop().unwrap_or_default();
        assert_eq!(
            (
                status,
                &error["error"]["code"],
                &error["error"]["data"]["code"]
            ),
            (
                StatusCode::BAD_REQUEST,
                &json!(-32003),
                &json!("no_session")
            ),
            "{method}"
        );
    }
    let (status, _, answer, _) = send(&gw, request(Method::POST, LIST, &[(SESSION, "x")])).await;
    let error = messages(&answer).pop().unwrap_or_default();
    assert_eq!(
        (status, &error["id"], &error["error"]["code"]),
        (StatusCode::NOT_FOUND, &json!(2), &json!(-32003))
    );
}

#[cfg(unix)]
#[tokio::test]
async fn session_ids_are_128_random_bits() {
    let gw = gateway(StdioTransport::new("sh", ["-c", ECHO]).connect(), None);
    let (a, b) = (init(&gw, &[]).await, init(&gw, &[]).await);
    assert_ne!(a, b);
    for id in [a, b] {
        assert!(
            id.len() == 32 && id.bytes().all(|b| b.is_ascii_hexdigit()),
            "{id}"
        );
    }
}

#[cfg(unix)]
#[tokio::test]
async fn an_open_get_stream_keeps_its_session_from_idling() {
    let transport = StdioTransport::new("sh", ["-c", ECHO]).idle(Duration::from_millis(200));
    let gw = gateway(transport.connect(), None);
    let session = init(&gw, &[]).await;
    let (listen, _) = serve(&gw, request(Method::GET, "", &[(SESSION, &session)])).await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(list(&gw, &session).await, StatusCode::OK);
    drop(listen);
}

/// Opens `session`'s `GET` stream and returns what it says, until `until`
/// is in it or 3 s pass, while `then` runs.
#[cfg_attr(not(unix), allow(dead_code))]
async fn heard(gw: &Gateway, session: &str, until: &str, then: impl Future) -> String {
    let (listen, _) = serve(gw, request(Method::GET, "", &[(SESSION, session)])).await;
    then.await;
    let mut body = listen.into_body();
    let mut seen = String::new();
    let _ = tokio::time::timeout(Duration::from_secs(3), async {
        while let Some(Ok(frame)) = body.frame().await {
            seen.push_str(&String::from_utf8_lossy(
                &frame.into_data().unwrap_or_default(),
            ));
            if seen.contains(until) {
                return;
            }
        }
    })
    .await;
    seen
}

#[cfg(unix)]
#[tokio::test]
async fn progress_goes_to_its_post_and_the_servers_own_messages_to_the_get_stream() {
    let script = r#"read a; echo '{"jsonrpc":"2.0","id":1,"result":{}}'; read b
echo '{"jsonrpc":"2.0","method":"notifications/message","params":{"level":"info"}}'
echo '{"jsonrpc":"2.0","method":"notifications/progress","params":{"progressToken":"p","progress":1}}'
echo '{"jsonrpc":"2.0","id":2,"result":{}}'; exec sleep 100"#;
    let gw = gateway(StdioTransport::new("sh", ["-c", script]).connect(), None);
    let session = init(&gw, &[]).await;
    let call = r#"{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"_meta":{"progressToken":"p"}}}"#;
    let posted = std::sync::OnceLock::new();
    let seen = heard(&gw, &session, "notifications/message", async {
        let answer = send(&gw, request(Method::POST, call, &[(SESSION, &session)])).await;
        let _ = posted.set(answer.2);
    })
    .await;
    let posted = posted.get().cloned().unwrap_or_default();
    let methods: Vec<Value> = messages(&posted)
        .iter()
        .map(|m| m["method"].clone())
        .collect();
    assert_eq!(
        methods,
        [json!("notifications/progress"), Value::Null],
        "{posted}"
    );
    assert!(seen.contains("notifications/message"), "{seen}");
}

#[cfg(unix)]
#[tokio::test]
async fn a_timed_out_post_cancels_its_requests() {
    let script = r#"read a; echo '{"jsonrpc":"2.0","id":1,"result":{}}'; read b; read c
case "$c" in *'"method":"notifications/cancelled","params":{"requestId":2'*)
  echo '{"jsonrpc":"2.0","method":"notifications/message","params":{"saw":"cancelled"}}';; esac
exec sleep 100"#;
    let transport = StdioTransport::new("sh", ["-c", script]).timeout(Duration::from_millis(300));
    let gw = gateway(transport.connect(), None);
    let session = init(&gw, &[]).await;
    let seen = heard(&gw, &session, "saw", async {
        assert_eq!(list(&gw, &session).await, StatusCode::GATEWAY_TIMEOUT);
    })
    .await;
    assert!(seen.contains("\"saw\":\"cancelled\""), "{seen}");
}

/// Sets the query of every call.
struct Requery(Option<&'static str>);

#[async_trait]
impl Plugin for Requery {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let query = self.0;
        cx.filter(move |input: &mut Input| {
            input.query = query.map(str::to_owned);
            Ok(())
        })?;
        Ok(None)
    }
}

#[tokio::test]
async fn the_query_travels_in_the_kind_without_the_keys_the_adapter_lifted() {
    let result = r#"{"jsonrpc":"2.0","id":1,"result":{}}"#;
    let queries = |mock: &ScriptedMcp| -> Vec<Option<String>> {
        let uris = mock
            .sent()
            .into_iter()
            .map(|r| r.uri().query().map(str::to_owned));
        uris.collect()
    };
    let sends = |plugin: Option<Requery>, uri: &'static str| async move {
        let mock = ScriptedMcp::new();
        mock.reply(json_reply(StatusCode::OK, result));
        let gw = gateway(mock.clone().connect(), None);
        let gw = match plugin {
            Some(p) => gw.plugin(p),
            None => gw,
        };
        let (status, ..) = send(&gw, request_at(uri, Method::POST, INIT, &[])).await;
        assert_eq!(status, StatusCode::OK);
        (mock.sent()[0].uri().path().to_owned(), queries(&mock))
    };
    // Forwarded as it came, to the format's path.
    let (path, query) = sends(None, "/mcp?x=1&y=a%20b").await;
    assert_eq!(
        (&path[..], query),
        ("/mcp", vec![Some("x=1&y=a%20b".into())])
    );
    let (_, query) = sends(None, "/mcp").await;
    assert_eq!(query, [None]);
    // A plugin changes or removes it.
    let (_, query) = sends(Some(Requery(Some("z=2"))), "/mcp?x=1").await;
    assert_eq!(query, [Some("z=2".to_owned())]);
    let (_, query) = sends(Some(Requery(None)), "/mcp?x=1").await;
    assert_eq!(query, [None]);
    // A key parameter is lifted by the adapter, and not put back.
    let (_, query) = sends(None, "/mcp?x=1&api_key=sekret&token=t").await;
    assert_eq!(query, [Some("x=1".to_owned())]);
}

#[tokio::test]
async fn a_servers_json_rpc_refusal_keeps_its_code_message_and_status() {
    let refusals = [
        r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32602,"message":"bad params","data":{"field":"a"}}}"#,
        r#"{"jsonrpc":"2.0","error":{"code":-32602,"message":"bad params","data":{"field":"a"}}}"#,
        r#"[{"jsonrpc":"2.0","error":{"code":-32602,"message":"bad params","data":{"field":"a"}}}]"#,
        r#"[{"jsonrpc":"2.0","id":1,"error":{"code":-32602,"message":"bad params","data":{"field":"a"}}}]"#,
    ];
    for body in refusals {
        let mock = ScriptedMcp::new();
        mock.reply(json_reply(StatusCode::BAD_REQUEST, body));
        let gw = gateway(mock.connect(), None);
        let (status, _, text, _) = send(&gw, request(Method::POST, INIT, &[])).await;
        let error: Value = serde_json::from_str(&text).unwrap_or_default();
        assert_eq!(status, StatusCode::BAD_REQUEST, "{text}");
        assert_eq!(error["id"], 1);
        assert_eq!(error["error"]["code"], -32602, "{text}");
        assert_eq!(error["error"]["message"], "bad params");
        assert_eq!(error["error"]["data"]["data"], json!({ "field": "a" }));
        assert_eq!(error["error"]["data"]["code"], "upstream");
    }
    // A body that says no JSON-RPC error is an internal error, as ever.
    for body in [
        "upstream down",
        r#"{"error":"nope"}"#,
        r#"{"error":{"code":"x","message":"m"}}"#,
    ] {
        let mock = ScriptedMcp::new();
        mock.reply(typed(StatusCode::BAD_GATEWAY, Some("text/plain"), &[body]));
        let gw = gateway(mock.connect(), None);
        let (status, _, text, _) = send(&gw, request(Method::POST, INIT, &[])).await;
        let error: Value = serde_json::from_str(&text).unwrap_or_default();
        assert_eq!(status, StatusCode::BAD_GATEWAY, "{text}");
        assert_eq!(error["error"]["code"], -32603, "{text}");
        assert_eq!(error["error"]["message"], "the provider returned 502");
    }
}

#[tokio::test]
async fn buffered_and_dropped_typed_messages_keep_their_own_sse_cursors() {
    #[derive(Default)]
    struct Buffered(Vec<Output>);
    impl respan_gateway_core::Filter<Output> for Buffered {
        fn edit(&mut self, mut event: Output, _: &mut Vec<Output>) -> Result<(), Fault> {
            if event.event_id.as_deref() == Some("drop") {
                return Ok(());
            }
            for message in &mut event.messages {
                if let respan_gateway_mcp::Message::Notification { method, .. } = message {
                    method.push_str("/edited");
                }
            }
            self.0.push(event);
            Ok(())
        }
        fn end(&mut self, end: &Result<(), Fault>, out: &mut Vec<Output>) -> Result<(), Fault> {
            out.append(&mut self.0);
            end.clone()
        }
    }
    struct Buffer;
    #[async_trait]
    impl Plugin for Buffer {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            cx.filter(Buffered::default())?;
            Ok(None)
        }
    }
    let mock = ScriptedMcp::new();
    mock.reply(sse(&[
        "id: first\nevent: message\ndata: {\"jsonrpc\":\"2.0\",\"method\":\"first\"}\n\n",
        "id: drop\ndata: {\"jsonrpc\":\"2.0\",\"method\":\"drop\"}\n\n",
        "id: last\nevent: message\ndata: {\"jsonrpc\":\"2.0\",\"method\":\"last\"}\n\n",
    ]));
    let gw = gateway(mock.connect(), None).plugin(Buffer);
    let (_, _, body, _) = send(&gw, request(Method::GET, "", &[])).await;
    assert_eq!(
        body,
        concat!(
            ": stream\n\n",
            "id: first\nevent: message\ndata: {\"jsonrpc\":\"2.0\",\"method\":\"first/edited\"}\n\n",
            "id: last\nevent: message\ndata: {\"jsonrpc\":\"2.0\",\"method\":\"last/edited\"}\n\n",
        )
    );
}
