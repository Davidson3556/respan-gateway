//! HTTP's rules at the gateway's edges, which left the kernel (decision
//! 0012): what the adapter ([`respan_gateway_http::serve`]) strips and lifts
//! on arrival and on the way out, and what a transport (`MockAt`, which
//! mirrors `HttpTransport`) strips from a call and passes through. Moved
//! from the testkit's `relay.rs`, bodies unchanged but where marked.

use std::sync::Arc;

use respan_gateway_core::http::{self, HeaderMap, HeaderValue, StatusCode};
use respan_gateway_core::{
    Body, ClientContext, Fault, Format, Gateway, Plugin, ProviderContext, Response, Transport,
    async_trait, bytes, futures, http_body_util,
};
use respan_gateway_formats::OpenAiChat;
use respan_gateway_formats::conversation::{Conversation, Reply};
use respan_gateway_http::{Asked, CallHeaders, Credential};
use respan_gateway_testkit::{Calls, MockTransport, Outcomes, mock, post, read_whole, serve};

const OPENAI: &str = "http://openai.test/v1";
const BACKUP: &str = "http://backup.test/v1";
const CHAT: &str = "/v1/chat/completions";

const STREAM: &[&str] = &[
    "data: {\"id\":\"c1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Hel",
    "lo\"}}]}\n\ndata: {\"id\":\"c1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\" world\"}}]}\n\n",
    "data: {\"id\":\"c1\",\"choices\":[],\"usage\":{\"prompt_tokens\":5,\"completion_tokens\":2}}\n\n",
    "data: [DONE]\n\n",
];

const STREAM_REQ: &str = r#"{"model":"gpt-x","stream":true,"stream_options":{"include_usage":true},"messages":[{"role":"user","content":"hi"}]}"#;
const UNARY_REQ: &str = r#"{"model":"gpt-x",  "messages":[]}"#;
const UNARY: &str = r#"{"id":"c2","choices":[{"message":{"content":"hi"}}],"usage":{"prompt_tokens":4,"completion_tokens":1}}"#;

type Call = http::Request<Body>;

fn chat() -> Call {
    post(CHAT, STREAM_REQ)
}

fn unary_chat() -> Call {
    post(CHAT, UNARY_REQ)
}

/// A provider to add to a gateway.
type AddProvider = Box<dyn FnOnce(Gateway) -> Result<Gateway, Fault>>;

fn add_provider(name: &str, format: impl Format, transport: impl Transport) -> AddProvider {
    let name = name.to_owned();
    Box::new(move |g| g.provider(name, format, transport))
}

/// A provider at `base_url` with its own key, or with no headers, so the
/// client's credentials pass through.
fn provider(
    mock: &MockTransport,
    name: &str,
    base_url: &str,
    key: Option<&'static str>,
) -> AddProvider {
    let mut headers = HeaderMap::new();
    if let Some(k) = key {
        headers.insert("authorization", HeaderValue::from_static(k));
    }
    add_provider(name, OpenAiChat, mock.at(base_url).with_headers(headers))
}

/// A gateway serving OpenAI clients: `list`, `plugins` in order, and
/// `target`.
fn gateway(
    list: impl IntoIterator<Item = AddProvider>,
    plugins: Vec<Arc<dyn Plugin>>,
    target: Option<&str>,
) -> Result<Gateway, Fault> {
    let gw = list.into_iter().try_fold(Gateway::new(), |g, add| add(g))?;
    let mut named = plugins.into_iter().enumerate();
    let gw = named.try_fold(gw.serve(OpenAiChat)?, |gw, (i, p)| {
        gw.plugin(format!("p{i}"), p)
    })?;
    match target {
        Some(t) => gw.target(t),
        None => Ok(gw),
    }
}

/// A gateway on a mock transport, recording every outcome: `openai` with a
/// key of its own, `backup` with none.
struct Harness {
    gw: Gateway,
    mock: MockTransport,
    log: Outcomes,
    calls: Calls,
}

impl Harness {
    fn new(chain: Vec<Arc<dyn Plugin>>, target: Option<&str>) -> Self {
        let (log, calls, mock) = (Outcomes::default(), Calls::default(), MockTransport::new());
        let mut plugins: Vec<Arc<dyn Plugin>> =
            vec![Arc::new(log.clone()), Arc::new(calls.clone())];
        plugins.extend(chain);
        let list = [
            provider(&mock, "openai", OPENAI, Some("Bearer sk-test")),
            provider(&mock, "backup", BACKUP, None),
        ];
        let gw = gateway(list, plugins, target).unwrap_or_else(|e| panic!("gateway: {e}"));
        Self {
            gw,
            mock,
            log,
            calls,
        }
    }

    /// Send, read the whole response, and wait for the end hooks.
    async fn send(&self, req: Call) -> (StatusCode, HeaderMap, Result<bytes::Bytes, Fault>) {
        let out = read_whole(serve(&self.gw, req).await.0).await;
        self.gw.idle().await;
        out
    }

    /// Like `send`, for a body that must not be aborted.
    async fn text(&self, call: Call) -> (StatusCode, String) {
        let (status, _, body) = self.send(call).await;
        let body = body.unwrap_or_else(|f| panic!("aborted: {f}"));
        (status, String::from_utf8_lossy(&body).into_owned())
    }
}

fn relay() -> Harness {
    Harness::new(Vec::new(), Some("openai"))
}

/// Sends each request to the provider its `x-to` header names.
struct ByHeader;

#[async_trait]
impl Plugin for ByHeader {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let asked = cx.extensions().get::<Asked>();
        let to = asked.and_then(|a| a.headers.get("x-to")?.to_str().ok());
        let to = to.unwrap_or_default().to_owned();
        cx.set_target(to);
        Ok(None)
    }
}

/// Runs `f` on the provider edge of every call.
struct OnProvider(fn(&mut ProviderContext) -> Result<(), Fault>);

#[async_trait]
impl Plugin for OnProvider {
    async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
        (self.0)(px)
    }
}

/// Sets the temperature of every request.
struct Cool;

#[async_trait]
impl Plugin for Cool {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        cx.filter(|chat: &mut Conversation| {
            chat.temperature = Some(0.0);
            Ok(())
        })?;
        Ok(None)
    }
}

#[tokio::test]
async fn the_clients_credentials_reach_only_a_provider_with_none_configured() {
    // A transport with headers of its own drops the client's key; one with
    // none passes it on, as a local subscription proxy needs. Other client
    // headers reach both.
    for (target, base, auth) in [
        ("openai", OPENAI, Some("Bearer sk-test")),
        ("backup", BACKUP, Some("Bearer client-key")),
    ] {
        let h = Harness::new(Vec::new(), Some(target));
        let mut req = post(CHAT, UNARY_REQ);
        let headers = req.headers_mut();
        headers.insert(
            "authorization",
            HeaderValue::from_static("Bearer client-key"),
        );
        headers.insert(
            "openai-organization",
            HeaderValue::from_static("client-org"),
        );
        headers.insert("openai-beta", HeaderValue::from_static("assistants=v2"));
        h.mock.reply(base, mock::json(StatusCode::OK, UNARY));
        let (status, _, _) = h.send(req).await;
        assert_eq!(status, StatusCode::OK, "{target}");
        let sent = h.mock.sent();
        let got = |name| sent[0].headers().get(name).and_then(|v| v.to_str().ok());
        assert_eq!(got("authorization"), auth, "{target}");
        assert_eq!(got("openai-organization"), Some("client-org"), "{target}");
        assert_eq!(got("openai-beta"), Some("assistants=v2"), "{target}");
    }
}

#[tokio::test]
async fn the_clients_connection_sets_the_version() {
    let h = relay();
    let mut reply = mock::json(StatusCode::OK, UNARY);
    *reply.version_mut() = http::Version::HTTP_2;
    h.mock.reply(OPENAI, reply);

    let (resp, _) = serve(&h.gw, post(CHAT, UNARY_REQ)).await;

    assert_eq!(resp.version(), http::Version::default());
}

#[tokio::test]
async fn the_clients_key_stays_off_a_transport_with_headers_of_its_own() {
    let mock = MockTransport::new();
    // Any header of its own takes the client's key off, whatever its name:
    // `x-team` is no known key, yet the provider may take it for one.
    let at = |name: &'static str| {
        let name = http::HeaderName::from_static(name);
        let headers = HeaderMap::from_iter([(name, HeaderValue::from_static("provider"))]);
        mock.at(OPENAI).with_headers(headers)
    };
    let gw = gateway(
        [
            add_provider("api-key", OpenAiChat, at("api-key")),
            add_provider("x-team", OpenAiChat, at("x-team")),
            add_provider("shared", OpenAiChat, Arc::new(at("x-team"))),
        ],
        vec![Arc::new(ByHeader)],
        None,
    )
    .expect("builds");
    for target in ["api-key", "x-team", "shared"] {
        mock.reply(OPENAI, mock::json(StatusCode::OK, UNARY));
        let mut req = post(CHAT, UNARY_REQ);
        let headers = req.headers_mut();
        headers.insert("x-to", HeaderValue::from_static(target));
        headers.insert("authorization", HeaderValue::from_static("Bearer client"));

        let (status, _, _) = read_whole(serve(&gw, req).await.0).await;

        assert_eq!(status, StatusCode::OK, "{target}");
        let sent = mock.sent();
        let sent = sent.last().expect("sent");
        assert!(sent.headers().get("authorization").is_none(), "{target}");
    }
}

#[tokio::test]
async fn a_relayed_head_leaves_out_the_providers_origin_headers() {
    let h = relay();
    let mut reply = mock::json(StatusCode::OK, UNARY);
    for (name, value) in [
        ("openai-organization", "org-operator"),
        ("set-cookie", "__cf_bm=x; Domain=.openai.com"),
        ("alt-svc", "h3=\":443\""),
        ("strict-transport-security", "max-age=31536000"),
        ("connection", "x-hop"),
        ("x-hop", "1"),
        ("keep-alive", "timeout=5"),
        ("x-request-id", "req_1"),
    ] {
        reply
            .headers_mut()
            .insert(name, HeaderValue::from_static(value));
    }
    h.mock.reply(OPENAI, reply);

    let (_, headers, _) = h.send(unary_chat()).await;

    // Headers about the response stay; the provider origin's state does not
    // reach clients.
    for origin in [
        "set-cookie",
        "alt-svc",
        "strict-transport-security",
        "connection",
        "x-hop",
        "keep-alive",
    ] {
        assert!(headers.get(origin).is_none(), "{origin}");
    }
    assert!(headers.get("openai-organization").is_some());
    assert_eq!(
        headers.get("x-request-id"),
        Some(&HeaderValue::from_static("req_1"))
    );
}

#[tokio::test]
async fn the_clients_credentials_go_once() {
    let h = Harness::new(Vec::new(), Some("backup"));
    let mut req = post(CHAT, UNARY_REQ);
    let headers = req.headers_mut();
    headers.insert(
        "authorization",
        HeaderValue::from_static("Bearer client-key"),
    );
    headers.insert("openai-organization", HeaderValue::from_static("org-1"));
    h.mock.reply(BACKUP, mock::json(StatusCode::OK, UNARY));

    let _ = h.send(req).await;

    let sent = h.mock.sent();
    for name in ["authorization", "openai-organization"] {
        assert_eq!(sent[0].headers().get_all(name).iter().count(), 1, "{name}");
    }
}

#[tokio::test]
async fn a_transport_header_of_several_values_sends_them_all() {
    let mut headers = HeaderMap::new();
    headers.append("openai-beta", HeaderValue::from_static("a"));
    headers.append("openai-beta", HeaderValue::from_static("b"));
    let mock = MockTransport::new();
    let gw = gateway(
        [add_provider(
            "openai",
            OpenAiChat,
            mock.at(OPENAI).with_headers(headers),
        )],
        Vec::new(),
        Some("openai"),
    )
    .expect("builds");
    mock.reply(OPENAI, mock::json(StatusCode::OK, UNARY));
    let mut req = post(CHAT, UNARY_REQ);
    let client = HeaderValue::from_static("client");
    req.headers_mut().insert("openai-beta", client);

    let _ = read_whole(serve(&gw, req).await.0).await;

    let sent = mock.sent();
    let beta: Vec<_> = sent[0].headers().get_all("openai-beta").iter().collect();
    assert_eq!(beta, ["a", "b"]);
}

#[tokio::test]
async fn a_calls_own_key_keeps_the_clients_credentials_off_it() {
    let tenant = OnProvider(|px| {
        let key = HeaderValue::from_static("Bearer tenant");
        let headers = HeaderMap::from_iter([(http::header::AUTHORIZATION, key)]);
        px.extensions_mut().insert(CallHeaders(headers));
        Ok(())
    });
    let h = Harness::new(vec![Arc::new(tenant)], Some("backup"));
    h.mock.reply(BACKUP, mock::json(StatusCode::OK, UNARY));
    let mut req = unary_chat();
    let headers = req.headers_mut();
    headers.insert("authorization", HeaderValue::from_static("Bearer client"));
    headers.insert("x-api-key", HeaderValue::from_static("client"));
    *req.uri_mut() = http::Uri::from_static("/v1/chat/completions?key=client");

    assert_eq!(h.text(req).await.0, StatusCode::OK);

    let sent = h.mock.sent();
    let auth: Vec<_> = sent[0].headers().get_all("authorization").iter().collect();
    assert_eq!(auth, ["Bearer tenant"]);
    assert!(sent[0].headers().get("x-api-key").is_none());
    assert_eq!(sent[0].uri().query(), None);
}

#[tokio::test]
async fn a_changed_body_is_sent_without_the_clients_content_encoding() {
    for (chain, kept) in [
        (Vec::new(), true),
        (vec![Arc::new(Cool) as Arc<dyn Plugin>], false),
    ] {
        let h = Harness::new(chain, Some("openai"));
        h.mock.reply(OPENAI, mock::json(StatusCode::OK, UNARY));
        let mut req = unary_chat();
        let identity = HeaderValue::from_static("identity");
        req.headers_mut().insert("content-encoding", identity);

        assert_eq!(h.text(req).await.0, StatusCode::OK);
        let sent = h.mock.sent();
        assert_eq!(sent[0].headers().contains_key("content-encoding"), kept);
    }
}

#[tokio::test]
async fn a_transport_with_its_own_key_drops_credentials_from_the_query() {
    let query = "key=a&api_key=b&access_token=c&api-version=1&beta=true";
    // `openai` has a key of its own; `backup` has none, so it puts the
    // client's back (changed: after the rest, as the adapter lifted them).
    let passed = "api-version=1&beta=true&key=a&api_key=b&access_token=c";
    for (target, base, sent) in [
        ("openai", OPENAI, Some("api-version=1&beta=true")),
        ("backup", BACKUP, Some(passed)),
    ] {
        let h = Harness::new(Vec::new(), Some(target));
        h.mock.reply(base, mock::json(StatusCode::OK, UNARY));
        let mut req = unary_chat();
        *req.uri_mut() = format!("{CHAT}?{query}").parse().unwrap_or_default();

        assert_eq!(h.text(req).await.0, StatusCode::OK);
        assert_eq!(h.mock.sent()[0].uri().query(), sent, "{target}");
    }
}

#[tokio::test]
async fn a_relay_passes_the_clients_end_to_end_headers() {
    // To a provider with no headers of its own, credentials pass too.
    let h = Harness::new(Vec::new(), Some("backup"));
    h.mock.reply(BACKUP, mock::json(StatusCode::OK, UNARY));
    let mut req = post(CHAT, UNARY_REQ);
    let denied = [
        "cookie",
        "accept-encoding",
        "forwarded",
        "via",
        "x-real-ip",
        "x-forwarded-for",
        "x-forwarded-host",
        "x-http-method-override",
        "x-method-override",
        "if-none-match",
        "range",
    ];
    let passed = [
        ("user-agent", "OpenAI/Python 1.0"),
        ("x-app", "notes"),
        ("x-stainless-lang", "python"),
        ("idempotency-key", "k1"),
        ("x-max-tokens", "10"),
        ("x-monkey", "m"),
        ("x-tokenizer", "t"),
        ("x-api-key", "k"),
        ("x-auth-token", "k"),
        ("x-client-secret", "k"),
        ("x-goog-api-key", "k"),
        ("cf-aig-authorization", "k"),
        ("x-password", "k"),
    ];
    for (name, value) in denied.map(|name| (name, "v")).iter().chain(&passed) {
        let value = HeaderValue::from_static(value);
        req.headers_mut().insert(*name, value);
    }

    assert_eq!(h.text(req).await.0, StatusCode::OK);

    let sent = h.mock.sent();
    let headers = sent[0].headers();
    for (name, value) in passed {
        assert_eq!(headers[name], value);
    }
    for name in denied {
        assert!(headers.get(name).is_none(), "{name}");
    }
    assert_eq!(headers.get_all("content-type").iter().count(), 1);
}

#[tokio::test]
async fn a_plugin_removes_the_gateways_own_credential() {
    /// Checks the gateway's key and removes it, so no format passes it on.
    struct Auth;

    #[async_trait]
    impl Plugin for Auth {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            // Changed: the adapter lifted the credential out of the head.
            let credential = cx.extensions_mut().remove::<Credential>();
            match credential.as_ref().and_then(|c| c.get("x-gateway-auth")) {
                Some(key) if key == "secret" => Ok(None),
                _ => Err(Fault::new(StatusCode::UNAUTHORIZED, "unauthorized", "no")),
            }
        }
    }

    let h = Harness::new(vec![Arc::new(Auth)], Some("backup"));
    h.mock.reply(BACKUP, mock::json(StatusCode::OK, UNARY));
    let mut req = unary_chat();
    let key = HeaderValue::from_static("secret");
    req.headers_mut().insert("x-gateway-auth", key);

    assert_eq!(h.text(req).await.0, StatusCode::OK);
    assert!(h.mock.sent()[0].headers().get("x-gateway-auth").is_none());
}

#[tokio::test]
async fn a_clients_connection_header_names_only_its_own_headers() {
    let h = Harness::new(Vec::new(), Some("backup"));
    h.mock.reply(BACKUP, mock::json(StatusCode::OK, UNARY));
    let mut req = unary_chat();
    let headers = req.headers_mut();
    let named = HeaderValue::from_static("content-type, x-hop");
    headers.insert("connection", named);
    headers.insert("x-hop", HeaderValue::from_static("1"));

    assert_eq!(h.text(req).await.0, StatusCode::OK);

    // The format's content type stays; a header `Connection` names goes.
    let sent = h.mock.sent();
    assert_eq!(sent[0].headers()["content-type"], "application/json");
    assert!(sent[0].headers().get("x-hop").is_none());
}

#[tokio::test]
async fn a_bodiless_response_ends_as_its_head_goes_out() {
    /// Answers 204 with a stream, which the client's format writes as a
    /// whole body that no host sends.
    struct NoContent;

    #[async_trait]
    impl Plugin for NoContent {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            cx.edit_response_head(|head| {
                head.status = StatusCode::NO_CONTENT;
                Ok(())
            });
            let outputs = [Ok(Reply::text(0, "hi"))];
            Ok(Some(Response::stream(futures::stream::iter(outputs))))
        }
    }

    let h = Harness::new(vec![Arc::new(NoContent)], None);

    let (resp, _) = serve(&h.gw, unary_chat()).await;
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);
    // A host never polls a 204's body.
    drop(resp);
    h.gw.idle().await;

    // Changed: the adapter reads the body it does not send to its end; an
    // outcome has no head.
    assert_eq!(h.log.only().end, Ok(()));
}

#[tokio::test]
async fn a_providers_te_header_does_not_reach_the_client() {
    let h = relay();
    let mut reply = mock::json(StatusCode::OK, UNARY);
    reply
        .headers_mut()
        .insert("te", HeaderValue::from_static("trailers"));
    h.mock.reply(OPENAI, reply);

    let (_, headers, _) = h.send(unary_chat()).await;

    assert!(headers.get("te").is_none());
}

#[tokio::test]
async fn the_kernel_leaves_origin_headers_to_the_transport() {
    /// Answers with a cookie of its origin, which it does not strip.
    struct Cookie;

    #[async_trait]
    impl Transport for Cookie {
        async fn send(&self, _req: http::Request<Body>) -> Result<http::Response<Body>, Fault> {
            let body = http_body_util::Full::new(bytes::Bytes::from(UNARY));
            let mut resp = http::Response::new(http_body_util::BodyExt::boxed_unsync(
                http_body_util::BodyExt::map_err(body, Into::into),
            ));
            let headers = resp.headers_mut();
            headers.insert("content-type", HeaderValue::from_static("application/json"));
            headers.insert("set-cookie", HeaderValue::from_static("a=1"));
            Ok(resp)
        }
    }

    let gw = gateway(
        [add_provider("openai", OpenAiChat, Cookie)],
        vec![],
        Some("openai"),
    )
    .unwrap_or_else(|e| panic!("{e}"));

    let (status, headers, _) = read_whole(serve(&gw, unary_chat()).await.0).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        headers.get("set-cookie"),
        Some(&HeaderValue::from_static("a=1"))
    );
}

/// A bodiless response ends its call `Ok` when its head goes out, even if the
/// provider sent a body.
#[tokio::test]
async fn a_bodiless_response_ends_its_call_ok_though_the_provider_sent_a_body() {
    let h = Harness::new(Vec::new(), Some("openai"));
    let mut body = mock::sse(STREAM);
    *body.status_mut() = StatusCode::NO_CONTENT;
    h.mock.reply(OPENAI, body);

    let (status, _, _) = h.send(chat()).await;

    assert_eq!(status, StatusCode::NO_CONTENT);
    assert_eq!(h.calls.all()[0].end, Ok(()));
}
