//! What the SDK's tests share: providers on a mock transport, a funded
//! bank, and the plugins a real gateway would run beside the budget.

#![allow(dead_code)]

use std::sync::Arc;
use std::time::Duration;

use respan_gateway_core::bytes::Bytes;
use respan_gateway_core::futures::{self, TryStreamExt, future::Either};
use respan_gateway_core::http::{self, HeaderMap, HeaderValue, StatusCode};
use respan_gateway_core::{
    Body, ClientContext, Decoder, Doc, Encoder, Fault, Format, Gateway, Outcome, Plugin,
    ProviderContext, Response, Transport, Usage, async_trait,
};
use respan_gateway_formats::conversation::{Conversation, Message, Reply, Role, units};
use respan_gateway_formats::{self as formats, AnthropicMessages, OpenAiChat};
use respan_gateway_testkit::{MockAt, MockTransport, Outcomes, post, read_whole};
use respan_money::{Bank, Basis, Entry, Money, Price, Prices};

pub const OPENAI: &str = "http://openai.test/v1";
pub const BACKUP: &str = "http://backup.test/v1";
/// An Anthropic provider: calls to it are translated.
pub const CLAUDE: &str = "http://claude.test/v1";
/// Records the call, then waits a second before its head.
pub const SLOW: &str = "http://slow.test/v1";
/// Waits a second before the call is sent.
pub const COLD: &str = "http://cold.test/v1";
pub const CHAT: &str = "/v1/chat/completions";
pub const KEY: &str = "sk-1";
pub const CAP: u64 = 1000;

pub const UNARY_REQ: &str = r#"{"model":"gpt-x","messages":[{"role":"user","content":"hi"}]}"#;
pub const STREAM_REQ: &str =
    r#"{"model":"gpt-x","stream":true,"messages":[{"role":"user","content":"hi"}]}"#;

/// 4 in, 1 out: 70 micros at gpt-x.
pub const UNARY: &str = r#"{"id":"c2","choices":[{"message":{"content":"hi"}}],"usage":{"prompt_tokens":4,"completion_tokens":1}}"#;
/// 11 characters of output and no usage.
pub const UNARY_NO_USAGE: &str = r#"{"id":"c3","choices":[{"message":{"content":"hello world"}}]}"#;
pub const UNARY_COST: i64 = 4 * 10 + 30;

/// "Hello world" (11 characters), then 5 in and 2 out: 110 micros.
pub const STREAM: &[&str] = &[
    "data: {\"id\":\"c1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Hel",
    "lo\"}}]}\n\ndata: {\"id\":\"c1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\" world\"}}]}\n\n",
    "data: {\"id\":\"c1\",\"choices\":[],\"usage\":{\"prompt_tokens\":5,\"completion_tokens\":2}}\n\n",
    "data: [DONE]\n\n",
];
pub const STREAM_COST: i64 = 5 * 10 + 2 * 30;

pub fn prices() -> Prices {
    Prices::new()
        .model("gpt-x", Price::new(10, 30))
        .model("gpt-big", Price::new(100, 300))
        .model("judge-1", Price::new(1, 2))
}

/// A bank where `sk-1` sits in team `t1` of customer `acme`, each with
/// `funds`.
pub fn bank(funds: i64) -> Bank {
    let bank = Bank::new();
    bank.open("acme", None, funds)
        .open("t1", Some("acme"), funds)
        .open(KEY, Some("t1"), funds);
    bank
}

pub fn money(bank: &Bank) -> Money {
    Money::new(bank.clone(), prices()).cap(CAP)
}

/// A client request with the key as its bearer token.
pub fn chat(body: &str) -> http::Request<Body> {
    let mut req = post(CHAT, body.to_owned());
    let auth = HeaderValue::from_str(&format!("Bearer {KEY}")).unwrap_or_else(|e| panic!("{e}"));
    req.headers_mut().insert("authorization", auth);
    req
}

/// A transport that waits around a mock.
#[derive(Debug, Clone)]
pub struct Delayed {
    pub inner: MockAt,
    pub before: Duration,
    pub after: Duration,
}

#[async_trait]
impl Transport for Delayed {
    async fn send(&self, req: http::Request<Body>) -> Result<http::Response<Body>, Fault> {
        tokio::time::sleep(self.before).await;
        let resp = self.inner.send(req).await;
        tokio::time::sleep(self.after).await;
        resp
    }
}

fn keyed(mock: &MockTransport, base: &str) -> MockAt {
    let mut headers = HeaderMap::new();
    headers.insert(
        "authorization",
        HeaderValue::from_static("Bearer sk-provider"),
    );
    mock.at(base).with_headers(headers)
}

pub fn providers(gw: Gateway, mock: &MockTransport) -> Result<Gateway, Fault> {
    let second = Duration::from_secs(1);
    let zero = Duration::ZERO;
    let delayed = |base, before, after| Delayed {
        inner: keyed(mock, base),
        before,
        after,
    };
    gw.provider("openai", OpenAiChat, keyed(mock, OPENAI))?
        .provider("backup", OpenAiChat, keyed(mock, BACKUP))?
        .provider("claude", AnthropicMessages, keyed(mock, CLAUDE))?
        .provider("slow", OpenAiChat, delayed(SLOW, zero, second))?
        .provider("cold", OpenAiChat, delayed(COLD, second, zero))?
        .provider("opaque", Opaque, mock.at(OPENAI))
}

/// A gateway on mock providers, recording each request's outcome.
pub struct H {
    pub gw: Gateway,
    pub mock: MockTransport,
    pub bank: Bank,
    pub log: Outcomes,
}

impl H {
    /// The recorder, then `plugins` in order, named `p0`, `p1`, ...
    pub fn new(bank: Bank, plugins: Vec<Arc<dyn Plugin>>, target: Option<&str>) -> Self {
        let mock = MockTransport::new();
        let log = Outcomes::default();
        let build = || -> Result<Gateway, Fault> {
            let mut gw = providers(Gateway::new(), &mock)?
                .serve(OpenAiChat)?
                .serve(Opaque)?
                .plugin("log", log.clone())?;
            for (i, p) in plugins.into_iter().enumerate() {
                gw = gw.plugin(format!("p{i}"), p)?;
            }
            match target {
                Some(t) => gw.target(t),
                None => Ok(gw),
            }
        };
        let gw = build().unwrap_or_else(|e| panic!("gateway: {e}"));
        Self {
            gw,
            mock,
            bank,
            log,
        }
    }

    /// Send, read the whole response, and wait for every end hook.
    pub async fn send(&self, req: http::Request<Body>) -> (StatusCode, String) {
        let (status, _, body) = read_whole(self.gw.handle(req).await.0).await;
        self.gw.idle().await;
        let body = body.map(|b| String::from_utf8_lossy(&b).into_owned());
        (status, body.unwrap_or_else(|f| format!("aborted: {f}")))
    }

    /// The journal, after checking every hold was settled exactly once.
    pub fn settled(&self) -> Vec<Entry> {
        let journal = self.bank.journal();
        assert_eq!(self.bank.open_holds(), 0, "holds left open: {journal:?}");
        assert_eq!(self.bank.held(KEY), 0);
        let mut holds: Vec<_> = journal.iter().filter_map(|e| e.hold).collect();
        let n = holds.len();
        holds.dedup();
        assert_eq!(holds.len(), n, "a hold settled twice: {journal:?}");
        journal
    }

    pub fn bases(&self) -> Vec<(Basis, i64)> {
        self.settled()
            .iter()
            .map(|e| (e.basis, e.charged))
            .collect()
    }
}

/// The input estimate behind a hold of `entry` at gpt-x under the cap.
pub fn input_of(entry: &Entry) -> i64 {
    (entry.held - CAP as i64 * 30) / 10
}

// Plugins a real gateway runs beside the budget.

/// Tries providers in order until one answers with content: the idiom of
/// docs/ARCHITECTURE.md, as it compiles against `ready(self)`.
pub struct Fallback(pub Vec<&'static str>);

#[async_trait]
impl Plugin for Fallback {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        for name in &self.0 {
            let Ok(resp) = cx.call(*name).await else {
                continue;
            };
            if resp.head().status.is_success()
                && let Ok(resp) = resp.ready().await
            {
                return Ok(Some(resp));
            }
        }
        Err(Fault::upstream("every provider failed"))
    }
}

/// Like [`Fallback`], but drops a failed attempt before the next and
/// falls back from a success that fails before any content: the idiom of
/// the spotless plan (E5).
pub struct DroppingFallback(pub Vec<&'static str>);

#[async_trait]
impl Plugin for DroppingFallback {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let mut last = Err(Fault::upstream("no providers"));
        for name in &self.0 {
            drop(last);
            last = match cx.call(*name).await {
                Ok(resp) if resp.head().status.is_success() => resp.ready().await,
                other => other,
            };
            if last.as_ref().is_ok_and(|r| r.head().status.is_success()) {
                break;
            }
        }
        last.map(Some)
    }
}

/// Calls `first`; after `delay`, also `second`; keeps whichever answers
/// first and drops the other.
pub struct Hedge {
    pub first: &'static str,
    pub second: &'static str,
    pub delay: Duration,
}

#[async_trait]
impl Plugin for Hedge {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let first = cx.call(self.first);
        let second = async {
            tokio::time::sleep(self.delay).await;
            cx.call(self.second).await
        };
        let won = match futures::future::select(first, Box::pin(second)).await {
            Either::Left((won, _)) | Either::Right((won, _)) => won,
        };
        won.map(Some)
    }
}

/// Calls both at once, waits for both heads, keeps the first.
pub struct KeepFirst(pub &'static str, pub &'static str);

#[async_trait]
impl Plugin for KeepFirst {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let (a, b) = futures::future::join(cx.call(self.0), cx.call(self.1)).await;
        drop(b);
        a.map(Some)
    }
}

/// Asks a judge model which provider should answer, then targets it.
pub struct SmartRouter;

#[async_trait]
impl Plugin for SmartRouter {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let mut ask = Conversation::new([Message::new(Role::User, "openai or backup?")]);
        ask.model = Some("judge-1".into());
        let verdict = cx.call("openai").with_input(ask);
        let verdict: Vec<Reply> = verdict.await?.outputs().try_collect().await?;
        let pick = verdict.iter().find_map(|e| match e {
            Reply::Text { text, .. } => Some(text.clone()),
            _ => None,
        });
        cx.set_target(pick.unwrap_or_else(|| "openai".into()));
        Ok(None)
    }
}

/// Answers from a cache, with the usage the cached answer cost: 100 in,
/// 10 out.
pub struct Cache;

pub fn cached() -> Response {
    let events = [
        Ok(Reply::start("cached", "gpt-x")),
        Ok(Reply::text(0, "from cache")),
    ];
    Response::stream(futures::stream::iter(events)).with_usage(cached_usage())
}

/// What the cached answer cost: 100 in, 10 out.
pub fn cached_usage() -> Usage {
    let mut usage = Usage::default();
    usage.add(units::INPUT_TOKENS, 100.0);
    usage.add(units::OUTPUT_TOKENS, 10.0);
    usage
}

#[async_trait]
impl Plugin for Cache {
    async fn on_client(&self, _cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        Ok(Some(cached()))
    }
}

/// Asks a judge whether the cache may answer, then answers from it.
pub struct JudgedCache;

#[async_trait]
impl Plugin for JudgedCache {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let mut ask = Conversation::new([Message::new(Role::User, "is the cache fresh?")]);
        ask.model = Some("judge-1".into());
        let verdict = cx.call("openai").with_input(ask);
        let _: Vec<Reply> = verdict.await?.outputs().try_collect().await?;
        Ok(Some(cached()))
    }
}

/// Refuses every request on the client edge.
pub struct Guard;

#[async_trait]
impl Plugin for Guard {
    async fn on_client(&self, _cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        Err(Fault::new(StatusCode::FORBIDDEN, "blocked", "no"))
    }
}

/// Runs `f` on the provider edge of every call.
pub struct OnProvider(pub fn(&mut ProviderContext) -> Result<(), Fault>);

#[async_trait]
impl Plugin for OnProvider {
    async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
        (self.0)(px)
    }
}

/// Panics in every call's end hook.
pub struct PanicAtEnd;

#[async_trait]
impl Plugin for PanicAtEnd {
    async fn on_provider(&self, _px: &mut ProviderContext) -> Result<(), Fault> {
        Ok(())
    }

    async fn on_provider_end(&self, _px: &ProviderContext, _o: &Outcome) {
        panic!("a buggy end hook");
    }
}

/// Fails the response's text with a panic, on the client edge.
pub struct PanicFilter;

#[async_trait]
impl Plugin for PanicFilter {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        cx.filter(|e: &mut Reply| match e {
            Reply::Text { text, .. } if text.contains("world") => panic!("a buggy filter"),
            _ => Ok(()),
        })?;
        Ok(None)
    }
}

/// Calls the target, then panics holding its response.
pub struct PanicHolding;

#[async_trait]
impl Plugin for PanicHolding {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let _held = cx.call("openai").await?;
        panic!("a buggy plugin");
    }
}

/// A protocol the gateway only relays: it has no kind, so no cost bound.
pub struct Opaque;

impl Format for Opaque {
    type Input = ();
    type Output = ();

    fn decode_input(&self, _: &http::request::Parts, _: &Doc) -> Result<(), Fault> {
        Ok(())
    }

    fn encode_input(&self, (): &(), _: Option<(&Doc, &())>) -> Result<Doc, Fault> {
        Ok(Doc::default())
    }

    fn name(&self) -> &str {
        "opaque"
    }

    fn accepts(&self, head: &http::request::Parts) -> bool {
        formats::posts_to(head, "/opaque")
    }

    fn prepare(
        &self,
        body: Doc,
        _: Option<&()>,
        client: &http::request::Parts,
        relayed: bool,
    ) -> Result<http::Request<Doc>, Fault> {
        let mut req = http::Request::new(body);
        *req.method_mut() = http::Method::POST;
        *req.uri_mut() = http::Uri::from_static("/opaque");
        formats::pass_headers(&mut req, relayed.then_some(client));
        Ok(req)
    }

    fn decoder(&self, head: &http::response::Parts, _: &http::Request<Doc>) -> Box<dyn Decoder> {
        formats::bytes_decoder(head)
    }

    fn encoder(
        &self,
        head: &mut http::response::Parts,
        _: &http::Request<Doc>,
        relayed: bool,
    ) -> Box<dyn Encoder> {
        formats::bytes_encoder(head, relayed)
    }
}

pub fn opaque() -> http::Request<Body> {
    let mut req = post("/opaque", Bytes::from_static(b"anything"));
    let auth = HeaderValue::from_str(&format!("Bearer {KEY}")).unwrap_or_else(|e| panic!("{e}"));
    req.headers_mut().insert("authorization", auth);
    req
}
