//! Request logs through a sink you choose, and use case 11, zero data
//! retention for one tenant.
//!
//! `Log` writes one record per call (`on_provider_end`) and one per request
//! (`on_client_end`) to a [`Sink`]. Core has no request id, so it makes its
//! own and keeps it in an extension, which every call copies. Content
//! reaches it only through what it asks for: the completion through an
//! output filter, the prompt through the input. Usage reaches it through a
//! provider-edge filter that sums what each call's provider reports
//! (`meter`), so a call's record has its usage and a request's has none.
//! For a tenant marked `NoContent` it installs no content filter and
//! stores no text, while usage and status, for billing and audit, still
//! flow.
//!
//! **Partial:** any plugin can remove an extension, the marker included.
//! Register the plugin that sets it first, as here, and keep the type
//! private to your crate. Caches and stored responses are other plugins;
//! each must check the same marker.
//!
//! `cargo run -p respan-gateway-examples --example logging`

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use mockllm::FakeOpenAi;
use respan_gateway_examples::{CallUsage, meter};
use respan_gateway_http::Asked;
use respan_gateway_http::http::HeaderValue;
use respan_gateway_providers::OpenAiChat;
use respan_gateway_sdk::Extensions;
use respan_gateway_sdk::Usage;
use respan_gateway_sdk::conversation::{Conversation, Message, Reply};
use respan_gateway_sdk::{
    ClientContext, Fault, Gateway, Plugin, ProviderContext, Response, async_trait,
};
#[allow(dead_code)]
#[path = "../../tests/support/http.rs"]
mod http_support;
#[allow(dead_code)]
#[path = "../tests/support.rs"]
mod provider_support;
use http_support::{post, read_whole, serve};

/// One record: a request, or one of its calls (`call` names the provider).
#[derive(Debug, Clone, PartialEq)]
struct Record {
    request: u64,
    call: Option<String>,
    /// The status of the fault that ended it; none for a success.
    status: Option<u16>,
    /// `ok`, or the code of the fault that ended it.
    end: String,
    /// What the call's provider reported; none for a request.
    usage: Usage,
    prompt: Option<String>,
    completion: Option<String>,
}

/// Where records go. End hooks await it, and a request's `on_client_end`
/// waits for its calls' end hooks: keep it fast or spawn. To add your own, implement it,
/// such as a Postgres insert or a `tracing::info!(?record)`.
#[async_trait]
trait Sink: Send + Sync + 'static {
    async fn write(&self, record: Record);
}

/// Records in memory, for `main` and the tests to read.
#[derive(Clone, Default)]
struct Memory(Arc<Mutex<Vec<Record>>>);

#[async_trait]
impl Sink for Memory {
    async fn write(&self, record: Record) {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(record);
    }
}

/// The request's tenant forbids storing prompts and completions.
#[derive(Clone, Copy)]
struct NoContent;

#[derive(Clone, Copy)]
struct RequestId(u64);

/// The completion's text, captured by the request's output filter.
#[derive(Clone, Default)]
struct Captured(Arc<Mutex<String>>);

struct Log<S> {
    sink: S,
    next: AtomicU64,
}

impl Record {
    /// A record without content or usage, from what every hook sees.
    fn new(ext: &Extensions, call: Option<&str>, end: &Result<(), Fault>) -> Self {
        Self {
            request: ext.get::<RequestId>().map_or(0, |id| id.0),
            call: call.map(Into::into),
            status: end
                .as_ref()
                .err()
                .map(|f| respan_gateway_http::status(f).as_u16()),
            end: end
                .as_ref()
                .map_or_else(|f| f.code().into(), |()| "ok".into()),
            usage: Usage::default(),
            prompt: None,
            completion: None,
        }
    }
}

#[async_trait]
impl<S: Sink> Plugin for Log<S> {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let id = RequestId(self.next.fetch_add(1, Ordering::Relaxed));
        cx.extensions_mut().insert(id);
        if cx.extensions().get::<NoContent>().is_some() || !cx.is_kind::<Conversation>() {
            return Ok(None);
        }
        let captured = Captured::default();
        let into = captured.0.clone();
        cx.filter(move |reply: &mut Reply| {
            into.lock()
                .unwrap_or_else(PoisonError::into_inner)
                .extend(reply.texts());
            Ok(())
        })?;
        cx.extensions_mut().insert(captured);
        Ok(None)
    }

    async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
        meter(px)
    }

    async fn on_provider_end(&self, px: &ProviderContext, end: &Result<(), Fault>) {
        let mut record = Record::new(px.extensions(), Some(px.provider()), end);
        record.usage = CallUsage::of(px).unwrap_or_default();
        self.sink.write(record).await;
    }

    async fn on_client_end(&self, cx: &ClientContext, end: &Result<(), Fault>) {
        let ext = &cx.extensions();
        let mut record = Record::new(ext, None, end);
        // Checked again: a marker set after `on_client` still holds.
        let captured = ext
            .get::<Captured>()
            .filter(|_| ext.get::<NoContent>().is_none());
        if let Some(captured) = captured {
            let chat = cx.input::<Conversation>().ok();
            let text = |c: &Conversation| c.messages.iter().map(Message::text).collect::<Vec<_>>();
            record.prompt = chat.map(|c| text(c).join("\n"));
            let completion = captured.0.lock().unwrap_or_else(PoisonError::into_inner);
            record.completion = Some(completion.clone());
        }
        self.sink.write(record).await;
    }
}

/// Marks requests of zero-retention tenants, named by `x-tenant` here
/// (the adapter's view of the head, [`Asked`]); in practice, by the tenant
/// your auth plugin resolved.
struct Tenancy(&'static [&'static str]);

#[async_trait]
impl Plugin for Tenancy {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let asked = cx.extensions().get::<Asked>();
        let tenant = asked.and_then(|a| a.headers.get("x-tenant")?.to_str().ok());
        if tenant.is_some_and(|t| self.0.contains(&t)) {
            cx.extensions_mut().insert(NoContent);
        }
        Ok(None)
    }
}

fn gateway(
    transport: impl Fn(respan_gateway_sdk::Extensions) -> respan_gateway_http::Outgoing
    + Send
    + Sync
    + 'static,
    sink: impl Sink,
) -> Result<Gateway, Fault> {
    let log = Log {
        sink,
        next: AtomicU64::new(1),
    };
    Gateway::new()
        .provider::<Conversation, _, _>("openai", OpenAiChat, transport)?
        .client::<Conversation, _, _>(OpenAiChat)?
        .plugin(Tenancy(&["zdr-bank"]))
        .plugin(log)
        .target("openai")
}

const REQUEST: &str = r#"{"model":"gpt-x","stream":true,"stream_options":{"include_usage":true},"messages":[{"role":"user","content":"hi"}]}"#;
const STREAM: &[&str] = &[
    "data: {\"id\":\"c1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Hello\"}}]}\n\n",
    "data: {\"id\":\"c1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\" world\"},\"finish_reason\":\"stop\"}]}\n\n",
    "data: {\"id\":\"c1\",\"choices\":[],\"usage\":{\"prompt_tokens\":5,\"completion_tokens\":2}}\n\n",
    "data: [DONE]\n\n",
];

/// Logs a request from each tenant: `acme`, then zero-retention `zdr-bank`.
async fn run(fake: &FakeOpenAi) -> Result<Vec<Record>, Fault> {
    let memory = Memory::default();
    let gw = gateway(
        provider_support::openai(&fake.base_url())?.connect(),
        memory.clone(),
    )?;
    for tenant in ["acme", "zdr-bank"] {
        fake.answer(provider_support::sse(STREAM));
        let mut req = post("/v1/chat/completions", REQUEST);
        req.headers_mut()
            .insert("x-tenant", HeaderValue::from_static(tenant));
        read_whole(serve(&gw, req).await.0).await.2?;
        gw.idle().await;
    }
    let records = memory.0.lock().unwrap_or_else(PoisonError::into_inner);
    Ok(records.clone())
}

#[tokio::main]
async fn main() -> Result<(), Fault> {
    let fake = FakeOpenAi::start().await;
    for record in run(&fake).await? {
        println!("{record:?}");
    }
    Ok(())
}

#[cfg(test)]
#[path = "../tests/logging.rs"]
mod tests;
