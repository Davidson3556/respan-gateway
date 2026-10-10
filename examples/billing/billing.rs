//! Billing: price each call's usage from your own price table and write it
//! to your own store. The kernel counts no usage: a provider-edge `Output`
//! filter sums what the provider reports (`meter`), and `on_provider_end`
//! bills it, once per call, so a fallback's or a judge's calls are billed
//! too, each at the model it used. No hold, no limit: to stop spending, see
//! `wallet` and `cost_cap`.
//!
//! **Order matters.** A filter sees a reply only if no filter before it
//! failed on it or held it back, and a call's output filters run the last
//! added first. So register billing after every plugin that filters
//! outputs (last): its filter then sees each reply first. A failure still
//! ends the stream, and usage the provider would have reported later is
//! never seen: bill such a call from an estimate,
//! or hold before it as `wallet` does.
//!
//! `cargo run -p respan-gateway-examples --example billing`

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};

use mockllm::{Answer, FakeOpenAi};
use respan_gateway_conversation::Input;
use respan_gateway_conversation::Usage;
use respan_gateway_conversation::openai::OpenAiChat;
use respan_gateway_core::{Fault, Gateway, Plugin, ProviderContext, async_trait};
use respan_gateway_examples::{CallUsage, Price, meter};
use respan_gateway_http::Wire;
use respan_gateway_http::http::HeaderValue;
#[allow(dead_code)]
#[path = "../../tests/support/http.rs"]
mod http_support;
use http_support::{post, read_whole, serve};
#[allow(dead_code)]
#[path = "../support/http.rs"]
mod provider_support;

/// One billed call.
#[derive(Debug, Clone, PartialEq)]
struct Row {
    account: String,
    model: String,
    usage: Usage,
    /// `None` for a model with no price: bill it later, or alert.
    micros: Option<i64>,
}

/// Where rows go. A SQL version:
///
/// ```ignore
/// struct Sql(sqlx::PgPool);
///
/// #[async_trait]
/// impl UsageStore for Sql {
///     async fn record(&self, row: Row) -> Result<(), Fault> {
///         sqlx::query("INSERT INTO usage (account, model, usage, micros) VALUES ($1, $2, $3, $4)")
///             .bind(row.account).bind(row.model)
///             .bind(sqlx::types::Json(row.usage.iter().collect::<HashMap<_, _>>()))
///             .bind(row.micros)
///             .execute(&self.0).await
///             .map(drop).map_err(|e| Fault::internal("usage store").with_source(e))
///     }
/// }
/// ```
#[async_trait]
trait UsageStore: Send + Sync + 'static {
    async fn record(&self, row: Row) -> Result<(), Fault>;
}

#[derive(Clone, Default)]
struct Memory(Arc<Mutex<Vec<Row>>>);

impl Memory {
    fn rows(&self) -> Vec<Row> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

#[async_trait]
impl UsageStore for Memory {
    async fn record(&self, row: Row) -> Result<(), Fault> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(row);
        Ok(())
    }
}

struct Billing<S> {
    prices: HashMap<&'static str, Price>,
    store: S,
}

#[async_trait]
impl<S: UsageStore> Plugin for Billing<S> {
    async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
        meter(px)
    }

    async fn on_provider_end(&self, px: &ProviderContext, _end: &Result<(), Fault>) {
        let Some(usage) = CallUsage::of(px).filter(|u| !u.is_empty()) else {
            return;
        };
        // Who pays: here a header an auth plugin vouched for.
        let asked = px.extensions().get::<Wire>().and_then(Wire::get);
        let header = asked.and_then(|a| a.headers().get("x-account")?.to_str().ok());
        let account = header.unwrap_or("anonymous");
        // The model this call asked for, as every plugin left it.
        let chat = px.input::<Input>().ok();
        let model = chat.and_then(|c| c.model.clone()).unwrap_or_default();
        let price = self.prices.get(model.as_str());
        let row = Row {
            account: account.to_owned(),
            micros: price.map(|p| p.of(&usage)),
            model,
            usage,
        };
        // Keep it fast or spawn: the request's `on_client_end` waits for
        // this hook. An `Err` here is yours to log or queue for retry.
        let _ = self.store.record(row).await;
    }
}

const SMALL: &str = "gpt-4o-mini";
const LARGE: &str = "gpt-4o";
const UNPRICED: &str = "gpt-4.1";

fn gateway(
    provider: impl Fn(respan_gateway_core::Extensions) -> respan_gateway_http::ProviderIo
    + Send
    + Sync
    + 'static,
    store: Memory,
) -> Result<Gateway, Fault> {
    let prices = HashMap::from([
        (SMALL, Price::new(150_000, 600_000)),
        (LARGE, Price::new(2_500_000, 10_000_000)),
    ]);
    Gateway::new()
        .provider::<Input, _, _>("openai", OpenAiChat::OPENAI, provider)?
        .client::<Input, _, _>(OpenAiChat::OPENAI)?
        // Last, so its filter sees each reply first.
        .plugin(Billing { prices, store })
        .target("openai")
}

/// Bills `account` for asking `model`: the provider answers with this
/// `(input, output)` usage, or with a 502 if `None`.
async fn ask(
    gw: &Gateway,
    openai: &FakeOpenAi,
    account: &'static str,
    model: &str,
    usage: Option<(u32, u32)>,
) {
    if let Some((input, output)) = usage {
        let answer = format!(
            r#"{{"id":"c1","object":"chat.completion","model":"{model}","choices":[{{"index":0,"finish_reason":"stop","message":{{"role":"assistant","content":"hi"}}}}],"usage":{{"prompt_tokens":{input},"completion_tokens":{output}}}}}"#
        );
        openai.answer(Answer::raw(answer));
    } else {
        openai.answer(Answer::error(502).message("upstream unavailable"));
    }
    let body = format!(r#"{{"model":"{model}","messages":[{{"role":"user","content":"hi"}}]}}"#);
    let mut req = post("/v1/chat/completions", body);
    let account = HeaderValue::from_static(account);
    req.headers_mut().insert("x-account", account);
    let _ = read_whole(serve(gw, req).await.0).await;
    gw.idle().await;
    assert_eq!(
        openai
            .requests()
            .last()
            .and_then(|r| r.body["model"].as_str())
            .map(str::to_owned),
        Some(model.to_owned())
    );
}

#[tokio::main]
async fn main() -> Result<(), Fault> {
    let (openai, store) = (FakeOpenAi::start().await, Memory::default());
    let gw = gateway(
        provider_support::openai(&openai.base_url())?.connect(),
        store.clone(),
    )?;
    ask(&gw, &openai, "acme", SMALL, Some((1_000, 500))).await;
    ask(&gw, &openai, "acme", LARGE, Some((1_000, 500))).await;
    ask(&gw, &openai, "globex", UNPRICED, Some((10, 10))).await;
    for Row {
        account,
        model,
        micros,
        ..
    } in store.rows()
    {
        println!("{account:8} {model:9} {micros:?} micro-dollars");
    }
    Ok(())
}

#[cfg(test)]
#[path = "../tests/cases/billing/billing.rs"]
mod tests;
