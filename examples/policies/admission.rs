//! Use case 10, waiting instead of failing: an admission stage before any
//! call. `on_client` may await, so a request parks in a priority queue
//! until a slot frees or its deadline passes, then gets 429 with
//! `Retry-After`. Paid requests go first and wait longer.
//!
//! Within a priority, end users share fairly: the queue is round-robin over
//! users (`x-user` here; your auth plugin in practice), each with its own
//! first-come queue. Configured weights give a user several turns per round;
//! equal weights give ordinary round-robin. Priority is considered first.
//!
//! The slot is held until the request ends, its response included, and
//! released in `on_client_end`: core keeps a request's context until its
//! end hooks run. A request that leaves while parked just drops out.
//!
//! `Slots::open` shares the queue across processes using a SQLite file.
//! Renewable leases recover capacity after a crash. Live requests stop when
//! renewal fails, including while waiting for a provider head; capacity is
//! reclaimed after a grace period. Already-dispatched upstream computation
//! cannot be recalled. The database file needs local SQLite-compatible locks.
//! The default in-memory store uses the same scheduling rules.
//!
//! `cargo run -p respan-gateway-examples --example admission`

use respan_gateway_conversation::{Input, Output};
use respan_gateway_core::futures::{FutureExt, StreamExt, stream};
use respan_gateway_core::futures::{future::BoxFuture, stream::BoxStream};
use respan_gateway_core::{ErrorCode, RetryAfter};
use respan_gateway_examples::LIMIT;
use respan_gateway_examples::admission::{Slot, Slots};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use mockllm::{Answer, FakeOpenAi};
use respan_gateway_conversation::openai::OpenAiChat;
use respan_gateway_core::{ClientContext, Fault, Gateway, Plugin, Response, async_trait};
use respan_gateway_http::Status;
use respan_gateway_http::Wire;
use respan_gateway_http::http::{self, HeaderValue, StatusCode, header::RETRY_AFTER};
#[allow(dead_code)]
#[path = "../../tests/support/http.rs"]
mod http_support;
use http_support::{post, read_whole, serve};
#[allow(dead_code)]
#[path = "../support/http.rs"]
mod provider_support;
use tokio::time::Instant;

/// The request's slot, taken back at its end.
#[derive(Clone)]
struct Held(Arc<Mutex<Option<Slot>>>);

impl Held {
    fn lost(&self) -> BoxFuture<'static, ()> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_ref()
            .map(|slot| slot.lost().boxed())
            .unwrap_or_else(|| std::future::ready(()).boxed())
    }
}

async fn guarded<T>(
    held: &Held,
    work: impl std::future::Future<Output = Result<T, Fault>>,
) -> Result<T, Fault> {
    tokio::select! {
        biased;
        _ = held.lost() => Err(Fault::new("admission lease expired").with(ErrorCode("busy".into())).with(Status(StatusCode::TOO_MANY_REQUESTS))),
        result = work => result,
    }
}

fn bounded<T: Send + 'static>(
    input: BoxStream<'static, Result<T, Fault>>,
    held: Held,
) -> BoxStream<'static, Result<T, Fault>> {
    stream::unfold((input, false), move |(mut input, ended)| {
        let held = held.clone();
        async move {
            if ended {
                return None;
            }
            let next = guarded(&held, async { input.next().await.transpose() }).await;
            match next {
                Ok(Some(value)) => Some((Ok(value), (input, false))),
                Ok(None) => None,
                Err(fault) => Some((Err(fault), (input, true))),
            }
        }
    })
    .boxed()
}

type HttpReply = (
    http::Response<respan_gateway_http::Body>,
    Option<respan_gateway_http::Completion>,
);

/// Keep lease loss observable while opening a provider or transferring bytes.
fn supervised<F, Fut>(
    send: F,
) -> impl Fn(
    http::Request<respan_gateway_http::Body>,
) -> respan_gateway_core::futures::future::BoxFuture<'static, Result<HttpReply, Fault>>
+ Send
+ Sync
+ 'static
where
    F: Fn(http::Request<respan_gateway_http::Body>) -> Fut + Clone + Send + Sync + 'static,
    Fut: std::future::Future<Output = Result<HttpReply, Fault>> + Send + 'static,
{
    move |call| {
        let send = send.clone();
        async move {
            let Some(held) = call
                .extensions()
                .get::<respan_gateway_core::Extensions>()
                .and_then(|extensions| extensions.get::<Held>())
                .cloned()
            else {
                return send(call).await;
            };
            let call = call
                .map(|body| respan_gateway_http::Body::stream(bounded(body.boxed(), held.clone())));
            let (response, completion) = guarded(&held, send(call)).await?;
            let response = response
                .map(|body| respan_gateway_http::Body::stream(bounded(body.boxed(), held.clone())));
            let completion =
                completion.map(|writing| async move { guarded(&held, writing).await }.boxed());
            Ok((response, completion))
        }
        .boxed()
    }
}

/// Admits each request into `slots`: `x-tier: paid` first, waiting up to
/// `paid`; others up to `free`.
struct Admission {
    slots: Slots,
    paid: Duration,
    free: Duration,
}

#[async_trait]
impl Plugin for Admission {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let asked = cx.extensions().get::<Wire>().and_then(Wire::get);
        let paid = asked
            .and_then(|a| a.headers().get("x-tier"))
            .is_some_and(|t| t == "paid");
        let (priority, wait) = if paid { (1, self.paid) } else { (0, self.free) };
        let user = asked.and_then(|a| a.headers().get("x-user")?.to_str().ok());
        let user = user.unwrap_or_default().to_owned();
        let until = Instant::now() + wait;
        let Some(slot) = self.slots.acquire(priority, &user, until).await? else {
            let busy = "every slot is busy; retry later";
            let fault = Fault::new(busy)
                .with(ErrorCode("busy".into()))
                .with(Status(StatusCode::TOO_MANY_REQUESTS));
            return Err(fault.with(RetryAfter(Duration::from_secs(1))));
        };
        let held = Held(Arc::new(Mutex::new(Some(slot))));
        let monitoring = held.clone();
        cx.wrap(move |outputs: respan_gateway_core::EventStream<Output>| {
            bounded(outputs.boxed(), monitoring)
        })?;
        cx.extensions_mut().insert(held);
        // Explicitly forward the demonstration's tracing label.
        if let Some(label) = cx
            .extensions()
            .get::<Wire>()
            .and_then(Wire::get)
            .and_then(|a| a.headers().get("x-case-label"))
            .cloned()
        {
            let mut headers = http::HeaderMap::new();
            headers.insert("x-case-label", label);
            cx.extensions_mut()
                .insert(respan_gateway_http::CallHeaders(headers));
        }
        Ok(None)
    }

    async fn on_client_end(&self, cx: &ClientContext, _end: &Result<(), Fault>) {
        if let Some(Held(slot)) = cx.extensions().get::<Held>() {
            let slot = slot.lock().unwrap_or_else(PoisonError::into_inner).take();
            if let Some(slot) = slot {
                let _ = slot.release().await;
            }
        }
    }
}

const OK: &str =
    r#"{"id":"c","choices":[{"index":0,"message":{"role":"assistant","content":"hi"}}]}"#;

fn gateway(
    provider: respan_gateway_http::HttpTransport,
    slots: &Slots,
    free: Duration,
) -> Result<Gateway, Fault> {
    let admission = Admission {
        slots: slots.clone(),
        paid: Duration::from_secs(5),
        free,
    };
    Gateway::new()
        .provider::<Input, _, _>(
            "openai",
            OpenAiChat::OPENAI,
            respan_gateway_http::connect(
                supervised(move |request| {
                    let client = provider.clone();
                    async move { client.send(request).await }
                }),
                LIMIT,
            ),
        )?
        .client::<Input, _, _>(OpenAiChat::OPENAI)?
        .plugin(admission)
        .target("openai")
}

fn request(label: &'static str, tier: &'static str) -> http::Request<respan_gateway_http::Body> {
    let body = r#"{"model":"gpt-4o-mini","messages":[{"role":"user","content":"hi"}]}"#;
    let mut req = post("/v1/chat/completions", body);
    req.headers_mut()
        .insert("x-tier", HeaderValue::from_static(tier));
    req.headers_mut()
        .insert("x-case-label", HeaderValue::from_static(label));
    req
}

#[tokio::main]
async fn main() -> Result<(), Fault> {
    let openai = FakeOpenAi::start().await;
    openai.answer(Answer::raw(OK));
    let gw = gateway(
        provider_support::openai(&openai.base_url())?,
        &Slots::new(1)?,
        Duration::from_millis(200),
    )?;
    // The only slot is held while this response is unread.
    let (held, _) = serve(&gw, request("a", "paid")).await;
    let (status, headers, _) = read_whole(serve(&gw, request("b", "free")).await.0).await;
    let retry = headers.get(RETRY_AFTER);
    println!("a free request, after 200 ms parked: {status}, retry-after {retry:?}");
    drop(held);
    Ok(())
}

#[cfg(test)]
#[path = "../tests/cases/policies/admission.rs"]
mod tests;
