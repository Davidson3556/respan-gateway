//! Use case 8, state that outlives the connection, at its smallest: an
//! idempotent request. The client sends an `Idempotency-Key` header (an
//! operation id in the body works the same). The first request with it
//! runs, and what it sent the client is stored, keyed by tenant, key and a
//! hash of the conversation it asks: the same key with another one runs on its own. A
//! retry, or a duplicate sent while the first runs, gets the stored result
//! or waits for the run in flight: the provider is called once. The tenant
//! is a hash of all its credentials, which the adapter lifted into a
//! [`Credential`], so replaying another tenant's result takes its
//! credential, not just its key. A zero-retention request (marked
//! `NoContent`, as in `logging.rs`) stores nothing: it runs every time.
//!
//! **What core cannot do:** keep a call running once its client left. A
//! request that ends, a disconnect included, ends its open calls
//! `abandoned`, so a stream cut mid-way cannot be finished and stored. Here
//! a cut run stores nothing and releases its key, so the retry runs again
//! (a second provider call) rather than replaying half an answer. To
//! outlive the connection, run the job as its own request that a task
//! reads to the end (`tokio::spawn` of `Gateway::handle`), and let clients
//! wait on the store. `jobs_background` does it, with signed ids.
//!
//! The runnable example uses SQLite (`JOBS_DB`, default `jobs.sqlite3`).
//! Completed replies survive restart. Renewable claims prevent concurrent
//! duplicates; lease loss cancels a live call. Recovery retries use a stable
//! provider Idempotency-Key; remote exactly-once execution requires provider
//! support. Stored results currently have no expiration or tenant purge.
//!
//! `cargo run -p respan-gateway-examples --example jobs`

use parking_lot::Mutex;
use respan_gateway_core::ErrorCode;
use respan_gateway_examples::LIMIT;
use sha2::{Digest, Sha256};
use std::sync::Arc;

use mockllm::FakeOpenAi;
use respan_gateway_conversation::{Input, Output};
use respan_gateway_conversation_http::openai::OpenAiChat;
use respan_gateway_core::futures::{FutureExt, StreamExt, stream, stream::BoxStream};
use respan_gateway_core::{ClientContext, Fault, Gateway, Plugin, Provider, Response, async_trait};
use respan_gateway_http::Body;
use respan_gateway_http::http::{self, HeaderValue, header::AUTHORIZATION};
use respan_gateway_http::{Credential, raw::Original};
#[allow(dead_code)]
#[path = "../support/request.rs"]
mod http_support;
use http_support::{post, read_whole, serve};
#[allow(dead_code)]
#[path = "../support/http.rs"]
mod provider_support;
use tokio::sync::watch;
use tokio::time::Instant;

#[path = "../support/jobs/jobs_store.rs"]
mod persistent;
use persistent::Sqlite;

/// Hashes of the tenant's credential and of the conversation, around the
/// idempotency key. SHA-256 is stable across process restarts.
type Key = (String, String, String);

fn digest(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// Include every credential form, keeping repeated values in their wire order.
fn tenant(cx: &ClientContext) -> String {
    let credential = cx.extensions().get::<Credential>();
    let parts = credential.map(|credential| {
        let mut headers: Vec<_> = credential
            .headers()
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_bytes()))
            .collect();
        headers.sort_by_key(|(name, _)| *name);
        (headers, credential.query().collect::<Vec<_>>())
    });
    digest(&serde_json::to_vec(&parts).expect("credential fields serialize"))
}

/// Marks a zero-retention request; your tenancy plugin sets it.
#[derive(Clone)]
struct NoContent;

/// A run's result: the replies its client got.
type Replies = Arc<Vec<Output>>;

/// What a request with a key does.
enum Claim {
    Run(u64, Option<Instant>),
    Done(Replies),
}

/// Results by key. End hooks await it: keep it fast or spawn. A durable
/// store is a table keyed by `Key` with a TTL: `claim` is an
/// `INSERT .. ON CONFLICT DO NOTHING` (or Redis `SET NX EX`), waiting
/// polls or subscribes until the row is done, and a result is each reply's
/// events, which a format encodes and decodes again to replay.
#[async_trait]
trait Store: Send + Sync + 'static {
    /// `Run` for the first request with `key`; else its result, waiting
    /// while another request runs it.
    async fn claim(&self, key: &Key) -> Result<Claim, Fault>;
    /// Keep a run's result, or with `None` release `key` to the next request.
    async fn finish(&self, key: &Key, owner: u64, result: Option<Replies>) -> Result<(), Fault>;
    /// Durable stores renew a live claim; memory stores need no lease.
    async fn renew(&self, _key: &Key, _owner: u64) -> Result<Instant, Fault> {
        Err(Fault::internal("this store has no lease"))
    }
    fn heartbeat(&self) -> Option<std::time::Duration> {
        None
    }
}

/// The key of the run this request is, and the replies it sent.
#[derive(Clone)]
struct Running {
    key: Key,
    sent: Arc<Mutex<Vec<Output>>>,
    owner: u64,
    stop: Arc<Mutex<Option<tokio::sync::oneshot::Sender<()>>>>,
    deadline: Option<watch::Receiver<Instant>>,
}

impl Running {
    async fn lost(&self) {
        let Some(mut deadline) = self.deadline.clone() else {
            return std::future::pending().await;
        };
        loop {
            let until = *deadline.borrow_and_update();
            tokio::select! {
                biased;
                _ = tokio::time::sleep_until(until) => return,
                changed = deadline.changed() => if changed.is_err() { return; },
            }
        }
    }

    async fn guard<T>(
        &self,
        work: impl std::future::Future<Output = Result<T, Fault>>,
    ) -> Result<T, Fault> {
        tokio::select! {
            biased;
            _ = self.lost() => Err(Fault::new("the idempotent operation lost its lease").with(ErrorCode("lease_lost".into()))),
            result = work => result,
        }
    }

    fn bounded<T: Send + 'static>(
        &self,
        input: BoxStream<'static, Result<T, Fault>>,
    ) -> BoxStream<'static, Result<T, Fault>> {
        let running = self.clone();
        stream::unfold((input, false), move |(mut input, ended)| {
            let running = running.clone();
            async move {
                if ended {
                    return None;
                }
                match running
                    .guard(async { input.next().await.transpose() })
                    .await
                {
                    Ok(Some(value)) => Some((Ok(value), (input, false))),
                    Ok(None) => None,
                    Err(fault) => Some((Err(fault), (input, true))),
                }
            }
        })
        .boxed()
    }
}

/// A stable upstream key handles crash ambiguity when the provider supports
type HttpReply = (
    http::Response<respan_gateway_http::Body>,
    Option<respan_gateway_http::Completion>,
);

/// idempotency. Losing a lease stops local work; it cannot undo remote compute.
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
    move |mut call| {
        let send = send.clone();
        async move {
            let Some(running) = call
                .extensions()
                .get::<respan_gateway_core::Extensions>()
                .and_then(|extensions| extensions.get::<Running>())
                .cloned()
            else {
                return send(call).await;
            };
            let key = digest(&serde_json::to_vec(&running.key).expect("key serializes"));
            call.headers_mut().insert(
                "idempotency-key",
                HeaderValue::from_str(&key)
                    .map_err(|_| Fault::internal("invalid operation key"))?,
            );
            let call =
                call.map(|body| respan_gateway_http::Body::stream(running.bounded(body.boxed())));
            let (response, completion) = running.guard(send(call)).await?;
            let response = response
                .map(|body| respan_gateway_http::Body::stream(running.bounded(body.boxed())));
            let completion =
                completion.map(|writing| async move { running.guard(writing).await }.boxed());
            Ok((response, completion))
        }
        .boxed()
    }
}

/// Runs each keyed request once. Register it after the plugin that marks
/// zero-retention requests, and before any that edits the request or takes
/// the [`Credential`] off.
struct Idempotent<S>(Arc<S>);

#[async_trait]
impl<S: Store> Plugin for Idempotent<S> {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let asked = cx.extensions().get::<Original>().and_then(Original::get);
        let header = asked.and_then(|a| a.headers().get("idempotency-key")?.to_str().ok());
        let stores = cx.is_kind::<Input>() && cx.extensions().get::<NoContent>().is_none();
        let Some(key) = header.filter(|_| stores) else {
            return Ok(None);
        };
        // The body as the client sent it, which the HTTP adapter publishes:
        // stable across restarts and versions, unlike any in-memory form.
        let wire = cx.extensions().get::<respan_gateway_http::raw::Original>();
        let body = wire
            .and_then(|w| w.get())
            .map_or(&[][..], |r| &r.body()[..]);
        let key = (tenant(cx), key.to_owned(), digest(body));
        let (owner, deadline) = match self.0.claim(&key).await? {
            Claim::Done(replies) => {
                let replies = Arc::unwrap_or_clone(replies).into_iter().map(Ok);
                return Ok(Some(Response::stream(stream::iter(replies))));
            }
            Claim::Run(owner, deadline) => (owner, deadline),
        };
        // In the extensions first, so `on_client_end` releases it whatever follows.
        let sent = Arc::default();
        let (stop, mut stopped) = tokio::sync::oneshot::channel();
        let lease = deadline.map(watch::channel);
        let running = Running {
            key: key.clone(),
            sent: Arc::clone(&sent),
            owner,
            stop: Arc::new(Mutex::new(Some(stop))),
            deadline: lease.as_ref().map(|(_, receiver)| receiver.clone()),
        };
        cx.extensions_mut().insert(running.clone());
        cx.wrap(move |outputs: respan_gateway_core::EventStream<Output>| {
            running.bounded(outputs.boxed())
        })?;
        if let (Some(interval), Some((renewed, _))) = (self.0.heartbeat(), lease) {
            let store = self.0.clone();
            tokio::spawn(async move {
                loop {
                    let previous = *renewed.borrow();
                    let next = async {
                        tokio::time::sleep(interval).await;
                        store.renew(&key, owner).await
                    };
                    tokio::select! {
                        biased;
                        _ = &mut stopped => return,
                        result = tokio::time::timeout_at(previous, next) => match result {
                            Ok(Ok(until)) if until > Instant::now() => { renewed.send_replace(until); },
                            _ => break,
                        },
                    }
                }
                renewed.send_replace(Instant::now());
            });
        }
        cx.filter(move |reply: &mut Output| {
            sent.lock().push(reply.clone());
            Ok(())
        })?;
        Ok(None)
    }

    async fn on_client_end(&self, cx: &ClientContext, end: &Result<(), Fault>) {
        let Some(running) = cx.extensions().get::<Running>() else {
            return;
        };
        let live = running
            .deadline
            .as_ref()
            .is_none_or(|deadline| *deadline.borrow() > Instant::now());
        let result = (end.is_ok() && live).then(|| running.sent.lock().clone().into());
        running.stop.lock().take();
        let _ = self.0.finish(&running.key, running.owner, result).await;
    }
}

fn gateway<S: Store>(
    transport: respan_gateway_http_transport::HttpTransport,
    store: &Arc<S>,
) -> Result<Gateway, Fault> {
    Gateway::new()
        .provider(Provider::new(
            "openai",
            OpenAiChat,
            respan_gateway_http::connect(
                supervised(move |request| {
                    let client = transport.clone();
                    async move { client.send(request).await }
                }),
                LIMIT,
            ),
        ))?
        .client::<Input, _, _>(OpenAiChat)?
        .plugin(Idempotent(store.clone()))
        .target("openai")
}

const STREAM: &[&str] = &[
    "data: {\"id\":\"c1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Hello\"}}]}\n\n",
    "data: {\"id\":\"c1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\" world\"},\"finish_reason\":\"stop\"}]}\n\n",
    "data: [DONE]\n\n",
];

const BODY: &str =
    r#"{"model":"gpt-6-luna","stream":true,"messages":[{"role":"user","content":"hi"}]}"#;

/// A request with idempotency key `key` from the tenant whose credential is `token`.
fn keyed(token: &'static str, key: &'static str, body: &'static str) -> http::Request<Body> {
    let mut req = post("/v1/chat/completions", body);
    let headers = req.headers_mut();
    headers.insert(AUTHORIZATION, HeaderValue::from_static(token));
    headers.insert("idempotency-key", HeaderValue::from_static(key));
    req
}

async fn send(gw: &Gateway, req: http::Request<Body>) -> Result<String, Fault> {
    let (_, _, body) = read_whole(serve(gw, req).await.0).await;
    gw.idle().await;
    Ok(String::from_utf8_lossy(&body?).into_owned())
}

const ACME: &str = "Bearer acme-key";

#[tokio::main]
async fn main() -> Result<(), Fault> {
    let provider = FakeOpenAi::start().await;
    provider.answer(provider_support::sse(STREAM));
    let path = std::env::var("JOBS_DB").unwrap_or_else(|_| "jobs.sqlite3".into());
    let store = Arc::new(Sqlite::open(path)?);
    let gw = gateway(provider_support::openai(&provider.base_url())?, &store)?;
    println!("first:\n{}", send(&gw, keyed(ACME, "op-1", BODY)).await?);
    println!("retry:\n{}", send(&gw, keyed(ACME, "op-1", BODY)).await?);
    println!("provider calls: {}", provider.requests().len());
    Ok(())
}
