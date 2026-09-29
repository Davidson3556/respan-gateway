//! Use case 8, state that outlives the connection, at its smallest: an
//! idempotent request. The client sends an `Idempotency-Key` header (an
//! operation id in the body works the same). The first request with it
//! runs, and what it sent the client is stored, keyed by tenant, key and a
//! hash of the body: the same key with another body runs on its own. A
//! retry, or a duplicate sent while the first runs, gets the stored result
//! or waits for the run in flight: the provider is called once. The tenant
//! is a hash of `Authorization`, which the adapter lifted into a
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
//! `cargo run -p respan-gateway-examples --example jobs`

use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use respan_gateway_core::futures::stream;
use respan_gateway_core::{ClientContext, Fault, Gateway, Plugin, Response, async_trait};
use respan_gateway_formats::OpenAiChat;
use respan_gateway_formats::conversation::{Conversation, Reply};
use respan_gateway_http::Body;
use respan_gateway_http::http::{self, HeaderValue, header::AUTHORIZATION};
use respan_gateway_http::{Asked, Credential};
use respan_gateway_testkit::{MockTransport, mock, post, read_whole, serve};
use tokio::sync::watch;

fn locked<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Hashes of the tenant's credential and of the body, around the
/// idempotency key. A durable store hashes with SHA-256.
type Key = (u64, String, u64);

fn hash(value: impl Hash) -> u64 {
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

/// Marks a zero-retention request; your tenancy plugin sets it.
#[derive(Clone)]
struct NoContent;

/// A run's result: the replies its client got.
type Replies = Arc<Vec<Reply>>;

/// What a request with a key does.
enum Claim {
    Run,
    Done(Replies),
}

/// Results by key. End hooks await it: keep it fast or spawn. A durable
/// store is a table keyed by `Key` with a TTL: `claim` is an
/// `INSERT .. ON CONFLICT DO NOTHING` (or Redis `SET NX EX`), waiting
/// polls or subscribes until the row is done, and a result is each reply's
/// bytes (`reply.came_in(format)`), decoded again to replay.
#[async_trait]
trait Store: Send + Sync + 'static {
    /// `Run` for the first request with `key`; else its result, waiting
    /// while another request runs it.
    async fn claim(&self, key: &Key) -> Claim;
    /// Keep a run's result, or with `None` release `key` to the next request.
    async fn finish(&self, key: &Key, result: Option<Replies>);
}

/// In memory: each key's result, `None` while it runs. Keeps every result
/// forever; a real store expires them.
#[derive(Default)]
struct Memory(Mutex<HashMap<Key, watch::Sender<Option<Replies>>>>);

#[async_trait]
impl Store for Memory {
    async fn claim(&self, key: &Key) -> Claim {
        loop {
            let mut result = {
                let mut all = locked(&self.0);
                match all.get(key) {
                    Some(running) => running.subscribe(),
                    None => {
                        all.insert(key.clone(), watch::channel(None).0);
                        return Claim::Run;
                    }
                }
            };
            // `Err` if the run was released: claim again.
            if let Ok(Some(done)) = result.wait_for(Option::is_some).await.map(|r| r.clone()) {
                return Claim::Done(done);
            }
        }
    }

    async fn finish(&self, key: &Key, result: Option<Replies>) {
        let mut all = locked(&self.0);
        if let Some(done) = result {
            if let Some(run) = all.get(key) {
                run.send_replace(Some(done));
            }
        } else {
            all.remove(key);
        }
    }
}

/// The key of the run this request is, and the replies it sent.
#[derive(Clone)]
struct Running(Key, Arc<Mutex<Vec<Reply>>>);

/// Runs each keyed request once. Register it after the plugin that marks
/// zero-retention requests, and before any that edits the request or takes
/// the [`Credential`] off.
struct Idempotent<S>(Arc<S>);

#[async_trait]
impl<S: Store> Plugin for Idempotent<S> {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let asked = cx.extensions().get::<Asked>();
        let header = asked.and_then(|a| a.headers.get("idempotency-key")?.to_str().ok());
        let stores = cx.is_kind::<Conversation>() && cx.extensions().get::<NoContent>().is_none();
        let Some(key) = header.filter(|_| stores) else {
            return Ok(None);
        };
        let tenant = cx.extensions().get::<Credential>();
        // The body as it came: the bytes the client's conversation came in.
        let chat = cx.input::<Conversation>()?;
        let body = chat.came_in(cx.format()).map_or(&[][..], |o| &o.bytes);
        let key = (
            hash(tenant.and_then(|c| c.get(AUTHORIZATION.as_str()))),
            key.to_owned(),
            hash(body),
        );
        if let Claim::Done(replies) = self.0.claim(&key).await {
            let replies = Arc::unwrap_or_clone(replies).into_iter().map(Ok);
            return Ok(Some(Response::stream(stream::iter(replies))));
        }
        // In the extensions first, so `on_client_end` releases it whatever follows.
        let sent = Arc::default();
        let running = Running(key, Arc::clone(&sent));
        cx.extensions_mut().insert(running);
        cx.filter(move |reply: &mut Reply| {
            locked(&sent).push(reply.clone());
            Ok(())
        })?;
        Ok(None)
    }

    async fn on_client_end(&self, cx: &ClientContext, end: &Result<(), Fault>) {
        let Some(Running(key, sent)) = cx.extensions().get::<Running>() else {
            return;
        };
        let result = end.is_ok().then(|| locked(sent).clone().into());
        self.0.finish(key, result).await;
    }
}

const OPENAI: &str = "http://openai.test/v1";

fn gateway(mock: &MockTransport, store: &Arc<Memory>) -> Result<Gateway, Fault> {
    Gateway::new()
        .provider("openai", OpenAiChat, mock.at(OPENAI))?
        .client(OpenAiChat)?
        .plugin("idempotent", Idempotent(store.clone()))?
        .target("openai")
}

const STREAM: &[&str] = &[
    "data: {\"id\":\"c1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Hello\"}}]}\n\n",
    "data: {\"id\":\"c1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\" world\"},\"finish_reason\":\"stop\"}]}\n\n",
    "data: [DONE]\n\n",
];

const BODY: &str = r#"{"model":"gpt-x","stream":true,"messages":[{"role":"user","content":"hi"}]}"#;

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
    let mock = MockTransport::new();
    mock.reply(OPENAI, mock::sse(STREAM));
    let gw = gateway(&mock, &Arc::default())?;
    println!("first:\n{}", send(&gw, keyed(ACME, "op-1", BODY)).await?);
    println!("retry:\n{}", send(&gw, keyed(ACME, "op-1", BODY)).await?);
    println!("provider calls: {}", mock.sent().len());
    Ok(())
}

#[cfg(test)]
#[path = "tests/jobs.rs"]
mod tests;
