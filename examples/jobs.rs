//! Use case 8, state that outlives the connection, at its smallest: an
//! idempotent request. The client sends an `Idempotency-Key` header (an
//! operation id in the body works the same). The first request with it
//! runs, and what it sent the client is stored, keyed by tenant and key. A
//! retry, or a duplicate sent while the first runs, gets the stored result
//! or waits for the run in flight: the provider is called once.
//!
//! **What core cannot do:** keep a call running once its client left. A
//! request that ends, a disconnect included, ends its open calls
//! `abandoned`, so a stream cut mid-way cannot be finished and stored. Here
//! a cut run stores nothing and releases its key, so the retry runs again
//! (a second provider call) rather than replaying half an answer. To
//! outlive the connection, run the job as its own request that a task
//! reads to the end (`tokio::spawn` of `Gateway::handle`), and let clients
//! wait on the store. Signed operation ids and durable holds are not built.
//!
//! `cargo run -p respan-gateway-examples --example jobs`

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use respan_gateway_core::futures::stream;
use respan_gateway_core::http::{self, HeaderValue};
use respan_gateway_core::{ClientContext, Fault, Gateway, Outcome, Plugin, Response, async_trait};
use respan_gateway_formats::OpenAiChat;
use respan_gateway_formats::conversation::{Conversation, Reply};
use respan_gateway_testkit::{MockTransport, mock, post, read_whole};
use tokio::sync::watch;

fn locked<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Tenant and idempotency key.
type Key = (String, String);

/// A run's result: the replies its client got.
type Replies = Arc<Vec<Reply>>;

/// What a request with a key does.
enum Claim {
    Run,
    Done(Replies),
}

/// Results by key. End hooks await it: keep it fast or spawn. A durable
/// store is a table keyed by (tenant, key) with a TTL: `claim` is an
/// `INSERT .. ON CONFLICT DO NOTHING` (or Redis `SET NX EX`), waiting
/// polls or subscribes until the row is done, and a result is each reply's
/// bytes (`reply.origin(format)`), decoded again to replay.
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
        match result {
            Some(done) => drop(all.get(key).map(|run| run.send_replace(Some(done)))),
            None => drop(all.remove(key)),
        }
    }
}

/// The key of the run this request is, and the replies it sent.
#[derive(Clone)]
struct Running(Key, Arc<Mutex<Vec<Reply>>>);

/// Runs each keyed request once. Register it after the plugin that
/// resolves the tenant (here `x-tenant`) and first among the rest.
struct Idempotent<S>(Arc<S>);

#[async_trait]
impl<S: Store> Plugin for Idempotent<S> {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let header = |name| cx.head().headers.get(name).and_then(|v| v.to_str().ok());
        let Some(key) = header("idempotency-key").filter(|_| cx.is_kind::<Conversation>()) else {
            return Ok(None);
        };
        let tenant = header("x-tenant").unwrap_or_default();
        let key = (tenant.to_owned(), key.to_owned());
        if let Claim::Done(replies) = self.0.claim(&key).await {
            let replies = Arc::unwrap_or_clone(replies).into_iter().map(Ok);
            return Ok(Some(Response::stream(stream::iter(replies))));
        }
        // In the extensions first, so `on_client_end` releases it whatever follows.
        let sent = Arc::default();
        let running = Running(key, Arc::clone(&sent));
        cx.head_mut().extensions.insert(running);
        cx.filter(move |reply: &mut Reply| {
            locked(&sent).push(reply.clone());
            Ok(())
        })?;
        Ok(None)
    }

    async fn on_client_end(&self, cx: &ClientContext, outcome: &Outcome) {
        let Some(Running(key, sent)) = cx.head().extensions.get::<Running>() else {
            return;
        };
        let success = outcome.head.as_ref().is_some_and(|h| h.status.is_success());
        let ok = success && outcome.end.is_ok();
        let result = ok.then(|| locked(sent).clone().into());
        self.0.finish(key, result).await;
    }
}

const OPENAI: &str = "http://openai.test/v1";

fn gateway(mock: &MockTransport, store: &Arc<Memory>) -> Result<Gateway, Fault> {
    Gateway::new()
        .provider("openai", OpenAiChat, mock.at(OPENAI))?
        .serve(OpenAiChat)?
        .plugin("idempotent", Idempotent(store.clone()))?
        .target("openai")
}

const STREAM: &[&str] = &[
    "data: {\"id\":\"c1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Hello\"}}]}\n\n",
    "data: {\"id\":\"c1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\" world\"},\"finish_reason\":\"stop\"}]}\n\n",
    "data: [DONE]\n\n",
];

fn request(tenant: &'static str, key: &'static str) -> http::Request<respan_gateway_core::Body> {
    let body = r#"{"model":"gpt-x","stream":true,"messages":[{"role":"user","content":"hi"}]}"#;
    let mut req = post("/v1/chat/completions", body);
    let headers = req.headers_mut();
    headers.insert("x-tenant", HeaderValue::from_static(tenant));
    headers.insert("idempotency-key", HeaderValue::from_static(key));
    req
}

async fn send(gw: &Gateway, tenant: &'static str, key: &'static str) -> Result<String, Fault> {
    let (_, _, body) = read_whole(gw.handle(request(tenant, key)).await.0).await;
    gw.idle().await;
    Ok(String::from_utf8_lossy(&body?).into_owned())
}

#[tokio::main]
async fn main() -> Result<(), Fault> {
    let mock = MockTransport::new();
    mock.reply(OPENAI, mock::sse(STREAM));
    let gw = gateway(&mock, &Arc::default())?;
    println!("first:\n{}", send(&gw, "acme", "op-1").await?);
    println!("retry:\n{}", send(&gw, "acme", "op-1").await?);
    println!("provider calls: {}", mock.sent().len());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use respan_gateway_core::http_body_util::BodyExt;

    /// Whether an SSE body holds the whole answer. A replay is written from
    /// the stored replies, so its bytes are the gateway's, not the provider's.
    fn whole(sse: &str) -> bool {
        sse.contains(r#""content":"Hello""#)
            && sse.contains(r#""content":" world""#)
            && sse.ends_with("data: [DONE]\n\n")
    }

    #[tokio::test]
    async fn a_duplicate_waits_for_the_run_in_flight_and_the_provider_is_called_once()
    -> Result<(), Fault> {
        let mock = MockTransport::new();
        mock.reply(OPENAI, mock::sse(STREAM))
            .reply(OPENAI, mock::sse(STREAM));
        let store = Arc::new(Memory::default());
        let gw = gateway(&mock, &store)?;
        // The first is in flight while its body is unread.
        let (first, _) = gw.handle(request("acme", "op-1")).await;
        let duplicate = tokio::spawn({
            let gw = gw.clone();
            async move { send(&gw, "acme", "op-1").await }
        });
        let key = ("acme".to_owned(), "op-1".to_owned());
        let waiting = || locked(&store.0)[&key].receiver_count();
        while waiting() == 0 {
            tokio::task::yield_now().await;
        }
        read_whole(first).await.2?;
        let duplicate = duplicate
            .await
            .map_err(|e| Fault::internal(e.to_string()))??;
        assert!(whole(&duplicate), "{duplicate}");
        let retry = send(&gw, "acme", "op-1").await?;
        assert!(whole(&retry), "a later retry too");
        assert_eq!(mock.sent().len(), 1);
        // Keys are per tenant: another's same key runs.
        send(&gw, "beta", "op-1").await?;
        assert_eq!(mock.sent().len(), 2);
        Ok(())
    }

    #[tokio::test]
    async fn a_run_cut_by_a_disconnect_stores_nothing_and_runs_again() -> Result<(), Fault> {
        let mock = MockTransport::new();
        mock.reply(OPENAI, mock::sse(STREAM))
            .reply(OPENAI, mock::sse(STREAM));
        let gw = gateway(&mock, &Arc::default())?;
        let (resp, outcome) = gw.handle(request("acme", "op-1")).await;
        let mut body = resp.into_body();
        let frame = body.frame().await.transpose()?;
        assert!(frame.is_some(), "the client got the start");
        drop(body);
        let end = outcome.await.end.map_err(|f| f.code().to_owned());
        assert_eq!(end, Err("abandoned".into()), "core cut the call");
        gw.idle().await;

        let retry = send(&gw, "acme", "op-1").await?;
        assert!(whole(&retry), "not half an answer: {retry}");
        assert_eq!(mock.sent().len(), 2, "the cut run cost a second call");
        assert!(whole(&send(&gw, "acme", "op-1").await?), "now it is stored");
        assert_eq!(mock.sent().len(), 2);
        Ok(())
    }
}
