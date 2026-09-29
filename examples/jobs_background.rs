//! Use case 8, work that outlives its client, and ids that name a place:
//! a Responses-style API (`POST /v1/responses`, `GET /v1/responses/{id}`)
//! with two things `jobs.rs` (idempotency) does not have.
//!
//! - **Background jobs.** A request with `"background": true` is answered at
//!   once with a signed id. The plugin runs the job as a request of its own,
//!   through the gateway's own `serve` in a spawned task, so it outlives the
//!   client: every later plugin runs over it again, with the client's
//!   [`Credential`], and the provider gets the tenant's key. A poll is
//!   answered from the plugin's store: `in_progress`, then the finished body.
//!   `Gateway::idle` waits for the detached run, so a shutdown can drain it.
//! - **Signed ids.** The client never sees a provider's id: it gets one that
//!   names the deployment (`resp_{deployment}{key}{tag}`, 38 characters, under
//!   Azure's 64) and is signed, so a later `GET` goes to the deployment the id
//!   names, with the provider's own id in its path. A forged id is a 404
//!   before any call. Here the tag is a hash; use an HMAC.
//!
//! The format is `rest`'s: its input carries the method and path, so what a
//! plugin changes in it, the encoder writes.
//!
//! **Not built:** a durable store (this one is in memory, and forgets
//! nothing), a webhook when a job finishes, and holding the tenant's budget
//! for a job for as long as it runs.
//!
//! **Friction:** a plugin has no handle to its gateway. `Jobs::attach` is
//! given it once the last builder call is made (a changed gateway is a new
//! one), and the plugin and its gateway then hold each other.
//!
//! `cargo run -p respan-gateway-examples --example jobs_background`

use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError};

use respan_gateway_core::futures::stream;
use respan_gateway_core::{
    ClientContext, Extensions, Fault, Gateway, Plugin, Response, async_trait,
};
use respan_gateway_examples::rest::{Ask, Rest};
use respan_gateway_formats::opaque::Got;
use respan_gateway_http::bytes::Bytes;
use respan_gateway_http::http::{self, Method};
use respan_gateway_http::http_body_util::BodyExt;
use respan_gateway_http::{Asked, Body, Credential, Transport, built_wrong, full};
use respan_gateway_testkit::{MockTransport, mock, read_whole, serve};
use serde_json::{Value, json};

/// The Responses API as a format: its input is the request as it came, so
/// a plugin can read its method and body and rewrite its path.
const RESPONSES: Rest = Rest::new("responses", "/v1/responses");

/// The deployments a signed id can name, by the provider name.
const DEPLOYMENTS: [&str; 2] = ["east", "west"];
/// The signing secret; a real one is random, and the tag an HMAC-SHA256.
const SECRET: u64 = 0x5eed;
/// Where a request that names no deployment goes; your routing decides.
const DEFAULT: usize = 1;

/// `resp_` + deployment + 16 hex of the key + 16 hex of the tag.
fn sign(deployment: usize, key: u64) -> String {
    let mut tag = DefaultHasher::new();
    (SECRET, deployment, key).hash(&mut tag);
    format!("resp_{deployment}{key:016x}{:016x}", tag.finish())
}

/// The deployment a signed id names; `None` if it was not signed here.
fn verify(id: &str) -> Option<usize> {
    let rest = id.strip_prefix("resp_").filter(|r| r.len() == 33)?;
    let deployment: usize = rest[..1].parse().ok()?;
    let key = u64::from_str_radix(&rest[1..17], 16).ok()?;
    (deployment < DEPLOYMENTS.len() && sign(deployment, key) == id).then_some(deployment)
}

/// What the plugin knows of an id it issued.
#[derive(Clone)]
enum Entry {
    /// A relayed response: the provider's id.
    Native(String),
    /// A background job, running.
    Running,
    /// A background job's answer, as the client would have got it.
    Done(Bytes),
}

/// Marks the gateway's own run of a job, with its id.
#[derive(Clone)]
struct Job(String);

#[derive(Default)]
struct Shared {
    /// The plugin's own gateway, set once it is built.
    gateway: OnceLock<Gateway>,
    /// A durable store (SQL, Redis) with a TTL in production.
    entries: Mutex<HashMap<String, Entry>>,
    keys: AtomicU64,
}

#[derive(Clone, Default)]
struct Jobs(Arc<Shared>);

/// A response the plugin answers itself.
fn answer(body: impl Into<Bytes>) -> Option<Response> {
    let body = Got::<Rest>::new(body.into());
    Some(Response::stream(stream::iter([Ok(body)])))
}

fn not_found() -> Fault {
    Fault::not_found("no such response")
}

impl Jobs {
    /// Give the plugin its gateway, once the last builder call is made.
    fn attach(&self, gateway: &Gateway) {
        let _ = self.0.gateway.set(gateway.clone());
    }

    fn entries(&self) -> MutexGuard<'_, HashMap<String, Entry>> {
        self.0
            .entries
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// The job's own run, re-entered: route it to the deployment its id names.
    fn rerun(cx: &mut ClientContext, Job(id): &Job) -> Result<Option<Response>, Fault> {
        let deployment = verify(id).ok_or_else(|| Fault::internal("a job's own id is bad"))?;
        cx.set_target(DEPLOYMENTS[deployment]);
        Ok(None)
    }

    /// A poll: a job's state, or the response at the deployment the id names.
    fn poll(&self, cx: &mut ClientContext, path: &str) -> Result<Option<Response>, Fault> {
        let id = path.rsplit('/').next().unwrap_or_default();
        let deployment = verify(id).ok_or_else(not_found)?;
        match self.entries().get(id).cloned().ok_or_else(not_found)? {
            Entry::Done(body) => Ok(answer(body)),
            Entry::Running => Ok(answer(
                json!({ "id": id, "status": "in_progress" }).to_string(),
            )),
            Entry::Native(native) => {
                cx.set_target(DEPLOYMENTS[deployment]);
                let path = format!("/v1/responses/{native}");
                cx.filter(move |ask: &mut Ask| Ok(ask.path.clone_from(&path)))?;
                Ok(None)
            }
        }
    }

    /// A relayed request: the client sees the signed id for the provider's.
    fn relay(
        &self,
        cx: &mut ClientContext,
        id: String,
        deployment: usize,
    ) -> Result<Option<Response>, Fault> {
        cx.set_target(DEPLOYMENTS[deployment]);
        let shared = self.clone();
        cx.filter(move |got: &mut Got<Rest>| {
            let mut body: Value = serde_json::from_slice(&got.0)
                .map_err(|_| Fault::upstream("the provider's answer is not JSON"))?;
            if let Some(native) = body["id"].as_str() {
                shared
                    .entries()
                    .insert(id.clone(), Entry::Native(native.to_owned()));
                body["id"] = id.clone().into();
                *got = Got::new(body.to_string());
            }
            Ok(())
        })?;
        Ok(None)
    }

    /// A background job: answer now; run it as a request of the gateway's own.
    fn start(&self, cx: &ClientContext, ask: &Ask, id: String) -> Result<Option<Response>, Fault> {
        let gateway = self.0.gateway.get().cloned();
        let gateway = gateway.ok_or_else(|| Fault::internal("Jobs::attach was not called"))?;
        let mut extensions = Extensions::new();
        extensions.insert(Job(id.clone()));
        // The job is the tenant's: it runs with the tenant's key.
        if let Some(credential) = cx.extensions().get::<Credential>() {
            extensions.insert(credential.clone());
        }
        let uri = cx
            .extensions()
            .get::<Asked>()
            .map_or_else(|| "/v1/responses".into(), |a| a.uri.to_string());
        let mut req = http::Request::post(uri)
            .body(full(ask.body.clone()))
            .map_err(built_wrong)?;
        req.extensions_mut().insert(extensions);
        self.entries().insert(id.clone(), Entry::Running);
        let jobs = self.clone();
        let queued = json!({ "id": id, "status": "queued" }).to_string();
        tokio::spawn(async move {
            let (response, ended) = respan_gateway_http::serve(&gateway, req).await;
            let body = response.into_body().collect().await.map(|b| b.to_bytes());
            let done = match (body, ended.await) {
                (Ok(body), Ok(())) => Entry::Done(body),
                (_, Err(fault)) | (Err(fault), _) => {
                    let failed = json!({ "id": id, "status": "failed", "error": fault.message() });
                    Entry::Done(failed.to_string().into())
                }
            };
            jobs.entries().insert(id, done);
        });
        Ok(answer(queued))
    }
}

#[async_trait]
impl Plugin for Jobs {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let Ok(ask) = cx.input::<Ask>().cloned() else {
            return Ok(None);
        };
        if let Some(job) = cx.extensions().get::<Job>().cloned() {
            return Self::rerun(cx, &job);
        }
        if ask.method == Method::GET {
            return self.poll(cx, &ask.path);
        }
        let key = self.0.keys.fetch_add(1, Ordering::Relaxed) + 1;
        let id = sign(DEFAULT, key);
        let background = String::from_utf8_lossy(&ask.body).contains(r#""background":true"#);
        match background {
            true => self.start(cx, &ask, id),
            false => self.relay(cx, id, DEFAULT),
        }
    }
}

const EAST: &str = "http://east.test";
const WEST: &str = "http://west.test";

/// The gateway, before `jobs` is given it: do that after the last builder call.
fn gateway(east: impl Transport, west: impl Transport, jobs: &Jobs) -> Result<Gateway, Fault> {
    Gateway::new()
        .serve(RESPONSES)?
        .provider("east", RESPONSES, east)?
        .provider("west", RESPONSES, west)?
        .plugin("jobs", jobs.clone())
}

fn request(method: &str, path: &str, body: &str) -> http::Request<Body> {
    http::Request::builder()
        .method(method)
        .uri(path)
        .header("authorization", "Bearer tenant-key")
        .body(full(body.to_owned()))
        .expect("a request")
}

/// The JSON a request is answered with.
async fn send(gateway: &Gateway, req: http::Request<Body>) -> Result<Value, Fault> {
    let (_, _, body) = read_whole(serve(gateway, req).await.0).await;
    serde_json::from_slice(&body?).map_err(|_| Fault::internal("an answer that is not JSON"))
}

#[tokio::main]
async fn main() -> Result<(), Fault> {
    let mock = MockTransport::new();
    let done = r#"{"id":"native-1","output":"done"}"#;
    mock.reply(WEST, mock::json(http::StatusCode::OK, done));
    let jobs = Jobs::default();
    let gateway = gateway(mock.at(EAST), mock.at(WEST), &jobs)?;
    jobs.attach(&gateway);

    let queued = send(
        &gateway,
        request("POST", "/v1/responses", r#"{"background":true}"#),
    )
    .await?;
    println!("queued: {queued}");
    let poll = format!(
        "/v1/responses/{}",
        queued["id"].as_str().unwrap_or_default()
    );
    println!(
        "polled: {}",
        send(&gateway, request("GET", &poll, "")).await?
    );
    gateway.idle().await;
    println!(
        "later:  {}",
        send(&gateway, request("GET", &poll, "")).await?
    );
    Ok(())
}

#[cfg(test)]
#[path = "tests/jobs_background.rs"]
mod tests;
