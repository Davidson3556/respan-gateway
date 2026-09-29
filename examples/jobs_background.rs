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
//!   The job's run is marked [`Job`], and a marked request is never started
//!   as a job again, so a job cannot recurse; a tenant marked `NoContent`
//!   (zero retention, as in `logging.rs`) is refused background mode, since
//!   a job's answer must be kept until it is polled. The run is a plain
//!   foreground request: `background` is taken out of its body, or a
//!   provider that honours it would answer `queued` and the job would never
//!   finish.
//! - **Signed ids.** The client never sees a provider's id, on a post or a poll: it gets one that
//!   names the deployment (`resp_{deployment}{key}{tag}`, 38 characters, under
//!   Azure's 64) and is signed together with the tenant (a hash of all its
//!   credentials, `x-api-key` included), so a later `GET` goes to the deployment the id names, with
//!   the provider's own id in its path. A forged id, or another tenant's, is
//!   a 404 before any call. Here the tag is a hash; use an HMAC. An id is
//!   mapped wherever a client puts one: a `previous_response_id` in a body
//!   (which also picks the deployment the chained turn goes to) and every
//!   `/v1/responses/{id}...` path (`GET`, `DELETE`, `/cancel`,
//!   `/input_items`), with the provider's id in what goes out and the
//!   signed one in what comes back, the `previous_response_id` an answer
//!   echoes included. The endpoints under the prefix that are not ids
//!   (`/v1/responses/input_tokens`) pass through to the default deployment.
//!
//! The format is `rest`'s: its input carries the method and path, so what a
//! plugin changes in it, the encoder writes.
//!
//! **Not built:** streaming (`"stream": true`, or `?stream=true`, is refused
//! with 400: mapping ids in an event stream needs an event-aware filter), a
//! durable store (this one is in memory, and forgets
//! nothing), a webhook when a job finishes, holding the tenant's budget
//! for a job for as long as it runs, and ids of a job still running: the
//! provider has not named it yet, so its `/cancel` and a
//! `previous_response_id` of it are refused with 404. Ids in other places
//! of a body (a `conversation`, a tool's `response_id`) and the list
//! endpoints are not mapped either.
//!
//! `cargo run -p respan-gateway-examples --example jobs_background`

use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

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

/// Who a request is for: a hash of all its credentials (a durable store
/// hashes with SHA-256), which the adapter lifted into a [`Credential`]:
/// `Authorization`, `x-api-key`, a key in the query, whatever the tenant
/// sends. Tenants that send none are one.
fn tenant(cx: &ClientContext) -> u64 {
    let mut parts: Vec<String> = Vec::new();
    if let Some(credential) = cx.extensions().get::<Credential>() {
        let headers = credential.headers().iter();
        parts.extend(headers.map(|(name, value)| format!("{name}: {:?}", value.as_bytes())));
        parts.extend(credential.query().map(str::to_owned));
    }
    parts.sort();
    let mut hasher = DefaultHasher::new();
    parts.hash(&mut hasher);
    hasher.finish()
}

/// `resp_` + deployment + 16 hex of the key + 16 hex of the tag, which
/// covers the tenant too, so the id is only good for the tenant it was
/// issued to.
fn sign(deployment: usize, key: u64, tenant: u64) -> String {
    let mut tag = DefaultHasher::new();
    (SECRET, deployment, key, tenant).hash(&mut tag);
    format!("resp_{deployment}{key:016x}{:016x}", tag.finish())
}

/// The deployment a signed id names; `None` if it was not signed here for
/// this tenant.
fn verify(id: &str, tenant: u64) -> Option<usize> {
    let rest = id.strip_prefix("resp_").filter(|r| r.len() == 33)?;
    let deployment: usize = rest[..1].parse().ok()?;
    let key = u64::from_str_radix(&rest[1..17], 16).ok()?;
    (deployment < DEPLOYMENTS.len() && sign(deployment, key, tenant) == id).then_some(deployment)
}

/// What the plugin knows of a background job.
#[derive(Clone)]
enum Entry {
    Running,
    /// The job's answer, as the client would have got it.
    Done(Bytes),
}

/// Marks the gateway's own run of a job, with its id. A request that
/// carries it is never started as a job: re-entry cannot recurse.
#[derive(Clone)]
struct Job(String);

/// Marks a zero-retention request; your tenancy plugin sets it.
#[derive(Clone)]
struct NoContent;

#[derive(Default)]
struct Shared {
    /// A durable store (SQL, Redis) with a TTL in production.
    entries: Mutex<HashMap<String, Entry>>,
    /// The provider's id for each signed one, of a job once it has answered.
    natives: Mutex<HashMap<String, String>>,
    keys: AtomicU64,
}

#[derive(Clone, Default)]
struct Jobs(Arc<Shared>);

/// A response the plugin answers itself.
fn answer(body: impl Into<Bytes>) -> Option<Response> {
    let body = Got::<Rest>::new(body.into());
    Some(Response::stream(stream::iter([Ok(body)])))
}

/// The Responses endpoints under `/v1/responses/` that are not ids.
const ENDPOINTS: [&str; 1] = ["input_tokens"];

fn not_found() -> Fault {
    Fault::not_found("no such response")
}

/// The signed id at the start of a path `/v1/responses/{id}...`, and the rest
/// of the path (`/cancel`, `?background=true`, or nothing).
fn split(path: &str) -> Option<(&str, &str)> {
    let path = path.strip_prefix("/v1/responses/")?;
    let (id, rest) = path.split_at(path.find(['/', '?']).unwrap_or(path.len()));
    (!ENDPOINTS.contains(&id)).then_some((id, rest))
}

/// Whether a request asks for a streamed answer, in its body or its query.
fn streams(cx: &ClientContext, body: &Value) -> bool {
    let asked = cx.extensions().get::<Asked>();
    let query = asked
        .and_then(|asked| asked.uri.query())
        .unwrap_or_default();
    body["stream"] == true || query.split('&').any(|pair| pair == "stream=true")
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

impl Jobs {
    /// `got` with the signed `id` for the provider's, which is returned, and
    /// the signed id for the `previous_response_id` it echoes.
    fn show(&self, got: &mut Got<Rest>, id: &str) -> Result<Option<String>, Fault> {
        let mut body: Value = serde_json::from_slice(&got.0)
            .map_err(|_| Fault::upstream("the provider's answer is not JSON"))?;
        let native = body["id"].as_str().map(str::to_owned);
        if native.is_some() {
            body["id"] = id.into();
        }
        if let Some(previous) = body["previous_response_id"].as_str() {
            let natives = lock(&self.0.natives);
            let signed = natives.iter().find(|(_, native)| *native == previous);
            if let Some((signed, _)) = signed {
                body["previous_response_id"] = signed.as_str().into();
            }
        }
        *got = Got::new(body.to_string());
        Ok(native)
    }

    /// The provider's id for a signed one, if it has named it.
    fn native(&self, id: &str) -> Option<String> {
        lock(&self.0.natives).get(id).cloned()
    }

    /// A `previous_response_id` in `body` goes to the provider as its own id,
    /// and the request to the deployment that id lives at, which is returned.
    /// An id that is not the tenant's, or not named yet, is a 404.
    fn chain(&self, cx: &mut ClientContext, body: &Value) -> Result<Option<usize>, Fault> {
        let Some(signed) = body["previous_response_id"].as_str() else {
            return Ok(None);
        };
        let deployment = verify(signed, tenant(cx)).ok_or_else(not_found)?;
        let native = self.native(signed).ok_or_else(not_found)?;
        let mut body = body.clone();
        body["previous_response_id"] = native.into();
        let body = Bytes::from(body.to_string());
        cx.filter(move |ask: &mut Ask| {
            ask.body.clone_from(&body);
            Ok(())
        })?;
        Ok(Some(deployment))
    }

    /// A request about an id (`GET` it, `DELETE` it, `/cancel`, `/input_items`):
    /// a job's state is answered from the store, anything else goes to the
    /// deployment the id names, with the provider's id in its path.
    fn route(
        &self,
        cx: &mut ClientContext,
        ask: &Ask,
        id: &str,
        rest: &str,
    ) -> Result<Option<Response>, Fault> {
        let deployment = verify(id, tenant(cx)).ok_or_else(not_found)?;
        let job = lock(&self.0.entries).get(id).cloned();
        if let (Some(job), true) = (job, ask.method == Method::GET && rest.is_empty()) {
            return Ok(match job {
                Entry::Done(body) => answer(body),
                Entry::Running => answer(json!({ "id": id, "status": "in_progress" }).to_string()),
            });
        }
        let native = self.native(id).ok_or_else(not_found)?;
        cx.set_target(DEPLOYMENTS[deployment]);
        let path = format!("/v1/responses/{native}{rest}");
        cx.filter(move |ask: &mut Ask| {
            ask.path.clone_from(&path);
            Ok(())
        })?;
        let id = id.to_owned();
        let jobs = self.clone();
        cx.filter(move |got: &mut Got<Rest>| jobs.show(got, &id).map(drop))?;
        Ok(None)
    }

    /// A relayed request: the client sees the signed id for the provider's,
    /// which is kept.
    fn relay(
        &self,
        cx: &mut ClientContext,
        id: String,
        deployment: usize,
    ) -> Result<Option<Response>, Fault> {
        cx.set_target(DEPLOYMENTS[deployment]);
        let jobs = self.clone();
        cx.filter(move |got: &mut Got<Rest>| {
            if let Some(native) = jobs.show(got, &id)? {
                lock(&jobs.0.natives).insert(id.clone(), native);
            }
            Ok(())
        })?;
        Ok(None)
    }

    /// A background job: answer now; run it as a foreground request of the
    /// gateway's own, which is `body` without `background`.
    fn start(
        &self,
        cx: &ClientContext,
        mut body: Value,
        id: String,
    ) -> Result<Option<Response>, Fault> {
        if cx.extensions().get::<NoContent>().is_some() {
            return Err(Fault::bad_request(
                "background mode keeps output: not for this tenant",
            ));
        }
        if let Some(fields) = body.as_object_mut() {
            fields.remove("background");
        }
        // Moved into the job, not kept in the plugin: the gateway holds it.
        let gateway = cx.gateway();
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
            .body(full(body.to_string()))
            .map_err(built_wrong)?;
        req.extensions_mut().insert(extensions);
        lock(&self.0.entries).insert(id.clone(), Entry::Running);
        let jobs = self.clone();
        let queued = json!({ "id": id, "status": "queued" }).to_string();
        // Called here, not in the task: the request counts for `idle` from
        // this call.
        let served = respan_gateway_http::serve(&gateway, req);
        tokio::spawn(async move {
            let (response, ended) = served.await;
            let body = response.into_body().collect().await.map(|b| b.to_bytes());
            let done = match (body, ended.await) {
                (Ok(body), Ok(())) => Entry::Done(body),
                (_, Err(fault)) | (Err(fault), _) => {
                    let failed = json!({ "id": id, "status": "failed", "error": fault.message() });
                    Entry::Done(failed.to_string().into())
                }
            };
            lock(&jobs.0.entries).insert(id, done);
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
        let body: Value = serde_json::from_slice(&ask.body).unwrap_or_default();
        // The job's own run, re-entered: route it to the deployment its id names.
        if let Some(Job(id)) = cx.extensions().get::<Job>().cloned() {
            let deployment =
                verify(&id, tenant(cx)).ok_or_else(|| Fault::internal("bad job id"))?;
            self.chain(cx, &body)?;
            return self.relay(cx, id, deployment);
        }
        if streams(cx, &body) {
            return Err(Fault::bad_request(
                "streaming is not supported by this example",
            ));
        }
        if let Some((id, rest)) = split(&ask.path) {
            return self.route(cx, &ask, id, rest);
        }
        let deployment = self.chain(cx, &body)?.unwrap_or(DEFAULT);
        let key = self.0.keys.fetch_add(1, Ordering::Relaxed) + 1;
        let id = sign(deployment, key, tenant(cx));
        match body["background"] == true {
            true => self.start(cx, body, id),
            false => self.relay(cx, id, deployment),
        }
    }
}

const EAST: &str = "http://east.test";
const WEST: &str = "http://west.test";

fn gateway(east: impl Transport, west: impl Transport, jobs: &Jobs) -> Result<Gateway, Fault> {
    Gateway::new()
        .client(RESPONSES)?
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
