//! Use case 8: durable Responses jobs and tenant-bound ids.
//!
//! `background: true` persists a foreground request in SQLite and answers
//! with a signed id. A detached request runs through every gateway plugin;
//! `Gateway::idle` waits for its result, settlement and webhook attempt.
//! Requests marked `NoContent` cannot start a stored job.
//!
//! Jobs, native-id mappings, credentials, signing keys, budget holds and
//! the webhook outbox survive restart. `resume` claims expired worker leases
//! and retries with the same provider Idempotency-Key. Exactly-once remote
//! execution requires the provider to honor that key. Call `resume` again
//! after a live lease expires; a supervisor can poll it periodically.
//!
//! Each job caps output tokens and reserves input bytes plus that cap in
//! a durable tenant budget. Completion charges reported tokens, or the
//! reservation when usage is unknown. A running job can be cancelled before
//! its provider id arrives. Configure tenant budgets with `budget`.
//!
//! `webhook` names a completion URL. Completion and the outbox are committed
//! together; delivery retries through `deliver_webhooks` or `resume`.
//! Delivery is at least once, with the signed job id as Idempotency-Key.
//!
//! Signed ids use a persisted random HMAC key, bind all tenant credentials,
//! and fit Azure's 64-character limit. Polls, DELETE, /cancel, /input_items
//! and previous_response_id are translated to the owning deployment's id.
//!
//! Still not implemented: streamed Responses, chaining a job that has not
//! received a provider id, ids elsewhere in arbitrary bodies, list endpoints,
//! and tenant-wide deletion. Stored results have no expiration in this example.
//! Set JOBS_DB to choose the SQLite file.
//!
//! `cargo run -p respan-gateway-examples --example jobs_background`

use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use respan_gateway_examples::rest::{Ask, Rest};
use respan_gateway_examples::store::{
    Database,
    rusqlite::{OptionalExtension, Transaction},
    sql,
};
use respan_gateway_http::bytes::{Bytes, BytesMut};
use respan_gateway_http::formats::opaque::Got;
use respan_gateway_http::http::{self, HeaderMap, Method, StatusCode, Uri};
use respan_gateway_http::http_body_util::BodyExt;
use respan_gateway_http::{Asked, Body, Credential, HttpClient, built_wrong, full};
use respan_gateway_sdk::futures::{TryStreamExt, future, stream};
use respan_gateway_sdk::{
    ClientContext, EventStream, Extensions, Fault, Gateway, Plugin, Response, async_trait,
};
#[allow(dead_code)]
#[path = "../tests/support/http.rs"]
mod http_support;
use http_support::{read_whole, serve};
use serde_json::{Value, json};

#[path = "background_worker.rs"]
mod worker;

/// The Responses API as a format: its input is the request as it came, so
/// a plugin can read its method and body and rewrite its path.
const RESPONSES: Rest = Rest::new("responses", "/v1/responses");

/// The deployments a signed id can name, by the provider name.
const DEPLOYMENTS: [&str; 2] = ["east", "west"];
/// Where a request that names no deployment goes; your routing decides.
const DEFAULT: usize = 1;

/// Who a request is for: a hash of all its credentials (a durable store
/// hashes with SHA-256), which the adapter lifted into a [`Credential`]:
/// `Authorization`, `x-api-key`, a key in the query, whatever the tenant
/// sends. Tenants that send none are one.
fn tenant(cx: &ClientContext) -> [u8; 32] {
    identity(cx.extensions().get::<Credential>())
}

fn identity(credential: Option<&Credential>) -> [u8; 32] {
    let mut parts: Vec<String> = Vec::new();
    if let Some(credential) = credential {
        let headers = credential.headers().iter();
        parts.extend(headers.map(|(name, value)| format!("{name}: {:?}", value.as_bytes())));
        parts.extend(credential.query().map(str::to_owned));
    }
    parts.sort();
    Sha256::digest(serde_json::to_vec(&parts).expect("credential parts serialize")).into()
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Signed ids fit within Azure's 64-character limit: 54 characters with
/// a 128-bit HMAC tag and a key allocated by the persistent database.
fn sign(secret: &[u8], deployment: usize, key: u64, tenant: [u8; 32]) -> String {
    let stem = format!("resp_{deployment}{key:016x}");
    let mut mac = Hmac::<Sha256>::new_from_slice(secret).expect("HMAC accepts any key length");
    mac.update(stem.as_bytes());
    mac.update(&tenant);
    let tag = mac.finalize().into_bytes();
    let tag = hex(&tag[..16]);
    stem + &tag
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
struct Job {
    id: String,
    owner: String,
    output: Arc<Mutex<BytesMut>>,
}

/// Marks a zero-retention request; your tenancy plugin sets it.
#[derive(Clone)]
struct NoContent;

/// Where a provider's id is unique: a tenant's key at a deployment.
type Home = ([u8; 32], usize);

struct Shared {
    db: Database,
    active: Mutex<HashMap<String, tokio::task::AbortHandle>>,
}

impl Default for Shared {
    fn default() -> Self {
        Self {
            db: Database::memory().expect("in-memory job database"),
            active: Mutex::default(),
        }
    }
}

#[derive(Clone, Default)]
struct Jobs(Arc<Shared>);

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS response_ids (id TEXT PRIMARY KEY, tenant TEXT NOT NULL, deployment INTEGER NOT NULL, native TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS background_jobs (id TEXT PRIMARY KEY, result BLOB, request TEXT NOT NULL, uri TEXT NOT NULL, headers TEXT NOT NULL,
    tenant TEXT NOT NULL, reserve INTEGER NOT NULL, owner TEXT NOT NULL DEFAULT '', lease INTEGER NOT NULL DEFAULT 0, webhook TEXT);
CREATE TABLE IF NOT EXISTS job_accounts (tenant TEXT PRIMARY KEY, balance INTEGER NOT NULL, held INTEGER NOT NULL DEFAULT 0, spent INTEGER NOT NULL DEFAULT 0);
CREATE TABLE IF NOT EXISTS job_webhooks (id TEXT PRIMARY KEY, url TEXT NOT NULL, body BLOB NOT NULL, delivered INTEGER NOT NULL DEFAULT 0);
CREATE TABLE IF NOT EXISTS job_secrets (secret BLOB NOT NULL);
CREATE TABLE IF NOT EXISTS job_counters (name TEXT PRIMARY KEY, value INTEGER NOT NULL);
INSERT OR IGNORE INTO job_counters VALUES('id',0);
";

/// A response the plugin answers itself.
fn answer(body: impl Into<Bytes>) -> Option<Response> {
    let body = Got::<Rest>::new(body.into());
    Some(Response::stream(stream::iter([Ok(body)])))
}

fn not_found() -> Fault {
    Fault::not_found("no such response")
}

/// The signed id at the start of a path `/v1/responses/{id}...`, and the rest
/// of the path (`/cancel`, `?background=true`, or nothing). Whatever under
/// the prefix is not shaped like one of ours (`input_tokens`, `compact`) is
/// an endpoint of the provider's.
fn split(path: &str) -> Option<(&str, &str)> {
    let path = path.strip_prefix("/v1/responses/")?;
    let (id, rest) = path.split_at(path.find(['/', '?']).unwrap_or(path.len()));
    id.starts_with("resp_").then_some((id, rest))
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
    fn map_output(
        &self,
        cx: &mut ClientContext,
        id: String,
        deployment: usize,
        remember: bool,
    ) -> Result<(), Fault> {
        let (jobs, home) = (self.clone(), (tenant(cx), deployment));
        cx.wrap(move |events: EventStream<Got<Rest>>| {
            stream::once(async move {
                let body = events.try_fold(BytesMut::new(), |mut bytes, chunk| async move {
                    bytes.extend_from_slice(&chunk.0); Ok(bytes)
                }).await?;
                if body.is_empty() { return Ok(None); }
                let mut got = Got::new(body.freeze());
                let native = jobs.show(&mut got, &id, home).await?;
                if remember && let Some(native) = native {
                    jobs.db(move |tx| {
                        tx.execute("INSERT INTO response_ids(id,tenant,deployment,native) VALUES(?1,?2,?3,?4) ON CONFLICT(id) DO UPDATE SET native=excluded.native",(id,hex(&home.0),home.1,native)).map_err(sql)?;
                        Ok(())
                    }).await?;
                }
                Ok(Some(got))
            }).try_filter_map(|body| future::ready(Ok(body)))
        })
    }

    fn open(path: impl AsRef<std::path::Path>) -> Result<Self, Fault> {
        Ok(Self(Arc::new(Shared {
            db: Database::open(path)?,
            active: Mutex::default(),
        })))
    }

    async fn db<T: Send + 'static>(
        &self,
        work: impl FnOnce(&Transaction<'_>) -> Result<T, Fault> + Send + 'static,
    ) -> Result<T, Fault> {
        self.0
            .db
            .transact(move |tx| {
                tx.execute_batch(SCHEMA).map_err(sql)?;
                let exists: bool = tx
                    .query_row("SELECT EXISTS(SELECT 1 FROM job_secrets)", [], |row| {
                        row.get(0)
                    })
                    .map_err(sql)?;
                if !exists {
                    let mut secret = [0u8; 32];
                    getrandom::fill(&mut secret).map_err(|e| Fault::internal(e.to_string()))?;
                    tx.execute("INSERT INTO job_secrets VALUES(?1)", [secret.as_slice()])
                        .map_err(sql)?;
                }
                work(tx)
            })
            .await
    }

    async fn next_id(&self, deployment: usize, tenant: [u8; 32]) -> Result<String, Fault> {
        self.db(move |tx| {
            let key: u64 = tx
                .query_row(
                    "UPDATE job_counters SET value=value+1 WHERE name='id' RETURNING value",
                    [],
                    |row| row.get(0),
                )
                .map_err(sql)?;
            let secret: Vec<u8> = tx
                .query_row("SELECT secret FROM job_secrets", [], |row| row.get(0))
                .map_err(sql)?;
            Ok(sign(&secret, deployment, key, tenant))
        })
        .await
    }

    async fn verify(&self, id: &str, tenant: [u8; 32]) -> Result<Option<usize>, Fault> {
        let Some(rest) = id.strip_prefix("resp_").filter(|r| r.len() == 49) else {
            return Ok(None);
        };
        let Some(deployment) = rest
            .get(..1)
            .and_then(|s| s.parse::<usize>().ok())
            .filter(|d| *d < DEPLOYMENTS.len())
        else {
            return Ok(None);
        };
        let Some(key) = rest
            .get(1..17)
            .and_then(|s| u64::from_str_radix(s, 16).ok())
        else {
            return Ok(None);
        };
        let Some(tag) = rest.get(17..) else {
            return Ok(None);
        };
        let tag: Option<Vec<u8>> = (0..32)
            .step_by(2)
            .map(|at| {
                tag.get(at..at + 2)
                    .and_then(|s| u8::from_str_radix(s, 16).ok())
            })
            .collect();
        let Some(tag) = tag else {
            return Ok(None);
        };
        self.db(move |tx| {
            let secret: Vec<u8> = tx
                .query_row("SELECT secret FROM job_secrets", [], |row| row.get(0))
                .map_err(sql)?;
            let mut mac =
                Hmac::<Sha256>::new_from_slice(&secret).expect("HMAC accepts any key length");
            mac.update(format!("resp_{deployment}{key:016x}").as_bytes());
            mac.update(&tenant);
            Ok(mac
                .verify_truncated_left(&tag)
                .is_ok()
                .then_some(deployment))
        })
        .await
    }

    async fn entry(&self, id: &str) -> Result<Option<Entry>, Fault> {
        let id = id.to_owned();
        self.db(move |tx| {
            let result: Option<Option<Vec<u8>>> = tx
                .query_row(
                    "SELECT result FROM background_jobs WHERE id=?1",
                    [id],
                    |row| row.get(0),
                )
                .optional()
                .map_err(sql)?;
            Ok(result.map(|body| body.map_or(Entry::Running, |body| Entry::Done(body.into()))))
        })
        .await
    }

    /// `got` with the signed `id` for the provider's, which is returned, and
    /// the signed id for the `previous_response_id` it echoes, of the same home.
    async fn show(
        &self,
        got: &mut Got<Rest>,
        id: &str,
        home: Home,
    ) -> Result<Option<String>, Fault> {
        let mut body: Value = serde_json::from_slice(&got.0)
            .map_err(|_| Fault::upstream("the provider's answer is not JSON"))?;
        let event = body["type"]
            .as_str()
            .is_some_and(|kind| kind.starts_with("response."));
        if event && body.get("response_id").is_some() {
            body["response_id"] = id.into();
        }
        let mut native = None;
        let response = if event {
            body.get_mut("response")
        } else {
            Some(&mut body)
        };
        if let Some(response) = response {
            native = response["id"].as_str().map(str::to_owned);
            if native.is_some() {
                response["id"] = id.into();
            }
            if let Some(previous) = response["previous_response_id"].as_str() {
                let previous = previous.to_owned();
                let signed: Option<String> = self.db(move |tx| {
                    tx.query_row("SELECT id FROM response_ids WHERE tenant=?1 AND deployment=?2 AND native=?3 LIMIT 1", (hex(&home.0), home.1, previous), |row| row.get(0)).optional().map_err(sql)
                }).await?;
                if let Some(signed) = signed {
                    response["previous_response_id"] = signed.into();
                }
            }
        }
        *got = Got::new(body.to_string());
        Ok(native)
    }

    /// The provider's id for a signed one, if it has named it.
    async fn native(&self, id: &str) -> Result<Option<String>, Fault> {
        let id = id.to_owned();
        self.db(move |tx| {
            tx.query_row("SELECT native FROM response_ids WHERE id=?1", [id], |row| {
                row.get(0)
            })
            .optional()
            .map_err(sql)
        })
        .await
    }

    /// A `previous_response_id` in `body` goes to the provider as its own id,
    /// and the request to the deployment that id lives at, which is returned.
    /// An id that is not the tenant's, or not named yet, is a 404.
    async fn chain(&self, cx: &mut ClientContext, body: &Value) -> Result<Option<usize>, Fault> {
        let Some(signed) = body["previous_response_id"].as_str() else {
            return Ok(None);
        };
        let deployment = self
            .verify(signed, tenant(cx))
            .await?
            .ok_or_else(not_found)?;
        let native = self.native(signed).await?.ok_or_else(not_found)?;
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
    async fn route(
        &self,
        cx: &mut ClientContext,
        ask: &Ask,
        id: &str,
        rest: &str,
    ) -> Result<Option<Response>, Fault> {
        let deployment = self.verify(id, tenant(cx)).await?.ok_or_else(not_found)?;
        let job = self.entry(id).await?;
        if matches!(job, Some(Entry::Running))
            && (ask.method == Method::DELETE || (ask.method == Method::POST && rest == "/cancel"))
        {
            let cancelled = Bytes::from(json!({"id":id,"status":"cancelled"}).to_string());
            self.finish(id, None, cancelled.clone()).await?;
            if let Some(run) = lock(&self.0.active).remove(id) {
                run.abort();
            }
            self.deliver_webhooks().await?;
            return Ok(answer(cancelled));
        }
        if let (Some(job), true) = (job, ask.method == Method::GET && rest.is_empty()) {
            return Ok(match job {
                Entry::Done(body) => answer(body),
                Entry::Running => answer(json!({ "id": id, "status": "in_progress" }).to_string()),
            });
        }
        let native = self.native(id).await?.ok_or_else(not_found)?;
        cx.set_target(DEPLOYMENTS[deployment]);
        let path = format!("/v1/responses/{native}{rest}");
        cx.filter(move |ask: &mut Ask| {
            ask.path.clone_from(&path);
            Ok(())
        })?;
        self.map_output(cx, id.to_owned(), deployment, false)?;
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
        self.map_output(cx, id, deployment, true)?;
        Ok(None)
    }

    /// A background job: answer now; run it as a foreground request of the
    /// gateway's own, which is `body` without `background`.
    async fn start(
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
        let webhook = body
            .as_object_mut()
            .and_then(|fields| fields.remove("webhook"))
            .and_then(|value| value.as_str().map(str::to_owned));
        if let Some(url) = &webhook {
            let url =
                reqwest::Url::parse(url).map_err(|_| Fault::bad_request("invalid webhook URL"))?;
            if !matches!(url.scheme(), "https" | "http") {
                return Err(Fault::bad_request("webhook must use HTTP"));
            }
        }
        let max = body
            .get("max_output_tokens")
            .and_then(Value::as_u64)
            .unwrap_or(1024);
        if max > i64::MAX as u64 {
            return Err(Fault::bad_request("invalid output cap"));
        }
        body["max_output_tokens"] = max.into();
        let reserve = (body.to_string().len() as i64).saturating_add(max as i64);
        let mut uri = cx
            .extensions()
            .get::<Asked>()
            .map_or_else(|| "/v1/responses".into(), |a| a.uri.to_string());
        if let Some(credential) = cx.extensions().get::<Credential>() {
            let query = credential.query().collect::<Vec<_>>().join("&");
            if !query.is_empty() {
                uri.push(if uri.contains('?') { '&' } else { '?' });
                uri.push_str(&query);
            }
        }
        let stored_headers: Vec<(String, Vec<u8>)> = cx
            .extensions()
            .get::<Credential>()
            .into_iter()
            .flat_map(|c| c.headers().iter())
            .map(|(name, value)| (name.to_string(), value.as_bytes().to_vec()))
            .collect();
        let (saved_id, saved_body, home) = (id.clone(), body.to_string(), hex(&tenant(cx)));
        let headers =
            serde_json::to_string(&stored_headers).map_err(|e| Fault::internal(e.to_string()))?;
        self.db(move |tx| {
            tx.execute("INSERT OR IGNORE INTO job_accounts(tenant,balance) VALUES(?1,?2)", (&home, i64::MAX)).map_err(sql)?;
            let held = tx.execute("UPDATE job_accounts SET held=held+?1 WHERE tenant=?2 AND balance-spent-held>=?1", (reserve, &home)).map_err(sql)?;
            if held == 0 { return Err(Fault::new("insufficient_funds", "job exceeds available budget").with(respan_gateway_http::Status(402))); }
            tx.execute("INSERT INTO background_jobs(id,request,uri,headers,tenant,reserve,webhook) VALUES(?1,?2,?3,?4,?5,?6,?7)", (saved_id, saved_body, uri, headers, home, reserve, webhook)).map_err(sql)?;
            Ok(())
        }).await?;
        self.run(&cx.gateway(), &id).await?;
        let queued = json!({ "id": id, "status": "queued" }).to_string();
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
        if let Some(job) = cx.extensions().get::<Job>().cloned() {
            let deployment = self
                .verify(&job.id, tenant(cx))
                .await?
                .ok_or_else(|| Fault::internal("bad job id"))?;
            let output = job.output.clone();
            cx.filter(move |got: &mut Got<Rest>| {
                lock(&output).extend_from_slice(&got.0);
                Ok(())
            })?;
            self.chain(cx, &body).await?;
            return self.relay(cx, job.id, deployment);
        }
        if streams(cx, &body) {
            return Err(Fault::bad_request(
                "streaming is not supported by this example",
            ));
        }
        if let Some((id, rest)) = split(&ask.path) {
            return self.route(cx, &ask, id, rest).await;
        }
        let deployment = self.chain(cx, &body).await?.unwrap_or(DEFAULT);
        let id = self.next_id(deployment, tenant(cx)).await?;
        match body["background"] == true {
            true => self.start(cx, body, id).await,
            false => self.relay(cx, id, deployment),
        }
    }
    async fn on_client_end(&self, cx: &ClientContext, end: &Result<(), Fault>) {
        if let Some(job) = cx.extensions().get::<Job>() {
            let body = match end {
                Ok(()) => lock(&job.output).clone().freeze(),
                Err(fault) => Bytes::from(
                    json!({ "id": job.id, "status": "failed", "error": fault.message() })
                        .to_string(),
                ),
            };
            let _ = self.finish(&job.id, Some(&job.owner), body).await;
            lock(&self.0.active).remove(&job.id);
            let _ = self.deliver_webhooks().await;
        }
    }
}

/// Local Responses endpoint: this protocol's native-id operations are not
/// supported by the pinned provider fake. Each deployment has its own queue.
struct ResponsesServer {
    base: String,
    state: Arc<Mutex<ResponsesState>>,
    task: tokio::task::JoinHandle<Result<(), std::io::Error>>,
}

#[derive(Default)]
struct ResponsesState {
    replies: VecDeque<(StatusCode, String)>,
    requests: Vec<Recorded>,
}

#[derive(Clone)]
#[allow(dead_code)] // Captured fields are inspected by the example's tests.
struct Recorded {
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
}

impl ResponsesServer {
    async fn start() -> Result<Self, Fault> {
        let state = Arc::new(Mutex::new(ResponsesState::default()));
        let app = axum::Router::new().fallback(axum::routing::any({
            let state = state.clone();
            move |method: Method, uri: Uri, headers: HeaderMap, body: Bytes| {
                let state = state.clone();
                async move {
                    let mut state = lock(&state);
                    state.requests.push(Recorded {
                        method,
                        uri,
                        headers,
                        body,
                    });
                    let (status, body) = state.replies.pop_front().unwrap_or((
                        StatusCode::INTERNAL_SERVER_ERROR,
                        r#"{"error":"unscripted Responses request"}"#.into(),
                    ));
                    (
                        status,
                        [(http::header::CONTENT_TYPE, "application/json")],
                        body,
                    )
                }
            }
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .map_err(|error| Fault::internal(error.to_string()))?;
        let base = format!(
            "http://{}",
            listener
                .local_addr()
                .map_err(|error| Fault::internal(error.to_string()))?
        );
        let task = tokio::spawn(async move { axum::serve(listener, app).await });
        Ok(Self { base, state, task })
    }

    fn answer(&self, status: StatusCode, body: &str) {
        lock(&self.state)
            .replies
            .push_back((status, body.to_owned()));
    }

    #[cfg(test)]
    fn requests(&self) -> Vec<Recorded> {
        lock(&self.state).requests.clone()
    }

    fn transport(&self) -> Result<HttpClient, Fault> {
        HttpClient::new(&self.base)
    }
}

impl Drop for ResponsesServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn deployments() -> Result<(ResponsesServer, ResponsesServer), Fault> {
    Ok((
        ResponsesServer::start().await?,
        ResponsesServer::start().await?,
    ))
}

fn gateway(
    east: impl Fn(respan_gateway_sdk::Extensions) -> respan_gateway_http::Outgoing
    + Send
    + Sync
    + 'static,
    west: impl Fn(respan_gateway_sdk::Extensions) -> respan_gateway_http::Outgoing
    + Send
    + Sync
    + 'static,
    jobs: &Jobs,
) -> Result<Gateway, Fault> {
    Gateway::new()
        .client::<Ask, _, _>(RESPONSES)?
        .provider::<Ask, _, _>("east", RESPONSES, east)?
        .provider::<Ask, _, _>("west", RESPONSES, west)?
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
    let (east, west) = deployments().await?;
    let done = r#"{"id":"native-1","output":"done"}"#;
    west.answer(http::StatusCode::OK, done);
    let path = std::env::var("JOBS_DB").unwrap_or_else(|_| "background-jobs.sqlite3".into());
    let jobs = Jobs::open(path)?;
    let credential = Credential::of(request("POST", "/v1/responses", "{}").headers().clone());
    jobs.budget(identity(Some(&credential)), 1_000_000).await?;
    let gateway = gateway(
        east.transport()?.connect(),
        west.transport()?.connect(),
        &jobs,
    )?;
    jobs.resume(&gateway).await?;

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

#[cfg(test)]
#[path = "tests/background_durable.rs"]
mod durable_tests;
