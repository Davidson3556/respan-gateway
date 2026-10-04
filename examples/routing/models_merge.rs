//! Use case 4, one request, many upstream calls: `GET /v1/models` is answered
//! by asking every configured provider and merging their lists into one.
//!
//! The endpoint is its own format, `Models`, built on `formats::opaque`:
//! its values are the bytes of the request and of each answer, which is all
//! a plugin that never reads a chat needs. Its `MergeModels` plugin answers
//! in `on_client`: it makes a call to each provider (a relay of the client's
//! request, each under its provider's own key, set on its transport), reads
//! each answer's JSON, and returns one list, the first entry of an id
//! winning. A provider that fails is left out of the list; only when every
//! one fails does the request fail.
//!
//! Registration and the plugin's provider list come from the same configuration,
//! so every configured provider is included without exposing gateway internals.
//!
//! Not covered: another provider's list shape than OpenAI's
//! (`{"data": [{"id": ..}]}`); prefixing ids with their provider; caching the
//! merged list.
//!
//! `cargo run -p respan-gateway-examples --example models_merge`

use respan_gateway_http::HttpClient;
use respan_gateway_http::formats;
use respan_gateway_http::formats::opaque::{self, Got, Sent};
use respan_gateway_http::formats::whole;
use respan_gateway_http::http::{self, HeaderMap, HeaderValue, StatusCode};
use respan_gateway_http::{self as h, Incoming, Outgoing};
use respan_gateway_sdk::Upstream;
use respan_gateway_sdk::futures::future::join_all;
use respan_gateway_sdk::futures::{TryStreamExt, stream};
use respan_gateway_sdk::{
    ClientContext, Connection, Fault, Format, Gateway, Plugin, Response, async_trait,
};
#[allow(dead_code)]
#[path = "../../tests/support/http.rs"]
mod http_support;
use http_support::{read_whole, serve};
use serde_json::{Value, json};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex, PoisonError};

/// `GET /v1/models`, relayed unread by one provider, merged by the plugin.
#[derive(Clone)]
struct Models;

impl Format<Incoming, Sent<Models>, Got<Models>> for Models {
    fn name(&self) -> &str {
        "models"
    }

    fn matches(&self, io: &Incoming) -> bool {
        io.request().method() == http::Method::GET && io.request().uri().path() == "/v1/models"
    }

    fn bind(&self, io: Incoming) -> Result<Connection<Sent<Models>, Got<Models>>, Fault> {
        Ok(io.bind(
            |request| Ok(opaque::read_request(request)),
            |reply, _source| {
                // A merged list has no provider's head to borrow a content type from.
                let mut head = reply.head();
                formats::default_content_type(&mut head, "application/json");
                let write = |one: &[Got<Models>]| {
                    Ok(one
                        .iter()
                        .flat_map(|g| g.0.body().to_vec())
                        .collect::<Vec<u8>>())
                };
                let fail = |fault: &Fault| {
                    json!({ "error": { "message": fault.message() } })
                        .to_string()
                        .into_bytes()
                };
                Ok(reply.write(head, whole::collect(write, fail)))
            },
            h::plain_refusal,
        ))
    }
}

impl Format<Outgoing, Got<Models>, Sent<Models>> for Models {
    fn name(&self) -> &str {
        "models"
    }

    fn bind(&self, io: Outgoing) -> Result<Connection<Got<Models>, Sent<Models>>, Fault> {
        Ok(io.bind(
            opaque::write_request,
            |response, request| opaque::read_response("models", response, &request.into_parts().0),
            |head, _| head.status.is_success(),
        ))
    }
}

/// Answers `GET /v1/models` with the lists of `providers` merged.
struct MergeModels(Vec<&'static str>);

#[async_trait]
impl Plugin for MergeModels {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        if !cx.is_kind::<Sent<Models>>() {
            return Ok(None);
        }
        let calls: Vec<_> = self.0.iter().map(|p| cx.call(*p)).collect();
        let lists: Vec<Result<Vec<Value>, Fault>> =
            join_all(calls.into_iter().map(|call| async move {
                let frames: Vec<Got<Models>> = call.await?.outputs().try_collect().await?;
                let bytes: Vec<u8> = frames.iter().flat_map(|f| f.0.body().to_vec()).collect();
                let mut list: Value = serde_json::from_slice(&bytes)
                    .map_err(|_| Upstream::fault("a provider's models are not JSON"))?;
                match list["data"].take() {
                    Value::Array(models) if models.iter().all(|m| m["id"].is_string()) => {
                        Ok(models)
                    }
                    _ => Err(Upstream::fault("a provider's models are not a list of ids")),
                }
            }))
            .await;
        let (lists, failures): (Vec<_>, Vec<_>) = lists.into_iter().partition(Result::is_ok);
        if lists.is_empty() {
            return Err(failures
                .into_iter()
                .find_map(Result::err)
                .unwrap_or_else(|| Upstream::fault("no provider")));
        }
        let mut data: Vec<Value> = Vec::new();
        for model in lists.iter().flatten().flatten() {
            if !data.iter().any(|seen| seen["id"] == model["id"]) {
                data.push(model.clone());
            }
        }
        let merged = json!({ "object": "list", "data": data }).to_string();
        Ok(Some(Response::stream(stream::iter([Ok(
            Got::<Models>::new(merged),
        )]))))
    }
}

/// This endpoint is deliberately served directly: mockllm models chat and
/// embeddings, but not OpenAI's `GET /v1/models` list. One server models one
/// upstream, with its own queue and captured requests, not URL dispatch.
struct LocalModels {
    url: String,
    answers: Arc<Mutex<VecDeque<(StatusCode, String)>>>,
    #[allow(dead_code)]
    seen: Arc<Mutex<Vec<(String, String, String)>>>,
    task: tokio::task::JoinHandle<()>,
}

impl LocalModels {
    async fn start() -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind models provider");
        let url = format!(
            "http://{}",
            listener.local_addr().expect("models provider address")
        );
        let answers = Arc::new(Mutex::new(VecDeque::new()));
        let seen = Arc::new(Mutex::new(Vec::new()));
        let queue = answers.clone();
        let requests = seen.clone();
        let app = axum::Router::new().route(
            "/v1/models",
            axum::routing::get(move |req: http::Request<axum::body::Body>| {
                let (queue, requests) = (queue.clone(), requests.clone());
                async move {
                    let key = req
                        .headers()
                        .get("authorization")
                        .and_then(|h| h.to_str().ok())
                        .unwrap_or_default()
                        .to_owned();
                    requests
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .push((req.method().to_string(), req.uri().path().to_owned(), key));
                    let (status, body) = queue
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .pop_front()
                        .unwrap_or((
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "no scripted answer".into(),
                        ));
                    (status, [("content-type", "application/json")], body)
                }
            }),
        );
        let task = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        Self {
            url,
            answers,
            seen,
            task,
        }
    }

    fn answer(&self, answer: (StatusCode, String)) {
        self.answers
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push_back(answer);
    }

    #[cfg(test)]
    fn requests(&self) -> Vec<(String, String, String)> {
        self.seen
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

impl Drop for LocalModels {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// A key set on a provider's own transport, so the client's is not sent.
fn key(value: &'static str) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert("authorization", HeaderValue::from_static(value));
    headers
}

fn gateway(a: &LocalModels, b: &LocalModels) -> Result<Gateway, Fault> {
    let providers = [("a", a, "Bearer key-a"), ("b", b, "Bearer key-b")];
    let mut gateway = Gateway::new().client::<Sent<Models>, _, _>(Models)?;
    for (name, server, token) in providers {
        let transport = HttpClient::new(&server.url)?.with_headers(key(token))?;
        gateway = gateway.provider::<Sent<Models>, _, _>(name, Models, transport.connect())?;
    }
    Ok(gateway.plugin(MergeModels(
        providers.iter().map(|(name, ..)| *name).collect(),
    )))
}

fn request() -> http::Request<respan_gateway_http::Body> {
    let body = respan_gateway_http::Body::full("");
    match http::Request::get("/v1/models")
        .header("authorization", "Bearer client")
        .body(body)
    {
        Ok(request) => request,
        Err(_) => unreachable!("a fixed request is valid"),
    }
}

fn list(ids: &[&str]) -> (StatusCode, String) {
    let data: Vec<Value> = ids
        .iter()
        .map(|id| json!({ "id": id, "object": "model" }))
        .collect();
    (
        StatusCode::OK,
        json!({ "object": "list", "data": data }).to_string(),
    )
}

#[tokio::main]
async fn main() -> Result<(), Fault> {
    let (a, b) = (LocalModels::start().await, LocalModels::start().await);
    a.answer(list(&["gpt-x", "shared"]));
    b.answer(list(&["llama", "shared"]));
    let gw = gateway(&a, &b)?;
    let (status, _, body) = read_whole(serve(&gw, request()).await.0).await;
    println!("{status} {}", String::from_utf8_lossy(&body?));
    Ok(())
}

#[cfg(test)]
#[path = "../tests/cases/routing/models_merge.rs"]
mod tests;
