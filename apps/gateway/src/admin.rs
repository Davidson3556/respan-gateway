//! The web UI under `/ui/`, and the admin API it reads under `/ui/api/`.

use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant, SystemTime};

use axum::Json;
use axum::Router;
use axum::extract::{Path, Query, State};
use axum::middleware::from_fn_with_state;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Redirect, Response};
use axum::routing::get;
use respan_gateway_core::futures::{Stream, StreamExt, stream};
use respan_gateway_http::http::header::{CACHE_CONTROL, CONTENT_TYPE};
use respan_gateway_http::http::{HeaderValue, StatusCode};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::watch;

use crate::auth::{self, ClientKeys};
use crate::compose::Plan;
use crate::formats::{KnownFormat, first_of_each_kind};
use crate::requests::{RequestLog, rfc3339};

mod assets {
    include!(concat!(env!("OUT_DIR"), "/ui_assets.rs"));
}

/// What the admin API reports.
#[derive(Debug)]
pub struct Admin {
    pub log: Arc<RequestLog>,
    pub source: String,
    pub auth_required: bool,
    pub listen: OnceLock<SocketAddr>,
    started_at: SystemTime,
    started: Instant,
    /// Each provider, as `/ui/api/providers` lists it, but its counts.
    providers: Vec<Value>,
    routes: Value,
    /// Set once the server stops: open event streams then end.
    stopping: Arc<watch::Sender<bool>>,
}

impl Admin {
    pub fn new(
        log: Arc<RequestLog>,
        source: String,
        plan: &Plan,
        formats: &[KnownFormat],
        stopping: Arc<watch::Sender<bool>>,
    ) -> Self {
        let providers = plan
            .upstreams
            .iter()
            .map(|u| {
                let formats: Vec<&str> = u.entries.iter().map(|e| e.format.as_str()).collect();
                let kinds = first_of_each_kind(u.entries.iter().map(|e| (e.kind, ())));
                let kinds: Vec<&str> = kinds.iter().map(|(kind, _)| kind.label).collect();
                json!({
                    "name": u.name,
                    "vendor": u.preset.map(|p| p.id),
                    "quirks": u.preset.map_or(&[][..], |p| p.quirks).iter()
                        .map(|q| json!({ "name": q.name, "description": q.description }))
                        .collect::<Vec<_>>(),
                    "formats": formats,
                    "kinds": kinds,
                    "base_url": u.entries.first().map(|e| e.base_url.as_str()),
                    "credentials": if u.own_key { "gateway" } else { "client" },
                })
            })
            .collect();
        let serve: Vec<Value> = plan
            .serve
            .iter()
            .filter_map(|name| formats.iter().find(|f| f.name == *name))
            .map(|f| json!({ "format": f.name, "kind": f.kind.label, "endpoints": f.endpoints }))
            .collect();
        let prefixes = plan.routes.prefixes();
        let prefixes = prefixes
            .iter()
            .map(|(prefix, provider)| json!({ "prefix": prefix, "provider": provider }));
        let defaults = plan.routes.defaults();
        let defaults = defaults
            .iter()
            .map(|(kind, provider)| json!({ "kind": kind, "provider": provider }));
        let routes = json!({
            "serve": serve,
            "routes": prefixes.collect::<Vec<_>>(),
            "defaults": defaults.collect::<Vec<_>>(),
        });
        Self {
            log,
            source,
            auth_required: !plan.keys.is_empty(),
            listen: OnceLock::new(),
            started_at: SystemTime::now(),
            started: Instant::now(),
            providers,
            routes,
            stopping,
        }
    }
}

/// `/` to the UI, the UI's files, and the admin API behind `keys`.
pub fn router(admin: Arc<Admin>, keys: Arc<ClientKeys>) -> Router {
    let api = Router::new()
        .route("/ui/api/overview", get(overview))
        .route("/ui/api/providers", get(providers))
        .route("/ui/api/routes", get(routes))
        .route("/ui/api/requests", get(requests))
        .route("/ui/api/requests/{id}", get(request))
        .route("/ui/api/events", get(events))
        .route("/ui/api/{*rest}", get(not_found))
        .route_layer(from_fn_with_state(keys, auth::require))
        .with_state(admin);
    Router::new()
        .route("/", get(|| async { Redirect::to("/ui/") }))
        .route("/ui", get(|| async { Redirect::to("/ui/") }))
        .route("/ui/", get(|| async { asset("index.html") }))
        .route(
            "/ui/{*path}",
            get(|Path(path): Path<String>| async move { asset(&path) }),
        )
        .merge(api)
}

async fn overview(State(admin): State<Arc<Admin>>) -> Json<Value> {
    Json(json!({
        "version": crate::VERSION,
        "listen": admin.listen.get().map(ToString::to_string),
        "started_at": rfc3339(admin.started_at),
        "uptime_seconds": admin.started.elapsed().as_secs(),
        "auth_required": admin.auth_required,
        "config_source": admin.source,
        "totals": admin.log.totals(),
    }))
}

async fn providers(State(admin): State<Arc<Admin>>) -> Json<Value> {
    let providers: Vec<Value> = admin
        .providers
        .iter()
        .map(|p| {
            let mut p = p.clone();
            let stats = admin
                .log
                .provider_stats(p["name"].as_str().unwrap_or_default());
            p["requests"] = stats.requests.into();
            p["errors"] = stats.errors.into();
            p["avg_latency_ms"] = stats.avg_latency_ms().into();
            p
        })
        .collect();
    Json(json!({ "providers": providers }))
}

async fn routes(State(admin): State<Arc<Admin>>) -> Json<Value> {
    Json(admin.routes.clone())
}

#[derive(Debug, Deserialize)]
struct Limit {
    limit: Option<usize>,
}

async fn requests(State(admin): State<Arc<Admin>>, Query(q): Query<Limit>) -> Json<Value> {
    let requests = admin.log.recent(q.limit.unwrap_or(100));
    Json(json!({ "requests": requests }))
}

async fn request(State(admin): State<Arc<Admin>>, Path(id): Path<String>) -> Response {
    match admin.log.get(&id) {
        Some(record) => Json(record).into_response(),
        None => not_found().await,
    }
}

async fn not_found() -> Response {
    not_found_response()
}

fn not_found_response() -> Response {
    auth::error(StatusCode::NOT_FOUND, "not_found_error", "not found")
}

/// Every request as it starts and finishes, with a comment each 15 s,
/// until the server stops.
async fn events(
    State(admin): State<Arc<Admin>>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let receiver = admin.log.subscribe();
    let events = stream::unfold(receiver, |mut receiver| async move {
        loop {
            match receiver.recv().await {
                Ok(json) => {
                    let event = Event::default().event("request").data(&*json);
                    return Some((Ok(event), receiver));
                }
                Err(RecvError::Lagged(_)) => continue,
                Err(RecvError::Closed) => return None,
            }
        }
    });
    let mut stopping = admin.stopping.subscribe();
    let stopped = async move {
        let _ = stopping.wait_for(|stop| *stop).await;
    };
    Sse::new(events.take_until(stopped)).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("ping"),
    )
}

/// The UI's file at `path`, or its index for a path of the app's own; a
/// missing file under `assets/` is a 404.
fn asset(path: &str) -> Response {
    let file = |path: &str| assets::ASSETS.iter().find(|(name, _)| *name == path);
    let found = match path.starts_with("assets/") {
        true => file(path),
        false => file(path).or_else(|| file("index.html")),
    };
    let Some(&(name, bytes)) = found else {
        return not_found_response();
    };
    let cache = match name.starts_with("assets/") {
        true => "public, max-age=31536000, immutable",
        false => "no-cache",
    };
    let headers = [
        (CONTENT_TYPE, HeaderValue::from_static(content_type(name))),
        (CACHE_CONTROL, HeaderValue::from_static(cache)),
    ];
    (headers, bytes).into_response()
}

fn content_type(name: &str) -> &'static str {
    let extension = name.rsplit_once('.').map_or("", |(_, e)| e);
    match extension {
        "html" => "text/html; charset=utf-8",
        "js" | "mjs" => "text/javascript; charset=utf-8",
        "css" => "text/css; charset=utf-8",
        "json" | "map" => "application/json",
        "svg" => "image/svg+xml",
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "gif" => "image/gif",
        "webp" => "image/webp",
        "ico" => "image/x-icon",
        "woff" => "font/woff",
        "woff2" => "font/woff2",
        "txt" => "text/plain; charset=utf-8",
        "wasm" => "application/wasm",
        _ => "application/octet-stream",
    }
}
