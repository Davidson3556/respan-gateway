//! A strict local fake of OpenAI's Videos API: it keeps the jobs it made,
//! finishes one when it is first polled, refuses what the real API refuses
//! (a missing key or field, an unknown field, a number of `seconds`, a size
//! or a reference it does not take, content not ready, a body on a `GET`),
//! and serves content in chunks.

use std::convert::Infallible;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use axum::body::Body as Original;
use futures::StreamExt;
use respan_gateway_core::Extensions;
use respan_gateway_http::ProviderIo;
use respan_gateway_http::bytes::Bytes;
use respan_gateway_http::http::{self, StatusCode};
use respan_gateway_http::http_body_util::BodyExt;
use serde_json::{Value, json};

use super::{
    Fake, KEY, Rules, Seen, answered, bad, error, form, json_answer, missing, no_model, string,
    unknown, unrecognized,
};

/// A running fake of OpenAI's Videos API.
pub struct Videos {
    /// Its base URL, with the version.
    pub base: String,
    seen: Arc<Mutex<Vec<Seen>>>,
}

struct State {
    rules: Rules,
    seen: Arc<Mutex<Vec<Seen>>>,
    jobs: Mutex<Vec<Value>>,
}

impl Videos {
    /// A fake of OpenAI's own rules.
    pub async fn openai() -> Videos {
        Videos::start(Rules::default()).await
    }

    /// A fake of `rules`.
    pub async fn start(rules: Rules) -> Videos {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("a port");
        let address = listener.local_addr().expect("an address");
        let seen = Arc::new(Mutex::new(Vec::new()));
        let state = Arc::new(State {
            rules,
            seen: seen.clone(),
            jobs: Mutex::new(Vec::new()),
        });
        let app = axum::Router::new().fallback(move |request: http::Request<Original>| {
            let state = state.clone();
            async move { Ok::<_, Infallible>(answer(request, &state).await) }
        });
        tokio::spawn(async move { axum::serve(listener, app).await });
        Videos {
            base: format!("http://{address}/v1"),
            seen,
        }
    }

    /// The calls it took, in order.
    pub fn seen(&self) -> Vec<Seen> {
        self.seen
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// A transport to it, with its key.
    pub fn transport(&self) -> impl Fn(Extensions) -> ProviderIo + Clone + Send + Sync + 'static {
        Fake::transport_to(&self.base)
    }
}

/// The video a fake serves: 64 KiB in 8 chunks.
pub fn video_chunks() -> Vec<Bytes> {
    (0..8u8)
        .map(|i| {
            Bytes::from(
                (0..8192)
                    .map(|j| (j as u8).wrapping_mul(3) ^ i)
                    .collect::<Vec<u8>>(),
            )
        })
        .collect()
}

/// The thumbnail a fake serves.
pub const THUMBNAIL: &[u8] = b"RIFF\0\0\0\0WEBPVP8 thumbnail";

fn not_found(id: &str) -> http::Response<Original> {
    let message = format!("Video with id '{id}' not found.");
    error(StatusCode::NOT_FOUND, &message, None, "not_found")
}

fn locked<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

async fn answer(request: http::Request<Original>, state: &State) -> http::Response<Original> {
    let (head, body) = request.into_parts();
    let auth = head.headers.get(http::header::AUTHORIZATION);
    if auth.and_then(|v| v.to_str().ok()) != Some(&format!("Bearer {KEY}")) {
        let message = "Incorrect API key provided.";
        return error(StatusCode::UNAUTHORIZED, message, None, "invalid_api_key");
    }
    let Ok(body) = body.collect().await.map(|b| b.to_bytes()) else {
        return bad("The body could not be read.", None);
    };
    let content_type = head
        .headers
        .get(http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_owned();
    let mut call = match (&head.method, content_type.as_str()) {
        (&http::Method::POST, c) if c.starts_with("multipart/form-data") => {
            match form(c, body).await {
                Ok(call) => call,
                Err(message) => return bad(message, None),
            }
        }
        (&http::Method::POST, _) => match serde_json::from_slice::<Value>(&body) {
            Ok(Value::Object(fields)) if content_type == "application/json" => Seen {
                fields,
                ..Seen::default()
            },
            _ => return bad("We could not parse the JSON body of your request.", None),
        },
        _ if !body.is_empty() => return bad("A GET or DELETE takes no body.", None),
        _ => Seen::default(),
    };
    call.method = head.method.to_string();
    call.path = head.uri.path().to_owned();
    call.query = head.uri.query().map(str::to_owned);
    call.content_type = content_type;
    locked(&state.seen).push(call.clone());
    let query = |key: &str| {
        let pairs = head.uri.query().unwrap_or_default().split('&');
        pairs
            .filter_map(|p| p.split_once('='))
            .find(|(k, _)| *k == key)
            .map(|(_, v)| v.to_owned())
    };
    let segments: Vec<&str> = call.path.split('/').skip(1).collect();
    match (&head.method, segments.as_slice()) {
        (&http::Method::POST, ["v1", "videos"]) => create(&call, state),
        (&http::Method::GET, ["v1", "videos"]) => {
            list(state, query("after"), query("limit"), query("order"))
        }
        (&http::Method::GET, ["v1", "videos", id]) => retrieve(id, state),
        (&http::Method::DELETE, ["v1", "videos", id]) => delete(id, state),
        (&http::Method::GET, ["v1", "videos", id, "content"]) => {
            content(id, query("variant"), state)
        }
        (&http::Method::POST, ["v1", "videos", id, "remix"]) => remix(id, &call, state),
        _ => error(StatusCode::NOT_FOUND, "Invalid URL.", None, "not_found"),
    }
}

fn new_job(state: &State, model: &str, seconds: &str, size: &str) -> Value {
    let mut jobs = locked(&state.jobs);
    let job = json!({
        "id": format!("video_{:032x}", jobs.len() + 1),
        "object": "video",
        "created_at": 1_700_000_000,
        "status": "queued",
        "model": model,
        "progress": 0,
        "seconds": seconds,
        "size": size,
        "completed_at": null,
        "expires_at": null,
        "remixed_from_video_id": null,
        "error": null,
    });
    jobs.push(job.clone());
    job
}

fn create(call: &Seen, state: &State) -> http::Response<Original> {
    let known = ["model", "prompt", "seconds", "size", "input_reference"];
    if let Some(field) = unknown(call, &known, &state.rules) {
        return unrecognized(field);
    }
    let Some(_) = string(call, "prompt").filter(|p| !p.is_empty()) else {
        return missing("prompt");
    };
    let model = string(call, "model").unwrap_or("sora-2");
    if !["sora-2", "sora-2-pro"].contains(&model) {
        return no_model(model);
    }
    let seconds = match call.fields.get("seconds") {
        None => "4",
        Some(Value::String(s)) if ["4", "8", "12"].contains(&s.as_str()) => s,
        Some(_) => {
            let message = "Invalid value for 'seconds': expected one of '4', '8' or '12'.";
            return bad(message, Some("seconds"));
        }
    };
    let size = string(call, "size").unwrap_or("720x1280");
    if !["720x1280", "1280x720", "1024x1792", "1792x1024"].contains(&size) {
        return bad("Invalid value for 'size'.", Some("size"));
    }
    for file in &call.files {
        let mime = file.content_type.as_deref().unwrap_or_default();
        if !["image/jpeg", "image/png", "image/webp", "video/mp4"].contains(&mime) {
            let message =
                format!("Invalid file 'input_reference': unsupported mimetype ('{mime}').");
            return bad(&message, Some("input_reference"));
        }
    }
    json_answer(new_job(state, model, seconds, size), &state.rules)
}

/// The job `id`, finished once polled.
fn retrieve(id: &str, state: &State) -> http::Response<Original> {
    let mut jobs = locked(&state.jobs);
    let Some(job) = jobs.iter_mut().find(|j| j["id"] == id) else {
        return not_found(id);
    };
    job["status"] = "completed".into();
    job["progress"] = 100.into();
    job["completed_at"] = 1_700_000_100.into();
    job["expires_at"] = 1_700_086_500.into();
    json_answer(job.clone(), &state.rules)
}

fn delete(id: &str, state: &State) -> http::Response<Original> {
    let mut jobs = locked(&state.jobs);
    let Some(at) = jobs.iter().position(|j| j["id"] == id) else {
        return not_found(id);
    };
    jobs.remove(at);
    json_answer(
        json!({"id": id, "object": "video.deleted", "deleted": true}),
        &state.rules,
    )
}

fn list(
    state: &State,
    after: Option<String>,
    limit: Option<String>,
    order: Option<String>,
) -> http::Response<Original> {
    let mut jobs = locked(&state.jobs).clone();
    match order.as_deref() {
        None | Some("desc") => jobs.reverse(),
        Some("asc") => {}
        Some(_) => return bad("Invalid value for 'order'.", Some("order")),
    }
    if let Some(after) = after {
        let at = jobs.iter().position(|j| j["id"] == after.as_str());
        jobs.drain(..at.map_or(0, |at| at + 1));
    }
    let Ok(limit) = limit.as_deref().unwrap_or("20").parse::<usize>() else {
        return bad("Invalid value for 'limit'.", Some("limit"));
    };
    let has_more = jobs.len() > limit;
    jobs.truncate(limit);
    json_answer(
        json!({
            "object": "list",
            "data": jobs,
            "first_id": jobs.first().map(|j| j["id"].clone()),
            "last_id": jobs.last().map(|j| j["id"].clone()),
            "has_more": has_more,
        }),
        &state.rules,
    )
}

fn content(id: &str, variant: Option<String>, state: &State) -> http::Response<Original> {
    let jobs = locked(&state.jobs);
    let Some(job) = jobs.iter().find(|j| j["id"] == id) else {
        return not_found(id);
    };
    if job["status"] != "completed" {
        return bad(
            "Video is not ready yet, use GET /v1/videos/{video_id} to check status.",
            None,
        );
    }
    match variant.as_deref() {
        None | Some("video") => {
            let paced = futures::stream::iter(video_chunks()).then(|chunk| async move {
                tokio::time::sleep(Duration::from_millis(5)).await;
                Ok::<_, Infallible>(chunk)
            });
            answered("video/mp4", Original::from_stream(paced))
        }
        Some("thumbnail") => answered("image/webp", THUMBNAIL),
        Some(_) => bad("Invalid value for 'variant'.", Some("variant")),
    }
}

fn remix(id: &str, call: &Seen, state: &State) -> http::Response<Original> {
    if !call.files.is_empty() || call.content_type != "application/json" {
        return bad("We could not parse the JSON body of your request.", None);
    }
    if let Some(field) = unknown(call, &["prompt"], &state.rules) {
        return unrecognized(field);
    }
    if string(call, "prompt").is_none() {
        return missing("prompt");
    }
    let source = locked(&state.jobs).iter().find(|j| j["id"] == id).cloned();
    let Some(source) = source else {
        return not_found(id);
    };
    if source["status"] != "completed" {
        return bad("The video to remix is not completed.", None);
    }
    let field = |key: &str| source[key].as_str().unwrap_or_default().to_owned();
    let mut job = new_job(state, &field("model"), &field("seconds"), &field("size"));
    job["remixed_from_video_id"] = id.into();
    if let Some(stored) = locked(&state.jobs).last_mut() {
        *stored = job.clone();
    }
    json_answer(job, &state.rules)
}
