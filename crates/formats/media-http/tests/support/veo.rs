//! A strict local fake of Veo as the Gemini API serves it: it keeps the
//! operations it started, done from their second poll, refuses what the
//! real API refuses (a missing key, an unknown field, a duration or aspect
//! ratio Veo does not make, a model it does not know), answers errors in
//! Google's schema, and serves a video at the URI its operation names, or
//! redirects there to storage, as the Gemini API may.

use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use axum::body::Body as Wire;
use futures::StreamExt;
use respan_gateway_core::Extensions;
use respan_gateway_http::http::{self, HeaderMap, HeaderValue, StatusCode};
use respan_gateway_http::http_body_util::BodyExt;
use respan_gateway_http::{HttpTransport, ProviderIo};
use serde_json::{Value, json};

use super::answered;
use super::videos::video_chunks;

/// The key the fake takes.
pub const KEY: &str = "veo-test-key";

/// A call the fake took.
#[derive(Debug, Clone)]
pub struct Call {
    pub method: String,
    pub path: String,
    pub query: Option<String>,
    pub body: Value,
}

/// A running fake of Veo.
pub struct Veo {
    /// Its base URL, with the version.
    pub base: String,
    seen: Arc<Mutex<Vec<Call>>>,
}

struct State {
    origin: String,
    redirect: bool,
    seen: Arc<Mutex<Vec<Call>>>,
    /// Each operation's prompt and how often it was polled.
    operations: Mutex<HashMap<String, (String, u32)>>,
}

fn locked<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

impl Veo {
    /// A fake that serves a video where its operation says.
    pub async fn start() -> Veo {
        Veo::serve(false).await
    }

    /// A fake that redirects a video's download to storage.
    pub async fn redirecting() -> Veo {
        Veo::serve(true).await
    }

    async fn serve(redirect: bool) -> Veo {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("a port");
        let address = listener.local_addr().expect("an address");
        let seen = Arc::new(Mutex::new(Vec::new()));
        let state = Arc::new(State {
            origin: format!("http://{address}"),
            redirect,
            seen: seen.clone(),
            operations: Mutex::new(HashMap::new()),
        });
        let app = axum::Router::new().fallback(move |request: http::Request<Wire>| {
            let state = state.clone();
            async move { Ok::<_, Infallible>(answer(request, &state).await) }
        });
        tokio::spawn(async move { axum::serve(listener, app).await });
        Veo {
            base: format!("http://{address}/v1beta"),
            seen,
        }
    }

    /// The calls it took, in order.
    pub fn seen(&self) -> Vec<Call> {
        locked(&self.seen).clone()
    }

    fn http(&self) -> HttpTransport {
        let mut headers = HeaderMap::new();
        headers.insert("x-goog-api-key", HeaderValue::from_static(KEY));
        HttpTransport::new(&self.base)
            .and_then(|t| t.with_headers(headers))
            .expect("a transport")
    }

    /// A transport to it, with its key.
    pub fn transport(&self) -> impl Fn(Extensions) -> ProviderIo + Clone + Send + Sync + 'static {
        self.http().connect()
    }

    /// A transport to it that follows redirects.
    pub fn following(&self) -> impl Fn(Extensions) -> ProviderIo + Clone + Send + Sync + 'static {
        self.http().follow_redirects(|_| true).connect()
    }
}

/// An error in Google's schema.
fn error(status: StatusCode, message: &str) -> http::Response<Wire> {
    let name = match status {
        StatusCode::BAD_REQUEST => "INVALID_ARGUMENT",
        StatusCode::FORBIDDEN => "PERMISSION_DENIED",
        StatusCode::NOT_FOUND => "NOT_FOUND",
        _ => "INTERNAL",
    };
    let body = json!({"error": {"code": status.as_u16(), "message": message, "status": name}});
    let mut response = answered("application/json; charset=UTF-8", body.to_string());
    *response.status_mut() = status;
    response
}

fn invalid(message: &str) -> http::Response<Wire> {
    error(StatusCode::BAD_REQUEST, message)
}

fn json_answer(v: Value) -> http::Response<Wire> {
    answered("application/json; charset=UTF-8", v.to_string())
}

fn video() -> http::Response<Wire> {
    let paced = futures::stream::iter(video_chunks()).then(|chunk| async move {
        tokio::time::sleep(Duration::from_millis(5)).await;
        Ok::<_, Infallible>(chunk)
    });
    answered("video/mp4", Wire::from_stream(paced))
}

async fn answer(request: http::Request<Wire>, state: &State) -> http::Response<Wire> {
    let (head, body) = request.into_parts();
    let Ok(body) = body.collect().await.map(|b| b.to_bytes()) else {
        return invalid("The body could not be read.");
    };
    let path = head.uri.path().to_owned();
    let parsed = match body.is_empty() {
        true => Value::Null,
        false => match serde_json::from_slice::<Value>(&body) {
            Ok(v) => v,
            Err(_) => return invalid("Invalid JSON payload received."),
        },
    };
    locked(&state.seen).push(Call {
        method: head.method.to_string(),
        path: path.clone(),
        query: head.uri.query().map(str::to_owned),
        body: parsed.clone(),
    });
    // Storage takes the signed URL it redirected to, not the key.
    if let Some(file) = path.strip_prefix("/storage/") {
        return match head.method == http::Method::GET && !file.is_empty() {
            true => video(),
            false => error(StatusCode::NOT_FOUND, "No such object."),
        };
    }
    let key = head
        .headers
        .get("x-goog-api-key")
        .and_then(|v| v.to_str().ok());
    if key != Some(KEY) {
        return error(
            StatusCode::FORBIDDEN,
            "Method doesn't allow unregistered callers. Please use an API Key.",
        );
    }
    let Some(rest) = path.strip_prefix("/v1beta/") else {
        return error(StatusCode::NOT_FOUND, "Not found.");
    };
    let get = head.method == http::Method::GET;
    if let Some(model) = rest
        .strip_prefix("models/")
        .and_then(|r| r.strip_suffix(":predictLongRunning"))
    {
        if head.method != http::Method::POST {
            return error(StatusCode::NOT_FOUND, "Not found.");
        }
        return create(model, &parsed, state);
    }
    if get && rest.starts_with("models/") && rest.contains("/operations/") {
        if !body.is_empty() {
            return invalid("A GET takes no body.");
        }
        return poll(rest, state);
    }
    if let Some(file) = rest
        .strip_prefix("files/")
        .and_then(|f| f.strip_suffix(":download"))
    {
        if !get || head.uri.query() != Some("alt=media") {
            return invalid("Download requires alt=media.");
        }
        if state.redirect {
            let mut response = answered("text/html", "");
            *response.status_mut() = StatusCode::FOUND;
            let to = format!("{}/storage/{file}?signature=s1", state.origin);
            let to = HeaderValue::from_str(&to).expect("a location");
            response.headers_mut().insert(http::header::LOCATION, to);
            return response;
        }
        return video();
    }
    error(StatusCode::NOT_FOUND, "Not found.")
}

fn create(model: &str, body: &Value, state: &State) -> http::Response<Wire> {
    if !["veo-3.0-generate-001", "veo-3.0-fast-generate-001"].contains(&model) {
        let message = format!("models/{model} is not found for API version v1beta.");
        return error(StatusCode::NOT_FOUND, &message);
    }
    let unknown = |v: &Value, known: &[&str]| {
        let keys = v.as_object().into_iter().flatten().map(|(k, _)| k.as_str());
        keys.into_iter()
            .find(|k| !known.contains(k))
            .map(str::to_owned)
    };
    if let Some(name) = unknown(body, &["instances", "parameters"]) {
        return invalid(&format!(
            "Invalid JSON payload received. Unknown name \"{name}\"."
        ));
    }
    let instances = body["instances"]
        .as_array()
        .map(Vec::as_slice)
        .unwrap_or_default();
    let [instance] = instances else {
        return invalid("Exactly one instance is required.");
    };
    if let Some(name) = unknown(instance, &["prompt", "image"]) {
        return invalid(&format!(
            "Invalid JSON payload received. Unknown name \"{name}\" at 'instances[0]'."
        ));
    }
    let Some(prompt) = instance["prompt"].as_str().filter(|p| !p.is_empty()) else {
        return invalid("A prompt is required.");
    };
    if instance.get("image").is_some() {
        let image = &instance["image"];
        if unknown(image, &["bytesBase64Encoded", "mimeType"]).is_some()
            || !["image/png", "image/jpeg", "image/webp"]
                .contains(&image["mimeType"].as_str().unwrap_or_default())
            || image["bytesBase64Encoded"]
                .as_str()
                .is_none_or(|b| super::unb64_str(b).is_none())
        {
            return invalid("The image must be base64 bytes of a PNG, JPEG or WebP.");
        }
    }
    let parameters = &body["parameters"];
    let known = [
        "aspectRatio",
        "resolution",
        "durationSeconds",
        "negativePrompt",
        "personGeneration",
        "seed",
    ];
    if let Some(name) = unknown(parameters, &known) {
        return invalid(&format!(
            "Invalid JSON payload received. Unknown name \"{name}\" at 'parameters'."
        ));
    }
    if let Some(aspect) = parameters.get("aspectRatio")
        && !["16:9", "9:16"].contains(&aspect.as_str().unwrap_or_default())
    {
        return invalid("aspectRatio must be 16:9 or 9:16.");
    }
    if let Some(resolution) = parameters.get("resolution")
        && !["720p", "1080p"].contains(&resolution.as_str().unwrap_or_default())
    {
        return invalid("resolution must be 720p or 1080p.");
    }
    if let Some(seconds) = parameters.get("durationSeconds")
        && !matches!(seconds.as_u64(), Some(4 | 6 | 8))
    {
        return invalid(
            "The number value for `durationSeconds` is out of bound. Please provide a value between 4 and 8, inclusive.",
        );
    }
    let mut operations = locked(&state.operations);
    let name = format!("models/{model}/operations/op{}", operations.len() + 1);
    operations.insert(name.clone(), (prompt.to_owned(), 0));
    json_answer(json!({"name": name}))
}

/// An operation, done from its second poll: with a video, an error, or, for
/// a prompt the filters take, neither.
fn poll(name: &str, state: &State) -> http::Response<Wire> {
    let mut operations = locked(&state.operations);
    let Some((prompt, polls)) = operations.get_mut(name) else {
        return error(StatusCode::NOT_FOUND, "Operation not found.");
    };
    *polls += 1;
    if *polls == 1 {
        return json_answer(json!({"name": name}));
    }
    let done = match prompt.as_str() {
        "fail" => json!({
            "name": name,
            "done": true,
            "error": {"code": 13, "message": "Video generation failed.", "status": "INTERNAL"},
        }),
        "filtered" => json!({
            "name": name,
            "done": true,
            "response": {
                "@type": "type.googleapis.com/google.ai.generativelanguage.v1beta.PredictLongRunningResponse",
                "generateVideoResponse": {
                    "raiMediaFilteredCount": 1,
                    "raiMediaFilteredReasons": ["The prompt was blocked by safety filters."],
                },
            },
        }),
        _ => {
            let file = name.rsplit('/').next().unwrap_or_default();
            json!({
                "name": name,
                "done": true,
                "response": {
                    "@type": "type.googleapis.com/google.ai.generativelanguage.v1beta.PredictLongRunningResponse",
                    "generateVideoResponse": {
                        "generatedSamples": [{"video": {
                            "uri": format!("{}/v1beta/files/{file}:download?alt=media", state.origin),
                        }}],
                    },
                },
            })
        }
    };
    json_answer(done)
}
