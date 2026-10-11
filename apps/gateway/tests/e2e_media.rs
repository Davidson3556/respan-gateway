//! The media kinds through the shipped binary: images, transcription,
//! speech, embeddings and a video job, each over real HTTP to a process of
//! `respan-gateway` and on to a strict fake of OpenAI's media endpoints.
//!
//! The in-process tests (`media.rs`) check what each kind means; these
//! check what only a process shows: large bodies framed over the wire in
//! both directions, binary bytes untouched, the body limit, and the request
//! log the admin API serves afterwards.
//!
//! Run: `cargo test -p respan-gateway --test e2e_media`.

#![allow(clippy::unwrap_used)]

mod support;

use std::sync::{Arc, Mutex};

use axum::body::{Bytes, to_bytes};
use axum::extract::Request;
use axum::http::{HeaderMap, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use serde_json::{Value, json};
use support::{Gateway, client, json as parse};

/// What the fake was sent, in order: `(method, path, headers, body)`.
type Seen = (Method, String, HeaderMap, Bytes);

#[derive(Default)]
struct State {
    seen: Vec<Seen>,
    /// The video jobs it made, with how many times each was asked after.
    videos: Vec<(String, usize)>,
}

/// OpenAI's media endpoints. It refuses what the real API refuses that a
/// gateway could get wrong: a call without its key, a body or multipart form
/// missing a required field, an audio file that is not the one uploaded, a
/// job it did not make, and the content of a job that is not done.
#[derive(Clone)]
struct Fake {
    base: String,
    state: Arc<Mutex<State>>,
}

/// An uploaded file no text decoder would pass through unchanged, big
/// enough to arrive in many frames.
fn upload() -> Vec<u8> {
    (0..3_000_000u32).map(|i| (i * 7 + i / 251) as u8).collect()
}

/// A synthesized clip, likewise.
fn audio() -> Vec<u8> {
    (0..700_000u32).map(|i| (i * 13 + i / 97) as u8).collect()
}

const VIDEO: &[u8] = b"\x00\x00\x00\x18ftypmp42\xff\x00video";

/// A base64 image of about 1.5 MB, in one JSON string.
fn image() -> String {
    use base64::Engine;
    let pixels: Vec<u8> = (0..1_100_000u32).map(|i| (i * 31 + i / 53) as u8).collect();
    base64::engine::general_purpose::STANDARD.encode(pixels)
}

impl Fake {
    async fn start() -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let fake = Fake {
            base: format!("http://{}", listener.local_addr().unwrap()),
            state: Arc::default(),
        };
        let serving = fake.clone();
        let app = axum::Router::new().fallback(move |req: Request| {
            let fake = serving.clone();
            async move { fake.answer(req).await }
        });
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        fake
    }

    fn seen(&self) -> Vec<Seen> {
        self.state.lock().unwrap().seen.clone()
    }

    async fn answer(&self, req: Request) -> Response {
        let (head, body) = req.into_parts();
        let body = to_bytes(body, 16 << 20).await.unwrap();
        let path = head.uri.path().to_owned();
        let seen = (
            head.method.clone(),
            path.clone(),
            head.headers.clone(),
            body.clone(),
        );
        self.state.lock().unwrap().seen.push(seen);
        if head.headers.get("authorization").map(|v| v.as_bytes()) != Some(b"Bearer sk-media") {
            return error(StatusCode::UNAUTHORIZED, "wrong key");
        }
        let asked: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
        let segments: Vec<&str> = path.split('/').skip(1).collect();
        match (&head.method, segments.as_slice()) {
            (&Method::POST, ["v1", "images", "generations"]) => {
                if !asked["prompt"].is_string() || !asked["model"].is_string() {
                    return error(StatusCode::BAD_REQUEST, "model and prompt are required");
                }
                let usage = json!({"input_tokens": 9, "output_tokens": 70, "total_tokens": 79});
                let data = json!([{"b64_json": image(), "revised_prompt": asked["prompt"]}]);
                axum::Json(json!({"created": 1, "data": data, "usage": usage})).into_response()
            }
            (&Method::POST, ["v1", "audio", "transcriptions"]) => {
                let form = head.headers["content-type"].to_str().unwrap_or_default();
                let Some(boundary) = form.strip_prefix("multipart/form-data; boundary=") else {
                    return error(StatusCode::BAD_REQUEST, "not multipart");
                };
                let parts = multipart(&body, boundary);
                let field = |name: &str| parts.iter().find(|(n, _)| n == name).map(|(_, v)| v);
                if field("model").map(Vec::as_slice) != Some(b"whisper-1") {
                    return error(StatusCode::BAD_REQUEST, "model is required");
                }
                if field("file") != Some(&upload()) {
                    return error(StatusCode::BAD_REQUEST, "the file is not the one uploaded");
                }
                let usage = json!({"type": "duration", "seconds": 4});
                axum::Json(json!({"text": "hello world", "usage": usage})).into_response()
            }
            (&Method::POST, ["v1", "audio", "speech"]) => {
                if !asked["input"].is_string() || !asked["voice"].is_string() {
                    return error(StatusCode::BAD_REQUEST, "input and voice are required");
                }
                ([("content-type", "audio/mpeg")], audio()).into_response()
            }
            (&Method::POST, ["v1", "embeddings"]) => {
                let Some(inputs) = asked["input"].as_array() else {
                    return error(StatusCode::BAD_REQUEST, "input is required");
                };
                let data: Vec<Value> = (0..inputs.len())
                    .map(|i| json!({"object": "embedding", "index": i, "embedding": [0.25, 0.5]}))
                    .collect();
                let usage = json!({"prompt_tokens": 6, "total_tokens": 6});
                axum::Json(json!({"object": "list", "data": data, "model": asked["model"], "usage": usage}))
                    .into_response()
            }
            (&Method::POST, ["v1", "videos"]) => {
                if !asked["prompt"].is_string() {
                    return error(StatusCode::BAD_REQUEST, "prompt is required");
                }
                let mut state = self.state.lock().unwrap();
                let id = format!("video_{}", state.videos.len() + 1);
                state.videos.push((id.clone(), 0));
                axum::Json(job(&id, "queued")).into_response()
            }
            (method, ["v1", "videos", id, rest @ ..]) => {
                let mut state = self.state.lock().unwrap();
                let Some((_, asked)) = state.videos.iter_mut().find(|(v, _)| v == id) else {
                    return error(StatusCode::NOT_FOUND, "no such video");
                };
                match (method, rest) {
                    (&Method::GET, []) => {
                        *asked += 1;
                        let status = if *asked < 2 {
                            "in_progress"
                        } else {
                            "completed"
                        };
                        axum::Json(job(id, status)).into_response()
                    }
                    (&Method::GET, ["content"]) if *asked < 2 => {
                        error(StatusCode::CONFLICT, "the video is not ready")
                    }
                    (&Method::GET, ["content"]) => {
                        ([("content-type", "video/mp4")], VIDEO).into_response()
                    }
                    _ => error(StatusCode::NOT_FOUND, "no such route"),
                }
            }
            _ => error(StatusCode::NOT_FOUND, "no such route"),
        }
    }
}

/// The `(name, bytes)` of each part of a multipart body.
fn multipart(body: &[u8], boundary: &str) -> Vec<(String, Vec<u8>)> {
    let delimiter = format!("--{boundary}").into_bytes();
    let find = |from: &[u8], needle: &[u8]| from.windows(needle.len()).position(|w| w == needle);
    let mut parts = Vec::new();
    let mut rest = body;
    while let Some(at) = find(rest, &delimiter) {
        rest = &rest[at + delimiter.len()..];
        if rest.starts_with(b"--") {
            break;
        }
        let Some(split) = find(rest, b"\r\n\r\n") else {
            break;
        };
        let head = String::from_utf8_lossy(&rest[..split]).into_owned();
        let content = &rest[split + 4..];
        let end = find(content, &[b"\r\n".as_slice(), &delimiter].concat()).unwrap();
        let name = head
            .split("name=\"")
            .nth(1)
            .and_then(|n| n.split('"').next());
        parts.push((name.unwrap_or_default().to_owned(), content[..end].to_vec()));
        rest = &content[end + 2..];
    }
    parts
}

fn job(id: &str, status: &str) -> Value {
    json!({"id": id, "object": "video", "status": status, "model": "sora-2",
        "progress": 0, "created_at": 1, "seconds": "4", "size": "720x1280"})
}

fn error(status: StatusCode, message: &str) -> Response {
    let body = json!({"error": {"message": message, "type": "invalid_request_error"}});
    (status, axum::Json(body)).into_response()
}

fn config(fake: &Fake, extra: &str) -> String {
    format!(
        "providers:\n  openai: {{ vendor: openai, key: sk-media, base_url: '{}/v1' }}\n{extra}",
        fake.base
    )
}

async fn post_json(gw: &Gateway, path: &str, body: Value) -> reqwest::Response {
    client()
        .post(gw.url(path))
        .header("content-type", "application/json")
        .body(body.to_string())
        .send()
        .await
        .unwrap()
}

async fn transcribe(gw: &Gateway, file: Vec<u8>) -> reqwest::Response {
    let boundary = "b0undary";
    let mut body = Vec::new();
    body.extend_from_slice(
        format!(
            "--{boundary}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"a.wav\"\r\n\
             Content-Type: audio/wav\r\n\r\n"
        )
        .as_bytes(),
    );
    body.extend_from_slice(&file);
    body.extend_from_slice(
        format!(
            "\r\n--{boundary}\r\nContent-Disposition: form-data; name=\"model\"\r\n\r\nwhisper-1\r\n\
             --{boundary}\r\nContent-Disposition: form-data; name=\"response_format\"\r\n\r\njson\r\n\
             --{boundary}--\r\n"
        )
        .as_bytes(),
    );
    client()
        .post(gw.url("/v1/audio/transcriptions"))
        .header(
            "content-type",
            format!("multipart/form-data; boundary={boundary}"),
        )
        .body(body)
        .send()
        .await
        .unwrap()
}

/// `resp`'s body as JSON.
async fn body(resp: reqwest::Response) -> Value {
    parse(&resp.text().await.unwrap())
}

async fn get_json(gw: &Gateway, path: &str) -> Value {
    let resp = client().get(gw.url(path)).send().await.unwrap();
    assert_eq!(resp.status(), 200, "{path}");
    body(resp).await
}

/// The request log's record of `path`, once the gateway has finished it.
async fn record(gw: &Gateway, path: &str) -> Value {
    for _ in 0..200 {
        let log = get_json(gw, "/ui/api/requests?limit=50").await;
        let found =
            log["requests"].as_array().unwrap().iter().find(|r| {
                r["path"] == path && !r["status"].is_null() && !r["duration_ms"].is_null()
            });
        if let Some(found) = found {
            return found.clone();
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    panic!("no finished {path} in the request log");
}

#[tokio::test]
async fn an_image_arrives_whole_and_is_logged_with_its_usage() {
    let fake = Fake::start().await;
    let gw = Gateway::start(&config(&fake, "")).await;
    let resp = post_json(
        &gw,
        "/v1/images/generations",
        json!({"model": "gpt-image-1", "prompt": "a cat", "size": "1024x1024"}),
    )
    .await;
    assert_eq!(resp.status(), 200);
    let image: Value = body(resp).await;
    assert_eq!(
        image["data"][0]["b64_json"],
        self::image(),
        "1.5 MB, unchanged"
    );
    assert_eq!(image["data"][0]["revised_prompt"], "a cat");
    let record = record(&gw, "/v1/images/generations").await;
    assert_eq!(record["kind"], "image");
    assert_eq!(record["provider"], "openai");
    assert_eq!(record["model"], "gpt-image-1");
    assert_eq!(
        record["usage"],
        json!({"input_tokens": 9, "output_tokens": 70})
    );
}

#[tokio::test]
async fn a_multi_megabyte_upload_reaches_the_provider_byte_for_byte() {
    let fake = Fake::start().await;
    let gw = Gateway::start(&config(&fake, "")).await;
    let resp = transcribe(&gw, upload()).await;
    assert_eq!(resp.status(), 200, "the fake checks the file it got");
    assert_eq!(body(resp).await["text"], "hello world");
    let sent = fake.seen().pop().unwrap();
    assert!(
        sent.2["content-type"]
            .to_str()
            .unwrap()
            .starts_with("multipart/form-data; boundary="),
        "still multipart: {:?}",
        sent.2
    );
    let record = record(&gw, "/v1/audio/transcriptions").await;
    assert_eq!(record["kind"], "transcription");
    assert_eq!(record["model"], "whisper-1");
    assert_eq!(
        record["usage"],
        json!({"input_tokens": 0, "output_tokens": 0, "seconds": 4.0})
    );
}

#[tokio::test]
async fn an_upload_over_the_body_limit_is_refused_before_the_provider() {
    let fake = Fake::start().await;
    let gw = Gateway::start(&config(&fake, "body_limit: 1MiB\n")).await;
    let resp = transcribe(&gw, upload()).await;
    assert_eq!(resp.status(), 413);
    assert!(fake.seen().is_empty(), "nothing was sent on");
}

#[tokio::test]
async fn speech_comes_back_as_the_same_audio_bytes() {
    let fake = Fake::start().await;
    let gw = Gateway::start(&config(&fake, "")).await;
    let resp = post_json(
        &gw,
        "/v1/audio/speech",
        json!({"model": "gpt-4o-mini-tts", "input": "hi", "voice": "alloy"}),
    )
    .await;
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.headers()["content-type"], "audio/mpeg");
    assert_eq!(resp.bytes().await.unwrap().as_ref(), audio().as_slice());
    let record = record(&gw, "/v1/audio/speech").await;
    assert_eq!(record["kind"], "speech");
    assert_eq!(record["status"], 200);
    assert!(record["error"].is_null(), "{record}");
}

#[tokio::test]
async fn embeddings_are_relayed_and_their_tokens_counted() {
    let fake = Fake::start().await;
    let gw = Gateway::start(&config(&fake, "")).await;
    let resp = post_json(
        &gw,
        "/v1/embeddings",
        json!({"model": "text-embedding-3-small", "input": ["a cat", "a kitten"]}),
    )
    .await;
    assert_eq!(resp.status(), 200);
    let body: Value = body(resp).await;
    assert_eq!(body["data"].as_array().unwrap().len(), 2);
    assert_eq!(body["data"][1]["embedding"], json!([0.25, 0.5]));
    let record = record(&gw, "/v1/embeddings").await;
    assert_eq!(record["kind"], "embedding");
    assert_eq!(
        record["usage"],
        json!({"input_tokens": 6, "output_tokens": 0})
    );
}

#[tokio::test]
async fn a_video_job_is_created_polled_and_downloaded() {
    let fake = Fake::start().await;
    let gw = Gateway::start(&config(&fake, "")).await;
    let resp = post_json(
        &gw,
        "/v1/videos",
        json!({"model": "sora-2", "prompt": "a boat", "seconds": "4"}),
    )
    .await;
    assert_eq!(resp.status(), 200);
    let made: Value = body(resp).await;
    assert_eq!(made["status"], "queued");
    let id = made["id"].as_str().unwrap();

    let early = client()
        .get(gw.url(&format!("/v1/videos/{id}/content")))
        .send()
        .await
        .unwrap();
    assert_eq!(early.status(), 409, "the provider's refusal passes through");

    let mut statuses = Vec::new();
    for _ in 0..2 {
        let polled = get_json(&gw, &format!("/v1/videos/{id}")).await;
        statuses.push(polled["status"].as_str().unwrap().to_owned());
    }
    assert_eq!(statuses, ["in_progress", "completed"].map(String::from));

    let content = client()
        .get(gw.url(&format!("/v1/videos/{id}/content")))
        .send()
        .await
        .unwrap();
    assert_eq!(content.status(), 200);
    assert_eq!(content.headers()["content-type"], "video/mp4");
    assert_eq!(content.bytes().await.unwrap().as_ref(), VIDEO);

    let unknown = client()
        .get(gw.url("/v1/videos/video_404"))
        .send()
        .await
        .unwrap();
    assert_eq!(unknown.status(), 404);

    let created = record(&gw, "/v1/videos").await;
    assert_eq!(created["kind"], "video");
    assert_eq!(created["provider"], "openai");
    let downloaded = record(&gw, &format!("/v1/videos/{id}/content")).await;
    assert_eq!(
        (&downloaded["kind"], &downloaded["status"]),
        (&json!("video"), &json!(200))
    );
}

#[tokio::test]
async fn media_usage_adds_up_in_the_overview_totals() {
    let fake = Fake::start().await;
    let gw = Gateway::start(&config(&fake, "")).await;
    post_json(
        &gw,
        "/v1/embeddings",
        json!({"model": "text-embedding-3-small", "input": ["a", "b"]}),
    )
    .await;
    post_json(
        &gw,
        "/v1/images/generations",
        json!({"model": "gpt-image-1", "prompt": "x"}),
    )
    .await;
    record(&gw, "/v1/images/generations").await;
    record(&gw, "/v1/embeddings").await;
    let overview = get_json(&gw, "/ui/api/overview").await;
    assert_eq!(
        overview["totals"]["input_tokens"], 15,
        "6 embedded plus 9 for the image"
    );
}
