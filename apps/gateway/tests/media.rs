//! The media kinds through the application, in process: each kind relayed
//! to its provider, an OpenAI Videos client answered by Veo, models routed
//! by prefix within their kind, video jobs' follow-ups routed by id, the
//! kinds of `/v1/models`, and media usage in the request log. The providers
//! are a strict fake of OpenAI's media endpoints and of Veo, below.

#![allow(clippy::unwrap_used)]

use std::sync::{Arc, Mutex};

use axum::body::{Body, Bytes, to_bytes};
use axum::extract::Request;
use axum::http::{HeaderMap, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use respan_gateway::{App, Config};
use serde_json::{Value, json};
use tower::ServiceExt;

/// What a provider of the fake was sent.
#[derive(Debug, Clone)]
struct Seen {
    method: Method,
    path: String,
    headers: HeaderMap,
    body: Bytes,
}

impl Seen {
    fn json(&self) -> Value {
        serde_json::from_slice(&self.body).unwrap()
    }

    fn key(&self) -> Option<&str> {
        let key = self.headers.get("authorization");
        let key = key.or_else(|| self.headers.get("x-goog-api-key"));
        key.and_then(|v| v.to_str().ok())
    }
}

#[derive(Default)]
struct State {
    seen: Vec<Seen>,
    /// The video jobs it made, by id.
    videos: Vec<String>,
}

/// A vendor's media endpoints: OpenAI's under `/v1`, Veo's under `/v1beta`.
/// It refuses what the real APIs refuse that the gateway could get wrong:
/// a call without a key, a body without its required fields, a job it did
/// not make.
#[derive(Clone)]
struct Fake {
    base: String,
    state: Arc<Mutex<State>>,
}

/// Bytes no text decoder would pass through unchanged.
const AUDIO: &[u8] = b"ID3\x00\xff\xfe\x80audio";
const VIDEO: &[u8] = b"\x00\x00\x00\x18ftypmp42\xff\x00video";

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

    fn last(&self) -> Seen {
        self.seen().pop().expect("a call")
    }

    async fn answer(&self, req: Request) -> Response {
        let (head, body) = req.into_parts();
        let seen = Seen {
            method: head.method,
            path: head.uri.to_string(),
            headers: head.headers,
            body: to_bytes(body, 1 << 20).await.unwrap(),
        };
        self.state.lock().unwrap().seen.push(seen.clone());
        if seen.key().is_none() {
            return error(StatusCode::UNAUTHORIZED, "no key");
        }
        let path = seen.path.split('?').next().unwrap_or_default().to_owned();
        let segments: Vec<&str> = path.split('/').skip(1).collect();
        match (&seen.method, segments.as_slice()) {
            (&Method::GET, ["v1", "models"]) => {
                let ids = [
                    "gpt-4o-mini",
                    "gpt-image-1",
                    "whisper-1",
                    "gpt-4o-mini-tts",
                    "sora-2",
                    "text-embedding-3-small",
                ];
                let data: Vec<Value> = ids.iter().map(|id| json!({"id": id})).collect();
                axum::Json(json!({"object": "list", "data": data})).into_response()
            }
            (&Method::POST, ["v1", "images", "generations"]) => {
                let asked = seen.json();
                if !asked["prompt"].is_string() {
                    return error(StatusCode::BAD_REQUEST, "prompt is required");
                }
                let image = json!({"b64_json": "aW1hZ2U=", "revised_prompt": asked["prompt"]});
                let usage = json!({"input_tokens": 9, "output_tokens": 70, "total_tokens": 79});
                axum::Json(json!({"created": 1, "data": [image], "usage": usage})).into_response()
            }
            (&Method::POST, ["v1", "embeddings"]) => {
                let asked = seen.json();
                if !asked["input"].is_string() {
                    return error(StatusCode::BAD_REQUEST, "input is required");
                }
                let item = json!({"object": "embedding", "index": 0, "embedding": [0.25, 0.5]});
                let usage = json!({"prompt_tokens": 3, "total_tokens": 3});
                axum::Json(json!({"object": "list", "data": [item], "model": asked["model"], "usage": usage}))
                    .into_response()
            }
            (&Method::POST, ["v1", "audio", "transcriptions"]) => {
                let form = seen.headers["content-type"].to_str().unwrap();
                let has_file = String::from_utf8_lossy(&seen.body).contains("name=\"file\"");
                if !form.starts_with("multipart/form-data") || !has_file {
                    return error(StatusCode::BAD_REQUEST, "file is required");
                }
                let usage = json!({"type": "duration", "seconds": 4});
                axum::Json(json!({"text": "hello", "usage": usage})).into_response()
            }
            (&Method::POST, ["v1", "audio", "speech"]) => {
                let asked = seen.json();
                if !asked["input"].is_string() || !asked["voice"].is_string() {
                    return error(StatusCode::BAD_REQUEST, "input and voice are required");
                }
                ([("content-type", "audio/mpeg")], AUDIO).into_response()
            }
            (&Method::POST, ["v1", "videos"]) => {
                let asked = seen.json();
                if !asked["prompt"].is_string() {
                    return error(StatusCode::BAD_REQUEST, "prompt is required");
                }
                let mut state = self.state.lock().unwrap();
                let id = format!("video_{}", state.videos.len() + 1);
                state.videos.push(id.clone());
                axum::Json(job(&id, "queued")).into_response()
            }
            (method, ["v1", "videos", id, rest @ ..]) => {
                let made = self.state.lock().unwrap().videos.iter().any(|v| v == id);
                match (made, method, rest) {
                    (false, ..) => error(StatusCode::NOT_FOUND, "no such video"),
                    (true, &Method::GET, []) => axum::Json(job(id, "completed")).into_response(),
                    (true, &Method::GET, ["content"]) => {
                        ([("content-type", "video/mp4")], VIDEO).into_response()
                    }
                    (true, &Method::DELETE, []) => {
                        let deleted = json!({"id": id, "object": "video.deleted", "deleted": true});
                        axum::Json(deleted).into_response()
                    }
                    _ => error(StatusCode::NOT_FOUND, "no such route"),
                }
            }
            (&Method::POST, ["v1beta", "models", action]) => {
                let Some((model, "predictLongRunning")) = action.split_once(':') else {
                    return error(StatusCode::NOT_FOUND, "no such action");
                };
                if !seen.json()["instances"][0]["prompt"].is_string() {
                    return error(StatusCode::BAD_REQUEST, "prompt is required");
                }
                let name = format!("models/{model}/operations/op1");
                axum::Json(json!({"name": name})).into_response()
            }
            (&Method::GET, ["v1beta", "models", model, "operations", op]) => {
                let uri = format!("{}/v1beta/files/{op}:download?alt=media", self.base);
                let sample = json!({"video": {"uri": uri}});
                let response = json!({"generateVideoResponse": {"generatedSamples": [sample]}});
                let name = format!("models/{model}/operations/{op}");
                axum::Json(json!({"name": name, "done": true, "response": response}))
                    .into_response()
            }
            (&Method::GET, ["v1beta", "files", _]) => {
                ([("content-type", "video/mp4")], VIDEO).into_response()
            }
            _ => error(StatusCode::NOT_FOUND, "no such route"),
        }
    }
}

fn job(id: &str, status: &str) -> Value {
    json!({"id": id, "object": "video", "status": status, "model": "sora-2",
        "progress": 0, "created_at": 1, "seconds": "4", "size": "720x1280"})
}

fn error(status: StatusCode, message: &str) -> Response {
    let body = json!({"error": {"message": message, "type": "invalid_request_error"}});
    (status, axum::Json(body)).into_response()
}

fn app(providers: &str) -> App {
    let yaml = format!("log: off\nproviders:\n{providers}");
    App::new(Config::parse(&yaml).unwrap()).unwrap()
}

/// `(status, content type, body)` of `request`, whole.
async fn send(app: &App, request: axum::http::Request<Body>) -> (StatusCode, String, Bytes) {
    let resp = app.router().oneshot(request).await.unwrap();
    let status = resp.status();
    let content_type = resp.headers().get("content-type");
    let content_type = content_type.map_or("", |v| v.to_str().unwrap()).to_owned();
    let body = to_bytes(resp.into_body(), 1 << 20).await.unwrap();
    (status, content_type, body)
}

async fn post(app: &App, path: &str, body: Value) -> (StatusCode, Value) {
    let request = axum::http::Request::post(path)
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    let (status, _, body) = send(app, request).await;
    (status, serde_json::from_slice(&body).unwrap_or(Value::Null))
}

async fn get(app: &App, path: &str) -> (StatusCode, String, Bytes) {
    send(
        app,
        axum::http::Request::get(path).body(Body::empty()).unwrap(),
    )
    .await
}

async fn get_json(app: &App, path: &str) -> Value {
    let (status, _, body) = get(app, path).await;
    assert_eq!(status, StatusCode::OK, "{path}: {body:?}");
    serde_json::from_slice(&body).unwrap()
}

async fn transcribe(app: &App, model: &str) -> (StatusCode, Value) {
    let boundary = "b0undary";
    let body = format!(
        "--{boundary}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"a.wav\"\r\n\
         Content-Type: audio/wav\r\n\r\nRIFF\0\0\0\0WAVE\r\n\
         --{boundary}\r\nContent-Disposition: form-data; name=\"model\"\r\n\r\n{model}\r\n\
         --{boundary}\r\nContent-Disposition: form-data; name=\"response_format\"\r\n\r\njson\r\n\
         --{boundary}--\r\n"
    );
    let request = axum::http::Request::post("/v1/audio/transcriptions")
        .header(
            "content-type",
            format!("multipart/form-data; boundary={boundary}"),
        )
        .body(Body::from(body))
        .unwrap();
    let (status, _, body) = send(app, request).await;
    (status, serde_json::from_slice(&body).unwrap_or(Value::Null))
}

#[tokio::test]
async fn each_media_kind_is_relayed_to_its_provider_and_logged() {
    let openai = Fake::start().await;
    let app = app(&format!(
        "  openai: {{ vendor: openai, key: sk-o, base_url: '{}/v1' }}",
        openai.base
    ));

    let (status, image) = post(
        &app,
        "/v1/images/generations",
        json!({"model": "gpt-image-1", "prompt": "a cat", "size": "1024x1024", "n": 1}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{image}");
    assert_eq!(image["data"][0]["b64_json"], "aW1hZ2U=");
    assert_eq!(openai.last().path, "/v1/images/generations");
    assert_eq!(openai.last().key(), Some("Bearer sk-o"));

    let (status, transcript) = transcribe(&app, "whisper-1").await;
    assert_eq!(status, StatusCode::OK, "{transcript}");
    assert_eq!(transcript["text"], "hello");

    let speech = json!({"model": "gpt-4o-mini-tts", "input": "hi", "voice": "alloy"});
    let request = axum::http::Request::post("/v1/audio/speech")
        .header("content-type", "application/json")
        .body(Body::from(speech.to_string()))
        .unwrap();
    let (status, content_type, audio) = send(&app, request).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(content_type, "audio/mpeg");
    assert_eq!(audio.as_ref(), AUDIO, "binary bytes pass unchanged");

    let (status, embedded) = post(
        &app,
        "/v1/embeddings",
        json!({"model": "text-embedding-3-small", "input": "hi"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{embedded}");
    assert_eq!(embedded["data"][0]["embedding"], json!([0.25, 0.5]));
    assert_eq!(openai.last().path, "/v1/embeddings");

    let (status, job) = post(
        &app,
        "/v1/videos",
        json!({"model": "sora-2", "prompt": "a boat", "seconds": "4"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{job}");
    // One provider of the format: its ids are its own.
    assert_eq!(job["id"], "video_1");
    let retrieved = get_json(&app, "/v1/videos/video_1").await;
    assert_eq!(retrieved["status"], "completed");
    let (status, content_type, video) = get(&app, "/v1/videos/video_1/content").await;
    assert_eq!(
        (status, content_type.as_str()),
        (StatusCode::OK, "video/mp4")
    );
    assert_eq!(video.as_ref(), VIDEO);
    let deleted = axum::http::Request::delete("/v1/videos/video_1")
        .body(Body::empty())
        .unwrap();
    assert_eq!(send(&app, deleted).await.0, StatusCode::OK);

    app.gateway().idle().await;
    let log = get_json(&app, "/ui/api/requests?limit=10").await;
    let log = log["requests"].as_array().unwrap();
    let record = |path: &str| {
        let found = log.iter().find(|r| r["path"] == path);
        found
            .unwrap_or_else(|| panic!("no {path} in {log:?}"))
            .clone()
    };
    let image = record("/v1/images/generations");
    assert_eq!(image["kind"], "image");
    assert_eq!(image["format"], "openai-images");
    assert_eq!(image["model"], "gpt-image-1");
    assert_eq!(image["provider"], "openai");
    assert_eq!(
        image["usage"],
        json!({"input_tokens": 9, "output_tokens": 70})
    );
    let transcription = record("/v1/audio/transcriptions");
    assert_eq!(transcription["kind"], "transcription");
    assert_eq!(transcription["model"], "whisper-1");
    assert_eq!(
        transcription["usage"],
        json!({"input_tokens": 0, "output_tokens": 0, "seconds": 4.0})
    );
    let speech = record("/v1/audio/speech");
    assert_eq!(
        (&speech["kind"], &speech["status"]),
        (&json!("speech"), &json!(200))
    );
    assert!(speech["error"].is_null(), "{speech}");
    let embedding = record("/v1/embeddings");
    assert_eq!(embedding["kind"], "embedding");
    assert_eq!(embedding["model"], "text-embedding-3-small");
    assert_eq!(
        embedding["usage"],
        json!({"input_tokens": 3, "output_tokens": 0})
    );
    let content = record("/v1/videos/video_1/content");
    assert_eq!(
        (&content["kind"], &content["provider"]),
        (&json!("video"), &json!("openai"))
    );
    assert!(content["error"].is_null(), "{content}");
    let overview = get_json(&app, "/ui/api/overview").await;
    assert_eq!(overview["totals"]["input_tokens"], 12);
}

#[tokio::test]
async fn an_openai_videos_client_is_answered_by_veo() {
    let google = Fake::start().await;
    let app = app(&format!(
        "  gemini: {{ vendor: gemini, key: AIza, base_url: '{}/v1beta/openai' }}",
        google.base
    ));
    let (status, job) = post(
        &app,
        "/v1/videos",
        json!({"model": "veo-3.0-generate-001", "prompt": "a boat", "seconds": "8"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{job}");
    let created = google.last();
    assert_eq!(
        created.path,
        "/v1beta/models/veo-3.0-generate-001:predictLongRunning"
    );
    assert_eq!(created.key(), Some("AIza"));
    assert_eq!(created.json()["parameters"]["durationSeconds"], 8);
    let id = job["id"].as_str().unwrap();
    assert!(id.starts_with("veo_"), "{id}");
    assert_eq!(job["status"], "in_progress");

    let retrieved = get_json(&app, &format!("/v1/videos/{id}")).await;
    assert_eq!(retrieved["status"], "completed");
    let (status, content_type, video) = get(&app, &format!("/v1/videos/{id}/content")).await;
    assert_eq!(status, StatusCode::OK, "{video:?}");
    assert_eq!(content_type, "video/mp4");
    assert_eq!(video.as_ref(), VIDEO);
    assert_eq!(google.last().path, "/v1beta/files/op1:download?alt=media");
    assert_eq!(google.last().key(), Some("AIza"));
}

#[tokio::test]
async fn a_model_prefix_routes_only_within_the_kinds_its_provider_serves() {
    let (groq, openai) = (Fake::start().await, Fake::start().await);
    let app = app(&format!(
        "  groq: {{ vendor: groq, key: gsk, base_url: '{}/v1' }}
  openai: {{ vendor: openai, key: sk-o, base_url: '{}/v1' }}",
        groq.base, openai.base
    ));
    for (model, to) in [
        ("whisper-large-v3", &groq),
        ("whisper-1", &openai),
        ("gpt-4o-mini-transcribe", &openai),
        // No preset claims it: the first provider of the kind.
        ("some-asr", &groq),
    ] {
        let before = to.seen().len();
        let (status, said) = transcribe(&app, model).await;
        assert_eq!(status, StatusCode::OK, "{model}: {said}");
        assert_eq!(to.seen().len(), before + 1, "{model}");
    }
    // Groq serves no images: whatever the model, OpenAI draws.
    let (status, _) = post(
        &app,
        "/v1/images/generations",
        json!({"model": "llama-image", "prompt": "a cat"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(openai.last().path, "/v1/images/generations");
    let routes = get_json(&app, "/ui/api/routes").await;
    let defaults = &routes["defaults"];
    assert!(
        defaults
            .as_array()
            .unwrap()
            .contains(&json!({"kind": "image", "provider": "openai"})),
        "{defaults}"
    );
}

#[tokio::test]
async fn a_video_follow_up_goes_to_the_provider_that_made_the_job() {
    let (east, west, google) = (
        Fake::start().await,
        Fake::start().await,
        Fake::start().await,
    );
    let app = app(&format!(
        "  east: {{ formats: openai-videos, key: e, base_url: '{}/v1' }}
  west: {{ formats: openai-videos, key: w, base_url: '{}/v1' }}
  veo: {{ formats: gemini-veo, key: g, base_url: '{}/v1beta' }}",
        east.base, west.base, google.base
    ));
    let create = |model: &'static str| {
        post(
            &app,
            "/v1/videos",
            json!({"model": model, "prompt": "a boat"}),
        )
    };
    // Both make `video_1`: the ids say whose.
    let (_, from_east) = create("east/sora-2").await;
    let (_, from_west) = create("west/sora-2").await;
    assert_eq!(from_east["id"], "east~video_1");
    assert_eq!(from_west["id"], "west~video_1");
    let retrieved = get_json(&app, "/v1/videos/west~video_1").await;
    assert_eq!(retrieved["id"], "west~video_1");
    assert_eq!(west.last().path, "/v1/videos/video_1");
    assert_eq!(west.last().key(), Some("Bearer w"));
    let (status, _, video) = get(&app, "/v1/videos/east~video_1/content").await;
    assert_eq!((status, video.as_ref()), (StatusCode::OK, VIDEO));
    assert_eq!(east.last().path, "/v1/videos/video_1/content");

    // Veo's ids are its own spelling, and Veo alone speaks it.
    let (status, job) = create("veo/veo-3.0-generate-001").await;
    assert_eq!(status, StatusCode::OK, "{job}");
    let id = job["id"].as_str().unwrap();
    assert!(id.starts_with("veo_"), "{id}");
    let retrieved = get_json(&app, &format!("/v1/videos/{id}")).await;
    assert_eq!(retrieved["status"], "completed");
    assert!(google.last().path.contains("/operations/op1"));

    // An id no provider made is the default provider's to refuse.
    let (status, ..) = get(&app, "/v1/videos/nobody~video_9").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn models_are_listed_with_their_kinds() {
    let openai = Fake::start().await;
    let app = app(&format!(
        "  openai: {{ vendor: openai, key: sk-o, base_url: '{}/v1' }}",
        openai.base
    ));
    let list = get_json(&app, "/v1/models").await;
    let kinds: Vec<(&str, &str)> = list["data"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| (m["id"].as_str().unwrap(), m["kind"].as_str().unwrap()))
        .collect();
    assert_eq!(
        kinds,
        [
            ("gpt-4o-mini", "chat"),
            ("gpt-image-1", "image"),
            ("whisper-1", "transcription"),
            ("gpt-4o-mini-tts", "speech"),
            ("sora-2", "video"),
            ("text-embedding-3-small", "embedding"),
        ]
    );

    // A provider of a media format alone lists the models it serves.
    let images = Fake::start().await;
    let app = self::app(&format!(
        "  images: {{ formats: openai-images, key: k, base_url: '{}/v1' }}",
        images.base
    ));
    let list = get_json(&app, "/v1/models").await;
    assert_eq!(
        list["data"],
        json!([{"id": "gpt-image-1", "object": "model", "owned_by": "images", "kind": "image"}])
    );
}
