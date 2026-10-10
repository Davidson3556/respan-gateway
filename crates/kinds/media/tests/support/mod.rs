//! A strict local fake of OpenAI's media endpoints, served over HTTP on
//! localhost: it checks each call by the real API's rules, refusing what
//! it refuses (a missing key or field, an unknown field, a file of a type
//! it does not take, a stream a model cannot give), and answers in its
//! framing. A vendor's differences are its [`Rules`].

use std::convert::Infallible;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use axum::body::Body as Wire;
use futures::StreamExt;
use respan_gateway_core::{Extensions, Gateway};
use respan_gateway_http::bytes::Bytes;
use respan_gateway_http::http::{self, HeaderMap, HeaderValue, StatusCode};
use respan_gateway_http::http_body_util::BodyExt;
use respan_gateway_http::{Body, HttpTransport, ProviderIo};
use serde_json::{Map, Value, json};

use crate::http_support::{read_whole, serve};

/// The key the fake takes.
pub const KEY: &str = "sk-test";

/// What a vendor's endpoints take and answer beyond OpenAI's.
#[derive(Debug, Clone, Default)]
pub struct Rules {
    /// Fields it takes that OpenAI's API does not.
    pub extra_fields: Vec<&'static str>,
    /// Fields it adds to a JSON answer.
    pub extra_answer: Map<String, Value>,
    /// Whether it refuses to stream a transcription, as Groq does.
    pub no_streamed_transcription: bool,
}

/// A file of a form the fake was sent.
#[derive(Debug, Clone)]
pub struct File {
    pub name: String,
    pub filename: Option<String>,
    pub content_type: Option<String>,
    pub bytes: Bytes,
}

/// A call the fake took: its path, content type, fields (a form's as
/// strings, `name[]` as an array under that name) and files.
#[derive(Debug, Clone, Default)]
pub struct Seen {
    pub path: String,
    pub content_type: String,
    pub fields: Map<String, Value>,
    pub files: Vec<File>,
}

/// A running fake.
pub struct Fake {
    /// Its base URL, as OpenAI's SDKs take one: with the version.
    pub base: String,
    seen: Arc<Mutex<Vec<Seen>>>,
}

impl Fake {
    /// A fake of OpenAI's own rules.
    pub async fn openai() -> Fake {
        Fake::start(Rules::default()).await
    }

    /// A fake of `rules`.
    pub async fn start(rules: Rules) -> Fake {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("a port");
        let address = listener.local_addr().expect("an address");
        let seen = Arc::new(Mutex::new(Vec::new()));
        let state = (seen.clone(), Arc::new(rules));
        let app = axum::Router::new().fallback(move |request: http::Request<Wire>| {
            let (seen, rules) = state.clone();
            async move { Ok::<_, Infallible>(answer(request, &seen, &rules).await) }
        });
        tokio::spawn(async move { axum::serve(listener, app).await });
        Fake {
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
        let mut headers = HeaderMap::new();
        let key = HeaderValue::from_str(&format!("Bearer {KEY}")).expect("a header");
        headers.insert(http::header::AUTHORIZATION, key);
        HttpTransport::new(&self.base)
            .and_then(|t| t.with_headers(headers))
            .expect("a transport")
            .connect()
    }
}

/// An OpenAI error answer.
fn error(
    status: StatusCode,
    message: &str,
    param: Option<&str>,
    code: &str,
) -> http::Response<Wire> {
    let body = json!({"error": {
        "message": message, "type": "invalid_request_error", "param": param, "code": code,
    }});
    let mut response = http::Response::new(Wire::from(body.to_string()));
    *response.status_mut() = status;
    response.headers_mut().insert(
        http::header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    response
}

fn bad(message: &str, param: Option<&str>) -> http::Response<Wire> {
    error(StatusCode::BAD_REQUEST, message, param, "invalid_value")
}

/// An answer of `content_type` and `body`.
fn answered(content_type: &str, body: impl Into<Wire>) -> http::Response<Wire> {
    let mut response = http::Response::new(body.into());
    let value = HeaderValue::from_str(content_type).expect("a content type");
    response
        .headers_mut()
        .insert(http::header::CONTENT_TYPE, value);
    response
}

fn json_answer(mut v: Value, rules: &Rules) -> http::Response<Wire> {
    if let Some(o) = v.as_object_mut() {
        o.extend(rules.extra_answer.clone());
    }
    answered("application/json", v.to_string())
}

/// Server-sent events, each with its `event:` name, as OpenAI writes them.
fn events(events: Vec<Value>) -> http::Response<Wire> {
    let body: String = events
        .iter()
        .map(|e| {
            format!(
                "event: {}\ndata: {e}\n\n",
                e["type"].as_str().unwrap_or_default()
            )
        })
        .collect();
    answered("text/event-stream", body)
}

async fn answer(
    request: http::Request<Wire>,
    seen: &Mutex<Vec<Seen>>,
    rules: &Rules,
) -> http::Response<Wire> {
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
    let path = head.uri.path().to_owned();
    let read = match content_type.starts_with("multipart/form-data") {
        true => form(&content_type, body).await,
        false => match serde_json::from_slice::<Value>(&body) {
            Ok(Value::Object(fields)) if content_type == "application/json" => Ok(Seen {
                fields,
                ..Seen::default()
            }),
            _ => Err("We could not parse the JSON body of your request."),
        },
    };
    let mut call = match read {
        Ok(call) => call,
        Err(message) => return bad(message, None),
    };
    call.path = path.clone();
    call.content_type = content_type;
    seen.lock()
        .unwrap_or_else(PoisonError::into_inner)
        .push(call.clone());
    if head.method != http::Method::POST {
        return error(
            StatusCode::METHOD_NOT_ALLOWED,
            "Not allowed.",
            None,
            "method",
        );
    }
    match path.as_str() {
        "/v1/images/generations" => images(&call, rules, false),
        "/v1/images/edits" => images(&call, rules, true),
        "/v1/audio/transcriptions" => transcriptions(&call, rules, false),
        "/v1/audio/translations" => transcriptions(&call, rules, true),
        "/v1/audio/speech" => speech(&call, rules),
        _ => error(StatusCode::NOT_FOUND, "Invalid URL.", None, "not_found"),
    }
}

/// A form, read by a parser independent of the gateway's.
async fn form(content_type: &str, body: Bytes) -> Result<Seen, &'static str> {
    let boundary = multer::parse_boundary(content_type).map_err(|_| "No boundary.")?;
    let source = futures::stream::once(async move { Ok::<_, Infallible>(body) });
    let mut parts = multer::Multipart::new(source, boundary);
    let mut seen = Seen::default();
    while let Some(part) = parts.next_field().await.map_err(|_| "A broken form.")? {
        let name = part.name().unwrap_or_default().to_owned();
        let filename = part.file_name().map(str::to_owned);
        let content_type = part.content_type().map(ToString::to_string);
        let bytes = part.bytes().await.map_err(|_| "A broken part.")?;
        if filename.is_some() {
            seen.files.push(File {
                name,
                filename,
                content_type,
                bytes,
            });
            continue;
        }
        let text = String::from_utf8(bytes.to_vec()).map_err(|_| "A field is not UTF-8.")?;
        match seen.fields.get_mut(&name) {
            Some(Value::Array(all)) if name.ends_with("[]") => all.push(text.into()),
            None if name.ends_with("[]") => _ = seen.fields.insert(name, json!([text])),
            None => _ = seen.fields.insert(name, text.into()),
            Some(_) => return Err("A field is repeated."),
        }
    }
    Ok(seen)
}

/// The first field of `call` that neither OpenAI nor the vendor takes.
fn unknown<'a>(call: &'a Seen, known: &[&str], rules: &Rules) -> Option<&'a str> {
    let names = call.fields.keys().map(String::as_str);
    let files = call.files.iter().map(|f| f.name.as_str());
    names
        .chain(files)
        .find(|k| !known.contains(k) && !rules.extra_fields.contains(k))
}

fn unrecognized(field: &str) -> http::Response<Wire> {
    let message = format!("Unrecognized request argument supplied: {field}");
    bad(&message, Some(field))
}

fn missing(param: &str) -> http::Response<Wire> {
    let message = format!("Missing required parameter: '{param}'.");
    error(
        StatusCode::BAD_REQUEST,
        &message,
        Some(param),
        "missing_required_parameter",
    )
}

fn string<'a>(call: &'a Seen, key: &str) -> Option<&'a str> {
    call.fields.get(key).and_then(Value::as_str)
}

fn says_true(call: &Seen, key: &str) -> bool {
    matches!(call.fields.get(key), Some(Value::Bool(true))) || string(call, key) == Some("true")
}

fn number(call: &Seen, key: &str) -> Option<u64> {
    match call.fields.get(key)? {
        Value::Number(n) => n.as_u64(),
        Value::String(s) => s.parse().ok(),
        _ => None,
    }
}

fn no_model(model: &str) -> http::Response<Wire> {
    let message = format!("The model `{model}` does not exist or you do not have access to it.");
    error(StatusCode::NOT_FOUND, &message, None, "model_not_found")
}

/// The bytes of image `i` a fake answers with.
pub fn image_bytes(i: usize) -> Vec<u8> {
    format!("\u{89}PNG fake image {i}").into_bytes()
}

fn b64(bytes: &[u8]) -> String {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

fn images(call: &Seen, rules: &Rules, edit: bool) -> http::Response<Wire> {
    let mut known = vec![
        "model",
        "prompt",
        "n",
        "size",
        "quality",
        "response_format",
        "output_format",
        "output_compression",
        "background",
        "moderation",
        "user",
        "stream",
        "partial_images",
    ];
    known.extend(match edit {
        true => &["image", "image[]", "mask", "input_fidelity"][..],
        false => &["style"][..],
    });
    if let Some(field) = unknown(call, &known, rules) {
        return unrecognized(field);
    }
    let Some(_) = string(call, "prompt").filter(|p| !p.is_empty()) else {
        return missing("prompt");
    };
    let model = string(call, "model").unwrap_or("dall-e-2");
    if !["dall-e-2", "dall-e-3", "gpt-image-1"].contains(&model) {
        return no_model(model);
    }
    if edit {
        let images = call.files.iter().filter(|f| f.name.starts_with("image"));
        if images.clone().count() == 0 {
            return missing("image");
        }
        for file in images.chain(call.files.iter().filter(|f| f.name == "mask")) {
            let mime = file.content_type.as_deref().unwrap_or_default();
            if !["image/png", "image/jpeg", "image/webp"].contains(&mime) {
                let message = format!(
                    "Invalid file '{}': unsupported mimetype ('{mime}'). Supported file formats are 'image/jpeg', 'image/png', and 'image/webp'.",
                    file.name
                );
                return bad(&message, Some(&file.name));
            }
        }
    }
    let gpt = model.starts_with("gpt-image");
    let usage = json!({
        "input_tokens": 50, "output_tokens": 4160, "total_tokens": 4210,
        "input_tokens_details": {"text_tokens": 40, "image_tokens": 10},
    });
    let settings = json!({
        "size": string(call, "size").unwrap_or("1024x1024"),
        "quality": string(call, "quality").unwrap_or("high"),
        "background": string(call, "background").unwrap_or("opaque"),
        "output_format": string(call, "output_format").unwrap_or("png"),
    });
    if says_true(call, "stream") {
        if !gpt {
            return bad("Streaming is not supported for this model.", Some("stream"));
        }
        let prefix = if edit {
            "image_edit"
        } else {
            "image_generation"
        };
        let mut all = Vec::new();
        for i in 0..number(call, "partial_images").unwrap_or(0) {
            let mut e = json!({
                "type": format!("{prefix}.partial_image"),
                "b64_json": b64(format!("partial {i}").as_bytes()),
                "created_at": 1_700_000_000,
                "partial_image_index": i,
            });
            merge(&mut e, &settings);
            all.push(e);
        }
        let mut done = json!({
            "type": format!("{prefix}.completed"),
            "b64_json": b64(&image_bytes(0)),
            "created_at": 1_700_000_000,
            "usage": usage,
        });
        merge(&mut done, &settings);
        all.push(done);
        return events(all);
    }
    let url = string(call, "response_format") == Some("url");
    if url && gpt {
        return bad(
            "Unknown parameter: 'response_format'.",
            Some("response_format"),
        );
    }
    let n = number(call, "n").unwrap_or(1) as usize;
    let data: Vec<Value> = (0..n)
        .map(|i| match url {
            true => json!({"url": format!("https://images.test/{i}.png"), "revised_prompt": "a cat, revised"}),
            false => json!({"b64_json": b64(&image_bytes(i))}),
        })
        .collect();
    let mut body = json!({"created": 1_700_000_000, "data": data});
    if gpt {
        body["usage"] = usage;
        merge(&mut body, &settings);
    }
    json_answer(body, rules)
}

fn merge(v: &mut Value, more: &Value) {
    if let (Some(v), Some(more)) = (v.as_object_mut(), more.as_object()) {
        v.extend(more.clone());
    }
}

/// The transcript a fake answers with.
pub const SPOKEN: &str = "Hello there. General Kenobi.";

/// The SubRip a fake answers with.
pub const SUBRIP: &str = "1\n00:00:00,000 --> 00:00:01,500\nHello there.\n\n2\n00:00:01,500 --> 00:00:03,000\nGeneral Kenobi.\n\n";

fn transcriptions(call: &Seen, rules: &Rules, translate: bool) -> http::Response<Wire> {
    let mut known = vec!["file", "model", "prompt", "response_format", "temperature"];
    if !translate {
        known.extend([
            "language",
            "timestamp_granularities[]",
            "include[]",
            "stream",
            "chunking_strategy",
            "known_speaker_names[]",
            "known_speaker_references[]",
        ]);
    }
    if let Some(field) = unknown(call, &known, rules) {
        return unrecognized(field);
    }
    if !call.files.iter().any(|f| f.name == "file") {
        return missing("file");
    }
    let Some(model) = string(call, "model") else {
        return missing("model");
    };
    let gpt = model.starts_with("gpt-4o");
    if ![
        "whisper-1",
        "whisper-large-v3",
        "gpt-4o-transcribe",
        "gpt-4o-mini-transcribe",
    ]
    .contains(&model)
    {
        return no_model(model);
    }
    let format = string(call, "response_format").unwrap_or("json");
    let formats: &[&str] = match (gpt, translate) {
        (true, _) => &["json", "text"],
        (false, true) => &["json", "text", "srt", "verbose_json", "vtt"],
        (false, false) => &["json", "text", "srt", "verbose_json", "vtt"],
    };
    if !formats.contains(&format) {
        let message = format!("response_format '{format}' is not compatible with model '{model}'.");
        return bad(&message, Some("response_format"));
    }
    let tokens = json!({
        "type": "tokens", "input_tokens": 14, "output_tokens": 7, "total_tokens": 21,
        "input_token_details": {"text_tokens": 0, "audio_tokens": 14},
    });
    let duration = json!({"type": "duration", "seconds": 3});
    if says_true(call, "stream") {
        if !gpt || rules.no_streamed_transcription {
            return bad("Streaming is not supported for this model.", Some("stream"));
        }
        let mut all: Vec<Value> = ["Hello", " there.", " General", " Kenobi."]
            .iter()
            .map(|d| json!({"type": "transcript.text.delta", "delta": d}))
            .collect();
        all.push(json!({"type": "transcript.text.done", "text": SPOKEN, "usage": tokens}));
        // OpenAI names no event: its SDKs read each by its `type`.
        let body: String = all.iter().map(|e| format!("data: {e}\n\n")).collect();
        return answered("text/event-stream", body);
    }
    let usage = if gpt { tokens } else { duration };
    match format {
        "text" => answered("text/plain; charset=utf-8", format!("{SPOKEN}\n")),
        "srt" | "vtt" => answered("text/plain; charset=utf-8", SUBRIP),
        "verbose_json" => {
            let segment = |id: u64, start: f64, end: f64, text: &str| {
                json!({
                    "id": id, "seek": 0, "start": start, "end": end, "text": text,
                    "tokens": [50364, 2425], "temperature": 0.0, "avg_logprob": -0.25,
                    "compression_ratio": 0.8, "no_speech_prob": 0.01,
                })
            };
            let mut body = json!({
                "task": if translate { "translate" } else { "transcribe" },
                "language": "english",
                "duration": 3.0,
                "text": SPOKEN,
                "segments": [
                    segment(0, 0.0, 1.5, " Hello there."),
                    segment(1, 1.5, 3.0, " General Kenobi."),
                ],
                "usage": usage,
            });
            let granularities = call.fields.get("timestamp_granularities[]");
            if granularities
                .is_some_and(|g| g.as_array().is_some_and(|g| g.contains(&json!("word"))))
            {
                body["words"] = json!([{"word": "Hello", "start": 0.0, "end": 0.5}]);
            }
            json_answer(body, rules)
        }
        _ => json_answer(json!({"text": SPOKEN, "usage": usage}), rules),
    }
}

/// The audio a fake speaks: 64 KiB in 8 chunks.
pub fn spoken_audio() -> Vec<Bytes> {
    (0..8u8)
        .map(|i| Bytes::from((0..8192).map(|j| (j as u8) ^ i).collect::<Vec<u8>>()))
        .collect()
}

fn speech(call: &Seen, rules: &Rules) -> http::Response<Wire> {
    let known = [
        "model",
        "input",
        "voice",
        "instructions",
        "response_format",
        "speed",
        "stream_format",
    ];
    if let Some(field) = unknown(call, &known, rules) {
        return unrecognized(field);
    }
    for required in ["model", "input", "voice"] {
        if !call.fields.contains_key(required) {
            return missing(required);
        }
    }
    let model = string(call, "model").unwrap_or_default();
    if !["tts-1", "tts-1-hd", "gpt-4o-mini-tts"].contains(&model) {
        return no_model(model);
    }
    let chunks = spoken_audio();
    if string(call, "stream_format") == Some("sse") {
        if model != "gpt-4o-mini-tts" {
            return bad(
                "stream_format 'sse' is not supported for this model.",
                Some("stream_format"),
            );
        }
        let mut all: Vec<Value> = chunks
            .iter()
            .map(|c| json!({"type": "speech.audio.delta", "audio": b64(c)}))
            .collect();
        all.push(json!({
            "type": "speech.audio.done",
            "usage": {"input_tokens": 5, "output_tokens": 40, "total_tokens": 45},
        }));
        let body: String = all.iter().map(|e| format!("data: {e}\n\n")).collect();
        return answered("text/event-stream", body);
    }
    let mime = match string(call, "response_format").unwrap_or("mp3") {
        "wav" => "audio/wav",
        "opus" => "audio/opus",
        _ => "audio/mpeg",
    };
    let paced = futures::stream::iter(chunks).then(|chunk| async move {
        tokio::time::sleep(Duration::from_millis(5)).await;
        Ok::<_, Infallible>(chunk)
    });
    answered(mime, Wire::from_stream(paced))
}

/// A gateway serving `format` to clients, relaying to `fake` as provider
/// `openai`.
pub fn gateway<I, O, F>(format: F, fake: &Fake) -> Gateway
where
    I: Clone + Send + Sync + 'static,
    O: Clone + Send + Sync + 'static,
    F: respan_gateway_core::Format<respan_gateway_http::ClientIo, I, O>
        + respan_gateway_core::Format<ProviderIo, O, I>
        + Copy,
{
    Gateway::new()
        .provider("openai", format, fake.transport())
        .and_then(|g| g.client::<I, O, respan_gateway_http::ClientIo>(format))
        .and_then(|g| g.target("openai"))
        .expect("a gateway")
}

/// A client's request of `content_type` and `body` to `path`.
pub fn request(path: &str, content_type: &str, body: impl Into<Bytes>) -> http::Request<Body> {
    let mut request = crate::http_support::post(path, body.into());
    let value = HeaderValue::from_str(content_type).expect("a content type");
    request
        .headers_mut()
        .insert(http::header::CONTENT_TYPE, value);
    request
}

/// A client's JSON request to `path`.
pub fn json_request(path: &str, body: Value) -> http::Request<Body> {
    request(path, "application/json", body.to_string())
}

/// A form a client sends, written apart from the gateway's writer.
#[derive(Default)]
pub struct Form(Vec<u8>);

const BOUNDARY: &str = "----client-form-7MA4YWxkTrZu0gW";

impl Form {
    pub fn text(mut self, name: &str, value: &str) -> Self {
        self.0.extend(
            format!(
                "--{BOUNDARY}\r\nContent-Disposition: form-data; name=\"{name}\"\r\n\r\n{value}\r\n"
            )
            .into_bytes(),
        );
        self
    }

    pub fn file(mut self, name: &str, filename: &str, content_type: &str, bytes: &[u8]) -> Self {
        self.0.extend(
            format!(
                "--{BOUNDARY}\r\nContent-Disposition: form-data; name=\"{name}\"; filename=\"{filename}\"\r\nContent-Type: {content_type}\r\n\r\n"
            )
            .into_bytes(),
        );
        self.0.extend_from_slice(bytes);
        self.0.extend_from_slice(b"\r\n");
        self
    }

    /// The client's request of this form to `path`.
    pub fn to(mut self, path: &str) -> http::Request<Body> {
        self.0.extend(format!("--{BOUNDARY}--\r\n").into_bytes());
        let content_type = format!("multipart/form-data; boundary={BOUNDARY}");
        request(path, &content_type, self.0)
    }
}

/// What the gateway answers `request`: its status, headers and whole body.
pub async fn send(
    gateway: &Gateway,
    request: http::Request<Body>,
) -> (StatusCode, HeaderMap, Bytes) {
    let (response, _) = serve(gateway, request).await;
    let (status, headers, body) = read_whole(response).await;
    (status, headers, body.expect("a whole body"))
}

/// What the gateway answers `request`, frame by frame.
pub async fn send_frames(
    gateway: &Gateway,
    request: http::Request<Body>,
) -> (StatusCode, HeaderMap, Vec<Bytes>) {
    let (response, _) = serve(gateway, request).await;
    let (head, mut body) = response.into_parts();
    let mut frames = Vec::new();
    while let Some(frame) = body.frame().await {
        if let Ok(data) = frame.expect("a frame").into_data() {
            frames.push(data);
        }
    }
    (head.status, head.headers, frames)
}

/// A JSON body.
pub fn json_of(body: &[u8]) -> Value {
    serde_json::from_slice(body)
        .unwrap_or_else(|e| panic!("not JSON ({e}): {}", String::from_utf8_lossy(body)))
}

/// The JSON data of each event of a stream.
pub fn events_of(body: &[u8]) -> Vec<Value> {
    String::from_utf8_lossy(body)
        .split("\n\n")
        .filter_map(|event| {
            let data: Vec<&str> = event
                .lines()
                .filter_map(|l| l.strip_prefix("data: "))
                .collect();
            (!data.is_empty()).then(|| json_of(data.join("\n").as_bytes()))
        })
        .collect()
}

/// The content type of a response.
pub fn content_type(headers: &HeaderMap) -> &str {
    headers
        .get(http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
}

/// Base64 `text` decoded.
pub fn unb64(text: &Value) -> Vec<u8> {
    use base64::Engine;
    let text = text.as_str().expect("base64 text");
    base64::engine::general_purpose::STANDARD
        .decode(text)
        .expect("base64")
}
