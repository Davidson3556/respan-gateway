//! Google's Veo as the Gemini API serves it, over HTTP: a provider format
//! of the [`video`](respan_gateway_media::video) kind, [`GeminiVeo`]. A provider's base URL
//! includes the version, `https://generativelanguage.googleapis.com/v1beta`,
//! and its transport the key, `x-goog-api-key`.

use crate::edge::{bind_provider, content, kept, sends};
use base64::Engine;
use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
use respan_gateway_conversation::{Media, MediaSource};
use respan_gateway_core::{
    BadRequest, Connection, ErrorCode, EventStream, Fault, Format, Upstream,
};
use respan_gateway_http::bytes::Bytes;
use respan_gateway_http::formats::whole;
use respan_gateway_http::http::header::CONTENT_TYPE;
use respan_gateway_http::http::{self, HeaderValue, Method, Uri, request::Parts as RequestHead};
use respan_gateway_http::{Body, Encoder, ProviderIo};
use serde_json::{Map, Value, json};
use std::sync::{Arc, OnceLock};

use respan_gateway_media::video::{Create, Input, Job, JobError, Output, Status, Variant};

const NAME: &str = "gemini-veo";
/// What starts the id of a job of Veo's.
const PREFIX: &str = "veo_";

/// Google's Veo, as the Gemini API serves it: a provider of the video kind.
///
/// A creation is `POST /models/{model}:predictLongRunning`, which needs a
/// model; its `size` goes as an `aspectRatio` (16:9 or 9:16, the only ones
/// Veo makes) and a `resolution` (`1080p` from 1080 pixels on the short
/// side, else `720p`), its `seconds` as `durationSeconds`, its reference
/// image as the instance's base64 `image`, and its native fields of this
/// format, such as `negativePrompt` or `personGeneration`, as parameters.
///
/// A job is Veo's long-running operation, its id the operation's name
/// (`models/{model}/operations/{op}`) as unpadded base64url after `veo_`
/// ([`GeminiVeo::job_id`]): a path segment, as a client gives an id back.
/// An operation not done is `in_progress`; done with a video, `completed`,
/// its videos' URIs [`Job::videos`]; done with an error, or with no video as
/// when Veo's filters took them all, `failed`. Retrieving is `GET
/// /{operation}`. Content is a `GET` of the URL its job names, needed in
/// the request ([`LocateContent`](respan_gateway_media::video::LocateContent) finds it), at
/// the provider's base: its path after its version segment, so the key
/// never goes to another host. The Gemini API may answer it with a redirect
/// to storage, which a transport follows only if its client does
/// (`HttpTransport::with_client`).
///
/// Veo cannot remix, delete or list videos, nor make a thumbnail or a
/// spritesheet: those are refused, 400, before any call. As no client
/// speaks this format, what Veo answers beyond the kind is dropped.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct GeminiVeo;

impl GeminiVeo {
    /// The format's name, `gemini-veo`, which tags its native fields.
    pub const NAME: &'static str = NAME;

    /// The job id of the operation `name`.
    pub fn job_id(name: &str) -> String {
        format!("{PREFIX}{}", URL_SAFE_NO_PAD.encode(name))
    }

    /// The operation a job `id` names, if it is one of this format's: its
    /// segments of letters, digits, `-`, `_` and `.`, and no `.` or `..`.
    pub fn operation(id: &str) -> Option<String> {
        let name = URL_SAFE_NO_PAD.decode(id.strip_prefix(PREFIX)?).ok()?;
        let name = String::from_utf8(name).ok()?;
        let mut segments = name.split('/');
        segments.all(is_segment).then_some(name)
    }
}

/// Whether `s` is a path segment that every reader reads as it is.
fn is_segment(s: &str) -> bool {
    let plain = |c: char| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.');
    !s.is_empty() && s != "." && s != ".." && s.chars().all(plain)
}

impl Format<ProviderIo, Output, Input> for GeminiVeo {
    fn name(&self) -> &str {
        NAME
    }

    fn bind(&self, io: ProviderIo) -> Result<Connection<Output, Input>, Fault> {
        // What the call was, from the input it was made of.
        let sent = Arc::new(OnceLock::new());
        let seen = Arc::clone(&sent);
        Ok(bind_provider(
            io,
            NAME,
            move |head, input| {
                let (encoder, call) = call(head, input)?;
                _ = seen.set(call);
                Ok(encoder)
            },
            move |response, _| read_response(response, sent.get()),
        ))
    }
}

/// An operation Veo has no form for, refused.
fn unsupported(what: &str) -> Fault {
    BadRequest::fault(format!("Veo cannot {what}")).with(ErrorCode("unsupported".into()))
}

/// The operation a job `id` names, else a 404.
fn operation_of(id: &str) -> Result<String, Fault> {
    GeminiVeo::operation(id).ok_or_else(|| Fault::not_found(format!("no Veo video has id {id}")))
}

/// What a call asked of Veo, which its answer is read by.
#[derive(Debug)]
enum Call {
    /// A creation, made with these `parameters`.
    Create(Value),
    /// An operation's state.
    Retrieve,
    /// A video's bytes.
    Content,
}

/// The call of `input`: its method and path, its body's encoder, and what
/// it asks.
fn call(head: &mut RequestHead, input: &Input) -> Result<(Box<dyn Encoder<Input>>, Call), Fault> {
    let (method, path, body, asked) = match input {
        Input::Create(create) => {
            let model = create.model.as_deref().filter(|m| is_segment(m));
            let model = model.ok_or_else(|| {
                BadRequest::fault("a Veo video needs a model, such as veo-3.0-generate-001")
            })?;
            let path = format!("/models/{model}:predictLongRunning");
            let (body, parameters) = create_body(create)?;
            (Method::POST, path, Some(body), Call::Create(parameters))
        }
        Input::Retrieve { id } => {
            let path = format!("/{}", operation_of(id)?);
            (Method::GET, path, None, Call::Retrieve)
        }
        Input::Content { id, variant, url } => {
            if *variant != Variant::Video {
                return Err(unsupported("make a thumbnail or a spritesheet"));
            }
            operation_of(id)?;
            let url = url.as_deref().ok_or_else(|| {
                Fault::config("Veo's content is at the URL its job names, which the request needs")
            })?;
            (Method::GET, content_path(url)?, None, Call::Content)
        }
        Input::Remix { .. } => return Err(unsupported("remix a video")),
        Input::Delete { .. } => return Err(unsupported("delete a video")),
        Input::List { .. } => return Err(unsupported("list videos")),
        _ => return Err(unsupported("answer this request")),
    };
    head.method = method;
    head.uri = path
        .parse()
        .map_err(|e| Fault::internal("a call's path is no path").with_source(e))?;
    let Some(body) = body else {
        return Ok((sends(Bytes::new()), asked));
    };
    let json = HeaderValue::from_static("application/json");
    head.headers.insert(CONTENT_TYPE, json);
    Ok((sends(body), asked))
}

/// A creation's body: one instance, of its prompt and reference image, and
/// its parameters, which are also given apart.
fn create_body(create: &Create) -> Result<(Bytes, Value), Fault> {
    let mut instance = Map::new();
    instance.insert("prompt".into(), create.prompt.clone().into());
    if let Some(reference) = &create.reference {
        let MediaSource::Bytes(bytes) = &reference.source else {
            return Err(BadRequest::fault(
                "Veo takes a reference image's bytes, not its URL",
            ));
        };
        let image =
            json!({"bytesBase64Encoded": STANDARD.encode(bytes), "mimeType": reference.mime_type});
        instance.insert("image".into(), image);
    }
    let mut parameters = Map::new();
    if let Some(size) = &create.size {
        let (aspect, resolution) = aspect_of(size)?;
        parameters.insert("aspectRatio".into(), aspect.into());
        parameters.insert("resolution".into(), resolution.into());
    }
    if let Some(seconds) = create.seconds {
        parameters.insert("durationSeconds".into(), seconds.into());
    }
    for (key, value) in kept(NAME, &create.native)? {
        parameters.entry(key).or_insert(value);
    }
    let parameters = Value::Object(parameters);
    let mut body = json!({ "instances": [instance] });
    if parameters.as_object().is_some_and(|p| !p.is_empty()) {
        body["parameters"] = parameters.clone();
    }
    let body = serde_json::to_vec(&body)
        .map_err(|e| Fault::internal("a body could not be written").with_source(e))?;
    Ok((Bytes::from(body), parameters))
}

/// Veo's aspect ratio and resolution of a size `WxH`.
fn aspect_of(size: &str) -> Result<(&'static str, &'static str), Fault> {
    let parsed = size.split_once(['x', 'X']).and_then(|(w, h)| {
        let (w, h) = (w.trim().parse::<u32>().ok()?, h.trim().parse::<u32>().ok()?);
        Some((w, h)).filter(|_| w > 0 && h > 0)
    });
    let refused = || {
        BadRequest::fault(format!(
            "'size' {size} is not one Veo makes: it makes 16:9 and 9:16 videos"
        ))
    };
    let (w, h) = parsed.ok_or_else(refused)?;
    let (long, short) = (w.max(h), w.min(h));
    if w == h || (f64::from(long) / f64::from(short) - 16.0 / 9.0).abs() > 0.05 {
        return Err(refused());
    }
    let aspect = if w > h { "16:9" } else { "9:16" };
    let resolution = if short >= 1080 { "1080p" } else { "720p" };
    Ok((aspect, resolution))
}

/// The size, `WxH`, of a video of `aspect` and `resolution`, Veo's defaults
/// where they say none.
fn size_of(aspect: Option<&str>, resolution: Option<&str>) -> Option<String> {
    let short = match resolution.unwrap_or("720p") {
        "720p" => 720,
        "1080p" => 1080,
        _ => return None,
    };
    let long = short * 16 / 9;
    match aspect.unwrap_or("16:9") {
        "16:9" => Some(format!("{long}x{short}")),
        "9:16" => Some(format!("{short}x{long}")),
        _ => None,
    }
}

/// The path, relative to the provider's base, of content at `url`: its
/// path after its version segment, such as `v1beta`, and its query.
fn content_path(url: &str) -> Result<String, Fault> {
    let unusable = || Upstream::fault("the URL of Veo's video is not one it can be fetched at");
    let uri = url.parse::<Uri>().map_err(|_| unusable())?;
    let path = uri.path().trim_start_matches('/');
    let path = match path.split_once('/') {
        Some((version, rest)) if is_version(version) => rest,
        None if is_version(path) => "",
        _ => path,
    };
    if path.is_empty() {
        return Err(unusable());
    }
    Ok(match uri.query() {
        Some(query) => format!("/{path}?{query}"),
        None => format!("/{path}"),
    })
}

/// Whether a path segment names an API version, such as `v1` or `v1beta`.
fn is_version(segment: &str) -> bool {
    let mut chars = segment.chars();
    chars.next() == Some('v') && chars.next().is_some_and(|c| c.is_ascii_digit())
}

/// A provider's answer to the call `sent`: an operation, read whole, or
/// content, chunk by chunk as it comes.
fn read_response(response: http::Response<Body>, sent: Option<&Call>) -> EventStream<Output> {
    let body = response.into_body();
    match sent {
        Some(Call::Content) => content(body),
        Some(Call::Create(parameters)) => {
            let parameters = parameters.clone();
            whole::read(body, move |raw, out| {
                out.push(Output::Job(job_of(&raw, &parameters)?));
                Ok(())
            })
        }
        _ => whole::read(body, |raw, out| {
            out.push(Output::Job(job_of(&raw, &Value::Null)?));
            Ok(())
        }),
    }
}

/// The job of an operation `raw`, and of the `parameters` it was made with
/// if this answers its creation.
fn job_of(raw: &[u8], parameters: &Value) -> Result<Job, Fault> {
    let unreadable =
        |what: &str| Upstream::fault(format!("the provider's {NAME} operation {what}"));
    let v = serde_json::from_slice::<Value>(raw).map_err(|_| unreadable("is not JSON"))?;
    let name = v["name"]
        .as_str()
        .ok_or_else(|| unreadable("has no name"))?;
    let mut job = Job::new(GeminiVeo::job_id(name), Status::InProgress);
    let mut segments = name.split('/');
    if segments.next() == Some("models") {
        job.model = segments.next().map(str::to_owned);
    }
    job.seconds = parameters["durationSeconds"]
        .as_u64()
        .and_then(|s| u32::try_from(s).ok());
    if parameters["aspectRatio"].is_string() || parameters["resolution"].is_string() {
        let (aspect, resolution) = (&parameters["aspectRatio"], &parameters["resolution"]);
        job.size = size_of(aspect.as_str(), resolution.as_str());
    }
    if v["done"] != true {
        return Ok(job);
    }
    if let Some(error) = v["error"].as_object() {
        job.status = Status::Failed;
        let code = match (&error.get("status"), &error.get("code")) {
            (Some(Value::String(status)), _) => Some(status.clone()),
            (_, Some(code)) => Some(code.to_string()),
            _ => None,
        };
        let message = error.get("message").and_then(Value::as_str);
        job.error = Some(JobError::new(code, message.unwrap_or("the video failed")));
        return Ok(job);
    }
    let response = &v["response"]["generateVideoResponse"];
    let samples = response["generatedSamples"]
        .as_array()
        .into_iter()
        .flatten();
    for sample in samples {
        let Some(uri) = sample["video"]["uri"].as_str() else {
            continue;
        };
        let mime = sample["video"]["mimeType"].as_str().unwrap_or("video/mp4");
        job.videos
            .push(Media::new(mime, MediaSource::Url(uri.to_owned())));
    }
    if job.videos.is_empty() {
        // Veo's filters took them all, and say why.
        job.status = Status::Failed;
        let reason = response["raiMediaFilteredReasons"][0].as_str();
        let message = reason.unwrap_or("the provider made no video");
        job.error = Some(JobError::new(Some("content_filtered".into()), message));
        return Ok(job);
    }
    job.status = Status::Completed;
    Ok(job)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_job_id_is_a_path_segment_that_names_its_operation() {
        let name = "models/veo-3.0-generate-001/operations/a1b2-c3_d4";
        let id = GeminiVeo::job_id(name);
        assert!(id.starts_with("veo_"));
        assert!(
            id.chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
        );
        assert_eq!(GeminiVeo::operation(&id).as_deref(), Some(name));
        for refused in [
            "video_123".to_owned(),
            "veo_!!".to_owned(),
            GeminiVeo::job_id("models/../operations/x"),
            GeminiVeo::job_id("models//x"),
            GeminiVeo::job_id("models/x?y"),
        ] {
            assert_eq!(GeminiVeo::operation(&refused), None, "{refused}");
        }
    }

    #[test]
    fn a_size_is_an_aspect_ratio_and_a_resolution_and_back() {
        for (size, aspect, resolution) in [
            ("1280x720", "16:9", "720p"),
            ("720x1280", "9:16", "720p"),
            ("1792x1024", "16:9", "720p"),
            ("1920x1080", "16:9", "1080p"),
            ("1080x1920", "9:16", "1080p"),
        ] {
            let read = aspect_of(size).ok();
            assert_eq!(read, Some((aspect, resolution)), "{size}");
        }
        assert_eq!(
            size_of(Some("9:16"), Some("1080p")).as_deref(),
            Some("1080x1920")
        );
        assert_eq!(size_of(None, None).as_deref(), Some("1280x720"));
        for size in ["1024x1024", "1024x768", "big", "0x0"] {
            assert!(aspect_of(size).is_err(), "{size}");
        }
    }

    #[test]
    fn content_is_fetched_at_the_providers_base() {
        let at = |url: &str| content_path(url).ok();
        assert_eq!(
            at("https://generativelanguage.googleapis.com/v1beta/files/abc:download?alt=media")
                .as_deref(),
            Some("/files/abc:download?alt=media")
        );
        assert_eq!(
            at("https://elsewhere.test/files/x").as_deref(),
            Some("/files/x")
        );
        assert_eq!(at("https://h.test/v1beta"), None);
    }
}
