//! Veo's clients: the Gemini API's `predictLongRunning`, its operations
//! and its file downloads, served by Veo itself and by OpenAI Videos.

use respan_gateway_core::{Extensions, Gateway, Provider};
use respan_gateway_http::http::{Method, StatusCode};
use respan_gateway_http::{ClientIo, ProviderIo};
use respan_gateway_media::video::{self, LocateContent, VideoJobs};
use respan_gateway_media_http::gemini::GeminiVeo;
use respan_gateway_media_http::openai::OpenAiVideos;
use serde_json::{Value, json};

use crate::support::veo::Veo;
use crate::support::videos::{Videos, video_chunks};
use crate::support::{bodiless, content_type, json_of, json_request, send, send_frames};

/// A gateway serving Veo's clients from the provider `name` of `format`.
fn gateway<F, T>(name: &str, format: F, transport: T) -> Gateway
where
    F: respan_gateway_core::Format<ProviderIo, video::Output, video::Input>
        + Clone
        + Send
        + Sync
        + 'static,
    T: Fn(Extensions) -> ProviderIo + Clone + Send + Sync + 'static,
{
    Gateway::new()
        .provider(Provider::new(name, format, transport))
        .and_then(|g| g.client::<video::Input, video::Output, ClientIo>(GeminiVeo))
        .and_then(|g| g.target(name))
        .expect("a gateway")
        .plugin(VideoJobs::new().provider(
            name,
            name,
            if name == "veo" { "veo_" } else { "video_" },
        ))
        .plugin(LocateContent::new().provider("veo"))
}

/// What `gateway` answers `request`, a success.
async fn ok(
    gateway: &Gateway,
    request: respan_gateway_http::http::Request<respan_gateway_http::Body>,
) -> Value {
    let (status, _, body) = send(gateway, request).await;
    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
    json_of(&body)
}

const CREATE: &str = "/v1beta/models/veo-3.0-generate-001:predictLongRunning";

/// The operation polled until done, at the name it gives.
async fn done(gateway: &Gateway, operation: &Value) -> Value {
    let name = operation["name"].as_str().expect("a name");
    for _ in 0..3 {
        let polled = ok(gateway, bodiless(Method::GET, &format!("/v1beta/{name}"))).await;
        assert_eq!(polled["name"], name);
        if polled["done"] == true {
            return polled;
        }
    }
    panic!("the operation never finished");
}

/// The gateway's path of a finished operation's video.
fn download(done: &Value) -> String {
    let uri = &done["response"]["generateVideoResponse"]["generatedSamples"][0]["video"]["uri"];
    let uri = uri.as_str().expect("a video's URI");
    uri.strip_prefix("http://localhost")
        .expect("at the gateway")
        .to_owned()
}

#[tokio::test]
async fn a_veo_client_makes_polls_and_downloads_a_veo_video() {
    let veo = Veo::start().await;
    let gateway = gateway("veo", GeminiVeo, veo.transport());
    let asked = json!({
        "instances": [{"prompt": "a fox in the snow"}],
        "parameters": {"aspectRatio": "16:9", "durationSeconds": 8, "negativePrompt": "rain"},
    });
    let operation = ok(&gateway, json_request(CREATE, asked)).await;
    let id = GeminiVeo::job_id("models/veo-3.0-generate-001/operations/op1");
    assert_eq!(
        operation,
        json!({"name": format!("models/veo-3.0-generate-001/operations/{id}")})
    );
    // What the kind has no place for reaches Veo as it was asked.
    assert_eq!(
        veo.seen()[0].body,
        json!({
            "instances": [{"prompt": "a fox in the snow"}],
            "parameters": {"aspectRatio": "16:9", "resolution": "720p", "durationSeconds": 8,
                "negativePrompt": "rain"},
        })
    );
    let done = done(&gateway, &operation).await;
    let path = download(&done);
    assert_eq!(path, format!("/v1beta/files/{id}:download?alt=media"));
    let (status, headers, frames) = send_frames(&gateway, bodiless(Method::GET, &path)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(content_type(&headers), "video/mp4");
    assert_eq!(frames.concat(), video_chunks().concat());
    // Veo's own URL, which needs its key, never reached the client.
    assert!(!done.to_string().contains("op1:download"), "{done}");
}

#[tokio::test]
async fn a_veo_client_is_served_by_openai_videos() {
    let openai = Videos::openai().await;
    let gateway = gateway("openai", OpenAiVideos, openai.transport());
    let asked = json!({
        "instances": [{"prompt": "a lighthouse"}],
        "parameters": {"aspectRatio": "9:16", "durationSeconds": 8},
    });
    let create = "/v1beta/models/sora-2:predictLongRunning";
    let operation = ok(&gateway, json_request(create, asked)).await;
    let sent = &openai.seen()[0].fields;
    assert_eq!(sent["model"], "sora-2");
    assert_eq!(sent["size"], "720x1280");
    assert_eq!(sent["seconds"], "8");
    assert_eq!(sent["prompt"], "a lighthouse");
    let name = operation["name"].as_str().expect("a name");
    assert!(name.contains("/operations/video_"), "{name}");
    let done = done(&gateway, &operation).await;
    let (status, _, frames) = send_frames(&gateway, bodiless(Method::GET, &download(&done))).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(frames.concat(), video_chunks().concat());
}

#[tokio::test]
async fn a_veo_clients_mistake_is_refused_in_googles_shape() {
    let veo = Veo::start().await;
    let gateway = gateway("veo", GeminiVeo, veo.transport());
    for asked in [
        json!({"instances": []}),
        json!({"instances": [{"prompt": "a", "video": {}}]}),
        json!({"instances": [{"prompt": "a"}], "parameters": {"durationSeconds": "long"}}),
    ] {
        let (status, _, body) = send(&gateway, json_request(CREATE, asked.clone())).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{asked}");
        let error = &json_of(&body)["error"];
        assert_eq!(error["code"], 400, "{asked}");
        assert_eq!(error["status"], "INVALID_ARGUMENT", "{asked}");
    }
    assert!(veo.seen().is_empty());
}
