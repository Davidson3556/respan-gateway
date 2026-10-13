//! OpenAI Videos clients served by Veo.

use respan_gateway_core::{Extensions, Gateway, Provider};
use respan_gateway_http::http::{Method, StatusCode};
use respan_gateway_http::{ClientIo, ProviderIo};
use respan_gateway_media::video::{self, LocateContent};
use respan_gateway_media_http::gemini::GeminiVeo;
use respan_gateway_media_http::openai::OpenAiVideos;
use serde_json::{Value, json};

use crate::support::veo::Veo;
use crate::support::videos::video_chunks;
use crate::support::{
    Form, bodiless, content_type, json_of, json_request, send, send_frames, unb64,
};

fn gateway<T>(transport: T) -> Gateway
where
    T: Fn(Extensions) -> ProviderIo + Clone + Send + Sync + 'static,
{
    Gateway::new()
        .provider(Provider::new("veo", GeminiVeo, transport))
        .and_then(|g| g.client::<video::Input, video::Output, ClientIo>(OpenAiVideos))
        .and_then(|g| g.target("veo"))
        .expect("a gateway")
        .plugin(LocateContent::new().provider("veo"))
}

/// A job made of `asked` through `gateway`.
async fn create(gateway: &Gateway, asked: Value) -> Value {
    let (status, _, body) = send(gateway, json_request("/v1/videos", asked)).await;
    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
    json_of(&body)
}

/// The job `id`, polled through `gateway`.
async fn poll(gateway: &Gateway, id: &str) -> Value {
    let (status, _, body) = send(gateway, bodiless(Method::GET, &format!("/v1/videos/{id}"))).await;
    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
    json_of(&body)
}

#[tokio::test]
async fn an_openai_client_makes_polls_and_downloads_a_veo_video() {
    let veo = Veo::start().await;
    let gateway = gateway(veo.transport());
    let asked = json!({
        "model": "veo-3.0-generate-001",
        "prompt": "a fox in the snow",
        "seconds": "8",
        "size": "1280x720",
    });
    let job = create(&gateway, asked).await;
    let name = "models/veo-3.0-generate-001/operations/op1";
    let id = GeminiVeo::job_id(name);
    assert_eq!(job["id"], id.as_str());
    assert_eq!(job["object"], "video");
    assert_eq!(job["status"], "in_progress");
    assert_eq!(job["model"], "veo-3.0-generate-001");
    assert_eq!(job["seconds"], "8");
    assert_eq!(job["size"], "1280x720");
    let seen = veo.seen();
    assert_eq!(
        (seen[0].method.as_str(), seen[0].path.as_str()),
        (
            "POST",
            "/v1beta/models/veo-3.0-generate-001:predictLongRunning"
        )
    );
    assert_eq!(
        seen[0].body,
        json!({
            "instances": [{"prompt": "a fox in the snow"}],
            "parameters": {"aspectRatio": "16:9", "resolution": "720p", "durationSeconds": 8},
        })
    );

    assert_eq!(poll(&gateway, &id).await["status"], "in_progress");
    assert_eq!(veo.seen()[1].path, format!("/v1beta/{name}"));
    let done = poll(&gateway, &id).await;
    assert_eq!(done["status"], "completed");
    assert_eq!(done["id"], id.as_str());

    let content = format!("/v1/videos/{id}/content");
    let (status, headers, frames) = send_frames(&gateway, bodiless(Method::GET, &content)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(content_type(&headers), "video/mp4");
    assert!(frames.len() > 1, "the video came in {} frame", frames.len());
    assert_eq!(frames.concat(), video_chunks().concat());
    let seen = veo.seen();
    let download = seen.last().expect("a download");
    assert_eq!(download.path, "/v1beta/files/op1:download");
    assert_eq!(download.query.as_deref(), Some("alt=media"));
    assert_eq!(seen[seen.len() - 2].path, format!("/v1beta/{name}"));
}

#[tokio::test]
async fn a_reference_image_goes_to_veo_in_base64() {
    let veo = Veo::start().await;
    let gateway = gateway(veo.transport());
    let image = b"\x89PNG\r\n\x1a\n first frame".to_vec();
    let form = Form::default()
        .text("model", "veo-3.0-fast-generate-001")
        .text("prompt", "she turns around")
        .text("size", "1080x1920")
        .file("input_reference", "first.png", "image/png", &image);
    let (status, _, body) = send(&gateway, form.to("/v1/videos")).await;
    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
    assert_eq!(json_of(&body)["size"], "1080x1920");
    let sent = &veo.seen()[0].body;
    assert_eq!(sent["instances"][0]["image"]["mimeType"], "image/png");
    assert_eq!(
        unb64(&sent["instances"][0]["image"]["bytesBase64Encoded"]),
        image
    );
    assert_eq!(
        sent["parameters"],
        json!({"aspectRatio": "9:16", "resolution": "1080p"})
    );
}

#[tokio::test]
async fn what_veo_cannot_do_is_refused_in_the_clients_schema_before_any_call() {
    let veo = Veo::start().await;
    let gateway = gateway(veo.transport());
    let id = GeminiVeo::job_id("models/veo-3.0-generate-001/operations/op1");
    for request in [
        json_request(
            &format!("/v1/videos/{id}/remix"),
            json!({"prompt": "again"}),
        ),
        bodiless(Method::DELETE, &format!("/v1/videos/{id}")),
        bodiless(Method::GET, "/v1/videos"),
        json_request(
            "/v1/videos",
            json!({"model": "veo-3.0-generate-001", "prompt": "x", "size": "1024x1024"}),
        ),
        json_request("/v1/videos", json!({"prompt": "no model"})),
    ] {
        let what = format!("{} {}", request.method(), request.uri());
        let (status, _, body) = send(&gateway, request).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{what}");
        let error = &json_of(&body)["error"];
        assert!(error["message"].is_string(), "{what}");
    }
    let (status, _, body) = send(&gateway, bodiless(Method::GET, "/v1/videos/video_123")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(json_of(&body)["error"]["code"], "not_found");
    assert!(veo.seen().is_empty());
}

#[tokio::test]
async fn a_thumbnail_of_a_veo_video_is_refused() {
    let veo = Veo::start().await;
    let gateway = gateway(veo.transport());
    let id = create(
        &gateway,
        json!({"model": "veo-3.0-generate-001", "prompt": "a"}),
    )
    .await;
    let id = id["id"].as_str().expect("an id");
    poll(&gateway, id).await;
    poll(&gateway, id).await;
    let called = veo.seen().len();
    let thumbnail = format!("/v1/videos/{id}/content?variant=thumbnail");
    let (status, _, body) = send(&gateway, bodiless(Method::GET, &thumbnail)).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(json_of(&body)["error"]["code"], "unsupported");
    // Refused before any call, the job not even retrieved to locate it.
    assert_eq!(veo.seen().len(), called);
}

#[tokio::test]
async fn a_veo_error_reaches_the_client_in_openais_schema() {
    let veo = Veo::start().await;
    let gateway = gateway(veo.transport());
    let asked = json!({"model": "veo-3.0-generate-001", "prompt": "long", "seconds": "12"});
    let (status, _, body) = send(&gateway, json_request("/v1/videos", asked)).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let error = &json_of(&body)["error"];
    assert_eq!(error["type"], "INVALID_ARGUMENT");
    assert!(
        error["message"]
            .as_str()
            .is_some_and(|m| m.contains("durationSeconds"))
    );
    assert_eq!(veo.seen()[0].body["parameters"]["durationSeconds"], 12);
}

#[tokio::test]
async fn a_failed_or_filtered_veo_video_is_a_failed_job_with_no_content() {
    let veo = Veo::start().await;
    let gateway = gateway(veo.transport());
    for (prompt, code) in [("fail", "INTERNAL"), ("filtered", "content_filtered")] {
        let job = create(
            &gateway,
            json!({"model": "veo-3.0-generate-001", "prompt": prompt}),
        )
        .await;
        let id = job["id"].as_str().expect("an id");
        poll(&gateway, id).await;
        let failed = poll(&gateway, id).await;
        assert_eq!(failed["status"], "failed", "{prompt}");
        assert_eq!(failed["error"]["code"], code, "{prompt}");
        assert!(
            failed["error"]["message"]
                .as_str()
                .is_some_and(|m| !m.is_empty())
        );

        let content = format!("/v1/videos/{id}/content");
        let (status, _, body) = send(&gateway, bodiless(Method::GET, &content)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{prompt}");
        assert_eq!(json_of(&body)["error"]["code"], "video_not_ready");
    }
}

#[tokio::test]
async fn a_download_redirected_to_storage_needs_a_client_that_follows_it() {
    let veo = Veo::redirecting().await;
    for (gateway, follows) in [
        (gateway(veo.following()), true),
        (gateway(veo.transport()), false),
    ] {
        let job = create(
            &gateway,
            json!({"model": "veo-3.0-generate-001", "prompt": "a"}),
        )
        .await;
        let id = job["id"].as_str().expect("an id");
        poll(&gateway, id).await;
        poll(&gateway, id).await;
        let content = format!("/v1/videos/{id}/content");
        let (status, _, body) = send(&gateway, bodiless(Method::GET, &content)).await;
        if follows {
            assert_eq!(status, StatusCode::OK);
            assert_eq!(body.as_ref(), video_chunks().concat());
        } else {
            assert_eq!(status, StatusCode::BAD_GATEWAY);
            assert!(json_of(&body)["error"]["message"].is_string());
        }
    }
}
