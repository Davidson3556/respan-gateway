//! Video jobs relayed through OpenAI Videos.

use respan_gateway_core::Gateway;
use respan_gateway_http::ClientIo;
use respan_gateway_http::http::{Method, StatusCode};
use respan_gateway_media::openai::OpenAiVideos;
use respan_gateway_media::video;
use serde_json::{Value, json};

use crate::support::videos::{THUMBNAIL, Videos, video_chunks};
use crate::support::{
    Form, Rules, bodiless, content_type, json_of, json_request, send, send_frames,
};

fn gateway(fake: &Videos) -> Gateway {
    Gateway::new()
        .provider("openai", OpenAiVideos, fake.transport())
        .and_then(|g| g.client::<video::Input, video::Output, ClientIo>(OpenAiVideos))
        .and_then(|g| g.target("openai"))
        .expect("a gateway")
}

/// A job made of `asked` through `gateway`.
async fn create(gateway: &Gateway, asked: Value) -> Value {
    let (status, _, body) = send(gateway, json_request("/v1/videos", asked)).await;
    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
    json_of(&body)
}

#[tokio::test]
async fn a_creation_is_relayed_with_what_the_kind_has_no_form_for() {
    let mut extra_answer = serde_json::Map::new();
    extra_answer.insert("x_trace".into(), json!({"id": "t-1"}));
    let fake = Videos::start(Rules {
        extra_fields: vec!["x_seed"],
        extra_answer,
        ..Rules::default()
    })
    .await;
    let gateway = gateway(&fake);
    let asked = json!({
        "model": "sora-2",
        "prompt": "a fox in the snow",
        "seconds": "8",
        "size": "1280x720",
        "x_seed": 7,
    });
    let (status, headers, body) =
        send(&gateway, json_request("/openai/v1/videos", asked.clone())).await;
    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
    assert_eq!(content_type(&headers), "application/json");

    let seen = fake.seen();
    assert_eq!(
        (seen[0].method.as_str(), seen[0].path.as_str()),
        ("POST", "/v1/videos")
    );
    assert_eq!(Value::Object(seen[0].fields.clone()), asked);

    let job = json_of(&body);
    assert_eq!(
        job,
        json!({
            "id": "video_00000000000000000000000000000001",
            "object": "video",
            "created_at": 1_700_000_000,
            "status": "queued",
            "model": "sora-2",
            "progress": 0,
            "seconds": "8",
            "size": "1280x720",
            "completed_at": null,
            "expires_at": null,
            "error": null,
            "remixed_from_video_id": null,
            "x_trace": {"id": "t-1"},
        })
    );
}

#[tokio::test]
async fn a_creation_with_a_reference_image_is_relayed_as_a_form() {
    let fake = Videos::openai().await;
    let gateway = gateway(&fake);
    let image = b"\x89PNG\r\n\x1a\n first frame".to_vec();
    let form = Form::default()
        .text("model", "sora-2-pro")
        .text("prompt", "she turns around")
        .text("seconds", "4")
        .text("size", "720x1280")
        .file("input_reference", "first.png", "image/png", &image);
    let (status, _, body) = send(&gateway, form.to("/v1/videos")).await;
    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
    assert_eq!(json_of(&body)["model"], "sora-2-pro");

    let seen = &fake.seen()[0];
    assert!(seen.content_type.starts_with("multipart/form-data"));
    assert_eq!(
        Value::Object(seen.fields.clone()),
        json!({"model": "sora-2-pro", "prompt": "she turns around", "seconds": "4", "size": "720x1280"})
    );
    let file = &seen.files[0];
    assert_eq!(file.name, "input_reference");
    assert_eq!(file.filename.as_deref(), Some("first.png"));
    assert_eq!(file.content_type.as_deref(), Some("image/png"));
    assert_eq!(file.bytes.as_ref(), image);
}

#[tokio::test]
async fn a_number_of_seconds_goes_as_the_string_openai_takes() {
    let fake = Videos::openai().await;
    let gateway = gateway(&fake);
    let job = create(&gateway, json!({"prompt": "waves", "seconds": 12})).await;
    assert_eq!(job["seconds"], "12");
    assert_eq!(fake.seen()[0].fields["seconds"], "12");
}

#[tokio::test]
async fn a_job_is_polled_remixed_listed_and_deleted() {
    let fake = Videos::openai().await;
    let gateway = gateway(&fake);
    let first = create(&gateway, json!({"prompt": "a lighthouse"})).await;
    let id = first["id"].as_str().expect("an id").to_owned();

    let (status, _, body) =
        send(&gateway, bodiless(Method::GET, &format!("/v1/videos/{id}"))).await;
    assert_eq!(status, StatusCode::OK);
    let polled = json_of(&body);
    assert_eq!(polled["status"], "completed");
    assert_eq!(polled["progress"], 100);
    assert_eq!(polled["completed_at"], 1_700_000_100);
    assert_eq!(polled["expires_at"], 1_700_086_500);

    let remix = json_request(
        &format!("/v1/videos/{id}/remix"),
        json!({"prompt": "at night"}),
    );
    let (status, _, body) = send(&gateway, remix).await;
    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
    let remixed = json_of(&body);
    assert_eq!(remixed["remixed_from_video_id"], id.as_str());
    assert_eq!(remixed["status"], "queued");

    let list = bodiless(Method::GET, "/v1/videos?limit=1&order=asc");
    let (status, _, body) = send(&gateway, list).await;
    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
    let page = json_of(&body);
    assert_eq!(page["object"], "list");
    assert_eq!(page["data"].as_array().map(Vec::len), Some(1));
    assert_eq!(page["data"][0]["id"], id.as_str());
    assert_eq!(page["first_id"], id.as_str());
    assert_eq!(page["last_id"], id.as_str());
    assert_eq!(page["has_more"], true);
    let listed = fake
        .seen()
        .into_iter()
        .find(|s| s.method == "GET" && s.path == "/v1/videos");
    assert_eq!(
        listed.and_then(|s| s.query).as_deref(),
        Some("limit=1&order=asc")
    );

    let path = format!("/v1/videos/{id}");
    let (status, _, body) = send(&gateway, bodiless(Method::DELETE, &path)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        json_of(&body),
        json!({"id": id, "object": "video.deleted", "deleted": true})
    );
    let (status, _, body) = send(&gateway, bodiless(Method::GET, &path)).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(json_of(&body)["error"]["code"], "not_found");
}

#[tokio::test]
async fn content_streams_through_intact_chunk_by_chunk() {
    let fake = Videos::openai().await;
    let gateway = gateway(&fake);
    let id = create(&gateway, json!({"prompt": "rain"})).await["id"].clone();
    let id = id.as_str().expect("an id");

    // Content is not ready before the job is done.
    let content = format!("/v1/videos/{id}/content");
    let (status, _, _) = send(&gateway, bodiless(Method::GET, &content)).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    send(&gateway, bodiless(Method::GET, &format!("/v1/videos/{id}"))).await;
    let (status, headers, frames) = send_frames(&gateway, bodiless(Method::GET, &content)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(content_type(&headers), "video/mp4");
    assert!(frames.len() > 1, "the video came in {} frame", frames.len());
    assert_eq!(frames.concat(), video_chunks().concat());

    let thumbnail = format!("{content}?variant=thumbnail");
    let (status, headers, body) = send(&gateway, bodiless(Method::GET, &thumbnail)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(content_type(&headers), "image/webp");
    assert_eq!(body.as_ref(), THUMBNAIL);
    let asked = fake.seen().last().and_then(|s| s.query.clone());
    assert_eq!(asked.as_deref(), Some("variant=thumbnail"));
}

#[tokio::test]
async fn a_bad_request_is_refused_before_any_call() {
    let fake = Videos::openai().await;
    let gateway = gateway(&fake);
    for (request, param) in [
        (
            json_request("/v1/videos", json!({"model": "sora-2"})),
            "prompt",
        ),
        (
            bodiless(Method::GET, "/v1/videos/v1/content?variant=poster"),
            "variant",
        ),
        (bodiless(Method::GET, "/v1/videos?limit=many"), "limit"),
    ] {
        let (status, _, body) = send(&gateway, request).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(json_of(&body)["error"]["param"], param);
    }
    assert!(fake.seen().is_empty());
}

#[tokio::test]
async fn a_providers_error_reaches_the_client_in_openais_schema() {
    let fake = Videos::openai().await;
    let gateway = gateway(&fake);
    let asked = json!({"model": "sora-9", "prompt": "a cat"});
    let (status, _, body) = send(&gateway, json_request("/v1/videos", asked)).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let error = &json_of(&body)["error"];
    assert_eq!(error["code"], "model_not_found");
    assert!(
        error["message"]
            .as_str()
            .is_some_and(|m| m.contains("sora-9"))
    );
}
