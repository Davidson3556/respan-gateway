//! Image generations and edits.

use respan_gateway_http::http::StatusCode;
use respan_gateway_media::openai::OpenAiImages;
use serde_json::{Value, json};

use crate::support::{
    Fake, Form, Rules, content_type, events_of, gateway, image_bytes, json_of, json_request, send,
    unb64,
};

#[tokio::test]
async fn a_generation_is_relayed_with_what_the_kind_has_no_form_for() {
    let mut extra_answer = serde_json::Map::new();
    extra_answer.insert("x_trace".into(), json!({"id": "t-1"}));
    let fake = Fake::start(Rules {
        extra_fields: vec!["seed"],
        extra_answer,
        ..Rules::default()
    })
    .await;
    let gateway = gateway(OpenAiImages, &fake);
    let asked = json!({
        "model": "gpt-image-1",
        "prompt": "a cat in a hat",
        "n": 2,
        "size": "1024x1536",
        "quality": "high",
        "background": "transparent",
        "moderation": "low",
        "seed": 7,
    });
    let (status, headers, body) = send(
        &gateway,
        json_request("/openai/v1/images/generations", asked.clone()),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
    assert_eq!(content_type(&headers), "application/json");

    let seen = fake.seen();
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0].path, "/v1/images/generations");
    assert_eq!(Value::Object(seen[0].fields.clone()), asked);

    let v = json_of(&body);
    assert_eq!(v["created"], 1_700_000_000);
    let data = v["data"].as_array().expect("images");
    assert_eq!(data.len(), 2);
    for (i, item) in data.iter().enumerate() {
        assert_eq!(unb64(&item["b64_json"]), image_bytes(i));
    }
    assert_eq!(
        v["usage"],
        json!({
            "input_tokens": 50, "output_tokens": 4160, "total_tokens": 4210,
            "input_tokens_details": {"text_tokens": 40, "image_tokens": 10},
        })
    );
    assert_eq!(v["background"], "transparent");
    assert_eq!(v["size"], "1024x1536");
    assert_eq!(v["x_trace"], json!({"id": "t-1"}));
}

#[tokio::test]
async fn an_edit_of_two_images_and_a_mask_is_relayed_as_a_form() {
    let fake = Fake::start(Rules {
        extra_fields: vec!["seed"],
        ..Rules::default()
    })
    .await;
    let gateway = gateway(OpenAiImages, &fake);
    let (first, second, mask) = (
        b"\x89PNG first".to_vec(),
        b"\xFF\xD8\xFF second".to_vec(),
        b"\x89PNG mask".to_vec(),
    );
    let form = Form::default()
        .text("model", "gpt-image-1")
        .text("prompt", "put them together")
        // A file an SDK could not type is typed by its name.
        .file("image[]", "first.png", "application/octet-stream", &first)
        .file("image[]", "second.jpg", "image/jpeg", &second)
        .file("mask", "mask.png", "image/png", &mask)
        .text("input_fidelity", "high")
        .text("seed", "7");
    let (status, _, body) = send(&gateway, form.to("/v1/images/edits")).await;
    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));

    let seen = fake.seen();
    assert_eq!(seen.len(), 1);
    let call = &seen[0];
    assert_eq!(call.path, "/v1/images/edits");
    assert!(
        call.content_type
            .starts_with("multipart/form-data; boundary=")
    );
    let files: Vec<_> = call
        .files
        .iter()
        .map(|f| {
            (
                f.name.as_str(),
                f.filename.as_deref(),
                f.content_type.as_deref(),
                f.bytes.to_vec(),
            )
        })
        .collect();
    assert_eq!(
        files,
        [
            ("image[]", Some("first.png"), Some("image/png"), first),
            ("image[]", Some("second.jpg"), Some("image/jpeg"), second),
            ("mask", Some("mask.png"), Some("image/png"), mask),
        ]
    );
    assert_eq!(
        Value::Object(call.fields.clone()),
        json!({
            "model": "gpt-image-1",
            "prompt": "put them together",
            "input_fidelity": "high",
            "seed": "7",
        })
    );
    assert_eq!(
        unb64(&json_of(&body)["data"][0]["b64_json"]),
        image_bytes(0)
    );
}

#[tokio::test]
async fn partial_images_stream_through_in_the_endpoints_events() {
    let fake = Fake::openai().await;
    let gateway = gateway(OpenAiImages, &fake);
    for (path, prefix) in [
        ("/v1/images/generations", "image_generation"),
        ("/v1/images/edits", "image_edit"),
    ] {
        let request = match prefix {
            "image_edit" => Form::default()
                .text("model", "gpt-image-1")
                .text("prompt", "a cat")
                .file("image", "cat.png", "image/png", b"\x89PNG cat")
                .text("stream", "true")
                .text("partial_images", "2")
                .to(path),
            _ => json_request(
                path,
                json!({"model": "gpt-image-1", "prompt": "a cat", "stream": true, "partial_images": 2}),
            ),
        };
        let (status, headers, body) = send(&gateway, request).await;
        assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
        assert_eq!(content_type(&headers), "text/event-stream");
        let events = events_of(&body);
        let types: Vec<_> = events
            .iter()
            .map(|e| e["type"].as_str().unwrap_or_default())
            .collect();
        let partial = format!("{prefix}.partial_image");
        let completed = format!("{prefix}.completed");
        assert_eq!(types, [partial.as_str(), &partial, &completed], "{path}");
        assert_eq!(events[1]["partial_image_index"], 1);
        assert_eq!(unb64(&events[1]["b64_json"]), b"partial 1");
        assert_eq!(events[2]["created_at"], 1_700_000_000);
        assert_eq!(events[2]["output_format"], "png");
        assert_eq!(unb64(&events[2]["b64_json"]), image_bytes(0));
        assert_eq!(
            events[2]["usage"]["input_tokens_details"]["image_tokens"],
            10
        );
        assert_eq!(
            fake.seen().last().map(|c| c.path.clone()).as_deref(),
            Some(path)
        );
    }
}

#[tokio::test]
async fn a_request_without_what_it_needs_is_refused_before_any_call() {
    let fake = Fake::openai().await;
    let gateway = gateway(OpenAiImages, &fake);
    let no_image = Form::default()
        .text("prompt", "edit what?")
        .to("/v1/images/edits");
    let no_prompt = json_request("/v1/images/generations", json!({"model": "dall-e-3"}));
    for (request, param) in [(no_prompt, "prompt"), (no_image, "image")] {
        let (status, _, body) = send(&gateway, request).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        let error = &json_of(&body)["error"];
        assert_eq!(error["param"], param);
        assert_eq!(error["type"], "invalid_request_error");
    }
    assert!(fake.seen().is_empty());
}

#[tokio::test]
async fn a_providers_error_reaches_the_client_in_openais_schema() {
    let fake = Fake::openai().await;
    let gateway = gateway(OpenAiImages, &fake);
    let asked = json!({"model": "dall-e-9", "prompt": "a cat"});
    let (status, headers, body) =
        send(&gateway, json_request("/v1/images/generations", asked)).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(content_type(&headers), "application/json");
    let error = &json_of(&body)["error"];
    assert_eq!(error["code"], "model_not_found");
    assert_eq!(error["type"], "invalid_request_error");
    assert_eq!(error["param"], Value::Null);
    assert!(
        error["message"]
            .as_str()
            .is_some_and(|m| m.contains("dall-e-9"))
    );
}

#[tokio::test]
async fn an_image_at_a_url_keeps_its_url_and_revised_prompt() {
    let fake = Fake::openai().await;
    let gateway = gateway(OpenAiImages, &fake);
    let asked = json!({"model": "dall-e-3", "prompt": "a cat", "response_format": "url"});
    let (status, _, body) = send(&gateway, json_request("/v1/images/generations", asked)).await;
    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
    assert_eq!(
        json_of(&body)["data"],
        json!([{"url": "https://images.test/0.png", "revised_prompt": "a cat, revised"}])
    );
}
