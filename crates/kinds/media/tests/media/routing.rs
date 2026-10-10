//! Each kind routed by its model.

use respan_gateway_core::Gateway;
use respan_gateway_http::http::StatusCode;
use respan_gateway_media::openai::{OpenAiImages, OpenAiSpeech, OpenAiTranscriptions};
use respan_gateway_media::{image, speech, transcription};
use respan_gateway_plugins::ModelRouter;
use serde_json::json;

use crate::support::{Fake, Form, json_request, send};

#[tokio::test]
async fn a_model_router_sends_each_kind_to_its_models_provider() {
    let (main, other) = (Fake::openai().await, Fake::openai().await);
    let gateway = Gateway::new()
        .provider("main-images", OpenAiImages, main.transport())
        .and_then(|g| g.provider("other-images", OpenAiImages, other.transport()))
        .and_then(|g| g.provider("main-audio", OpenAiTranscriptions, main.transport()))
        .and_then(|g| g.provider("other-audio", OpenAiTranscriptions, other.transport()))
        .and_then(|g| g.provider("main-speech", OpenAiSpeech, main.transport()))
        .and_then(|g| g.provider("other-speech", OpenAiSpeech, other.transport()))
        .and_then(|g| g.client::<image::Input, _, _>(OpenAiImages))
        .and_then(|g| g.client::<transcription::Input, _, _>(OpenAiTranscriptions))
        .and_then(|g| g.client::<speech::Input, _, _>(OpenAiSpeech))
        .and_then(|g| g.target("main-images"))
        .expect("a gateway")
        .plugin(
            ModelRouter::new(|i: &image::Input| i.model.as_deref()).route("dall-e", "other-images"),
        )
        .plugin(
            ModelRouter::new(|i: &transcription::Input| i.model.as_deref())
                .route("gpt-4o", "main-audio")
                .route("whisper", "other-audio"),
        )
        .plugin(
            ModelRouter::new(|i: &speech::Input| i.model.as_deref())
                .route("tts", "other-speech")
                .route("gpt", "main-speech"),
        );

    let image = |model: Option<&str>| {
        let mut asked = json!({"prompt": "a cat"});
        if let Some(model) = model {
            asked["model"] = model.into();
        }
        json_request("/v1/images/generations", asked)
    };
    let audio = |model: &str| {
        Form::default()
            .file("file", "a.wav", "audio/wav", b"RIFF\0\0\0\0WAVE")
            .text("model", model)
            .to("/v1/audio/transcriptions")
    };
    let speak = |model: &str| {
        json_request(
            "/v1/audio/speech",
            json!({"model": model, "input": "hi", "voice": "alloy"}),
        )
    };
    for (request, routed_to_other) in [
        (image(Some("gpt-image-1")), false),
        (image(Some("dall-e-3")), true),
        // Without a model the provider's default, dall-e-2, draws: the
        // request keeps its target.
        (image(None), false),
        (audio("whisper-1"), true),
        (audio("gpt-4o-transcribe"), false),
        (speak("tts-1"), true),
        (speak("gpt-4o-mini-tts"), false),
    ] {
        let (before_main, before_other) = (main.seen().len(), other.seen().len());
        let path = request.uri().path().to_owned();
        let (status, _, body) = send(&gateway, request).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "{path}: {}",
            String::from_utf8_lossy(&body)
        );
        let went = (
            main.seen().len() - before_main,
            other.seen().len() - before_other,
        );
        let want = if routed_to_other { (0, 1) } else { (1, 0) };
        assert_eq!(went, want, "{path}");
    }
}

#[tokio::test]
async fn a_video_goes_to_its_models_provider_and_a_follow_up_to_its_jobs() {
    use respan_gateway_http::http::Method;
    use respan_gateway_media::gemini::GeminiVeo;
    use respan_gateway_media::openai::OpenAiVideos;
    use respan_gateway_media::video::{self, LocateContent};

    use crate::support::bodiless;
    use crate::support::veo::Veo;
    use crate::support::videos::Videos;

    let (openai, veo) = (Videos::openai().await, Veo::start().await);
    let gateway = Gateway::new()
        .provider("openai", OpenAiVideos, openai.transport())
        .and_then(|g| g.provider("veo", GeminiVeo, veo.transport()))
        .and_then(|g| g.client::<video::Input, _, _>(OpenAiVideos))
        .and_then(|g| g.target("openai"))
        .expect("a gateway")
        // A job's id says whose it is: OpenAI's start `video_`, Veo's `veo_`.
        .plugin(
            ModelRouter::new(|i: &video::Input| i.model().or(i.job_id()))
                .route("veo", "veo")
                .route("sora", "openai")
                .route("video_", "openai"),
        )
        .plugin(LocateContent::new().provider("veo"));

    let mut ids = Vec::new();
    for model in ["sora-2", "veo-3.0-generate-001"] {
        let asked = json!({"model": model, "prompt": "a fox"});
        let (status, _, body) = send(&gateway, json_request("/v1/videos", asked)).await;
        assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
        let job: serde_json::Value = serde_json::from_slice(&body).expect("a job");
        ids.push(job["id"].as_str().expect("an id").to_owned());
    }
    assert_eq!((openai.seen().len(), veo.seen().len()), (1, 1));
    for (id, to_veo) in [(&ids[0], false), (&ids[1], true)] {
        let before = (openai.seen().len(), veo.seen().len());
        let request = bodiless(Method::GET, &format!("/v1/videos/{id}"));
        let (status, _, body) = send(&gateway, request).await;
        assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
        let went = (openai.seen().len() - before.0, veo.seen().len() - before.1);
        assert_eq!(went, if to_veo { (0, 1) } else { (1, 0) }, "{id}");
    }
}
