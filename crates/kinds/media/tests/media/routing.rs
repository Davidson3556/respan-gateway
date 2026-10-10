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
