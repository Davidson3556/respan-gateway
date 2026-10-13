//! Each kind routed by its model.

use std::sync::Arc;

use respan_gateway_core::{Gateway, Provider};
use respan_gateway_http::http::StatusCode;
use respan_gateway_media::{image, speech, transcription};
use respan_gateway_media_http::openai::{OpenAiImages, OpenAiSpeech, OpenAiTranscriptions};
use respan_gateway_plugin_routing::{Kind, Model, Routes, Routing, Speaks};
use serde_json::json;

use crate::support::{Fake, Form, json_request, send};

/// A kind of `label`, which translates.
const fn kind(label: &'static str) -> Kind {
    Kind {
        label,
        translates: true,
    }
}

const IMAGE: Kind = kind("image");
const TRANSCRIPTION: Kind = kind("transcription");
const SPEECH: Kind = kind("speech");
const VIDEO: Kind = kind("video");

fn speaks(format: &str, kind: Kind, internal: &str) -> Speaks {
    Speaks {
        format: format.into(),
        kind,
        internal: internal.into(),
    }
}

#[tokio::test]
async fn routing_sends_each_kind_to_its_models_provider() {
    let (main, other) = (Fake::openai().await, Fake::openai().await);
    let provider = |name: &str| {
        vec![
            speaks(OpenAiImages::NAME, IMAGE, &format!("{name}-images")),
            speaks(
                OpenAiTranscriptions::NAME,
                TRANSCRIPTION,
                &format!("{name}-audio"),
            ),
            speaks(OpenAiSpeech::NAME, SPEECH, &format!("{name}-speech")),
        ]
    };
    let routes = Routes::new()
        .provider("main", provider("main"))
        .provider("other", provider("other"))
        .claim("dall-e", "other")
        .claim("whisper", "other")
        .claim("tts", "other")
        .target("main");
    let routing = Routing::new(Arc::new(routes))
        .format(
            OpenAiImages::NAME,
            IMAGE,
            Model::of(image::Input::model, image::Input::set_model),
        )
        .format(
            OpenAiTranscriptions::NAME,
            TRANSCRIPTION,
            Model::of(transcription::Input::model, transcription::Input::set_model),
        )
        .format(
            OpenAiSpeech::NAME,
            SPEECH,
            Model::of(speech::Input::model, speech::Input::set_model),
        );
    let gateway = Gateway::new()
        .provider(Provider::new("main-images", OpenAiImages, main.transport()))
        .and_then(|g| {
            g.provider(Provider::new(
                "other-images",
                OpenAiImages,
                other.transport(),
            ))
        })
        .and_then(|g| {
            g.provider(Provider::new(
                "main-audio",
                OpenAiTranscriptions,
                main.transport(),
            ))
        })
        .and_then(|g| {
            g.provider(Provider::new(
                "other-audio",
                OpenAiTranscriptions,
                other.transport(),
            ))
        })
        .and_then(|g| g.provider(Provider::new("main-speech", OpenAiSpeech, main.transport())))
        .and_then(|g| {
            g.provider(Provider::new(
                "other-speech",
                OpenAiSpeech,
                other.transport(),
            ))
        })
        .and_then(|g| g.client::<image::Input, _, _>(OpenAiImages))
        .and_then(|g| g.client::<transcription::Input, _, _>(OpenAiTranscriptions))
        .and_then(|g| g.client::<speech::Input, _, _>(OpenAiSpeech))
        .expect("a gateway")
        .plugin(routing);

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
        // request goes to the target.
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
    use respan_gateway_media::video::{self, LocateContent};
    use respan_gateway_media_http::gemini::GeminiVeo;
    use respan_gateway_media_http::openai::OpenAiVideos;

    use crate::support::bodiless;
    use crate::support::veo::Veo;
    use crate::support::videos::Videos;

    let (openai, veo) = (Videos::openai().await, Veo::start().await);
    // A job's id says whose it is: OpenAI's start `video_`, Veo's `veo_`.
    let routes = Routes::new()
        .provider("openai", vec![speaks(OpenAiVideos::NAME, VIDEO, "openai")])
        .provider("veo", vec![speaks(GeminiVeo::NAME, VIDEO, "veo")])
        .claim("veo", "veo")
        .claim("sora", "openai")
        .claim("video_", "openai");
    let model = Model::of(
        |i: &video::Input| i.model().or(i.job_id()),
        video::Input::set_model,
    );
    let routing = Routing::new(Arc::new(routes)).format(OpenAiVideos::NAME, VIDEO, model);
    let gateway = Gateway::new()
        .provider(Provider::new("openai", OpenAiVideos, openai.transport()))
        .and_then(|g| g.provider(Provider::new("veo", GeminiVeo, veo.transport())))
        .and_then(|g| g.client::<video::Input, _, _>(OpenAiVideos))
        .expect("a gateway")
        .plugin(routing)
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
