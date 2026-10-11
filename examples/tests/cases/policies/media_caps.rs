use super::*;

fn setup(fake: &FakeOpenAi, meter: &Meter) -> Result<Gateway, Fault> {
    let transport = provider_support::openai(&fake.base_url())?.connect();
    gateway(transport, caps(meter))
}

fn image_request(key: &str, extra: Value) -> Request<Body> {
    let mut body = json!({ "model": "gpt-image-1", "prompt": "a fox" });
    body.as_object_mut().into_iter().for_each(|m| {
        m.extend(extra.as_object().cloned().unwrap_or_default());
    });
    json_request("/v1/images/generations", key, &body)
}

fn error_code(body: &str) -> Option<String> {
    let body: Value = serde_json::from_str(body).ok()?;
    body["error"]["code"].as_str().map(str::to_owned)
}

#[tokio::test]
async fn images_over_the_count_or_edge_are_refused_before_the_provider() -> Result<(), Fault> {
    let fake = FakeOpenAi::start().await;
    let gw = setup(&fake, &Meter::default())?;
    for (extra, code) in [
        (json!({ "n": 3 }), "too_many_images"),
        (json!({ "size": "2048x1024" }), "image_too_large"),
        (json!({ "size": "huge" }), "bad_image_size"),
    ] {
        let (status, body) = send(&gw, image_request("gw-acme", extra)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert_eq!(error_code(&body).as_deref(), Some(code), "{body}");
    }
    assert!(fake.requests().is_empty());
    Ok(())
}

#[tokio::test]
async fn images_within_the_caps_are_relayed() -> Result<(), Fault> {
    let fake = FakeOpenAi::start().await;
    fake.answer(Answer::image(&b"\x89PNG\r\n\x1a\nfox"[..]));
    let gw = setup(&fake, &Meter::default())?;
    let extra = json!({ "n": 1, "size": "1024x1024" });
    let (status, body) = send(&gw, image_request("gw-acme", extra)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(fake.requests().len(), 1);
    Ok(())
}

#[tokio::test]
async fn embedding_tokens_are_metered_per_client_key() -> Result<(), Fault> {
    let fake = FakeOpenAi::start().await;
    let meter = Meter::default();
    let gw = setup(&fake, &meter)?;
    let embed = |input: &str| json!({ "model": "text-embedding-3-small", "input": input });
    for (key, input) in [
        ("gw-acme", "one two three"),
        ("gw-beta", "one"),
        ("gw-acme", "four five"),
    ] {
        fake.answer(Answer::embedding(vec![0.5, 0.25]));
        let (status, body) = send(&gw, json_request("/v1/embeddings", key, &embed(input))).await;
        assert_eq!(status, StatusCode::OK, "{body}");
    }
    let (acme, beta) = (meter.used("gw-acme"), meter.used("gw-beta"));
    assert!(
        acme.embedding_tokens > beta.embedding_tokens,
        "{acme:?} {beta:?}"
    );
    assert!(beta.embedding_tokens > 0, "{beta:?}");
    assert_eq!(meter.used("gw-other"), Used::default());
    Ok(())
}

#[tokio::test]
async fn an_oversized_upload_is_refused_before_the_provider() -> Result<(), Fault> {
    let fake = FakeOpenAi::start().await;
    let gw = setup(&fake, &Meter::default())?;
    let (status, body) = send(&gw, upload("gw-acme", (1 << 20) + 1)).await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE, "{body}");
    assert_eq!(
        error_code(&body).as_deref(),
        Some("audio_too_large"),
        "{body}"
    );
    assert!(fake.requests().is_empty());
    Ok(())
}

#[tokio::test]
async fn a_key_past_its_audio_budget_is_refused_and_others_are_not() -> Result<(), Fault> {
    let fake = FakeOpenAi::start().await;
    let meter = Meter::default();
    let gw = setup(&fake, &meter)?;
    // 160 words of 0.5 seconds each: 80 seconds against a budget of 60.
    let words = vec!["word"; 160].join(" ");
    fake.answer(Answer::transcript(words.clone()));
    let (status, body) = send(&gw, upload("gw-acme", 1024)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(meter.used("gw-acme").audio_seconds >= 60.0);

    let (status, body) = send(&gw, upload("gw-acme", 1024)).await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS, "{body}");
    assert_eq!(
        error_code(&body).as_deref(),
        Some("audio_budget_spent"),
        "{body}"
    );
    assert_eq!(fake.requests().len(), 1);

    fake.answer(Answer::transcript(words));
    let (status, body) = send(&gw, upload("gw-beta", 1024)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    Ok(())
}
