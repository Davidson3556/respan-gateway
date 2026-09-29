use super::*;

#[tokio::test]
async fn only_the_text_only_attempt_gets_text_and_the_fallback_the_original() -> Result<(), Fault> {
    let mock = MockTransport::new();
    mock.reply(TEXT, mock::json(StatusCode::SERVICE_UNAVAILABLE, "{}"));
    mock.reply(PDF, mock::json(StatusCode::OK, OK));
    let gw = gateway(&mock, Arc::new(PdfToText::new(&["text-only"])))?;
    assert_eq!(send(&gw, "acme", REQUEST).await, StatusCode::OK);
    let sent = mock.sent();
    let converted = String::from_utf8_lossy(sent[0].body());
    assert!(converted.contains("[text of a 24-byte PDF]"), "{converted}");
    assert!(!converted.contains("file_data"), "{converted}");
    assert_eq!(sent[1].body(), REQUEST, "the fallback, the original");
    Ok(())
}

#[tokio::test]
async fn a_fallback_in_another_format_gets_the_pdf_in_its_own_shape() -> Result<(), Fault> {
    let mock = MockTransport::new();
    mock.reply(TEXT, mock::json(StatusCode::SERVICE_UNAVAILABLE, "{}"));
    mock.reply(PDF, mock::json(StatusCode::OK, CLAUDE_OK));
    let gw = gateway_claude(&mock, Arc::new(PdfToText::new(&["text-only"])))?;
    let mut req = post("/v1/chat/completions", REQUEST.to_owned());
    req.headers_mut()
        .insert("x-tenant", HeaderValue::from_static("acme"));
    let (status, _, body) = read_whole(serve(&gw, req).await.0).await;
    assert_eq!(status, StatusCode::OK);
    assert!(String::from_utf8_lossy(&body?).contains("Revenue is up."));
    let sent = mock.sent();
    assert!(String::from_utf8_lossy(sent[0].body()).contains("[text of a 24-byte PDF]"));
    assert!(sent[1].uri().path().ends_with("/messages"));
    let body: serde_json::Value =
        serde_json::from_slice(sent[1].body()).map_err(|_| Fault::internal("json"))?;
    let part = &body["messages"][0]["content"][1];
    assert_eq!(part["type"], "document");
    assert_eq!(part["source"]["media_type"], "application/pdf");
    assert_eq!(part["source"]["data"], "JVBERi0xLjcKJcfsj6IKMSAwIG9iago=");
    Ok(())
}

#[tokio::test]
async fn a_part_in_neither_shape_is_refused_by_the_other_format() -> Result<(), Fault> {
    let mock = MockTransport::new();
    // The text-only target fails, and no plugin rewrites the image for Claude.
    mock.reply(TEXT, mock::json(StatusCode::SERVICE_UNAVAILABLE, "{}"));
    let gw = gateway_claude(&mock, Arc::new(PdfToText::new(&["text-only"])))?;
    let image = r#"{"model":"any","messages":[{"role":"user","content":[{"type":"image_url","image_url":{"url":"http://x/y.png"}}]}]}"#;
    assert_eq!(send(&gw, "acme", image).await, StatusCode::BAD_REQUEST);
    assert_eq!(mock.sent().len(), 1, "nothing was sent to Claude");
    Ok(())
}

#[tokio::test]
async fn the_same_pdf_is_converted_once_per_tenant() -> Result<(), Fault> {
    let mock = MockTransport::new();
    for _ in 0..3 {
        mock.reply(TEXT, mock::json(StatusCode::OK, OK));
    }
    let pdfs = Arc::new(PdfToText::new(&["text-only"]));
    let gw = gateway(&mock, pdfs.clone())?;
    for tenant in ["acme", "acme", "beta"] {
        assert_eq!(send(&gw, tenant, REQUEST).await, StatusCode::OK);
    }
    assert_eq!(pdfs.conversions.load(Ordering::Relaxed), 2, "once each");
    assert_eq!(mock.sent()[0].body(), mock.sent()[1].body());
    Ok(())
}

#[tokio::test]
async fn stored_oversized_and_slow_pdfs_fail_before_any_call() -> Result<(), Fault> {
    let mock = MockTransport::new();
    // Real time: a blocking thread does not see tokio's paused clock.
    let strict = PdfToText {
        convert: |_| {
            std::thread::sleep(Duration::from_millis(300));
            Ok(String::new())
        },
        budget: Duration::from_millis(10),
        max: 8,
        ..PdfToText::new(&["text-only", "reads-pdf"])
    };
    let gw = gateway(&mock, Arc::new(strict))?;
    let stored = r#"{"model":"any","messages":[{"role":"user","content":[{"type":"file","file":{"file_id":"file-abc"}}]}]}"#;
    assert_eq!(send(&gw, "acme", stored).await, StatusCode::BAD_REQUEST);
    let large = send(&gw, "acme", REQUEST).await;
    assert_eq!(large, StatusCode::PAYLOAD_TOO_LARGE);
    let small = REQUEST.replace("JVBERi0xLjcKJcfsj6IKMSAwIG9iago=", "JVBE");
    let slow = send(&gw, "acme", &small).await;
    assert_eq!(slow, StatusCode::GATEWAY_TIMEOUT);
    assert!(mock.sent().is_empty());
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn the_budget_is_for_the_whole_request() -> Result<(), Fault> {
    use std::sync::atomic::{AtomicBool, AtomicUsize};
    static CONVERSIONS: AtomicUsize = AtomicUsize::new(0);
    static RELEASED: AtomicBool = AtomicBool::new(false);

    let mock = MockTransport::new();
    mock.reply(TEXT, mock::json(StatusCode::OK, OK));
    // Virtual time: a conversion takes 80 ms by moving the paused clock, so
    // each PDF fits the 120 ms budget and the two do not, whatever the
    // load. The third conversion (the second PDF of the second request)
    // then waits, so the deadline it passed is what ends the request.
    let pdfs = PdfToText {
        convert: |_| {
            let took = tokio::time::advance(Duration::from_millis(80));
            tokio::runtime::Handle::current().block_on(took);
            if CONVERSIONS.fetch_add(1, Ordering::SeqCst) >= 2 {
                // Bounded, so a budget that failed to end the request fails
                // the test instead of hanging it.
                for _ in 0..5000 {
                    if RELEASED.load(Ordering::SeqCst) {
                        break;
                    }
                    std::thread::sleep(Duration::from_millis(1));
                }
            }
            Ok(String::new())
        },
        budget: Duration::from_millis(120),
        ..PdfToText::new(&["text-only", "reads-pdf"])
    };
    let gw = gateway(&mock, Arc::new(pdfs))?;
    let one = send(&gw, "acme", REQUEST).await;
    assert_eq!(one, StatusCode::OK);
    let part = r#"{"type":"file","file":{"filename":"q3.pdf","file_data":"data:application/pdf;base64,JVBERi0xLjcKJcfsj6IKMSAwIG9iago="}}"#;
    let second = part.replace("JVBERi0x", "QUJDRA");
    let two = REQUEST.replace(part, &format!("{part},{second}"));
    // The fallback gets no fresh budget either.
    let timed_out = send(&gw, "beta", &two).await;
    RELEASED.store(true, Ordering::SeqCst);
    assert_eq!(timed_out, StatusCode::GATEWAY_TIMEOUT);
    assert_eq!(mock.sent().len(), 1, "only the first request's call");
    Ok(())
}

#[tokio::test]
async fn a_zero_retention_pdf_is_converted_but_never_remembered() -> Result<(), Fault> {
    let mock = MockTransport::new();
    for _ in 0..3 {
        mock.reply(TEXT, mock::json(StatusCode::OK, OK));
    }
    let pdfs = Arc::new(PdfToText::new(&["text-only"]));
    let gw = gateway(&mock, pdfs.clone())?;
    for _ in 0..2 {
        let mut req = post("/v1/chat/completions", REQUEST.to_owned());
        req.headers_mut()
            .insert("x-tenant", HeaderValue::from_static("acme"));
        let mut values = respan_gateway_core::Extensions::new();
        values.insert(NoContent);
        req.extensions_mut().insert(values);
        assert_eq!(read_whole(serve(&gw, req).await.0).await.0, StatusCode::OK);
    }
    assert_eq!(
        pdfs.conversions.load(Ordering::Relaxed),
        2,
        "converted each time"
    );
    assert!(pdfs.memo().is_empty(), "nothing stored");
    assert_eq!(send(&gw, "acme", REQUEST).await, StatusCode::OK);
    assert_eq!(
        pdfs.conversions.load(Ordering::Relaxed),
        3,
        "not served from a memo"
    );
    Ok(())
}
