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

#[tokio::test]
async fn the_budget_is_for_the_whole_request() -> Result<(), Fault> {
    let mock = MockTransport::new();
    mock.reply(TEXT, mock::json(StatusCode::OK, OK));
    // Real time, as above: each PDF fits the budget, the two do not.
    let pdfs = PdfToText {
        convert: |_| {
            std::thread::sleep(Duration::from_millis(80));
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
    assert_eq!(send(&gw, "beta", &two).await, StatusCode::GATEWAY_TIMEOUT);
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
