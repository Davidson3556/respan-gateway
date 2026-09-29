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
async fn a_part_in_neither_shape_is_never_sent_to_the_other_format() -> Result<(), Fault> {
    let mock = MockTransport::new();
    // The text-only target fails, and no plugin rewrites the image for Claude.
    mock.reply(TEXT, mock::json(StatusCode::SERVICE_UNAVAILABLE, "{}"));
    let gw = gateway_claude(&mock, Arc::new(PdfToText::new(&["text-only"])))?;
    let image = r#"{"model":"any","messages":[{"role":"user","content":[{"type":"image_url","image_url":{"url":"http://x/y.png"}}]}]}"#;
    // The client gets the provider's answer, not the fallback's refusal.
    assert_eq!(
        send(&gw, "acme", image).await,
        StatusCode::SERVICE_UNAVAILABLE
    );
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
async fn oversized_and_slow_pdfs_fail_before_any_call() -> Result<(), Fault> {
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
    let large = send(&gw, "acme", REQUEST).await;
    assert_eq!(large, StatusCode::PAYLOAD_TOO_LARGE);
    let small = REQUEST.replace("JVBERi0xLjcKJcfsj6IKMSAwIG9iago=", "JVBE");
    let slow = send(&gw, "acme", &small).await;
    assert_eq!(slow, StatusCode::GATEWAY_TIMEOUT);
    assert!(mock.sent().is_empty());
    Ok(())
}

/// A request naming a stored file, and the file: 12 bytes.
const STORED: &str = r#"{"model":"any","messages":[{"role":"user","content":[{"type":"text","text":"Summarise this."},{"type":"file","file":{"file_id":"file-abc"}}]}]}"#;
const FILE: &str = "%PDF-1.7 abc";

#[tokio::test]
async fn a_stored_file_is_fetched_under_the_tenants_key_and_converted() -> Result<(), Fault> {
    let mock = MockTransport::new();
    mock.reply(STORE, mock::json(StatusCode::OK, FILE));
    mock.reply(TEXT, mock::json(StatusCode::SERVICE_UNAVAILABLE, "{}"));
    mock.reply(PDF, mock::json(StatusCode::OK, OK));
    let gw = gateway(&mock, Arc::new(PdfToText::new(&["text-only"])))?;
    assert_eq!(send(&gw, "acme", STORED).await, StatusCode::OK);

    let sent = mock.sent();
    assert_eq!(
        sent[0].uri().to_string(),
        "http://files.test/v1/files/file-abc/content"
    );
    assert_eq!(
        sent[0].headers()["authorization"],
        "Bearer tenant-key",
        "the tenant's own key"
    );
    let text_only = String::from_utf8_lossy(sent[1].body());
    assert!(text_only.contains("[text of a 12-byte PDF]"), "{text_only}");
    // The fallback that reads PDFs gets the file inline, not its id.
    let inline = String::from_utf8_lossy(sent[2].body());
    assert!(
        inline.contains("data:application/pdf;base64,JVBERi0xLjcgYWJj"),
        "{inline}"
    );
    assert!(!inline.contains("file-abc"), "{inline}");
    Ok(())
}

#[tokio::test]
async fn a_stored_file_reaches_an_anthropic_fallback_as_a_document() -> Result<(), Fault> {
    let mock = MockTransport::new();
    mock.reply(STORE, mock::json(StatusCode::OK, FILE));
    mock.reply(TEXT, mock::json(StatusCode::SERVICE_UNAVAILABLE, "{}"));
    mock.reply(PDF, mock::json(StatusCode::OK, CLAUDE_OK));
    let gw = gateway_claude(&mock, Arc::new(PdfToText::new(&["text-only"])))?;
    assert_eq!(send(&gw, "acme", STORED).await, StatusCode::OK);
    let claude: serde_json::Value =
        serde_json::from_slice(mock.sent()[2].body()).map_err(|_| Fault::internal("json"))?;
    let part = &claude["messages"][0]["content"][1];
    assert_eq!(part["source"]["data"], "JVBERi0xLjcgYWJj");
    Ok(())
}

#[tokio::test]
async fn a_stored_file_that_is_missing_or_too_large_fails_before_any_call() -> Result<(), Fault> {
    let mock = MockTransport::new();
    mock.reply(
        STORE,
        mock::json(StatusCode::NOT_FOUND, r#"{"error":"no file"}"#),
    );
    let gw = gateway(&mock, Arc::new(PdfToText::new(&["text-only"])))?;
    assert_eq!(send(&gw, "acme", STORED).await, StatusCode::NOT_FOUND);

    let strict = PdfToText {
        max: 8,
        ..PdfToText::new(&["text-only"])
    };
    mock.reply(STORE, mock::json(StatusCode::OK, FILE));
    let gw = gateway(&mock, Arc::new(strict))?;
    assert_eq!(
        send(&gw, "acme", STORED).await,
        StatusCode::PAYLOAD_TOO_LARGE
    );
    assert!(
        mock.sent()
            .iter()
            .all(|c| c.uri().host() == Some("files.test"))
    );
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

#[tokio::test]
async fn an_agent_request_with_tools_and_a_pdf_falls_back_to_claude() -> Result<(), Fault> {
    let mock = MockTransport::new();
    mock.reply(TEXT, mock::json(StatusCode::SERVICE_UNAVAILABLE, "{}"));
    mock.reply(PDF, mock::json(StatusCode::OK, CLAUDE_OK));
    let gw = gateway_claude(&mock, Arc::new(PdfToText::new(&["text-only"])))?;
    let tool = r#"{"type":"function","function":{"name":"read","parameters":{"type":"object"}}}"#;
    let body = REQUEST.replace(r#"{"model""#, &format!(r#"{{"tools":[{tool}],"model""#));
    assert_eq!(send(&gw, "acme", &body).await, StatusCode::OK);
    let sent: serde_json::Value =
        serde_json::from_slice(mock.sent()[1].body()).map_err(|_| Fault::internal("json"))?;
    assert_eq!(sent["tools"][0]["name"], "read");
    assert_eq!(sent["tools"][0]["input_schema"]["type"], "object");
    assert_eq!(sent["messages"][0]["content"][1]["type"], "document");
    Ok(())
}
