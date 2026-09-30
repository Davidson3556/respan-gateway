use super::*;

#[tokio::test]
async fn only_the_text_only_attempt_gets_text_and_the_fallback_the_original() -> Result<(), Fault> {
    let mock = MockTransport::new();
    mock.reply(TEXT, mock::json(StatusCode::SERVICE_UNAVAILABLE, "{}"));
    mock.reply(PDF, mock::json(StatusCode::OK, OK));
    let gw = gateway(&mock, Arc::new(PdfToText::new(&["text-only"]).await?))?;
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
    mock.reply(CLAUDE, mock::json(StatusCode::OK, CLAUDE_OK));
    let gw = gateway_claude(&mock, Arc::new(PdfToText::new(&["text-only"]).await?))?;
    let mut req = post("/v1/chat/completions", REQUEST.to_owned());
    req.headers_mut()
        .insert("x-tenant", HeaderValue::from_static("acme"));
    let (status, _, body) = read_whole(serve(&gw, req).await.0).await;
    assert_eq!(status, StatusCode::OK);
    assert!(String::from_utf8_lossy(&body?).contains("Revenue is up."));
    let sent = mock.sent();
    assert!(String::from_utf8_lossy(sent[0].body()).contains("[text of a 24-byte PDF]"));
    assert_eq!(sent[1].uri().path(), "/v1/messages");
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
    let gw = gateway_claude(&mock, Arc::new(PdfToText::new(&["text-only"]).await?))?;
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
    let pdfs = Arc::new(PdfToText::new(&["text-only"]).await?);
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
        ..PdfToText::new(&["text-only", "reads-pdf"]).await?
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
    mock.reply(
        STORE,
        mock::json(StatusCode::OK, r#"{"filename":"report.pdf"}"#),
    );
    mock.reply(STORE, mock::json(StatusCode::OK, FILE));
    mock.reply(TEXT, mock::json(StatusCode::SERVICE_UNAVAILABLE, "{}"));
    mock.reply(PDF, mock::json(StatusCode::OK, OK));
    let gw = gateway(&mock, Arc::new(PdfToText::new(&["text-only"]).await?))?;
    assert_eq!(send(&gw, "acme", STORED).await, StatusCode::OK);

    let sent = mock.sent();
    assert_eq!(
        sent[0].uri().to_string(),
        "http://files.test/v1/files/file-abc"
    );
    assert_eq!(
        sent[0].headers()["authorization"],
        "Bearer tenant-key",
        "the tenant's own key"
    );
    let text_only = String::from_utf8_lossy(sent[2].body());
    assert!(text_only.contains("[text of a 12-byte PDF]"), "{text_only}");
    // The fallback that reads PDFs gets the file inline, not its id.
    let inline = String::from_utf8_lossy(sent[3].body());
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
    mock.reply(
        STORE,
        mock::json(StatusCode::OK, r#"{"filename":"report.pdf"}"#),
    );
    mock.reply(STORE, mock::json(StatusCode::OK, FILE));
    mock.reply(TEXT, mock::json(StatusCode::SERVICE_UNAVAILABLE, "{}"));
    mock.reply(CLAUDE, mock::json(StatusCode::OK, CLAUDE_OK));
    let gw = gateway_claude(&mock, Arc::new(PdfToText::new(&["text-only"]).await?))?;
    assert_eq!(send(&gw, "acme", STORED).await, StatusCode::OK);
    let claude: serde_json::Value =
        serde_json::from_slice(mock.sent()[3].body()).map_err(|_| Fault::internal("json"))?;
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
    let gw = gateway(&mock, Arc::new(PdfToText::new(&["text-only"]).await?))?;
    assert_eq!(send(&gw, "acme", STORED).await, StatusCode::NOT_FOUND);

    let strict = PdfToText {
        max: 8,
        ..PdfToText::new(&["text-only"]).await?
    };
    mock.reply(
        STORE,
        mock::json(StatusCode::OK, r#"{"filename":"report.pdf"}"#),
    );
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
        ..PdfToText::new(&["text-only", "reads-pdf"]).await?
    };
    let gw = gateway(&mock, Arc::new(pdfs))?;
    let one = send(&gw, "acme", REQUEST).await;
    assert_eq!(one, StatusCode::OK);
    let part = r#"{"type":"file","file":{"filename":"q3.pdf","file_data":"data:application/pdf;base64,JVBERi0xLjcKJcfsj6IKMSAwIG9iago="}}"#;
    let second = part.replace("JVBERi0x", "QUJDREVG");
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
    let pdfs = Arc::new(PdfToText::new(&["text-only"]).await?);
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
    let stored = pdfs
        .cache
        .transact(|tx| {
            tx.query_row("SELECT COUNT(*) FROM attachment_text", [], |row| {
                row.get::<_, i64>(0)
            })
            .map_err(sql)
        })
        .await?;
    assert_eq!(stored, 0, "nothing stored");
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
    mock.reply(CLAUDE, mock::json(StatusCode::OK, CLAUDE_OK));
    let gw = gateway_claude(&mock, Arc::new(PdfToText::new(&["text-only"]).await?))?;
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

#[tokio::test]
async fn stored_file_types_and_ids_are_validated_before_conversion() -> Result<(), Fault> {
    for (metadata, bytes, calls) in [
        (r#"{"filename":"secret.txt"}"#, None, 1),
        (r#"{"filename":"report.pdf"}"#, Some("not a PDF"), 2),
        (
            r#"{"filename":"report.pdf","mime_type":"text/plain"}"#,
            None,
            1,
        ),
    ] {
        let mock = MockTransport::new();
        mock.reply(STORE, mock::json(StatusCode::OK, metadata));
        if let Some(bytes) = bytes {
            mock.reply(STORE, mock::json(StatusCode::OK, bytes));
        }
        let gw = gateway(&mock, Arc::new(PdfToText::new(&["text-only"]).await?))?;
        assert_eq!(
            send(&gw, "acme", STORED).await,
            StatusCode::UNSUPPORTED_MEDIA_TYPE
        );
        assert_eq!(mock.sent().len(), calls);
    }
    let mock = MockTransport::new();
    let gw = gateway(&mock, Arc::new(PdfToText::new(&["text-only"]).await?))?;
    for id in ["../other", "file-abc?key=other", "file-abc/content"] {
        assert_eq!(
            send(&gw, "acme", &STORED.replace("file-abc", id)).await,
            StatusCode::BAD_REQUEST
        );
    }
    assert!(mock.sent().is_empty());
    Ok(())
}

#[tokio::test]
async fn cached_text_survives_reopening_and_can_be_deleted_per_tenant() -> Result<(), Fault> {
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| Fault::internal("clock"))?
        .as_nanos();
    let directory =
        std::env::temp_dir().join(format!("respan-attachment-{}-{nonce}", std::process::id()));
    std::fs::create_dir(&directory)
        .map_err(|e| Fault::internal("temp directory").with_source(e))?;
    let path = directory.join("cache.sqlite");
    {
        let first = PdfToText::with_store(&["text-only"], Database::open(&path)?).await?;
        first
            .text(
                "acme",
                "JVBE".into(),
                Instant::now() + Duration::from_secs(5),
                true,
            )
            .await?;
        assert_eq!(first.conversions.load(Ordering::Relaxed), 1);
    }
    {
        let reopened = PdfToText::with_store(&["text-only"], Database::open(&path)?).await?;
        reopened
            .text(
                "acme",
                "JVBE".into(),
                Instant::now() + Duration::from_secs(5),
                true,
            )
            .await?;
        assert_eq!(reopened.conversions.load(Ordering::Relaxed), 0);
        reopened
            .text(
                "beta",
                "JVBE".into(),
                Instant::now() + Duration::from_secs(5),
                true,
            )
            .await?;
        assert_eq!(reopened.conversions.load(Ordering::Relaxed), 1);
        reopened.forget("acme").await?;
        reopened
            .text(
                "acme",
                "JVBE".into(),
                Instant::now() + Duration::from_secs(5),
                true,
            )
            .await?;
        assert_eq!(reopened.conversions.load(Ordering::Relaxed), 2);
        reopened
            .text(
                "beta",
                "JVBE".into(),
                Instant::now() + Duration::from_secs(5),
                true,
            )
            .await?;
        assert_eq!(
            reopened.conversions.load(Ordering::Relaxed),
            2,
            "other tenant retained"
        );
    }
    std::fs::remove_dir_all(directory).map_err(|e| Fault::internal("cleanup").with_source(e))?;
    Ok(())
}

/// A small real PDF with a complete cross-reference table.
fn pdf_document(text: &str) -> String {
    let content = format!("BT /F1 12 Tf 10 100 Td ({text}) Tj ET");
    let objects = [
        "<< /Type /Catalog /Pages 2 0 R >>".to_owned(),
        "<< /Type /Pages /Kids [3 0 R] /Count 1 >>".to_owned(),
        "<< /Type /Page /Parent 2 0 R /MediaBox [0 0 300 200] /Resources << /Font << /F1 4 0 R >> >> /Contents 5 0 R >>".to_owned(),
        "<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>".to_owned(),
        format!("<< /Length {} >>\nstream\n{content}\nendstream", content.len()),
    ];
    let mut pdf = "%PDF-1.4\n".to_owned();
    let mut offsets = Vec::new();
    for (index, object) in objects.iter().enumerate() {
        offsets.push(pdf.len());
        pdf.push_str(&format!("{} 0 obj\n{object}\nendobj\n", index + 1));
    }
    let xref = pdf.len();
    pdf.push_str("xref\n0 6\n0000000000 65535 f \n");
    for offset in offsets {
        pdf.push_str(&format!("{offset:010} 00000 n \n"));
    }
    pdf.push_str(&format!(
        "trailer\n<< /Size 6 /Root 1 0 R >>\nstartxref\n{xref}\n%%EOF\n"
    ));
    STANDARD.encode(pdf)
}

#[tokio::test]
#[ignore = "requires Poppler pdftotext installed"]
async fn a_real_pdf_is_extracted_with_a_bounded_output() -> Result<(), Fault> {
    let pdf = pdf_document("Quarterly revenue");
    let text = extract_pdf(&pdf, 1024).await?;
    assert!(text.contains("Quarterly revenue"), "{text}");
    assert!(extract_pdf(&pdf, 4).await.is_err());
    assert!(
        extract_pdf(&STANDARD.encode("%PDF-malformed"), 1024)
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn completion_hooks_see_conversion_facts_for_each_attempt() -> Result<(), Fault> {
    #[derive(Clone)]
    struct Record(Arc<std::sync::Mutex<Vec<(String, usize)>>>);
    #[async_trait]
    impl Plugin for Record {
        async fn on_provider_end(&self, px: &ProviderContext, _: &Result<(), Fault>) {
            self.0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push((
                    px.provider().to_owned(),
                    px.extensions().get::<Converted>().map_or(0, |c| c.0),
                ));
        }
    }
    let mock = MockTransport::new();
    mock.reply(TEXT, mock::json(StatusCode::SERVICE_UNAVAILABLE, "{}"))
        .reply(PDF, mock::json(StatusCode::OK, OK));
    let records = Record(Arc::default());
    let gw = gateway(&mock, Arc::new(PdfToText::new(&["text-only"]).await?))?
        .plugin("records", records.clone())?;
    assert_eq!(send(&gw, "acme", REQUEST).await, StatusCode::OK);
    gw.idle().await;
    let records = records
        .0
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    assert!(records.contains(&("text-only".into(), 1)));
    assert!(records.contains(&("reads-pdf".into(), 0)));
    Ok(())
}

#[tokio::test]
async fn deletion_fences_a_conversion_already_in_flight() -> Result<(), Fault> {
    static STARTED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
    static RELEASED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
    let pdfs = Arc::new(PdfToText {
        convert: |_| {
            STARTED.store(true, Ordering::SeqCst);
            for _ in 0..2000 {
                if RELEASED.load(Ordering::SeqCst) {
                    return Ok("private text".into());
                }
                std::thread::sleep(Duration::from_millis(1));
            }
            Err(Fault::internal("test release timed out"))
        },
        ..PdfToText::new(&["text-only"]).await?
    });
    let conversion = tokio::spawn({
        let pdfs = pdfs.clone();
        async move {
            pdfs.text(
                "acme",
                "JVBE".into(),
                Instant::now() + Duration::from_secs(5),
                true,
            )
            .await
        }
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        while !STARTED.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .map_err(|_| Fault::internal("conversion did not start"))?;
    let deleted = pdfs.forget("acme").await;
    RELEASED.store(true, Ordering::SeqCst);
    deleted?;
    conversion
        .await
        .map_err(|_| Fault::internal("conversion task"))??;
    let stored = pdfs
        .cache
        .transact(|tx| {
            tx.query_row("SELECT COUNT(*) FROM attachment_text", [], |row| {
                row.get::<_, i64>(0)
            })
            .map_err(sql)
        })
        .await?;
    assert_eq!(
        stored, 0,
        "a late conversion must not resurrect deleted content"
    );
    Ok(())
}
