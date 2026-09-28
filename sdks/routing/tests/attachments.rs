//! Per-target attachment conversion on the provider edge (use case 1).

mod common;

use std::sync::Arc;

use base64::Engine;
use common::*;
use respan_gateway_core::http::StatusCode;
use respan_gateway_core::serde_json::{Value, json};
use respan_gateway_core::{Fault, Gateway};
use respan_gateway_formats::{AnthropicMessages, OpenAiChat};
use respan_gateway_routing::attachments::PdfToText;
use respan_gateway_routing::fallback::Fallback;
use respan_gateway_routing::retry::Backoff;
use respan_gateway_testkit::MockTransport;

/// A fake extractor: the "PDF" is its text.
fn extractor() -> respan_gateway_routing::attachments::Extract {
    Arc::new(|pdf: &[u8]| {
        String::from_utf8(pdf.to_vec()).map_err(|_| Fault::bad_request("not a PDF"))
    })
}

fn file_part(text: &str) -> Value {
    let b64 = base64::engine::general_purpose::STANDARD.encode(text);
    json!({"type": "file", "file": {"filename": "report.pdf",
        "file_data": format!("data:application/pdf;base64,{b64}")}})
}

fn with_pdf(text: &str) -> respan_gateway_core::http::Request<respan_gateway_core::Body> {
    chat_with(
        json!({"model": "gpt-x", "messages": [{"role": "user", "content": [
        {"type": "text", "text": "summarise"}, file_part(text)]}]}),
    )
}

fn gateway(mock: &MockTransport, pdf: Arc<PdfToText>, fallback_to: &str) -> Gateway {
    Gateway::new()
        .provider("text-only", OpenAiChat, mock.at(TEXT_ONLY))
        .and_then(|g| g.provider("backup", OpenAiChat, mock.at(BACKUP)))
        .and_then(|g| g.provider("anthropic", AnthropicMessages, mock.at(ANTHROPIC)))
        .and_then(|g| g.serve(OpenAiChat))
        .and_then(|g| {
            g.plugin("pdf", pdf)?.plugin(
                "fallback",
                Fallback::new(Backoff::once()).then(fallback_to, Some("m")),
            )
        })
        .and_then(|g| g.target("text-only"))
        .expect("gateway")
}

#[tokio::test]
async fn only_the_text_only_target_gets_text_and_the_fallback_gets_the_pdf() {
    let mock = MockTransport::new();
    let pdf = Arc::new(PdfToText::new(["text-only"], extractor()));
    let gw = gateway(&mock, pdf.clone(), "backup");
    mock.reply(TEXT_ONLY, json_reply(StatusCode::SERVICE_UNAVAILABLE, "{}"))
        .reply(BACKUP, json_reply(StatusCode::OK, &openai_json("summary")));

    let (status, _, _) = send(&gw, with_pdf("Q3 revenue rose")).await;

    assert_eq!(status, StatusCode::OK);
    let sent = mock.sent();
    let converted = sent_json(&sent[0]);
    let parts = &converted["messages"][0]["content"];
    assert_eq!(parts[0]["text"], "summarise");
    assert_eq!(
        parts[1],
        json!({"type": "text", "text": "[report.pdf]\nQ3 revenue rose"})
    );
    // The fallback reads PDFs: it gets the client's part, byte for byte.
    let original = sent_json(&sent[1]);
    assert_eq!(
        original["messages"][0]["content"][1],
        file_part("Q3 revenue rose")
    );
}

#[tokio::test]
async fn extraction_is_cached_across_requests() {
    let mock = MockTransport::new();
    let pdf = Arc::new(PdfToText::new(["text-only"], extractor()));
    let gw = gateway(&mock, pdf.clone(), "backup");
    for _ in 0..3 {
        mock.reply(TEXT_ONLY, json_reply(StatusCode::OK, &openai_json("ok")));
        let (status, _, _) = send(&gw, with_pdf("same file")).await;
        assert_eq!(status, StatusCode::OK);
    }
    assert_eq!(pdf.extractions(), 1);
}

#[tokio::test]
async fn a_request_without_a_pdf_is_relayed_untouched() {
    let mock = MockTransport::new();
    let pdf = Arc::new(PdfToText::new(["text-only"], extractor()));
    let gw = gateway(&mock, pdf, "backup");
    mock.reply(TEXT_ONLY, json_reply(StatusCode::OK, &openai_json("ok")));
    let body = r#"{"model":"gpt-x",  "messages":[{"role":"user","content":"hi"}]}"#;

    let _ = send(&gw, respan_gateway_testkit::post(CHAT, body)).await;

    // Unchanged input: the client's bytes, apart from the model write.
    let sent = String::from_utf8_lossy(mock.sent()[0].body()).into_owned();
    assert!(
        sent.contains("\"messages\":[{\"role\":\"user\",\"content\":\"hi\"}]"),
        "{sent}"
    );
}

#[tokio::test]
async fn a_stored_file_is_refused_for_a_text_only_target() {
    let mock = MockTransport::new();
    let pdf = Arc::new(PdfToText::new(["text-only"], extractor()));
    let gw = gateway(&mock, pdf, "backup");
    mock.reply(BACKUP, json_reply(StatusCode::OK, &openai_json("read it")));
    let req = chat_with(
        json!({"model": "gpt-x", "messages": [{"role": "user", "content": [
        {"type": "file", "file": {"file_id": "file-abc"}}]}]}),
    );

    let (status, _, body) = send(&gw, req).await;

    // The text-only call is refused by the hook, a client error, which the
    // fallback passes on rather than masking.
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body:?}");
    assert!(mock.sent().is_empty());
}

/// F10 (formats): a PDF cannot reach a fallback of another format. The
/// conversation kind holds a file part as `Content::Other`, in the
/// client's shape and with no format tag, and translation refuses it; no
/// plugin can hand Anthropic a `document` part either, since an `Other`
/// is refused across formats whatever it holds.
#[tokio::test]
#[ignore = "F10: Content::Other has no format tag, so a converted part cannot cross formats"]
async fn a_pdf_reaches_an_anthropic_fallback_as_a_document() {
    let mock = MockTransport::new();
    let pdf = Arc::new(PdfToText::new(["text-only"], extractor()));
    let gw = gateway(&mock, pdf, "anthropic");
    mock.reply(TEXT_ONLY, json_reply(StatusCode::SERVICE_UNAVAILABLE, "{}"))
        .reply(
            ANTHROPIC,
            json_reply(StatusCode::OK, &anthropic_json("summary")),
        );

    let (status, _, _) = send(&gw, with_pdf("Q3")).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        sent_json(&mock.sent()[1])["messages"][0]["content"][1]["type"],
        "document"
    );
}

/// What F10 does today: the fallback call is refused before it is sent,
/// and the client gets that refusal, not the text-only provider's 503: a
/// 400, since the client's own file part cannot be translated either
/// (V9-1; it was a `config` 500 under R8-A).
#[tokio::test]
async fn today_a_pdf_cannot_fall_back_to_another_format() {
    let mock = MockTransport::new();
    let pdf = Arc::new(PdfToText::new(["text-only"], extractor()));
    let gw = gateway(&mock, pdf, "anthropic");
    mock.reply(TEXT_ONLY, json_reply(StatusCode::SERVICE_UNAVAILABLE, "{}"));

    let (status, _, body) = send(&gw, with_pdf("Q3")).await;

    assert_eq!(status, StatusCode::BAD_REQUEST, "{body:?}");
    assert_eq!(mock.sent().len(), 1);
}
