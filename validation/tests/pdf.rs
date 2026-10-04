//! Real Poppler conversion; requires pdftotext on PATH.
#[path = "../../examples/support/attachments/pdf.rs"]
mod pdf_converter;
#[path = "../../examples/tests/support/pdf.rs"]
mod pdf_fixture;

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use pdf_converter::extract_pdf_with;
use pdf_fixture::pdf_document;
use respan_gateway_sdk::Fault;

#[tokio::test]
#[ignore = "requires Poppler pdftotext installed"]
async fn a_real_pdf_is_extracted_with_a_bounded_output() -> Result<(), Fault> {
    let pdf = pdf_document("Quarterly revenue");
    let text = extract_pdf_with("pdftotext", &pdf, 1024).await?;
    assert!(text.contains("Quarterly revenue"), "{text}");
    assert!(extract_pdf_with("pdftotext", &pdf, 4).await.is_err());
    assert!(
        extract_pdf_with("pdftotext", &STANDARD.encode("%PDF-malformed"), 1024)
            .await
            .is_err()
    );
    Ok(())
}
