//! Bounded Poppler conversion for the attachments example.
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use respan_gateway_core::{ErrorCode, Fault};
use respan_gateway_http::http::StatusCode;
use respan_gateway_http_faults::Status;

/// Poppler runs outside the gateway process and is killed on timeout or
/// cancellation. Input and extracted text are bounded independently.
pub(crate) async fn extract_pdf_with(
    converter: &str,
    encoded: &str,
    limit: usize,
) -> Result<String, Fault> {
    use std::process::Stdio;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let bytes = STANDARD
        .decode(encoded)
        .map_err(|_| Fault::bad_request("invalid PDF base64"))?;
    if !bytes.starts_with(b"%PDF-") {
        return Err(Fault::bad_request("not a PDF"));
    }
    let mut child = tokio::process::Command::new(converter)
        .args(["-enc", "UTF-8", "-", "-"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .map_err(|e| {
            Fault::config("install Poppler pdftotext for PDF conversion").with_source(e)
        })?;
    let mut input = child
        .stdin
        .take()
        .ok_or_else(|| Fault::internal("PDF stdin"))?;
    let output = child
        .stdout
        .take()
        .ok_or_else(|| Fault::internal("PDF stdout"))?;
    let write = async move {
        input
            .write_all(&bytes)
            .await
            .map_err(|e| Fault::bad_request("the PDF could not be read").with_source(e))?;
        input
            .shutdown()
            .await
            .map_err(|e| Fault::bad_request("the PDF could not be read").with_source(e))
    };
    let read = async move {
        let mut text = Vec::new();
        output
            .take(limit as u64 + 1)
            .read_to_end(&mut text)
            .await
            .map_err(|e| Fault::internal("the PDF text could not be read").with_source(e))?;
        if text.len() > limit {
            return Err(too_large());
        }
        String::from_utf8(text).map_err(|_| Fault::bad_request("the PDF text is not UTF-8"))
    };
    let wait = async {
        let status = child
            .wait()
            .await
            .map_err(|e| Fault::internal("PDF process failed").with_source(e))?;
        if status.success() {
            Ok(())
        } else {
            Err(Fault::bad_request("the PDF is malformed or encrypted"))
        }
    };
    let (_, text, _) = tokio::try_join!(write, read, wait)?;
    Ok(text)
}

pub(crate) fn too_large() -> Fault {
    Fault::new("PDF too large")
        .with(ErrorCode("too_large".into()))
        .with(Status(StatusCode::PAYLOAD_TOO_LARGE))
}
