//! Use case 1, converting attachments before the model sees them: a PDF
//! sent as an OpenAI `file` part becomes text, but only in calls to
//! text-only targets. The conversion is a provider-edge filter, so it is per
//! attempt: when the text-only model fails, the fallback that reads PDFs
//! gets the client's original bytes.
//!
//! Parsing is slow work on untrusted bytes: after a size check, it runs on
//! a blocking thread under a time budget for the whole request, all its
//! PDFs and attempts together. A conversion that runs out of time fails the
//! call but keeps its thread until it returns (a thread cannot be killed),
//! so give a real extractor its own limit or run it in a subprocess.
//! Agents resend the same PDF every turn, so the text is remembered per
//! tenant and content hash, here in a bounded map; in production, a shared
//! store keyed by tenant and SHA-256. There is no single-flight: two turns
//! in flight at once each convert the same PDF. A zero-retention request
//! (marked `NoContent`, as in `logging.rs`) is converted but not remembered.
//! Swap `fake_pdf_to_text` for a real extractor.
//!
//! **Partial:** a fallback in another format (OpenAI to Anthropic) cannot
//! take the file part; a `file_id` (a file uploaded earlier) is refused for
//! a text-only target; the body limit is the server's, the same for all.
//!
//! `cargo run -p respan-gateway-examples --example attachments`

use std::collections::HashMap;
use std::hash::{BuildHasher, RandomState};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use respan_gateway_core::{
    ClientContext, Fault, Gateway, Plugin, ProviderContext, Response, async_trait,
};
use respan_gateway_formats::OpenAiChat;
use respan_gateway_formats::conversation::{Content, Conversation};
use respan_gateway_http::Asked;
use respan_gateway_http::http::{HeaderValue, StatusCode};
use respan_gateway_routing::{Backoff, Until, first_success};
use respan_gateway_testkit::{MockTransport, mock, post, read_whole, serve};
use tokio::time::Instant;

/// How many texts the memo keeps, across tenants.
const MEMO_SIZE: usize = 1024;

/// Stands in for a real extractor: slow, blocking work on untrusted bytes.
fn fake_pdf_to_text(base64: &str) -> Result<String, Fault> {
    std::thread::sleep(Duration::from_millis(20));
    Ok(format!("[text of a {}-byte PDF]", base64.len() * 3 / 4))
}

/// The base64 of `part` if it is an inline PDF; `Err` for a stored file.
fn pdf(part: &Content) -> Result<Option<&str>, Fault> {
    let file = match part {
        Content::Other { raw, .. } if raw["type"] == "file" => &raw["file"],
        _ => return Ok(None),
    };
    if file["file_id"].is_string() {
        return Err(Fault::bad_request("a stored file, for a text-only model"));
    }
    let data = file["file_data"].as_str();
    Ok(data.and_then(|d| d.strip_prefix("data:application/pdf;base64,")))
}

/// When the request's conversion budget runs out.
#[derive(Clone, Copy)]
struct Deadline(Instant);

/// Marks a zero-retention request; your tenancy plugin sets it.
#[derive(Clone)]
struct NoContent;

/// Texts by tenant and content hash.
type Memo = HashMap<(String, u64), Arc<str>>;

struct PdfToText {
    text_only: &'static [&'static str],
    convert: fn(&str) -> Result<String, Fault>,
    budget: Duration,
    /// The largest PDF read, in base64 bytes.
    max: usize,
    /// Keyed at random, so no one can craft a PDF whose hash matches another's.
    hasher: RandomState,
    memo: Mutex<Memo>,
    conversions: AtomicUsize,
}

impl PdfToText {
    fn new(text_only: &'static [&'static str]) -> Self {
        Self {
            text_only,
            convert: fake_pdf_to_text,
            budget: Duration::from_secs(10),
            max: 40 << 20,
            hasher: RandomState::new(),
            memo: Mutex::default(),
            conversions: AtomicUsize::new(0),
        }
    }

    fn memo(&self) -> MutexGuard<'_, Memo> {
        self.memo.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The text of `base64`, a PDF of `tenant`'s, remembered or converted;
    /// with `keep` false, converted and not remembered.
    async fn text(
        &self,
        tenant: &str,
        base64: String,
        until: Instant,
        keep: bool,
    ) -> Result<Arc<str>, Fault> {
        let large = Fault::new(StatusCode::PAYLOAD_TOO_LARGE, "too_large", "PDF too large");
        if base64.len() > self.max {
            return Err(large);
        }
        let key = (tenant.to_owned(), self.hasher.hash_one(&base64));
        if keep && let Some(text) = self.memo().get(&key) {
            return Ok(text.clone());
        }
        let convert = self.convert;
        let work = tokio::task::spawn_blocking(move || convert(&base64));
        let slow = "the PDF took too long to read";
        let slow = |_| Fault::new(StatusCode::GATEWAY_TIMEOUT, "conversion_timeout", slow);
        let panicked = |e| Fault::internal("the PDF converter panicked").with_source(e);
        let text = tokio::time::timeout_at(until, work).await;
        let text: Arc<str> = text.map_err(slow)?.map_err(panicked)??.into();
        self.conversions.fetch_add(1, Ordering::Relaxed);
        if !keep {
            return Ok(text);
        }
        let mut memo = self.memo();
        // Full: drop any one. A shared cache would drop the least recently used.
        if memo.len() >= MEMO_SIZE
            && let Some(any) = memo.keys().next().cloned()
        {
            memo.remove(&any);
        }
        memo.insert(key, text.clone());
        Ok(text)
    }
}

#[async_trait]
impl Plugin for PdfToText {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let deadline = Deadline(Instant::now() + self.budget);
        cx.extensions_mut().insert(deadline);
        Ok(None)
    }

    async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
        if !self.text_only.contains(&px.provider()) || !px.is_kind::<Conversation>() {
            return Ok(());
        }
        // Named by `x-tenant` here; in practice, by your auth plugin.
        let ext = px.extensions();
        let until = ext.get::<Deadline>().map(|d| d.0);
        let until = until.unwrap_or_else(|| Instant::now() + self.budget);
        let asked = ext.get::<Asked>();
        let tenant = asked.and_then(|a| a.headers.get("x-tenant")?.to_str().ok());
        let tenant = tenant.unwrap_or_default().to_owned();
        let keep = ext.get::<NoContent>().is_none();
        let chat = px.input::<Conversation>()?;
        let mut pdfs = Vec::new();
        for part in chat.messages.iter().flat_map(|m| &m.content) {
            pdfs.extend(pdf(part)?.map(str::to_owned));
        }
        // Filters are synchronous: convert here, then swap the parts in.
        let mut texts = Vec::with_capacity(pdfs.len());
        for base64 in pdfs {
            texts.push(self.text(&tenant, base64, until, keep).await?);
        }
        let mut texts = texts.into_iter();
        px.filter(move |chat: &mut Conversation| {
            for part in chat.messages.iter_mut().flat_map(|m| &mut m.content) {
                if pdf(part)?.is_some()
                    && let Some(text) = texts.next()
                {
                    *part = Content::from(&*text);
                }
            }
            Ok(())
        })
    }
}

/// The text-only model, then the one that reads PDFs.
struct Fallback;

#[async_trait]
impl Plugin for Fallback {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let calls = ["text-only", "reads-pdf"].map(|p| cx.call(p));
        first_success(calls, &Backoff::once(), Until::Content)
            .await
            .map(Some)
    }
}

const TEXT: &str = "http://text-only.test/v1";
const PDF: &str = "http://reads-pdf.test/v1";

fn gateway(mock: &MockTransport, pdfs: Arc<PdfToText>) -> Result<Gateway, Fault> {
    Gateway::new()
        .provider("text-only", OpenAiChat, mock.at(TEXT))?
        .provider("reads-pdf", OpenAiChat, mock.at(PDF))?
        .serve(OpenAiChat)?
        .plugin("pdf-to-text", pdfs)?
        .plugin("fallback", Fallback)
}

const REQUEST: &str = r#"{"model":"any","messages":[{"role":"user","content":[{"type":"text","text":"Summarise this."},{"type":"file","file":{"filename":"q3.pdf","file_data":"data:application/pdf;base64,JVBERi0xLjcKJcfsj6IKMSAwIG9iago="}}]}]}"#;
const OK: &str = r#"{"id":"c","choices":[{"index":0,"message":{"role":"assistant","content":"Revenue is up."}}]}"#;

async fn send(gw: &Gateway, tenant: &'static str, body: &str) -> StatusCode {
    let mut req = post("/v1/chat/completions", body.to_owned());
    req.headers_mut()
        .insert("x-tenant", HeaderValue::from_static(tenant));
    read_whole(serve(gw, req).await.0).await.0
}

#[tokio::main]
async fn main() -> Result<(), Fault> {
    let mock = MockTransport::new();
    mock.reply(TEXT, mock::json(StatusCode::SERVICE_UNAVAILABLE, "{}"));
    mock.reply(PDF, mock::json(StatusCode::OK, OK));
    let gw = gateway(&mock, Arc::new(PdfToText::new(&["text-only"])))?;
    let status = send(&gw, "acme", REQUEST).await;
    for sent in mock.sent() {
        println!("{}\n  {}", sent.uri(), String::from_utf8_lossy(sent.body()));
    }
    println!("client got {status}");
    Ok(())
}

#[cfg(test)]
#[path = "tests/attachments.rs"]
mod tests;
