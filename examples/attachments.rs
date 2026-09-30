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
//! A fallback in another format gets the PDF too: for an Anthropic target the
//! filter rewrites the file part into Anthropic's own `document` part
//! (`Content::other("anthropic-messages", ..)`), which the format sends as it
//! is. A part in neither the target's shape nor the client's is refused, not
//! guessed at.
//!
//! A `file_id` (a file uploaded earlier) is resolved before any call, in
//! `on_client`: the files endpoint is asked as a subrequest
//! (`cx.call("files").with_input(Ask::get(..))`, a format of `rest`), under
//! the tenant's own key, not the gateway's, so a tenant reads only its own
//! files. The file is read within the size limit and the time budget, and
//! replaces its reference in the conversation as an inline part, so it is then
//! converted like any other PDF, in every call. A stored file is taken to be a
//! PDF, as only PDFs are converted here; a real resolver reads the file's type
//! from `GET /v1/files/{id}`.
//!
//! **Partial:** the body limit is the server's, the same for all.
//!
//! `cargo run -p respan-gateway-examples --example attachments`

use std::collections::HashMap;
use std::hash::{BuildHasher, RandomState};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use respan_gateway_core::futures::TryStreamExt;
use respan_gateway_core::{
    ClientContext, Fault, Gateway, Plugin, ProviderContext, Response, async_trait,
};
use respan_gateway_examples::rest::{Ask, Rest};
use respan_gateway_formats::conversation::{Content, Conversation};
use respan_gateway_formats::opaque::Got;
use respan_gateway_formats::{AnthropicMessages, OpenAiChat};
use respan_gateway_http::Status;
use respan_gateway_http::http::{HeaderValue, StatusCode};
use respan_gateway_http::{Asked, CallHeaders, Credential};
use respan_gateway_routing::{Backoff, Until, first_success};
use respan_gateway_testkit::{MockTransport, mock, post, read_whole, serve};
use serde_json::json;
use tokio::time::Instant;

/// How many texts the memo keeps, across tenants.
const MEMO_SIZE: usize = 1024;

/// Stands in for a real extractor: slow, blocking work on untrusted bytes.
fn fake_pdf_to_text(base64: &str) -> Result<String, Fault> {
    std::thread::sleep(Duration::from_millis(20));
    Ok(format!("[text of a {}-byte PDF]", base64.len() * 3 / 4))
}

/// The `file` of `part` if it is a file part.
fn file(part: &Content) -> Option<&serde_json::Value> {
    match part {
        Content::Other { raw, .. } if raw["type"] == "file" => Some(&raw["file"]),
        _ => None,
    }
}

/// The base64 of `part` if it is an inline PDF.
fn pdf(part: &Content) -> Option<&str> {
    let data = file(part)?["file_data"].as_str()?;
    data.strip_prefix("data:application/pdf;base64,")
}

/// The ids of the stored files `chat` refers to.
fn stored(chat: &Conversation) -> Vec<String> {
    let parts = chat.messages.iter().flat_map(|m| &m.content);
    let ids = parts.filter_map(|part| file(part)?["file_id"].as_str());
    ids.map(str::to_owned).collect()
}

fn too_large() -> Fault {
    Fault::new("too_large", "PDF too large").with(Status(413))
}

fn too_slow() -> Fault {
    let slow = "the PDF took too long to read";
    Fault::new("conversion_timeout", slow).with(Status(504))
}

/// The files endpoint: a provider named `files`, whose paths the plugin sets.
const FILES: Rest = Rest::new("files", "/v1/files");

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
        if base64.len() > self.max {
            return Err(too_large());
        }
        let key = (tenant.to_owned(), self.hasher.hash_one(&base64));
        if keep && let Some(text) = self.memo().get(&key) {
            return Ok(text.clone());
        }
        let convert = self.convert;
        let work = tokio::task::spawn_blocking(move || convert(&base64));
        let panicked = |e| Fault::internal("the PDF converter panicked").with_source(e);
        let text = tokio::time::timeout_at(until, work).await;
        let text: Arc<str> = text.map_err(|_| too_slow())?.map_err(panicked)??.into();
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

impl PdfToText {
    /// The stored file `id` as an inline PDF part's data, asked of the files
    /// endpoint under the tenant's key, read within `max` and `until`.
    async fn fetch(
        &self,
        cx: &mut ClientContext,
        id: &str,
        until: Instant,
    ) -> Result<String, Fault> {
        let key = cx.extensions().get::<Credential>();
        let key = key.map(|c| CallHeaders(c.headers().clone()));
        let call = cx
            .call("files")
            .with_input(Ask::get(format!("/v1/files/{id}/content")));
        let call = key
            .into_iter()
            .fold(call, |call, key| call.with_extension(key));
        let read = async {
            let mut bytes = Vec::new();
            let mut chunks = call.await?.outputs::<Got<Rest>>();
            while let Some(chunk) = chunks.try_next().await? {
                bytes.extend_from_slice(&chunk.0);
                if bytes.len() > self.max / 4 * 3 {
                    return Err(too_large());
                }
            }
            Ok(bytes)
        };
        let bytes = tokio::time::timeout_at(until, read).await;
        let bytes = bytes.map_err(|_| too_slow())??;
        Ok(format!(
            "data:application/pdf;base64,{}",
            STANDARD.encode(bytes)
        ))
    }
}

#[async_trait]
impl Plugin for PdfToText {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let deadline = Instant::now() + self.budget;
        cx.extensions_mut().insert(Deadline(deadline));
        let ids = cx.input::<Conversation>().map(stored).unwrap_or_default();
        if ids.is_empty() {
            return Ok(None);
        }
        // Resolved here, in `on_client`, which may call: the provider edge cannot.
        let mut inline = HashMap::new();
        for id in ids {
            let data = self.fetch(cx, &id, deadline).await?;
            inline.insert(id, data);
        }
        cx.filter(move |chat: &mut Conversation| {
            for part in chat.messages.iter_mut().flat_map(|m| &mut m.content) {
                let data = file(part).and_then(|f| f["file_id"].as_str());
                let Some(data) = data.and_then(|id| inline.get(id)) else {
                    continue;
                };
                let file = json!({ "file_data": data });
                *part = Content::other("openai-chat", json!({ "type": "file", "file": file }));
            }
            Ok(())
        })?;
        Ok(None)
    }

    async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
        if px.format() == "anthropic-messages" && px.is_kind::<Conversation>() {
            return px.filter(pdf_as_document);
        }
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
            pdfs.extend(pdf(part).map(str::to_owned));
        }
        // Filters are synchronous: convert here, then swap the parts in.
        let mut texts = Vec::with_capacity(pdfs.len());
        for base64 in pdfs {
            texts.push(self.text(&tenant, base64, until, keep).await?);
        }
        let mut texts = texts.into_iter();
        px.filter(move |chat: &mut Conversation| {
            for part in chat.messages.iter_mut().flat_map(|m| &mut m.content) {
                if pdf(part).is_some()
                    && let Some(text) = texts.next()
                {
                    *part = Content::from(&*text);
                }
            }
            Ok(())
        })
    }
}

/// A PDF part as Anthropic's `document` part, in the call to an Anthropic
/// target only: the part is in the target's own shape, so it is sent as is.
fn pdf_as_document(chat: &mut Conversation) -> Result<(), Fault> {
    for part in chat.messages.iter_mut().flat_map(|m| &mut m.content) {
        if let Some(data) = pdf(part) {
            let source = json!({ "type": "base64", "media_type": "application/pdf", "data": data });
            *part = Content::other(
                "anthropic-messages",
                json!({ "type": "document", "source": source }),
            );
        }
    }
    Ok(())
}

/// The text-only model, then the one that reads PDFs.
struct Fallback;

#[async_trait]
impl Plugin for Fallback {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let calls = ["text-only", "reads-pdf"].map(|p| cx.call(p));
        first_success(
            calls,
            &Backoff::once(),
            Until::content(respan_gateway_formats::conversation::Reply::is_content),
        )
        .await
        .map(Some)
    }
}

const TEXT: &str = "http://text-only.test/v1";
const PDF: &str = "http://reads-pdf.test/v1";
const STORE: &str = "http://files.test";

fn gateway(mock: &MockTransport, pdfs: Arc<PdfToText>) -> Result<Gateway, Fault> {
    Gateway::new()
        .provider::<Conversation, _, _>(
            "text-only",
            OpenAiChat,
            respan_gateway_http::connect(mock.at(TEXT)),
        )?
        .provider::<Conversation, _, _>(
            "reads-pdf",
            OpenAiChat,
            respan_gateway_http::connect(mock.at(PDF)),
        )?
        .provider::<Ask, _, _>("files", FILES, respan_gateway_http::connect(mock.at(STORE)))?
        .client::<Conversation, _, _>(OpenAiChat)?
        .plugin("pdf-to-text", pdfs)?
        .plugin("fallback", Fallback)
}

const REQUEST: &str = r#"{"model":"any","messages":[{"role":"user","content":[{"type":"text","text":"Summarise this."},{"type":"file","file":{"filename":"q3.pdf","file_data":"data:application/pdf;base64,JVBERi0xLjcKJcfsj6IKMSAwIG9iago="}}]}]}"#;
const OK: &str = r#"{"id":"c","choices":[{"index":0,"message":{"role":"assistant","content":"Revenue is up."}}]}"#;

async fn send(gw: &Gateway, tenant: &'static str, body: &str) -> StatusCode {
    let mut req = post("/v1/chat/completions", body.to_owned());
    let headers = req.headers_mut();
    headers.insert("x-tenant", HeaderValue::from_static(tenant));
    headers.insert(
        "authorization",
        HeaderValue::from_static("Bearer tenant-key"),
    );
    read_whole(serve(gw, req).await.0).await.0
}

/// As `gateway`, with the PDF reader an Anthropic model.
fn gateway_claude(mock: &MockTransport, pdfs: Arc<PdfToText>) -> Result<Gateway, Fault> {
    Gateway::new()
        .provider::<Conversation, _, _>(
            "text-only",
            OpenAiChat,
            respan_gateway_http::connect(mock.at(TEXT)),
        )?
        .provider::<Conversation, _, _>(
            "reads-pdf",
            AnthropicMessages,
            respan_gateway_http::connect(mock.at(PDF)),
        )?
        .provider::<Ask, _, _>("files", FILES, respan_gateway_http::connect(mock.at(STORE)))?
        .client::<Conversation, _, _>(OpenAiChat)?
        .plugin("pdf-to-text", pdfs)?
        .plugin("fallback", Fallback)
}

/// An Anthropic model's answer.
const CLAUDE_OK: &str = r#"{"id":"m","type":"message","role":"assistant","model":"c","content":[{"type":"text","text":"Revenue is up."}],"stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":1}}"#;

#[tokio::main]
async fn main() -> Result<(), Fault> {
    // The fallback reads PDFs in OpenAI's format, then in Anthropic's.
    for claude in [false, true] {
        let mock = MockTransport::new();
        mock.reply(TEXT, mock::json(StatusCode::SERVICE_UNAVAILABLE, "{}"));
        mock.reply(
            PDF,
            mock::json(StatusCode::OK, if claude { CLAUDE_OK } else { OK }),
        );
        let pdfs = Arc::new(PdfToText::new(&["text-only"]));
        let gw = if claude {
            gateway_claude(&mock, pdfs)?
        } else {
            gateway(&mock, pdfs)?
        };
        let status = send(&gw, "acme", REQUEST).await;
        for sent in mock.sent() {
            println!("{}\n  {}", sent.uri(), String::from_utf8_lossy(sent.body()));
        }
        println!("client got {status}");
    }
    Ok(())
}

#[cfg(test)]
#[path = "tests/attachments.rs"]
mod tests;
