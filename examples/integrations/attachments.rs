//! Use case 1, converting attachments before the model sees them: a PDF
//! sent as an OpenAI `file` part becomes text, but only in calls to
//! text-only targets. The conversion is a provider-edge filter, so it is per
//! attempt: when the text-only model fails, the fallback that reads PDFs
//! gets the client's original bytes.
//!
//! Parsing untrusted bytes runs in a Poppler `pdftotext` subprocess, killed
//! on timeout or cancellation. Input and extracted text are bounded, under
//! one budget for the whole request, all its PDFs and attempts together.
//! Install Poppler to run this example; tests can inject a blocking extractor.
//! Agents resend the same PDF every turn, so the text is remembered per
//! tenant and SHA-256 in a bounded SQLite cache. `with_store` opens a shared
//! file; the default is an isolated in-memory database. Two turns
//! in flight at once each convert the same PDF. A zero-retention request
//! (marked `NoContent`, as in `logging.rs`) deletes that tenant's cached
//! content and converts without reading or writing cached text.
//!
//! A fallback in another format gets the original typed document. The
//! provider format writes that document in the target format. Provider-specific
//! file references remain native until the resolver turns them into bytes.
//!
//! A `file_id` (a file uploaded earlier) is resolved before any call, in
//! `on_client`: the files endpoint is asked as a subrequest
//! (`cx.call("files").with_input(Ask::get(..))`, a format of `rest`), under
//! the tenant's own key, not the gateway's, so a tenant reads only its own
//! files. The file is read within the size limit and the time budget, and
//! replaces its reference in the conversation as an inline part, so it is then
//! converted like any other PDF, in every call. The resolver checks metadata
//! from `GET /v1/files/{id}` and the PDF signature before conversion.
//!
//! The HTTP adapter can allow 80 MiB on this route while keeping smaller
//! limits elsewhere: `respan_gateway_http::axum::router_with_limit(gateway,
//! |head| if head.uri.path() == "/v1/chat/completions" { 80 << 20 } else { 1 << 20 })`.
//! The converter allows 72 MiB of base64, enough for a 50 MiB PDF.
//!
//! `cargo run -p respan-gateway-examples --example attachments`

use respan_gateway_core::{BadRequest, ErrorCode, Upstream};
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use mockllm::{Answer, FakeAnthropic, FakeOpenAi};
use respan_gateway_conversation::{Content, Input, Media, MediaSource, Output};
use respan_gateway_conversation_http::anthropic::AnthropicMessages;
use respan_gateway_conversation_http::openai::OpenAiChat;
use respan_gateway_core::futures::TryStreamExt;
use respan_gateway_core::{
    ClientContext, Fault, Gateway, Plugin, ProviderContext, Response, async_trait,
};
use respan_gateway_examples::rest::{Ask, Rest};
use respan_gateway_examples::store::{
    Database,
    rusqlite::{OptionalExtension, params},
    sql,
};
use respan_gateway_http::HttpTransport;
use respan_gateway_http::Status;
use respan_gateway_http::framing::passthrough;
use respan_gateway_http::http::{HeaderValue, StatusCode};
use respan_gateway_http::{CallHeaders, Credential, Wire, retryable};
use respan_gateway_plugins::routing::{Backoff, Until, first_success};
#[allow(dead_code)]
#[path = "../../tests/support/http.rs"]
mod http_support;
use http_support::{post, read_whole, serve};
#[allow(dead_code)]
#[path = "../support/http.rs"]
mod provider_support;
use sha2::{Digest, Sha256};
use tokio::time::Instant;

/// How many texts the memo keeps, across tenants.
const MEMO_SIZE: usize = 1024;

/// Stands in for a real extractor: slow, blocking work on untrusted bytes.
#[cfg(test)]
fn fake_pdf_to_text(base64: &str) -> Result<String, Fault> {
    std::thread::sleep(Duration::from_millis(20));
    Ok(format!("[text of a {}-byte PDF]", base64.len() * 3 / 4))
}

#[path = "../support/attachments/pdf.rs"]
mod pdf_converter;
use pdf_converter::{extract_pdf_with, too_large};

/// The `file` of `part` if it is a file part.
fn file(part: &Content) -> Option<&serde_json::Value> {
    match part {
        Content::Native(respan_gateway_core::NativeValue { value: raw, .. })
            if raw["type"] == "file" =>
        {
            Some(&raw["file"])
        }
        _ => None,
    }
}

/// PDF bytes encoded for the bounded converter.
fn pdf(part: &Content) -> Option<String> {
    match part {
        Content::Document(media) if media.mime_type == "application/pdf" => match &media.source {
            MediaSource::Bytes(bytes) => Some(STANDARD.encode(bytes)),
            _ => None,
        },
        _ => {
            let data = file(part)?["file_data"].as_str()?;
            data.strip_prefix("data:application/pdf;base64,")
                .map(str::to_owned)
        }
    }
}

/// The ids of the stored files `chat` refers to.
fn stored(chat: &Input) -> Vec<String> {
    let parts = chat.messages.iter().flat_map(|m| &m.content);
    let ids = parts.filter_map(|part| file(part)?["file_id"].as_str());
    ids.map(str::to_owned).collect()
}

fn too_slow() -> Fault {
    let slow = "the PDF took too long to read";
    Fault::new(slow)
        .with(ErrorCode("conversion_timeout".into()))
        .with(Status(StatusCode::GATEWAY_TIMEOUT))
}

/// The files endpoint: a provider named `files`, whose paths the plugin sets.
const FILES: Rest = Rest::new("files", "/v1/files");

/// When the request's conversion budget runs out.
#[derive(Clone, Copy)]
struct Deadline(Instant);

/// Completion hooks can record how many attachments this attempt converted.
#[derive(Debug, Clone, Copy)]
pub struct Converted(pub usize);

/// Marks a zero-retention request; your tenancy plugin sets it.
#[derive(Clone)]
struct NoContent;

struct PdfToText {
    text_only: &'static [&'static str],
    /// Tests inject a blocking extractor; normal use runs a killable process.
    #[cfg(test)]
    convert: fn(&str) -> Result<String, Fault>,
    budget: Duration,
    /// The largest PDF read, in base64 bytes.
    max: usize,
    /// The same SQLite file shares cached conversions across gateway processes.
    cache: Database,
    conversions: AtomicUsize,
}

impl PdfToText {
    async fn new(text_only: &'static [&'static str]) -> Result<Self, Fault> {
        Self::with_store(text_only, Database::memory()?).await
    }

    async fn with_store(
        text_only: &'static [&'static str],
        cache: Database,
    ) -> Result<Self, Fault> {
        cache
            .transact(|tx| {
                tx.execute_batch(
                    "CREATE TABLE IF NOT EXISTS attachment_text (
                tenant TEXT NOT NULL, digest BLOB NOT NULL, text TEXT NOT NULL,
                PRIMARY KEY(tenant,digest));
                CREATE TABLE IF NOT EXISTS attachment_epochs (
                    tenant TEXT PRIMARY KEY, generation INTEGER NOT NULL)",
                )
                .map_err(sql)
            })
            .await?;
        Ok(Self {
            text_only,
            #[cfg(test)]
            convert: fake_pdf_to_text,
            budget: Duration::from_secs(10),
            max: 72 << 20,
            cache,
            conversions: AtomicUsize::new(0),
        })
    }

    /// Remove content when a tenant enables zero retention or requests deletion.
    async fn forget(&self, tenant: &str) -> Result<(), Fault> {
        let tenant = tenant.to_owned();
        self.cache
            .transact(move |tx| {
                tx.execute("DELETE FROM attachment_text WHERE tenant=?", [&tenant])
                    .map_err(sql)?;
                tx.execute(
                    "INSERT INTO attachment_epochs VALUES(?,1) ON CONFLICT(tenant)
                DO UPDATE SET generation=generation+1",
                    [&tenant],
                )
                .map_err(sql)?;
                Ok(())
            })
            .await
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
        let tenant = tenant.to_owned();
        let (base64, digest) = tokio::task::spawn_blocking(move || {
            let bytes = STANDARD
                .decode(&base64)
                .map_err(|_| BadRequest::fault("invalid PDF base64"))?;
            Ok::<_, Fault>((base64, Sha256::digest(bytes).to_vec()))
        })
        .await
        .map_err(|e| Fault::internal("PDF hashing failed").with_source(e))??;
        let generation = if keep {
            let (tenant, digest) = (tenant.clone(), digest.clone());
            let (cached, generation) = self
                .cache
                .transact(move |tx| {
                    let generation = tx
                        .query_row(
                            "SELECT generation FROM attachment_epochs WHERE tenant=?",
                            [&tenant],
                            |row| row.get::<_, i64>(0),
                        )
                        .optional()
                        .map_err(sql)?
                        .unwrap_or(0);
                    let cached = tx
                        .query_row(
                            "SELECT text FROM attachment_text WHERE tenant=? AND digest=?",
                            params![tenant, digest],
                            |row| row.get::<_, String>(0),
                        )
                        .optional()
                        .map_err(sql)?;
                    Ok((cached, generation))
                })
                .await?;
            if let Some(text) = cached {
                return Ok(text.into());
            }
            generation
        } else {
            0
        };
        let work = async {
            #[cfg(test)]
            {
                let convert = self.convert;
                tokio::task::spawn_blocking(move || convert(&base64))
                    .await
                    .map_err(|e| Fault::internal("the PDF converter panicked").with_source(e))?
            }
            #[cfg(not(test))]
            {
                extract_pdf_with("pdftotext", &base64, self.max).await
            }
        };
        let text: Arc<str> = tokio::time::timeout_at(until, work)
            .await
            .map_err(|_| too_slow())??
            .into();
        self.conversions.fetch_add(1, Ordering::Relaxed);
        if !keep {
            return Ok(text);
        }
        let stored = text.to_string();
        self.cache
            .transact(move |tx| {
                tx.execute(
                    "INSERT OR REPLACE INTO attachment_text SELECT ?,?,?
                WHERE COALESCE((SELECT generation FROM attachment_epochs WHERE tenant=?),0)=?",
                    params![tenant, digest, stored, tenant, generation],
                )
                .map_err(sql)?;
                tx.execute(
                    "DELETE FROM attachment_text WHERE rowid NOT IN
                (SELECT rowid FROM attachment_text ORDER BY rowid DESC LIMIT ?)",
                    [MEMO_SIZE as i64],
                )
                .map_err(sql)?;
                Ok(())
            })
            .await?;
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
        if id.is_empty()
            || !id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"_-".contains(&b))
        {
            return Err(BadRequest::fault("invalid stored file id"));
        }
        let key = cx
            .extensions()
            .get::<Credential>()
            .map(|c| CallHeaders(c.headers().clone()));
        let mut read = |path: String, limit: usize| {
            let call = cx.call("files").with_input(Ask::get(path));
            let call = key
                .clone()
                .into_iter()
                .fold(call, |call, key| call.with_extension(key));
            async move {
                let mut bytes = Vec::new();
                let mut chunks = call.await?.outputs::<passthrough::Output<Rest>>();
                while let Some(chunk) = chunks.try_next().await? {
                    if chunk.0.body().len() > limit.saturating_sub(bytes.len()) {
                        return Err(too_large());
                    }
                    bytes.extend_from_slice(chunk.0.body());
                }
                Ok(bytes)
            }
        };
        let fetch = async {
            let metadata = read(format!("/v1/files/{id}"), 16 * 1024).await?;
            let metadata: serde_json::Value = serde_json::from_slice(&metadata)
                .map_err(|_| Upstream::fault("invalid stored file metadata"))?;
            let pdf = metadata["mime_type"].as_str().map_or_else(
                || {
                    metadata["filename"]
                        .as_str()
                        .is_some_and(|name| name.to_ascii_lowercase().ends_with(".pdf"))
                },
                |mime| mime == "application/pdf",
            );
            if !pdf {
                return Err(Fault::new("only PDF attachments are supported")
                    .with(ErrorCode("unsupported_file".into()))
                    .with(Status(StatusCode::UNSUPPORTED_MEDIA_TYPE)));
            }
            let bytes = read(format!("/v1/files/{id}/content"), self.max / 4 * 3).await?;
            if !bytes.starts_with(b"%PDF-") {
                return Err(Fault::new("the stored file is not a PDF")
                    .with(ErrorCode("unsupported_file".into()))
                    .with(Status(StatusCode::UNSUPPORTED_MEDIA_TYPE)));
            }
            Ok(bytes)
        };
        let bytes = tokio::time::timeout_at(until, fetch)
            .await
            .map_err(|_| too_slow())??;
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
        if cx.extensions().get::<NoContent>().is_some() {
            let tenant = cx
                .extensions()
                .get::<Wire>()
                .and_then(Wire::get)
                .and_then(|asked| asked.headers().get("x-tenant")?.to_str().ok())
                .unwrap_or_default();
            tokio::time::timeout_at(deadline, self.forget(tenant))
                .await
                .map_err(|_| too_slow())??;
        }
        cx.extensions_mut().insert(Deadline(deadline));
        let ids = cx.input::<Input>().map(stored).unwrap_or_default();
        if ids.is_empty() {
            return Ok(None);
        }
        // Resolve once before provider attempts so every fallback sees the same file.
        let mut inline = HashMap::new();
        for id in ids {
            let data = self.fetch(cx, &id, deadline).await?;
            inline.insert(id, data);
        }
        cx.filter(move |chat: &mut Input| {
            for part in chat.messages.iter_mut().flat_map(|m| &mut m.content) {
                let data = file(part).and_then(|f| f["file_id"].as_str());
                let Some(data) = data.and_then(|id| inline.get(id)) else {
                    continue;
                };
                let encoded = data
                    .strip_prefix("data:application/pdf;base64,")
                    .ok_or_else(|| BadRequest::fault("invalid inline PDF"))?;
                let bytes = STANDARD
                    .decode(encoded)
                    .map_err(|_| BadRequest::fault("invalid PDF base64"))?;
                *part = Content::Document(Media::new(
                    "application/pdf",
                    MediaSource::Bytes(bytes.into()),
                ));
            }
            Ok(())
        })?;
        Ok(None)
    }

    async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
        if !self.text_only.contains(&px.provider()) || !px.is_kind::<Input>() {
            return Ok(());
        }
        // Named by `x-tenant` here; in practice, by your auth plugin.
        let ext = px.extensions();
        let until = ext.get::<Deadline>().map(|d| d.0);
        let until = until.unwrap_or_else(|| Instant::now() + self.budget);
        let asked = ext.get::<Wire>().and_then(Wire::get);
        let tenant = asked.and_then(|a| a.headers().get("x-tenant")?.to_str().ok());
        let tenant = tenant.unwrap_or_default().to_owned();
        let keep = ext.get::<NoContent>().is_none();
        let chat = px.input::<Input>()?;
        let mut pdfs = Vec::new();
        for part in chat.messages.iter().flat_map(|m| &m.content) {
            pdfs.extend(pdf(part));
        }
        // Filters are synchronous: convert here, then swap the parts in.
        let mut texts = Vec::with_capacity(pdfs.len());
        for base64 in pdfs {
            texts.push(
                tokio::time::timeout_at(until, self.text(&tenant, base64, until, keep))
                    .await
                    .map_err(|_| too_slow())??,
            );
        }
        px.extensions_mut().insert(Converted(texts.len()));
        let mut texts = texts.into_iter();
        px.filter(move |chat: &mut Input| {
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

/// The text-only model, then the one that reads PDFs.
struct Fallback;

#[async_trait]
impl Plugin for Fallback {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let calls = ["text-only", "reads-pdf"].map(|p| cx.call(p));
        first_success(
            calls,
            &Backoff::once(),
            Until::content(Output::is_content),
            retryable,
        )
        .await
        .map(Some)
    }
}

/// An endpoint-specific file store for metadata and raw content. The fake
/// model providers do not expose arbitrary stored file metadata, so this
/// small real HTTP server supplies just the two GETs this example uses.
type FileReplies = Arc<Mutex<VecDeque<(StatusCode, Vec<u8>)>>>;

struct FileServer {
    url: String,
    #[allow(dead_code)]
    answers: FileReplies,
    #[allow(dead_code)]
    requests: Arc<Mutex<Vec<(String, String)>>>,
    task: tokio::task::JoinHandle<()>,
}

impl FileServer {
    async fn start() -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind file store");
        let url = format!(
            "http://{}",
            listener.local_addr().expect("file store address")
        );
        let answers = Arc::new(Mutex::new(VecDeque::new()));
        let requests = Arc::new(Mutex::new(Vec::new()));
        let (queue, seen) = (answers.clone(), requests.clone());
        let handler = move |req: respan_gateway_http::http::Request<axum::body::Body>| {
            let (queue, seen) = (queue.clone(), seen.clone());
            async move {
                let path = req.uri().path().to_owned();
                let auth = req
                    .headers()
                    .get("authorization")
                    .and_then(|value| value.to_str().ok())
                    .unwrap_or_default()
                    .to_owned();
                seen.lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .push((path, auth));
                let (status, body) = queue
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .pop_front()
                    .unwrap_or((
                        StatusCode::INTERNAL_SERVER_ERROR,
                        b"unscripted file request".to_vec(),
                    ));
                (status, [("content-type", "application/json")], body)
            }
        };
        let app = axum::Router::new()
            .route("/v1/files/{id}", axum::routing::get(handler.clone()))
            .route("/v1/files/{id}/content", axum::routing::get(handler));
        let task = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        Self {
            url,
            answers,
            requests,
            task,
        }
    }

    #[cfg(test)]
    fn answer(&self, status: StatusCode, body: impl Into<Vec<u8>>) {
        self.answers
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push_back((status, body.into()));
    }

    #[cfg(test)]
    fn requests(&self) -> Vec<(String, String)> {
        self.requests
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

impl Drop for FileServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

struct Fixture {
    text: FakeOpenAi,
    pdf: FakeOpenAi,
    claude: FakeAnthropic,
    files: FileServer,
}

impl Fixture {
    async fn start() -> Self {
        Self {
            text: FakeOpenAi::start().await,
            pdf: FakeOpenAi::start().await,
            claude: FakeAnthropic::start().await,
            files: FileServer::start().await,
        }
    }
}

fn gateway(fixture: &Fixture, pdfs: Arc<PdfToText>) -> Result<Gateway, Fault> {
    Ok(Gateway::new()
        .provider::<Input, _, _>(
            "text-only",
            OpenAiChat,
            provider_support::openai(&fixture.text.base_url())?.connect(),
        )?
        .provider::<Input, _, _>(
            "reads-pdf",
            OpenAiChat,
            provider_support::openai(&fixture.pdf.base_url())?.connect(),
        )?
        .provider::<Ask, _, _>(
            "files",
            FILES,
            HttpTransport::new(&fixture.files.url)?.connect(),
        )?
        .client::<Input, _, _>(OpenAiChat)?
        .plugin(pdfs)
        .plugin(Fallback))
}

const REQUEST: &str = r#"{"model":"gpt-4o-mini","messages":[{"role":"user","content":[{"type":"text","text":"Summarise this."},{"type":"file","file":{"filename":"q3.pdf","file_data":"data:application/pdf;base64,JVBERi0xLjcKJcfsj6IKMSAwIG9iago="}}]}]}"#;
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
fn gateway_claude(fixture: &Fixture, pdfs: Arc<PdfToText>) -> Result<Gateway, Fault> {
    Ok(Gateway::new()
        .provider::<Input, _, _>(
            "text-only",
            OpenAiChat,
            provider_support::openai(&fixture.text.base_url())?.connect(),
        )?
        .provider::<Input, _, _>(
            "reads-pdf",
            AnthropicMessages,
            provider_support::anthropic(&fixture.claude.base_url())?.connect(),
        )?
        .provider::<Ask, _, _>(
            "files",
            FILES,
            HttpTransport::new(&fixture.files.url)?.connect(),
        )?
        .client::<Input, _, _>(OpenAiChat)?
        .plugin(pdfs)
        .plugin(Fallback))
}

/// An Anthropic model's answer.
const CLAUDE_OK: &str = r#"{"id":"m","type":"message","role":"assistant","model":"c","content":[{"type":"text","text":"Revenue is up."}],"stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":1}}"#;

#[tokio::main]
async fn main() -> Result<(), Fault> {
    // The fallback reads PDFs in OpenAI's format, then in Anthropic's.
    for claude in [false, true] {
        let fixture = Fixture::start().await;
        fixture
            .text
            .answer(Answer::error(503).message("unavailable"));
        if claude {
            fixture.claude.answer(Answer::raw(CLAUDE_OK));
        } else {
            fixture.pdf.answer(Answer::raw(OK));
        }
        let pdfs = Arc::new(PdfToText::new(&["text-only"]).await?);
        let gw = if claude {
            gateway_claude(&fixture, pdfs)?
        } else {
            gateway(&fixture, pdfs)?
        };
        let request = REQUEST.replace(
            "JVBERi0xLjcKJcfsj6IKMSAwIG9iago=",
            include_str!("../fixtures/report.pdf.b64"),
        );
        let status = send(&gw, "acme", &request).await;
        for sent in fixture
            .text
            .requests()
            .into_iter()
            .chain(fixture.pdf.requests())
        {
            println!("{}\n  {}", sent.path, String::from_utf8_lossy(&sent.raw));
        }
        for sent in fixture.claude.requests() {
            println!("{}\n  {}", sent.path, String::from_utf8_lossy(&sent.raw));
        }
        println!("client got {status}");
    }
    Ok(())
}

#[cfg(test)]
#[path = "../tests/cases/integrations/attachments.rs"]
mod tests;
