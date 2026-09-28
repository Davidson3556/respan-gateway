//! PDFs become text on the provider edge, only for targets that need it.

use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use base64::Engine;
use respan_gateway_core::serde_json::Value;
use respan_gateway_core::{Fault, Plugin, ProviderContext, async_trait};
use respan_gateway_formats::conversation::{Content, Conversation};

/// Turns PDF bytes into text. It may be slow; it runs on a blocking thread.
pub type Extract = Arc<dyn Fn(&[u8]) -> Result<String, Fault> + Send + Sync>;

/// Replaces each inline PDF part with its text in calls to the providers
/// named as text-only; every other call, a fallback's included, gets the
/// original parts. Extraction runs off the async threads and is cached by
/// content hash across requests, as agents resend the same file each turn.
///
/// Reads OpenAI `file` parts (`file_data` as a data URL) and Anthropic
/// `document` parts (a base64 source). A `file_id` cannot be resolved: the
/// core has no stored-file lookup yet, so such a call is refused rather
/// than sent a PDF the target cannot read.
pub struct PdfToText {
    text_only: HashSet<String>,
    extract: Extract,
    cache: Mutex<HashMap<u64, Arc<str>>>,
    extractions: AtomicUsize,
}

impl std::fmt::Debug for PdfToText {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PdfToText")
            .field("text_only", &self.text_only)
            .finish_non_exhaustive()
    }
}

impl PdfToText {
    /// Convert PDFs for calls to `text_only` providers with `extract`.
    pub fn new<S: Into<String>>(text_only: impl IntoIterator<Item = S>, extract: Extract) -> Self {
        Self {
            text_only: text_only.into_iter().map(Into::into).collect(),
            extract,
            cache: Mutex::default(),
            extractions: AtomicUsize::new(0),
        }
    }

    /// How many PDFs were extracted rather than found in the cache.
    pub fn extractions(&self) -> usize {
        self.extractions.load(Ordering::SeqCst)
    }

    async fn text(&self, pdf: Vec<u8>) -> Result<Arc<str>, Fault> {
        let mut h = DefaultHasher::new();
        pdf.hash(&mut h);
        let key = h.finish();
        if let Some(text) = self
            .cache
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&key)
        {
            return Ok(text.clone());
        }
        let extract = self.extract.clone();
        let text = tokio::task::spawn_blocking(move || extract(&pdf))
            .await
            .map_err(|e| Fault::internal("PDF extraction panicked").with_source(e))??;
        self.extractions.fetch_add(1, Ordering::SeqCst);
        let text: Arc<str> = text.into();
        let mut cache = self.cache.lock().unwrap_or_else(PoisonError::into_inner);
        cache.insert(key, text.clone());
        Ok(text)
    }
}

/// What a content part holds, if it is a PDF.
enum Pdf {
    Inline { name: String, bytes: Vec<u8> },
    Stored(String),
}

fn pdf(raw: &Value) -> Option<Result<Pdf, Fault>> {
    let bad = || Fault::bad_request("a PDF part's data is not valid base64");
    let decode = |b64: &str| {
        base64::engine::general_purpose::STANDARD
            .decode(b64)
            .map_err(|_| bad())
    };
    match raw["type"].as_str()? {
        "file" => {
            let file = &raw["file"];
            if let Some(id) = file["file_id"].as_str() {
                return Some(Ok(Pdf::Stored(id.to_owned())));
            }
            let data = file["file_data"].as_str()?;
            let b64 = data.strip_prefix("data:application/pdf;base64,")?;
            let name = file["filename"]
                .as_str()
                .unwrap_or("document.pdf")
                .to_owned();
            Some(decode(b64).map(|bytes| Pdf::Inline { name, bytes }))
        }
        "document" if raw["source"]["media_type"] == "application/pdf" => {
            let b64 = raw["source"]["data"].as_str()?;
            let name = raw["title"].as_str().unwrap_or("document.pdf").to_owned();
            Some(decode(b64).map(|bytes| Pdf::Inline { name, bytes }))
        }
        _ => None,
    }
}

/// The PDFs of `chat`'s content parts, in order.
fn pdfs(chat: &Conversation) -> impl Iterator<Item = Result<Pdf, Fault>> + '_ {
    let parts = chat.messages.iter().flat_map(|m| &m.content);
    parts.filter_map(|c| match c {
        Content::Other { raw, .. } => pdf(raw),
        _ => None,
    })
}

#[async_trait]
impl Plugin for PdfToText {
    async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
        if !self.text_only.contains(px.provider()) || !px.is_kind::<Conversation>() {
            return Ok(());
        }
        // Filters are synchronous, so extract here, then swap the texts in
        // with a filter, which finds the PDF parts again in the same order.
        let found: Vec<Pdf> = pdfs(px.input::<Conversation>()?).collect::<Result<_, _>>()?;
        let mut texts = Vec::with_capacity(found.len());
        for pdf in found {
            let (name, bytes) = match pdf {
                Pdf::Inline { name, bytes } => (name, bytes),
                Pdf::Stored(id) => {
                    return Err(Fault::bad_request(format!(
                        "file '{id}' cannot be read for a text-only model"
                    )));
                }
            };
            texts.push(format!("[{name}]\n{}", self.text(bytes).await?));
        }
        if texts.is_empty() {
            return Ok(());
        }
        let mut texts = texts.into_iter();
        px.filter(move |chat: &mut Conversation| {
            for part in chat.messages.iter_mut().flat_map(|m| &mut m.content) {
                if matches!(part, Content::Other { raw, .. } if pdf(raw).is_some())
                    && let Some(text) = texts.next()
                {
                    *part = Content::from(text);
                }
            }
            Ok(())
        })
    }
}
