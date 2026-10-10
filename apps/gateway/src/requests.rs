//! The request log: each request through the gateway, as the admin API and
//! stderr show it. The HTTP edge ([`track`]) sees its head, status, timing
//! and end; a plugin ([`Recorder`]) sees its format, model, provider and
//! usage.

use std::collections::{HashMap, VecDeque};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::{Context, Poll};
use std::time::{Instant, SystemTime};

use axum::body::Body;
use axum::extract::{Request, State};
use axum::middleware::Next;
use axum::response::Response;
use respan_gateway_conversation as conversation;
use respan_gateway_core::{ClientContext, Extensions, Fault, Plugin, ProviderContext, async_trait};
use respan_gateway_http::bytes::Bytes;
use respan_gateway_http::http::header::CONTENT_TYPE;
use respan_gateway_http::http_body::{self, Frame, SizeHint};
use respan_gateway_media::{image, speech, transcription};
use serde::Serialize;
use tokio::sync::broadcast;

use crate::formats::{Kind, KnownFormat, ReadModel};

/// How many finished requests are kept.
const KEPT: usize = 1000;
/// How much of an error body is read for its message.
const ERROR_BODY: usize = 4096;
/// The most characters of a client's path or model kept.
const KEPT_CHARS: usize = 200;

/// One request, as the admin API returns it.
#[derive(Debug, Clone, Serialize)]
pub struct RequestRecord {
    pub id: String,
    pub started_at: String,
    pub method: String,
    pub path: String,
    pub format: Option<String>,
    pub kind: Option<&'static str>,
    pub model: Option<String>,
    pub provider: Option<String>,
    pub status: Option<u16>,
    pub duration_ms: Option<u64>,
    pub ttfb_ms: Option<u64>,
    pub stream: bool,
    pub usage: Option<Usage>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Copy, Default, Serialize)]
pub struct Usage {
    pub input_tokens: u64,
    pub output_tokens: u64,
    /// Seconds of audio billed, as a transcription's provider may say
    /// instead of tokens.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub seconds: Option<f64>,
}

/// What one event of a response adds to its usage.
enum Counted {
    Tokens(u64, u64),
    Seconds(f64),
}

impl Counted {
    fn tokens(usage: &respan_gateway_conversation::Usage) -> Self {
        Counted::Tokens(usage.input_tokens, usage.output_tokens)
    }
}

impl Usage {
    fn add(&mut self, counted: Counted) {
        match counted {
            Counted::Tokens(input, output) => {
                self.input_tokens += input;
                self.output_tokens += output;
            }
            Counted::Seconds(seconds) => *self.seconds.get_or_insert(0.0) += seconds,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, Serialize)]
pub struct Totals {
    pub requests: u64,
    pub errors: u64,
    pub in_flight: u64,
    pub input_tokens: u64,
    pub output_tokens: u64,
}

/// A provider's finished requests.
#[derive(Debug, Clone, Copy, Default)]
pub struct ProviderStats {
    pub requests: u64,
    pub errors: u64,
    total_ms: u64,
}

impl ProviderStats {
    pub fn avg_latency_ms(&self) -> Option<u64> {
        (self.requests > 0).then(|| self.total_ms / self.requests)
    }
}

/// A request in the log, shared by the edge and the plugin.
#[derive(Debug)]
pub struct Entry {
    record: Mutex<RequestRecord>,
    started: Instant,
    finished: std::sync::atomic::AtomicBool,
}

impl Entry {
    fn record(&self) -> MutexGuard<'_, RequestRecord> {
        self.record.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub fn snapshot(&self) -> RequestRecord {
        self.record().clone()
    }

    fn elapsed_ms(&self) -> u64 {
        self.started
            .elapsed()
            .as_millis()
            .try_into()
            .unwrap_or(u64::MAX)
    }
}

/// The request's entry, in its extensions.
#[derive(Debug, Clone)]
struct Tracked(Arc<Entry>);

#[derive(Debug, Default)]
struct Kept {
    recent: VecDeque<Arc<Entry>>,
    totals: Totals,
    providers: HashMap<String, ProviderStats>,
}

#[derive(Debug)]
pub struct RequestLog {
    next_id: AtomicU64,
    kept: Mutex<Kept>,
    events: broadcast::Sender<Arc<str>>,
    print: bool,
}

impl RequestLog {
    pub fn new(print: bool) -> Arc<Self> {
        Arc::new(Self {
            next_id: AtomicU64::new(1),
            kept: Mutex::default(),
            events: broadcast::channel(256).0,
            print,
        })
    }

    fn kept(&self) -> MutexGuard<'_, Kept> {
        self.kept.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// The last `limit` requests, newest first.
    pub fn recent(&self, limit: usize) -> Vec<RequestRecord> {
        let kept = self.kept();
        kept.recent
            .iter()
            .rev()
            .take(limit)
            .map(|e| e.snapshot())
            .collect()
    }

    pub fn get(&self, id: &str) -> Option<RequestRecord> {
        let kept = self.kept();
        let entry = kept.recent.iter().find(|e| e.record().id == id);
        entry.map(|e| e.snapshot())
    }

    pub fn totals(&self) -> Totals {
        self.kept().totals
    }

    pub fn provider_stats(&self, provider: &str) -> ProviderStats {
        self.kept()
            .providers
            .get(provider)
            .copied()
            .unwrap_or_default()
    }

    /// Each request as it starts, changes and finishes, as JSON.
    pub fn subscribe(&self) -> broadcast::Receiver<Arc<str>> {
        self.events.subscribe()
    }

    fn publish(&self, entry: &Entry) {
        if let Ok(json) = serde_json::to_string(&*entry.record()) {
            let _ = self.events.send(json.into());
        }
    }

    fn start(&self, req: &Request) -> Arc<Entry> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let query_model = req.uri().query().and_then(|q| {
            let pairs = q.split('&').filter_map(|pair| pair.split_once('='));
            pairs
                .into_iter()
                .find(|(name, _)| *name == "model")
                .map(|(_, m)| clip(m))
        });
        let record = RequestRecord {
            id: format!("r_{id:06}"),
            started_at: rfc3339(SystemTime::now()),
            method: req.method().to_string(),
            path: clip(req.uri().path()),
            format: None,
            kind: None,
            model: query_model,
            provider: None,
            status: None,
            duration_ms: None,
            ttfb_ms: None,
            stream: false,
            usage: None,
            error: None,
        };
        let entry = Arc::new(Entry {
            record: Mutex::new(record),
            started: Instant::now(),
            finished: Default::default(),
        });
        {
            let mut kept = self.kept();
            kept.recent.push_back(entry.clone());
            if kept.recent.len() > KEPT {
                kept.recent.pop_front();
            }
            kept.totals.requests += 1;
            kept.totals.in_flight += 1;
        }
        self.publish(&entry);
        entry
    }

    fn finish(&self, entry: &Entry, error: Option<String>) {
        if entry.finished.swap(true, Ordering::SeqCst) {
            return;
        }
        let record = {
            let mut record = entry.record();
            record.duration_ms = Some(entry.elapsed_ms());
            if record.error.is_none() {
                record.error = error;
            }
            record.clone()
        };
        let failed = record.error.is_some() || record.status.is_some_and(|s| s >= 400);
        {
            let mut kept = self.kept();
            let totals = &mut kept.totals;
            totals.in_flight = totals.in_flight.saturating_sub(1);
            totals.errors += u64::from(failed);
            if let Some(usage) = record.usage {
                totals.input_tokens += usage.input_tokens;
                totals.output_tokens += usage.output_tokens;
            }
            if let Some(provider) = &record.provider {
                let stats = kept.providers.entry(provider.clone()).or_default();
                stats.requests += 1;
                stats.errors += u64::from(failed);
                stats.total_ms += record.duration_ms.unwrap_or_default();
            }
        }
        self.publish(entry);
        if self.print {
            eprintln!("{}", escape_controls(&line(&record)));
        }
    }
}

/// One finished request, in a line: `r_000001 POST /v1/chat/completions
/// 200 812ms gpt-4o-mini -> openai (ttfb 230ms, 12 in / 40 out tokens)`.
fn line(r: &RequestRecord) -> String {
    let status = r.status.map_or("-".to_owned(), |s| s.to_string());
    let mut line = format!(
        "{} {} {} {status} {}ms",
        r.id,
        r.method,
        r.path,
        r.duration_ms.unwrap_or_default()
    );
    if let Some(model) = &r.model {
        line.push_str(&format!(" {model}"));
    }
    if let Some(provider) = &r.provider {
        line.push_str(&format!(" -> {provider}"));
    }
    let mut details = Vec::new();
    if let Some(ttfb) = r.ttfb_ms.filter(|_| r.stream) {
        details.push(format!("ttfb {ttfb}ms"));
    }
    if let Some(u) = r.usage {
        if u.input_tokens + u.output_tokens > 0 || u.seconds.is_none() {
            details.push(format!(
                "{} in / {} out tokens",
                u.input_tokens, u.output_tokens
            ));
        }
        if let Some(seconds) = u.seconds {
            details.push(format!("{seconds} s of audio"));
        }
    }
    if !details.is_empty() {
        line.push_str(&format!(" ({})", details.join(", ")));
    }
    if let Some(error) = &r.error {
        line.push_str(&format!(": {error}"));
    }
    line
}

/// Logs each request through: its start now, its end once its body ends.
pub async fn track(State(log): State<Arc<RequestLog>>, mut req: Request, next: Next) -> Response {
    let entry = log.start(&req);
    let mut extensions = req
        .extensions_mut()
        .remove::<Extensions>()
        .unwrap_or_default();
    extensions.insert(Tracked(entry.clone()));
    req.extensions_mut().insert(extensions);
    let resp = next.run(req).await;
    let status = resp.status();
    {
        let mut record = entry.record();
        record.status = Some(status.as_u16());
        let content_type = resp
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|v| v.to_str().ok());
        record.stream = status.as_u16() == 101
            || content_type.is_some_and(|t| t.starts_with("text/event-stream"));
    }
    let (head, body) = resp.into_parts();
    if http_body::Body::is_end_stream(&body) {
        entry.record().ttfb_ms = Some(entry.elapsed_ms());
        log.finish(&entry, None);
        return Response::from_parts(head, body);
    }
    let error_body = status.as_u16() >= 400;
    let body = TrackedBody {
        inner: body,
        entry,
        log,
        error_body: error_body.then(Vec::new),
    };
    Response::from_parts(head, Body::new(body))
}

/// A response body that tells the log when its first bytes go and when it
/// ends.
struct TrackedBody {
    inner: Body,
    entry: Arc<Entry>,
    log: Arc<RequestLog>,
    /// What came of an error's body, for its message.
    error_body: Option<Vec<u8>>,
}

impl TrackedBody {
    fn end(&mut self, error: Option<String>) {
        let said = self.error_body.as_deref().map(error_message);
        let error = error.or(said.filter(|message| !message.is_empty()));
        self.log.finish(&self.entry, error);
    }
}

impl http_body::Body for TrackedBody {
    type Data = Bytes;
    type Error = axum::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, axum::Error>>> {
        let this = self.get_mut();
        let polled = Pin::new(&mut this.inner).poll_frame(cx);
        match &polled {
            Poll::Ready(Some(Ok(frame))) => {
                if let Some(data) = frame.data_ref() {
                    let mut record = this.entry.record();
                    if record.ttfb_ms.is_none() {
                        record.ttfb_ms = Some(this.entry.elapsed_ms());
                    }
                    drop(record);
                    if let Some(kept) = &mut this.error_body {
                        let room = ERROR_BODY.saturating_sub(kept.len());
                        kept.extend_from_slice(&data[..data.len().min(room)]);
                    }
                }
                if this.inner.is_end_stream() {
                    this.end(None);
                }
            }
            Poll::Ready(Some(Err(e))) => this.end(Some(e.to_string())),
            Poll::Ready(None) => this.end(None),
            Poll::Pending => {}
        }
        polled
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

impl Drop for TrackedBody {
    fn drop(&mut self) {
        self.end(Some("the client left before the response ended".to_owned()));
    }
}

/// The message of an error body: its JSON `error.message` or `message`, or
/// its text.
fn error_message(body: &[u8]) -> String {
    let json: Option<serde_json::Value> = serde_json::from_slice(body).ok();
    let said = json.as_ref().and_then(|v| {
        let error = &v["error"];
        error["message"]
            .as_str()
            .or(error.as_str())
            .or(v["message"].as_str())
    });
    let text = match said {
        Some(message) => message.to_owned(),
        None => String::from_utf8_lossy(body).trim().to_owned(),
    };
    match text.char_indices().nth(300) {
        Some((cut, _)) => format!("{}...", &text[..cut]),
        None => text,
    }
}

/// What a client sent, as kept: its first [`KEPT_CHARS`] characters, and
/// `…` if there were more.
fn clip(text: &str) -> String {
    match text.char_indices().nth(KEPT_CHARS) {
        Some((cut, _)) => format!("{}…", &text[..cut]),
        None => text.to_owned(),
    }
}

/// `line` with its control characters escaped, so one request is one line.
fn escape_controls(line: &str) -> String {
    let escaped = |c: char| match c.is_control() {
        true => c.escape_default().collect(),
        false => c.to_string(),
    };
    line.chars().map(escaped).collect()
}

/// The plugin that fills in what the gateway knows of a request.
pub struct Recorder {
    log: Arc<RequestLog>,
    /// Each format's kind, and how its requests name their model.
    formats: HashMap<String, (Kind, Arc<ReadModel>)>,
    /// Each core provider's configured name.
    providers: HashMap<String, String>,
}

impl std::fmt::Debug for Recorder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Recorder").finish_non_exhaustive()
    }
}

impl Recorder {
    pub fn new(
        log: Arc<RequestLog>,
        formats: &[KnownFormat],
        providers: HashMap<String, String>,
    ) -> Self {
        let formats = formats
            .iter()
            .map(|f| (f.name.clone(), (f.kind, f.model_reader())))
            .collect();
        Self {
            log,
            formats,
            providers,
        }
    }
}

#[async_trait]
impl Plugin for Recorder {
    async fn on_client(
        &self,
        cx: &mut ClientContext,
    ) -> Result<Option<respan_gateway_core::Response>, Fault> {
        let Some(Tracked(entry)) = cx.extensions().get::<Tracked>().cloned() else {
            return Ok(None);
        };
        {
            let mut record = entry.record();
            record.format = Some(cx.format().to_owned());
            if let Some((kind, model)) = self.formats.get(cx.format()) {
                record.kind = Some(kind.label);
                let model = model(cx).map(|m| clip(&m));
                record.model = model.or(record.model.take());
            }
        }
        self.log.publish(&entry);
        count(cx, &entry, |output: &conversation::Output| match output {
            conversation::Output::Usage(usage) => Some(Counted::tokens(usage)),
            _ => None,
        })?;
        count(cx, &entry, |output: &image::Output| match output {
            image::Output::Usage(usage) => Some(Counted::tokens(usage)),
            _ => None,
        })?;
        count(cx, &entry, |output: &transcription::Output| match output {
            transcription::Output::Usage(usage) => Some(Counted::tokens(usage)),
            transcription::Output::Seconds(seconds) => Some(Counted::Seconds(*seconds)),
            _ => None,
        })?;
        count(cx, &entry, |output: &speech::Output| match output {
            speech::Output::Usage(usage) => Some(Counted::tokens(usage)),
            _ => None,
        })?;
        Ok(None)
    }

    async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
        if let Some(Tracked(entry)) = px.extensions().get::<Tracked>() {
            let provider = self.providers.get(px.provider()).cloned();
            entry.record().provider = provider.or_else(|| Some(px.provider().to_owned()));
        }
        Ok(())
    }
}

/// Adds to `entry`'s usage what `read` finds in each output, if `T` is the
/// request's output type.
fn count<T: Clone + Send + Sync + 'static>(
    cx: &mut ClientContext,
    entry: &Arc<Entry>,
    read: fn(&T) -> Option<Counted>,
) -> Result<(), Fault> {
    if !cx.is_kind::<T>() {
        return Ok(());
    }
    let entry = entry.clone();
    cx.filter(move |output: &mut T| {
        if let Some(counted) = read(output) {
            let mut record = entry.record();
            record.usage.get_or_insert_with(Usage::default).add(counted);
        }
        Ok(())
    })
}

/// `time` as RFC 3339 in UTC, to the millisecond.
pub fn rfc3339(time: SystemTime) -> String {
    let since = time
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default();
    let secs = since.as_secs();
    let (days, rest) = (secs / 86_400, secs % 86_400);
    // Civil from days, after Howard Hinnant's algorithm.
    let z = days as i64 + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}.{:03}Z",
        rest / 3600,
        rest % 3600 / 60,
        rest % 60,
        since.subsec_millis()
    )
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn times_are_rfc3339_with_millis() {
        let at = |secs: u64, millis: u64| {
            rfc3339(SystemTime::UNIX_EPOCH + Duration::from_millis(secs * 1000 + millis))
        };
        assert_eq!(at(0, 0), "1970-01-01T00:00:00.000Z");
        assert_eq!(at(1_791_633_600, 7), "2026-10-10T12:00:00.007Z");
        assert_eq!(at(951_782_400, 999), "2000-02-29T00:00:00.999Z");
    }

    #[test]
    fn an_error_message_is_read_from_its_body() {
        assert_eq!(
            error_message(br#"{"error":{"message":"bad key","type":"x"}}"#),
            "bad key"
        );
        assert_eq!(error_message(br#"{"error":"nope"}"#), "nope");
        assert_eq!(
            error_message(b" no format served accepts GET /x \n"),
            "no format served accepts GET /x"
        );
    }

    #[test]
    fn what_a_client_sent_is_clipped_and_cannot_forge_a_line() {
        let long = "m".repeat(10_000);
        let kept = clip(&long);
        assert_eq!(kept.chars().count(), KEPT_CHARS + 1);
        assert!(kept.ends_with('…'));
        assert_eq!(clip("gpt-4o"), "gpt-4o");
        assert_eq!(
            escape_controls("r_1 POST /x 200 1ms a\nr_2 forged\u{1b}[2J"),
            "r_1 POST /x 200 1ms a\\nr_2 forged\\u{1b}[2J"
        );
    }

    #[test]
    fn a_line_says_what_matters() {
        let record = RequestRecord {
            id: "r_000001".into(),
            started_at: String::new(),
            method: "POST".into(),
            path: "/v1/chat/completions".into(),
            format: Some("openai-chat".into()),
            kind: Some("chat"),
            model: Some("gpt-4o-mini".into()),
            provider: Some("openai".into()),
            status: Some(200),
            duration_ms: Some(812),
            ttfb_ms: Some(230),
            stream: true,
            usage: Some(Usage {
                input_tokens: 12,
                output_tokens: 40,
                seconds: None,
            }),
            error: None,
        };
        assert_eq!(
            line(&record),
            "r_000001 POST /v1/chat/completions 200 812ms gpt-4o-mini -> openai (ttfb 230ms, 12 in / 40 out tokens)"
        );
    }
}
