//! The log itself: the kept requests, their totals, and the line each
//! prints.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Instant, SystemTime};

use axum::extract::Request;
use respan_gateway_core::Extensions;
use serde::Serialize;
use serde_json::{Map, Value};
use tokio::sync::broadcast;

use super::KEPT;
use super::record::{RequestRecord, clip, rfc3339};
use crate::config::{Config, Price};
use crate::history::{self, Filter, Writer};
use crate::logging::{Level, LogFormat};

#[derive(Debug, Clone, Copy, Default, Serialize)]
pub struct Totals {
    pub requests: u64,
    pub errors: u64,
    pub in_flight: u64,
    pub input_tokens: u64,
    pub output_tokens: u64,
    /// The sum of the requests' `cost_usd`; present once `prices` is set.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cost_usd: Option<f64>,
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
    finished: AtomicBool,
    /// Set by the plugin once it sees the request: its end hook is then
    /// heard, which ends a session (its `101` is only its start).
    pub(super) recorded: AtomicBool,
    /// Set when the plugin's end hook ran.
    pub(super) ended: AtomicBool,
}

impl Entry {
    /// A request that finished before this run, as the history file holds
    /// it.
    fn finished(record: RequestRecord) -> Self {
        Self {
            record: Mutex::new(record),
            started: Instant::now(),
            finished: AtomicBool::new(true),
            recorded: AtomicBool::new(false),
            ended: AtomicBool::new(false),
        }
    }

    pub(super) fn record(&self) -> MutexGuard<'_, RequestRecord> {
        self.record.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub fn snapshot(&self) -> RequestRecord {
        self.record().clone()
    }

    pub(super) fn elapsed_ms(&self) -> u64 {
        self.started
            .elapsed()
            .as_millis()
            .try_into()
            .unwrap_or(u64::MAX)
    }
}

/// The request's entry, in its extensions.
#[derive(Debug, Clone)]
pub(super) struct Tracked(pub(super) Arc<Entry>);

#[derive(Debug)]
struct Kept {
    /// The most requests held in `recent`.
    capacity: usize,
    format: LogFormat,
    recent: VecDeque<Arc<Entry>>,
    /// The config's `prices`.
    prices: Vec<(String, Price)>,
    /// Where finished requests are also written, if the config says.
    writer: Option<Writer>,
    totals: Totals,
    providers: HashMap<String, ProviderStats>,
}

#[derive(Debug)]
pub struct RequestLog {
    next_id: AtomicU64,
    kept: Mutex<Kept>,
    events: broadcast::Sender<Arc<str>>,
}

impl RequestLog {
    /// A log as `config` says: keeping its last `request_history` requests,
    /// printing each as its `log` says, starting with the ones its
    /// `request_history_file` holds.
    pub fn new(config: &Config) -> Arc<Self> {
        let capacity = config.request_history.unwrap_or(KEPT);
        let loaded = (config.request_history_file.as_deref())
            .map(|path| history::load(path, capacity))
            .unwrap_or_default();
        let next_id = (loaded.iter())
            .filter_map(|r| r.id.strip_prefix("r_")?.parse::<u64>().ok())
            .max()
            .map_or(1, |last| last + 1);
        let recent = loaded.into_iter().map(Entry::finished).map(Arc::new);
        let log = Arc::new(Self {
            next_id: AtomicU64::new(next_id),
            kept: Mutex::new(Kept {
                capacity,
                format: config.log,
                recent: recent.collect(),
                prices: Vec::new(),
                writer: None,
                totals: Totals::default(),
                providers: HashMap::new(),
            }),
            events: broadcast::channel(256).0,
        });
        log.configure(config);
        log
    }

    /// Applies a reloaded `config`: what is kept stays, up to its
    /// `request_history`.
    pub fn configure(&self, config: &Config) {
        let mut kept = self.kept();
        kept.capacity = config.request_history.unwrap_or(KEPT);
        kept.format = config.log;
        while kept.recent.len() > kept.capacity {
            kept.recent.pop_front();
        }
        kept.prices = (config.prices.iter())
            .map(|(prefix, price)| (prefix.clone(), *price))
            .collect();
        kept.totals.cost_usd = match kept.prices.is_empty() {
            true => None,
            false => Some(kept.totals.cost_usd.unwrap_or_default()),
        };
        if !kept.writer.as_ref().is_some_and(|w| w.is(config)) {
            kept.writer = Writer::from_config(config);
        }
    }

    fn kept(&self) -> MutexGuard<'_, Kept> {
        self.kept.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// The last `limit` requests that match `filter`, newest first.
    pub fn recent(&self, limit: usize, filter: &Filter) -> Vec<RequestRecord> {
        let kept = self.kept();
        let all = kept.recent.iter().rev().map(|e| e.snapshot());
        all.filter(|r| filter.matches(r)).take(limit).collect()
    }

    /// The kept requests that match `filter`, oldest first.
    pub fn export(&self, filter: &Filter) -> Vec<RequestRecord> {
        let kept = self.kept();
        let all = kept.recent.iter().map(|e| e.snapshot());
        all.filter(|r| filter.matches(r)).collect()
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

    pub(super) fn publish(&self, entry: &Entry) {
        if let Ok(json) = serde_json::to_string(&*entry.record()) {
            let _ = self.events.send(json.into());
        }
    }

    pub(super) fn start(&self, req: &Request) -> Arc<Entry> {
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
            client: None,
            status: None,
            duration_ms: None,
            ttfb_ms: None,
            stream: false,
            usage: None,
            cost_usd: None,
            steps: Vec::new(),
            error: None,
        };
        let entry = Arc::new(Entry {
            record: Mutex::new(record),
            started: Instant::now(),
            finished: AtomicBool::new(false),
            recorded: AtomicBool::new(false),
            ended: AtomicBool::new(false),
        });
        {
            let mut kept = self.kept();
            kept.recent.push_back(entry.clone());
            while kept.recent.len() > kept.capacity {
                kept.recent.pop_front();
            }
            kept.totals.requests += 1;
            kept.totals.in_flight += 1;
        }
        self.publish(&entry);
        entry
    }

    pub(super) fn finish(&self, entry: &Entry, error: Option<String>) {
        if entry.finished.swap(true, Ordering::SeqCst) {
            return;
        }
        let prices = self.kept().prices.clone();
        let record = {
            let mut record = entry.record();
            record.duration_ms = Some(entry.elapsed_ms());
            if record.error.is_none() {
                record.error = error;
            }
            if let (Some(model), Some(usage)) = (&record.model, &record.usage) {
                record.cost_usd = history::cost(&prices, model, usage);
            }
            record.clone()
        };
        let failed = record.error.is_some() || record.status.is_some_and(|s| s >= 400);
        let format = {
            let mut kept = self.kept();
            let totals = &mut kept.totals;
            totals.in_flight = totals.in_flight.saturating_sub(1);
            totals.errors += u64::from(failed);
            if let Some(usage) = record.usage {
                totals.input_tokens += usage.input_tokens;
                totals.output_tokens += usage.output_tokens;
            }
            if let Some(cost) = record.cost_usd {
                *totals.cost_usd.get_or_insert(0.0) += cost;
            }
            if let Some(provider) = &record.provider {
                let stats = kept.providers.entry(provider.clone()).or_default();
                stats.requests += 1;
                stats.errors += u64::from(failed);
                stats.total_ms += record.duration_ms.unwrap_or_default();
            }
            if let Some(writer) = &kept.writer {
                writer.append(&record);
            }
            kept.format
        };
        self.publish(entry);
        if format != LogFormat::Off {
            self.print(format, &record);
        }
    }
}

impl RequestLog {
    /// Writes a finished request to stderr: a warning if it was refused, an
    /// error if it failed.
    fn print(&self, format: LogFormat, record: &RequestRecord) {
        let level = match record.status {
            _ if record.error.is_some() => Level::Error,
            Some(s) if s >= 500 => Level::Error,
            Some(s) if s >= 400 => Level::Warn,
            _ => Level::Info,
        };
        let fields = match serde_json::to_value(record) {
            Ok(Value::Object(fields)) => fields,
            _ => Map::new(),
        };
        let message = match format {
            LogFormat::Json => "request".to_owned(),
            _ => escape_controls(&line(record)),
        };
        eprintln!("{}", format.line(level, &message, fields));
    }
}

/// One finished request, in a line: `r_000001 POST /v1/chat/completions
/// 200 812ms gpt-4o-mini -> openai (client app, ttfb 230ms, 12 in / 40 out
/// tokens)`.
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
    if let Some(client) = &r.client {
        details.push(format!("client {client}"));
    }
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
    if !r.steps.is_empty() {
        let tools: usize = r.steps.iter().map(|s| s.tools.len()).sum();
        details.push(format!("{} steps, {tools} tool calls", r.steps.len()));
    }
    if !details.is_empty() {
        line.push_str(&format!(" ({})", details.join(", ")));
    }
    if let Some(error) = &r.error {
        line.push_str(&format!(": {error}"));
    }
    line
}

/// `line` with its control characters escaped, so one request is one line.
fn escape_controls(line: &str) -> String {
    let escaped = |c: char| match c.is_control() {
        true => c.escape_default().collect(),
        false => c.to_string(),
    };
    line.chars().map(escaped).collect()
}

/// Names the client whose key the request in `extensions` carried, in its
/// record.
pub fn attribute(extensions: &Extensions, client: &str) {
    if let Some(Tracked(entry)) = extensions.get::<Tracked>() {
        entry.record().client = Some(client.to_owned());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::requests::record::Usage;

    #[test]
    fn a_client_cannot_forge_a_line() {
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
            kind: Some("chat".into()),
            model: Some("gpt-4o-mini".into()),
            provider: Some("openai".into()),
            client: None,
            status: Some(200),
            duration_ms: Some(812),
            ttfb_ms: Some(230),
            stream: true,
            usage: Some(Usage {
                input_tokens: 12,
                output_tokens: 40,
                cached_input_tokens: 0,
                seconds: None,
            }),
            cost_usd: None,
            steps: Vec::new(),
            error: None,
        };
        assert_eq!(
            line(&record),
            "r_000001 POST /v1/chat/completions 200 812ms gpt-4o-mini -> openai (ttfb 230ms, 12 in / 40 out tokens)"
        );
    }
}
