//! The request history beyond memory: an optional JSON-lines file each
//! finished request is appended to (and reloaded from at start), the cost
//! of a request from the config's `prices`, and the export's filter.

use std::collections::VecDeque;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::mpsc::{Receiver, SyncSender, sync_channel};

use chrono::{DateTime, NaiveDate, NaiveTime, Utc};
use serde::Deserialize;

use crate::config::Config;
use crate::requests::RequestRecord;

/// The most records waiting for the writer; past it, records miss the file
/// (never memory) rather than slow requests.
const QUEUE: usize = 4096;
/// The file is rotated at this size unless the config's
/// `request_log.file_max` says.
pub const DEFAULT_MAX: usize = 64 << 20;

/// Appends lines to a file from its own thread, so no request waits for
/// the disk.
#[derive(Debug)]
pub struct Writer {
    path: PathBuf,
    max: usize,
    lines: SyncSender<String>,
}

impl Writer {
    /// A writer for the file `config` names, if it names one.
    pub fn from_config(config: &Config) -> Option<Self> {
        let path = config.plugins.request_log.file.clone()?;
        let max = config.plugins.request_log.file_max.unwrap_or(DEFAULT_MAX);
        Some(Self::open(path, max))
    }

    fn open(path: PathBuf, max: usize) -> Self {
        let (lines, queue) = sync_channel::<String>(QUEUE);
        let target = path.clone();
        let spawned = std::thread::Builder::new()
            .name("request-history".into())
            .spawn(move || write_lines(&target, max, &queue));
        if let Err(e) = spawned {
            eprintln!("warning: request history: cannot start the writer: {e}");
        }
        Self { path, max, lines }
    }

    /// Whether `config` asks for this writer as it is.
    pub fn is(&self, config: &Config) -> bool {
        let max = config.plugins.request_log.file_max.unwrap_or(DEFAULT_MAX);
        config.plugins.request_log.file.as_deref() == Some(&self.path) && max == self.max
    }

    pub fn append(&self, record: &RequestRecord) {
        if let Ok(line) = serde_json::to_string(record) {
            // A full queue or a stopped writer: the file misses this one.
            let _ = self.lines.try_send(line);
        }
    }
}

/// Writes each line until the [`Writer`], and so the sender, is dropped. A
/// disk error is a warning, once per run of failures, and the next line
/// tries again.
fn write_lines(path: &Path, max: usize, queue: &Receiver<String>) {
    let mut file = None;
    let mut failing = false;
    while let Ok(line) = queue.recv() {
        match append(path, max, &mut file, &line) {
            Ok(()) => failing = false,
            Err(e) => {
                if !failing {
                    eprintln!("warning: request history: {}: {e}", path.display());
                }
                failing = true;
            }
        }
    }
}

fn open(path: &Path) -> std::io::Result<(File, usize)> {
    let mut options = OpenOptions::new();
    options.create(true).append(true);
    // Client names, models, paths and errors: the owner's to read.
    #[cfg(unix)]
    std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
    let file = options.open(path)?;
    let size = file.metadata()?.len();
    Ok((file, usize::try_from(size).unwrap_or(usize::MAX)))
}

/// Appends `line`, first moving a file it would grow past `max` to
/// `<path>.1` (replacing the one there). `file` is the open file and its
/// size; left empty on an error, so the next line opens it again.
fn append(
    path: &Path,
    max: usize,
    file: &mut Option<(File, usize)>,
    line: &str,
) -> std::io::Result<()> {
    let need = line.len() + 1;
    let (mut f, mut size) = match file.take() {
        Some(open) => open,
        None => open(path)?,
    };
    if size > 0 && size + need > max {
        drop(f);
        fs::rename(path, rotated(path))?;
        (f, size) = open(path)?;
    }
    f.write_all(format!("{line}\n").as_bytes())?;
    *file = Some((f, size + need));
    Ok(())
}

/// `requests.jsonl` -> `requests.jsonl.1`
fn rotated(path: &Path) -> PathBuf {
    let mut name = path.as_os_str().to_owned();
    name.push(".1");
    PathBuf::from(name)
}

/// The last `keep` records of the rotated file and the file, oldest first.
/// A line that is not a record (a torn last one) is skipped.
pub fn load(path: &Path, keep: usize) -> Vec<RequestRecord> {
    let mut last = VecDeque::new();
    for file in [rotated(path), path.to_owned()] {
        let Ok(file) = File::open(file) else { continue };
        for line in BufReader::new(file).lines().map_while(Result::ok) {
            if let Ok(record) = serde_json::from_str::<RequestRecord>(&line) {
                if last.len() == keep {
                    last.pop_front();
                }
                last.push_back(record);
            }
        }
    }
    last.into()
}

/// The export's filters, as the UI's: every one given must match.
#[derive(Debug, Default, Deserialize)]
pub struct Filter {
    pub kind: Option<String>,
    /// `ok`, `error`, `pending`, or a class such as `2xx`.
    pub status: Option<String>,
    pub provider: Option<String>,
    /// The name of the client key.
    pub client: Option<String>,
    pub model: Option<String>,
    /// Free text, matched in any case against the model and the path.
    pub q: Option<String>,
    /// An RFC 3339 time, or its date (midnight UTC): only requests started
    /// at or after it.
    #[serde(default, deserialize_with = "since")]
    pub since: Option<DateTime<Utc>>,
}

impl Filter {
    pub fn matches(&self, r: &RequestRecord) -> bool {
        let is = |want: &Option<String>, have: Option<&str>| {
            want.as_deref().is_none_or(|w| have == Some(w))
        };
        let model = r.model.as_deref().unwrap_or_default();
        is(&self.kind, r.kind.as_deref())
            && is(&self.client, r.client.as_deref())
            && is(&self.provider, r.provider.as_deref())
            && is(&self.model, r.model.as_deref())
            && self.status.as_deref().is_none_or(|s| status_is(s, r))
            && self.q.as_deref().map(str::trim).is_none_or(|q| {
                let q = q.to_lowercase();
                model.to_lowercase().contains(&q) || r.path.to_lowercase().contains(&q)
            })
            && self.since.is_none_or(|since| {
                DateTime::parse_from_rfc3339(&r.started_at).is_ok_and(|at| at >= since)
            })
    }
}

/// `since`: an RFC 3339 time, or a date, which means its midnight UTC.
fn since<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Option<DateTime<Utc>>, D::Error> {
    let Some(text) = Option::<String>::deserialize(d)? else {
        return Ok(None);
    };
    let time = DateTime::parse_from_rfc3339(&text)
        .map(|t| t.with_timezone(&Utc))
        .or_else(|_| {
            NaiveDate::parse_from_str(&text, "%Y-%m-%d")
                .map(|d| d.and_time(NaiveTime::MIN).and_utc())
        })
        .map_err(|_| {
            serde::de::Error::custom(format!("since: '{text}' is not an RFC 3339 time or date"))
        })?;
    Ok(Some(time))
}

fn status_is(want: &str, r: &RequestRecord) -> bool {
    match (want, r.status) {
        ("pending", status) => status.is_none(),
        (_, None) => false,
        ("ok", Some(s)) => s < 400 && r.error.is_none(),
        ("error", Some(s)) => s >= 400 || r.error.is_some(),
        (class, Some(s)) => {
            class.len() == 3
                && class.ends_with("xx")
                && class[..1].parse::<u16>().is_ok_and(|c| s / 100 == c)
        }
    }
}

/// The columns of a CSV export.
pub const CSV_HEADER: &str = "id,started_at,method,path,format,kind,model,provider,client,status,\
duration_ms,ttfb_ms,stream,input_tokens,output_tokens,cost_usd,error";

/// One record as a CSV row, with [`CSV_HEADER`]'s columns.
pub fn csv_row(r: &RequestRecord) -> String {
    let n = |v: Option<u64>| v.map(|v| v.to_string()).unwrap_or_default();
    let text = |v: Option<&str>| no_formula(v.unwrap_or_default());
    let cells = [
        no_formula(&r.id),
        no_formula(&r.started_at),
        no_formula(&r.method),
        no_formula(&r.path),
        text(r.format.as_deref()),
        text(r.kind.as_deref()),
        text(r.model.as_deref()),
        text(r.provider.as_deref()),
        text(r.client.as_deref()),
        n(r.status.map(u64::from)),
        n(r.duration_ms),
        n(r.ttfb_ms),
        r.stream.to_string(),
        n(r.usage.map(|u| u.input_tokens)),
        n(r.usage.map(|u| u.output_tokens)),
        r.cost_usd.map(|c| c.to_string()).unwrap_or_default(),
        text(r.error.as_deref()),
    ];
    csv_line(&cells)
}

/// `cells` as one CSV line, each quoted if it needs to be.
fn csv_line(cells: &[String]) -> String {
    let mut line = csv::WriterBuilder::new().from_writer(Vec::new());
    let _ = line.write_record(cells);
    let line = line.into_inner().unwrap_or_default();
    String::from_utf8_lossy(&line)
        .trim_end_matches(['\r', '\n'])
        .to_owned()
}

/// `text` with a leading `=`, `+`, `-` or `@` neutralized, so a spreadsheet
/// never reads client text as a formula.
fn no_formula(text: &str) -> String {
    match text.chars().next() {
        Some('=' | '+' | '-' | '@' | '\t' | '\r') => format!("'{text}"),
        _ => text.to_owned(),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn cells_are_quoted_and_cannot_be_formulas() {
        let cells = ["plain", "a,\"b\"", "=1+1"].map(no_formula);
        assert_eq!(csv_line(&cells), "plain,\"a,\"\"b\"\"\",'=1+1");
    }
}
