//! The gateway's log lines on stderr: a timestamp, a level and a message,
//! as text or as one JSON object a line.

use std::time::SystemTime;

use clap::ValueEnum;
use serde::Deserialize;
use serde_json::{Map, Value, json};

use crate::requests::rfc3339;

/// How log lines are written. `off` silences the request log alone:
/// warnings are always written.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, ValueEnum)]
#[serde(rename_all = "lowercase")]
pub enum LogFormat {
    #[default]
    Text,
    Json,
    Off,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Level {
    Info,
    Warn,
    Error,
}

impl Level {
    fn label(self) -> &'static str {
        match self {
            Level::Info => "info",
            Level::Warn => "warn",
            Level::Error => "error",
        }
    }
}

impl LogFormat {
    /// The line for `message` and its `fields`.
    pub fn line(self, level: Level, message: &str, fields: Map<String, Value>) -> String {
        let ts = rfc3339(SystemTime::now());
        match self {
            LogFormat::Json => {
                let mut line = Map::new();
                line.insert("ts".into(), json!(ts));
                line.insert("level".into(), json!(level.label()));
                line.insert("msg".into(), json!(message));
                line.extend(fields);
                Value::Object(line).to_string()
            }
            _ => format!("{ts} {:<5} {message}", level.label().to_ascii_uppercase()),
        }
    }

    /// Writes a message of the gateway's own to stderr.
    pub fn say(self, level: Level, message: &str) {
        eprintln!("{}", self.line(level, message, Map::new()));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_line_is_text_or_json() {
        let text = LogFormat::Text.line(Level::Warn, "careful", Map::new());
        assert!(text.contains(" WARN  careful"), "{text}");
        let json = LogFormat::Json.line(Level::Info, "hi", Map::new());
        let json: Value = serde_json::from_str(&json).unwrap_or_default();
        assert_eq!(json["level"], "info");
        assert_eq!(json["msg"], "hi");
        assert!(json["ts"].as_str().is_some_and(|ts| ts.ends_with('Z')));
    }
}
