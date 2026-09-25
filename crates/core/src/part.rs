use std::collections::BTreeMap;
use std::sync::OnceLock;

use bytes::Bytes;
use http::{HeaderMap, StatusCode};
use serde_json::Value;

use crate::Fault;

#[derive(Debug, Clone, Default)]
pub struct ResponseHead {
    pub status: StatusCode,
    pub headers: HeaderMap,
}

impl ResponseHead {
    pub fn content_type(&self) -> Option<&str> {
        self.headers
            .get(http::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
    }
}

/// What flows through a response stream.
#[derive(Debug)]
pub enum Part {
    Head(ResponseHead),
    /// A whole message: a unary body.
    Doc(Doc),
    /// A piece of streamed content.
    Delta(Delta),
    /// A frame with no content (keep-alives, metadata), relayed as is.
    Frame(Bytes),
    /// Usage reported by the provider, as an increment.
    Usage(Usage),
    /// Exactly one per stream.
    End(End),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum End {
    Done,
    Failed(Fault),
}

/// A whole JSON message. Parsed on first read; relays its original bytes
/// until it is edited.
#[derive(Debug, Clone)]
pub struct Doc {
    raw: Bytes,
    parsed: OnceLock<Option<Value>>,
    edited: bool,
}

impl Doc {
    pub fn new(raw: Bytes) -> Self {
        Self {
            raw,
            parsed: OnceLock::new(),
            edited: false,
        }
    }

    pub fn from_json(value: Value) -> Self {
        let parsed = OnceLock::new();
        let _ = parsed.set(Some(value));
        Self {
            raw: Bytes::new(),
            parsed,
            edited: true,
        }
    }

    pub fn raw(&self) -> &Bytes {
        &self.raw
    }

    pub fn json(&self) -> Option<&Value> {
        self.parsed
            .get_or_init(|| serde_json::from_slice(&self.raw).ok())
            .as_ref()
    }

    /// Mutable access. Marks the doc edited, so it is re-serialized on encode.
    pub fn json_mut(&mut self) -> Option<&mut Value> {
        self.json();
        self.edited = true;
        self.parsed.get_mut().and_then(Option::as_mut)
    }

    pub fn is_edited(&self) -> bool {
        self.edited
    }

    pub fn to_bytes(&self) -> Bytes {
        match (self.edited, self.json()) {
            (true, Some(v)) => Bytes::from(serde_json::to_vec(v).unwrap_or_default()),
            _ => self.raw.clone(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeltaKind {
    Text,
    Args,
    Reasoning,
    Bytes,
}

/// A piece of streamed content. Relays its original frame until the text is
/// edited through [`Delta::text_mut`].
#[derive(Debug, Clone)]
pub struct Delta {
    block: u32,
    kind: DeltaKind,
    text: String,
    raw: Option<Bytes>,
    frame: Option<Value>,
}

impl Delta {
    pub fn new(block: u32, kind: DeltaKind, text: impl Into<String>) -> Self {
        Self {
            block,
            kind,
            text: text.into(),
            raw: None,
            frame: None,
        }
    }

    /// For formats: a delta decoded from `raw`, keeping the parsed frame so an
    /// edited delta can be re-encoded in the same shape.
    pub fn decoded(
        block: u32,
        kind: DeltaKind,
        text: impl Into<String>,
        raw: Bytes,
        frame: Value,
    ) -> Self {
        Self {
            block,
            kind,
            text: text.into(),
            raw: Some(raw),
            frame: Some(frame),
        }
    }

    pub fn block(&self) -> u32 {
        self.block
    }

    pub fn kind(&self) -> DeltaKind {
        self.kind
    }

    pub fn text(&self) -> &str {
        &self.text
    }

    pub fn text_mut(&mut self) -> &mut String {
        self.raw = None;
        &mut self.text
    }

    /// The original frame bytes, if the delta is unedited.
    pub fn raw(&self) -> Option<&Bytes> {
        self.raw.as_ref()
    }

    pub fn frame(&self) -> Option<&Value> {
        self.frame.as_ref()
    }
}

pub mod units {
    pub const INPUT_TOKENS: &str = "input_tokens";
    pub const OUTPUT_TOKENS: &str = "output_tokens";
    pub const CACHED_INPUT_TOKENS: &str = "cached_input_tokens";
    pub const REASONING_TOKENS: &str = "reasoning_tokens";
}

/// An open map of usage units. Always additive.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Usage(BTreeMap<String, f64>);

impl Usage {
    pub fn add(&mut self, unit: impl Into<String>, amount: f64) {
        *self.0.entry(unit.into()).or_insert(0.0) += amount;
    }

    pub fn get(&self, unit: &str) -> f64 {
        self.0.get(unit).copied().unwrap_or(0.0)
    }

    pub fn merge(&mut self, other: &Usage) {
        for (unit, amount) in &other.0 {
            self.add(unit.clone(), *amount);
        }
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&str, f64)> {
        self.0.iter().map(|(k, v)| (k.as_str(), *v))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unedited_doc_keeps_its_bytes() {
        let raw = Bytes::from_static(b"{\"b\": 1,  \"a\": 2}");
        let doc = Doc::new(raw.clone());
        assert_eq!(doc.json().and_then(|v| v["a"].as_i64()), Some(2));
        assert_eq!(doc.to_bytes(), raw);
    }

    #[test]
    fn edited_doc_keeps_key_order() {
        let mut doc = Doc::new(Bytes::from_static(b"{\"b\":1,\"a\":2}"));
        if let Some(v) = doc.json_mut() {
            v["a"] = 3.into();
        }
        assert_eq!(doc.to_bytes(), Bytes::from_static(b"{\"b\":1,\"a\":3}"));
    }

    #[test]
    fn editing_a_delta_drops_its_raw_frame() {
        let mut d = Delta::decoded(
            0,
            DeltaKind::Text,
            "hi",
            Bytes::from_static(b"x"),
            Value::Null,
        );
        assert!(d.raw().is_some());
        d.text_mut().push('!');
        assert!(d.raw().is_none());
        assert_eq!(d.text(), "hi!");
    }
}
