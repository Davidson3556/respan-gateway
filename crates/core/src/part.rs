use std::any::{Any, TypeId};
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::sync::{Arc, OnceLock};

use bytes::Bytes;
use http::StatusCode;
use serde_json::Value;

use crate::{Event, Fault};

/// A response head with `status` and nothing else.
pub(crate) fn head(status: StatusCode) -> http::response::Parts {
    let (mut head, ()) = http::Response::new(()).into_parts();
    head.status = status;
    head
}

/// What a response body is decoded into.
#[derive(Debug)]
#[non_exhaustive]
pub enum Part {
    /// One message: its output events and the bytes they came from.
    Item(Item),
    /// Bytes with no content: framing, a keep-alive, or a failed response's
    /// error body, which an `Err` end follows. Content goes in items, even
    /// a message the format cannot read, so filters see it all; only a
    /// format of no kind makes every byte a Frame. Written only in the
    /// format it came from.
    Frame(Bytes),
    /// An increment of usage, from the provider or
    /// [`Response::with_usage`](crate::Response::with_usage).
    Usage(Usage),
    /// The last Part of every response.
    End(Result<(), Fault>),
}

/// One message of a response: its events, such as an OpenAI chunk's start
/// and text, and the bytes it was decoded from, if any.
///
/// Relayed, it is written as its bytes until edited, then rebuilt from
/// them; otherwise it is written from its events.
pub struct Item {
    /// A `Vec<T>` of the kind's output type.
    events: Box<dyn Any + Send + Sync>,
    /// The `TypeId` of `T`.
    event_type: TypeId,
    raw: Option<Bytes>,
    edited: bool,
}

impl std::fmt::Debug for Item {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Item")
            .field("raw", &self.raw)
            .field("edited", &self.edited)
            .finish_non_exhaustive()
    }
}

impl Item {
    /// An item of `events`, with no bytes behind it.
    pub fn new<T: Event>(events: impl IntoIterator<Item = T>) -> Self {
        Self {
            events: Box::new(events.into_iter().collect::<Vec<T>>()),
            event_type: TypeId::of::<T>(),
            raw: None,
            edited: false,
        }
    }

    /// For decoders: the events of the message `raw`.
    pub fn decoded<T: Event>(events: impl IntoIterator<Item = T>, raw: Bytes) -> Self {
        Self {
            raw: Some(raw),
            ..Self::new(events)
        }
    }

    /// The events; `Err` with [`codes::WRONG_KIND`](crate::codes::WRONG_KIND)
    /// if they are not `T`s.
    pub fn events<T: Event>(&self) -> Result<&[T], Fault> {
        let events = self.events.downcast_ref::<Vec<T>>();
        events.map(Vec::as_slice).ok_or_else(Fault::wrong_kind::<T>)
    }

    /// Run each event through `f` if they are `T`s, keeping what it passes
    /// on; marked edited if that differs.
    pub(crate) fn filter<T: Any + Clone + PartialEq>(
        &mut self,
        mut f: impl FnMut(T, &mut Vec<T>) -> Result<(), Fault>,
    ) -> Result<(), Fault> {
        let Some(events) = self.events.downcast_mut::<Vec<T>>() else {
            return Ok(());
        };
        let before = std::mem::take(events);
        for event in before.iter().cloned() {
            f(event, events)?;
        }
        self.edited |= *events != before;
        Ok(())
    }

    /// Add `more` events, if they are `T`s.
    pub(crate) fn extend<T: Any>(&mut self, more: Vec<T>) {
        if let Some(events) = self.events.downcast_mut::<Vec<T>>() {
            events.extend(more);
        }
    }

    /// The bytes it was decoded from, kept after an edit so the format can
    /// write the edited events in their shape.
    pub fn raw(&self) -> Option<&Bytes> {
        self.raw.as_ref()
    }

    /// The bytes it was decoded from, if no edit changed it.
    pub fn unedited(&self) -> Option<&Bytes> {
        self.raw.as_ref().filter(|_| !self.edited)
    }

    /// The type of the events.
    pub(crate) fn event_type(&self) -> TypeId {
        self.event_type
    }

    /// Drop the provider's bytes, before another format writes it.
    pub(crate) fn forget_raw(&mut self) {
        self.raw = None;
    }
}

impl From<Item> for Part {
    fn from(item: Item) -> Self {
        Part::Item(item)
    }
}

impl From<Usage> for Part {
    fn from(usage: Usage) -> Self {
        Part::Usage(usage)
    }
}

/// A whole message body, JSON or not. It is parsed on first read and keeps
/// its bytes until changed through [`Doc::json_mut`], then is written from
/// its value. Copies share the value until one changes.
#[derive(Debug, Clone)]
pub struct Doc {
    raw: Bytes,
    parsed: Arc<OnceLock<Result<Value, Invalid>>>,
    /// The value it is written from, once changed or built from one.
    changed: Option<Arc<Value>>,
}

/// Bytes, such as a message as it arrived, parsed as JSON on first read.
impl From<Bytes> for Doc {
    fn from(raw: Bytes) -> Self {
        Self {
            raw,
            parsed: Arc::default(),
            changed: None,
        }
    }
}

/// No bytes.
impl Default for Doc {
    fn default() -> Self {
        Doc::from(Bytes::new())
    }
}

/// A JSON message made anew, written from its value.
impl From<Value> for Doc {
    fn from(value: Value) -> Self {
        Self {
            raw: Bytes::new(),
            parsed: Arc::default(),
            changed: Some(Arc::new(value)),
        }
    }
}

/// Why bytes are not JSON a [`Doc`] can hold.
#[derive(Debug)]
enum Invalid {
    NotJson,
    /// Nested 128 levels deep or more.
    TooDeep,
}

impl Doc {
    /// The message as JSON; `None` if it is not JSON or nests 128 levels
    /// deep or more. [`Gateway::handle`](crate::Gateway::handle) refuses a
    /// request whose model is read from one that deep.
    pub fn json(&self) -> Option<&Value> {
        match &self.changed {
            Some(value) => Some(value),
            None => self.parsed().as_ref().ok(),
        }
    }

    /// The message as JSON, to change in place; `None` if it is not JSON.
    pub fn json_mut(&mut self) -> Option<&mut Value> {
        if self.changed.is_none() {
            self.changed = Some(Arc::new(self.parsed().as_ref().ok()?.clone()));
        }
        self.changed.as_mut().map(Arc::make_mut)
    }

    /// Whether it was read as JSON and found nested too deeply.
    pub(crate) fn too_deep(&self) -> bool {
        matches!(self.parsed.get(), Some(Err(Invalid::TooDeep)))
    }

    /// The bytes it arrived as, or those of its value once changed.
    pub fn to_bytes(&self) -> Bytes {
        match &self.changed {
            Some(value) => Bytes::from(serde_json::to_vec(&**value).unwrap_or_default()),
            None => self.raw.clone(),
        }
    }

    fn parsed(&self) -> &Result<Value, Invalid> {
        self.parsed.get_or_init(|| {
            serde_json::from_slice(&self.raw).map_err(|e| {
                match e.to_string().starts_with("recursion limit exceeded") {
                    true => Invalid::TooDeep,
                    false => Invalid::NotJson,
                }
            })
        })
    }
}

/// An open map of usage units to additive amounts. `f64` is exact for
/// counts such as tokens; money belongs in a decimal type.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Usage(BTreeMap<Cow<'static, str>, f64>);

impl Usage {
    /// Add `amount` of `unit`.
    pub fn add(&mut self, unit: impl Into<Cow<'static, str>>, amount: f64) {
        *self.0.entry(unit.into()).or_insert(0.0) += amount;
    }

    /// The amount of `unit`; 0 if none was reported.
    pub fn get(&self, unit: &str) -> f64 {
        self.0.get(unit).copied().unwrap_or(0.0)
    }

    /// Whether no usage was reported.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Every unit and its amount.
    pub fn iter(&self) -> impl Iterator<Item = (Cow<'static, str>, f64)> + '_ {
        self.0.iter().map(|(k, v)| (k.clone(), *v))
    }
}

impl std::ops::AddAssign<&Usage> for Usage {
    fn add_assign(&mut self, other: &Usage) {
        for (unit, amount) in other.iter() {
            self.add(unit, amount);
        }
    }
}

impl std::ops::AddAssign for Usage {
    fn add_assign(&mut self, other: Usage) {
        *self += &other;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unedited_doc_keeps_its_bytes() {
        let raw = Bytes::from_static(b"{\"b\": 1,  \"a\": 2}");
        let doc = Doc::from(raw.clone());
        assert_eq!(doc.json().and_then(|v| v["a"].as_i64()), Some(2));
        assert_eq!(doc.to_bytes(), raw);
    }

    #[test]
    fn a_doc_changed_in_place_leaves_its_copies_alone() {
        let raw = Bytes::from_static(b"{\"a\": 1}");
        let original = Doc::from(raw.clone());
        let mut changed = original.clone();
        if let Some(v) = changed.json_mut() {
            v["a"] = 2.into();
        }
        assert_eq!(changed.to_bytes(), Bytes::from_static(b"{\"a\":2}"));
        assert_eq!(original.to_bytes(), raw);
    }

    #[test]
    fn json_nested_too_deeply_is_told_from_bytes_that_are_not_json() {
        let nested = |n| Doc::from(Bytes::from(format!("{}{}", "[".repeat(n), "]".repeat(n))));
        let (shallow, deep, broken) = (nested(127), nested(128), Doc::from(Bytes::from("[")));
        assert!(shallow.json().is_some() && !shallow.too_deep());
        assert!(deep.json().is_none() && deep.too_deep());
        assert!(broken.json().is_none() && !broken.too_deep());
    }

    #[test]
    fn a_doc_made_from_a_value_keeps_its_key_order() {
        let doc = Doc::from(serde_json::json!({ "b": 1, "a": 2 }));
        assert_eq!(doc.to_bytes(), Bytes::from_static(b"{\"b\":1,\"a\":2}"));
    }

    #[test]
    fn an_item_is_read_by_its_type_and_only_changes_are_edits() {
        let mut item = Item::decoded(vec![String::from("hi")], Bytes::from_static(b"x"));
        assert!(item.events::<u32>().is_err());
        let edit = |item: &mut Item, f: fn(&mut String)| {
            item.filter(|mut s: String, out| {
                f(&mut s);
                out.push(s);
                Ok(())
            })
        };
        let _ = item.filter(|n: u32, out| {
            out.push(n + 1);
            Ok(())
        });
        let _ = edit(&mut item, |s| s.truncate(2));
        assert!(item.unedited().is_some());
        let _ = edit(&mut item, |s| s.push('!'));
        assert!(item.unedited().is_none());
        assert_eq!(item.events::<String>().ok(), Some(&["hi!".to_owned()][..]));
        assert_eq!(item.raw(), Some(&Bytes::from_static(b"x")));
    }
}
