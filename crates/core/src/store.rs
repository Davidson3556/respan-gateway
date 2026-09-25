use std::collections::HashMap;
use std::fmt;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use serde_json::Value;

use crate::Fault;
use crate::edits::lock;

/// A typed store namespace. Declared as a constant by the crate that owns it,
/// so a typo is a compile error rather than a silent miss.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Class(&'static str);

impl Class {
    pub const fn new(name: &'static str) -> Self {
        Self(name)
    }

    pub fn name(&self) -> &'static str {
        self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Scope {
    Global,
    Tenant(String),
    Session(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Key {
    pub scope: Scope,
    pub class: Class,
    pub name: String,
}

impl Key {
    pub fn global(class: Class, name: impl Into<String>) -> Self {
        Self {
            scope: Scope::Global,
            class,
            name: name.into(),
        }
    }
}

impl fmt::Display for Key {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.scope {
            Scope::Global => write!(f, "g:{}:{}", self.class.0, self.name),
            Scope::Tenant(t) => write!(f, "t:{t}:{}:{}", self.class.0, self.name),
            Scope::Session(s) => write!(f, "s:{s}:{}:{}", self.class.0, self.name),
        }
    }
}

#[derive(Debug, Clone)]
pub enum Op {
    Get(Key),
    Put {
        key: Key,
        value: Value,
        ttl: Option<Duration>,
    },
    /// Add `by` to a number (missing counts as 0). If the result would fall
    /// outside `min..=max`, the whole batch is not applied.
    Add {
        key: Key,
        by: f64,
        min: Option<f64>,
        max: Option<f64>,
        ttl: Option<Duration>,
    },
    Del(Key),
}

impl Op {
    pub fn get(key: Key) -> Self {
        Op::Get(key)
    }

    pub fn put(key: Key, value: Value) -> Self {
        Op::Put {
            key,
            value,
            ttl: None,
        }
    }

    pub fn add(key: Key, by: f64) -> Self {
        Op::Add {
            key,
            by,
            min: None,
            max: None,
            ttl: None,
        }
    }

    pub fn del(key: Key) -> Self {
        Op::Del(key)
    }

    pub fn min(mut self, bound: f64) -> Self {
        if let Op::Add { min, .. } = &mut self {
            *min = Some(bound);
        }
        self
    }

    pub fn max(mut self, bound: f64) -> Self {
        if let Op::Add { max, .. } = &mut self {
            *max = Some(bound);
        }
        self
    }

    pub fn ttl(mut self, d: Duration) -> Self {
        match &mut self {
            Op::Put { ttl, .. } | Op::Add { ttl, .. } => *ttl = Some(d),
            _ => {}
        }
        self
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Applied {
    /// False when a bound in the batch would have been crossed; nothing was
    /// written.
    pub applied: bool,
    /// One value per op: the current value for Get, the new value for Add.
    pub values: Vec<Option<Value>>,
}

impl Applied {
    pub fn is_applied(&self) -> bool {
        self.applied
    }
}

/// Shared state. A batch is atomic: all ops apply or none do.
#[async_trait]
pub trait Store: Send + Sync + 'static {
    async fn apply(&self, ops: Vec<Op>) -> Result<Applied, Fault>;
}

/// Single-process store. The default when no backend is configured.
#[derive(Debug, Default)]
pub struct MemoryStore {
    entries: Mutex<HashMap<String, Entry>>,
}

#[derive(Debug, Clone)]
struct Entry {
    value: Value,
    expires: Option<Instant>,
}

impl MemoryStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl Store for MemoryStore {
    async fn apply(&self, ops: Vec<Op>) -> Result<Applied, Fault> {
        let now = Instant::now();
        let mut entries = lock(&self.entries);
        entries.retain(|_, e| e.expires.is_none_or(|t| t > now));

        let mut staged: HashMap<String, Option<Entry>> = HashMap::new();
        let mut values = Vec::with_capacity(ops.len());
        let current = |staged: &HashMap<String, Option<Entry>>, k: &str| match staged.get(k) {
            Some(e) => e.clone(),
            None => entries.get(k).cloned(),
        };

        for op in &ops {
            match op {
                Op::Get(key) => {
                    let k = key.to_string();
                    values.push(current(&staged, &k).map(|e| e.value));
                }
                Op::Put { key, value, ttl } => {
                    let e = Entry {
                        value: value.clone(),
                        expires: ttl.map(|d| now + d),
                    };
                    staged.insert(key.to_string(), Some(e));
                    values.push(None);
                }
                Op::Add {
                    key,
                    by,
                    min,
                    max,
                    ttl,
                } => {
                    let k = key.to_string();
                    let prev = current(&staged, &k);
                    let n = prev.as_ref().and_then(|e| e.value.as_f64()).unwrap_or(0.0) + by;
                    if min.is_some_and(|m| n < m) || max.is_some_and(|m| n > m) {
                        return Ok(Applied {
                            applied: false,
                            values: Vec::new(),
                        });
                    }
                    let expires = prev
                        .and_then(|e| e.expires)
                        .or_else(|| ttl.map(|d| now + d));
                    let value = serde_json::Number::from_f64(n)
                        .map(Value::Number)
                        .unwrap_or(Value::Null);
                    staged.insert(
                        k,
                        Some(Entry {
                            value: value.clone(),
                            expires,
                        }),
                    );
                    values.push(Some(value));
                }
                Op::Del(key) => {
                    staged.insert(key.to_string(), None);
                    values.push(None);
                }
            }
        }

        for (k, e) in staged {
            match e {
                Some(e) => {
                    entries.insert(k, e);
                }
                None => {
                    entries.remove(&k);
                }
            }
        }
        Ok(Applied {
            applied: true,
            values,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const LIMIT: Class = Class::new("limit");

    #[tokio::test]
    async fn add_respects_bounds_atomically() {
        let store = MemoryStore::new();
        let a = Key::global(LIMIT, "a");
        let b = Key::global(LIMIT, "b");
        let batch = || {
            vec![
                Op::add(a.clone(), 1.0).max(2.0),
                Op::add(b.clone(), 1.0).max(1.0),
            ]
        };
        assert!(
            store
                .apply(batch())
                .await
                .map(|r| r.applied)
                .unwrap_or(false)
        );
        assert!(
            !store
                .apply(batch())
                .await
                .map(|r| r.applied)
                .unwrap_or(true)
        );
        let got = store.apply(vec![Op::get(a)]).await.ok();
        assert_eq!(got.map(|r| r.values), Some(vec![Some(Value::from(1.0))]));
    }
}
