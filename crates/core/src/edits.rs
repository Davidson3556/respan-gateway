use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use futures::StreamExt;
use futures::stream::{self, BoxStream};
use tokio::sync::mpsc;

use crate::{End, Fault, Part, ResponseHead};

pub(crate) type PartStream = BoxStream<'static, Part>;

type MapFn = Box<dyn FnMut(&mut Part) + Send>;
type HeadFn = Box<dyn FnMut(&mut ResponseHead) + Send>;
pub(crate) type TapFn = Box<dyn FnMut(&Part) + Send>;

/// A stateful stream edit: read Parts from `input`, write Parts to `output`.
///
/// Returning without sending an `End` is safe: the kernel forwards the
/// upstream `End`. Returning `Err` ends the stream as failed.
#[async_trait]
pub trait Filter: Send + 'static {
    async fn run(self: Box<Self>, input: PartRx, output: PartTx) -> Result<(), Fault>;
}

#[derive(Debug)]
pub struct PartRx(mpsc::Receiver<Part>);

impl PartRx {
    pub async fn recv(&mut self) -> Option<Part> {
        self.0.recv().await
    }
}

#[derive(Debug)]
pub struct PartTx {
    tx: mpsc::Sender<Part>,
    sent_end: Arc<Mutex<bool>>,
}

impl PartTx {
    /// Returns false once the downstream is gone.
    pub async fn send(&self, part: Part) -> bool {
        if matches!(part, Part::End(_)) {
            *lock(&self.sent_end) = true;
        }
        self.tx.send(part).await.is_ok()
    }
}

pub(crate) enum Edit {
    Head(HeadFn),
    Map(MapFn),
    Tap(TapFn),
    Filter(Box<dyn Filter>),
}

/// The edits registered on one direction of a stream.
#[derive(Default)]
pub struct Edits(Mutex<Vec<Edit>>);

impl std::fmt::Debug for Edits {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Edits")
            .field("count", &lock(&self.0).len())
            .finish()
    }
}

impl Edits {
    /// Edit the response head. Never forces the body to be decoded.
    pub fn head(&self, f: impl FnMut(&mut ResponseHead) + Send + 'static) {
        lock(&self.0).push(Edit::Head(Box::new(f)));
    }

    /// Read every Part without changing anything. Unlike `map`, a tap never
    /// forces a relayed response to be re-encoded.
    pub fn tap(&self, f: impl FnMut(&Part) + Send + 'static) {
        lock(&self.0).push(Edit::Tap(Box::new(f)));
    }

    /// Stateless edit of every Part, in order.
    pub fn map(&self, f: impl FnMut(&mut Part) + Send + 'static) {
        lock(&self.0).push(Edit::Map(Box::new(f)));
    }

    /// Stateful edit over the whole stream.
    pub fn filter(&self, f: impl Filter) {
        lock(&self.0).push(Edit::Filter(Box::new(f)));
    }

    pub(crate) fn take(&self) -> Vec<Edit> {
        std::mem::take(&mut *lock(&self.0))
    }
}

pub(crate) fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Edits that change the body, so the response must be decoded and re-encoded.
pub(crate) fn changes_body(e: &Edit) -> bool {
    matches!(e, Edit::Map(_) | Edit::Filter(_))
}

pub(crate) fn taps(edits: Vec<Edit>) -> Vec<TapFn> {
    edits
        .into_iter()
        .filter_map(|e| match e {
            Edit::Tap(f) => Some(f),
            _ => None,
        })
        .collect()
}

pub(crate) fn apply_head(edits: &mut [Edit], head: &mut ResponseHead) {
    for e in edits {
        if let Edit::Head(f) = e {
            f(head);
        }
    }
}

/// Apply body edits, in order, to a Part stream.
pub(crate) fn apply(mut parts: PartStream, edits: Vec<Edit>) -> PartStream {
    for edit in edits {
        parts = match edit {
            Edit::Head(_) => parts,
            Edit::Map(mut f) => parts
                .map(move |mut p| {
                    f(&mut p);
                    p
                })
                .boxed(),
            Edit::Tap(mut f) => parts
                .map(move |p| {
                    f(&p);
                    p
                })
                .boxed(),
            Edit::Filter(filter) => run_filter(parts, filter),
        };
    }
    parts
}

/// Apply map edits to a single Part, for request bodies. Filters are not
/// supported on buffered request bodies yet.
pub(crate) fn apply_once(part: &mut Part, edits: Vec<Edit>) -> Result<(), Fault> {
    for edit in edits {
        match edit {
            Edit::Map(mut f) => f(part),
            Edit::Tap(mut f) => f(part),
            Edit::Head(_) => {}
            Edit::Filter(_) => {
                return Err(Fault::config(
                    "filters on request bodies are not supported yet",
                ));
            }
        }
    }
    Ok(())
}

fn run_filter(mut upstream: PartStream, filter: Box<dyn Filter>) -> PartStream {
    let (in_tx, in_rx) = mpsc::channel(64);
    let (out_tx, out_rx) = mpsc::channel(64);
    let upstream_end = Arc::new(Mutex::new(None::<End>));
    let sent_end = Arc::new(Mutex::new(false));

    let pump_end = upstream_end.clone();
    tokio::spawn(async move {
        while let Some(p) = upstream.next().await {
            if let Part::End(e) = &p {
                *lock(&pump_end) = Some(e.clone());
            }
            if in_tx.send(p).await.is_err() {
                break;
            }
        }
    });

    let tx = PartTx {
        tx: out_tx.clone(),
        sent_end: sent_end.clone(),
    };
    tokio::spawn(async move {
        let result = filter.run(PartRx(in_rx), tx).await;
        if *lock(&sent_end) {
            return;
        }
        let end = match result {
            Err(f) => End::Failed(f),
            Ok(()) => lock(&upstream_end)
                .clone()
                .unwrap_or(End::Failed(Fault::truncated())),
        };
        let _ = out_tx.send(Part::End(end)).await;
    });

    stream::unfold(
        out_rx,
        |mut rx| async move { rx.recv().await.map(|p| (p, rx)) },
    )
    .boxed()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Delta, DeltaKind};

    fn text(p: &Part) -> Option<&str> {
        match p {
            Part::Delta(d) => Some(d.text()),
            _ => None,
        }
    }

    fn source() -> PartStream {
        stream::iter(vec![
            Part::Delta(Delta::new(0, DeltaKind::Text, "a")),
            Part::Delta(Delta::new(0, DeltaKind::Text, "b")),
            Part::End(End::Done),
        ])
        .boxed()
    }

    struct Joiner;

    #[async_trait]
    impl Filter for Joiner {
        async fn run(self: Box<Self>, mut input: PartRx, output: PartTx) -> Result<(), Fault> {
            let mut all = String::new();
            while let Some(p) = input.recv().await {
                match p {
                    Part::Delta(d) => all.push_str(d.text()),
                    Part::End(_) => break,
                    other => {
                        output.send(other).await;
                    }
                }
            }
            output
                .send(Part::Delta(Delta::new(0, DeltaKind::Text, all)))
                .await;
            Ok(())
        }
    }

    #[tokio::test]
    async fn maps_then_filters_in_order_and_end_is_forwarded() {
        let edits = Edits::default();
        edits.map(|p| {
            if let Part::Delta(d) = p {
                *d.text_mut() = d.text().to_uppercase();
            }
        });
        edits.filter(Joiner);
        let out: Vec<Part> = apply(source(), edits.take()).collect().await;
        assert_eq!(text(&out[0]), Some("AB"));
        assert!(matches!(out[1], Part::End(End::Done)));
        assert_eq!(out.len(), 2);
    }
}
