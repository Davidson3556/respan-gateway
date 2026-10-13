//! A request's live inputs after its head: read once through the input
//! filters, and given back if no reader read any.

use std::collections::VecDeque;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use futures::channel::oneshot;
use futures::stream;
use futures::{FutureExt, StreamExt};

use crate::fault::{PANICKED, guarded};
use crate::layers::{Layer, cut, end_layers, locked, on_close, run};
use crate::lifecycle::Latch;
use crate::values::{AnyEvents, Value};
use crate::{Fault, Result};

/// The inputs after the head's, if any follow: read once, and their input
/// filters. A plugin's [`wrap`](crate::ClientContext::wrap) puts a new one
/// in front, whose events wrap the one `before` it.
pub(crate) struct Later {
    events: Mutex<Option<AnyEvents>>,
    /// Whether a reader read an input.
    read: AtomicBool,
    pub(crate) filters: Layer,
    before: Option<Arc<Later>>,
}

impl Later {
    pub(crate) fn new(events: Option<AnyEvents>) -> Self {
        Self {
            events: Mutex::new(events),
            read: AtomicBool::new(false),
            filters: Layer::default(),
            before: None,
        }
    }

    /// `events` in front of `before`, whose inputs they wrap.
    pub(crate) fn wrapping(events: AnyEvents, before: Arc<Later>) -> Self {
        Self {
            before: Some(before),
            ..Self::new(Some(events))
        }
    }

    /// The request ended: its filters end `end`, if they have not, and the
    /// events go, so nothing that outlives it keeps the client's connection;
    /// then those it wraps, so a reader sees the cut here first.
    pub(crate) fn close(&self, end: Result<()>) {
        let _ = locked(&self.filters).end(end.clone(), &mut None);
        locked(&self.events).take();
        if let Some(before) = &self.before {
            before.close(end);
        }
    }
}

/// `later`'s inputs, one per item, through its input filters, then those of
/// `call`, which is told the first failure; cut, when `until` closes or
/// either layer ended, with `Err(abandoned)`. Taken on the first poll; given
/// back if none was read.
pub(crate) fn inputs(
    later: Arc<Later>,
    call: Option<(Layer, oneshot::Sender<Fault>)>,
    until: &Latch,
) -> AnyEvents {
    let (call, failed) = call.unzip();
    let mut layers = vec![later.filters.clone()];
    layers.extend(call);
    let reading = Reading {
        later,
        events: None,
        layers,
        ready: VecDeque::new(),
        ended: false,
        failed,
    };
    let items = stream::unfold(
        reading,
        |mut r| async move { r.next().await.map(|i| (i, r)) },
    );
    cut(items.boxed(), on_close(until))
}

struct Reading {
    later: Arc<Later>,
    /// Taken from `later` on the first poll.
    events: Option<AnyEvents>,
    layers: Vec<Layer>,
    ready: VecDeque<Result<Value>>,
    ended: bool,
    /// Told the first failure, which ends the call that reads them.
    failed: Option<oneshot::Sender<Fault>>,
}

impl Reading {
    async fn next(&mut self) -> Option<Result<Value>> {
        loop {
            if let Some(item) = self.ready.pop_front() {
                if let Err(fault) = &item
                    && let Some(failed) = self.failed.take()
                {
                    let _ = failed.send(fault.clone());
                }
                return Some(item);
            }
            if self.ended {
                return None;
            }
            let events = match &mut self.events {
                Some(events) => events,
                None => match locked(&self.later.events).take() {
                    Some(events) => self.events.insert(events),
                    None => {
                        // No events: they failed, as when the request's end
                        // cut them; they were none, and ended; or another
                        // reader took them, or read them, which this one
                        // cannot send again.
                        self.ended = true;
                        let read = self.later.read.load(Ordering::SeqCst);
                        let fault = match locked(&self.later.filters).outcome() {
                            Some(Err(fault)) => fault,
                            Some(Ok(())) if !read => return None,
                            _ => Fault::internal("another reader took the request's inputs"),
                        };
                        self.ready.push_back(Err(fault));
                        continue;
                    }
                },
            };
            let item = AssertUnwindSafe(events.next()).catch_unwind().await;
            let item = item.unwrap_or_else(|_| Some(Err(Fault::internal(PANICKED))));
            if matches!(item, Some(Ok(_))) {
                self.later.read.store(true, Ordering::SeqCst);
            }
            // A failure ends the inputs, and every filter with it.
            let step = guarded(|| match item {
                Some(Ok(values)) => self.filter(values).or_else(|f| self.end(Err(f))),
                Some(Err(fault)) => self.end(Err(fault)),
                None => self.end(Ok(())),
            });
            if let Err(fault) = step {
                self.ended = true;
                self.ready.push_back(Err(fault));
            }
        }
    }

    /// Each input through the layers, as it is ready; once a layer ended,
    /// they are cut.
    fn filter(&mut self, input: Value) -> Result<()> {
        let layers: Vec<&Layer> = self.layers.iter().collect();
        if layers.iter().any(|l| locked(l).ended()) {
            return Err(Fault::abandoned());
        }
        let mut values = input.batch();
        run(&layers, &mut values)?;
        self.ready.extend(values.split().into_iter().map(Ok));
        Ok(())
    }

    /// The inputs end `end`: each layer once, `later`'s first, and what
    /// they kept back follows, unless the end is a failure.
    fn end(&mut self, end: Result<()>) -> Result<()> {
        self.ended = true;
        let mut tail = None;
        let layers: Vec<&Layer> = self.layers.iter().collect();
        let end = end_layers(&layers, end, &mut tail);
        if end.is_ok() {
            let made = tail.map(|v| v.split()).unwrap_or_default();
            self.ready.extend(made.into_iter().map(Ok));
        }
        end
    }
}

/// A reader that read no input of inputs still open gives them back, so
/// another, such as a retry, reads every input.
impl Drop for Reading {
    fn drop(&mut self) {
        let live = !locked(&self.later.filters).ended();
        let read = self.later.read.load(Ordering::SeqCst);
        if let Some(events) = self.events.take().filter(|_| !read && live) {
            *locked(&self.later.events) = Some(events);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::Abandoned;

    use futures::StreamExt;

    use super::*;

    /// Inputs cut by the request's end end `Err(abandoned)`, however
    /// the end and the first poll are ordered: here the request has ended
    /// but its latch is not yet closed, as when the end lands in the middle
    /// of a poll that already saw it open.
    #[tokio::test]
    async fn a_cut_before_the_first_poll_is_never_a_clean_end() {
        let ended = Latch::default();
        let later = Arc::new(Later::new(Some(stream::pending().boxed())));
        later.close(Err(Fault::abandoned()));
        let mut inputs = inputs(later, None, &ended);
        let first = inputs.next().await.map(|r| r.err());
        assert!(matches!(first, Some(Some(f)) if f.is::<Abandoned>()));
        assert!(inputs.next().await.is_none());
    }
}
