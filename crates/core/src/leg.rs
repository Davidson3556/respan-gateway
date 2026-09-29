//! The layer engine: a stream pulled through filter layers, each ended once
//! with what it kept back flowing on through the layers after it; a cut
//! gives `Err(abandoned)`, never a clean end; every poll of an edge's stream
//! is guarded. A session's inputs or a call's own to the call, a provider's events to a
//! [`Response`](crate::Response), and a response to the client run on it.

use std::collections::VecDeque;
use std::future::ready;
use std::panic::AssertUnwindSafe;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use futures::stream::{self, BoxStream};
use futures::{FutureExt, StreamExt};

use crate::Fault;
use crate::fault::{PANICKED, guarded};
use crate::format::{AnyEvents, AnyFormat, AnyInput, AnyInputs, Values};
use crate::lifecycle::InFlight;
use crate::plugin::Filters;
use crate::response::{Batch, Source};

/// One stage's filters of one direction, shared by whoever runs or ends it.
pub(crate) type Layer = Arc<Mutex<Filters>>;

/// Lock `m`, even if poisoned: what it guards stays consistent.
pub(crate) fn locked<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Run `values` through `layers` in order, until the first failure; `values`
/// keeps what passed before it.
pub(crate) fn run(layers: &[&Layer], values: &mut Values) -> Result<(), Fault> {
    layers
        .iter()
        .try_for_each(|layer| locked(layer).run(values))
}

/// End each of `layers` once, in order, with `end`: what one keeps back
/// runs through those after it before they end, and joins `tail`. Returns
/// the end.
pub(crate) fn end(
    layers: &[&Layer],
    mut end: Result<(), Fault>,
    tail: &mut Option<Values>,
) -> Result<(), Fault> {
    for (i, layer) in layers.iter().enumerate() {
        end = locked(layer).end(end, tail);
        if let (Some(values), Some(next)) = (tail.as_mut(), layers.get(i + 1))
            && let Err(fault) = locked(next).run(values)
        {
            *tail = None;
            end = Err(fault);
        }
    }
    end
}

/// `items` until `cut` is idle, then `Err(abandoned)`: only their own end
/// is a clean one.
pub(crate) fn cut<T: Send + 'static>(
    items: BoxStream<'static, Result<T, Fault>>,
    cut: &Arc<InFlight>,
) -> BoxStream<'static, Result<T, Fault>> {
    // `None` marks the items' own end, which the cut must not pass for.
    let items = items.map(Some).chain(stream::once(ready(None)));
    let cut = items.take_until(cut.idle()).boxed();
    stream::unfold(Some(cut), |cut| async move {
        let mut cut = cut?;
        match cut.next().await {
            Some(Some(item)) => Some((item, Some(cut))),
            Some(None) => None,
            None => Some((Err(Fault::abandoned()), None)),
        }
    })
    .boxed()
}

/// The inputs after an opening, if any follow, a session's or a call's own:
/// read once, and their first input filters (the client edge's, for a
/// session's).
pub(crate) struct Later {
    events: Mutex<Option<AnyEvents>>,
    /// Whether none follow the opening.
    pub(crate) ended: bool,
    pub(crate) filters: Layer,
}

impl Later {
    pub(crate) fn new(events: Option<AnyEvents>) -> Self {
        Self {
            ended: events.is_none(),
            events: Mutex::new(events),
            filters: Layer::default(),
        }
    }

    /// The request ended: its filters end `end`, if they have not, and the
    /// events go, so nothing that outlives it keeps the client's connection.
    pub(crate) fn end(&self, end: Result<(), Fault>) {
        locked(&self.events).take();
        let _ = locked(&self.filters).end(end, &mut None);
    }
}

/// `later`'s inputs through its input filters, then `call`'s;
/// cut, when `until` is
/// idle or either layer ended, with `Err(abandoned)`. Taken on the first
/// poll; given back if none was read.
pub(crate) fn inputs(
    later: Arc<Later>,
    format: Arc<dyn AnyFormat>,
    call: Option<Layer>,
    until: &Arc<InFlight>,
) -> AnyInputs {
    let mut layers = vec![later.filters.clone()];
    layers.extend(call);
    let reading = Reading {
        later,
        events: None,
        read: false,
        format,
        layers,
        ready: VecDeque::new(),
        ended: false,
    };
    let items = stream::unfold(
        reading,
        |mut r| async move { r.next().await.map(|i| (i, r)) },
    );
    cut(items.boxed(), until)
}

struct Reading {
    later: Arc<Later>,
    /// Taken from `later` on the first poll.
    events: Option<AnyEvents>,
    /// Whether it read an input.
    read: bool,
    format: Arc<dyn AnyFormat>,
    layers: Vec<Layer>,
    ready: VecDeque<Result<AnyInput, Fault>>,
    ended: bool,
}

impl Reading {
    async fn next(&mut self) -> Option<Result<AnyInput, Fault>> {
        loop {
            if let Some(item) = self.ready.pop_front() {
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
                        self.ended = true;
                        // Filters ended and no events: the inputs ended as the
                        // filters did, a cut by the request's end too.
                        return match locked(&self.later.filters).outcome() {
                            Some(Ok(())) => None,
                            Some(Err(fault)) => Some(Err(fault)),
                            None => {
                                let taken = "the request's inputs were taken by another reader";
                                Some(Err(Fault::internal(taken)))
                            }
                        };
                    }
                },
            };
            let item = AssertUnwindSafe(events.next()).catch_unwind().await;
            let item = item.unwrap_or_else(|_| Some(Err(Fault::internal(PANICKED))));
            self.read |= matches!(item, Some(Ok(_)));
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
    fn filter(&mut self, values: Values) -> Result<(), Fault> {
        let layers: Vec<&Layer> = self.layers.iter().collect();
        for input in self.format.inputs(values) {
            if layers.iter().any(|l| locked(l).ended()) {
                return Err(Fault::abandoned());
            }
            let mut values = self.format.values(vec![input]);
            run(&layers, &mut values)?;
            self.ready
                .extend(self.format.inputs(values).into_iter().map(Ok));
        }
        Ok(())
    }

    /// The inputs end `end`: each layer once, `later`'s first, and what
    /// they kept back follows, unless the end is a failure.
    fn end(&mut self, end: Result<(), Fault>) -> Result<(), Fault> {
        self.ended = true;
        let mut tail = None;
        let layers: Vec<&Layer> = self.layers.iter().collect();
        let end = self::end(&layers, end, &mut tail);
        if end.is_ok() {
            let made = tail.map(|v| self.format.inputs(v)).unwrap_or_default();
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
        if let Some(events) = self.events.take().filter(|_| !self.read && live) {
            *locked(&self.later.events) = Some(events);
        }
    }
}

/// A response on its way to the client: what was read ahead, then its
/// source, each batch read (its call ends at its end) and run through the
/// client's output filters, which end with it.
pub(crate) struct Outgoing {
    ahead: VecDeque<Batch>,
    source: Source,
    layer: Layer,
    /// Whether it relays, so a provider's words go with a failure.
    relayed: bool,
    ready: VecDeque<Result<Values, Fault>>,
    done: bool,
}

impl Outgoing {
    pub(crate) fn new(ahead: VecDeque<Batch>, source: Source, layer: Layer, relayed: bool) -> Self {
        let (ready, done) = (VecDeque::new(), false);
        Self {
            ahead,
            source,
            layer,
            relayed,
            ready,
            done,
        }
    }

    pub(crate) fn events(self) -> AnyEvents {
        stream::unfold(self, |mut o| async move { o.next().await.map(|v| (v, o)) }).boxed()
    }

    async fn next(&mut self) -> Option<Result<Values, Fault>> {
        loop {
            if let Some(item) = self.ready.pop_front() {
                return Some(item);
            }
            if self.done {
                return None;
            }
            // A panic reading or filtering ends the call and the reply.
            if AssertUnwindSafe(self.step()).catch_unwind().await.is_err() {
                let fault = Fault::internal(PANICKED);
                self.source.end(Err(fault.clone()));
                let _ = locked(&self.layer).end(Err(fault.clone()), &mut None);
                self.ready.push_back(Err(fault));
                self.done = true;
            }
        }
    }

    /// The next batch, through the client's output filters; at the end, what
    /// they kept back and the end, a failure unrelayed unless it relays.
    async fn step(&mut self) {
        let mut batch = match self.ahead.pop_front() {
            Some(batch) => batch,
            None => self.source.next().await.0,
        };
        batch.read();
        let mut end = batch.end.take();
        if let Some(mut values) = batch.values.take() {
            if let Err(fault) = run(&[&self.layer], &mut values) {
                end = Some(Err(fault));
            }
            self.ready.push_back(Ok(values));
        }
        let Some(end) = end else { return };
        self.done = true;
        self.source.end(end.clone());
        // Filters see every end, a failure too.
        let mut tail = None;
        let end = self::end(&[&self.layer], end, &mut tail);
        self.ready.extend(tail.map(Ok));
        if let Err(fault) = end {
            // A provider's words go only where its head would.
            let fault = match self.relayed {
                true => fault,
                false => fault.unrelayed(),
            };
            self.ready.push_back(Err(fault));
        }
    }
}

/// Dropped before its end, unread: its call ends as its end, if it is
/// ready, says, else abandoned as its source goes (rule 4).
impl Drop for Outgoing {
    fn drop(&mut self) {
        if self.done {
            return;
        }
        let ahead = self.ahead.iter_mut().find(|b| b.end.is_some());
        let mut batch = match ahead {
            Some(batch) => std::mem::take(batch),
            None => {
                let next = AssertUnwindSafe(|| self.source.next().now_or_never());
                match std::panic::catch_unwind(next) {
                    Ok(Some((batch, _))) => batch,
                    _ => return,
                }
            }
        };
        let _ = std::panic::catch_unwind(AssertUnwindSafe(|| batch.read()));
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use futures::StreamExt;

    use super::*;
    use crate::format::Erased;
    use futures::future::BoxFuture;

    use crate::{Events, Extensions, Format, Half, Origin};

    struct Nil;

    impl Format for Nil {
        type Input = String;
        type Output = String;
        type Wire = ();
        type Dialer = ();

        fn name(&self) -> &str {
            "nil"
        }

        fn dial(
            &self,
            _: &(),
            _: Extensions,
            _: Option<&Origin>,
            _: Option<&String>,
            _: Option<Events<String>>,
        ) -> Result<BoxFuture<'static, Result<Half<String>, Fault>>, Fault> {
            Err(Fault::internal("unused"))
        }
    }

    /// B1: inputs cut by the request's end end `Err(abandoned)`, however
    /// the end and the first poll are ordered: here the request has ended
    /// but its cut is not yet idle, as when the end lands in the middle of
    /// a poll that already saw the cut open.
    #[tokio::test]
    async fn a_cut_before_the_first_poll_is_never_a_clean_end() {
        let cut = Arc::<InFlight>::default();
        let _running = InFlight::enter(&cut);
        let later = Arc::new(Later::new(Some(stream::pending().boxed())));
        later.end(Err(Fault::abandoned()));
        let format = Arc::new(Erased::new(Nil));
        let mut inputs = inputs(later, format, None, &cut);
        let first = inputs
            .next()
            .await
            .map(|r| r.err().map(|f| f.code().to_string()));
        assert_eq!(first, Some(Some("abandoned".to_string())));
        assert!(inputs.next().await.is_none());
    }
}
