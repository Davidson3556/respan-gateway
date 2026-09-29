//! The layer engine: a stream pulled through filter layers, each ended once
//! with what it kept back flowing on through the layers after it; a cut
//! gives `Err(abandoned)`, never a clean end; every poll of an edge's stream
//! is guarded. Session inputs to a call, a provider's events to a
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

/// A session's inputs after its opening: read once, and the session's
/// input filters.
pub(crate) struct Later {
    events: Mutex<Option<AnyEvents>>,
    /// Whether they had ended when the session opened (rule 1).
    pub(crate) ended: bool,
    pub(crate) filters: Layer,
}

impl Later {
    /// `events`, polled once: ended at once, or kept with what came.
    pub(crate) fn new(mut events: AnyEvents) -> Self {
        let first = std::panic::catch_unwind(AssertUnwindSafe(|| events.next().now_or_never()));
        let (events, ended) = match first {
            Ok(Some(None)) => (None, true),
            Ok(Some(Some(item))) => (Some(stream::once(ready(item)).chain(events).boxed()), false),
            Ok(None) => (Some(events), false),
            Err(_) => (
                Some(stream::iter([Err(Fault::internal(PANICKED))]).boxed()),
                false,
            ),
        };
        Self {
            events: Mutex::new(events),
            ended,
            filters: Layer::default(),
        }
    }

    /// The session ended: its filters end `end`, if they have not, and the
    /// events go, so nothing that outlives it keeps the client's connection.
    pub(crate) fn end(&self, end: Result<(), Fault>) {
        locked(&self.events).take();
        let _ = locked(&self.filters).end(end, &mut None);
    }
}

/// `later`'s inputs through the session's input filters, then `call`'s,
/// each with whether it is the client's as decoded; cut, when `until` is
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
    ready: VecDeque<Result<(AnyInput, bool), Fault>>,
    ended: bool,
}

impl Reading {
    async fn next(&mut self) -> Option<Result<(AnyInput, bool), Fault>> {
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
                        // Filters ended and no events: the inputs ended.
                        if locked(&self.later.filters).ended() {
                            return None;
                        }
                        let taken = "the request's inputs were taken by another reader";
                        return Some(Err(Fault::internal(taken)));
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
    /// they are cut. One the layers left equal to its decoding is the
    /// client's.
    fn filter(&mut self, values: Values) -> Result<(), Fault> {
        let layers: Vec<&Layer> = self.layers.iter().collect();
        for input in self.format.inputs(values) {
            let (ended, empty) = layers.iter().fold((false, true), |(ended, empty), l| {
                let f = locked(l);
                (ended || f.ended(), empty && f.is_empty())
            });
            if ended {
                return Err(Fault::abandoned());
            }
            if empty {
                self.ready.push_back(Ok((input, true)));
                continue;
            }
            let mut values = self.format.values(vec![input.clone()]);
            run(&layers, &mut values)?;
            for changed in self.format.inputs(values) {
                let same = self.format.same(&input, &changed);
                self.ready.push_back(Ok((changed, same)));
            }
        }
        Ok(())
    }

    /// The inputs end `end`: each layer once, the session's first, and what
    /// they kept back follows, unless the end is a failure.
    fn end(&mut self, end: Result<(), Fault>) -> Result<(), Fault> {
        self.ended = true;
        let mut tail = None;
        let layers: Vec<&Layer> = self.layers.iter().collect();
        let end = self::end(&layers, end, &mut tail);
        if end.is_ok() {
            let made = tail.map(|v| self.format.inputs(v)).unwrap_or_default();
            let made = made.into_iter().map(|input| Ok((input, false)));
            self.ready.extend(made);
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
            // A panic reading or filtering ends the call and the answer.
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
/// ready, says, else abandoned as its source goes (rule 5).
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
