//! The layer engine's shared pieces: filter layers run and ended in order,
//! and a stream cut when its request or call ends. A cut gives
//! `Err(abandoned)`, never a clean end. Live inputs ([`crate::live_inputs`])
//! and the response to the client ([`crate::client_output`]) run on it.

use parking_lot::Mutex;
use std::future::ready;
use std::sync::Arc;

use futures::future::BoxFuture;
use futures::stream::{self, BoxStream};
use futures::{FutureExt, StreamExt};

use crate::lifecycle::Latch;
use crate::plugin::Filters;
use crate::values::Values;
use crate::{Fault, Result};

/// One stage's filters of one direction, shared by whoever runs or ends it.
pub(crate) type Layer = Arc<Mutex<Filters>>;

/// Run `values` through `layers` in order, until the first failure; `values`
/// keeps what passed before it.
pub(crate) fn run(layers: &[&Layer], values: &mut Values) -> Result<()> {
    layers.iter().try_for_each(|layer| layer.lock().run(values))
}

/// End each of `layers` once, in order, with `end`: what one keeps back
/// runs through those after it before they end, and joins `tail`. Returns
/// the end.
pub(crate) fn end_layers(
    layers: &[&Layer],
    mut end: Result<()>,
    tail: &mut Option<Values>,
) -> Result<()> {
    for (i, layer) in layers.iter().enumerate() {
        end = layer.lock().end(end, tail);
        if let (Some(values), Some(next)) = (tail.as_mut(), layers.get(i + 1))
            && let Err(fault) = next.lock().run(values)
        {
            *tail = None;
            end = Err(fault);
        }
    }
    end
}

/// `items` until `until` resolves, then `Err` with the fault it gives: only
/// their own end is a clean one, and not if `until` is ready with it.
pub(crate) fn cut<T: Send + 'static>(
    items: BoxStream<'static, Result<T>>,
    until: BoxFuture<'static, Fault>,
) -> BoxStream<'static, Result<T>> {
    // `None` marks the items' own end, which the cut must not pass for.
    let items = items.map(Some).chain(stream::once(ready(None)));
    let cut = items.take_until(until);
    stream::unfold(Some(cut), |cut| async move {
        let mut cut = cut?;
        match cut.next().await {
            Some(Some(item)) => Some((item, Some(cut))),
            // An end that a cut caused, such as a plugin's stream ending as
            // the stream it wraps was cut, is the cut's.
            Some(None) => (cut.take_future()?.now_or_never()).map(|f| (Err(f), None)),
            None => Some((
                Err(cut.take_result().unwrap_or_else(Fault::abandoned)),
                None,
            )),
        }
    })
    .boxed()
}

/// Resolves [`Abandoned`](crate::FaultKind::Abandoned) once `ended`
/// closes, as when its request or call ended.
pub(crate) fn on_close(ended: &Latch) -> BoxFuture<'static, Fault> {
    ended.closed().map(|()| Fault::abandoned()).boxed()
}

/// Yield one poll after a batch is safely retained. Wake synchronously so a
/// bounded caller that probes immediately ready events can distinguish this
/// cooperative pause from a genuinely pending source.
pub(crate) async fn yield_ready() {
    let mut yielded = false;
    futures::future::poll_fn(move |cx| {
        if std::mem::replace(&mut yielded, true) {
            std::task::Poll::Ready(())
        } else {
            cx.waker().wake_by_ref();
            std::task::Poll::Pending
        }
    })
    .await
}
