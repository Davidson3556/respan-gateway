//! Builds the outgoing flow for the application-facing edge.

use std::collections::VecDeque;
use std::panic::AssertUnwindSafe;
use std::sync::{Arc, Mutex};

use futures::future::ready;
use futures::{FutureExt, StreamExt, stream};

use crate::call::SharedCall;
use crate::fault::PANICKED;
use crate::format::AnyFormat;
use crate::leg::{ToClient, cut, on_close};
use crate::lifecycle::EndHooks;
use crate::response::{Batch, Feed};
use crate::values::Value;
use crate::{ClientContext, EventStream, Fault, Response, Result};

/// The client's response through its output stages, one value per item, and
/// the selected call, retained until delivery ends.
pub(crate) async fn outflow(
    format: &Arc<dyn AnyFormat>,
    resp: Result<Response>,
    hooks: &mut EndHooks<ClientContext>,
    discarded: &Arc<Mutex<Vec<Batch>>>,
) -> (EventStream<Value>, Option<SharedCall>) {
    let cx = hooks.cx();
    // A call's response goes only to its own request; one of another kind
    // fails, even with no outputs.
    let resp = resp.and_then(|resp| match resp.feed.call() {
        Some(c) if !cx.open.holds(c) => {
            Err(Fault::internal("a response to another request was sent"))
        }
        _ if resp.output != format.kind().1 => Err(Fault::another_kind(
            "a response of another kind was sent".into(),
        )),
        _ => Ok(resp),
    });
    let mut resp = match resp {
        Ok(resp) => resp,
        Err(fault) => return (EventStream::refused(fault), None),
    };
    if resp.is_refused() {
        // Its one batch is the refusal, through the call's filters.
        let read = AssertUnwindSafe(async {
            let mut batch = match resp.ahead.pop_front() {
                Some(batch) => batch,
                None => resp.feed.next().await,
            };
            batch.read();
            batch.ending.map(|ending| ending.outcome().clone())
        });
        let fault = match read.catch_unwind().await {
            Ok(Some(Err(fault))) => fault,
            Ok(_) => Fault::internal("a refusal that did not fail"),
            Err(_) => Fault::internal(PANICKED),
        };
        return (EventStream::refused(fault), None);
    }
    let call = resp.feed.call().cloned();
    let Response { feed, ahead, .. } = resp;
    let ended = cx.ended.clone();
    let stages = match cx.stages() {
        Ok(stages) => stages,
        Err(fault) => return (EventStream::refused(fault), call),
    };
    let mut events = ToClient::new(ahead, feed, stages.latest.clone(), discarded.clone()).events();
    // Each wrap, the last added first, then the filters added before it.
    for (wrap, layer) in stages.wrapped.iter_mut().rev() {
        let Some(wrap) = wrap.take() else { continue };
        let wrap = AssertUnwindSafe(move || wrap(events));
        // A panic is an internal fault.
        let panicked = || stream::once(ready(Err(Fault::internal(PANICKED)))).boxed();
        let outputs = std::panic::catch_unwind(wrap).unwrap_or_else(|_| panicked());
        let feed = Feed::made(outputs);
        events = ToClient::new(VecDeque::new(), feed, layer.clone(), discarded.clone()).events();
    }
    // Once the request ends, its outputs are cut.
    let events = cut(events, on_close(&ended));
    let events = events.flat_map(|values| {
        stream::iter(match values {
            Ok(values) => values.split().into_iter().map(Ok).collect(),
            Err(fault) => vec![Err(fault)],
        })
    });
    let flow = EventStream::stream(events);
    (flow, call)
}
