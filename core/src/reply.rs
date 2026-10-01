//! Builds the outgoing flow for the application-facing edge.

use std::collections::VecDeque;
use std::panic::AssertUnwindSafe;
use std::sync::{Arc, Mutex};

use futures::future::ready;
use futures::{FutureExt, StreamExt, stream};

use crate::call::SharedCall;
use crate::fault::PANICKED;
use crate::format::{AnyFormat, AnyValue};
use crate::leg::{Outgoing, cut, idle};
use crate::lifecycle::EndHooks;
use crate::response::{Batch, Source};
use crate::{ClientContext, EventStream, Fault, Response, Result};

/// Apply client output stages and retain the selected call until delivery ends.
pub(crate) async fn reply(
    format: &Arc<dyn AnyFormat>,
    resp: Result<Response>,
    hooks: &mut EndHooks<ClientContext>,
    discarded: &Arc<Mutex<Vec<Batch>>>,
) -> (EventStream<AnyValue>, Option<SharedCall>) {
    let cx = hooks.cx();
    // A call's response goes only to its own request; one of another kind
    // fails, even with no outputs.
    let resp = resp.and_then(|resp| match resp.body.call() {
        Some(c) if !cx.open.holds(c) => {
            Err(Fault::internal("a response to another request was sent"))
        }
        _ if resp.output.is_some_and(|o| o != format.kind().1) => Err(Fault::another_kind(
            "a response of another kind was sent".into(),
        )),
        _ => Ok(resp),
    });
    let mut resp = match resp {
        Ok(resp) => resp,
        Err(fault) => return (EventStream::refused(fault), None),
    };
    if resp.refused {
        // Its one batch is the refusal, through the call's filters.
        let read = AssertUnwindSafe(async {
            let mut batch = match resp.ahead.pop_front() {
                Some(batch) => batch,
                None => resp.body.next().await,
            };
            batch.read();
            batch.end.map(|end| end.outcome().clone())
        });
        let fault = match read.catch_unwind().await {
            Ok(Some(Err(fault))) => fault,
            _ => Fault::internal(PANICKED),
        };
        // A provider's words go only where its head would.
        let fault = match resp.metadata.is_some() {
            true => fault,
            false => fault.unrelayed(),
        };
        return (EventStream::refused(fault), None);
    }
    let call = resp.body.call().cloned();
    let Response {
        metadata,
        body,
        ahead,
        ..
    } = resp;
    let relayed = metadata.is_some();
    let mut events =
        Outgoing::new(ahead, body, cx.outputs.clone(), relayed, discarded.clone()).events();
    // Each wrap, the last added first, then the filters added before it.
    let wrapped = cx.wrapped.get_mut().unwrap_or_else(|e| e.into_inner());
    for (wrap, layer) in wrapped.iter_mut().rev() {
        let Some(wrap) = wrap.take() else { continue };
        let wrap = AssertUnwindSafe(move || wrap(events));
        // A panic is an internal fault.
        let panicked = || stream::once(ready(Err(Fault::internal(PANICKED)))).boxed();
        let outputs = std::panic::catch_unwind(wrap).unwrap_or_else(|_| panicked());
        let source = Source::made(outputs);
        events = Outgoing::new(
            VecDeque::new(),
            source,
            layer.clone(),
            relayed,
            discarded.clone(),
        )
        .events();
    }
    // Once the client writer stops, its outputs
    // are cut.
    let events = cut(events, idle(&cx.cut));
    let format = format.clone();
    let events = events.flat_map(move |values| {
        let values = match values {
            Ok(values) => format.outputs(values).into_iter().map(Ok).collect(),
            Err(fault) => vec![Err(fault)],
        };
        stream::iter(values)
    });
    let mut flow = EventStream::stream(events);
    flow.set_metadata(metadata);
    (flow, call)
}
