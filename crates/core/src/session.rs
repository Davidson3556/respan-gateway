//! Turns a session's response into its client edge's reply.

use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use futures::FutureExt;

use crate::call::SharedCall;
use crate::fault::PANICKED;
use crate::format::{AnyFormat, AnyReply};
use crate::leg::{Outgoing, cut, locked};
use crate::lifecycle::EndHooks;
use crate::{ClientContext, Ended, Fault, Response};

/// The reply of `resp`: a refusal, or the response's origin (on a relay)
/// and outputs; with [`Ended`], which owns the session's end hooks.
pub(crate) async fn reply(
    format: &Arc<dyn AnyFormat>,
    resp: Result<Response, Fault>,
    mut hooks: EndHooks<ClientContext>,
) -> AnyReply {
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
        Err(fault) => return refusal(fault, hooks),
    };
    if resp.refused {
        // Its one batch is the refusal, through the call's filters.
        let read = AssertUnwindSafe(async {
            let (mut batch, _) = resp.body.next().await;
            batch.read();
            batch.end
        });
        let fault = match read.catch_unwind().await {
            Ok(Some(Err(fault))) => fault,
            _ => Fault::internal(PANICKED),
        };
        // A provider's words go only where its head would.
        let fault = match resp.origin.is_some() {
            true => fault,
            false => fault.unrelayed(),
        };
        return refusal(fault, hooks);
    }
    let call = resp.body.call().cloned();
    let (outputs, until) = (cx.outputs.clone(), cx.cut.clone());
    let Response {
        origin,
        body,
        ahead,
        ..
    } = resp;
    let outgoing = Outgoing::new(ahead, body, outputs, origin.is_some());
    // Once the session ended, as when its edge dropped `Ended`, its outputs
    // are cut.
    let events = cut(outgoing.events(), &until);
    ((origin, Ok(events)), ended(hooks, call))
}

/// A refusal with `fault`, which the client edge writes as one.
pub(crate) fn refusal(fault: Fault, hooks: EndHooks<ClientContext>) -> AnyReply {
    ((None, Err(fault)), ended(hooks, None))
}

/// Ends the session once its edge wrote the end, and on a failure its
/// reply's call too, if its outputs are still held (dropped, they ended
/// it); dropped uncalled, the session ends abandoned.
fn ended(hooks: EndHooks<ClientContext>, call: Option<SharedCall>) -> Ended {
    let call = call.as_ref().map(Arc::downgrade);
    Box::new(move |end| {
        let mut hooks = hooks;
        if let (Err(fault), Some(call)) = (&end, call.and_then(|c| c.upgrade())) {
            locked(&call).finish(Err(fault.clone()));
        }
        hooks.finish(end);
    })
}
