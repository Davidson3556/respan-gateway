use std::any::{Any, TypeId};
use std::marker::PhantomData;
use std::sync::Arc;

use crate::events::Single;

use futures::future::{BoxFuture, ready};
use futures::{FutureExt, SinkExt};

use crate::values::{Value, clone_value, take_value};
use crate::{EventSink, EventStream, Fault, Result};

/// A receive future and a standard outgoing sink. Synchronous preparation
/// may fail before sending; the event pump runs alongside receiving.
pub type Connection<R, W> = (BoxFuture<'static, Result<EventStream<R>>>, EventSink<W>);

/// A protocol that receives `R` and sends `W` over transport `T`.
///
/// A format holds shared configuration. Each binding consumes one transport
/// and creates its own receive and send state. The gateway assigns the edge's
/// role and chooses the message types at registration. `R` and `W` must differ
/// because event types select filter direction. A format can implement
/// both `Format<T, A, B>` and `Format<T, B, A>` for the same transport type.
pub trait Format<T, R, W>: Send + Sync + 'static {
    /// Protocol identity. Registrations with the same name must agree on
    /// the gateway's input and output types, after accounting for orientation.
    fn name(&self) -> &str;

    /// Whether this format accepts the incoming transport.
    fn matches(&self, _io: &T) -> bool {
        false
    }

    /// Consume a transport and create its receive future and outgoing sink.
    /// The sink survives an opening failure. Opening and delivery may need
    /// to be driven concurrently, including while consuming their streams.
    fn bind(&self, io: T) -> Result<Connection<R, W>>;
}

pub(crate) trait AnyFormat: Send + Sync + 'static {
    fn name(&self) -> &str;
    /// The gateway's input and output types, whichever edge it is on.
    fn kind(&self) -> (TypeId, TypeId);
    fn matches(&self, io: &dyn Any) -> bool;
    fn bind(&self, io: Box<dyn Any + Send>) -> Result<Connection<Value, Value>>;
}

/// The connection with each value erased.
fn erase_connection<R, W>((opening, send): Connection<R, W>) -> Connection<Value, Value>
where
    R: Clone + Send + Sync + 'static,
    W: Clone + Send + Sync + 'static,
{
    let opening = async move {
        opening
            .await
            .map(|events| events.map_values(|v| Ok(Box::new(v) as Value), clone_value::<R>))
    };
    let send = EventSink::preparing(move |events: &mut EventStream<Value>| {
        let mut send = send;
        let mut typed =
            std::mem::replace(events, EventStream::empty()).map_values(take_value::<W>, W::clone);
        let prepared = send.prepare(&mut typed);
        *events = typed.map_values(|value| Ok(Box::new(value) as Value), clone_value::<W>);
        prepared?;
        Ok(send.with(|event: Result<Value>| ready(Ok(event.and_then(take_value::<W>)))))
    });
    (opening.boxed(), send)
}

/// Pins the message types and transport at registration, which assigns the
/// role: a client binding receives inputs and sends outputs, a provider
/// binding receives outputs and sends inputs.
pub(crate) struct Erased<F, T, R, W> {
    format: F,
    /// The gateway's input and output types.
    kind: (TypeId, TypeId),
    types: PhantomData<fn(T, R, W)>,
}

impl<F, T, R: 'static, W: 'static> Erased<F, T, R, W> {
    pub(crate) fn client(format: F) -> Self {
        Self::new(format, (TypeId::of::<R>(), TypeId::of::<W>()))
    }

    pub(crate) fn provider(format: F) -> Self {
        Self::new(format, (TypeId::of::<W>(), TypeId::of::<R>()))
    }

    fn new(format: F, kind: (TypeId, TypeId)) -> Self {
        Self {
            format,
            kind,
            types: PhantomData,
        }
    }
}

impl<F, T, R, W> AnyFormat for Erased<F, T, R, W>
where
    F: Format<T, R, W>,
    T: Send + 'static,
    R: Clone + Send + Sync + 'static,
    W: Clone + Send + Sync + 'static,
{
    fn name(&self) -> &str {
        self.format.name()
    }
    fn kind(&self) -> (TypeId, TypeId) {
        self.kind
    }
    fn matches(&self, io: &dyn Any) -> bool {
        io.downcast_ref::<T>()
            .is_some_and(|io| self.format.matches(io))
    }
    fn bind(&self, io: Box<dyn Any + Send>) -> Result<Connection<Value, Value>> {
        let io = io
            .downcast::<T>()
            .map_err(|_| Fault::internal("a transport of another type than its format's"))?;
        self.format.bind(*io).map(erase_connection)
    }
}

/// Snapshot an explicitly supplied input of registered type `T`.
pub(crate) fn snapshot<T: Clone + Send + Sync + 'static>(
    value: &dyn Any,
) -> Result<Arc<Single<Value>>> {
    let value = value
        .downcast_ref::<T>()
        .ok_or_else(Fault::wrong_kind::<T>)?;
    Ok(Single::ready(Ok(Box::new(value.clone())), clone_value::<T>))
}

/// Whether `T` is `format`'s input type (`true`) or output type (`false`);
/// `Err` for another.
pub(crate) fn is_input<T: Any>(format: &dyn AnyFormat) -> Result<bool> {
    match format.kind() {
        (input, _) if input == TypeId::of::<T>() => Ok(true),
        (_, output) if output == TypeId::of::<T>() => Ok(false),
        _ => Err(Fault::wrong_kind::<T>()),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    #[tokio::test]
    async fn erased_preparation_preserves_deferred_decode_and_stopped_source() {
        let held = Arc::new(());
        let captured = held.clone();
        let mut source = EventStream::lazy(move || -> Result<u8> {
            let _keep_alive = &captured;
            panic!("raw relay decoded during erasure");
        })
        .map_values(|v| Ok(Box::new(v) as Value), clone_value::<u8>);
        let sink = EventSink::preparing(|source: &mut EventStream<u8>| {
            source.stop();
            Ok(futures::sink::unfold(
                (),
                |(), _: Result<u8>| -> futures::future::Ready<Result<()>> {
                    panic!("stopped source was submitted");
                },
            ))
        });
        let (_, mut sink) = erase_connection((
            futures::future::ready(Ok(EventStream::<u16>::empty())).boxed(),
            sink,
        ));
        sink.prepare(&mut source).expect("prepare");
        assert_eq!(Arc::strong_count(&held), 2);
        crate::events::deliver(source, sink)
            .await
            .expect("deliver raw");
        assert_eq!(Arc::strong_count(&held), 1);
    }
}
