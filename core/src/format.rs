use std::any::{Any, TypeId};
use std::marker::PhantomData;
use std::sync::Arc;

use crate::events::Single;

use futures::future::{BoxFuture, ready};
use futures::{FutureExt, SinkExt};

use crate::values::{Values, clone_values, one, take_one};
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
    /// Snapshot an explicitly supplied input using its registered type.
    fn input(&self, value: &dyn Any) -> Result<Arc<Single<Values>>>;
    fn matches(&self, io: &dyn Any) -> bool;
    fn bind(&self, io: Box<dyn Any + Send>) -> Result<Connection<Values, Values>>;
}

/// The connection with each value erased alone; its sink takes one value per
/// item, as the kernel writes them.
fn erase_connection<R, W>((opening, send): Connection<R, W>) -> Connection<Values, Values>
where
    R: Clone + Send + Sync + 'static,
    W: Clone + Send + Sync + 'static,
{
    let opening = async move {
        opening
            .await
            .map(|events| events.map_values(|v| Ok(one(v)), clone_values::<R>))
    };
    let send = EventSink::preparing(move |events: &mut EventStream<Values>| {
        let mut send = send;
        let mut typed =
            std::mem::replace(events, EventStream::empty()).map_values(take_one::<W>, W::clone);
        let prepared = send.prepare(&mut typed);
        *events = typed.map_values(|value| Ok(one(value)), clone_values::<W>);
        prepared?;
        Ok(send.with(|event: Result<Values>| ready(Ok(event.and_then(take_one::<W>)))))
    });
    (opening.boxed(), send)
}

/// Pins the message types and transport at registration. Only the core
/// assigns a role; provider bindings receive outputs and send inputs.
pub(crate) struct Erased<F, T, R, W, const PROVIDER: bool> {
    format: F,
    types: PhantomData<fn(T, R, W)>,
}

impl<F, T, R, W, const PROVIDER: bool> Erased<F, T, R, W, PROVIDER> {
    pub(crate) fn new(format: F) -> Self {
        Self {
            format,
            types: PhantomData,
        }
    }
}

impl<F, T, R, W, const PROVIDER: bool> AnyFormat for Erased<F, T, R, W, PROVIDER>
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
        if PROVIDER {
            (TypeId::of::<W>(), TypeId::of::<R>())
        } else {
            (TypeId::of::<R>(), TypeId::of::<W>())
        }
    }
    fn input(&self, value: &dyn Any) -> Result<Arc<Single<Values>>> {
        fn snapshot<T: Clone + Send + Sync + 'static>(
            value: &dyn Any,
        ) -> Result<Arc<Single<Values>>> {
            let value = value
                .downcast_ref::<T>()
                .ok_or_else(Fault::wrong_kind::<T>)?;
            Ok(Single::ready(Ok(one(value.clone())), clone_values::<T>))
        }
        if PROVIDER {
            snapshot::<W>(value)
        } else {
            snapshot::<R>(value)
        }
    }
    fn matches(&self, io: &dyn Any) -> bool {
        io.downcast_ref::<T>()
            .is_some_and(|io| self.format.matches(io))
    }
    fn bind(&self, io: Box<dyn Any + Send>) -> Result<Connection<Values, Values>> {
        let io = io
            .downcast::<T>()
            .map_err(|_| Fault::internal("a transport of another type than its format's"))?;
        self.format.bind(*io).map(erase_connection)
    }
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
    async fn erased_preparation_preserves_lazy_metadata_and_stopped_source() {
        let held = Arc::new(());
        let captured = held.clone();
        let mut source = EventStream::lazy(move || -> Result<u8> {
            let _keep_alive = &captured;
            panic!("raw relay decoded during erasure");
        })
        .map_values(|v| Ok(one(v)), clone_values::<u8>)
        .with_metadata("toy", 23_u16);
        let sink = EventSink::preparing(|source: &mut EventStream<u8>| {
            assert!(source.is_single() && source.is_lazy());
            assert_eq!(source.metadata::<u16>("toy"), Some(&23));
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
        assert!(source.is_single() && source.is_lazy());
        assert_eq!(source.metadata::<u16>("toy"), Some(&23));
        assert_eq!(Arc::strong_count(&held), 2);
        crate::events::deliver(source, sink)
            .await
            .expect("deliver raw");
        assert_eq!(Arc::strong_count(&held), 1);
    }
}
