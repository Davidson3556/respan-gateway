use std::any::{Any, TypeId};
use std::marker::PhantomData;
use std::sync::Arc;

use futures::future::BoxFuture;
use futures::stream::{self, BoxStream};
use futures::{FutureExt, SinkExt, StreamExt};

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

/// An event whose concrete kind is retained by its registered format.
pub(crate) type AnyValue = Arc<dyn Any + Send + Sync>;
/// A batch of values of one concrete type.
pub(crate) type Values = Box<dyn Any + Send>;
pub(crate) type AnyEvents = BoxStream<'static, Result<Values>>;
pub(crate) type ValueStream = BoxStream<'static, Result<AnyValue>>;

pub(crate) trait AnyFormat: Send + Sync + 'static {
    fn name(&self) -> &str;
    fn kind(&self) -> (TypeId, TypeId);
    fn matches(&self, io: &dyn Any) -> bool;
    fn bind(&self, io: Box<dyn Any + Send>) -> Result<Connection<AnyValue, AnyValue>>;
    fn values(&self, inputs: Vec<AnyValue>) -> Values;
    fn inputs(&self, values: Values) -> Vec<AnyValue>;
    fn output_values(&self, outputs: Vec<AnyValue>) -> Values;
    fn outputs(&self, values: Values) -> Vec<AnyValue>;
    fn append(&self, values: &mut Values, more: Values);
}

/// Typed events erased, one value per item.
pub(crate) fn erase<T: Send + 'static>(events: BoxStream<'static, Result<T>>) -> AnyEvents {
    events
        .map(|e| e.map(|v| Box::new(vec![v]) as Values))
        .boxed()
}

/// Erased events typed, each item's values in turn.
pub(crate) fn typed<T: Send + 'static>(events: AnyEvents) -> BoxStream<'static, Result<T>> {
    let each = |item: Result<Values>| {
        let values =
            item.and_then(|v| v.downcast::<Vec<T>>().map_err(|_| Fault::wrong_kind::<T>()));
        stream::iter(match values {
            Ok(values) => values.into_iter().map(Ok).collect::<Vec<_>>(),
            Err(fault) => vec![Err(fault)],
        })
    };
    events.flat_map(each).boxed()
}

/// `input` as `T`, moved if not shared.
fn owned<T: Clone + Send + Sync + 'static>(input: AnyValue) -> Result<T> {
    let input = input
        .downcast::<T>()
        .map_err(|_| Fault::wrong_kind::<T>())?;
    Ok(Arc::try_unwrap(input).unwrap_or_else(|shared| (*shared).clone()))
}

fn erase_connection<R, W>((opening, send): Connection<R, W>) -> Connection<AnyValue, AnyValue>
where
    R: Clone + Send + Sync + 'static,
    W: Clone + Send + Sync + 'static,
{
    let opening = async move {
        opening
            .await
            .map(|events| events.map_values(|value| Ok(Arc::new(value) as AnyValue)))
    }
    .boxed();
    let send = EventSink::preparing(move |events: &mut EventStream<AnyValue>| {
        let mut send = send;
        let mut typed = std::mem::replace(events, EventStream::empty()).map_values(owned::<W>);
        let prepared = send.prepare(&mut typed);
        *events = typed.map_values(|value| Ok(Arc::new(value) as AnyValue));
        prepared?;
        Ok(send
            .with(|event: Result<AnyValue>| futures::future::ready(Ok(event.and_then(owned::<W>)))))
    });
    (opening, send)
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
    fn matches(&self, io: &dyn Any) -> bool {
        io.downcast_ref::<T>()
            .is_some_and(|io| self.format.matches(io))
    }
    fn bind(&self, io: Box<dyn Any + Send>) -> Result<Connection<AnyValue, AnyValue>> {
        let io = io
            .downcast::<T>()
            .map_err(|_| Fault::internal("a transport of another type than its format's"))?;
        self.format.bind(*io).map(erase_connection)
    }
    fn values(&self, inputs: Vec<AnyValue>) -> Values {
        if PROVIDER {
            values::<W>(inputs)
        } else {
            values::<R>(inputs)
        }
    }
    fn inputs(&self, values: Values) -> Vec<AnyValue> {
        if PROVIDER {
            items::<W>(values)
        } else {
            items::<R>(values)
        }
    }
    fn output_values(&self, outputs: Vec<AnyValue>) -> Values {
        if PROVIDER {
            values::<R>(outputs)
        } else {
            values::<W>(outputs)
        }
    }
    fn outputs(&self, values: Values) -> Vec<AnyValue> {
        if PROVIDER {
            items::<R>(values)
        } else {
            items::<W>(values)
        }
    }
    fn append(&self, values: &mut Values, more: Values) {
        if PROVIDER {
            append::<R>(values, more)
        } else {
            append::<W>(values, more)
        }
    }
}

fn values<T: Clone + Send + Sync + 'static>(items: Vec<AnyValue>) -> Values {
    Box::new(
        items
            .into_iter()
            .filter_map(|v| owned::<T>(v).ok())
            .collect::<Vec<_>>(),
    )
}

fn items<T: Send + Sync + 'static>(values: Values) -> Vec<AnyValue> {
    values
        .downcast::<Vec<T>>()
        .unwrap_or_default()
        .into_iter()
        .map(|v| Arc::new(v) as AnyValue)
        .collect()
}

fn append<T: Send + 'static>(values: &mut Values, more: Values) {
    let more = more.downcast::<Vec<T>>().map(|v| *v);
    if let (Some(values), Ok(mut more)) = (values.downcast_mut::<Vec<T>>(), more) {
        values.append(&mut more);
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
    use super::*;

    #[tokio::test]
    async fn erased_preparation_preserves_lazy_metadata_and_stopped_source() {
        let held = Arc::new(());
        let captured = held.clone();
        let mut source = EventStream::lazy(move || -> Result<AnyValue> {
            let _keep_alive = &captured;
            panic!("raw relay decoded during erasure");
        })
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
