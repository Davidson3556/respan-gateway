//! Events of a registered message type, erased: the representation the
//! kernel reads, filters, buffers and writes. An edge reads and writes one
//! [`Value`] per item; filters and buffers work on [`Values`], batches of
//! ready events. Typed access downcasts; a value of another type is an
//! internal fault, never silently dropped.

use std::any::Any;

use futures::StreamExt;
use futures::stream::{self, BoxStream};

use crate::{Fault, Result};

/// One event of a registered message type.
pub(crate) type Value = Box<dyn AnyValue>;

/// Values of one registered message type, in order: a batch of ready events.
pub(crate) type Values = Box<dyn AnyValues>;

/// Erased events, one per item, the first `Err` the last.
pub(crate) type AnyEvents = BoxStream<'static, Result<Value>>;

/// Erased batches, the first `Err` the last.
pub(crate) type AnyBatches = BoxStream<'static, Result<Values>>;

pub(crate) trait AnyValue: Any + Send + Sync {
    /// A batch of it alone.
    fn batch(self: Box<Self>) -> Values;
}

impl<T: Send + Sync + 'static> AnyValue for T {
    fn batch(self: Box<Self>) -> Values {
        Box::new(vec![*self])
    }
}

pub(crate) trait AnyValues: Any + Send + Sync {
    /// Append `value`, which must be of the same type.
    fn push(&mut self, value: Value) -> Result<()>;

    /// Each value alone, in order.
    fn split(self: Box<Self>) -> Vec<Value>;
}

impl<T: Send + Sync + 'static> AnyValues for Vec<T> {
    fn push(&mut self, value: Value) -> Result<()> {
        Vec::push(self, take_value::<T>(value)?);
        Ok(())
    }

    fn split(self: Box<Self>) -> Vec<Value> {
        self.into_iter().map(|v| Box::new(v) as Value).collect()
    }
}

/// Copy a replayable snapshot whose registered type is `T`.
pub(crate) fn clone_value<T: Clone + Send + Sync + 'static>(value: &Value) -> Value {
    let value = view_value::<T>(value).expect("snapshot has its registered type");
    Box::new(value.clone())
}

/// The value as `T`.
pub(crate) fn take_value<T: 'static>(value: Value) -> Result<T> {
    let value = (value as Box<dyn Any>).downcast::<T>();
    value.map(|v| *v).map_err(|_| Fault::wrong_kind::<T>())
}

/// The value as `T`, to read.
pub(crate) fn view_value<T: 'static>(value: &Value) -> Option<&T> {
    (&**value as &dyn Any).downcast_ref()
}

/// The values as `T`s.
pub(crate) fn take<T: 'static>(values: Values) -> Result<Vec<T>> {
    let values = (values as Box<dyn Any>).downcast::<Vec<T>>();
    values.map(|v| *v).map_err(|_| Fault::wrong_kind::<T>())
}

/// The values as `T`s, to read.
pub(crate) fn view<T: 'static>(values: &Values) -> Option<&[T]> {
    let values = (&**values as &dyn Any).downcast_ref::<Vec<T>>();
    values.map(Vec::as_slice)
}

/// The values as `T`s, to edit.
pub(crate) fn view_mut<T: 'static>(values: &mut Values) -> Result<&mut Vec<T>> {
    let values = (&mut **values as &mut dyn Any).downcast_mut::<Vec<T>>();
    values.ok_or_else(Fault::wrong_kind::<T>)
}

/// Typed events erased, one per item.
pub(crate) fn erase<T: Send + Sync + 'static>(events: BoxStream<'static, Result<T>>) -> AnyEvents {
    events
        .map(|event| event.map(|v| Box::new(v) as Value))
        .boxed()
}

/// Erased batches typed, each batch's values in turn.
pub(crate) fn typed<T: Send + 'static>(batches: AnyBatches) -> BoxStream<'static, Result<T>> {
    let each = |item: Result<Values>| {
        stream::iter(match item.and_then(take::<T>) {
            Ok(values) => values.into_iter().map(Ok).collect::<Vec<_>>(),
            Err(fault) => vec![Err(fault)],
        })
    };
    batches.flat_map(each).boxed()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn values_of_another_type_fail_rather_than_vanish() {
        let mut values = (Box::new(1_u8) as Value).batch();
        assert!(values.push(Box::new(2_u16)).is_err());
        assert!(values.push(Box::new(2_u8)).is_ok());
        assert_eq!(view::<u8>(&values), Some(&[1, 2][..]));
        assert!(take_value::<u16>(Box::new(1_u8)).is_err());
        let split: Vec<u8> = values.split().into_iter().flat_map(take_value).collect();
        assert_eq!(split, [1, 2]);
    }
}
