//! Events of a registered message type, erased: the one representation the
//! kernel reads, filters, buffers and writes. Typed access downcasts; a
//! value of another type is an internal fault, never silently dropped.

use std::any::Any;

use futures::StreamExt;
use futures::stream::{self, BoxStream};

use crate::{Fault, Result};

/// Values of one registered message type, in order: one event, or a batch of
/// ready events. An edge's sink receives them one value per item.
pub(crate) type Values = Box<dyn AnyValues>;

/// Erased events, the first `Err` the last.
pub(crate) type AnyEvents = BoxStream<'static, Result<Values>>;

pub(crate) trait AnyValues: Any + Send + Sync {
    fn len(&self) -> usize;

    /// Append `more`, which must be of the same type.
    fn append(&mut self, more: Values) -> Result<()>;

    /// Each value alone, in order.
    fn split(self: Box<Self>) -> Vec<Values>;
}

impl<T: Send + Sync + 'static> AnyValues for Vec<T> {
    fn len(&self) -> usize {
        Vec::len(self)
    }

    fn append(&mut self, more: Values) -> Result<()> {
        Vec::append(self, &mut take::<T>(more)?);
        Ok(())
    }

    fn split(self: Box<Self>) -> Vec<Values> {
        self.into_iter().map(one).collect()
    }
}

/// Copy a replayable snapshot whose registered type is `T`.
pub(crate) fn clone_values<T: Clone + Send + Sync + 'static>(values: &Values) -> Values {
    Box::new(
        view::<T>(values)
            .expect("snapshot has its registered type")
            .to_vec(),
    )
}

/// `value` alone.
pub(crate) fn one<T: Send + Sync + 'static>(value: T) -> Values {
    Box::new(vec![value])
}

/// The values as `T`s.
pub(crate) fn take<T: 'static>(values: Values) -> Result<Vec<T>> {
    let values = (values as Box<dyn Any>).downcast::<Vec<T>>();
    values.map(|v| *v).map_err(|_| Fault::wrong_kind::<T>())
}

/// The one value as `T`.
pub(crate) fn take_one<T: 'static>(values: Values) -> Result<T> {
    match <[T; 1]>::try_from(take::<T>(values)?) {
        Ok([value]) => Ok(value),
        Err(_) => Err(Fault::internal(
            "one value was expected, as a whole input is",
        )),
    }
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

/// Typed events erased, one value per item.
pub(crate) fn erase<T: Send + Sync + 'static>(events: BoxStream<'static, Result<T>>) -> AnyEvents {
    events.map(|event| event.map(one)).boxed()
}

/// Erased events typed, each item's values in turn.
pub(crate) fn typed<T: Send + 'static>(events: AnyEvents) -> BoxStream<'static, Result<T>> {
    let each = |item: Result<Values>| {
        stream::iter(match item.and_then(take::<T>) {
            Ok(values) => values.into_iter().map(Ok).collect::<Vec<_>>(),
            Err(fault) => vec![Err(fault)],
        })
    };
    events.flat_map(each).boxed()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn values_of_another_type_fail_rather_than_vanish() {
        let mut values = one(1_u8);
        assert!(values.append(one(2_u16)).is_err());
        assert!(values.append(one(2_u8)).is_ok());
        assert_eq!(view::<u8>(&values), Some(&[1, 2][..]));
        assert!(take::<u16>(one(1_u8)).is_err());
        assert!(take_one::<u8>(Box::new(vec![1_u8, 2])).is_err());
        let split: Vec<u8> = values.split().into_iter().flat_map(take_one).collect();
        assert_eq!(split, [1, 2]);
    }
}
