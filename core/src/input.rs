use std::any::{Any, TypeId};
use std::sync::Arc;

use crate::events::{Metadata, Single};
use crate::fault::guarded;
use crate::format::AnyFormat;
use crate::plugin::Filters;
use crate::values::{Values, clone_values, one, view};
use crate::{EventStream, Fault, Result};

/// The current whole-input snapshot, or the shared live reader's marker.
/// Each edit replaces the snapshot; an earlier call keeps its prior one.
#[derive(Clone)]
pub(crate) struct Input {
    format: Arc<dyn AnyFormat>,
    single: Option<Arc<Single<Values>>>,
    metadata: Metadata,
    /// An explicitly supplied call input, never eligible for relay.
    pub(crate) own: bool,
}

impl Input {
    pub(crate) fn new(
        format: Arc<dyn AnyFormat>,
        single: Option<Arc<Single<Values>>>,
        metadata: Metadata,
    ) -> Self {
        Self {
            format,
            single,
            metadata,
            own: false,
        }
    }

    pub(crate) fn own(format: Arc<dyn AnyFormat>, input: Arc<Single<Values>>) -> Self {
        Self {
            single: Some(input),
            own: true,
            ..Self::new(format, None, None)
        }
    }

    pub(crate) fn is_live(&self) -> bool {
        self.single.is_none()
    }

    pub(crate) fn metadata_copy(&self) -> Metadata {
        self.metadata.clone()
    }

    fn value(&self) -> Result<&Values> {
        self.single
            .as_ref()
            .ok_or_else(|| {
                Fault::internal(
                    "a streaming request's inputs come after its head, to filters and take_inputs",
                )
            })?
            .value()
    }

    pub(crate) fn get<T: Any>(&self) -> Result<&T> {
        if self.format.kind().0 != TypeId::of::<T>() {
            return Err(Fault::wrong_kind::<T>());
        }
        match view::<T>(self.value()?) {
            Some([value]) => Ok(value),
            _ => Err(Fault::wrong_kind::<T>()),
        }
    }

    /// A whole input stays one value; its next snapshot owns either the edit
    /// or its failure. The metadata belongs to this snapshot, not the value.
    pub(crate) fn filter<T: Clone + Send + Sync + 'static>(
        &mut self,
        mut filters: Filters,
    ) -> Result<()> {
        let filtered = self.get::<T>().cloned().and_then(|input| {
            guarded(|| {
                let mut values = Some(one(input));
                let edited = match &mut values {
                    Some(values) => filters.run(values),
                    None => Ok(()),
                };
                filters.end(edited, &mut values)?;
                match values {
                    Some(values) if values.len() == 1 => Ok(values),
                    _ => Err(Fault::internal(
                        "an input filter left other than one input where a whole body is one",
                    )),
                }
            })
        });
        let outcome = filtered.as_ref().map(|_| ()).map_err(Clone::clone);
        let _ = filters.end(outcome.clone(), &mut None);
        self.single = Some(Single::ready(filtered, clone_values::<T>));
        outcome
    }

    pub(crate) fn events(&self, relay: bool) -> Result<EventStream<Values>> {
        let single = self
            .single
            .as_ref()
            .ok_or_else(|| Fault::internal("a live input needs its stream"))?;
        // A filtered failure happens before transport preparation, without
        // forcing a still-lazy original input to decode.
        if let Some(fault) = single.known_error() {
            return Err(fault);
        }
        let mut events = single.clone().into_events();
        if relay {
            events.set_metadata(self.metadata_copy());
        }
        Ok(events)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::format::Erased;
    use crate::{Connection, Format};

    struct Counted {
        text: String,
        copies: Arc<AtomicUsize>,
    }

    impl Clone for Counted {
        fn clone(&self) -> Self {
            self.copies.fetch_add(1, Ordering::SeqCst);
            Self {
                text: self.text.clone(),
                copies: self.copies.clone(),
            }
        }
    }

    struct Protocol;
    impl Format<(), Counted, ()> for Protocol {
        fn name(&self) -> &str {
            "counted"
        }
        fn bind(&self, _: ()) -> Result<Connection<Counted, ()>> {
            unreachable!("snapshot filtering does not bind I/O")
        }
    }

    #[test]
    fn filtering_copies_only_the_snapshot_it_edits() {
        let copies = Arc::new(AtomicUsize::new(0));
        let value = Counted {
            text: "before".into(),
            copies: copies.clone(),
        };
        let format = Arc::new(Erased::<_, (), Counted, (), false>::new(Protocol));
        let single = Single::ready(Ok(one(value)), clone_values::<Counted>);
        let mut input = Input::new(format, Some(single), None);
        let previous = input.clone();
        assert_eq!(copies.load(Ordering::SeqCst), 0);
        input
            .filter::<Counted>(Filters::of(|value: &mut Counted| {
                value.text = "after".into();
                Ok(())
            }))
            .expect("filter");
        assert_eq!(copies.load(Ordering::SeqCst), 1);
        assert_eq!(previous.get::<Counted>().expect("previous").text, "before");
        assert_eq!(input.get::<Counted>().expect("edited").text, "after");
    }
}
