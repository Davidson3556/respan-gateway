use crate::Result;
use std::any::{Any, TypeId};
use std::sync::Arc;

use crate::Events;
use crate::Fault;
use crate::events::{Metadata, Single};
use crate::fault::guarded;
use crate::format::{AnyFormat, AnyValue};
use crate::plugin::Filters;

/// The input that came with a request's head, and the holder's changed
/// copy; or a call's own.
#[derive(Clone)]
pub(crate) struct Input {
    format: Arc<dyn AnyFormat>,
    single: Option<Arc<Single<AnyValue>>>,
    metadata: Metadata,
    /// Set once a filter ran over it or a call brings its own; a filter's
    /// failure is kept.
    changed: Option<Result<AnyValue>>,
    /// Whether it is a call's own, none of the client's.
    pub(crate) own: bool,
}

impl Input {
    pub(crate) fn new(
        format: Arc<dyn AnyFormat>,
        single: Option<Arc<Single<AnyValue>>>,
        metadata: Metadata,
    ) -> Self {
        Self {
            format,
            single,
            metadata,
            changed: None,
            own: false,
        }
    }

    /// A call's own `input`, of `format`'s kind.
    pub(crate) fn own(format: Arc<dyn AnyFormat>, input: AnyValue) -> Self {
        Self {
            changed: Some(Ok(input)),
            own: true,
            ..Self::new(format, None, None)
        }
    }

    /// The format whose kind it is of.
    fn format(&self) -> &Arc<dyn AnyFormat> {
        &self.format
    }

    /// The client's input, decoded once; `None` if none came.
    fn base(&self) -> Option<Result<&AnyValue>> {
        self.single.as_ref().map(|single| single.value())
    }

    /// The input as it is now.
    fn value(&self) -> Result<&AnyValue> {
        match &self.changed {
            Some(changed) => changed.as_ref().map_err(Clone::clone),
            None => self.base().unwrap_or_else(|| {
                let later =
                    "a streaming request's inputs come after its head, to filters and take_inputs";
                Err(Fault::internal(later))
            }),
        }
    }

    /// The input as `T`; another type is refused without decoding.
    pub(crate) fn get<T: Any>(&self) -> Result<&T> {
        if self.format().kind().0 != TypeId::of::<T>() {
            return Err(Fault::wrong_kind::<T>());
        }
        self.value()?
            .downcast_ref()
            .ok_or_else(Fault::wrong_kind::<T>)
    }

    /// Run the one input of a request or call whose inputs ended through
    /// `filters`, and end them: it stays one input.
    pub(crate) fn filter(&mut self, mut filters: Filters) -> Result<()> {
        let format = self.format().clone();
        let filtered = self.value().cloned().and_then(|input| {
            // Dropping the held copy lets an input no one shares be moved.
            drop(self.changed.take());
            let filtered = guarded(|| {
                let mut values = Some(format.values(vec![input]));
                let edited = match &mut values {
                    Some(values) => filters.run(values),
                    None => Ok(()),
                };
                filters.end(edited, &mut values)?;
                let inputs = values.map(|v| format.inputs(v)).unwrap_or_default();
                // Dropping a whole body's one input is refusing, `Err`'s job.
                match <[AnyValue; 1]>::try_from(inputs) {
                    Ok([input]) => Ok(input),
                    Err(_) => Err(Fault::internal(
                        "an input filter left other than one input where a whole body is one",
                    )),
                }
            });
            self.changed = Some(filtered.clone());
            filtered
        });
        // Filters that a bad body or a panic kept from their end get it here.
        let _ = filters.end(filtered.clone().map(drop), &mut None);
        filtered.map(drop)
    }

    /// Supply the event sequence accepted by the provider sink.
    /// A filtered value is eager; an unchanged value keeps its lazy decoder.
    pub(crate) fn events(&self, relay: bool) -> Result<Events<AnyValue>> {
        let mut events = match &self.changed {
            Some(value) => Events::single(value.clone()?),
            None => self
                .single
                .as_ref()
                .ok_or_else(|| Fault::internal("a live input needs its stream"))?
                .clone()
                .into_events(),
        };
        if relay {
            events.set_metadata(self.metadata.clone());
        }
        Ok(events)
    }
}
