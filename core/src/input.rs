use std::any::{Any, TypeId};
use std::sync::Arc;

use crate::events::{Metadata, Single};
use crate::fault::guarded;
use crate::format::{AnyFormat, AnyValue};
use crate::plugin::Filters;
use crate::{EventStream, Fault, Result};

/// The current whole-input snapshot, or the shared live reader's marker.
/// Each edit replaces the snapshot; an earlier call keeps its prior one.
#[derive(Clone)]
pub(crate) struct Input {
    format: Arc<dyn AnyFormat>,
    single: Option<Arc<Single<AnyValue>>>,
    metadata: Metadata,
    /// An explicitly supplied call input, never eligible for relay.
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
            own: false,
        }
    }

    pub(crate) fn own(format: Arc<dyn AnyFormat>, input: AnyValue) -> Self {
        Self {
            single: Some(Single::ready(Ok(input), Clone::clone)),
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

    fn value(&self) -> Result<&AnyValue> {
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
        self.value()?
            .downcast_ref()
            .ok_or_else(Fault::wrong_kind::<T>)
    }

    /// A whole input stays one value; its next snapshot owns either the edit
    /// or its failure. The metadata belongs to this snapshot, not the value.
    pub(crate) fn filter(&mut self, mut filters: Filters) -> Result<()> {
        let format = self.format.clone();
        let filtered = self.value().cloned().and_then(|input| {
            guarded(|| {
                let mut values = Some(format.values(vec![input]));
                let edited = match &mut values {
                    Some(values) => filters.run(values),
                    None => Ok(()),
                };
                filters.end(edited, &mut values)?;
                let inputs = values.map(|v| format.inputs(v)).unwrap_or_default();
                match <[AnyValue; 1]>::try_from(inputs) {
                    Ok([input]) => Ok(input),
                    Err(_) => Err(Fault::internal(
                        "an input filter left other than one input where a whole body is one",
                    )),
                }
            })
        });
        let _ = filters.end(filtered.clone().map(drop), &mut None);
        self.single = Some(Single::ready(filtered.clone(), Clone::clone));
        filtered.map(drop)
    }

    pub(crate) fn events(&self, relay: bool) -> Result<EventStream<AnyValue>> {
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
