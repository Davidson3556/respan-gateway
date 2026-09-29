use std::any::{Any, TypeId};
use std::panic::AssertUnwindSafe;
use std::sync::{Arc, OnceLock};

use crate::Fault;
use crate::fault::{PANICKED, guarded};
use crate::format::{AnyFormat, AnyInput, Origin};
use crate::plugin::Filters;

/// The client's origin, its opening input decoded when first needed.
struct Base {
    format: Arc<dyn AnyFormat>,
    origin: Option<Origin>,
    decoded: OnceLock<Option<Result<AnyInput, Fault>>>,
}

/// The input that came with a request's head, and the holder's changed
/// copy; or a call's own.
#[derive(Clone)]
pub(crate) struct Opening {
    base: Arc<Base>,
    /// Set once a filter ran over it or a call brings its own; a filter's
    /// failure is kept.
    changed: Option<Result<AnyInput, Fault>>,
    /// Whether it is a call's own, none of the client's.
    pub(crate) own: bool,
}

impl Opening {
    pub(crate) fn new(format: Arc<dyn AnyFormat>, origin: Option<Origin>) -> Self {
        let base = Base {
            format,
            origin,
            decoded: OnceLock::new(),
        };
        Self {
            base: Arc::new(base),
            changed: None,
            own: false,
        }
    }

    /// A call's own opening, of `format`'s kind: `input`, or none for a
    /// call whose own inputs stream.
    pub(crate) fn own(format: Arc<dyn AnyFormat>, input: Option<AnyInput>) -> Self {
        Self {
            changed: input.map(Ok),
            own: true,
            ..Self::new(format, None)
        }
    }

    /// The format whose kind it is of.
    fn format(&self) -> &Arc<dyn AnyFormat> {
        &self.base.format
    }

    /// The client's opening, decoded once; `None` if none came.
    fn base(&self) -> Option<Result<&AnyInput, Fault>> {
        let base = &*self.base;
        let decoded = base.decoded.get_or_init(|| {
            let origin = base.origin.as_ref()?;
            // A panic is an internal fault.
            let opening = AssertUnwindSafe(|| base.format.opening(origin));
            std::panic::catch_unwind(opening)
                .unwrap_or_else(|_| Some(Err(Fault::internal(PANICKED))))
        });
        decoded.as_ref().map(|d| d.as_ref().map_err(Clone::clone))
    }

    /// The opening as it is now.
    fn value(&self) -> Result<&AnyInput, Fault> {
        match &self.changed {
            Some(changed) => changed.as_ref().map_err(Clone::clone),
            None => self.base().unwrap_or_else(|| {
                let later =
                    "a streaming request's inputs come after its head, to filters and take_inputs";
                Err(Fault::internal(later))
            }),
        }
    }

    /// The opening as `T`; another type is refused without decoding.
    pub(crate) fn get<T: Any>(&self) -> Result<&T, Fault> {
        if self.format().kind().0 != TypeId::of::<T>() {
            return Err(Fault::wrong_kind::<T>());
        }
        self.value()?
            .downcast_ref()
            .ok_or_else(Fault::wrong_kind::<T>)
    }

    /// Run the one input of a session or call whose inputs ended through
    /// `filters`, and end them: it stays one input.
    pub(crate) fn filter(&mut self, mut filters: Filters) -> Result<(), Fault> {
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
                match <[AnyInput; 1]>::try_from(inputs) {
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

    /// What a call writes first: a call's own or a changed input as its
    /// value; on a relay, unchanged, `None` (the origin's own, never
    /// decoded); else the decoded opening, if any.
    pub(crate) fn sent(&self, relay: bool) -> Result<Option<AnyInput>, Fault> {
        match &self.changed {
            Some(changed) => changed.clone().map(Some),
            None if relay => Ok(None),
            None => self.base().map(|base| base.cloned()).transpose(),
        }
    }
}
