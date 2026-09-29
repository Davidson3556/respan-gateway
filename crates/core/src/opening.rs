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

/// The input that came with a session's head, and the holder's changed
/// copy; or a call's own.
#[derive(Clone)]
pub(crate) struct Opening {
    base: Arc<Base>,
    /// Set once a filter ran over it or a call brings its own; a filter's
    /// failure is kept.
    changed: Option<Result<AnyInput, Fault>>,
    /// The format of a call's own input, which `changed` then has.
    pub(crate) own: Option<Arc<dyn AnyFormat>>,
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
            own: None,
        }
    }

    /// Bring `input`, a call's own, of `format`'s kind.
    pub(crate) fn bring(&mut self, format: Arc<dyn AnyFormat>, input: AnyInput) {
        self.changed = Some(Ok(input));
        self.own = Some(format);
    }

    /// The format whose kind it is of.
    fn format(&self) -> &Arc<dyn AnyFormat> {
        self.own.as_ref().unwrap_or(&self.base.format)
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

    /// What a call writes first, and whether it is the client's as decoded:
    /// a call's own or a changed input as its value; on a relay, unchanged,
    /// `None` (the origin's own, never decoded); else the decoded opening,
    /// if any.
    pub(crate) fn sent(&self, relay: bool) -> Result<Option<(AnyInput, bool)>, Fault> {
        let changed = self.changed.clone().transpose()?;
        Ok(match changed {
            Some(input) if self.own.is_some() => Some((input, false)),
            Some(input) => {
                let base = self.base().and_then(Result::ok);
                let same = base.is_some_and(|b| self.base.format.same(b, &input));
                Some((input, same))
            }
            None if relay => None,
            None => match self.base() {
                None => None,
                Some(base) => Some((base?.clone(), true)),
            },
        })
    }
}
