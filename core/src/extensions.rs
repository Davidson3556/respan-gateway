use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::Arc;

/// A value an [`Extensions`] can hold.
trait Value: Any + Send + Sync {
    fn clone_box(&self) -> Box<dyn Value>;
}

impl<T: Clone + Send + Sync + 'static> Value for T {
    fn clone_box(&self) -> Box<dyn Value> {
        Box::new(self.clone())
    }
}

/// Values of any `Clone` type, at most one per type, that the adapter,
/// plugins and transports hand each other with a request or call. The
/// kernel reads none.
#[derive(Default)]
pub struct Extensions(HashMap<TypeId, Box<dyn Value>>);

impl Clone for Extensions {
    fn clone(&self) -> Self {
        Self(
            self.0
                .iter()
                .map(|(k, v)| (*k, Value::clone_box(&**v)))
                .collect(),
        )
    }
}

impl std::fmt::Debug for Extensions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Extensions")
            .field("len", &self.0.len())
            .finish()
    }
}

/// `value`, of type `T`, unboxed.
fn unbox<T: 'static>(value: Box<dyn Value>) -> Option<T> {
    (value as Box<dyn Any>).downcast().ok().map(|v| *v)
}

impl Extensions {
    /// None.
    pub fn new() -> Self {
        Self::default()
    }

    /// Put `value` in, returning the one of its type it replaces.
    pub fn insert<T: Clone + Send + Sync + 'static>(&mut self, value: T) -> Option<T> {
        unbox(self.0.insert(TypeId::of::<T>(), Box::new(value))?)
    }

    /// The value of type `T`, if any.
    pub fn get<T: 'static>(&self) -> Option<&T> {
        (&**self.0.get(&TypeId::of::<T>())? as &dyn Any).downcast_ref()
    }

    /// The value of type `T`, if any, to change.
    pub fn get_mut<T: 'static>(&mut self) -> Option<&mut T> {
        (&mut **self.0.get_mut(&TypeId::of::<T>())? as &mut dyn Any).downcast_mut()
    }

    /// Take out the value of type `T`, if any.
    pub fn remove<T: 'static>(&mut self) -> Option<T> {
        unbox(self.0.remove(&TypeId::of::<T>())?)
    }

    /// Put in every value of `other`, replacing those of its types.
    pub fn extend(&mut self, other: Extensions) {
        self.0.extend(other.0);
    }
}

/// Data a format owns, under its name: readable only by code that knows
/// both the name and the type. The kernel carries it unopened.
#[derive(Clone)]
pub(crate) struct FormatData {
    format: Arc<str>,
    data: Arc<dyn Any + Send + Sync>,
}

impl FormatData {
    pub(crate) fn new(format: &str, data: impl Any + Send + Sync) -> Self {
        Self {
            format: Arc::from(format),
            data: Arc::new(data),
        }
    }

    /// The name of the format that owns it.
    pub(crate) fn format(&self) -> &str {
        &self.format
    }

    /// The data as `T`, if `format` owns it.
    pub(crate) fn get<T: Any>(&self, format: &str) -> Option<&T> {
        (*self.format == *format)
            .then(|| self.data.downcast_ref())
            .flatten()
    }
}
