use std::any::Any;
use std::sync::Arc;

use futures::future::BoxFuture;

use crate::format::{AnyFormat, is_input};
use crate::input::Input;
use crate::leg::{Layer, locked};
use crate::lifecycle::{Edge, InFlight, InFlightGuard};
use crate::plugin::Filters;
use crate::{ClientContext, Extensions, Fault, Filter, Plugin};

/// A provider the gateway can call: a format and its dialer.
#[derive(Clone)]
pub(crate) struct Provider {
    pub(crate) format: Arc<dyn AnyFormat>,
    pub(crate) dialer: Arc<dyn Any + Send + Sync>,
}

/// One call to a provider, as the provider edge's hooks see it.
pub struct ProviderContext {
    /// The request as it was when the call was made.
    pub(crate) client: ClientContext,
    /// The provider's format.
    pub(crate) format: Arc<dyn AnyFormat>,
    pub(crate) provider: String,
    /// Its first input: its own, or the one that came with the request's head.
    pub(crate) input: Input,
    /// Filters of its outputs, whoever reads them.
    pub(crate) outputs: Layer,
    /// Filters of the later inputs it sends, shared with their reader.
    pub(crate) inputs: Layer,
    /// Whether it streams the request's inputs, still flowing.
    pub(crate) flowing: bool,
    /// Idle once it ends: its response's events and its inputs' reader
    /// stop, so a reader still waiting wakes.
    pub(crate) cut: Arc<InFlight>,
    /// In `cut` until it ends, then in its request's `settling` until its
    /// end hooks finish, as its request's end hooks wait for them.
    pub(crate) running: InFlightGuard,
    /// Set once its format took it to send.
    pub(crate) sent: bool,
    /// Set if the provider answered and refused it at its head.
    pub(crate) refused: bool,
}

impl std::fmt::Debug for ProviderContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProviderContext")
            .field("provider", &self.provider)
            .finish_non_exhaustive()
    }
}

impl Edge for ProviderContext {
    fn client(&self) -> &ClientContext {
        &self.client
    }

    fn hook<'a>(&'a self, plugin: &'a dyn Plugin, end: &'a Result<(), Fault>) -> BoxFuture<'a, ()> {
        plugin.on_provider_end(self, end)
    }

    /// Its response and inputs are cut off, and filters still open end,
    /// dropping what they keep back, before its end hooks run. Its end
    /// hooks settle for its request from here.
    fn close(&mut self, end: &Result<(), Fault>) {
        self.running = InFlight::enter(&self.client.settling);
        let _ = locked(&self.inputs).end(end.clone(), &mut None);
        let _ = locked(&self.outputs).end(end.clone(), &mut None);
    }
}

impl ProviderContext {
    /// The request as it was when this call was made. Its
    /// [`extensions`](ClientContext::extensions) are this call's: the
    /// request's, its own ([`Call::with_extension`](crate::Call::with_extension)),
    /// and what its hooks set. The transport gets them with the call.
    pub fn client(&self) -> &ClientContext {
        &self.client
    }

    /// The provider this call goes to.
    pub fn provider(&self) -> &str {
        &self.provider
    }

    /// The name of the provider's format.
    pub fn format(&self) -> &str {
        self.format.name()
    }

    /// Whether `T` is the provider's format's
    /// [`Input`](crate::Format::Input) or [`Output`](crate::Format::Output)
    /// type, which the typed accessors take.
    pub fn is_kind<T: Any>(&self) -> bool {
        is_input::<T>(&*self.format).is_ok()
    }

    /// This call's first input as `T`, the call's input type: its own
    /// ([`Call::with_input`](crate::Call::with_input)), else the one that
    /// came with the request's head. `Err` as [`ClientContext::input`].
    pub fn input<T: Any>(&self) -> Result<&T, Fault> {
        self.input.get()
    }

    /// Run `filter` as [`ClientContext::filter`] does, for this call alone:
    /// over its inputs after the client edge's filters, or its outputs,
    /// whoever reads them, before them.
    pub fn filter<T: Clone + Send + Sync + 'static>(
        &mut self,
        filter: impl Filter<T>,
    ) -> Result<(), Fault> {
        match is_input::<T>(&*self.format)? {
            false => locked(&self.outputs).add(filter, false),
            // Later inputs follow only a session's head.
            true if self.flowing => locked(&self.inputs).add(filter, true),
            true => self.input.filter(Filters::of(filter)),
        }
    }

    /// Whether the call was handed to its transport, so it may have reached
    /// the provider. `false` in `on_provider`, and for a call that failed
    /// before that: by a hook or an input it could not write.
    pub fn is_sent(&self) -> bool {
        self.sent
    }

    /// Whether the provider answered and refused the call at its head: its
    /// response failed as a whole before it began, with no value and not
    /// broken off, as by an error status. At a call's end, `Err` then means
    /// the provider refused. It is false in `on_provider`; for a call whose
    /// transport failed, before a head or in its body; and for a response
    /// that began. A fault alone cannot say which of these it is, as a
    /// provider's refusal and a response cut off look alike.
    pub fn is_refused(&self) -> bool {
        self.refused
    }

    /// This call's extensions: the request's, its own
    /// ([`Call::with_extension`](crate::Call::with_extension)) and what its
    /// hooks set with [`extensions_mut`](ProviderContext::extensions_mut).
    pub fn extensions(&self) -> &Extensions {
        &self.client.extensions
    }

    /// This call's extensions, to change, such as to hand a tenant's key to
    /// the transport or keep a tally for the call's end hook. Changes reach
    /// this call alone.
    pub fn extensions_mut(&mut self) -> &mut Extensions {
        &mut self.client.extensions
    }
}
