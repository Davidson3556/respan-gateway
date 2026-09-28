use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use futures::future::BoxFuture;

use crate::context::unlocked;
use crate::format::{AnyFormat, is_input};
use crate::input::{Input, InputFilters, locked};
use crate::lifecycle::{Edge, InFlight, InFlightGuard};
use crate::plugin::Filters;
use crate::transport::{AnyTransport, Head};
use crate::{ClientContext, Extensions, Fault, Filter, Outcome, Plugin};

/// A provider the gateway can call: a format and a transport.
#[derive(Clone)]
pub(crate) struct Provider {
    pub(crate) format: Arc<dyn AnyFormat>,
    pub(crate) transport: Arc<dyn AnyTransport>,
}

pub(crate) type CallEdit = Box<dyn FnOnce(&mut Head, &mut Bytes) -> Result<(), Fault> + Send>;

/// One call to a provider, as the provider edge's hooks see it: its
/// request, provider, format and first input, and edits and filters of the
/// call.
pub struct ProviderContext {
    /// The request as it was when the call was made.
    pub(crate) client: ClientContext,
    /// The provider's format.
    pub(crate) format: Arc<dyn AnyFormat>,
    pub(crate) provider: String,
    /// Its first input.
    pub(crate) input: Input,
    /// In `Mutex`es for the reason `ClientContext`'s edits are.
    pub(crate) outputs: Mutex<Filters>,
    /// Filters of the later inputs it sends, shared with their reader.
    pub(crate) inputs: InputFilters,
    /// Edits of the call's head and first frame, in the order added.
    pub(crate) call_edits: Mutex<Vec<CallEdit>>,
    /// Idle once it ends: its response's body and its inputs' reader stop,
    /// so a reader still waiting wakes.
    pub(crate) cut: Arc<InFlight>,
    /// In `cut` until it ends, then in its request's `settling` until its
    /// end hooks finish, as its request's end hooks wait for them.
    pub(crate) running: InFlightGuard,
    /// Set once the call is handed to its transport.
    pub(crate) sent: bool,
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

    fn hook<'a>(&'a self, plugin: &'a dyn Plugin, outcome: &'a Outcome) -> BoxFuture<'a, ()> {
        plugin.on_provider_end(self, outcome)
    }

    /// Its response and inputs are cut off, and filters still open end,
    /// dropping what they keep back, before its end hooks run. Its end
    /// hooks settle for its request from here.
    fn close(&mut self, end: &Result<(), Fault>) {
        self.running = InFlight::enter(&self.client.settling);
        let _ = locked(&self.inputs).end(end.clone(), &mut None);
        let _ = unlocked(&mut self.outputs).end(end.clone(), &mut None);
    }
}

impl ProviderContext {
    /// The request as it was when this call was made, its extensions this
    /// call's ([`ProviderContext::extensions`]).
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
    /// over the inputs it sends, after the client edge's filters, or over its
    /// response's outputs, whoever reads it, before them.
    pub fn filter<T: Clone + PartialEq + Send + Sync + 'static>(
        &mut self,
        filter: impl Filter<T>,
    ) -> Result<(), Fault> {
        match is_input::<T>(&*self.format)? {
            false => unlocked(&mut self.outputs).add(filter, false),
            // Later inputs follow only a streaming request, never an own input.
            true if self.client.later.is_some() && self.input.own_format.is_none() => {
                locked(&self.inputs).add(filter, true)
            }
            true => self.input.filter(Filters::of(filter)),
        }
    }

    /// Whether the call was handed to its transport, so it may have reached
    /// the provider: `false` in `on_provider`, and in `on_provider_end` for
    /// a call refused or failed before it was sent, such as by a hook, an
    /// edit, or an input that could not be written.
    pub fn is_sent(&self) -> bool {
        self.sent
    }

    /// This call's extensions, [`ProviderContext::client`]'s: the
    /// request's, its own ([`Call::with_extension`](crate::Call::with_extension)),
    /// and what its hooks set. The transport gets them with the call.
    pub fn extensions(&self) -> &Extensions {
        &self.client.extensions
    }

    /// Like [`ProviderContext::extensions`], for changing them, such as to
    /// hand a tenant's key to the transport
    /// ([`Transport::send`](crate::Transport::send)).
    pub fn extensions_mut(&mut self) -> &mut Extensions {
        &mut self.client.extensions
    }

    /// Edit this call's head, of type `H`, the provider's format's
    /// [`Ask`](crate::Format::Ask), and its first frame, after its
    /// [`request_encoder`](crate::Format::request_encoder) and before the
    /// transport, in the order added; the decoder gets it as edited. `Err`
    /// fails the call, as a head of another type does with
    /// [`codes::INTERNAL`](crate::codes::INTERNAL). The kernel reads
    /// nothing of it. Put credentials in an extension the transport reads
    /// ([`ProviderContext::extensions_mut`]), as a transport may strip them.
    pub fn edit_call<H: Any>(
        &mut self,
        f: impl FnOnce(&mut H, &mut Bytes) -> Result<(), Fault> + Send + 'static,
    ) {
        let edit = move |head: &mut Head, first: &mut Bytes| match head.downcast_mut() {
            Some(head) => f(head, first),
            None => Err(Fault::internal(
                "a call edit of another wire than the provider's",
            )),
        };
        unlocked(&mut self.call_edits).push(Box::new(edit));
    }
}

/// The provider `name`, if it exists and speaks `format` or another format
/// of its kind.
pub(crate) fn provider_for<'a>(
    format: &dyn AnyFormat,
    providers: &'a HashMap<String, Provider>,
    name: &str,
) -> Result<&'a Provider, Fault> {
    let Some(p) = providers.get(name) else {
        return Err(Fault::config(format!("unknown provider '{name}'")));
    };
    let (from, to) = (format.name(), p.format.name());
    if from != to && format.kind() != p.format.kind() {
        return Err(Fault::config(format!(
            "the client speaks {from}, provider '{name}' speaks {to}, and they share no kind"
        )));
    }
    Ok(p)
}
