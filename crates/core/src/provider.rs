use std::any::Any;
use std::sync::Arc;

use futures::future::BoxFuture;

use crate::events::Single;
use crate::format::{AnyFormat, Erased, is_input, snapshot};
use crate::input::Input;
use crate::layers::{Layer, locked};
use crate::lifecycle::{Edge, InFlight, InFlightGuard, Latch};
use crate::plugin::Filters;
use crate::values::Value;
use crate::{ClientContext, Extensions, Filter, Format, Plugin, Result};

/// A provider a gateway can call, added with
/// [`Gateway::provider`](crate::Gateway::provider): its name, the format it
/// speaks, how each call gets a transport, and its own plugins.
///
/// ```ignore
/// let groq = Provider::new("groq", OpenAiChat, connect(..)).plugin(GroqQuirks);
/// let gateway = Gateway::new().provider(groq)?;
/// ```
#[derive(Clone)]
pub struct Provider {
    pub(crate) name: String,
    pub(crate) format: Arc<dyn AnyFormat>,
    /// Snapshots a call's own input with the registered input type's `Clone`.
    pub(crate) snapshot: fn(&dyn Any) -> Result<Arc<Single<Value>>>,
    pub(crate) factory: Arc<dyn Fn(Extensions) -> Box<dyn Any + Send> + Send + Sync>,
    /// Its own plugins, run on its calls alone, after the gateway's.
    pub(crate) plugins: Plugins,
}

/// A provider's own plugins ([`Provider::plugin`]).
pub(crate) type Plugins = Arc<[Arc<dyn Plugin>]>;

impl Provider {
    /// Provider `name`, speaking `format`. After its hooks run, each attempt
    /// gets a fresh transport from `factory`, with that call's extensions.
    /// The factory may return a lightweight handle backed by a shared pool.
    /// `I` is the gateway input type and `O` its output type: this binding
    /// receives `O` and sends `I`.
    pub fn new<I, O, T>(
        name: impl Into<String>,
        format: impl Format<T, O, I>,
        factory: impl Fn(Extensions) -> T + Send + Sync + 'static,
    ) -> Provider
    where
        T: Send + 'static,
        I: Clone + Send + Sync + 'static,
        O: Clone + Send + Sync + 'static,
    {
        let factory =
            Arc::new(move |extensions| Box::new(factory(extensions)) as Box<dyn Any + Send>);
        Provider {
            name: name.into(),
            format: Arc::new(Erased::<_, T, O, I>::provider(format)),
            snapshot: snapshot::<I>,
            factory,
            plugins: Arc::new([]),
        }
    }

    /// Add `plugin` to this provider alone: its provider-edge hooks
    /// ([`on_provider`](Plugin::on_provider) and
    /// [`on_provider_end`](Plugin::on_provider_end)) run on this provider's
    /// calls, after the gateway's plugins and those added here before it;
    /// its client-edge hooks never run. It is part of the provider, not a
    /// policy: what a vendor departs from its format's spec in goes here,
    /// so the gateway's plugins see the kind's events.
    pub fn plugin(mut self, plugin: impl Plugin) -> Provider {
        let mut plugins = self.plugins.to_vec();
        plugins.push(Arc::new(plugin));
        self.plugins = plugins.into();
        self
    }

    /// Its name.
    pub fn name(&self) -> &str {
        &self.name
    }
}

impl std::fmt::Debug for Provider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Provider")
            .field("name", &self.name)
            .field("format", &self.format.name())
            .field("plugins", &self.plugins.len())
            .finish_non_exhaustive()
    }
}

/// One call to a provider, as the provider edge's hooks see it.
pub struct ProviderContext {
    /// The request as it was when the call was made.
    pub(crate) client: ClientContext,
    /// The provider's format.
    pub(crate) format: Arc<dyn AnyFormat>,
    pub(crate) provider: String,
    /// Its provider's own plugins, whose hooks run after the gateway's.
    pub(crate) plugins: Plugins,
    /// Its first input: its own, or the one that came with the request's head.
    pub(crate) input: Input,
    /// Filters of its outputs, whoever reads them.
    pub(crate) outputs: Layer,
    /// Filters of the later inputs it sends, shared with their reader.
    pub(crate) inputs: Layer,
    /// Whether it carries the request's live inputs.
    pub(crate) live: bool,
    /// Closed once it ends: its response's events and its inputs' reader
    /// stop, so a reader still waiting wakes.
    pub(crate) ended: Latch,
    /// Once it ended, in its request's `settling` until its end hooks
    /// finish, as its request's end hooks wait for them.
    pub(crate) settling: Option<InFlightGuard>,
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

    /// The gateway's plugins, then its provider's own.
    fn plugins(&self) -> impl Iterator<Item = &Arc<dyn Plugin>> + Send {
        self.client
            .gateway
            .plugins
            .iter()
            .chain(self.plugins.iter())
    }

    fn hook<'a>(&'a self, plugin: &'a dyn Plugin, end: &'a Result<()>) -> BoxFuture<'a, ()> {
        plugin.on_provider_end(self, end)
    }

    /// Its response and inputs are cut off, and filters still open end,
    /// dropping what they keep back, before its end hooks run. Its end
    /// hooks settle for its request from here.
    fn close(&mut self, end: &Result<()>) {
        self.ended.close();
        self.settling = Some(InFlight::enter(&self.client.settling));
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
    /// input type or output type
    /// which the typed accessors take.
    pub fn is_kind<T: Any>(&self) -> bool {
        is_input::<T>(&*self.format).is_ok()
    }

    /// This call's first input as `T`, the call's input type: its own
    /// ([`Call::with_input`](crate::Call::with_input)), else the one that
    /// came with the request's head. `Err` as [`ClientContext::input`].
    pub fn input<T: Any>(&self) -> Result<&T> {
        self.input.get()
    }

    /// Run `filter` as [`ClientContext::filter`] does, for this call alone:
    /// over its inputs after the client edge's filters, or its outputs,
    /// whoever reads them, before them.
    pub fn filter<T: Clone + Send + Sync + 'static>(
        &mut self,
        filter: impl Filter<T>,
    ) -> Result<()> {
        match is_input::<T>(&*self.format)? {
            false => locked(&self.outputs).add(filter, false),
            // Later inputs follow only a session's head.
            true if self.live => locked(&self.inputs).add(filter, true),
            true => self.input.filter::<T>(Filters::of(filter)),
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
