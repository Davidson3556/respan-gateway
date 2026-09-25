use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use crate::edits::{apply_once, lock};
use crate::gateway::Inner;
use crate::store::{Class, Key, Scope};
use crate::{
    Doc, Edits, Envelope, Extensions, Fault, Outcome, Part, ProviderOutcome, ProviderResponse,
    Store, Target,
};

/// Who is calling. An auth plugin attaches it with
/// [`ClientRequest::insert_ext`]; tenant-scoped store keys read it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Principal {
    pub id: String,
    pub tenant: String,
}

/// A group of client requests that share state. For now every request is a
/// session of one; sockets, jobs and sealed ids come later.
#[derive(Debug, Clone)]
pub struct Session {
    id: String,
}

impl Session {
    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn key(&self, class: Class, name: impl Into<String>) -> Key {
        Key {
            scope: Scope::Session(self.id.clone()),
            class,
            name: name.into(),
        }
    }
}

pub(crate) type CloseFn = Box<dyn FnOnce(&ClientRequest, &Outcome) + Send>;
pub(crate) type ProviderCloseFn = Box<dyn FnOnce(&ProviderOutcome) + Send>;

/// One unit of work from a client. Not necessarily an HTTP request: it can
/// come from an embedding program or be one turn of a socket. Transport
/// details, when there are any, are in [`ClientRequest::ext`].
pub struct ClientRequest {
    id: String,
    route: String,
    format: &'static str,
    envelope: Envelope,
    body: Mutex<Doc>,
    ext: Mutex<Extensions>,
    session: Session,
    store: Arc<dyn Store>,
    from_client: Edits,
    to_client: Edits,
    on_close: Mutex<Vec<CloseFn>>,
    gateway: Arc<Inner>,
    /// Index of the chain entry currently running.
    position: AtomicUsize,
    next_index: AtomicUsize,
}

impl std::fmt::Debug for ClientRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientRequest")
            .field("id", &self.id)
            .field("route", &self.route)
            .field("format", &self.format)
            .field("envelope", &self.envelope)
            .finish()
    }
}

pub(crate) struct ClientRequestInit {
    pub id: String,
    pub route: String,
    pub format: &'static str,
    pub envelope: Envelope,
    pub body: Doc,
    pub ext: Extensions,
    pub store: Arc<dyn Store>,
    pub gateway: Arc<Inner>,
}

impl ClientRequest {
    pub(crate) fn new(init: ClientRequestInit) -> Self {
        Self {
            session: Session {
                id: init.id.clone(),
            },
            id: init.id,
            route: init.route,
            format: init.format,
            envelope: init.envelope,
            body: Mutex::new(init.body),
            ext: Mutex::new(init.ext),
            store: init.store,
            from_client: Edits::default(),
            to_client: Edits::default(),
            on_close: Mutex::default(),
            gateway: init.gateway,
            position: AtomicUsize::new(0),
            next_index: AtomicUsize::new(0),
        }
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn route(&self) -> &str {
        &self.route
    }

    /// The name of the Format the client spoke.
    pub fn format(&self) -> &str {
        self.format
    }

    pub fn model(&self) -> Option<&str> {
        self.envelope.model.as_deref()
    }

    pub fn is_stream(&self) -> bool {
        self.envelope.stream
    }

    pub fn envelope(&self) -> &Envelope {
        &self.envelope
    }

    /// The message, in the client's format. Do not hold the guard across an
    /// `.await`.
    pub fn body(&self) -> MutexGuard<'_, Doc> {
        lock(&self.body)
    }

    /// A value the caller or an earlier plugin attached, by type.
    pub fn ext<T: Clone + Send + Sync + 'static>(&self) -> Option<T> {
        lock(&self.ext).get::<T>().cloned()
    }

    /// Attach a value for later plugins, replacing any of the same type.
    pub fn insert_ext<T: Clone + Send + Sync + 'static>(&self, value: T) {
        lock(&self.ext).insert(value);
    }

    pub fn session(&self) -> &Session {
        &self.session
    }

    pub fn store(&self) -> &dyn Store {
        self.store.as_ref()
    }

    /// A key scoped to this request's tenant, or global if no [`Principal`]
    /// is attached.
    pub fn key(&self, class: Class, name: impl Into<String>) -> Key {
        let scope = match self.ext::<Principal>() {
            Some(p) => Scope::Tenant(p.tenant),
            None => Scope::Global,
        };
        Key {
            scope,
            class,
            name: name.into(),
        }
    }

    /// The request body on its way in from the client.
    pub fn from_client(&self) -> &Edits {
        &self.from_client
    }

    /// The response on its way out to the client.
    pub fn to_client(&self) -> &Edits {
        &self.to_client
    }

    /// Runs exactly once when the request ends, however it ends, after every
    /// provider request's own close hooks. Receives the request in its final
    /// state, so a hook registered early still sees what later plugins set.
    pub fn on_close(&self, f: impl FnOnce(&ClientRequest, &Outcome) + Send + 'static) {
        lock(&self.on_close).push(Box::new(f));
    }

    /// Send this request to one provider. The `on_provider_request` hooks of
    /// the chain entries that have run so far see it first. A provider error
    /// status is an `Ok` response; check [`ProviderResponse::status`].
    pub async fn call(&self, provider: &str) -> Result<ProviderResponse, Fault> {
        self.apply_from_client()?;
        let route = self
            .gateway
            .routes
            .get(&self.route)
            .ok_or_else(|| Fault::internal("route vanished"))?;
        let upto = self.position.load(Ordering::Relaxed);
        let index = self.next_index.fetch_add(1, Ordering::Relaxed);
        crate::providers::call(&self.gateway, route, self, upto, index, provider).await
    }

    pub(crate) fn set_position(&self, i: usize) {
        self.position.store(i, Ordering::Relaxed);
    }

    /// Apply pending `from_client` edits to the body, once.
    pub(crate) fn apply_from_client(&self) -> Result<(), Fault> {
        let edits = self.from_client.take();
        if edits.is_empty() {
            return Ok(());
        }
        let mut body = self.body();
        let mut part = Part::Doc(std::mem::replace(&mut *body, Doc::new(bytes::Bytes::new())));
        let result = apply_once(&mut part, edits);
        if let Part::Doc(d) = part {
            *body = d;
        }
        result
    }

    pub(crate) fn take_close_fns(&self) -> Vec<CloseFn> {
        std::mem::take(&mut *lock(&self.on_close))
    }
}

/// A request from the gateway to one provider. A client request can make
/// several: fallback, retries, hedging, splitting.
pub struct ProviderRequest<'a> {
    client: &'a ClientRequest,
    target: &'a Target,
    index: usize,
    body: Mutex<Doc>,
    to_provider: Edits,
    from_provider: Edits,
    on_close: Mutex<Vec<ProviderCloseFn>>,
}

impl std::fmt::Debug for ProviderRequest<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProviderRequest")
            .field("client", &self.client.id)
            .field("provider", &self.target.name)
            .field("index", &self.index)
            .finish()
    }
}

pub(crate) struct ProviderRequestParts {
    pub body: Doc,
    pub to_provider: Edits,
    pub from_provider: Edits,
    pub on_close: Vec<ProviderCloseFn>,
}

impl<'a> ProviderRequest<'a> {
    pub(crate) fn new(client: &'a ClientRequest, target: &'a Target, index: usize) -> Self {
        let body = client.body().clone();
        Self {
            client,
            target,
            index,
            body: Mutex::new(body),
            to_provider: Edits::default(),
            from_provider: Edits::default(),
            on_close: Mutex::default(),
        }
    }

    pub fn client(&self) -> &ClientRequest {
        self.client
    }

    pub fn provider(&self) -> &Target {
        self.target
    }

    /// 0 for the first provider request of this client request, then 1, 2...
    pub fn index(&self) -> usize {
        self.index
    }

    /// This provider request's own copy of the body.
    pub fn body(&self) -> MutexGuard<'_, Doc> {
        lock(&self.body)
    }

    /// The request body on its way to the provider.
    pub fn to_provider(&self) -> &Edits {
        &self.to_provider
    }

    /// The response on its way back from the provider.
    pub fn from_provider(&self) -> &Edits {
        &self.from_provider
    }

    /// Runs exactly once when this provider request ends: failed, unused,
    /// finished, or abandoned by the client.
    pub fn on_close(&self, f: impl FnOnce(&ProviderOutcome) + Send + 'static) {
        lock(&self.on_close).push(Box::new(f));
    }

    pub(crate) fn into_parts(self) -> ProviderRequestParts {
        let into = |m: Mutex<Doc>| {
            m.into_inner()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
        };
        ProviderRequestParts {
            body: into(self.body),
            to_provider: self.to_provider,
            from_provider: self.from_provider,
            on_close: self
                .on_close
                .into_inner()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        }
    }
}
