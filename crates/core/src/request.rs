use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Instant;

use crate::edits::lock;
use crate::store::{Class, Key, Scope};
use crate::{Doc, Edits, Record, RequestHead, Store, Target};

/// Who is calling. Set by an auth plugin.
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

type CloseFn = Box<dyn FnOnce(&Record) + Send>;

/// A request from the client to the gateway. Plugins see it once per request.
pub struct ClientRequest {
    pub(crate) id: String,
    pub(crate) route: String,
    head: RequestHead,
    body: Mutex<Doc>,
    stream: bool,
    model: Option<String>,
    principal: Mutex<Option<Principal>>,
    session: Session,
    store: Arc<dyn Store>,
    labels: Mutex<BTreeMap<String, String>>,
    from_client: Edits,
    to_client: Edits,
    on_close: Mutex<Vec<CloseFn>>,
    pub(crate) started: Instant,
}

impl std::fmt::Debug for ClientRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientRequest")
            .field("id", &self.id)
            .field("route", &self.route)
            .field("model", &self.model)
            .field("stream", &self.stream)
            .finish()
    }
}

pub(crate) struct ClientRequestInit {
    pub id: String,
    pub route: String,
    pub head: RequestHead,
    pub body: Doc,
    pub stream: bool,
    pub model: Option<String>,
    pub store: Arc<dyn Store>,
}

impl ClientRequest {
    pub(crate) fn new(init: ClientRequestInit) -> Self {
        Self {
            session: Session {
                id: init.id.clone(),
            },
            id: init.id,
            route: init.route,
            head: init.head,
            body: Mutex::new(init.body),
            stream: init.stream,
            model: init.model,
            principal: Mutex::new(None),
            store: init.store,
            labels: Mutex::default(),
            from_client: Edits::default(),
            to_client: Edits::default(),
            on_close: Mutex::default(),
            started: Instant::now(),
        }
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn route(&self) -> &str {
        &self.route
    }

    pub fn head(&self) -> &RequestHead {
        &self.head
    }

    /// The request body. Do not hold the guard across an `.await`.
    pub fn body(&self) -> MutexGuard<'_, Doc> {
        lock(&self.body)
    }

    pub fn is_stream(&self) -> bool {
        self.stream
    }

    pub fn model(&self) -> Option<&str> {
        self.model.as_deref()
    }

    pub fn principal(&self) -> Option<Principal> {
        lock(&self.principal).clone()
    }

    pub fn set_principal(&self, p: Principal) {
        *lock(&self.principal) = Some(p);
    }

    pub fn session(&self) -> &Session {
        &self.session
    }

    pub fn store(&self) -> &dyn Store {
        self.store.as_ref()
    }

    /// A key scoped to this request's tenant, or global if no principal is set.
    pub fn key(&self, class: Class, name: impl Into<String>) -> Key {
        let scope = match self.principal() {
            Some(p) => Scope::Tenant(p.tenant),
            None => Scope::Global,
        };
        Key {
            scope,
            class,
            name: name.into(),
        }
    }

    pub fn label(&self, key: impl Into<String>, value: impl Into<String>) {
        lock(&self.labels).insert(key.into(), value.into());
    }

    /// The request body on its way in from the client.
    pub fn from_client(&self) -> &Edits {
        &self.from_client
    }

    /// The response on its way out to the client.
    pub fn to_client(&self) -> &Edits {
        &self.to_client
    }

    /// Runs exactly once when the request closes, however it ends.
    pub fn on_close(&self, f: impl FnOnce(&Record) + Send + 'static) {
        lock(&self.on_close).push(Box::new(f));
    }

    pub(crate) fn labels(&self) -> BTreeMap<String, String> {
        lock(&self.labels).clone()
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

    pub(crate) fn into_parts(self) -> (Doc, Edits, Edits) {
        let body = self
            .body
            .into_inner()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        (body, self.to_provider, self.from_provider)
    }
}
