use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use http::Method;
use http::header::CONTENT_TYPE;

use crate::edits::lock;
use crate::gateway::Inner;
use crate::response::{ClosedLog, Closer};
use crate::store::{Class, Key, Scope};
use crate::{
    Closed, Extensions, Fault, Peer, Principal, Request, Response, ResponseHead, Session, Store,
};

/// Everything about one client request that is not the message itself,
/// shared by every hook: ids, session, store, extensions, and the ability to
/// call providers.
pub struct Context {
    id: String,
    route: String,
    session: Session,
    store: Arc<dyn Store>,
    ext: Mutex<Extensions>,
    client: Request,
    gateway: Arc<Inner>,
    /// Index of the chain entry currently running.
    position: AtomicUsize,
    /// How many chain entries have run `on_client_request`.
    ran: AtomicUsize,
    next_index: AtomicUsize,
    closed: ClosedLog,
}

impl std::fmt::Debug for Context {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Context")
            .field("id", &self.id)
            .field("route", &self.route)
            .finish()
    }
}

pub(crate) struct ContextInit {
    pub id: String,
    pub route: String,
    pub client: Request,
    pub ext: Extensions,
    pub store: Arc<dyn Store>,
    pub gateway: Arc<Inner>,
}

impl Context {
    pub(crate) fn new(init: ContextInit) -> Self {
        Self {
            session: Session {
                id: init.id.clone(),
            },
            id: init.id,
            route: init.route,
            store: init.store,
            ext: Mutex::new(init.ext),
            client: init.client,
            gateway: init.gateway,
            position: AtomicUsize::new(0),
            ran: AtomicUsize::new(0),
            next_index: AtomicUsize::new(0),
            closed: ClosedLog::default(),
        }
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn route(&self) -> &str {
        &self.route
    }

    pub fn session(&self) -> &Session {
        &self.session
    }

    /// The client's request.
    pub fn client(&self) -> &Request {
        &self.client
    }

    pub fn store(&self) -> &dyn Store {
        self.store.as_ref()
    }

    /// A key scoped to the caller's tenant, or global if no [`Principal`] is
    /// attached.
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

    /// A value the caller or an earlier plugin attached, by type.
    pub fn ext<T: Clone + Send + Sync + 'static>(&self) -> Option<T> {
        lock(&self.ext).get::<T>().cloned()
    }

    /// Attach a value for later hooks, replacing any of the same type.
    pub fn insert_ext<T: Clone + Send + Sync + 'static>(&self, value: T) {
        lock(&self.ext).insert(value);
    }

    /// Send the client's request to one provider. The `on_provider_request`
    /// hooks of the chain entries that have run so far see it first. A
    /// provider error status is an `Ok` response; check [`Response::status`].
    /// Dropping the response without responding with it cancels it.
    pub async fn call(&self, provider: &str) -> Result<Response, Fault> {
        self.client.apply_request_edits()?;
        let gw = &self.gateway;
        let route = gw
            .routes
            .get(&self.route)
            .ok_or_else(|| Fault::internal("route vanished"))?;
        let target = gw
            .targets
            .get(provider)
            .ok_or_else(|| Fault::config(format!("unknown provider '{provider}'")))?;
        let format = gw.format(&target.format)?;
        if format.name() != route.format.name() {
            return Err(Fault::config(format!(
                "provider '{provider}' speaks {}, route speaks {}: translation is not supported yet",
                format.name(),
                route.format.name()
            )));
        }

        let seen_by = (self.position.load(Ordering::Relaxed) + 1).min(route.chain.len());
        let index = self.next_index.fetch_add(1, Ordering::Relaxed);
        let mut envelope = self.client.envelope().clone();
        if target.model.is_some() {
            envelope.model = target.model.clone();
        }
        let body = self.client.body().clone();
        let req = Request::new(
            Peer::Provider(target.clone()),
            format.name(),
            envelope,
            body,
            index,
        );
        let fail = |req: Request, f: Fault| {
            lock(&self.closed).push(Closed {
                request: req,
                response: Err(f.clone()),
                seen_by,
            });
            Err(f)
        };

        for plugin in route.chain.iter().take(seen_by) {
            if let Err(f) = plugin.on_provider_request(self, &req).await {
                return fail(req, f);
            }
        }
        let built = (|| {
            req.apply_request_edits()?;
            let mut body = req.body();
            format.prepare_request(&mut body, target.model.as_deref(), req.is_stream())?;
            let url = format!(
                "{}{}",
                target.base_url.trim_end_matches('/'),
                format.provider_path(req.envelope())
            );
            let mut builder = http::Request::builder()
                .method(Method::POST)
                .uri(url)
                .header(CONTENT_TYPE, "application/json");
            for (k, v) in &target.headers {
                builder = builder.header(k, v);
            }
            builder
                .body(body.to_bytes())
                .map_err(|e| Fault::config(e.to_string()))
        })();
        let http_req = match built {
            Ok(r) => r,
            Err(f) => return fail(req, f),
        };
        let resp = match gw.transport.send(http_req).await {
            Ok(r) => r,
            Err(f) => return fail(req, f),
        };
        let (parts, body) = resp.into_parts();
        let edits = req.take_response_edits();
        Ok(Response::provider(
            ResponseHead {
                status: parts.status,
                headers: parts.headers,
            },
            format,
            body,
            edits,
            Closer {
                log: self.closed.clone(),
                request: req,
                seen_by,
            },
        ))
    }

    /// Chain entry `i` is about to run `on_client_request`.
    pub(crate) fn enter(&self, i: usize) {
        self.position.store(i, Ordering::Relaxed);
        self.ran.store(i + 1, Ordering::Relaxed);
    }

    pub(crate) fn ran(&self) -> usize {
        self.ran.load(Ordering::Relaxed)
    }

    pub(crate) fn take_closed(&self) -> Vec<Closed> {
        std::mem::take(&mut *lock(&self.closed))
    }
}
