//! Per-tenant providers and credentials, changed while the gateway runs.

use std::collections::HashMap;
use std::sync::{Arc, PoisonError, RwLock};

use respan_gateway_core::http::{HeaderMap, StatusCode, header::AUTHORIZATION};
use respan_gateway_core::{
    Body, ClientContext, Fault, Plugin, ProviderContext, Response, Transport, async_trait, http,
};
use respan_gateway_formats::conversation::{Conversation, set_model};
use respan_gateway_server::CallHeaders;

/// One tenant's settings.
#[derive(Debug, Clone, Default)]
pub struct Tenant {
    /// The provider that answers this tenant, if not the gateway's default.
    pub target: Option<String>,
    /// The model it asks for, if not the client's.
    pub model: Option<String>,
    /// The headers sent to each provider, by provider name, such as the
    /// tenant's own API key.
    pub keys: HashMap<String, HeaderMap>,
    /// The endpoint of each provider behind a [`ByEndpoint`] transport, by
    /// provider name, such as the tenant's own Azure deployment.
    pub endpoints: HashMap<String, String>,
}

/// The tenant a request belongs to, in its extensions.
#[derive(Debug, Clone)]
pub struct TenantOf(pub Arc<Tenant>);

/// Which endpoint of a [`ByEndpoint`] transport a call goes to, in its
/// extensions.
#[derive(Debug, Clone)]
pub struct Endpoint(pub String);

/// Tenants by their gateway key, the bearer token clients send. Updated at
/// any time through a clone; requests read the version current when they
/// start.
///
/// On the client edge it takes the gateway key off the request, so no
/// provider sees it, refuses an unknown one with 401, and sets the tenant's
/// target and model. On the provider edge it gives every call, the judge's
/// and fallbacks' included, the tenant's headers for that provider
/// ([`CallHeaders`]) and endpoint ([`Endpoint`]), or refuses a call to a
/// provider the tenant has no key for.
#[derive(Debug, Clone, Default)]
pub struct Tenants(Arc<RwLock<HashMap<String, Arc<Tenant>>>>);

impl Tenants {
    /// Add or replace the tenant with gateway key `key`.
    pub fn set(&self, key: impl Into<String>, tenant: Tenant) {
        let mut all = self.0.write().unwrap_or_else(PoisonError::into_inner);
        all.insert(key.into(), Arc::new(tenant));
    }

    /// Remove the tenant with gateway key `key`.
    pub fn remove(&self, key: &str) {
        let mut all = self.0.write().unwrap_or_else(PoisonError::into_inner);
        all.remove(key);
    }

    fn get(&self, key: &str) -> Option<Arc<Tenant>> {
        let all = self.0.read().unwrap_or_else(PoisonError::into_inner);
        all.get(key).cloned()
    }
}

#[async_trait]
impl Plugin for Tenants {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let head = cx.head_mut();
        let key = head.headers.remove(AUTHORIZATION);
        let key = key.as_ref().and_then(|v| v.to_str().ok());
        let key = key
            .and_then(|v| v.strip_prefix("Bearer "))
            .unwrap_or_default();
        let unknown = || Fault::new(StatusCode::UNAUTHORIZED, "unauthorized", "unknown key");
        let tenant = self.get(key).ok_or_else(unknown)?;
        head.extensions.insert(TenantOf(tenant.clone()));
        if let Some(target) = &tenant.target {
            cx.set_target(target);
        }
        // The model is a conversation's; a request of another kind keeps
        // its own.
        if let Some(model) = &tenant.model
            && cx.is_kind::<Conversation>()
        {
            cx.filter(set_model(model))?;
        }
        Ok(None)
    }

    async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
        // A call made before `on_client` resolved the tenant, by a plugin
        // ordered earlier, is refused rather than sent with no key or the
        // client's (FINDINGS F9).
        let Some(TenantOf(tenant)) = px.extensions().get::<TenantOf>().cloned() else {
            return Err(Fault::new(
                StatusCode::UNAUTHORIZED,
                "unauthorized",
                "the tenant is not known yet",
            ));
        };
        let provider = px.provider().to_owned();
        let Some(keys) = tenant.keys.get(&provider) else {
            return Err(Fault::new(
                StatusCode::FORBIDDEN,
                "no_key",
                format!("no key for provider '{provider}'"),
            ));
        };
        px.extensions_mut().insert(CallHeaders(keys.clone()));
        if let Some(endpoint) = tenant.endpoints.get(&provider) {
            px.extensions_mut().insert(Endpoint(endpoint.clone()));
        }
        Ok(())
    }
}

/// One provider reached at one of several endpoints, chosen per call by
/// its [`Endpoint`], such as a tenant's own deployment. Thousands of
/// tenants share one provider name, as the core's targets are names fixed
/// when the gateway is built (FINDINGS F9).
#[derive(Clone, Default)]
pub struct ByEndpoint {
    endpoints: HashMap<String, Arc<dyn Transport>>,
}

impl std::fmt::Debug for ByEndpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let names: Vec<&String> = self.endpoints.keys().collect();
        f.debug_struct("ByEndpoint")
            .field("endpoints", &names)
            .finish()
    }
}

impl ByEndpoint {
    /// No endpoints.
    pub fn new() -> Self {
        Self::default()
    }

    /// Reach endpoint `name` through `transport`.
    pub fn endpoint(mut self, name: impl Into<String>, transport: impl Transport) -> Self {
        self.endpoints.insert(name.into(), Arc::new(transport));
        self
    }
}

#[async_trait]
impl Transport for ByEndpoint {
    async fn send(&self, call: http::Request<Body>) -> Result<http::Response<Body>, Fault> {
        let name = call.extensions().get::<Endpoint>().map(|e| e.0.as_str());
        let transport = name.and_then(|n| self.endpoints.get(n));
        let transport =
            transport.ok_or_else(|| Fault::config("the call names no known endpoint"))?;
        transport.send(call).await
    }
}
