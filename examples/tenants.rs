//! Use case 13, thousands of tenants changing config all day: tenant policy
//! lives outside the gateway's static setup, in a map a control plane edits
//! while requests run.
//!
//! - Each edit carries a version; an older one arriving late is ignored, so
//!   updates can come from any event stream, one tenant at a time.
//! - A request reads its tenant once, in `on_client`, and keeps that
//!   snapshot in an extension: every call it makes (fallbacks, judges)
//!   sees the same version, and nothing is written back from the request path.
//! - Each call gets the tenant's key for its provider as [`CallHeaders`];
//!   the transports hold no keys, and the gateway's own key never leaves.
//!
//! `cargo run -p respan-gateway-examples --example tenants`

use std::collections::HashMap;
use std::sync::{Arc, PoisonError, RwLock};

use respan_gateway_core::{
    ClientContext, Fault, Gateway, Plugin, ProviderContext, Response, async_trait,
};
use respan_gateway_formats::OpenAiChat;
use respan_gateway_http::http::header::AUTHORIZATION;
use respan_gateway_http::http::{HeaderMap, HeaderValue, StatusCode};
use respan_gateway_http::{CallHeaders, Credential};
use respan_gateway_testkit::{MockTransport, mock, post, read_whole, serve};

/// One tenant's policy, at one version.
#[derive(Debug, Clone, Default)]
struct Tenant {
    version: u64,
    target: String,
    /// Headers for each provider it may call, such as its own API key.
    keys: HashMap<String, HeaderMap>,
}

/// Tenants by gateway key. Clones share the map.
#[derive(Clone, Default)]
struct Tenants(Arc<RwLock<HashMap<String, Arc<Tenant>>>>);

impl Tenants {
    /// Apply an edit from the control plane; `false` if it is older than
    /// the version held.
    fn upsert(&self, key: &str, tenant: Tenant) -> bool {
        let mut all = self.0.write().unwrap_or_else(PoisonError::into_inner);
        if all
            .get(key)
            .is_some_and(|held| held.version >= tenant.version)
        {
            return false;
        }
        all.insert(key.to_owned(), Arc::new(tenant));
        true
    }

    fn get(&self, key: &str) -> Option<Arc<Tenant>> {
        self.0
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(key)
            .cloned()
    }
}

#[async_trait]
impl Plugin for Tenants {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        // The gateway key is ours: take it off before anything is sent.
        let credential = cx.extensions_mut().remove::<Credential>();
        let key = credential.as_ref().and_then(Credential::bearer);
        let unknown = || Fault::new(StatusCode::UNAUTHORIZED, "unauthorized", "unknown key");
        let tenant = self.get(key.unwrap_or_default()).ok_or_else(unknown)?;
        cx.set_target(tenant.target.clone());
        cx.extensions_mut().insert(tenant);
        Ok(None)
    }

    async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
        let keys = px
            .client()
            .extensions()
            .get::<Arc<Tenant>>()
            .and_then(|t| t.keys.get(px.provider()));
        // No key, no call: never fall back to someone else's credentials.
        let keys = keys.cloned().ok_or_else(|| {
            Fault::new(StatusCode::FORBIDDEN, "no_key", "no key for this provider")
        })?;
        px.extensions_mut().insert(CallHeaders(keys));
        Ok(())
    }
}

const OPENAI: &str = "http://openai.test/v1";
const AZURE: &str = "http://azure.test/v1";

fn gateway(mock: &MockTransport, tenants: Tenants) -> Result<Gateway, Fault> {
    Gateway::new()
        .provider("openai", OpenAiChat, mock.at(OPENAI))?
        .provider("azure", OpenAiChat, mock.at(AZURE))?
        .serve(OpenAiChat)?
        .plugin("tenants", tenants)
}

fn bearer(key: &str) -> Result<HeaderValue, Fault> {
    HeaderValue::from_str(&format!("Bearer {key}"))
        .map_err(|e| respan_gateway_http::built_wrong(e.into()))
}

fn tenant(version: u64, target: &str, key: &str) -> Result<Tenant, Fault> {
    let headers = HeaderMap::from_iter([(AUTHORIZATION, bearer(key)?)]);
    let keys = HashMap::from([(target.to_owned(), headers)]);
    let target = target.into();
    Ok(Tenant {
        version,
        target,
        keys,
    })
}

async fn send(gw: &Gateway, gateway_key: &str) -> Result<StatusCode, Fault> {
    let body = r#"{"model":"gpt-x","messages":[{"role":"user","content":"hi"}]}"#;
    let mut req = post("/v1/chat/completions", body);
    req.headers_mut()
        .insert(AUTHORIZATION, bearer(gateway_key)?);
    Ok(read_whole(serve(gw, req).await.0).await.0)
}

const OK: &str =
    r#"{"id":"c","choices":[{"index":0,"message":{"role":"assistant","content":"hi"}}]}"#;

#[tokio::main]
async fn main() -> Result<(), Fault> {
    let mock = MockTransport::new();
    for base in [OPENAI, AZURE] {
        mock.reply(base, mock::json(StatusCode::OK, OK));
    }
    let tenants = Tenants::default();
    let gw = gateway(&mock, tenants.clone())?;
    tenants.upsert("gw-acme", tenant(1, "openai", "sk-acme-1")?);
    println!("v1: {}", send(&gw, "gw-acme").await?);
    // The control plane moves acme to Azure while the gateway runs.
    tenants.upsert("gw-acme", tenant(2, "azure", "az-acme-2")?);
    println!("v2: {}", send(&gw, "gw-acme").await?);
    for sent in mock.sent() {
        println!(
            "{} with {:?}",
            sent.uri(),
            sent.headers().get(AUTHORIZATION)
        );
    }
    Ok(())
}

#[cfg(test)]
#[path = "tests/tenants.rs"]
mod tests;
