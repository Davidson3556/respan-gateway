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

use respan_gateway_core::http::header::AUTHORIZATION;
use respan_gateway_core::http::{self, HeaderMap, HeaderValue, StatusCode};
use respan_gateway_core::{
    ClientContext, Fault, Gateway, Plugin, ProviderContext, Response, async_trait,
};
use respan_gateway_formats::OpenAiChat;
use respan_gateway_server::CallHeaders;
use respan_gateway_testkit::{MockTransport, mock, post, read_whole};

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
        let key = cx.head_mut().headers.remove(AUTHORIZATION);
        let key = key
            .as_ref()
            .and_then(|v| v.to_str().ok()?.strip_prefix("Bearer "));
        let unknown = || Fault::new(StatusCode::UNAUTHORIZED, "unauthorized", "unknown key");
        let tenant = self.get(key.unwrap_or_default()).ok_or_else(unknown)?;
        cx.set_target(tenant.target.clone());
        cx.head_mut().extensions.insert(tenant);
        Ok(None)
    }

    async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
        let keys = px
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
    Ok(HeaderValue::from_str(&format!("Bearer {key}")).map_err(http::Error::from)?)
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
    Ok(read_whole(gw.handle(req).await.0).await.0)
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
mod tests {
    use super::*;

    fn auth(req: &http::Request<respan_gateway_core::bytes::Bytes>) -> Option<&str> {
        req.headers()
            .get(AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
    }

    #[tokio::test]
    async fn each_tenant_gets_its_target_and_key_and_edits_apply_live() -> Result<(), Fault> {
        let mock = MockTransport::new();
        for base in [OPENAI, OPENAI, AZURE] {
            mock.reply(base, mock::json(StatusCode::OK, OK));
        }
        let tenants = Tenants::default();
        let gw = gateway(&mock, tenants.clone())?;
        assert!(tenants.upsert("gw-acme", tenant(1, "openai", "sk-acme")?));
        assert!(tenants.upsert("gw-beta", tenant(1, "openai", "sk-beta")?));
        assert_eq!(send(&gw, "gw-acme").await?, StatusCode::OK);
        assert_eq!(send(&gw, "gw-beta").await?, StatusCode::OK);

        assert!(tenants.upsert("gw-acme", tenant(3, "azure", "az-acme")?));
        // A stale edit arriving late changes nothing.
        assert!(!tenants.upsert("gw-acme", tenant(2, "openai", "sk-old")?));
        assert_eq!(send(&gw, "gw-acme").await?, StatusCode::OK);

        let sent = mock.sent();
        let seen: Vec<_> = sent.iter().map(|r| (r.uri().host(), auth(r))).collect();
        assert_eq!(
            seen,
            [
                (Some("openai.test"), Some("Bearer sk-acme")),
                (Some("openai.test"), Some("Bearer sk-beta")),
                (Some("azure.test"), Some("Bearer az-acme")),
            ]
        );
        Ok(())
    }

    /// Edits the tenant mid-request, as the control plane may at any time.
    struct EditsMidway(Tenants);

    #[async_trait]
    impl Plugin for EditsMidway {
        async fn on_client(&self, _cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            self.0.upsert("gw-acme", tenant(2, "azure", "az-acme")?);
            Ok(None)
        }
    }

    #[tokio::test]
    async fn a_request_keeps_the_version_it_started_with() -> Result<(), Fault> {
        let mock = MockTransport::new();
        mock.reply(OPENAI, mock::json(StatusCode::OK, OK));
        let tenants = Tenants::default();
        tenants.upsert("gw-acme", tenant(1, "openai", "sk-acme")?);
        let gw = gateway(&mock, tenants.clone())?.plugin("edits", EditsMidway(tenants.clone()))?;
        assert_eq!(send(&gw, "gw-acme").await?, StatusCode::OK);
        assert_eq!(auth(&mock.sent()[0]), Some("Bearer sk-acme"));
        assert_eq!(tenants.get("gw-acme").map(|t| t.version), Some(2));
        Ok(())
    }

    #[tokio::test]
    async fn unknown_keys_and_providers_without_a_key_are_refused() -> Result<(), Fault> {
        let mock = MockTransport::new();
        let tenants = Tenants::default();
        let gw = gateway(&mock, tenants.clone())?;
        assert_eq!(send(&gw, "gw-nobody").await?, StatusCode::UNAUTHORIZED);
        let mut keyless = tenant(1, "openai", "sk")?;
        keyless.keys.clear();
        tenants.upsert("gw-keyless", keyless);
        assert_eq!(send(&gw, "gw-keyless").await?, StatusCode::FORBIDDEN);
        assert!(mock.sent().is_empty());
        Ok(())
    }
}
