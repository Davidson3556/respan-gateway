//! A per-tenant "no retention" policy.
//!
//! [`TenantAuth`] resolves the client's key to a tenant, removes the key,
//! and marks the request with [`Tenant`] and, for a tenant that requires
//! it, [`NoRetention`], through `head_mut().extensions`. The marks reach
//! every later hook: `on_client_end` through the final head, and each call
//! made after it through the call's copy of the extensions.
//!
//! [`ContentLog`] records prompts and completions except under
//! [`NoRetention`]; [`CallLog`] records each call's usage for billing,
//! always, and its prompt except under [`NoRetention`]. [`NoStore`] asks an
//! OpenAI provider not to store the call.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};

use respan_gateway_core::http::{StatusCode, header::AUTHORIZATION};
use respan_gateway_core::{
    ClientContext, Fault, Outcome, Plugin, ProviderContext, Response, Usage, async_trait,
};
use respan_gateway_formats::conversation::{Conversation, Reply};

use crate::guard::Readable;

/// The tenant a request belongs to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tenant(pub String);

/// The tenant requires that no prompt or completion is stored.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NoRetention;

/// Resolves `authorization: Bearer <key>` to a tenant; refuses unknown keys
/// with a 401.
#[derive(Debug, Clone, Default)]
pub struct TenantAuth {
    keys: HashMap<String, (Tenant, bool)>,
}

impl TenantAuth {
    /// No tenants yet.
    pub fn new() -> Self {
        Self::default()
    }

    /// Key `key` belongs to `tenant`, which requires no retention if
    /// `no_retention`.
    pub fn tenant(mut self, key: &str, tenant: &str, no_retention: bool) -> Self {
        let t = Tenant(tenant.to_owned());
        self.keys.insert(key.to_owned(), (t, no_retention));
        self
    }
}

#[async_trait]
impl Plugin for TenantAuth {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let key = cx
            .head()
            .headers
            .get(AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "));
        let Some((tenant, no_retention)) = key.and_then(|k| self.keys.get(k)).cloned() else {
            return Err(Fault::new(
                StatusCode::UNAUTHORIZED,
                "unauthorized",
                "unknown key",
            ));
        };
        let head = cx.head_mut();
        // The gateway's own key must not reach a provider.
        head.headers.remove(AUTHORIZATION);
        head.extensions.insert(tenant);
        if no_retention {
            head.extensions.insert(NoRetention);
        }
        Ok(None)
    }
}

/// One request as [`ContentLog`] stored it.
#[derive(Debug, Clone, PartialEq)]
pub struct Record {
    /// Its tenant, if resolved.
    pub tenant: Option<String>,
    /// The status the client got.
    pub status: Option<StatusCode>,
    /// Its usage.
    pub usage: Usage,
    /// The prompt, unless the tenant forbids retention.
    pub prompt: Option<String>,
    /// The completion's text, unless the tenant forbids retention.
    pub completion: Option<String>,
}

/// What a request's completion filter captured, for its end hook.
#[derive(Debug, Clone, Default)]
struct Captured(Arc<Mutex<String>>);

/// Records every conversation request's prompt and completion, except for
/// tenants requiring no retention. The mark is read again at the end, so a
/// log ordered before the auth plugin still honours it for what it stores;
/// what it held in memory meanwhile is its own (FINDINGS F7).
#[derive(Debug, Clone, Default)]
pub struct ContentLog(Arc<Mutex<Vec<Record>>>);

impl ContentLog {
    /// Every record so far.
    pub fn records(&self) -> Vec<Record> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

fn retains(ext: &respan_gateway_core::http::Extensions) -> bool {
    ext.get::<NoRetention>().is_none()
}

#[async_trait]
impl Plugin for ContentLog {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        // A log lets other kinds pass: it only records.
        if !retains(&cx.head().extensions) || !cx.is_kind::<Reply>() {
            return Ok(None);
        }
        let captured = Captured::default();
        let into = captured.0.clone();
        cx.filter(move |r: &mut Reply| {
            if let Reply::Text { text, .. } = r {
                into.lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .push_str(text);
            }
            Ok(())
        })?;
        // The only per-request slot shared with the end hook (FINDINGS F3).
        cx.head_mut().extensions.insert(captured);
        Ok(None)
    }

    async fn on_client_end(&self, cx: &ClientContext, outcome: &Outcome) {
        let ext = &cx.head().extensions;
        let keep = retains(ext);
        let prompt = cx.input::<Conversation>().ok().map(Readable::text);
        let completion = ext
            .get::<Captured>()
            .map(|c| c.0.lock().unwrap_or_else(PoisonError::into_inner).clone());
        let record = Record {
            tenant: ext.get::<Tenant>().map(|t| t.0.clone()),
            status: outcome.head.as_ref().map(|h| h.status),
            usage: outcome.usage.clone(),
            prompt: prompt.filter(|_| keep),
            completion: completion.filter(|_| keep),
        };
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(record);
    }
}

/// One call as [`CallLog`] stored it: billing, and content if allowed.
#[derive(Debug, Clone, PartialEq)]
pub struct CallRecord {
    /// Its tenant, if resolved when the call was made.
    pub tenant: Option<String>,
    /// The provider called.
    pub target: String,
    /// The provider's status.
    pub status: Option<StatusCode>,
    /// The call's usage.
    pub usage: Usage,
    /// The prompt sent, unless the tenant forbids retention.
    pub prompt: Option<String>,
}

/// Records every call on the provider edge: usage always, the prompt
/// unless the call carries [`NoRetention`].
#[derive(Debug, Clone, Default)]
pub struct CallLog(Arc<Mutex<Vec<CallRecord>>>);

impl CallLog {
    /// Every record so far.
    pub fn records(&self) -> Vec<CallRecord> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

#[async_trait]
impl Plugin for CallLog {
    async fn on_provider_end(&self, px: &ProviderContext, outcome: &Outcome) {
        let ext = px.extensions();
        let prompt = px.input::<Conversation>().ok().map(Readable::text);
        let record = CallRecord {
            tenant: ext.get::<Tenant>().map(|t| t.0.clone()),
            target: px.provider().to_owned(),
            status: outcome.head.as_ref().map(|h| h.status),
            usage: outcome.usage.clone(),
            prompt: prompt.filter(|_| retains(ext)),
        };
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(record);
    }
}

/// Asks OpenAI providers not to store calls of tenants requiring no
/// retention, by setting `store: false` in the call as sent, whatever the
/// client's format.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoStore;

#[async_trait]
impl Plugin for NoStore {
    async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
        if retains(px.extensions()) || px.format() != "openai-chat" {
            return Ok(());
        }
        px.edit_call(|call| {
            if let Some(body) = call.body_mut().json_mut().and_then(|v| v.as_object_mut()) {
                body.insert("store".into(), false.into());
            }
            Ok(())
        });
        Ok(())
    }
}
