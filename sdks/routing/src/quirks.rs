//! Providers that reject what their format sends.

use std::collections::HashSet;
use std::sync::Arc;

use respan_gateway_core::serde_json::Value;
use respan_gateway_core::{Fault, Plugin, ProviderContext, async_trait};

/// Removes top-level JSON fields from the body of every call to the named
/// providers, as it is sent, for a provider that rejects fields its format
/// writes, such as an OpenAI-compatible server refusing `stream_options`
/// or `user`. It edits the call's final request
/// ([`ProviderContext::edit_call`]), after the format wrote it.
#[derive(Debug, Clone)]
pub struct DropFields {
    providers: HashSet<String>,
    fields: Arc<[String]>,
}

impl DropFields {
    /// Drop `fields` from calls to `providers`.
    pub fn new<P: Into<String>, F: Into<String>>(
        providers: impl IntoIterator<Item = P>,
        fields: impl IntoIterator<Item = F>,
    ) -> Self {
        Self {
            providers: providers.into_iter().map(Into::into).collect(),
            fields: fields.into_iter().map(Into::into).collect(),
        }
    }
}

#[async_trait]
impl Plugin for DropFields {
    async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
        if !self.providers.contains(px.provider()) {
            return Ok(());
        }
        let fields = self.fields.clone();
        px.edit_call(move |req| {
            let body = req.body_mut();
            let has = |v: &Value| fields.iter().any(|f| v.get(f).is_some());
            if body.json().is_some_and(has)
                && let Some(Value::Object(v)) = body.json_mut()
            {
                v.retain(|k, _| !fields.contains(k));
            }
            Ok(())
        });
        Ok(())
    }
}
