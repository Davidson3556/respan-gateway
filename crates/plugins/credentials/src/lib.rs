#![warn(missing_docs)]
//! Client credentials: a client's own credential headers go to the
//! providers named as having no key of their own, on a call of the client's
//! format, as the client would have sent them to the vendor.

use std::collections::HashSet;

use respan_gateway_core::{Fault, Plugin, ProviderContext, async_trait};
use respan_gateway_http::http::header::SEC_WEBSOCKET_PROTOCOL;
use respan_gateway_http::{Credential, call::CallHeaders};

/// Sends a client's own credential headers on a call to a provider of the
/// client's format that has no key of its own, as the client would have
/// sent them to the vendor. No key crosses a translation, and a provider
/// not named never sees one.
///
/// It sets the call's [`CallHeaders`], so a plugin registered after it that
/// sets them too, such as a tenant's key, has the last word.
#[derive(Debug, Default)]
pub struct ClientCredentials {
    /// The core providers without a key of their own.
    keyless: HashSet<String>,
}

impl ClientCredentials {
    /// Forward a client's credentials to the core providers named `keyless`.
    pub fn new<S: Into<String>>(keyless: impl IntoIterator<Item = S>) -> Self {
        Self {
            keyless: keyless.into_iter().map(Into::into).collect(),
        }
    }
}

#[async_trait]
impl Plugin for ClientCredentials {
    async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
        let own_format = px.format() == px.client().format();
        if !own_format || !self.keyless.contains(px.provider()) {
            return Ok(());
        }
        let Some(credential) = px.extensions().get::<Credential>() else {
            return Ok(());
        };
        let mut headers = credential.headers().clone();
        // A subprotocol is the session's to offer, keys and all.
        headers.remove(SEC_WEBSOCKET_PROTOCOL);
        if !headers.is_empty() {
            px.extensions_mut().insert(CallHeaders(headers));
        }
        Ok(())
    }
}
