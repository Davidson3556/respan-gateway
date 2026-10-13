#![warn(missing_docs)]
//! The relay plugin: a call to a provider of its client's own format is
//! [`Relayed`]. The wire then carries the client's key to a provider with
//! none of its own, and the provider's response head and error body back to
//! the client as they came.

use respan_gateway_core::{Fault, Plugin, ProviderContext, async_trait};
use respan_gateway_http::raw::Relayed;

/// Mark each call to a provider of its client's own format [`Relayed`]: its
/// client's key reaches a provider with none of its own, and the provider's
/// head and error answer reach the client as they came; on a WebSocket
/// transport (`respan_gateway_ws::WsTransport`), its close code too. A call
/// translated to another format is left as it is. The request itself is the
/// format's to write, its spec's headers and query parameters carried from
/// the client's.
#[derive(Clone, Copy, Debug, Default)]
pub struct Relay;

#[async_trait]
impl Plugin for Relay {
    async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
        if px.format() == px.client().format() {
            px.extensions_mut().insert(Relayed);
        }
        Ok(())
    }
}
