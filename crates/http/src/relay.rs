//! Explicit HTTP forwarding policy for local proxies and live sessions.
use crate::{self as h, ClientHead, Credential, ProviderHead, RawErrors, SendRaw, Wire};
use respan_gateway_core::{ClientContext, Fault, Plugin, Response, async_trait};

/// Opt into raw request, credentials, response head and error body forwarding.
/// Its [`RawErrors`], granted only on a call to a provider of the client's
/// own format, are the permission a transport reads too: on a WebSocket one
/// (`respan_gateway_ws::WsTransport`), the provider's close code goes to the
/// client as a `CloseCode`.
///
/// It sends the target call itself: it is the routing for the requests it
/// handles, answering each with its call to the target, so register no
/// other router after it.
#[derive(Debug)]
pub struct Relay {
    raw: bool,
}
impl Relay {
    /// Forward the client's original request, its key, and the provider's
    /// head and error body. A whole body goes out as the client sent it
    /// ([`SendRaw`]), unedited: an edit a plugin made to the conversation is
    /// not sent, and keeping a raw relay and such plugins apart is the
    /// composer's responsibility. The request keeps the client's path and
    /// headers as they came, so its provider sits at the vendor's origin.
    pub const RAW: Self = Self { raw: true };
    /// Encode typed input, forwarding only response heads and raw errors.
    pub const HEADS: Self = Self { raw: false };
}
#[async_trait]
impl Plugin for Relay {
    async fn on_provider(
        &self,
        px: &mut respan_gateway_core::ProviderContext,
    ) -> Result<(), Fault> {
        // A raw request, the client's key, the provider's head and its raw
        // errors (bodies and close codes alike) go only to and from a
        // provider of the client's own format.
        if px.format() != px.client().format() {
            if px.extensions().get::<SendRaw>().is_some() {
                return Err(Fault::config(format!(
                    "a raw relay from a {} client cannot go to provider '{}', which speaks {}",
                    px.client().format(),
                    px.provider(),
                    px.format()
                )));
            }
            px.extensions_mut().remove::<ProviderHead>();
            return Ok(());
        }
        let client = px.client().format().to_owned();
        px.extensions_mut().insert(RawErrors { client });
        Ok(())
    }
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let Some(target) = cx.target().map(str::to_owned) else {
            return Ok(None);
        };
        let mut call = cx.call(target);
        if self.raw {
            let wire = cx
                .extensions()
                .get::<Wire>()
                .ok_or_else(|| Fault::internal("missing Wire"))?;
            let (mut head, bytes) = wire
                .get()
                .ok_or_else(|| Fault::internal("missing body"))?
                .clone()
                .into_parts();
            if let Some(credential) = cx.extensions().get::<Credential>() {
                credential.restore(&mut head);
            }
            let raw = SendRaw(Wire::new(
                h::http::Request::from_parts(head, bytes),
                wire.is_live(),
            ));
            call = call.with_extension(raw);
        }
        let slot = ProviderHead::default();
        let mut response = call.with_extension(slot.clone()).await?;
        if let Some(client) = cx.extensions().get::<ClientHead>() {
            client.set(slot.get());
        }
        if slot.get().is_some_and(|h| {
            !h.status.is_success() && h.status != h::http::StatusCode::SWITCHING_PROTOCOLS
        }) {
            response.until_end().await?;
        }
        Ok(Some(response))
    }
}
