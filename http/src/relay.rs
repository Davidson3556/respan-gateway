//! Explicit HTTP forwarding policy for local proxies and live sessions.
use crate::{self as h, ClientHead, Credential, ProviderError, ProviderHead, SendRaw, Wire};
use respan_gateway_sdk::conversation::Conversation;
use respan_gateway_sdk::{ClientContext, Fault, Plugin, Response, async_trait};

/// Opt into raw request, credentials, response head and error body forwarding.
#[derive(Debug)]
pub struct Relay {
    raw: bool,
}
impl Relay {
    /// Forward the client's original request, its key, and the provider's
    /// head and error body. A whole body goes out as the client sent it
    /// ([`SendRaw`]), so it refuses a conversation that a plugin before it
    /// edited, rather than drop the edit. Filters that plugins after it
    /// add in `on_provider` are not checked: register it last. The request
    /// keeps the client's path and headers as they came, so its provider
    /// sits at the vendor's origin.
    pub const RAW: Self = Self { raw: true };
    /// Encode typed input, forwarding only response heads and raw errors.
    pub const HEADS: Self = Self { raw: false };
}
#[async_trait]
impl Plugin for Relay {
    async fn on_provider(&self, px: &mut respan_gateway_sdk::ProviderContext) -> Result<(), Fault> {
        // A raw request, the client's key, and the provider's head go only
        // to and from a provider of the client's own format.
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
        }
        if px.is_kind::<respan_gateway_sdk::conversation::Conversation>() {
            let client = px.client().format().to_owned();
            px.filter::<respan_gateway_sdk::conversation::Reply>(RawErrors { client })?;
        }
        Ok(())
    }
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let Some(target) = cx.target().map(str::to_owned) else {
            return Ok(None);
        };
        let mut call = cx.call(target);
        if self.raw {
            refuse_edited(cx)?;
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
        let client = cx.format().to_owned();
        let raw = |fault| raw_fault(fault, &client);
        let mut response = call.with_extension(slot.clone()).await.map_err(raw)?;
        if let Some(client) = cx.extensions().get::<ClientHead>() {
            client.set(slot.get());
        }
        if slot.get().is_some_and(|h| {
            !h.status.is_success() && h.status != h::http::StatusCode::SWITCHING_PROTOCOLS
        }) {
            response.until_end().await.map_err(raw)?;
        }
        Ok(Some(response))
    }
}
/// `Err` if a plugin edited the client's conversation, which a raw relay
/// would send unedited. Other kinds keep no original to compare with.
fn refuse_edited(cx: &ClientContext) -> Result<(), Fault> {
    let Ok(now) = cx.input::<Conversation>() else {
        return Ok(());
    };
    match now.said() {
        Some(said) if said != now => Err(Fault::config(
            "a plugin edited the request, which a raw relay would send unedited",
        )),
        _ => Ok(()),
    }
}

/// `fault` with its provider's error body to write as it came, if that
/// provider spoke the `client`'s format.
fn raw_fault(fault: Fault, client: &str) -> Fault {
    let Some(mut error) = fault.get::<ProviderError>().cloned() else {
        return fault;
    };
    if error.format != client {
        return fault;
    }
    if error.response.status().is_success() {
        *error.response.status_mut() = h::status(&fault);
    }
    fault.with(h::RawReply(error.response))
}

/// Explicitly forward provider error bodies from a typed response filter,
/// only from a provider of the `client`'s format.
#[derive(Debug)]
pub struct RawErrors {
    /// The client's format name.
    pub client: String,
}
impl<T: 'static> respan_gateway_sdk::Filter<T> for RawErrors {
    fn edit(&mut self, value: T, out: &mut Vec<T>) -> Result<(), Fault> {
        out.push(value);
        Ok(())
    }
    fn end(&mut self, end: &Result<(), Fault>, _: &mut Vec<T>) -> Result<(), Fault> {
        end.clone().map_err(|fault| raw_fault(fault, &self.client))
    }
}
