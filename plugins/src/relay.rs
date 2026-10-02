//! Explicit HTTP forwarding policy for local proxies and live sessions.
use respan_gateway_http::{
    self as h, ClientHead, Credential, ProviderError, ProviderHead, SendRaw, Wire,
};
use respan_gateway_sdk::{ClientContext, Fault, Plugin, Response, async_trait};

/// Opt into raw request, credentials, response head and error body forwarding.
#[derive(Debug)]
pub struct Relay {
    raw: bool,
}
impl Relay {
    /// Forward original requests (bypassing whole-body edits), keys, heads and errors.
    pub const RAW: Self = Self { raw: true };
    /// Encode typed input, forwarding only response heads and raw errors.
    pub const HEADS: Self = Self { raw: false };
}
#[async_trait]
impl Plugin for Relay {
    async fn on_provider(&self, px: &mut respan_gateway_sdk::ProviderContext) -> Result<(), Fault> {
        if px.is_kind::<respan_gateway_sdk::conversation::Conversation>() {
            px.filter::<respan_gateway_sdk::conversation::Reply>(RawErrors)?;
        }
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
            if cx.format() == "openai-chat" {
                h::formats::set_path(&mut head, "/chat/completions");
            }
            if cx.format() == "anthropic-messages" {
                head.headers
                    .entry("anthropic-version")
                    .or_insert(h::http::HeaderValue::from_static("2023-06-01"));
            }
            if cx.format() == "passthrough" {
                let path = h::formats::forwarded_path(head.uri.path());
                h::formats::set_path(&mut head, &path);
            }
            let raw = SendRaw(Wire::new(
                h::http::Request::from_parts(head, bytes),
                wire.is_live(),
            ));
            call = call.with_extension(raw);
        }
        let slot = ProviderHead::default();
        let mut response = call.with_extension(slot.clone()).await.map_err(raw_fault)?;
        if let Some(client) = cx.extensions().get::<ClientHead>() {
            client.set(slot.get());
        }
        if slot.get().is_some_and(|h| {
            !h.status.is_success() && h.status != h::http::StatusCode::SWITCHING_PROTOCOLS
        }) {
            response.until_end().await.map_err(raw_fault)?;
        }
        Ok(Some(response))
    }
}
fn raw_fault(fault: Fault) -> Fault {
    let Some(mut error) = fault.get::<ProviderError>().cloned() else {
        return fault;
    };
    if error.response.status().is_success() {
        *error.response.status_mut() = h::status(&fault);
    }
    fault.with(h::RawReply(error.response))
}

/// Explicitly forward provider error bodies from a typed response filter.
#[derive(Debug)]
pub struct RawErrors;
impl<T: 'static> respan_gateway_sdk::Filter<T> for RawErrors {
    fn edit(&mut self, value: T, out: &mut Vec<T>) -> Result<(), Fault> {
        out.push(value);
        Ok(())
    }
    fn end(&mut self, end: &Result<(), Fault>, _: &mut Vec<T>) -> Result<(), Fault> {
        end.clone().map_err(raw_fault)
    }
}
