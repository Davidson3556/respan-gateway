#![warn(missing_docs)]
//! Framing that formats build on: whether a response is a stream of
//! server-sent events ([`sse`]), and a body's or an event's JSON. The formats
//! themselves are the `respan-gateway-*-http` crates'; traffic of no kind
//! relays through `respan-gateway-passthrough-http`.

pub mod sse;

use respan_gateway_http::http::{
    HeaderValue, header::CONTENT_TYPE, response::Parts as ResponseHead,
};

use serde_json::{self, Value};

/// A provider's message as JSON: a whole body, or an event's `data:`.
pub fn json(raw: impl AsRef<[u8]>) -> Option<Value> {
    let raw = raw.as_ref();
    serde_json::from_slice(raw)
        .ok()
        .or_else(|| respan_gateway_sse::event_json(raw))
}

/// Set a response's content type to `value` if it has none (RFC 9110 8.3).
pub fn default_content_type(head: &mut ResponseHead, value: &'static str) {
    let value = HeaderValue::from_static(value);
    head.headers.entry(CONTENT_TYPE).or_insert(value);
}
