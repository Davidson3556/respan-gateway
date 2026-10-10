#![warn(missing_docs)]
//! The realtime kind: directional events with explicit wire-format identity.
//! Input travels client to provider; output travels provider to client.
//!
//! Its wire is a WebSocket session, one JSON event per frame: [`JsonEvents`]
//! (behind the default `ws` feature), with the vendors' presets
//! ([`JsonEvents::OPENAI_REALTIME`], [`JsonEvents::RESPONSES_WS`],
//! [`JsonEvents::GEMINI_LIVE`]). Without that feature the crate is the kind's
//! types alone, with no I/O.

use respan_gateway_core::{FormatId, NativeValue};
use serde_json::Value;

#[cfg(feature = "ws")]
mod json_events;
#[cfg(feature = "ws")]
pub use json_events::JsonEvents;

macro_rules! event {
    ($(#[$doc:meta])* $name:ident) => {
        $(#[$doc])*
        #[derive(Debug, Clone, PartialEq)]
        pub struct $name {
            /// Provider-format event, including the identity required to encode it.
            pub event: NativeValue,
        }
        impl $name {
            /// A JSON event belonging to `format`.
            pub fn new(format: impl Into<FormatId>, json: Value) -> Self {
                Self { event: NativeValue::new(format, json) }
            }
            /// Inspect the native JSON payload.
            pub fn json(&self) -> &Value { &self.event.value }
            /// Edit the native JSON payload, keeping its format identity.
            pub fn json_mut(&mut self) -> &mut Value { std::sync::Arc::make_mut(&mut self.event.value) }
        }
    };
}
event!(/// One client event addressed to a provider.
    Input);
event!(/// One provider event addressed to a client.
    Output);
