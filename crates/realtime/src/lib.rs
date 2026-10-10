#![warn(missing_docs)]
//! Directional realtime events with explicit wire-format identity.
//! Input travels client to provider; output travels provider to client.

use respan_gateway_core::{FormatId, NativeValue};
use serde_json::Value;

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
