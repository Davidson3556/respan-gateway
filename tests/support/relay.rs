//! Use the production forwarding policy in protocol fixtures.
#![allow(dead_code, unused_imports)]
#[path = "../../plugins/src/relay.rs"]
mod policy;
pub use policy::*;
