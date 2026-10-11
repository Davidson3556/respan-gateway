//! The request log: each request through the gateway, as the admin API and
//! stderr show it. The HTTP edge ([`track`]) sees its head, status, timing
//! and end; a plugin ([`Recorder`]) sees its format, model, provider and
//! usage; [`attribute`] names its client.

mod count;
mod edge;
mod log;
mod record;
mod recorder;

pub use edge::track;
pub use log::{RequestLog, attribute};
pub use record::{RequestRecord, Usage, rfc3339};
pub use recorder::{Recorder, steps};

/// How many finished requests are kept unless the config's
/// `request_history` says.
pub const KEPT: usize = 1000;
