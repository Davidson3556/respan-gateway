//! What a request's response says it used, counted into its record as its
//! client gets it: tokens, or seconds of audio.

use std::sync::Arc;

use respan_gateway_core::{ClientContext, Fault};
use respan_gateway_plugin_pricing::{Used, on_usage};

use super::log::Entry;
use super::record::Usage;

/// Counts into `entry` the usage each kind's response reports.
pub(super) fn all(cx: &mut ClientContext, entry: &Arc<Entry>) -> Result<(), Fault> {
    let entry = entry.clone();
    on_usage(
        cx,
        Arc::new(move |used| {
            let mut record = entry.record();
            let usage = record.usage.get_or_insert_with(Usage::default);
            match used {
                Used::Usage(t) => {
                    usage.input_tokens += t.input_tokens;
                    usage.output_tokens += t.output_tokens;
                    usage.cached_input_tokens += t.cached_input_tokens;
                }
                Used::Seconds(seconds) => *usage.seconds.get_or_insert(0.0) += seconds,
            }
        }),
    )
}
