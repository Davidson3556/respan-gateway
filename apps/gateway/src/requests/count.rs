//! What each kind of response says it used: tokens, or seconds of audio.

use std::sync::Arc;

use respan_gateway_conversation as conversation;
use respan_gateway_core::{ClientContext, Fault};
use respan_gateway_decision as decision;
use respan_gateway_media::{embedding, image, speech, transcription};
use respan_gateway_realtime as realtime;

use super::log::Entry;
use super::record::Usage;

/// What one event of a response adds to its usage.
enum Counted {
    Tokens(u64, u64, u64),
    Seconds(f64),
}

impl Counted {
    fn tokens(usage: &respan_gateway_conversation::Usage) -> Self {
        Counted::Tokens(
            usage.input_tokens,
            usage.output_tokens,
            usage.cached_input_tokens,
        )
    }
}

impl Usage {
    fn add(&mut self, counted: Counted) {
        match counted {
            Counted::Tokens(input, output, cached) => {
                self.input_tokens += input;
                self.output_tokens += output;
                self.cached_input_tokens += cached;
            }
            Counted::Seconds(seconds) => *self.seconds.get_or_insert(0.0) += seconds,
        }
    }
}

/// Counts into `entry` the usage each kind's response reports.
pub(super) fn all(cx: &mut ClientContext, entry: &Arc<Entry>) -> Result<(), Fault> {
    count(cx, entry, |output: &conversation::Output| match output {
        conversation::Output::Usage(usage) => Some(Counted::tokens(usage)),
        _ => None,
    })?;
    count(cx, entry, |output: &image::Output| match output {
        image::Output::Usage(usage) => Some(Counted::tokens(usage)),
        _ => None,
    })?;
    count(cx, entry, |output: &transcription::Output| match output {
        transcription::Output::Usage(usage) => Some(Counted::tokens(usage)),
        transcription::Output::Seconds(seconds) => Some(Counted::Seconds(*seconds)),
        _ => None,
    })?;
    count(cx, entry, |output: &speech::Output| match output {
        speech::Output::Usage(usage) => Some(Counted::tokens(usage)),
        _ => None,
    })?;
    count(cx, entry, |output: &realtime::Output| match output {
        realtime::Output::Usage(usage) => Some(Counted::tokens(&usage.tokens)),
        _ => None,
    })?;
    count(cx, entry, |output: &embedding::Output| match output {
        embedding::Output::Usage(usage) => Some(Counted::tokens(usage)),
        _ => None,
    })?;
    count(cx, entry, |output: &decision::Output| match output {
        decision::Output::Usage(usage) => Some(Counted::tokens(usage)),
        _ => None,
    })?;
    Ok(())
}

/// Adds to `entry`'s usage what `read` finds in each output, if `T` is the
/// request's output type.
fn count<T: Clone + Send + Sync + 'static>(
    cx: &mut ClientContext,
    entry: &Arc<Entry>,
    read: fn(&T) -> Option<Counted>,
) -> Result<(), Fault> {
    if !cx.is_kind::<T>() {
        return Ok(());
    }
    let entry = entry.clone();
    cx.filter(move |output: &mut T| {
        if let Some(counted) = read(output) {
            let mut record = entry.record();
            record.usage.get_or_insert_with(Usage::default).add(counted);
        }
        Ok(())
    })
}
