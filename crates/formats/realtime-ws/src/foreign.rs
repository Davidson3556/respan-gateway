//! What a format does with another format's natives: its provider's writer
//! drops those that cannot change the answer and refuses the rest; its
//! client's writer drops them all, as what the model said is said.

use respan_gateway_core::{Fault, NativeFields};
use respan_gateway_realtime::{Input, Output, Session, Turns};
use serde_json::{Map, Value};

use crate::json::untranslatable_input;

/// The fields of other formats' natives a format's provider drops, each a
/// path or the start of one, such as `session.tracing`; a null one is
/// always dropped, and any other refused.
#[derive(Debug)]
pub(crate) struct Drops {
    /// The format that writes.
    pub(crate) format: &'static str,
    /// Of a session's native.
    pub(crate) session: &'static [&'static str],
    /// Of its voice detection's.
    pub(crate) vad: &'static [&'static str],
    /// Of a message's own.
    pub(crate) message: &'static [&'static str],
    /// Of any other event's.
    pub(crate) event: &'static [&'static str],
}

impl Drops {
    /// `event` with other formats' native fields dropped; refused, naming
    /// the first, if one could change the answer. A tool's are all
    /// refused, and a native event is left to the writer.
    pub(crate) fn admit(&self, event: &Input) -> Result<Input, Fault> {
        let mut event = event.clone();
        match &mut event {
            Input::Session(s) => self.session(s)?,
            Input::Message { message, native } => {
                self.drop(&mut message.native, self.message)?;
                self.drop(native, self.event)?;
            }
            Input::ToolResult(result) => self.drop(&mut result.native, self.event)?,
            Input::Audio { native, .. }
            | Input::Commit { native }
            | Input::Respond { native }
            | Input::Cancel { native } => self.drop(native, self.event)?,
            _ => {}
        }
        Ok(event)
    }

    fn session(&self, s: &mut Session) -> Result<(), Fault> {
        self.drop(&mut s.native, self.session)?;
        if let Some(Turns::Server(vad)) = &mut s.turns {
            self.drop(&mut vad.native, self.vad)?;
        }
        for tool in s.tools.iter_mut().flatten() {
            self.drop(&mut tool.native, &[])?;
        }
        Ok(())
    }

    fn drop(&self, native: &mut Option<NativeFields>, droppable: &[&str]) -> Result<(), Fault> {
        let Some(fields) = native.as_ref().filter(|n| *n.format != *self.format) else {
            return Ok(());
        };
        if let Some(path) = kept(&fields.fields, "", droppable) {
            return Err(untranslatable_input(self.format, &format!("`{path}`")));
        }
        *native = None;
        Ok(())
    }
}

/// The path of the first field of `fields`, under `at`, that is not null
/// and that `droppable` does not name: the innermost, as a refusal names it.
fn kept(fields: &Map<String, Value>, at: &str, droppable: &[&str]) -> Option<String> {
    fields.iter().find_map(|(key, value)| {
        let path = format!("{at}{key}");
        if value.is_null() || droppable.contains(&path.as_str()) {
            return None;
        }
        match value {
            Value::Object(fields) if !fields.is_empty() => {
                kept(fields, &format!("{path}."), droppable)
            }
            _ => Some(path),
        }
    })
}

/// `event` with every other format's native fields dropped, for `format`'s
/// client; `None` for another format's native event.
pub(crate) fn strip(format: &str, event: &Output) -> Option<Output> {
    let mut event = event.clone();
    let mine = |native: &mut Option<NativeFields>| {
        if native.as_ref().is_some_and(|n| *n.format != *format) {
            *native = None;
        }
    };
    match &mut event {
        Output::Opened(s) | Output::Updated(s) => {
            mine(&mut s.native);
            if let Some(Turns::Server(vad)) = &mut s.turns {
                mine(&mut vad.native);
            }
            for tool in s.tools.iter_mut().flatten() {
                mine(&mut tool.native);
            }
        }
        Output::SpeechStarted { native, .. }
        | Output::SpeechStopped { native, .. }
        | Output::InputTranscript { native, .. }
        | Output::ResponseStart { native, .. }
        | Output::Audio { native, .. }
        | Output::Text { native, .. }
        | Output::Transcript { native, .. }
        | Output::ToolCall { native, .. }
        | Output::ResponseDone { native, .. }
        | Output::Error { native, .. } => mine(native),
        Output::Native(native) if *native.format != *format => return None,
        _ => {}
    }
    Some(event)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn a_field_is_dropped_where_named_or_null_and_else_refused() {
        let fields = |v: Value| v.as_object().cloned().expect("object");
        let droppable = ["event_id", "session.tracing"];
        let kept = |v| kept(&fields(v), "", &droppable);
        assert_eq!(
            kept(json!({"event_id": "e", "session": {"tracing": "auto"}})),
            None
        );
        assert_eq!(kept(json!({"session": {"prompt": null}})), None);
        assert_eq!(
            kept(json!({"session": {"tracing": 1, "prompt": {"id": "p"}}})),
            Some("session.prompt.id".into())
        );
        assert_eq!(kept(json!({"response": {}})), Some("response".into()));
    }
}
