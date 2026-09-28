//! Server-sent events, for any format that streams them.

use respan_gateway_core::bytes::{Bytes, BytesMut};
use respan_gateway_core::serde_json::{self, Value};
use respan_gateway_core::{Decoder, Fault, http};

/// A response's content type, if it is readable.
pub(crate) fn content_type(head: &http::response::Parts) -> Option<&str> {
    let value = head.headers.get(http::header::CONTENT_TYPE)?;
    value.to_str().ok()
}

/// Whether a response is a stream: a success labelled `text/event-stream`.
pub fn is_stream(head: &http::response::Parts) -> bool {
    let essence = content_type(head).and_then(|c| c.split(';').next());
    let sse = essence.is_some_and(|e| e.trim().eq_ignore_ascii_case("text/event-stream"));
    sse && head.status.is_success()
}

/// What a stream's events mean, for an [`SseDecoder`].
pub(crate) trait Events<T>: Send + 'static {
    /// The values of one event, as it came.
    fn event(&mut self, raw: Bytes, out: &mut Vec<T>) -> Result<(), Fault>;

    /// The stream ended: whether it was complete.
    fn end(&mut self, out: &mut Vec<T>) -> Result<(), Fault>;
}

/// Splits a body into events. At the end, a last event of whole lines is
/// kept without its blank line; one cut mid-line is dropped, as clients do.
pub(crate) struct SseDecoder<E> {
    buf: BytesMut,
    /// Where to resume searching `buf` for the end of an event.
    scanned: usize,
    events: E,
}

impl<E> SseDecoder<E> {
    pub(crate) fn new(events: E) -> Self {
        let (buf, scanned) = (BytesMut::new(), 0);
        Self {
            buf,
            scanned,
            events,
        }
    }

    /// Split off the first event ended by a blank line; at the end, a last
    /// `\r` ends its line.
    fn next_event(&mut self, at_end: bool) -> Option<Bytes> {
        let mut i = self.scanned;
        while let Some(first) = line_end(&self.buf[i..], at_end) {
            if first == 0 {
                i += 1;
                continue;
            }
            match line_end(&self.buf[i + first..], at_end) {
                None => break,
                Some(0) => i += first,
                Some(second) => {
                    self.scanned = 0;
                    return Some(self.buf.split_to(i + first + second).freeze());
                }
            }
        }
        self.scanned = i;
        None
    }

    /// The events of `bytes`, then, `at_end`, the last one left.
    fn split(&mut self, bytes: &[u8], at_end: bool) -> Vec<Bytes> {
        self.buf.extend_from_slice(bytes);
        let mut events = Vec::new();
        while let Some(event) = self.next_event(at_end) {
            events.push(event);
        }
        if at_end {
            if self.buf.ends_with(b"\n") || self.buf.ends_with(b"\r") {
                events.push(self.buf.split().freeze());
            }
            self.buf.clear();
        }
        events
    }
}

/// The length of the line ending at the start of `b`: `\r\n`, `\n` or `\r`,
/// or 0 for none. `None` if more bytes could change the answer.
fn line_end(b: &[u8], at_end: bool) -> Option<usize> {
    match b {
        [] => None,
        [b'\r'] => at_end.then_some(1),
        [b'\r', b'\n', ..] => Some(2),
        [b'\r' | b'\n', ..] => Some(1),
        _ => Some(0),
    }
}

impl<T: 'static, E: Events<T>> Decoder<T> for SseDecoder<E> {
    fn decode(&mut self, bytes: Bytes, out: &mut Vec<T>) -> Result<(), Fault> {
        for event in self.split(&bytes, false) {
            self.events.event(event, out)?;
        }
        Ok(())
    }

    fn end(&mut self, out: &mut Vec<T>) -> Result<(), Fault> {
        for event in self.split(&[], true) {
            self.events.event(event, out)?;
        }
        self.events.end(out)
    }
}

/// Each event to a closure; the stream ends with its body.
impl<T, F> Events<T> for F
where
    F: FnMut(Bytes, &mut Vec<T>) -> Result<(), Fault> + Send + 'static,
{
    fn event(&mut self, raw: Bytes, out: &mut Vec<T>) -> Result<(), Fault> {
        self(raw, out)
    }

    fn end(&mut self, _out: &mut Vec<T>) -> Result<(), Fault> {
        Ok(())
    }
}

/// A decoder for a stream, as [`is_stream`] reads the head, or a whole body
/// ([`whole::decoder`](crate::whole::decoder)). `on_message` gets each
/// event as it came, a comment's too, and pushes its value; its `Err` ends
/// the response. The stream ends `Ok` with its body.
pub fn or_whole<T, F>(
    format: &str,
    head: &http::response::Parts,
    on_message: F,
) -> Box<dyn Decoder<T>>
where
    T: 'static,
    F: FnMut(Bytes, &mut Vec<T>) -> Result<(), Fault> + Send + 'static,
{
    match is_stream(head) {
        true => Box::new(SseDecoder::new(on_message)),
        false => crate::whole::decoder(format, head, on_message),
    }
}

/// A comment of one line, which clients skip, such as a quiet stream's
/// opening bytes ([`Encoder::open`](respan_gateway_core::Encoder::open)).
pub fn comment(text: &str) -> String {
    format!(": {text}\n\n")
}

/// An event of `data`, one `data:` line per line, named `name` if given.
pub fn event(name: Option<&str>, data: impl std::fmt::Display) -> String {
    let data = data.to_string();
    let mut out = name.map(|n| format!("event: {n}\n")).unwrap_or_default();
    for line in data.split("\r\n").flat_map(|l| l.split(['\n', '\r'])) {
        out.push_str("data: ");
        out.push_str(line);
        out.push('\n');
    }
    out.push('\n');
    out
}

/// `data` as an event with the fields of `like` but its `data`, such as
/// `event` and `id`; without `like`, an event named `name`.
pub fn event_as(like: Option<&Bytes>, name: Option<&str>, data: impl std::fmt::Display) -> String {
    let Some(like) = like else {
        return event(name, data);
    };
    let like = String::from_utf8_lossy(like);
    // Any line ending the decoder reads, a bare `\r` too.
    let fields = like
        .split(['\n', '\r'])
        .filter(|l| !l.is_empty() && !l.starts_with("data:") && *l != "data");
    let mut out: String = fields.flat_map(|l| [l, "\n"]).collect();
    out.push_str(&event(None, data));
    out
}

/// An event's `data:` as JSON.
pub(crate) fn event_json(raw: &[u8]) -> Option<Value> {
    serde_json::from_str(&event_data(raw)?).ok()
}

/// A failed body as JSON: a JSON body, a one-element array unwrapped
/// (Gemini), or the first event that says an error.
pub(crate) fn failure_json(raw: &Bytes) -> Option<Value> {
    if let Ok(v) = serde_json::from_slice::<Value>(raw) {
        return Some(match v {
            Value::Array(mut one) if one.len() == 1 => one.remove(0),
            v => v,
        });
    }
    // The body is whole: its last event ends with it.
    let mut split = SseDecoder::new(());
    let mut events = split.split(raw, false);
    events.extend(split.split(b"\n\n", false));
    let json = events.iter().filter_map(|event| event_json(event));
    json.into_iter().find(crate::common::says_error)
}

/// The `data:` of an event, lines joined, if it has any.
pub(crate) fn event_data(raw: &[u8]) -> Option<String> {
    let text = String::from_utf8_lossy(raw);
    let data: Vec<&str> = text
        .split(['\n', '\r'])
        .filter_map(|l| l.strip_prefix("data:").or((l == "data").then_some("")))
        .map(|d| d.strip_prefix(' ').unwrap_or(d))
        .collect();
    (!data.is_empty()).then(|| data.join("\n"))
}

#[cfg(test)]
mod tests {
    use super::*;

    type Splitter = SseDecoder<()>;

    fn frames() -> Splitter {
        SseDecoder::new(())
    }

    fn splitter(stream: &[u8]) -> Splitter {
        let mut d = frames();
        d.buf.extend_from_slice(stream);
        d
    }

    /// Each event as bytes, and the stream ends with the body.
    struct Each;

    impl Events<Bytes> for Each {
        fn event(&mut self, raw: Bytes, out: &mut Vec<Bytes>) -> Result<(), Fault> {
            out.push(raw);
            Ok(())
        }

        fn end(&mut self, _out: &mut Vec<Bytes>) -> Result<(), Fault> {
            Ok(())
        }
    }

    #[test]
    fn every_legal_line_ending_ends_an_event() {
        for (stream, first) in [
            (&b"data: a\n\ndata: b"[..], &b"data: a\n\n"[..]),
            (b"data: a\r\n\r\ndata: b", b"data: a\r\n\r\n"),
            (b"data: a\r\rdata: b", b"data: a\r\r"),
            (b"data: a\n\r\ndata: b", b"data: a\n\r\n"),
        ] {
            let mut d = splitter(stream);
            assert_eq!(d.next_event(false).as_deref(), Some(first));
            assert_eq!(d.next_event(false), None);
        }
    }

    #[test]
    fn a_last_bare_cr_ends_its_event_when_the_body_ends() {
        let mut d = SseDecoder::new(Each);
        let mut out = Vec::new();
        let _ = d.decode(Bytes::from_static(b"data: a\r\rdata: b\r\r"), &mut out);
        assert_eq!(out.len(), 1);
        let _ = d.end(&mut out);
        assert_eq!(&out[1][..], b"data: b\r\r");
    }

    #[test]
    fn a_last_event_of_whole_lines_is_kept_and_a_cut_one_dropped() {
        for (body, kept) in [
            (&b"data: [DONE]\n"[..], Some(&b"data: [DONE]\n"[..])),
            (b"data: [DO", None),
        ] {
            let mut d = SseDecoder::new(Each);
            let mut out = Vec::new();
            let _ = d.decode(Bytes::from_static(body), &mut out);
            let _ = d.end(&mut out);
            assert_eq!(out.first().map(|b| &b[..]), kept);
        }
    }

    #[test]
    fn an_event_rewritten_in_the_shape_of_another_keeps_its_fields() {
        let like = Bytes::from_static(b"id: 7\nevent: message\ndata: {\"a\":1}\n\n");
        let again = event_as(Some(&like), None, "{\"a\":2}");
        assert_eq!(
            event_as(None, Some("message"), 1),
            "event: message\ndata: 1\n\n"
        );
        assert_eq!(again, "id: 7\nevent: message\ndata: {\"a\":2}\n\n");
        assert_eq!(
            crate::json(again.as_bytes()),
            Some(serde_json::json!({"a": 2}))
        );
        assert_eq!(crate::json(b"{\"b\":3}"), Some(serde_json::json!({"b": 3})));
    }

    fn head(content_type: &'static str) -> http::response::Parts {
        let mut head = http::Response::new(()).into_parts().0;
        let value = http::HeaderValue::from_static(content_type);
        head.headers.insert(http::header::CONTENT_TYPE, value);
        head
    }

    #[test]
    fn a_decoder_for_a_stream_or_a_whole_body_follows_the_head() {
        for (content_type, body, values) in [
            (
                "text/event-stream",
                &b"data: 1\n\n: ping\n\ndata: 2\n\n"[..],
                3,
            ),
            ("application/json", b"{\"a\":1}", 1),
        ] {
            let mut d = or_whole("f", &head(content_type), |raw, out: &mut Vec<Bytes>| {
                out.push(raw);
                Ok(())
            });
            let mut out = Vec::new();
            let _ = d.decode(Bytes::from_static(body), &mut out);
            assert!(d.end(&mut out).is_ok());
            assert_eq!(out.len(), values, "{content_type}");
        }
    }

    #[test]
    fn a_failure_is_read_by_its_error_event_whatever_its_line_endings() {
        for body in [
            &b"data: {\"type\":\"ping\"}\r\rdata: {\"error\":{\"message\":\"busy\"}}\r\r"[..],
            b"data: {\"type\":\"ping\"}\n\r\ndata: {\"error\":{\"message\":\"busy\"}}\n\r\n",
        ] {
            let v = failure_json(&Bytes::from_static(body));
            assert_eq!(
                v.map(|v| v["error"]["message"].clone()),
                Some("busy".into())
            );
        }
    }

    #[test]
    fn a_failure_is_read_by_its_error_event_however_it_ends() {
        let busy = |body: &'static [u8]| {
            failure_json(&Bytes::from_static(body)).map(|v| v["error"]["message"].clone())
        };
        let said = Some(Value::from("busy"));
        // No blank line after the last event, or no line end at all.
        assert_eq!(
            busy(b"data: {\"type\":\"ping\"}\n\ndata: {\"error\":{\"message\":\"busy\"}}\n"),
            said
        );
        assert_eq!(busy(b"data: {\"error\":{\"message\":\"busy\"}}"), said);
        // A null error is none.
        assert_eq!(
            busy(b"data: {\"error\":null}\n\ndata: {\"error\":{\"message\":\"busy\"}}\n\n"),
            said
        );
        // Pings alone say nothing.
        assert_eq!(busy(b"data: {\"type\":\"ping\"}\n\n"), None);
    }

    #[test]
    fn a_bare_data_line_is_empty_data() {
        assert_eq!(event_data(b"data\n\n").as_deref(), Some(""));
        let like = Bytes::from_static(b"id: 1\ndata\n\n");
        assert_eq!(event_as(Some(&like), None, "x"), "id: 1\ndata: x\n\n");
    }

    #[test]
    fn a_stream_stops_at_its_first_failure() {
        let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let seen = calls.clone();
        let mut d = or_whole(
            "f",
            &head("text/event-stream"),
            move |raw, out: &mut Vec<Bytes>| {
                seen.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                if event_data(&raw).is_some() {
                    return Err(Fault::upstream("unreadable"));
                }
                out.push(raw);
                Ok(())
            },
        );
        let mut out = Vec::new();
        let decoded = d.decode(
            Bytes::from_static(b": ping\n\ndata: x\n\ndata: y\n\n"),
            &mut out,
        );
        assert!(decoded.is_err() && out.len() == 1);
        assert_eq!(calls.load(std::sync::atomic::Ordering::Relaxed), 2);
    }

    #[test]
    fn a_stream_is_a_success_labelled_as_sse_in_any_case() {
        let head = |status: u16, content_type: &'static str| {
            let mut head = http::Response::new(()).into_parts().0;
            head.status = http::StatusCode::from_u16(status).unwrap_or_default();
            let value = http::HeaderValue::from_static(content_type);
            head.headers.insert(http::header::CONTENT_TYPE, value);
            head
        };
        assert!(is_stream(&head(200, "Text/Event-Stream; charset=utf-8")));
        assert!(!is_stream(&head(200, "text/event-streamx")));
        assert!(!is_stream(&head(503, "text/event-stream")));
    }

    #[test]
    fn an_event_rewritten_with_bare_cr_endings_loses_its_old_data() {
        let like = Bytes::from_static(b"event: message\rdata: {\"secret\":1}\r\r");
        let again = event_as(Some(&like), None, "{}");
        assert_eq!(again, "event: message\ndata: {}\n\n");
    }

    #[test]
    fn data_of_several_lines_is_an_event_of_several_data_lines() {
        assert_eq!(event(None, "not\njson"), "data: not\ndata: json\n\n");
        let like = Bytes::from_static(b"id: 7\ndata: x\n\n");
        let again = event_as(Some(&like), None, "a\n\ndata: b");
        assert_eq!(again, "id: 7\ndata: a\ndata: \ndata: data: b\n\n");
        assert_eq!(
            event_data(event(None, "a\r\nb").as_bytes()).as_deref(),
            Some("a\nb")
        );
    }

    #[test]
    fn a_split_crlf_waits_for_its_second_half() {
        let mut d = splitter(b"data: a\r\n\r");
        assert_eq!(d.next_event(false), None);
        d.buf.extend_from_slice(b"\n");
        assert_eq!(
            d.next_event(false).as_deref(),
            Some(&b"data: a\r\n\r\n"[..])
        );
    }

    #[test]
    fn a_stream_split_at_any_byte_decodes_every_event_once() {
        let stream = b"data: {\"a\":1}\n\nevent: x\r\ndata: {\"b\":2}\r\n\r\ndata: [DONE]\n\n";
        for cut in 0..=stream.len() {
            let mut d = SseDecoder::new(Each);
            let mut out = Vec::new();
            let _ = d.decode(Bytes::copy_from_slice(&stream[..cut]), &mut out);
            let _ = d.decode(Bytes::copy_from_slice(&stream[cut..]), &mut out);
            let decoded: Vec<u8> = out.iter().flat_map(|b| b.to_vec()).collect();
            assert_eq!((out.len(), &decoded[..]), (3, &stream[..]), "cut at {cut}");
        }
    }
}
