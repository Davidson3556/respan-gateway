//! Server-sent events, for any format that streams them.

use respan_gateway_core::bytes::{Bytes, BytesMut};
use respan_gateway_core::serde_json::{self, Value};
use respan_gateway_core::{Decoder, Fault, Part, http};

/// A response's content type, if it is readable.
pub(crate) fn content_type(head: &http::response::Parts) -> Option<&str> {
    let value = head.headers.get(http::header::CONTENT_TYPE)?;
    value.to_str().ok()
}

/// Whether a response is a stream: a success labelled `text/event-stream`.
/// Anything else, failures included, is read whole.
pub fn is_stream(head: &http::response::Parts) -> bool {
    let essence = content_type(head).and_then(|c| c.split(';').next());
    let sse = essence.is_some_and(|e| e.trim().eq_ignore_ascii_case("text/event-stream"));
    sse && head.status.is_success()
}

/// Splits a body into events for `on_event`. At the end of the body, a last
/// event of whole lines is kept without its blank line; one cut mid-line is
/// dropped, as SSE clients drop it.
pub(crate) struct SseDecoder<F> {
    buf: BytesMut,
    /// Where to resume searching `buf` for the end of an event.
    scanned: usize,
    on_event: F,
}

impl<F> std::fmt::Debug for SseDecoder<F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SseDecoder")
            .field("buffered", &self.buf.len())
            .finish_non_exhaustive()
    }
}

impl<F: FnMut(Bytes, &mut Vec<Part>)> SseDecoder<F> {
    /// A decoder handing each event's bytes to `on_event`.
    pub(crate) fn new(on_event: F) -> Self {
        Self {
            buf: BytesMut::new(),
            scanned: 0,
            on_event,
        }
    }

    /// Split off the first event ended by a blank line. At the end of the
    /// body, a last `\r` ends its line.
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

    fn events(&mut self, at_end: bool, out: &mut Vec<Part>) {
        while let Some(event) = self.next_event(at_end) {
            (self.on_event)(event, out);
        }
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

impl<F> Decoder for SseDecoder<F>
where
    F: FnMut(Bytes, &mut Vec<Part>) + Send + 'static,
{
    fn decode(&mut self, bytes: Bytes, out: &mut Vec<Part>) {
        self.buf.extend_from_slice(&bytes);
        self.events(false, out);
    }

    fn finish(&mut self, out: &mut Vec<Part>) {
        self.events(true, out);
        if self.buf.ends_with(b"\n") || self.buf.ends_with(b"\r") {
            (self.on_event)(self.buf.split().freeze(), out);
        }
        self.buf.clear();
    }
}

/// A decoder for a stream or a whole body, as [`is_stream`] reads the head.
///
/// In a stream, `on_message` gets each event that carries data; one without,
/// such as a keep-alive comment, passes on as a Frame. `on_message` pushes
/// `Part::End` at the event that ends the exchange, or the decoder is
/// wrapped in [`at_eof`]; later events are dropped. An `Err` from
/// `on_message` ends the response with that fault. `on_message` decides
/// whether an error inside a success is a failure, as for a chat format, or
/// content, as a JSON-RPC error reply is. A whole body goes to
/// [`whole::decoder`](crate::whole::decoder).
pub fn or_whole<F>(head: &http::response::Parts, mut on_message: F) -> Box<dyn Decoder>
where
    F: FnMut(Bytes, &mut Vec<Part>) -> Result<(), Fault> + Send + 'static,
{
    match is_stream(head) {
        true => {
            let mut ended = false;
            Box::new(SseDecoder::new(move |raw, out: &mut Vec<Part>| {
                if ended {
                    return;
                }
                if event_data(&raw).is_none() {
                    return out.push(Part::Frame(raw));
                }
                let from = out.len();
                if let Err(fault) = on_message(raw, out) {
                    out.push(Part::End(Err(fault)));
                }
                ended = out[from..].iter().any(|p| matches!(p, Part::End(_)));
            }))
        }
        false => crate::whole::decoder(head, on_message),
    }
}

/// Wraps `decoder` for a stream that ends with its body rather than with an
/// event, such as a server's own event stream. It ends `Ok` unless `decoder`
/// ended it first.
pub fn at_eof(decoder: Box<dyn Decoder>) -> Box<dyn Decoder> {
    Box::new(AtEof(decoder))
}

struct AtEof(Box<dyn Decoder>);

impl Decoder for AtEof {
    fn decode(&mut self, bytes: Bytes, out: &mut Vec<Part>) {
        self.0.decode(bytes, out);
    }

    fn finish(&mut self, out: &mut Vec<Part>) {
        self.0.finish(out);
        // Parts after the first end are ignored.
        out.push(Part::End(Ok(())));
    }
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

/// `data` as an event in the shape of `like`, an event the provider sent:
/// its fields other than `data`, such as `event` and `id`, are kept.
/// Without `like`, an event named `name`.
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

/// A failed body as JSON. A JSON body is taken whole, a one-element array
/// unwrapped (Gemini sends one). Otherwise, the first event that says an
/// error, past any ping; `None` if none does.
pub(crate) fn failure_json(raw: &Bytes) -> Option<Value> {
    if let Ok(v) = serde_json::from_slice::<Value>(raw) {
        return Some(match v {
            Value::Array(mut one) if one.len() == 1 => one.remove(0),
            v => v,
        });
    }
    let mut split = SseDecoder::new(|event, out: &mut Vec<Part>| out.push(Part::Frame(event)));
    let mut events = Vec::new();
    // The body is whole: its last event ends with it.
    split.decode(raw.clone(), &mut events);
    split.decode(Bytes::from_static(b"\n\n"), &mut events);
    events.iter().find_map(|part| match part {
        Part::Frame(event) => event_json(event).filter(crate::common::says_error),
        _ => None,
    })
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

    type Events = SseDecoder<fn(Bytes, &mut Vec<Part>)>;

    fn frames() -> Events {
        SseDecoder::new(|raw, out| out.push(Part::Frame(raw)))
    }

    fn splitter(stream: &[u8]) -> Events {
        let mut d = frames();
        d.buf.extend_from_slice(stream);
        d
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
        let mut d = frames();
        let mut out = Vec::new();
        d.decode(Bytes::from_static(b"data: a\r\rdata: b\r\r"), &mut out);
        assert_eq!(out.len(), 1);
        d.finish(&mut out);
        assert!(matches!(&out[1], Part::Frame(b) if &b[..] == b"data: b\r\r"));
    }

    #[test]
    fn a_last_event_of_whole_lines_is_kept_and_a_cut_one_dropped() {
        for (body, kept) in [
            (&b"data: [DONE]\n"[..], Some(&b"data: [DONE]\n"[..])),
            (b"data: [DO", None),
        ] {
            let mut d = frames();
            let mut out = Vec::new();
            d.decode(Bytes::from_static(body), &mut out);
            d.finish(&mut out);
            let got = out.first().map(|p| match p {
                Part::Frame(b) => &b[..],
                _ => b"",
            });
            assert_eq!(got, kept);
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

    #[test]
    fn a_decoder_for_a_stream_or_a_whole_body_follows_the_head() {
        let head = |content_type: &'static str| {
            let mut head = http::Response::new(()).into_parts().0;
            let value = http::HeaderValue::from_static(content_type);
            head.headers.insert(http::header::CONTENT_TYPE, value);
            head
        };
        for (content_type, body, parts) in [
            ("text/event-stream", &b"data: 1\n\ndata: 2\n\n"[..], 2),
            ("application/json", b"{\"a\":1}", 2),
        ] {
            let mut d = or_whole(&head(content_type), |raw, out| {
                out.push(Part::Frame(raw));
                Ok(())
            });
            let mut out = Vec::new();
            d.decode(Bytes::from_static(body), &mut out);
            d.finish(&mut out);
            assert_eq!(out.len(), parts, "{content_type}");
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
    fn a_stream_frames_comments_and_stops_at_its_end() {
        let mut head = http::Response::new(()).into_parts().0;
        let value = http::HeaderValue::from_static("text/event-stream");
        head.headers.insert(http::header::CONTENT_TYPE, value);
        let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let seen = calls.clone();
        let mut d = or_whole(&head, move |_, _| {
            seen.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Err(Fault::upstream("unreadable"))
        });
        let mut out = Vec::new();
        d.decode(
            Bytes::from_static(b": ping\n\ndata: x\n\ndata: y\n\n"),
            &mut out,
        );
        assert!(matches!(&out[..], [Part::Frame(_), Part::End(Err(_))]));
        assert_eq!(calls.load(std::sync::atomic::Ordering::Relaxed), 1);
    }

    #[test]
    fn an_error_in_a_success_is_the_formats_to_judge() {
        // A JSON-RPC error reply is content, not the provider failing.
        let reply = "data: {\"jsonrpc\":\"2.0\",\"id\":1,\"error\":{\"code\":-32601}}\n\n";
        let mut head = http::Response::new(()).into_parts().0;
        let value = http::HeaderValue::from_static("text/event-stream");
        head.headers.insert(http::header::CONTENT_TYPE, value);
        let mut d = or_whole(&head, |raw, out| {
            out.push(Part::Frame(raw));
            out.push(Part::End(Ok(())));
            Ok(())
        });
        let mut out = Vec::new();
        d.decode(Bytes::from_static(reply.as_bytes()), &mut out);
        assert!(matches!(&out[..], [Part::Frame(_), Part::End(Ok(()))]));
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
    fn a_stream_that_ends_with_its_body_ends_ok() {
        let mut d = at_eof(Box::new(frames()));
        let mut out = Vec::new();
        d.decode(Bytes::from_static(b"data: 1\n\n"), &mut out);
        d.finish(&mut out);
        assert!(matches!(&out[..], [Part::Frame(_), Part::End(Ok(()))]));
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
            let mut d = frames();
            let mut out = Vec::new();
            d.decode(Bytes::copy_from_slice(&stream[..cut]), &mut out);
            d.decode(Bytes::copy_from_slice(&stream[cut..]), &mut out);
            let mut decoded = Vec::new();
            for part in &out {
                if let Part::Frame(b) = part {
                    decoded.extend_from_slice(b);
                }
            }
            assert_eq!((out.len(), &decoded[..]), (3, &stream[..]), "cut at {cut}");
        }
    }
}
