//! Server-sent events, for any format that streams them.

use crate::Body;
use futures::stream::BoxStream;
use respan_gateway_http::bytes::{Bytes, BytesMut};
use respan_gateway_http::http;
use respan_gateway_sdk::Fault;
use serde_json::{self, Value};

/// A response's content type, if it is readable.
pub fn content_type(head: &http::response::Parts) -> Option<&str> {
    let value = head.headers.get(http::header::CONTENT_TYPE)?;
    value.to_str().ok()
}

/// Whether a response is a stream: a success labelled `text/event-stream`.
pub fn is_stream(head: &http::response::Parts) -> bool {
    let essence = content_type(head).and_then(|c| c.split(';').next());
    let sse = essence.is_some_and(|e| e.trim().eq_ignore_ascii_case("text/event-stream"));
    sse && head.status.is_success()
}

/// Splits a body into events. At the end, a last event of whole lines is
/// kept without its blank line; one cut mid-line is dropped, as clients do.
#[derive(Debug, Default)]
pub struct Parser {
    buf: BytesMut,
    /// Where to resume searching `buf` for the end of an event.
    scanned: usize,
}

impl Parser {
    /// Create an incremental synchronous event splitter.
    pub fn new() -> Self {
        let (buf, scanned) = (BytesMut::new(), 0);
        Self { buf, scanned }
    }

    /// Split complete events from one wire frame.
    pub fn push(&mut self, bytes: Bytes) -> Vec<Bytes> {
        self.split(&bytes, false)
    }
    /// Finish the body, retaining a trailing event of whole lines only.
    pub fn finish(&mut self) -> Vec<Bytes> {
        self.split(&[], true)
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

/// Parse SSE through ordinary stateful functions. Wire failure does not flush
/// an incomplete event; the terminal function receives the original fault.
pub fn read<T, S, P, E>(
    body: Body,
    state: S,
    event: P,
    end: E,
) -> BoxStream<'static, Result<T, Fault>>
where
    T: Send + 'static,
    S: Send + 'static,
    P: FnMut(&mut S, Bytes, &mut Vec<T>) -> Result<(), Fault> + Send + 'static,
    E: FnOnce(&mut S, Result<(), Fault>, &mut Vec<T>) -> Result<(), Fault> + Send + 'static,
{
    crate::formats::read(
        body,
        (Parser::new(), state, event),
        |(parser, state, event), bytes, out| {
            for raw in parser.push(bytes) {
                event(state, raw, out)?;
            }
            Ok(())
        },
        move |(parser, state, event), wire, out| {
            if wire.is_ok() {
                for raw in parser.finish() {
                    event(state, raw, out)?;
                }
            }
            end(state, wire, out)
        },
    )
}

/// Read SSE events for a successful event-stream response, otherwise read its
/// whole body. Non-success responses preserve their original provider answer.
pub fn or_whole<T, F>(
    format: &str,
    response: http::Response<Body>,
    mut on_message: F,
) -> BoxStream<'static, Result<T, Fault>>
where
    T: Send + 'static,
    F: FnMut(Bytes, &mut Vec<T>) -> Result<(), Fault> + Send + 'static,
{
    let (head, body) = response.into_parts();
    if is_stream(&head) {
        read(
            body,
            (),
            move |_, raw, out| on_message(raw, out),
            |_, wire, _| wire,
        )
    } else {
        crate::formats::whole::read(format, http::Response::from_parts(head, body), on_message)
    }
}

/// A comment of one line, which clients skip, such as a quiet stream's
/// opening bytes ([`Reply::open`](crate::Reply::open)).
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
pub fn event_json(raw: &[u8]) -> Option<Value> {
    serde_json::from_str(&event_data(raw)?).ok()
}

/// A failed body as JSON: a JSON body, a one-element array unwrapped
/// (Gemini), or the first event that says an error.
pub fn failure_json(raw: &Bytes) -> Option<Value> {
    if let Ok(v) = serde_json::from_slice::<Value>(raw) {
        return Some(match v {
            Value::Array(mut one) if one.len() == 1 => one.remove(0),
            v => v,
        });
    }
    // The body is whole: its last event ends with it.
    let mut split = Parser::new();
    let mut events = split.split(raw, false);
    events.extend(split.split(b"\n\n", false));
    let json = events.iter().filter_map(|event| event_json(event));
    json.into_iter().find(crate::formats::errors::says_error)
}

/// The `data:` of an event, lines joined, if it has any.
pub fn event_data(raw: &[u8]) -> Option<String> {
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

    type Splitter = Parser;

    fn frames() -> Splitter {
        Parser::new()
    }

    fn splitter(stream: &[u8]) -> Splitter {
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
        let mut d = Parser::new();
        let mut out = Vec::new();
        out.extend(d.push(Bytes::from_static(b"data: a\r\rdata: b\r\r")));
        assert_eq!(out.len(), 1);
        out.extend(d.finish());
        assert_eq!(&out[1][..], b"data: b\r\r");
    }

    #[test]
    fn a_last_event_of_whole_lines_is_kept_and_a_cut_one_dropped() {
        for (body, kept) in [
            (&b"data: [DONE]\n"[..], Some(&b"data: [DONE]\n"[..])),
            (b"data: [DO", None),
        ] {
            let mut d = Parser::new();
            let mut out = Vec::new();
            out.extend(d.push(Bytes::from_static(body)));
            out.extend(d.finish());
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
            crate::formats::json(again.as_bytes()),
            Some(serde_json::json!({"a": 2}))
        );
        assert_eq!(
            crate::formats::json(b"{\"b\":3}"),
            Some(serde_json::json!({"b": 3}))
        );
    }

    fn head(content_type: &'static str) -> http::response::Parts {
        let mut head = http::Response::new(()).into_parts().0;
        let value = http::HeaderValue::from_static(content_type);
        head.headers.insert(http::header::CONTENT_TYPE, value);
        head
    }

    #[tokio::test]
    async fn a_decoder_for_a_stream_or_a_whole_body_follows_the_head() {
        for (content_type, body, values) in [
            (
                "text/event-stream",
                &b"data: 1\n\n: ping\n\ndata: 2\n\n"[..],
                3,
            ),
            ("application/json", b"{\"a\":1}", 1),
        ] {
            let d = or_whole(
                "f",
                http::Response::from_parts(head(content_type), Body::full(body)),
                |raw, out: &mut Vec<Bytes>| {
                    out.push(raw);
                    Ok(())
                },
            );
            use futures::TryStreamExt;
            let out: Vec<_> = d.try_collect().await.expect("expected value");
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

    #[tokio::test]
    async fn a_stream_stops_at_its_first_failure() {
        let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let seen = calls.clone();
        let d = or_whole(
            "f",
            http::Response::from_parts(
                head("text/event-stream"),
                Body::full(Bytes::from_static(b": ping\n\ndata: x\n\ndata: y\n\n")),
            ),
            move |raw, out: &mut Vec<Bytes>| {
                seen.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                if event_data(&raw).is_some() {
                    return Err(Fault::upstream("unreadable"));
                }
                out.push(raw);
                Ok(())
            },
        );
        use futures::StreamExt;
        let out: Vec<_> = d.collect().await;
        assert!(out.len() == 2 && out[0].is_ok() && out[1].is_err());
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
            let mut d = Parser::new();
            let mut out = Vec::new();
            out.extend(d.push(Bytes::copy_from_slice(&stream[..cut])));
            out.extend(d.push(Bytes::copy_from_slice(&stream[cut..])));
            let decoded: Vec<u8> = out.iter().flat_map(|b| b.to_vec()).collect();
            assert_eq!((out.len(), &decoded[..]), (3, &stream[..]), "cut at {cut}");
        }
    }
}
