//! Server-sent events in a response: whether it is a stream. Reading one is
//! [`Sse`](crate::Sse)'s, and the codec `respan-gateway-sse`'s.

use respan_gateway_http::http;

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

#[cfg(test)]
mod tests {
    use super::*;
    use respan_gateway_sse::event;

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
    fn a_body_or_an_events_data_reads_as_json() {
        assert_eq!(
            crate::framing::json(b"{\"b\":3}"),
            Some(serde_json::json!({"b": 3}))
        );
        assert_eq!(
            crate::framing::json(event(None, "{\"a\":2}").as_bytes()),
            Some(serde_json::json!({"a": 2}))
        );
    }
}
