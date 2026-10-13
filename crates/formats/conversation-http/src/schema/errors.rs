//! How a provider's error body reads: its code, message and fields. HTTP
//! reads no body's meaning ([`provider_fault`]); these are the shapes the
//! vendors' APIs and their compatible servers send, each named where it is
//! read: OpenAI's and Anthropic's `{"error": {..}}`, Gemini's and
//! Cloudflare's lists, Mistral's and FastAPI's `detail`, and a problem's
//! `title` (RFC 9457).

use respan_gateway_core::{Details, ErrorCode, Fault, RetryAfter};
use respan_gateway_http::bytes::Bytes;
use respan_gateway_http::http::{self, StatusCode, header::CONTENT_TYPE};
use respan_gateway_http_faults::{ProviderError, Status, provider_fault};
use respan_gateway_sse::{Parser, event_json};
use serde_json::Value;

use super::Schema;

/// Whether a JSON value says anything: not null or empty.
pub(crate) fn says(v: &Value) -> bool {
    match v {
        Value::Null => false,
        Value::Array(a) => !a.is_empty(),
        Value::Object(o) => !o.is_empty(),
        _ => true,
    }
}

/// A string that says something.
fn said(v: &Value) -> Option<&str> {
    v.as_str().filter(|s| !s.is_empty())
}

/// Whether a provider's JSON message says an error: an `error` that says
/// something, object or string, as compatible servers send.
pub(crate) fn says_error(v: &Value) -> bool {
    let error = &v["error"];
    says(error) && *error != false && *error != "" && error.as_f64() != Some(0.0)
}

/// `fault`, of a provider's answer that is no success ([`provider_fault`]),
/// read: the code, message and fields its body's JSON `v` says, under
/// `status` if an error's, else a 502; its own message if `v` says none, and
/// its `Retry-After` and the answer as it came, stay.
fn read(fault: Fault, status: StatusCode, v: &Value) -> Fault {
    // `{"error": {..}}` (OpenAI, Anthropic), the first of a list under
    // `error` or `errors` (Gemini, Cloudflare), or fields at the top.
    let fields = [&v["error"], &v["error"][0], &v["errors"][0]]
        .into_iter()
        .find(|e| e.is_object() && says(e))
        .or_else(|| (v["error"].is_null() && v.is_object()).then_some(v));
    let error = fields.unwrap_or(v);
    // Not an envelope's tag, such as `"type": "error"`, nor a problem
    // type's URI.
    let code = ["type", "error_type", "status", "code"]
        .iter()
        .filter_map(|k| error[*k].as_str())
        .find(|c| !c.is_empty() && *c != "error" && !c.contains(':'));
    // Its `message` (Mistral nests one in `detail[0].msg`), the error as
    // a string, a `detail` or its first `msg` (FastAPI), a problem's
    // `title`, or the body as a string; never empty.
    let message = said(&error["message"])
        .or(said(&v["message"]))
        .or(said(&error["message"]["detail"][0]["msg"]))
        .or(said(&v["error"]))
        .or(said(&v["detail"]))
        .or(said(&v["detail"][0]["msg"]))
        .or(said(&error["title"]))
        .or(said(v))
        .unwrap_or(fault.message())
        .to_owned();
    let to = match status.is_client_error() || status.is_server_error() {
        true => status,
        false => StatusCode::BAD_GATEWAY,
    };
    let mut read = match code {
        Some(code) => Fault::new(message).with(ErrorCode(code.to_owned().into())),
        None => Fault::upstream(message),
    };
    read = read.with(Status(to));
    if let Some(&wait) = fault.get::<RetryAfter>() {
        read = read.with(wait);
    }
    if let Some(error) = fault.get::<ProviderError>() {
        read = read.with(error.clone());
    }
    let Some((fields, object)) =
        fields.and_then(|fields| fields.as_object().map(|object| (fields, object)))
    else {
        return read;
    };
    let mut details = object.clone();
    // Fields at the top are the error's, not its null `error`.
    if std::ptr::eq(fields, v) {
        details.remove("error");
    }
    read.with(Details(details))
}

/// The fault of a provider's answer `v` of `status` that says an error in
/// a success, such as an error event: what
/// it says ([`answer`]), the answer, `raw`, going as it came to a client of
/// this format.
pub(crate) fn said_error(status: StatusCode, raw: Bytes, v: &Value) -> Fault {
    let mut answer = http::Response::new(raw);
    *answer.status_mut() = status;
    read(provider_fault(answer), status, v)
}

/// Merge the fault's details into the format's error fields: each is added
/// if missing, and a string or null replaces the format's but for
/// `message` and `type`.
pub fn merge_details(mut error: Value, fault: &Fault) -> Value {
    let (Some(merged), Some(details)) =
        (error.as_object_mut(), fault.get::<Details>().map(|d| &d.0))
    else {
        return error;
    };
    for (k, d) in details {
        let says_fault = matches!(k.as_str(), "message" | "type");
        if !merged.contains_key(k) || (!says_fault && (d.is_null() || d.is_string())) {
            merged.insert(k.clone(), d.clone());
        }
    }
    error
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
    let mut split = Parser::new(None);
    let mut events = split.push(raw.clone()).unwrap_or_default();
    events.extend(split.push(Bytes::from_static(b"\n\n")).unwrap_or_default());
    let json = events.iter().filter_map(|event| event_json(event));
    json.into_iter().find(says_error)
}

/// The fault of a provider's `answer` in `schema`'s format that is no
/// success, a format's error reader ([`ProviderIo::bind`]): the HTTP fault
/// ([`provider_fault`]) with what its body says, its code, message and
/// fields, under the status the format reads it as ([`Schema::status`]). The
/// answer, kept as it came, is JSON if it says no type, as the format's are.
///
/// [`ProviderIo::bind`]: respan_gateway_http::ProviderIo::bind
pub(crate) fn answer<C: Schema>(_: &C, answer: http::Response<Bytes>) -> Fault {
    let status = C::status(answer.status());
    read_answer(status, answer)
}

/// The fault of a provider's `answer` that is no success,
/// read leniently as the vendors and their compatible servers write errors
/// (`{"error": {..}}`, a string `error`, Gemini's and Cloudflare's lists,
/// FastAPI's `detail`, a problem's `title`): the HTTP fault
/// ([`provider_fault`]) with the code, message and fields its body says,
/// under its status if an error's, else a 502. Any HTTP format of a JSON
/// API can use it as its error reader ([`ProviderIo::bind`]).
///
/// [`ProviderIo::bind`]: respan_gateway_http::ProviderIo::bind
pub fn read_provider_error(answer: http::Response<Bytes>) -> Fault {
    let status = answer.status();
    read_answer(status, answer)
}

/// [`read_provider_error`], under `status`.
fn read_answer(status: StatusCode, mut answer: http::Response<Bytes>) -> Fault {
    let json = http::HeaderValue::from_static("application/json");
    answer.headers_mut().entry(CONTENT_TYPE).or_insert(json);
    let v = failure_json(answer.body()).unwrap_or_default();
    read(provider_fault(answer), status, &v)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// What an answer of `status` whose body is `v` says.
    fn provider_error(status: StatusCode, v: &Value) -> Fault {
        let mut answer = http::Response::new(Bytes::from(v.to_string()));
        *answer.status_mut() = status;
        read(provider_fault(answer), status, v)
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
    fn only_an_error_that_says_something_is_one() {
        for (error, says) in [
            (json!({}), false),
            (json!(""), false),
            (json!(false), false),
            (Value::Null, false),
            (json!("x"), true),
            (json!([]), false),
            (json!(true), true),
            (json!(0), false),
            (json!(0.0), false),
            (json!({ "message": "x" }), true),
        ] {
            assert_eq!(says_error(&json!({ "error": error })), says, "{error}");
        }
    }

    #[test]
    fn real_provider_errors_keep_their_code_and_words() {
        let answered = "the provider returned 400";
        for (body, code, message) in [
            (
                json!({"error": {"type": "invalid_request_error", "message": "m"}}),
                "invalid_request_error",
                "m",
            ),
            (
                json!({"type": "error", "error": {"type": "overloaded_error", "message": "busy"}}),
                "overloaded_error",
                "busy",
            ),
            (
                json!({"error": "model not found"}),
                "upstream",
                "model not found",
            ),
            (
                json!({"error": "boom", "error_type": "overloaded"}),
                "overloaded",
                "boom",
            ),
            (
                json!({"error": {"code": 429, "status": "RESOURCE_EXHAUSTED", "message": "quota"}}),
                "RESOURCE_EXHAUSTED",
                "quota",
            ),
            (
                json!({"errors": [{"code": "rate_limited", "message": "slow"}], "success": false}),
                "rate_limited",
                "slow",
            ),
            (json!({"detail": "Not found"}), "upstream", "Not found"),
            (
                json!({"detail": [{"msg": "field required"}]}),
                "upstream",
                "field required",
            ),
            (
                json!({"object": "error", "type": "BadRequestError", "message": "bad"}),
                "BadRequestError",
                "bad",
            ),
            (
                json!({"jsonrpc": "2.0", "error": {"code": -32600, "message": "invalid"}}),
                "upstream",
                "invalid",
            ),
            (
                json!({"object": "error", "message": {"detail": [{"msg": "Field required"}]}, "type": "invalid_request_message_error"}),
                "invalid_request_message_error",
                "Field required",
            ),
            (
                json!({"type": "https://example.com/probs/nope", "title": "Nope"}),
                "upstream",
                "Nope",
            ),
            (json!("Unauthorized"), "upstream", "Unauthorized"),
            (json!({"error": {}, "message": "top"}), "upstream", "top"),
            (
                json!({"error": {"code": "x"}, "message": "top"}),
                "x",
                "top",
            ),
            (Value::Null, "upstream", answered),
        ] {
            let fault = provider_error(StatusCode::BAD_REQUEST, &body);
            assert_eq!((fault.code(), fault.message()), (code, message), "{body}");
        }
    }

    #[test]
    fn an_envelopes_tag_is_no_error_code() {
        let code = |v: Value| provider_error(StatusCode::OK, &v).code().to_owned();
        assert_eq!(
            code(json!({"type": "error", "error": "Overloaded"})),
            "upstream"
        );
        assert_eq!(
            code(json!({"error": {"type": "overloaded_error"}})),
            "overloaded_error"
        );
        assert_eq!(
            code(json!({"error": {"status": "RESOURCE_EXHAUSTED", "code": 429}})),
            "RESOURCE_EXHAUSTED"
        );
        let cloudflare =
            json!({"errors": [{"code": "rate_limited", "message": "slow"}], "success": false});
        let fault = provider_error(StatusCode::TOO_MANY_REQUESTS, &cloudflare);
        assert_eq!((fault.code(), fault.message()), ("rate_limited", "slow"));
        assert_eq!(
            fault.get(),
            Some(&Details(
                cloudflare["errors"][0].as_object().expect("fields").clone()
            ))
        );
        let listed = provider_error(
            StatusCode::OK,
            &json!({"error": [{"type": "t", "message": "m"}]}),
        );
        assert_eq!((listed.code(), listed.message()), ("t", "m"));
        let message = provider_error(StatusCode::OK, &json!({"error": {"message": ""}}));
        assert_eq!(message.message(), "the provider returned 200");
    }
}
