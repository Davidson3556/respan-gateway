//! Generic provider-error recognition, decoding, and detail preservation.

use respan_gateway_core::Fault;
use respan_gateway_http::{Status, http::StatusCode};
use serde_json::Value;

use crate::Details;

/// Whether a JSON value says anything: not null or empty.
pub fn says(v: &Value) -> bool {
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
pub fn says_error(v: &Value) -> bool {
    let error = &v["error"];
    says(error) && *error != false && *error != "" && error.as_f64() != Some(0.0)
}

/// The fault for a provider's error body in a response of `status`: an
/// error's status, else a 502, with the error's fields as details.
pub fn provider_error(status: StatusCode, v: &Value) -> Fault {
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
        .find(|c| !c.is_empty() && *c != "error" && !c.contains(':'))
        .unwrap_or(respan_gateway_core::codes::UPSTREAM);
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
        .or(said(v));
    let answered = || crate::whole::answered(status);
    let message = message.map_or_else(answered, str::to_owned);
    let to = match status.is_client_error() || status.is_server_error() {
        true => status,
        false => StatusCode::BAD_GATEWAY,
    };
    let fault = Fault::new(code.to_owned(), message).with(Status(to.as_u16()));
    let Some(fields) = fields else { return fault };
    let mut details = fields.clone();
    // Fields at the top are the error's, not its null `error`.
    if let Some(top) = details.as_object_mut().filter(|_| std::ptr::eq(fields, v)) {
        top.remove("error");
    }
    fault.with(Details(details))
}

/// Merge the fault's details into the format's error fields: each is added
/// if missing, and a string or null replaces the format's but for
/// `message` and `type`.
pub fn merge_details(mut error: Value, fault: &Fault) -> Value {
    let (Some(merged), Some(Value::Object(details))) =
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

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
        assert_eq!(fault.get(), Some(&Details(cloudflare["errors"][0].clone())));
        let listed = provider_error(
            StatusCode::OK,
            &json!({"error": [{"type": "t", "message": "m"}]}),
        );
        assert_eq!((listed.code(), listed.message()), ("t", "m"));
        let message = provider_error(StatusCode::OK, &json!({"error": {"message": ""}}));
        assert_eq!(message.message(), "the provider returned 200");
    }
}
