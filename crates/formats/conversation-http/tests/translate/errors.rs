//! Translate errors behavior.

use super::*;

#[tokio::test]
async fn a_provider_error_reaches_the_client_in_its_format() {
    let h = Harness::new(Vec::new()).await;
    let error = r#"{"type":"error","error":{"type":"rate_limit_error","message":"slow down"}}"#;
    h.anthropic
        .answer(json_response(StatusCode::TOO_MANY_REQUESTS, error));

    let (status, body) = h.text(openai_request("claude-x", true)).await;

    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    let v: Value = serde_json::from_str(&body).unwrap_or_default();
    assert_eq!(
        v,
        json!({ "error": { "message": "slow down", "type": "rate_limit_error", "param": null, "code": "rate_limit_error" } })
    );
    assert!(matches!(h.log.only(), Err(f) if fault_code(&f) == "rate_limit_error"));
}

#[tokio::test]
async fn a_request_that_cannot_be_translated_is_refused() {
    let h = Harness::new(Vec::new()).await;
    let format = json!({ "type": "unknown_format" });
    let body = json!({ "model": "claude-x", "messages": [], "response_format": format });

    let (status, body) = h.text(call(CHAT, body.to_string())).await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(
        body.contains("'response_format' in the request cannot be translated"),
        "{body}"
    );
    assert!(h.sent().is_empty());
}

// The same request, edited by a plugin whose edit does not cause the
// failure, is still the client's fault: its own request fails the same way.
#[tokio::test]
async fn an_edit_that_does_not_cause_the_failure_leaves_it_the_clients_fault() {
    /// Edits every conversation.
    struct Edit;

    #[async_trait]
    impl Plugin for Edit {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            cx.filter(|chat: &mut Input| {
                chat.options.temperature = Some(0.5);
                Ok(())
            })?;
            Ok(None)
        }
    }

    let h = Harness::new(vec![Arc::new(Edit)]).await;
    let format = json!({ "type": "unknown_format" });
    let body = json!({ "model": "claude-x", "messages": [], "response_format": format });

    let (status, _) = h.text(call(CHAT, body.to_string())).await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(matches!(h.log.only(), Err(f) if fault_code(&f) == "bad_request"));
}

// An edit that itself makes the request untranslatable is, without
// provenance, refused like any request the format cannot write.
#[tokio::test]
async fn an_edit_that_causes_the_failure_is_refused_as_a_bad_request() {
    /// Adds a field Anthropic has no place for.
    struct Break;

    #[async_trait]
    impl Plugin for Break {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            cx.filter(|chat: &mut Input| {
                let format = json!({ "response_format": { "type": "json_object" } });
                let fields = format.as_object().cloned().unwrap_or_default();
                chat.native = Some(NativeFields::new("openai-chat", fields));
                Ok(())
            })?;
            Ok(None)
        }
    }

    let h = Harness::new(vec![Arc::new(Break)]).await;
    let body = json!({ "model": "claude-x", "max_tokens": 9, "messages": [{ "role": "user", "content": "hi" }] });

    let (status, _) = h.text(call(CHAT, body.to_string())).await;

    // Without the bytes a request came as, the gateway cannot tell an edit
    // from the client's own field: a request it cannot write is the client's.
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(matches!(h.log.only(), Err(f) if fault_code(&f) == "bad_request"));
}

#[tokio::test]
async fn a_cut_translated_stream_ends_with_an_error_in_the_client_format() {
    let h = Harness::new(Vec::new()).await;
    h.anthropic.answer(sse(&ANTHROPIC_STREAM[..3]));

    let (_, body) = h.text(openai_request("claude-x", true)).await;

    let last = events(&body).pop().map(|(_, d)| d).unwrap_or_default();
    assert_eq!(last["error"]["type"], "truncated");
    assert!(!body.contains("[DONE]"));
    assert!(matches!(h.log.only(), Err(f) if fault_code(&f) == "truncated"));
}

#[tokio::test]
async fn a_translated_stream_that_starts_with_what_cannot_be_translated_is_refused() {
    let h = Harness::new(Vec::new()).await;
    let redacted = "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"redacted_thinking\",\"data\":\"x\"}}\n\n";
    h.anthropic.answer(sse(&[ANTHROPIC_STREAM[0], redacted]));

    let (status, body) = h.text(openai_request("claude-x", true)).await;

    // Nothing was sent before the failure, so it is a refusal.
    assert_eq!(status, StatusCode::BAD_GATEWAY, "{body}");
    assert!(!body.contains("\"role\":\"assistant\""), "{body}");
}

#[tokio::test]
async fn a_translated_chunk_that_fails_partway_is_refused_whole() {
    let h = Harness::new(Vec::new()).await;
    let chunk = r#"data: {"id":"c","model":"gpt-x","choices":[{"index":0,"delta":{"content":"hi","refusal":"no"}}]}

"#;
    h.openai.answer(sse(&[chunk]));

    let (status, body) = h.text(anthropic_request(true)).await;

    // Its text was not sent before its refusal failed.
    assert_eq!(status, StatusCode::BAD_GATEWAY, "{body}");
    assert!(!body.contains("message_start"), "{body}");
}

#[tokio::test]
async fn an_error_in_a_success_is_a_failure_not_an_empty_answer() {
    let h = Harness::new(Vec::new()).await;
    let error = r#"{"error":{"message":"boom","type":"server_error"}}"#;
    h.openai.answer(json_response(StatusCode::OK, error));

    let (status, body) = h.text(anthropic_request(false)).await;

    assert_eq!(status, StatusCode::BAD_GATEWAY, "{body}");
    assert!(body.contains("\"type\":\"server_error\""), "{body}");
    assert!(h.log.only().is_err());
}

#[tokio::test]
async fn a_success_that_is_not_a_response_is_a_failure_not_an_empty_answer() {
    let h = Harness::new(Vec::new()).await;
    h.openai
        .answer(json_response(StatusCode::OK, r#"{"status":"queued"}"#));

    let (status, body) = h.text(anthropic_request(false)).await;

    assert_eq!(status, StatusCode::BAD_GATEWAY, "{body}");
    assert!(h.log.only().is_err());
}

#[tokio::test]
async fn an_openai_error_reaches_an_anthropic_client_in_its_format() {
    let error = r#"{"error":{"message":"bad key","type":"invalid_request_error"}}"#;
    let h = Harness::with_local(
        vec![Arc::new(Cross)],
        Some(local_json(StatusCode::UNAUTHORIZED, error)),
        None,
    )
    .await;

    let (status, body) = h.text(anthropic_request(false)).await;

    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let v: Value = serde_json::from_str(&body).unwrap_or_default();
    assert_eq!(
        v,
        json!({ "type": "error", "error": { "type": "invalid_request_error", "message": "bad key" } })
    );
}

#[tokio::test]
async fn an_anthropic_error_event_reaches_an_openai_client_with_its_code() {
    let mut stream = ANTHROPIC_STREAM[..3].to_vec();
    stream.push("event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\",\"message\":\"Overloaded\"}}\n\n");
    let h = Harness::new(Vec::new()).await;
    h.anthropic.answer(sse(&stream));

    let (_, body) = h.text(openai_request("claude-x", true)).await;

    let last = events(&body).pop().map(|(_, d)| d).unwrap_or_default();
    assert_eq!(last["error"]["type"], "overloaded_error");
    assert!(matches!(h.log.only(), Err(f) if fault_code(&f) == "overloaded_error"));
}

#[tokio::test]
async fn a_filtered_stream_that_ends_in_a_provider_error_ends_with_that_error() {
    let mut stream = ANTHROPIC_STREAM[..3].to_vec();
    stream.push("event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\",\"message\":\"Overloaded\"}}\n\n");
    let h = Harness::new(vec![Arc::new(Shout)]).await;
    h.anthropic.answer(sse(&stream));
    let body = json!({
        "model": "claude-x",
        "max_tokens": 10,
        "stream": true,
        "messages": [{ "role": "user", "content": "hi" }],
    });

    let (_, out) = h.text(call(MESSAGES, body.to_string())).await;

    let (name, data) = events(&out).pop().unwrap_or_default();
    assert_eq!(name.as_deref(), Some("error"));
    assert_eq!(data["error"]["type"], "overloaded_error");
    assert!(matches!(h.log.only(), Err(f) if fault_code(&f) == "overloaded_error"));
}

#[tokio::test]
async fn a_system_message_mid_conversation_is_refused_not_moved() {
    let h = Harness::new(Vec::new()).await;
    let body = json!({
        "model": "claude-x",
        "messages": [
            { "role": "user", "content": "hi" },
            { "role": "system", "content": "be brief" },
        ],
    });

    let (status, _) = h.text(call(CHAT, body.to_string())).await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(h.sent().is_empty());
}

#[tokio::test]
async fn usage_counts_even_when_its_chunk_cannot_be_translated() {
    let h = Harness::new(Vec::new()).await;
    // A provider that reports usage on a tool call chunk.
    let chunk = "data: {\"id\":\"c1\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"arguments\":\"{}\"}}]}}],\"usage\":{\"prompt_tokens\":9,\"completion_tokens\":1}}\n\n";
    h.openai.answer(sse(&[chunk]));

    let _ = h.text(anthropic_request(true)).await;

    assert_eq!(h.usage.total().input_tokens, 9);
    assert!(h.log.only().is_err());
}

#[tokio::test]
async fn anthropics_own_end_is_kept_when_it_breaks_off_after_it() {
    let h = Harness::new(vec![Arc::new(Unmask)]).await;
    let cut = &ANTHROPIC_STREAM[..ANTHROPIC_STREAM.len() - 1];
    h.anthropic.answer(sse(cut));
    let body = json!({
        "model": "claude-x", "max_tokens": 10, "stream": true,
        "messages": [{ "role": "user", "content": "hi" }],
    });

    let (_, out) = h.text(call(MESSAGES, body.to_string())).await;

    let names: Vec<String> = events(&out).into_iter().filter_map(|(n, _)| n).collect();
    assert_eq!(names[names.len() - 2..], ["message_delta", "error"]);
}

#[tokio::test]
async fn a_relayed_stream_cut_after_its_last_block_keeps_every_event() {
    let h = Harness::new(Vec::new()).await;
    let cut = &ANTHROPIC_STREAM[..ANTHROPIC_STREAM.len() - 1];
    h.anthropic.answer(sse(cut));
    let body = json!({
        "model": "claude-x", "max_tokens": 10, "stream": true,
        "messages": [{ "role": "user", "content": "hi" }],
    });

    let (_, out) = h.text(call(MESSAGES, body.to_string())).await;

    let (kept, error) = out.split_once("event: error").unwrap_or((&out, ""));
    assert_eq!(semantic(kept), semantic(&cut.concat()), "{out}");
    assert!(error.contains("truncated"), "{out}");
}

#[tokio::test]
async fn a_providers_error_body_is_the_whole_body_whatever_a_filter_says() {
    struct Audited;

    #[async_trait]
    impl Plugin for Audited {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            cx.filter(Audit)?;
            Ok(None)
        }
    }

    let limited = r#"{"type":"error","error":{"type":"rate_limit_error","message":"slow down"}}"#;
    let h = Harness::with_local(
        vec![Arc::new(Cross), Arc::new(Audited)],
        None,
        Some(local_json(StatusCode::TOO_MANY_REQUESTS, limited)),
    )
    .await;
    let body = json!({
        "model": "claude-x", "max_tokens": 10,
        "messages": [{ "role": "user", "content": "hi" }],
    });

    let (status, out) = h.text(call(MESSAGES, body.to_string())).await;

    assert_eq!(
        (status, out.as_str()),
        (StatusCode::TOO_MANY_REQUESTS, limited)
    );
}

#[tokio::test]
async fn any_translated_3xx_with_a_location_is_a_bad_gateway() {
    let choices = http::Response::builder()
        .status(StatusCode::MULTIPLE_CHOICES)
        .header("location", "http://elsewhere/")
        .body(Vec::new())
        .unwrap_or_else(|e| panic!("{e}"));
    let h = Harness::with_local(vec![Arc::new(Cross)], None, Some(one_response(choices))).await;

    let (status, headers, _) = h.send(openai_request("claude-x", false)).await;

    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert!(headers.get("location").is_none());
}

#[tokio::test]
async fn a_translated_redirect_is_a_bad_gateway() {
    let ends = CallEnds::default();
    let moved = http::Response::builder()
        .status(StatusCode::FOUND)
        .header("location", "http://elsewhere/")
        .body(Vec::new())
        .unwrap_or_else(|e| panic!("{e}"));
    let h = Harness::with_local(
        vec![Arc::new(Cross), Arc::new(ends.clone())],
        None,
        Some(one_response(moved)),
    )
    .await;

    let (status, headers, body) = h.send(openai_request("claude-x", false)).await;

    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert!(headers.get("location").is_none());
    let body: Value = serde_json::from_slice(&body.unwrap_or_default()).unwrap_or_default();
    assert_eq!(body["error"]["code"], "upstream", "{body}");
    // The call ends with the refusal, which carries the provider's answer.
    let ends = ends.0.lock().map(|all| all.clone()).unwrap_or_default();
    assert!(matches!(&ends[..], [o] if o.as_ref().is_err_and(|f| fault_code(f) == "upstream")));
    let answer = ends[0].as_ref().err();
    let answer = answer.and_then(|f| {
        f.get::<respan_gateway_http_faults::ProviderError>()
            .map(|e| &e.0)
    });
    assert_eq!(answer.map(|a| a.status()), Some(StatusCode::FOUND));
}

#[tokio::test]
async fn a_translated_failure_labelled_as_a_stream_is_still_a_failure() {
    let failed = http::Response::builder()
        .status(StatusCode::SERVICE_UNAVAILABLE)
        .header("content-type", "text/event-stream")
        .body(
            OPENAI_STREAM
                .iter()
                .map(|c| Ok(bytes::Bytes::from(*c)))
                .collect::<Vec<_>>(),
        )
        .unwrap_or_default();
    let h = Harness::with_local(vec![Arc::new(Cross)], Some(one_response(failed)), None).await;

    let (status, _, body) = h.send(anthropic_request(true)).await;

    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    let body: Value = serde_json::from_slice(&body.unwrap_or_default()).unwrap_or_default();
    assert_eq!(body["type"], "error", "{body}");
    let end = h
        .log
        .only()
        .map_err(|f| respan_gateway_http_faults::status(&f).as_u16());
    assert_eq!(end, Err(503));
}

#[tokio::test]
async fn a_provider_error_framed_as_an_event_keeps_its_words() {
    let error =
        "data: {\"error\":{\"type\":\"rate_limit_exceeded\",\"message\":\"slow down\"}}\n\n";
    let failed = http::Response::builder()
        .status(StatusCode::TOO_MANY_REQUESTS)
        .header("content-type", "text/event-stream")
        .body(vec![Ok(bytes::Bytes::from(error))])
        .unwrap_or_default();
    let h = Harness::with_local(vec![Arc::new(Cross)], Some(one_response(failed)), None).await;

    let (status, _, body) = h.send(anthropic_request(true)).await;

    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    let body = String::from_utf8(body.unwrap_or_default().to_vec()).unwrap_or_default();
    assert!(body.contains("slow down"), "{body}");
    let end = h.log.only().err().map(|f| f.message().to_owned());
    assert_eq!(end.as_deref(), Some("slow down"));
}

#[tokio::test]
async fn an_anthropic_stream_error_with_no_type_keeps_its_words() {
    let h = Harness::new(Vec::new()).await;
    let stream = [
        ANTHROPIC_STREAM[0],
        "data: {\"error\":{\"type\":\"overloaded_error\",\"message\":\"busy\"}}\n\n",
    ];
    h.anthropic.answer(sse(&stream));

    let (_, _, body) = h.send(openai_request("claude-x", true)).await;

    let body = String::from_utf8(body.unwrap_or_default().to_vec()).unwrap_or_default();
    assert!(
        body.contains("busy") && body.contains("overloaded_error"),
        "{body}"
    );
    let end = h
        .log
        .only()
        .map_err(|f| (fault_code(&f), f.message().to_owned()));
    assert_eq!(end, Err(("overloaded_error".to_owned(), "busy".to_owned())));
}

#[tokio::test]
async fn a_providers_error_event_mid_stream_is_written_anew_when_translated() {
    let h = Harness::new(Vec::new()).await;
    let error = "data: {\"error\":{\"message\":\"boom\",\"type\":\"server_error\",\"param\":null,\"code\":null}}\n\n";
    h.openai
        .answer(sse(&[OPENAI_STREAM[0], OPENAI_STREAM[1], error]));

    let (_, _, body) = h.send(anthropic_request(true)).await;

    let body = String::from_utf8(body.unwrap_or_default().to_vec()).unwrap_or_default();
    let (name, data) = events(&body).pop().unwrap_or_default();
    assert_eq!(name.as_deref(), Some("error"));
    assert_eq!(data["type"], "error");
    assert_eq!(
        (&data["error"]["type"], &data["error"]["message"]),
        (&json!("server_error"), &json!("boom"))
    );
}

#[tokio::test]
async fn an_anthropic_error_after_its_message_delta_keeps_both_and_ends_last() {
    let h = Harness::new(Vec::new()).await;
    let error = "event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\",\"message\":\"busy\"}}\n\n";
    let stream = [ANTHROPIC_STREAM[0], ANTHROPIC_STREAM[5], error];
    h.anthropic.answer(sse(&stream));
    let body = json!({
        "model": "claude-x", "max_tokens": 10, "stream": true,
        "messages": [{ "role": "user", "content": "hi" }],
    });

    let (_, out) = h.text(call(MESSAGES, body.to_string())).await;

    assert!(out.contains("message_delta"), "{out}");
    assert!(out.ends_with(error), "{out}");
    assert_eq!(
        events(&out).last().expect("error event").1["error"]["message"],
        "busy"
    );
    let end = h.log.only().map_err(|f| fault_code(&f));
    assert_eq!(end, Err("overloaded_error".to_owned()));
}

// An Anthropic error event goes as it came to an Anthropic client, and in
// OpenAI's words to an OpenAI one.
#[tokio::test]
async fn an_anthropic_error_event_goes_as_it_came_only_to_its_format() {
    let error = "event: error\ndata: {\"type\": \"error\", \"error\": {\"type\": \"overloaded_error\", \"message\": \"busy\"}}\n\n";
    let mut stream = ANTHROPIC_STREAM[..3].to_vec();
    stream.push(error);

    let h = Harness::new(Vec::new()).await;
    h.anthropic.answer(sse(&stream));
    let body = json!({
        "model": "claude-x", "max_tokens": 10, "stream": true,
        "messages": [{ "role": "user", "content": "hi" }],
    });
    let (_, out) = h.text(call(MESSAGES, body.to_string())).await;
    assert!(!out.ends_with(error), "raw error needs opt-in");
    assert_eq!(
        events(&out).last().expect("error event").1["error"]["message"],
        "busy"
    );

    let h = Harness::new(Vec::new()).await;
    h.anthropic.answer(sse(&stream));
    let (_, out) = h.text(openai_request("claude-x", true)).await;
    let last = events(&out).pop().map(|(_, d)| d).unwrap_or_default();
    assert_eq!(last["error"]["code"], "overloaded_error", "{out}");
    assert!(last["type"].is_null(), "{out}");
}

// An OpenAI error event goes in Anthropic's words to an Anthropic client.
#[tokio::test]
async fn an_openai_error_event_reaches_an_anthropic_client_in_its_words() {
    let mut stream = OPENAI_STREAM[..2].to_vec();
    stream.push(
        "data: {\"error\":{\"message\":\"boom\",\"type\":\"server_error\",\"native\":1}}\n\n",
    );
    let h = Harness::new(Vec::new()).await;
    h.openai.answer(sse(&stream));

    let (_, out) = h.text(anthropic_request(true)).await;

    let (name, last) = events(&out).pop().unwrap_or((None, Value::Null));
    assert_eq!(name.as_deref(), Some("error"), "{out}");
    assert_eq!(last["type"], "error", "{out}");
    assert_eq!(last["error"]["message"], "boom", "{out}");
}

// An error inside a 200 goes as it came to a client of its format, and in
// the client's words to another.
#[tokio::test]
async fn an_error_in_a_success_goes_as_it_came_only_to_its_format() {
    let anthropic = r#"{"type": "error", "error": {"type": "overloaded_error", "message": "busy", "retry": true}}"#;
    let openai = r#"{"error": {"message": "boom", "type": "server_error", "native": 1}}"#;
    let messages = json!({
        "model": "claude-x", "max_tokens": 10,
        "messages": [{ "role": "user", "content": "hi" }],
    });

    // Anthropic to its own client, as it came.
    let h = Harness::new(Vec::new()).await;
    h.anthropic.answer(json_response(StatusCode::OK, anthropic));
    let (_, out) = h.text(call(MESSAGES, messages.to_string())).await;
    assert_ne!(out, anthropic);
    assert_eq!(
        serde_json::from_str::<Value>(&out).expect("encoded error")["error"]["message"],
        "busy"
    );

    // Anthropic to an OpenAI client, in OpenAI's words.
    let h = Harness::new(Vec::new()).await;
    h.anthropic.answer(json_response(StatusCode::OK, anthropic));
    let (_, out) = h.text(openai_request("claude-x", false)).await;
    let v: Value = serde_json::from_str(&out).unwrap_or_default();
    assert_eq!(v["error"]["code"], "overloaded_error", "{out}");
    assert!(v["type"].is_null(), "{out}");

    // OpenAI to an Anthropic client, in Anthropic's words.
    let h = Harness::new(Vec::new()).await;
    h.openai.answer(json_response(StatusCode::OK, openai));
    let (_, out) = h.text(anthropic_request(false)).await;
    let v: Value = serde_json::from_str(&out).unwrap_or_default();
    assert_eq!(
        (&v["type"], &v["error"]["message"]),
        (&json!("error"), &json!("boom"))
    );
}
