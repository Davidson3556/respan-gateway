//! Translate tools behavior.

use super::*;

#[tokio::test]
async fn an_openai_agent_request_reaches_anthropic_with_its_tools_and_calls() {
    let h = Harness::new(Vec::new()).await;
    h.anthropic.answer(Answer::text("hi"));
    let (status, response) = h.text(openai_agent()).await;
    assert_eq!(status, StatusCode::OK, "{response}");
    h.assert_requests(0, 1);

    let sent = h.sent_json(0);
    let tool = json!({ "name": "read", "description": "read a file",
        "input_schema": { "type": "object" } });
    assert_eq!(sent["tools"], json!([tool]));
    let choice = json!({ "type": "tool", "name": "read", "disable_parallel_tool_use": true });
    assert_eq!(sent["tool_choice"], choice);
    assert!(sent["parallel_tool_calls"].is_null());
    let call_made = json!({ "type": "tool_use", "id": "call_1", "name": "read",
        "input": { "path": "a.rs" } });
    let result =
        json!({ "type": "tool_result", "tool_use_id": "call_1", "content": "fn main() {}" });
    assert_eq!(
        sent["messages"],
        json!([
            { "role": "user", "content": "open a.rs" },
            { "role": "assistant", "content": [call_made] },
            { "role": "user", "content": [result] },
            { "role": "user", "content": "now what?" },
        ])
    );
}

#[tokio::test]
async fn an_anthropic_agent_request_reaches_openai_with_its_tools_and_calls() {
    let h = Harness::new(Vec::new()).await;
    h.openai.answer( json_response(StatusCode::OK, r#"{"id":"c","choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}]}"#));
    let (status, _) = h.text(anthropic_agent()).await;
    assert_eq!(status, StatusCode::OK);
    h.assert_requests(1, 0);

    let sent = h.sent_json(0);
    let function = json!({ "name": "read", "description": "read a file",
        "parameters": { "type": "object" } });
    assert_eq!(
        sent["tools"],
        json!([{ "type": "function", "function": function }])
    );
    assert_eq!(
        sent["tool_choice"],
        json!({ "type": "function", "function": { "name": "read" } })
    );
    assert_eq!(sent["parallel_tool_calls"], false);
    let call_made = json!({ "id": "call_1", "type": "function",
        "function": { "name": "read", "arguments": "{\"path\":\"a.rs\"}" } });
    assert_eq!(
        sent["messages"],
        json!([
            { "role": "user", "content": "open a.rs" },
            { "role": "assistant", "content": "Reading.", "tool_calls": [call_made] },
            { "role": "tool", "tool_call_id": "call_1", "content": "fn main() {}" },
            { "role": "user", "content": "now what?" },
        ])
    );
}

#[tokio::test]
async fn a_streamed_anthropic_tool_call_reaches_an_openai_client() {
    let h = Harness::new(Vec::new()).await;
    h.anthropic.answer(sse(ANTHROPIC_TOOL_STREAM));
    // Streamed, with usage.
    let body = json!({ "model": "claude-x", "stream": true, "stream_options": { "include_usage": true },
        "tools": [{ "type": "function", "function": { "name": "read" } }],
        "messages": [{ "role": "user", "content": "hi" }] });
    let req = call(CHAT, body.to_string());
    let (_, out) = h.text(req).await;
    h.assert_requests(0, 1);

    let chunks = events(&out);
    let deltas: Vec<&Value> = chunks
        .iter()
        .map(|(_, d)| &d["choices"][0]["delta"])
        .collect();
    let text: String = deltas
        .iter()
        .filter_map(|d| d["content"].as_str())
        .collect();
    assert_eq!(text, "Reading.");
    let calls: Vec<&Value> = deltas
        .iter()
        .flat_map(|d| d["tool_calls"].as_array())
        .flatten()
        .collect();
    assert_eq!(calls[0]["id"], "toolu_1");
    assert_eq!(calls[0]["function"]["name"], "read");
    assert_eq!(calls[0]["index"], 0);
    let args: String = calls
        .iter()
        .filter_map(|c| c["function"]["arguments"].as_str())
        .collect();
    assert_eq!(args, r#"{"path":"a.rs"}"#);
    let finish = chunks
        .iter()
        .find_map(|(_, d)| d["choices"][0]["finish_reason"].as_str());
    assert_eq!(finish, Some("tool_calls"));
    assert!(out.ends_with("data: [DONE]\n\n"), "{out}");
}

#[tokio::test]
async fn a_streamed_openai_tool_call_reaches_an_anthropic_client() {
    let h = Harness::new(Vec::new()).await;
    h.openai.answer(sse(OPENAI_TOOL_STREAM));
    let body = json!({ "model": "gpt-x", "max_tokens": 100, "stream": true,
        "tools": [{ "name": "read", "input_schema": { "type": "object" } }],
        "messages": [{ "role": "user", "content": "hi" }] });
    let (_, out) = h.text(call(MESSAGES, body.to_string())).await;
    h.assert_requests(1, 0);

    let sent = events(&out);
    let kinds: Vec<&str> = sent
        .iter()
        .filter_map(|(_, d)| d["type"].as_str())
        .collect();
    assert_eq!(
        kinds,
        [
            "message_start",
            "content_block_start",
            "content_block_delta",
            "content_block_delta",
            "content_block_stop",
            "message_delta",
            "message_stop",
        ]
    );
    let start = &sent[1].1;
    assert_eq!(
        start["content_block"],
        json!({ "type": "tool_use", "id": "call_1", "name": "read", "input": {} })
    );
    let args: String = sent
        .iter()
        .filter_map(|(_, d)| d["delta"]["partial_json"].as_str())
        .collect();
    assert_eq!(args, r#"{"path":"a.rs"}"#);
    assert_eq!(sent[5].1["delta"]["stop_reason"], "tool_use");
}

#[tokio::test]
async fn a_whole_tool_call_crosses_formats_both_ways() {
    // Anthropic's answer to an OpenAI client.
    let h = Harness::new(Vec::new()).await;
    let answer = json!({ "id": "m", "type": "message", "role": "assistant", "model": "c",
        "content": [{ "type": "tool_use", "id": "t1", "name": "read", "input": { "path": "a" } }],
        "stop_reason": "tool_use", "usage": { "input_tokens": 1, "output_tokens": 1 } });
    h.anthropic
        .answer(json_response(StatusCode::OK, &answer.to_string()));
    let body = json!({ "model": "claude-x", "messages": [{ "role": "user", "content": "hi" }],
        "tools": [{ "type": "function", "function": { "name": "read" } }] });
    let (_, out) = h.text(call(CHAT, body.to_string())).await;
    h.assert_requests(0, 1);
    let v: Value = serde_json::from_str(&out).unwrap_or_default();
    let message = &v["choices"][0]["message"];
    assert!(message["content"].is_null(), "{out}");
    assert_eq!(
        message["tool_calls"][0]["function"]["arguments"],
        r#"{"path":"a"}"#
    );
    assert_eq!(v["choices"][0]["finish_reason"], "tool_calls");

    // OpenAI's answer to an Anthropic client.
    let h = Harness::new(Vec::new()).await;
    let answer = json!({ "id": "c", "model": "g", "choices": [{ "index": 0, "finish_reason": "tool_calls",
        "message": { "role": "assistant", "content": null, "tool_calls": [
            { "id": "c1", "type": "function", "function": { "name": "read", "arguments": "{\"path\":\"a\"}" } }] } }],
        "usage": { "prompt_tokens": 1, "completion_tokens": 1 } });
    h.openai
        .answer(json_response(StatusCode::OK, &answer.to_string()));
    let body = json!({ "model": "gpt-x", "max_tokens": 9, "messages": [{ "role": "user", "content": "hi" }],
        "tools": [{ "name": "read", "input_schema": { "type": "object" } }] });
    let (_, out) = h.text(call(MESSAGES, body.to_string())).await;
    h.assert_requests(1, 0);
    let v: Value = serde_json::from_str(&out).unwrap_or_default();
    assert_eq!(
        v["content"],
        json!([{ "type": "tool_use", "id": "c1", "name": "read", "input": { "path": "a" } }])
    );
    assert_eq!(v["stop_reason"], "tool_use");
}

#[tokio::test]
async fn generated_tool_calls_cross_both_ways_whole_and_streamed() {
    let args = json!({ "path": "src/lib.rs", "line": 17 });
    for stream in [false, true] {
        let h = Harness::new(Vec::new()).await;
        h.anthropic.answer(Answer::tool_call("read", args.clone()));
        let request = json!({
            "model": "claude-sonnet-4-5", "stream": stream,
            "stream_options": { "include_usage": stream },
            "messages": [{ "role": "user", "content": "read src/lib.rs" }],
            "tools": [{ "type": "function", "function": {
                "name": "read", "parameters": { "type": "object" }
            }}],
        });
        let (status, out) = h.text(call(CHAT, request.to_string())).await;
        assert_eq!(status, StatusCode::OK, "{out}");
        h.assert_requests(0, 1);
        assert_eq!(h.anthropic.requests()[0].body["model"], "claude-sonnet-4-5");
        if stream {
            let chunks = events(&out);
            let calls: Vec<&Value> = chunks
                .iter()
                .flat_map(|(_, c)| {
                    c.pointer("/choices/0/delta/tool_calls")
                        .and_then(Value::as_array)
                })
                .flatten()
                .collect();
            assert_eq!(calls[0]["function"]["name"], "read", "{out}");
            let arguments: String = calls
                .iter()
                .filter_map(|c| c["function"]["arguments"].as_str())
                .collect();
            assert_eq!(
                serde_json::from_str::<Value>(&arguments).expect("valid tool JSON"),
                args
            );
            assert!(
                chunks
                    .iter()
                    .any(|(_, c)| c["choices"][0]["finish_reason"] == "tool_calls")
            );
            assert!(chunks.iter().any(|(_, c)| {
                c["usage"]["completion_tokens"]
                    .as_u64()
                    .is_some_and(|n| n > 0)
            }));
            assert!(out.ends_with("data: [DONE]\n\n"), "{out}");
        } else {
            let response: Value = serde_json::from_str(&out).expect("valid response JSON");
            let call = &response["choices"][0]["message"]["tool_calls"][0];
            assert_eq!(call["function"]["name"], "read");
            assert_eq!(
                serde_json::from_str::<Value>(
                    call["function"]["arguments"]
                        .as_str()
                        .expect("tool arguments")
                )
                .expect("valid tool JSON"),
                args
            );
            assert_eq!(response["choices"][0]["finish_reason"], "tool_calls");
            assert!(
                response["usage"]["completion_tokens"]
                    .as_u64()
                    .is_some_and(|n| n > 0)
            );
        }

        let h = Harness::new(Vec::new()).await;
        h.openai.answer(Answer::tool_call("read", args.clone()));
        let request = json!({
            "model": "gpt-4o", "max_tokens": 100, "stream": stream,
            "messages": [{ "role": "user", "content": "read src/lib.rs" }],
            "tools": [{ "name": "read", "input_schema": { "type": "object" } }],
        });
        let (status, out) = h.text(call(MESSAGES, request.to_string())).await;
        assert_eq!(status, StatusCode::OK, "{out}");
        h.assert_requests(1, 0);
        assert_eq!(h.openai.requests()[0].body["model"], "gpt-4o");
        if stream {
            let frames = events(&out);
            let starts: Vec<&Value> = frames
                .iter()
                .filter(|(name, _)| name.as_deref() == Some("content_block_start"))
                .map(|(_, body)| &body["content_block"])
                .collect();
            assert_eq!(starts.len(), 1, "{out}");
            assert_eq!(starts[0]["name"], "read");
            let arguments: String = frames
                .iter()
                .filter_map(|(_, body)| body["delta"]["partial_json"].as_str())
                .collect();
            assert_eq!(
                serde_json::from_str::<Value>(&arguments).expect("valid tool JSON"),
                args
            );
            assert!(
                frames
                    .iter()
                    .any(|(_, body)| body["delta"]["stop_reason"] == "tool_use")
            );
            assert!(frames.iter().any(|(_, body)| {
                body["usage"]["output_tokens"]
                    .as_u64()
                    .is_some_and(|n| n > 0)
            }));
        } else {
            let response: Value = serde_json::from_str(&out).expect("valid response JSON");
            assert_eq!(response["content"][0]["type"], "tool_use");
            assert_eq!(response["content"][0]["name"], "read");
            assert_eq!(response["content"][0]["input"], args);
            assert_eq!(response["stop_reason"], "tool_use");
            assert!(
                response["usage"]["output_tokens"]
                    .as_u64()
                    .is_some_and(|n| n > 0)
            );
        }
    }
}

#[tokio::test]
async fn a_tool_that_anthropic_runs_cannot_be_sent_to_openai() {
    let h = Harness::new(Vec::new()).await;
    let body = json!({ "model": "gpt-x", "max_tokens": 9, "messages": [{ "role": "user", "content": "hi" }],
        "tools": [{ "type": "web_search_20250305", "name": "web_search" }] });
    let (status, out) = h.text(call(MESSAGES, body.to_string())).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{out}");
    assert!(h.sent().is_empty());
}

#[tokio::test]
async fn text_released_before_a_tool_call_stays_in_its_own_block() {
    let tool = |name: &str, data: Value| format!("event: {name}\ndata: {data}\n\n");
    let mut stream = masked_anthropic_stream();
    stream.splice(
        5..5,
        [
            tool(
                "content_block_start",
                json!({"type": "content_block_start", "index": 1,
                "content_block": {"type": "tool_use", "id": "t", "name": "f", "input": {}}}),
            ),
            tool(
                "content_block_stop",
                json!({"type": "content_block_stop", "index": 1}),
            ),
        ],
    );
    let (h, body) = anthropic_messages(&stream).await;

    let (status, out) = h.text(call(MESSAGES, body)).await;

    assert_eq!(status, StatusCode::OK, "{out}");
    let seen: Vec<(String, Value)> = events(&out)
        .into_iter()
        .filter_map(|(n, v)| Some((n?, v["index"].clone())))
        .filter(|(n, _)| n.starts_with("content_block"))
        .collect();
    let at = |name: &str, index: i64| (name.to_owned(), json!(index));
    assert_eq!(
        seen,
        [
            at("content_block_start", 0),
            at("content_block_delta", 0),
            at("content_block_delta", 0),
            at("content_block_delta", 0),
            at("content_block_stop", 0),
            at("content_block_start", 1),
            at("content_block_stop", 1),
        ]
    );
    assert_eq!(anthropic_texts(&out), "Hi Alice, bye <");
}

#[tokio::test]
async fn a_tool_block_held_in_anthropics_stream_is_released_as_it_came() {
    let h = filtered(HoldAll(Vec::new())).await;
    let event = |name: &str, data: Value| format!("event: {name}\ndata: {data}\n\n");
    let stream = [
        ANTHROPIC_STREAM[0].to_owned(),
        event(
            "content_block_start",
            json!({"type": "content_block_start", "index": 0,
            "content_block": {"type": "tool_use", "id": "t", "name": "f", "input": {}}}),
        ),
        event(
            "content_block_stop",
            json!({"type": "content_block_stop", "index": 0}),
        ),
        ANTHROPIC_STREAM[5].to_owned(),
        ANTHROPIC_STREAM[6].to_owned(),
    ];
    let chunks: Vec<&str> = stream.iter().map(String::as_str).collect();
    h.anthropic.answer(sse(&chunks));
    let body = json!({
        "model": "claude-x", "max_tokens": 10, "stream": true,
        "messages": [{ "role": "user", "content": "hi" }],
    });

    let (_, out) = h.text(call(MESSAGES, body.to_string())).await;

    // It keeps its bytes, so it goes out as it came, where the provider put
    // it, not as the plugin's fault.
    assert!(!out.contains("event: error"), "{out}");
    assert!(out.contains("\"tool_use\""), "{out}");
    let names: Vec<String> = events(&out).into_iter().filter_map(|(n, _)| n).collect();
    assert_well_formed(&names, &out);
}

/// OpenAI names each tool call's fragment by `index`; the Anthropic client
/// must get each call's arguments in its own block.
#[tokio::test]
async fn parallel_openai_tool_calls_keep_their_arguments_for_an_anthropic_client() {
    let h = Harness::new(Vec::new()).await;
    h.openai.answer(sse(OPENAI_PARALLEL_STREAM));
    let body = json!({ "model": "gpt-x", "max_tokens": 100, "stream": true,
        "tools": [{ "name": "read", "input_schema": { "type": "object" } }],
        "messages": [{ "role": "user", "content": "hi" }] });
    let (_, out) = h.text(call(MESSAGES, body.to_string())).await;
    let sent = events(&out);
    let mut by_block: std::collections::BTreeMap<u64, (String, String)> = Default::default();
    for (_, d) in &sent {
        let i = d["index"].as_u64().unwrap_or(99);
        if d["type"] == "content_block_start" {
            by_block.entry(i).or_default().0 =
                d["content_block"]["id"].as_str().unwrap_or("").into();
        }
        if let Some(p) = d["delta"]["partial_json"].as_str() {
            by_block.entry(i).or_default().1.push_str(p);
        }
    }
    let got: Vec<(String, String)> = by_block.into_values().collect();
    assert_eq!(
        got,
        vec![
            ("call_a".into(), r#"{"path":"a"}"#.into()),
            ("call_b".into(), r#"{"path":"b"}"#.into())
        ],
        "{out}"
    );
}

/// The same calls read whole (the plugin asks the provider for a stream, the
/// client asked for a whole answer): each call keeps its own arguments.
#[tokio::test]
async fn parallel_openai_tool_calls_keep_their_arguments_in_a_whole_anthropic_answer() {
    struct Streamed;
    #[async_trait]
    impl Plugin for Streamed {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            cx.extensions_mut()
                .insert(respan_gateway_codec::StreamResponse(true));
            Ok(None)
        }
    }
    let h = Harness::new(vec![Arc::new(Streamed)]).await;
    h.openai.answer(sse(OPENAI_PARALLEL_STREAM));
    let body = json!({ "model": "gpt-x", "max_tokens": 100,
        "tools": [{ "name": "read", "input_schema": { "type": "object" } }],
        "messages": [{ "role": "user", "content": "hi" }] });
    let (_, out) = h.text(call(MESSAGES, body.to_string())).await;
    let v: Value = serde_json::from_str(&out).unwrap_or_default();
    assert_eq!(
        v["content"],
        json!([
            { "type": "tool_use", "id": "call_a", "name": "read", "input": { "path": "a" } },
            { "type": "tool_use", "id": "call_b", "name": "read", "input": { "path": "b" } },
        ]),
        "{out}"
    );
}

/// Anthropic's `is_error` on a tool result says the tool failed; sent to
/// OpenAI it must be kept (in the text) or refused, not dropped.
#[tokio::test]
async fn a_failed_tool_result_is_not_silently_turned_into_a_success() {
    let h = Harness::new(Vec::new()).await;
    h.openai.answer( json_response(StatusCode::OK, r#"{"id":"c","choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}]}"#));
    let body = json!({
        "model": "gpt-x", "max_tokens": 100,
        "tools": [{ "name": "read", "input_schema": { "type": "object" } }],
        "messages": [
            { "role": "user", "content": "open a.rs" },
            { "role": "assistant", "content": [{ "type": "tool_use", "id": "t1", "name": "read", "input": {} }] },
            { "role": "user", "content": [{ "type": "tool_result", "tool_use_id": "t1", "is_error": true, "content": "ENOENT" }] },
        ],
    });
    let (status, text) = h.text(call(MESSAGES, body.to_string())).await;
    if status == StatusCode::OK {
        let sent = h.sent_json(0);
        panic!("is_error dropped: sent {}", sent["messages"]);
    }
    assert_eq!(status, StatusCode::BAD_REQUEST, "{text}");
}

/// NativeFields fields on Anthropic's tool, call and result have no place in
/// OpenAI's: refused, never dropped.
#[tokio::test]
async fn an_extra_tool_field_is_refused_not_dropped() {
    let cached = json!({ "type": "ephemeral" });
    let tool =
        json!({ "name": "read", "input_schema": { "type": "object" }, "cache_control": cached });
    let use_block = json!({ "type": "tool_use", "id": "t1", "name": "read", "input": {} });
    let result = json!({ "type": "tool_result", "tool_use_id": "t1", "content": "x", "cache_control": cached });
    let ask = json!({ "role": "user", "content": "go" });
    let call_made = json!({ "role": "assistant", "content": [use_block] });
    let answer = json!({ "role": "user", "content": [result] });
    for body in [
        json!({ "model": "gpt-x", "max_tokens": 9, "tools": [tool], "messages": [ask] }),
        json!({ "model": "gpt-x", "max_tokens": 9, "messages": [ask, call_made, answer] }),
    ] {
        let h = Harness::new(Vec::new()).await;
        let (status, text) = h.text(call(MESSAGES, body.to_string())).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{text}");
        assert!(h.sent().is_empty());
    }
}

/// A whole OpenAI answer cut by the token limit has arguments that do not
/// parse; the Anthropic client must not be told the call had no input.
#[tokio::test]
async fn cut_off_tool_arguments_are_not_replaced_by_an_empty_input() {
    let h = Harness::new(Vec::new()).await;
    let answer = json!({ "id": "c", "model": "g", "choices": [{ "index": 0, "finish_reason": "length",
        "message": { "role": "assistant", "content": null, "tool_calls": [
            { "id": "c1", "type": "function", "function": { "name": "rm", "arguments": "{\"path\":\"/tmp/a\",\"recursive\":tr" } }] } }],
        "usage": { "prompt_tokens": 1, "completion_tokens": 1 } });
    h.openai
        .answer(json_response(StatusCode::OK, &answer.to_string()));
    let body = json!({ "model": "gpt-x", "max_tokens": 9, "messages": [{ "role": "user", "content": "hi" }],
        "tools": [{ "name": "rm", "input_schema": { "type": "object" } }] });
    let (status, out) = h.text(call(MESSAGES, body.to_string())).await;
    assert_eq!(
        status,
        StatusCode::BAD_GATEWAY,
        "cut arguments became input: {out}"
    );
}

/// Control: OpenAI's own order (each call's start, then its arguments) works.
#[tokio::test]
async fn control_sequential_openai_tool_calls_reach_an_anthropic_client() {
    const SEQ: &[&str] = &[
        "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"model\":\"gpt-x\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":null,\"tool_calls\":[{\"index\":0,\"id\":\"call_a\",\"type\":\"function\",\"function\":{\"name\":\"read\",\"arguments\":\"\"}}]}}]}\n\n",
        "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"model\":\"gpt-x\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"arguments\":\"{\\\"path\\\":\\\"a\\\"}\"}}]}}]}\n\n",
        "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"model\":\"gpt-x\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":1,\"id\":\"call_b\",\"type\":\"function\",\"function\":{\"name\":\"read\",\"arguments\":\"\"}}]}}]}\n\n",
        "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"model\":\"gpt-x\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":1,\"function\":{\"arguments\":\"{\\\"path\\\":\\\"b\\\"}\"}}]}}]}\n\n",
        "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"model\":\"gpt-x\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"tool_calls\"}]}\n\n",
        "data: [DONE]\n\n",
    ];
    let h = Harness::new(Vec::new()).await;
    h.openai.answer(sse(SEQ));
    let body = json!({ "model": "gpt-x", "max_tokens": 100, "stream": true,
        "tools": [{ "name": "read", "input_schema": { "type": "object" } }],
        "messages": [{ "role": "user", "content": "hi" }] });
    let (_, out) = h.text(call(MESSAGES, body.to_string())).await;
    let args: Vec<String> = events(&out)
        .iter()
        .filter_map(|(_, d)| {
            d["delta"]["partial_json"]
                .as_str()
                .map(|s| format!("{}:{s}", d["index"]))
        })
        .collect();
    assert_eq!(
        args,
        vec![
            r#"0:{"path":"a"}"#.to_owned(),
            r#"1:{"path":"b"}"#.to_owned()
        ],
        "{out}"
    );
}

/// An OpenAI client, an OpenAI provider, a call that is not a relay: the
/// parallel calls OpenAI numbers by index must each keep valid arguments.
#[tokio::test]
async fn l4_parallel_openai_tool_calls_written_anew_for_an_openai_client_keep_valid_arguments() {
    let h = Harness::with(vec![Arc::new(L4Own("openai"))]).await;
    h.openai.answer(sse(OPENAI_PARALLEL_STREAM));
    let body = json!({ "model": "gpt-x", "stream": true,
        "tools": [{ "type": "function", "function": { "name": "read", "parameters": { "type": "object" } } }],
        "messages": [{ "role": "user", "content": "hi" }] });
    let (_, out) = h.text(call(CHAT, body.to_string())).await;
    assert_eq!(
        l4_openai_calls(&out),
        vec![
            ("call_a".into(), r#"{"path":"a"}"#.into()),
            ("call_b".into(), r#"{"path":"b"}"#.into())
        ],
        "{out}"
    );
}

/// Anthropic's blocks come one after another; written anew for an Anthropic
/// client, each tool block must stop before the next one starts.
#[tokio::test]
async fn l4_sequential_anthropic_tool_blocks_written_anew_stop_before_the_next_starts() {
    const TWO_TOOLS: &[&str] = &[
        "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_1\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"claude-x\",\"content\":[],\"stop_reason\":null,\"stop_sequence\":null,\"usage\":{\"input_tokens\":5,\"output_tokens\":1}}}\n\n",
        "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"tool_use\",\"id\":\"t1\",\"name\":\"read\",\"input\":{}}}\n\n",
        "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"{\\\"path\\\":\\\"a\\\"}\"}}\n\n",
        "event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
        "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":1,\"content_block\":{\"type\":\"tool_use\",\"id\":\"t2\",\"name\":\"read\",\"input\":{}}}\n\n",
        "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":1,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"{\\\"path\\\":\\\"b\\\"}\"}}\n\n",
        "event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":1}\n\n",
        "event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"tool_use\",\"stop_sequence\":null},\"usage\":{\"output_tokens\":2}}\n\n",
        "event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n",
    ];
    let h = Harness::with(vec![Arc::new(L4Own("anthropic"))]).await;
    h.anthropic.answer(sse(TWO_TOOLS));
    let body = json!({ "model": "claude-x", "max_tokens": 100, "stream": true,
        "tools": [{ "name": "read", "input_schema": { "type": "object" } }],
        "messages": [{ "role": "user", "content": "hi" }] });
    let (_, out) = h.text(call(MESSAGES, body.to_string())).await;
    let seq: Vec<String> = events(&out)
        .into_iter()
        .filter(|(_, d)| d["type"] == "content_block_start" || d["type"] == "content_block_stop")
        .map(|(_, d)| format!("{}{}", d["type"].as_str().unwrap_or(""), d["index"]))
        .collect();
    assert_eq!(
        seq,
        [
            "content_block_start0",
            "content_block_stop0",
            "content_block_start1",
            "content_block_stop1"
        ],
        "{out}"
    );
}

/// A tool result's text block carries `cache_control`, which OpenAI has no
/// place for: refused, as the module says of fields with no equivalent.
#[tokio::test]
async fn l4_a_tool_result_text_blocks_extra_field_is_refused_not_dropped() {
    let h = Harness::new(Vec::new()).await;
    h.openai.answer( json_response(StatusCode::OK, r#"{"id":"c","choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}]}"#));
    let body = json!({
        "model": "gpt-x", "max_tokens": 100,
        "messages": [
            { "role": "user", "content": "open a.rs" },
            { "role": "assistant", "content": [{ "type": "tool_use", "id": "t1", "name": "read", "input": {} }] },
            { "role": "user", "content": [{ "type": "tool_result", "tool_use_id": "t1",
                "content": [{ "type": "text", "text": "fn main", "cache_control": { "type": "ephemeral" } }] }] },
        ],
    });
    let (status, text) = h.text(call(MESSAGES, body.to_string())).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{text}");
    h.assert_requests(0, 0);
}

/// Anthropic's `tool_choice` with a field OpenAI has no place for is
/// refused, not dropped.
#[tokio::test]
async fn l4_an_anthropic_tool_choice_extra_field_is_refused_not_dropped() {
    let h = Harness::new(Vec::new()).await;
    h.openai.answer( json_response(StatusCode::OK, r#"{"id":"c","choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}]}"#));
    let body = json!({
        "model": "gpt-x", "max_tokens": 100,
        "tools": [{ "name": "read", "input_schema": { "type": "object" } }],
        "tool_choice": { "type": "auto", "cache_control": { "type": "ephemeral" } },
        "messages": [{ "role": "user", "content": "hi" }],
    });
    let (status, text) = h.text(call(MESSAGES, body.to_string())).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{text}");
    h.assert_requests(0, 0);
}

/// With `n > 1`, each choice numbers its own tool calls, so a call of the
/// second choice must not merge into the first's: it is refused.
#[tokio::test]
async fn a_tool_call_of_a_later_choice_is_not_merged_into_the_first_choices() {
    const TWO_CHOICES: &[&str] = &[
        "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"model\":\"gpt-x\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"tool_calls\":[{\"index\":0,\"id\":\"call_a\",\"type\":\"function\",\"function\":{\"name\":\"read\",\"arguments\":\"{}\"}}]}}]}\n\n",
        "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"model\":\"gpt-x\",\"choices\":[{\"index\":1,\"delta\":{\"role\":\"assistant\",\"tool_calls\":[{\"index\":0,\"id\":\"call_b\",\"type\":\"function\",\"function\":{\"name\":\"read\",\"arguments\":\"{}\"}}]}}]}\n\n",
        "data: [DONE]\n\n",
    ];
    let h = Harness::new(Vec::new()).await;
    h.openai.answer(sse(TWO_CHOICES));
    let body = json!({ "model": "gpt-x", "max_tokens": 100, "stream": true,
        "tools": [{ "name": "read", "input_schema": { "type": "object" } }],
        "messages": [{ "role": "user", "content": "hi" }] });
    let (_, out) = h.text(call(MESSAGES, body.to_string())).await;
    assert!(!out.contains("call_b"), "{out}");
    assert!(out.contains("event: error"), "{out}");
}

/// Parallel OpenAI calls reach an Anthropic client one block at a time.
#[tokio::test]
async fn parallel_openai_tool_calls_reach_an_anthropic_client_in_blocks_that_do_not_overlap() {
    let h = Harness::new(Vec::new()).await;
    h.openai.answer(sse(OPENAI_PARALLEL_STREAM));
    let body = json!({ "model": "gpt-x", "max_tokens": 100, "stream": true,
        "tools": [{ "name": "read", "input_schema": { "type": "object" } }],
        "messages": [{ "role": "user", "content": "hi" }] });
    let (_, out) = h.text(call(MESSAGES, body.to_string())).await;
    let seq: Vec<String> = events(&out)
        .into_iter()
        .filter(|(_, d)| d["type"] == "content_block_start" || d["type"] == "content_block_stop")
        .map(|(_, d)| format!("{}{}", d["type"].as_str().unwrap_or(""), d["index"]))
        .collect();
    assert_eq!(
        seq,
        [
            "content_block_start0",
            "content_block_stop0",
            "content_block_start1",
            "content_block_stop1"
        ],
        "{out}"
    );
}

/// An edit of a streamed tool call's arguments is written into the
/// provider's own events, which keep the rest of what they say.
#[tokio::test]
async fn an_edited_tool_call_is_written_into_anthropics_own_events() {
    let event = |data: Value| {
        format!(
            "event: {}\ndata: {data}\n\n",
            data["type"].as_str().unwrap_or("")
        )
    };
    let mut stream: Vec<String> = ANTHROPIC_STREAM.iter().map(|s| s.to_string()).collect();
    stream.splice(
        5..5,
        [
            event(json!({"type": "content_block_start", "index": 1,
                "content_block": {"type": "tool_use", "id": "t", "name": "send", "input": {}}})),
            event(json!({"type": "content_block_delta", "index": 1,
                "delta": {"type": "input_json_delta", "partial_json": "{\"to\":\"<EMAIL_1>\"}"}})),
            event(json!({"type": "content_block_stop", "index": 1})),
        ],
    );
    let h = Harness::new(vec![Arc::new(Adds(RestoreArguments))]).await;
    let chunks: Vec<&str> = stream.iter().map(String::as_str).collect();
    h.anthropic.answer(sse(&chunks));
    let body = json!({ "model": "claude-x", "max_tokens": 10, "stream": true,
        "messages": [{ "role": "user", "content": "mail bob" }] });

    let (status, out) = h.text(call(MESSAGES, body.to_string())).await;

    assert_eq!(status, StatusCode::OK, "{out}");
    let events = events(&out);
    let arguments: String = events
        .iter()
        .filter_map(|(_, v)| v.pointer("/delta/partial_json").and_then(Value::as_str))
        .collect();
    assert_eq!(arguments, "{\"to\":\"bob@x.io\"}", "{out}");
    let start = events
        .iter()
        .find(|(_, v)| v["content_block"]["type"] == "tool_use")
        .map(|(_, v)| v["content_block"].clone());
    assert_eq!(
        start,
        Some(json!({"type": "tool_use", "id": "t", "name": "send", "input": {}}))
    );
}

/// The same edit in OpenAI's chunks, streamed and whole.
#[tokio::test]
async fn an_edited_tool_call_is_written_into_openais_own_chunks() {
    let chunk = |choices: Value| {
        format!(
            "data: {}\n\n",
            json!({"id": "c1", "model": "gpt-x", "choices": choices})
        )
    };
    let stream = [
        chunk(
            json!([{"index": 0, "delta": {"role": "assistant", "content": null,
            "tool_calls": [{"index": 0, "id": "t", "type": "function",
            "function": {"name": "send", "arguments": ""}}]}}]),
        ),
        chunk(json!([{"index": 0, "delta": {"tool_calls": [{"index": 0,
            "function": {"arguments": "{\"to\":\"<EMAIL_1>\"}"}}]}}])),
        chunk(json!([{"index": 0, "delta": {}, "finish_reason": "tool_calls"}])),
        "data: [DONE]\n\n".to_owned(),
    ];
    let plugins = || -> Vec<Arc<dyn Plugin>> {
        vec![Arc::new(To("openai")), Arc::new(Adds(RestoreArguments))]
    };
    let h = Harness::with(plugins()).await;
    let chunks: Vec<&str> = stream.iter().map(String::as_str).collect();
    h.openai.answer(sse(&chunks));
    let body = json!({ "model": "gpt-x", "stream": true,
        "messages": [{ "role": "user", "content": "mail bob" }] });

    let (status, out) = h.text(call(CHAT, body.to_string())).await;

    assert_eq!(status, StatusCode::OK, "{out}");
    let calls: Vec<Value> = events(&out)
        .into_iter()
        .filter_map(|(_, v)| v.pointer("/choices/0/delta/tool_calls/0").cloned())
        .collect();
    let arguments: String = calls
        .iter()
        .filter_map(|c| c.pointer("/function/arguments").and_then(Value::as_str))
        .collect();
    assert_eq!(arguments, "{\"to\":\"bob@x.io\"}", "{out}");
    assert_eq!(calls[0]["id"], "t", "{out}");

    let whole = json!({"id": "c2", "model": "gpt-x", "choices": [{"index": 0,
        "finish_reason": "tool_calls", "message": {"role": "assistant", "content": null,
        "tool_calls": [{"id": "t", "type": "function",
            "function": {"name": "send", "arguments": "{\"to\":\"<EMAIL_1>\"}"}}]}}]});
    let h = Harness::with(plugins()).await;
    h.openai
        .answer(json_response(StatusCode::OK, &whole.to_string()));
    let body = json!({ "model": "gpt-x", "messages": [{ "role": "user", "content": "mail bob" }] });

    let (status, out) = h.text(call(CHAT, body.to_string())).await;

    assert_eq!(status, StatusCode::OK, "{out}");
    let said: Value = serde_json::from_str(&out).unwrap_or_default();
    let (choice, was) = (&said["choices"][0], &whole["choices"][0]);
    let mut call = was["message"]["tool_calls"][0].clone();
    call["function"]["arguments"] = "{\"to\":\"bob@x.io\"}".into();
    assert_eq!(choice["message"]["tool_calls"], json!([call]));
    assert_eq!(choice["finish_reason"], "tool_calls");
    assert_eq!(
        (&said["id"], &said["model"]),
        (&whole["id"], &whole["model"])
    );
}
