//! Translate responses behavior.

use super::*;

#[tokio::test]
async fn an_openai_client_streams_from_anthropic() {
    let h = Harness::new(Vec::new()).await;
    h.anthropic.answer(sse(ANTHROPIC_STREAM));

    let (status, body) = h.text(openai_request("claude-x", true)).await;

    assert_eq!(status, StatusCode::OK);
    h.assert_requests(0, 1);
    let sent = &h.sent()[0];
    assert_eq!(sent.path, "/v1/messages");
    assert_eq!(
        sent.headers.get("anthropic-version"),
        Some(&HeaderValue::from_static("2023-06-01"))
    );
    assert_eq!(
        h.sent_json(0),
        json!({
            "model": "claude-x",
            "max_tokens": 4096,
            "system": "be brief",
            "messages": [{ "role": "user", "content": "hi" }],
            "stream": true,
        })
    );

    let chunks: Vec<Value> = events(&body).into_iter().map(|(_, d)| d).collect();
    assert_eq!(chunks[0]["id"], "msg_1");
    assert_eq!(chunks[0]["model"], "claude-x");
    assert_eq!(chunks[0]["choices"][0]["delta"]["role"], "assistant");
    let text: String = chunks
        .iter()
        .filter_map(|c| {
            c.pointer("/choices/0/delta/content")
                .and_then(Value::as_str)
        })
        .collect();
    assert_eq!(text, "Hello world");
    let n = chunks.len();
    assert_eq!(chunks[n - 3]["choices"][0]["finish_reason"], "stop");
    assert_eq!(
        chunks[n - 2]["usage"],
        json!({ "prompt_tokens": 5, "completion_tokens": 2, "total_tokens": 7 })
    );
    assert_eq!(chunks[n - 1], Value::String("[DONE]".into()));

    assert_eq!(says(h.log.only()), Ok(()));
    let usage = h.usage.total();
    assert_eq!(usage.input_tokens, 5);
    assert_eq!(usage.output_tokens, 2);
}

#[tokio::test]
async fn an_anthropic_client_streams_from_openai() {
    let h = Harness::new(Vec::new()).await;
    h.openai.answer(sse(OPENAI_STREAM));

    let (status, body) = h.text(anthropic_request(true)).await;

    assert_eq!(status, StatusCode::OK);
    h.assert_requests(1, 0);
    assert_eq!(h.sent()[0].path, "/v1/chat/completions");
    assert_eq!(
        h.sent_json(0),
        json!({
            "model": "gpt-x",
            "messages": [
                { "role": "system", "content": "be brief" },
                { "role": "user", "content": "hi" },
            ],
            "max_completion_tokens": 100,
            "stream": true,
            "stream_options": { "include_usage": true },
        })
    );

    let events = events(&body);
    let names: Vec<&str> = events.iter().filter_map(|(e, _)| e.as_deref()).collect();
    assert_eq!(
        names,
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
    assert_eq!(events[0].1["message"]["id"], "c1");
    assert_eq!(
        events[2].1["delta"],
        json!({ "type": "text_delta", "text": "Hello" })
    );
    assert_eq!(
        events[5].1,
        json!({
            "type": "message_delta",
            "delta": { "stop_reason": "end_turn", "stop_sequence": null },
            "usage": { "input_tokens": 5, "output_tokens": 2 },
        })
    );
    assert_eq!(says(h.log.only()), Ok(()));
}

#[tokio::test]
async fn the_client_gets_the_streaming_it_asked_for_whatever_a_plugin_asks_of_the_provider() {
    struct Whole;

    #[async_trait]
    impl Plugin for Whole {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            cx.extensions_mut()
                .insert(respan_gateway_conversation::StreamResponse(false));
            Ok(None)
        }
    }

    let h = Harness::new(vec![Arc::new(Whole)]).await;
    let reply = json!({
        "id": "msg_2", "type": "message", "role": "assistant", "model": "claude-x",
        "content": [{ "type": "text", "text": "hi" }],
        "stop_reason": "end_turn", "usage": { "input_tokens": 1, "output_tokens": 1 },
    });
    h.anthropic
        .answer(json_response(StatusCode::OK, &reply.to_string()));

    let (_, body) = h.text(openai_request("claude-x", true)).await;

    let sent: Value = serde_json::from_slice(h.sent()[0].raw.as_ref()).unwrap_or_default();
    assert_eq!(
        sent["stream"],
        Value::Null,
        "the provider was asked for a whole answer"
    );
    assert!(body.starts_with("data: "), "{body}");
    assert!(body.contains("\"content\":\"hi\""), "{body}");
}

#[tokio::test]
async fn unary_responses_translate_both_ways() {
    let h = Harness::new(Vec::new()).await;
    let reply = json!({
        "id": "msg_2", "type": "message", "role": "assistant", "model": "claude-x",
        "content": [{ "type": "text", "text": "hi there" }],
        "stop_reason": "max_tokens", "stop_sequence": null,
        "usage": { "input_tokens": 4, "output_tokens": 2 },
    });
    h.anthropic
        .answer(json_response(StatusCode::OK, &reply.to_string()));

    let (_, body) = h.text(openai_request("claude-x", false)).await;

    let v: Value = serde_json::from_str(&body).unwrap_or_default();
    assert_eq!(v["id"], "msg_2");
    assert_eq!(v["object"], "chat.completion");
    assert_eq!(
        v["choices"][0],
        json!({
            "index": 0,
            "message": { "role": "assistant", "content": "hi there" },
            "finish_reason": "length",
        })
    );
    assert_eq!(
        v["usage"],
        json!({ "prompt_tokens": 4, "completion_tokens": 2, "total_tokens": 6 })
    );

    let h = Harness::new(Vec::new()).await;
    let reply = json!({
        "id": "c2", "object": "chat.completion", "model": "gpt-x",
        "choices": [{ "index": 0, "message": { "role": "assistant", "content": "hi there" }, "finish_reason": "stop" }],
        "usage": { "prompt_tokens": 4, "completion_tokens": 2 },
    });
    h.openai
        .answer(json_response(StatusCode::OK, &reply.to_string()));

    let (_, body) = h.text(anthropic_request(false)).await;

    let v: Value = serde_json::from_str(&body).unwrap_or_default();
    assert_eq!(
        v,
        json!({
            "id": "c2", "type": "message", "role": "assistant", "model": "gpt-x",
            "content": [{ "type": "text", "text": "hi there" }],
            "stop_reason": "end_turn", "stop_sequence": null,
            "usage": { "input_tokens": 4, "output_tokens": 2 },
        })
    );
}

#[tokio::test]
async fn generated_named_model_text_and_usage_relay_in_native_formats() {
    let router = ModelRouter::new(|c: &Input| c.model.as_deref()).route("gpt", "openai");
    let h = Harness::with(vec![Arc::new(router)]).await;
    for stream in [false, true] {
        h.openai.answer(Answer::text("Hello from OpenAI"));
        let mut req = json!({ "model": "gpt-4o", "stream": stream,
            "messages": [{ "role": "user", "content": "hi" }] });
        if stream {
            req["stream_options"] = json!({ "include_usage": true });
        }
        let (status, out) = h.text(call(CHAT, req.to_string())).await;
        assert_eq!(status, StatusCode::OK, "{out}");
        if stream {
            let frames = events(&out);
            let text: String = frames
                .iter()
                .filter_map(|(_, c)| {
                    c.pointer("/choices/0/delta/content")
                        .and_then(Value::as_str)
                })
                .collect();
            assert_eq!(text, "Hello from OpenAI");
            assert!(
                frames
                    .iter()
                    .any(|(_, c)| c["usage"]["prompt_tokens"].as_u64().is_some_and(|n| n > 0))
            );
            assert!(out.ends_with("data: [DONE]\n\n"), "{out}");
        } else {
            let response: Value = serde_json::from_str(&out).expect("valid response JSON");
            assert_eq!(
                response["choices"][0]["message"]["content"],
                "Hello from OpenAI"
            );
            assert!(
                response["usage"]["prompt_tokens"]
                    .as_u64()
                    .is_some_and(|n| n > 0)
            );
        }

        h.anthropic.answer(Answer::text("Hello from Anthropic"));
        let req = json!({ "model": "claude-sonnet-4-5", "max_tokens": 100,
            "stream": stream, "messages": [{ "role": "user", "content": "hi" }] });
        let (status, out) = h.text(call(MESSAGES, req.to_string())).await;
        assert_eq!(status, StatusCode::OK, "{out}");
        if stream {
            assert_eq!(anthropic_texts(&out), "Hello from Anthropic");
            assert!(
                events(&out)
                    .iter()
                    .any(|(_, c)| c["usage"]["output_tokens"].as_u64().is_some_and(|n| n > 0))
            );
        } else {
            let response: Value = serde_json::from_str(&out).expect("valid response JSON");
            assert_eq!(response["content"][0]["text"], "Hello from Anthropic");
            assert!(
                response["usage"]["output_tokens"]
                    .as_u64()
                    .is_some_and(|n| n > 0)
            );
        }
    }
    h.assert_requests(2, 2);
    assert!(
        h.openai
            .requests()
            .iter()
            .all(|r| r.body["model"] == "gpt-4o")
    );
    assert!(
        h.anthropic
            .requests()
            .iter()
            .all(|r| r.body["model"] == "claude-sonnet-4-5")
    );
}

#[tokio::test]
async fn generated_anthropic_reasoning_relays_without_reinterpreting_thinking() {
    let h = Harness::with(Vec::new()).await;
    for stream in [false, true] {
        h.anthropic
            .answer(Answer::text("Answer").reasoning("First consider the question"));
        let req = json!({
            "model": "claude-sonnet-4-5", "max_tokens": 2048, "stream": stream,
            "thinking": { "type": "enabled", "budget_tokens": 1024, "display": "summarized" },
            "messages": [{ "role": "user", "content": "hi" }],
        });
        let (status, out) = h.text(call(MESSAGES, req.to_string())).await;
        assert_eq!(status, StatusCode::OK, "{out}");
        if stream {
            let frames = events(&out);
            let thinking: String = frames
                .iter()
                .filter_map(|(_, c)| c["delta"]["thinking"].as_str())
                .collect();
            assert_eq!(thinking, "First consider the question");
            assert_eq!(anthropic_texts(&out), "Answer");
            assert!(frames.iter().any(|(_, c)| {
                c["usage"]["output_tokens_details"]["thinking_tokens"]
                    .as_u64()
                    .is_some_and(|n| n > 0)
            }));
        } else {
            let response: Value = serde_json::from_str(&out).expect("valid response JSON");
            assert_eq!(response["content"][0]["type"], "thinking");
            assert_eq!(
                response["content"][0]["thinking"],
                "First consider the question"
            );
            assert_eq!(response["content"][1]["text"], "Answer");
            assert!(
                response["usage"]["output_tokens_details"]["thinking_tokens"]
                    .as_u64()
                    .is_some_and(|n| n > 0)
            );
        }
    }
    h.assert_requests(0, 2);
}

#[tokio::test]
async fn a_plugin_can_stream_from_facts_in_the_client_format() {
    let h = Harness::new(vec![Arc::new(Canned)]).await;

    let (_, body) = h.text(anthropic_request(true)).await;

    let names: Vec<Option<String>> = events(&body).into_iter().map(|(e, _)| e).collect();
    let names: Vec<&str> = names.iter().flatten().map(String::as_str).collect();
    assert_eq!(
        names,
        [
            "message_start",
            "content_block_start",
            "content_block_delta",
            "content_block_stop",
            "message_delta",
            "message_stop",
        ]
    );
    assert!(body.contains("\"id\":\"cached_1\""), "{body}");
    assert!(h.sent().is_empty());
}

#[tokio::test]
async fn an_anthropic_stream_is_relayed_event_for_event() {
    let h = Harness::new(Vec::new()).await;
    h.anthropic.answer(sse(ANTHROPIC_STREAM));
    // Same format end to end: nothing is translated, so tools are fine.
    let body = json!({
        "model": "claude-x",
        "max_tokens": 10,
        "stream": true,
        "messages": [{ "role": "user", "content": "hi" }],
        "tools": [{ "name": "f", "input_schema": { "type": "object" } }],
    });

    let (status, out) = h.text(call(MESSAGES, body.to_string())).await;

    assert_eq!(status, StatusCode::OK);
    // Every event, the keep-alive too, as it said it.
    assert_eq!(semantic(&out), semantic(&ANTHROPIC_STREAM.concat()));
}

#[tokio::test]
async fn a_model_nothing_answers_is_a_404_in_the_client_format() {
    let h = Harness::with(Vec::new()).await;

    let (status, body) = h.text(call(CHAT, openai_body("llama-3", false))).await;

    assert_eq!(status, StatusCode::NOT_FOUND);
    let v: Value = serde_json::from_str(&body).unwrap_or_default();
    assert_eq!(v["error"]["message"], "nothing answers this request");
    assert!(h.sent().is_empty());
}

#[tokio::test]
async fn provider_stream_quirks_translate_cleanly() {
    // Azure's shape: an opening chunk with no id, then finish and usage in
    // one chunk.
    let quirky = [
        "data: {\"id\":\"\",\"choices\":[],\"prompt_filter_results\":[]}\n\n",
        "data: {\"id\":\"c9\",\"model\":\"gpt-x\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Hi\"}}]}\n\n",
        "data: {\"id\":\"c9\",\"model\":\"gpt-x\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"length\"}],\"usage\":{\"prompt_tokens\":3,\"completion_tokens\":1}}\n\n",
        "data: [DONE]\n\n",
    ];
    let h = Harness::new(Vec::new()).await;
    h.openai.answer(sse(&quirky));

    let (_, body) = h.text(anthropic_request(true)).await;

    let events = events(&body);
    assert_eq!(events[0].1["message"]["id"], "c9");
    let delta = events
        .iter()
        .find(|(e, _)| e.as_deref() == Some("message_delta"));
    assert_eq!(
        delta.map(|(_, d)| d.clone()),
        Some(json!({
            "type": "message_delta",
            "delta": { "stop_reason": "max_tokens", "stop_sequence": null },
            "usage": { "input_tokens": 3, "output_tokens": 1 },
        }))
    );
}

#[tokio::test]
async fn several_anthropic_text_blocks_reach_an_openai_client_in_order() {
    let reply = json!({
        "id": "msg_3", "type": "message", "role": "assistant", "model": "claude-x",
        "content": [{ "type": "text", "text": "one " }, { "type": "text", "text": "two" }],
        "stop_reason": "end_turn",
        "usage": { "input_tokens": 1, "output_tokens": 2 },
    });
    let h = Harness::new(Vec::new()).await;
    h.anthropic
        .answer(json_response(StatusCode::OK, &reply.to_string()));

    let (_, body) = h.text(openai_request("claude-x", false)).await;

    let v: Value = serde_json::from_str(&body).unwrap_or_default();
    assert_eq!(v["choices"][0]["message"]["content"], "one two");
}

#[tokio::test]
async fn a_translated_stream_split_mid_event_decodes_once() {
    let whole = ANTHROPIC_STREAM.concat();
    let (a, b) = whole.split_at(whole.len() / 2);
    let split = http::Response::builder()
        .header("content-type", "text/event-stream")
        .body(vec![
            Ok(bytes::Bytes::from(a.to_owned())),
            Ok(bytes::Bytes::from(b.to_owned())),
        ])
        .unwrap_or_else(|e| panic!("{e}"));
    let h = Harness::with_local(vec![Arc::new(Cross)], None, Some(one_response(split))).await;

    let (_, body) = h.text(openai_request("claude-x", true)).await;

    let text: String = events(&body)
        .iter()
        .filter_map(|(_, c)| {
            c.pointer("/choices/0/delta/content")
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .collect();
    assert_eq!(text, "Hello world");
    assert_eq!(says(h.log.only()), Ok(()));
}

#[tokio::test]
async fn a_translated_response_keeps_the_providers_retry_after() {
    let h = Harness::new(Vec::new()).await;
    h.openai
        .answer(Answer::error(429).message("slow down").retry_after(7));

    let (status, headers, _) = read_whole(serve(&h.gw, anthropic_request(false)).await.0).await;

    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(
        headers.get("retry-after"),
        Some(&HeaderValue::from_static("7"))
    );
}

#[tokio::test]
async fn an_empty_plugin_made_stream_is_a_whole_anthropic_stream() {
    struct Silent;

    #[async_trait]
    impl Plugin for Silent {
        async fn on_client(&self, _cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let none = futures::stream::empty::<Result<Output, Fault>>();
            Ok(Some(Response::stream(none)))
        }
    }

    let h = Harness::new(vec![Arc::new(Silent)]).await;

    let (_, body) = h.text(anthropic_request(true)).await;

    let names: Vec<String> = events(&body).into_iter().filter_map(|(e, _)| e).collect();
    assert_eq!(names, ["message_start", "message_delta", "message_stop"]);
}

#[tokio::test]
async fn a_real_openai_reply_reaches_an_anthropic_client() {
    let h = Harness::new(Vec::new()).await;
    // OpenAI's real shape, with null and empty fields.
    let reply = r#"{"id":"chatcmpl-1","object":"chat.completion","created":1,"model":"gpt-x","choices":[{"index":0,"message":{"role":"assistant","content":"hi","refusal":null,"annotations":[]},"logprobs":null,"finish_reason":"stop"}],"usage":{"prompt_tokens":3,"completion_tokens":1,"total_tokens":4,"prompt_tokens_details":{"cached_tokens":0,"audio_tokens":0},"completion_tokens_details":{"reasoning_tokens":0,"audio_tokens":0,"accepted_prediction_tokens":0,"rejected_prediction_tokens":0}},"service_tier":"default","system_fingerprint":"fp"}"#;
    h.openai.answer(json_response(StatusCode::OK, reply));

    let (status, body) = h.text(anthropic_request(false)).await;

    assert_eq!(status, StatusCode::OK, "{body}");
    let v: Value = serde_json::from_str(&body).unwrap_or_default();
    assert_eq!(v["content"], json!([{ "type": "text", "text": "hi" }]));
}

#[tokio::test]
async fn an_anthropic_stream_of_data_lines_only_is_written_once() {
    let h = Harness::new(vec![Arc::new(Unmask)]).await;
    let stream: Vec<String> = ANTHROPIC_STREAM
        .iter()
        .map(|e| {
            e.lines()
                .filter(|l| !l.starts_with("event:"))
                .map(|l| format!("{l}\n"))
                .collect()
        })
        .collect();
    let chunks: Vec<&str> = stream.iter().map(String::as_str).collect();
    h.anthropic.answer(sse(&chunks));
    let body = json!({
        "model": "claude-x", "max_tokens": 10, "stream": true,
        "messages": [{ "role": "user", "content": "hi" }],
    });

    let (status, out) = h.text(call(MESSAGES, body.to_string())).await;

    assert_eq!(status, StatusCode::OK, "{out}");
    let types: Vec<String> = events(&out)
        .into_iter()
        .filter_map(|(_, v)| v["type"].as_str().map(str::to_owned))
        .collect();
    let count = |t: &str| types.iter().filter(|n| *n == t).count();
    assert_eq!(
        (
            count("message_start"),
            count("message_delta"),
            count("message_stop")
        ),
        (1, 1, 1),
        "{out}"
    );
}

#[tokio::test]
async fn a_guardrail_ending_an_anthropic_stream_early_ends_it() {
    /// Cuts the answer at " world", ending it as filtered.
    #[derive(Clone)]
    struct Cut;

    impl Filter<Output> for Cut {
        fn edit(&mut self, reply: Output, out: &mut Vec<Output>) -> Result<(), Fault> {
            match reply {
                Output::Delta {
                    block,
                    delta: Delta::Text(text),
                } if text == " world" => {
                    out.push(Output::text(block, " [cut]"));
                    out.push(Output::finished(Finish::Filtered));
                }
                reply => out.push(reply),
            }
            Ok(())
        }
    }

    let stream: Vec<String> = ANTHROPIC_STREAM.iter().map(|s| s.to_string()).collect();
    let out = anthropic_through(Cut, &stream).await;

    assert!(!out.contains("event: error"), "{out}");
    assert_eq!(anthropic_texts(&out), "Hello [cut]");
}

#[tokio::test]
async fn an_anthropic_response_is_reframed_as_its_client_asked() {
    let h = Harness::new(vec![Arc::new(Flip)]).await;
    let whole = json!({
        "id": "msg_2", "type": "message", "role": "assistant", "model": "claude-x",
        "content": [{ "type": "text", "text": "hi" }],
        "stop_reason": "end_turn", "usage": { "input_tokens": 1, "output_tokens": 1 },
    });
    h.anthropic
        .answer(json_response(StatusCode::OK, &whole.to_string()));
    h.anthropic.answer(sse(ANTHROPIC_STREAM));
    let ask = |stream: bool| {
        let body = json!({
            "model": "claude-x", "max_tokens": 10, "stream": stream,
            "messages": [{ "role": "user", "content": "hi" }],
        });
        call(MESSAGES, body.to_string())
    };

    let (_, streamed) = h.text(ask(true)).await;
    let (_, whole) = h.text(ask(false)).await;

    let names: Vec<_> = events(&streamed)
        .into_iter()
        .filter_map(|(n, _)| n)
        .collect();
    assert_eq!(names.first().map(String::as_str), Some("message_start"));
    assert_eq!(names.last().map(String::as_str), Some("message_stop"));
    assert!(streamed.contains("\"text\":\"hi\""), "{streamed}");
    let whole: Value = serde_json::from_str(&whole).unwrap_or_default();
    assert_eq!(whole["content"][0]["text"], "Hello world", "{whole}");
    assert_eq!(whole["usage"]["output_tokens"], 2, "{whole}");
}

#[tokio::test]
async fn a_translated_call_passes_none_of_the_clients_headers() {
    let h = Harness::new(Vec::new()).await;
    h.anthropic.answer(sse(ANTHROPIC_STREAM));
    let mut req = openai_request("claude-x", true);
    for (name, value) in [("user-agent", "OpenAI/Python 1.0"), ("x-app", "notes")] {
        req.headers_mut()
            .insert(name, HeaderValue::from_static(value));
    }

    let _ = h.send(req).await;

    let sent = h.sent();
    for name in ["user-agent", "x-app"] {
        assert!(sent[0].headers.get(name).is_none(), "{name}");
    }
}

/// The client's own untranslatable field, unchanged by the plugin (which only
/// aliased the model), is still the client's fault: a 400, as it is without
/// the plugin.
#[tokio::test]
async fn an_untranslatable_client_field_stays_the_clients_fault_when_a_plugin_only_aliases_the_model()
 {
    let h = Harness::new(vec![Arc::new(Alias)]).await;
    let format = json!({ "type": "json_object" });
    let body = json!({ "model": "claude-x", "messages": [{ "role": "user", "content": "hi" }], "response_format": format });
    let (status, text) = h.text(call(CHAT, body.to_string())).await;
    let code = match h.log.only() {
        Err(f) => fault_code(&f),
        Ok(()) => "ok".into(),
    };
    assert_eq!(
        (status, code.as_str()),
        (StatusCode::BAD_REQUEST, "bad_request"),
        "{text}"
    );
}

/// OpenAI's `strict` on a function asks for schema-exact arguments; sent to
/// Anthropic it must be kept or refused, not dropped.
#[tokio::test]
async fn a_strict_function_is_not_silently_loosened() {
    let h = Harness::new(Vec::new()).await;
    h.anthropic.answer(sse(ANTHROPIC_STREAM));
    let tool = json!({ "type": "function", "function": { "name": "read", "strict": true,
        "parameters": { "type": "object", "properties": {}, "additionalProperties": false } } });
    let body = json!({ "model": "claude-x", "tools": [tool], "messages": [{ "role": "user", "content": "hi" }] });
    let (status, _) = h.text(call(CHAT, body.to_string())).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

/// A repeated id on every delta of one call, as some providers send, is
/// one call.
#[tokio::test]
async fn a_provider_repeating_a_calls_start_on_every_delta_makes_one_call() {
    const REPEATED: &[&str] = &[
        "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"model\":\"gpt-x\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_a\",\"type\":\"function\",\"function\":{\"name\":\"read\",\"arguments\":\"{\\\"path\\\":\"}}]}}]}\n\n",
        "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"model\":\"gpt-x\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_a\",\"type\":\"function\",\"function\":{\"name\":\"read\",\"arguments\":\"\\\"a\\\"}\"}}]}}]}\n\n",
        "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"model\":\"gpt-x\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"tool_calls\"}]}\n\n",
        "data: [DONE]\n\n",
    ];
    let h = Harness::new(Vec::new()).await;
    h.openai.answer(sse(REPEATED));
    let body = json!({ "model": "gpt-x", "max_tokens": 100, "stream": true,
        "tools": [{ "name": "read", "input_schema": { "type": "object" } }],
        "messages": [{ "role": "user", "content": "hi" }] });
    let (_, out) = h.text(call(MESSAGES, body.to_string())).await;
    let sent = events(&out);
    let starts = sent
        .iter()
        .filter(|(_, d)| d["type"] == "content_block_start")
        .count();
    let json: String = sent
        .iter()
        .filter_map(|(_, d)| d["delta"]["partial_json"].as_str())
        .collect();
    assert_eq!((starts, json.as_str()), (1, r#"{"path":"a"}"#), "{out}");
}

/// A text block after a tool call, then the tool call's arguments (index 0
/// again): an Anthropic client must still get the arguments in the tool's
/// block, never a mangled stream.
#[tokio::test]
async fn l4_openai_arguments_after_interleaved_text_reach_an_anthropic_client() {
    const MIXED: &[&str] = &[
        "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"model\":\"gpt-x\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"tool_calls\":[{\"index\":0,\"id\":\"call_a\",\"type\":\"function\",\"function\":{\"name\":\"read\",\"arguments\":\"\"}}]}}]}\n\n",
        "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"model\":\"gpt-x\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hm\"}}]}\n\n",
        "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"model\":\"gpt-x\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"arguments\":\"{\\\"path\\\":\\\"a\\\"}\"}}]}}]}\n\n",
        "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"model\":\"gpt-x\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"tool_calls\"}]}\n\n",
        "data: [DONE]\n\n",
    ];
    let h = Harness::new(Vec::new()).await;
    h.openai.answer(sse(MIXED));
    let body = json!({ "model": "gpt-x", "max_tokens": 100, "stream": true,
        "tools": [{ "name": "read", "input_schema": { "type": "object" } }],
        "messages": [{ "role": "user", "content": "hi" }] });
    let (status, out) = h.text(call(MESSAGES, body.to_string())).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    h.assert_requests(1, 0);
    let events = events(&out);
    assert!(
        events
            .iter()
            .any(|(_, d)| d["content_block"]["id"] == "call_a"),
        "{out}"
    );
    assert!(
        events.iter().any(|(_, d)| d["delta"]["text"] == "hm"),
        "{out}"
    );
    // The arguments are in the tool's own block, which stops before the
    // text's opens: a stream that is never mangled.
    let args: String = events
        .iter()
        .filter_map(|(_, d)| d["delta"]["partial_json"].as_str())
        .collect();
    assert_eq!(args, r#"{"path":"a"}"#, "{out}");
    assert!(!out.contains("event: error"), "{out}");
    let names: Vec<String> = events.iter().filter_map(|(n, _)| n.clone()).collect();
    assert_well_formed(&names, &out);
}
