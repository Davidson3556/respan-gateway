//! What a format has that the kind cannot hold survives a trip through the
//! same format, and another format drops it if that is harmless, else
//! refuses it.

use respan_gateway_conversation::Schema;
use respan_gateway_conversation::anthropic::AnthropicMessages;
use respan_gateway_conversation::openai::OpenAiChat;
use respan_gateway_conversation::{Block, Content, Finish, Output, Role};
use respan_gateway_core::Upstream;
use respan_gateway_http::bytes::Bytes;
use respan_gateway_http::http::{self, HeaderValue, StatusCode, header::CONTENT_TYPE};
use respan_gateway_http::{Body, Status};
use serde_json::{Value, json};

const OPENAI: OpenAiChat = OpenAiChat::OPENAI;
const ANTHROPIC: AnthropicMessages = AnthropicMessages;

/// `body` as a request of format `from`, written as a request of format `to`.
fn sent(
    from: impl Schema,
    to: impl Schema,
    body: &Value,
) -> Result<Value, respan_gateway_core::Fault> {
    let chat = from.decode_request(Bytes::from(body.to_string()))?;
    Ok(to.encode_request(&chat)?.into())
}

fn same(schema: impl Schema, body: &Value) -> Value {
    sent(schema, schema, body).expect("written again")
}

fn refused(from: impl Schema, to: impl Schema, body: &Value) -> bool {
    sent(from, to, body).is_err()
}

fn anthropic(messages: Value) -> Value {
    json!({ "model": "m", "max_tokens": 5, "messages": messages })
}

fn openai(messages: Value) -> Value {
    json!({ "model": "m", "messages": messages })
}

#[test]
fn text_part_fields_survive_the_same_format_and_a_cache_marker_is_dropped_by_another() {
    let marked =
        json!({ "type": "text", "text": "the repo", "cache_control": { "type": "ephemeral" } });
    let body = anthropic(
        json!([{ "role": "user", "content": [marked, { "type": "text", "text": "hi" }] }]),
    );
    assert_eq!(same(ANTHROPIC, &body), body);
    let chat = ANTHROPIC
        .decode_request(Bytes::from(body.to_string()))
        .expect("chat");
    assert!(chat.messages[0].is_plain());
    assert_eq!(chat.messages[0].text(), "the repoh\u{69}");
    let to_openai = sent(ANTHROPIC, OPENAI, &body).expect("translated");
    assert_eq!(
        to_openai["messages"][0]["content"],
        json!([{ "type": "text", "text": "the repo" }, { "type": "text", "text": "hi" }])
    );
    // Another field of a text part is not harmless.
    let cited = json!({ "type": "text", "text": "x", "citations": [{ "type": "c" }] });
    let body = anthropic(json!([{ "role": "user", "content": [cited] }]));
    assert_eq!(same(ANTHROPIC, &body), body);
    assert!(refused(ANTHROPIC, OPENAI, &body));
    // The same of OpenAI's own text part fields.
    let part = json!({ "type": "text", "text": "x", "vendor": 1 });
    let body = openai(json!([{ "role": "user", "content": [part] }]));
    assert_eq!(same(OPENAI, &body), body);
    assert!(refused(OPENAI, ANTHROPIC, &body));
}

#[test]
fn tool_part_fields_survive_the_same_format_and_a_cache_marker_is_dropped_by_another() {
    let marker = json!({ "type": "ephemeral" });
    let call =
        json!({ "type": "tool_use", "id": "t", "name": "f", "input": {}, "cache_control": marker });
    let result = json!({ "type": "tool_result", "tool_use_id": "t", "content": "x", "cache_control": marker });
    let body = anthropic(json!([
        { "role": "user", "content": "go" },
        { "role": "assistant", "content": [call] },
        { "role": "user", "content": [result] },
    ]));
    assert_eq!(same(ANTHROPIC, &body), body);
    let chat = ANTHROPIC
        .decode_request(Bytes::from(body.to_string()))
        .expect("chat");
    assert!(
        matches!(&chat.messages[1].content[0], Content::ToolCall { native, .. } if native.as_ref().is_some_and(|n| !n.is_empty()))
    );
    assert!(
        matches!(&chat.messages[2].content[0], Content::ToolResult { native, .. } if native.as_ref().is_some_and(|n| !n.is_empty()))
    );
    let to_openai = sent(ANTHROPIC, OPENAI, &body).expect("translated");
    assert_eq!(to_openai["messages"][2]["role"], "tool");
    // Another field of a tool part is not harmless.
    let vendor_call =
        json!({ "type": "tool_use", "id": "t", "name": "f", "input": {}, "vendor": 1 });
    let body = anthropic(json!([
        { "role": "user", "content": "go" },
        { "role": "assistant", "content": [vendor_call] },
    ]));
    assert!(refused(ANTHROPIC, OPENAI, &body));
}

#[test]
fn a_cache_marker_on_a_tool_definition_a_tool_result_text_or_media_is_dropped_by_another_format() {
    let marker = json!({ "type": "ephemeral" });
    let marked_text = json!({ "type": "text", "text": "x", "cache_control": marker });
    let image = json!({ "type": "image", "cache_control": marker,
        "source": { "type": "url", "url": "https://example.com/a.png" } });
    let mut body = anthropic(json!([
        { "role": "user", "content": "go" },
        { "role": "assistant", "content": [
            { "type": "tool_use", "id": "t", "name": "f", "input": {} }] },
        { "role": "user", "content": [
            { "type": "tool_result", "tool_use_id": "t", "content": [marked_text] },
            image] },
    ]));
    body["tools"] = json!([{ "name": "f", "input_schema": { "type": "object" },
        "cache_control": marker }]);
    assert_eq!(same(ANTHROPIC, &body), body);
    let to_openai = sent(ANTHROPIC, OPENAI, &body).expect("translated");
    assert!(
        !to_openai.to_string().contains("cache_control"),
        "{to_openai}"
    );
}

#[test]
fn a_role_and_a_finish_of_a_format_are_written_by_that_format_alone() {
    let body =
        openai(json!([{ "role": "critic", "content": "hm" }, { "role": "user", "content": "hi" }]));
    assert_eq!(same(OPENAI, &body), body);
    let chat = OPENAI
        .decode_request(Bytes::from(body.to_string()))
        .expect("chat");
    assert_eq!(chat.messages[0].role, Role::native("openai-chat", "critic"));
    assert!(refused(OPENAI, ANTHROPIC, &body));

    let ask = http::Request::new(Bytes::from(json!({ "stream": true }).to_string()));
    let ask_whole = http::Request::new(Bytes::from_static(b"{}"));
    let call = http::Request::new(()).into_parts().0;
    let mut head = http::Response::new(()).into_parts().0;
    head.headers
        .insert(CONTENT_TYPE, HeaderValue::from_static("text/event-stream"));
    let stream = "data: {\"id\":\"c\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi\"},\"finish_reason\":\"insufficient_system_resource\"}]}\n\ndata: [DONE]\n\n";
    let replies = OpenAiChat::OPENAI
        .decode_response(http::Response::from_parts(head, Bytes::from(stream)), &call)
        .expect("decoded");
    assert!(replies.contains(&Output::finished(Finish::native(
        "openai-chat",
        "insufficient_system_resource"
    ))));
    let streamed = OpenAiChat::OPENAI
        .encode_response(&ask, &replies)
        .expect("a stream");
    let streamed = String::from_utf8_lossy(streamed.body()).into_owned();
    assert!(
        streamed.contains("\"finish_reason\":\"insufficient_system_resource\""),
        "{streamed}"
    );
    let whole = OpenAiChat::OPENAI
        .encode_response(&ask_whole, &replies)
        .expect("a body");
    let whole: Value = serde_json::from_slice(whole.body()).unwrap_or_default();
    assert_eq!(
        whole["choices"][0]["finish_reason"],
        "insufficient_system_resource"
    );
    // Another format cannot say it.
    assert!(AnthropicMessages.encode_response(&ask, &replies).is_err());
    assert!(
        AnthropicMessages
            .encode_response(&ask_whole, &replies)
            .is_err()
    );
}

#[test]
fn the_developer_role_is_a_role_of_its_own_that_anthropic_writes_as_system() {
    let body = openai(json!([
        { "role": "developer", "content": "be brief" },
        { "role": "user", "content": "hi" },
    ]));
    assert_eq!(same(OPENAI, &body), body);
    let chat = OPENAI
        .decode_request(Bytes::from(body.to_string()))
        .expect("chat");
    assert_eq!(chat.messages[0].role, Role::Developer);
    assert!(chat.messages[0].is_plain());
    let to_anthropic = sent(OPENAI, ANTHROPIC, &body).expect("translated");
    assert_eq!(to_anthropic["system"], "be brief");
    // After the conversation starts it has no place in Anthropic.
    let late = openai(json!([
        { "role": "user", "content": "hi" },
        { "role": "developer", "content": "be brief" },
    ]));
    assert!(refused(OPENAI, ANTHROPIC, &late));
    // Edited to another role, it is written as that role.
    let mut edited = chat;
    edited.messages[0].role = Role::User;
    let again = OPENAI
        .encode_request(&edited)
        .map(Value::from)
        .unwrap_or_default();
    assert_eq!(again["messages"][0]["role"], "user");
}

#[test]
fn semantic_limits_are_written_with_each_formats_canonical_key() {
    for key in ["max_tokens", "max_completion_tokens"] {
        let mut body = openai(json!([{ "role": "user", "content": "hi" }]));
        body[key] = 9.into();
        let mut canonical = body.clone();
        canonical
            .as_object_mut()
            .expect("request object")
            .remove("max_tokens");
        canonical["max_completion_tokens"] = json!(9);
        assert_eq!(same(OPENAI, &body), canonical);
        let to_anthropic = sent(OPENAI, ANTHROPIC, &body).expect("translated");
        assert_eq!(to_anthropic["max_tokens"], 9);
        assert!(to_anthropic.get("max_completion_tokens").is_none());
    }
    let anthropic = anthropic(json!([{ "role": "user", "content": "hi" }]));
    let to_openai = sent(ANTHROPIC, OPENAI, &anthropic).expect("translated");
    assert_eq!(to_openai["max_completion_tokens"], 5);
}

#[test]
fn anthropic_metadata_beside_the_user_id_survives_its_format() {
    let mut body = anthropic(json!([{ "role": "user", "content": "hi" }]));
    body["metadata"] = json!({ "user_id": "u", "trace": "t" });
    assert_eq!(same(ANTHROPIC, &body), body);
    let chat = ANTHROPIC
        .decode_request(Bytes::from(body.to_string()))
        .expect("chat");
    assert_eq!(chat.user.as_deref(), Some("u"));
    assert!(refused(ANTHROPIC, OPENAI, &body));
    // A user id alone translates, and a redacted one is written.
    let mut redacted = chat;
    redacted.user = Some("[x]".into());
    let again = ANTHROPIC
        .encode_request(&redacted)
        .map(Value::from)
        .unwrap_or_default();
    assert_eq!(again["metadata"], json!({ "user_id": "[x]", "trace": "t" }));
}

fn request_of(stream: bool) -> http::Request<Bytes> {
    let body = json!({ "stream": stream, "stream_options": { "include_usage": true } });
    http::Request::new(Bytes::from(body.to_string()))
}

fn decoded(
    decode: impl Fn(http::Response<Bytes>) -> Vec<Output>,
    stream: bool,
    body: &str,
) -> Vec<Output> {
    let mut head = http::Response::new(()).into_parts().0;
    let kind = if stream {
        "text/event-stream"
    } else {
        "application/json"
    };
    head.headers
        .insert(CONTENT_TYPE, HeaderValue::from_static(kind));
    decode(http::Response::from_parts(
        head,
        Bytes::from(body.to_owned()),
    ))
}

#[test]
fn the_vendor_fields_of_openai_usage_survive_its_format_and_are_dropped_by_another() {
    let usage = json!({ "prompt_tokens": 4, "completion_tokens": 7, "total_tokens": 11,
        "prompt_tokens_details": { "cached_tokens": 2, "audio_tokens": 1 },
        "completion_tokens_details": { "reasoning_tokens": 3, "accepted_prediction_tokens": 2 },
        "vendor": { "credits": 5 } });
    let whole = json!({ "id": "c", "model": "m", "choices": [{ "index": 0,
        "message": { "role": "assistant", "content": "hi" }, "finish_reason": "stop" }], "usage": usage });
    let chunk = json!({ "id": "c", "model": "m", "choices": [], "usage": usage });
    let stream = format!(
        "data: {}\n\ndata: {chunk}\n\ndata: [DONE]\n\n",
        json!({ "id": "c", "model": "m", "choices": [{ "index": 0, "delta": { "content": "hi" }, "finish_reason": "stop" }] })
    );
    let call = http::Request::new(()).into_parts().0;
    let decode = |r| {
        OpenAiChat::OPENAI
            .decode_response(r, &call)
            .expect("decoded")
    };
    for (is_stream, text) in [(false, whole.to_string()), (true, stream)] {
        let replies = decoded(decode, is_stream, &text);
        let out = OpenAiChat::OPENAI
            .encode_response(&request_of(is_stream), &replies)
            .expect("written");
        let body = String::from_utf8_lossy(out.body()).into_owned();
        let found: Value = match is_stream {
            false => serde_json::from_str::<Value>(&body).unwrap_or_default()["usage"].clone(),
            true => body
                .split("\n\n")
                .filter_map(|e| serde_json::from_str::<Value>(e.strip_prefix("data: ")?).ok())
                .find_map(|c| c.get("usage").cloned().filter(|u| !u.is_null()))
                .unwrap_or_default(),
        };
        assert_eq!(found, usage, "stream {is_stream}: {body}");
        // Another format drops what it has no place for, and does not refuse.
        let to_anthropic = AnthropicMessages.encode_response(&request_of(is_stream), &replies);
        assert!(to_anthropic.is_ok(), "stream {is_stream}");
    }
}

#[test]
fn the_vendor_fields_of_anthropic_usage_survive_its_format_and_are_dropped_by_another() {
    let start_usage = json!({ "input_tokens": 3, "output_tokens": 1,
        "cache_creation": { "ephemeral_5m_input_tokens": 2 },
        "service_tier": "standard" });
    let end_usage = json!({ "output_tokens": 4, "server_tool_use": { "web_search_requests": 1 } });
    let stream = format!(
        "event: message_start\ndata: {}\n\nevent: content_block_start\ndata: {}\n\nevent: content_block_delta\ndata: {}\n\nevent: content_block_stop\ndata: {}\n\nevent: message_delta\ndata: {}\n\nevent: message_stop\ndata: {}\n\n",
        json!({ "type": "message_start", "message": { "id": "m", "model": "c", "usage": start_usage } }),
        json!({ "type": "content_block_start", "index": 0, "content_block": { "type": "text", "text": "" } }),
        json!({ "type": "content_block_delta", "index": 0, "delta": { "type": "text_delta", "text": "hi" } }),
        json!({ "type": "content_block_stop", "index": 0 }),
        json!({ "type": "message_delta", "delta": { "stop_reason": "end_turn" }, "usage": end_usage }),
        json!({ "type": "message_stop" }),
    );
    let whole = json!({ "id": "m", "type": "message", "role": "assistant", "model": "c",
        "content": [{ "type": "text", "text": "hi" }], "stop_reason": "end_turn", "stop_sequence": null,
        "usage": { "input_tokens": 3, "output_tokens": 4,
            "cache_creation": { "ephemeral_5m_input_tokens": 2 }, "service_tier": "standard",
            "server_tool_use": { "web_search_requests": 1 } } });
    let call = http::Request::new(()).into_parts().0;
    let decode = |r| {
        AnthropicMessages
            .decode_response(r, &call)
            .expect("decoded")
    };
    for (is_stream, text) in [(false, whole.to_string()), (true, stream)] {
        let replies = decoded(decode, is_stream, &text);
        let out = AnthropicMessages
            .encode_response(&request_of(is_stream), &replies)
            .expect("written");
        let body = String::from_utf8_lossy(out.body()).into_owned();
        let found: Value = match is_stream {
            false => serde_json::from_str::<Value>(&body).unwrap_or_default()["usage"].clone(),
            true => body
                .split("\n\n")
                .filter_map(|e| {
                    let data = e.lines().find_map(|l| l.strip_prefix("data: "))?;
                    serde_json::from_str::<Value>(data).ok()
                })
                .filter(|e| e["type"] == "message_delta")
                .find_map(|e| e.get("usage").cloned())
                .unwrap_or_default(),
        };
        for (key, value) in [
            ("service_tier", json!("standard")),
            ("cache_creation", json!({ "ephemeral_5m_input_tokens": 2 })),
            ("server_tool_use", json!({ "web_search_requests": 1 })),
        ] {
            assert_eq!(found[key], value, "stream {is_stream}: {body}");
        }
        let to_openai = OpenAiChat::OPENAI.encode_response(&request_of(is_stream), &replies);
        assert!(to_openai.is_ok(), "stream {is_stream}");
    }
}

#[test]
fn unknown_fields_of_an_anthropic_block_survive_its_format_and_are_dropped_by_another() {
    let blocks = [
        json!({ "type": "text", "text": "hi", "citations_v9": [{ "n": 1 }] }),
        json!({ "type": "tool_use", "id": "t", "name": "f", "input": { "a": 1 }, "caller": { "type": "direct" } }),
        json!({ "type": "thinking", "thinking": "hm", "signature": "sig", "display": "summarized" }),
    ];
    let call = http::Request::new(()).into_parts().0;
    let decode = |r| {
        AnthropicMessages
            .decode_response(r, &call)
            .expect("decoded")
    };
    for block in blocks {
        let whole = json!({ "id": "m", "type": "message", "role": "assistant", "model": "c",
            "content": [block], "stop_reason": "end_turn", "stop_sequence": null,
            "usage": { "input_tokens": 1, "output_tokens": 1 } });
        let mut start = block.clone();
        if let Some(fields) = start.as_object_mut() {
            for (key, empty) in [
                ("text", json!("")),
                ("thinking", json!("")),
                ("input", json!({})),
            ] {
                if let Some(v) = fields.get_mut(key) {
                    *v = empty;
                }
            }
            fields.shift_remove("signature");
        }
        let stream = format!(
            "event: message_start\ndata: {}\n\nevent: content_block_start\ndata: {}\n\nevent: content_block_stop\ndata: {}\n\nevent: message_delta\ndata: {}\n\nevent: message_stop\ndata: {}\n\n",
            json!({ "type": "message_start", "message": { "id": "m", "model": "c", "usage": { "input_tokens": 1, "output_tokens": 0 } } }),
            json!({ "type": "content_block_start", "index": 0, "content_block": start }),
            json!({ "type": "content_block_stop", "index": 0 }),
            json!({ "type": "message_delta", "delta": { "stop_reason": "end_turn" }, "usage": { "output_tokens": 1 } }),
            json!({ "type": "message_stop" }),
        );
        let native: Vec<(String, Value)> = block
            .as_object()
            .into_iter()
            .flatten()
            .filter(|(k, _)| ["citations_v9", "caller", "display"].contains(&k.as_str()))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        assert_eq!(native.len(), 1);
        for (is_stream, text) in [(false, whole.to_string()), (true, stream)] {
            let replies = decoded(decode, is_stream, &text);
            let out = AnthropicMessages
                .encode_response(&request_of(is_stream), &replies)
                .expect("written");
            let body = String::from_utf8_lossy(out.body()).into_owned();
            let found: Value = match is_stream {
                false => {
                    serde_json::from_str::<Value>(&body).unwrap_or_default()["content"][0].clone()
                }
                true => body
                    .split("\n\n")
                    .filter_map(|e| {
                        let data = e.lines().find_map(|l| l.strip_prefix("data: "))?;
                        serde_json::from_str::<Value>(data).ok()
                    })
                    .find(|e| e["type"] == "content_block_start")
                    .map(|e| e["content_block"].clone())
                    .unwrap_or_default(),
            };
            for (key, value) in &native {
                assert_eq!(&found[key.as_str()], value, "stream {is_stream}: {body}");
            }
            // Another format drops them, and does not refuse: a dialect with
            // reasoning, as one block is thinking.
            let to_openai = OpenAiChat::DEEPSEEK.encode_response(&request_of(is_stream), &replies);
            let to_openai = to_openai.expect("translated");
            let words = String::from_utf8_lossy(to_openai.body());
            for (key, _) in &native {
                assert!(!words.contains(key.as_str()), "{words}");
            }
        }
    }
}

#[test]
fn unknown_fields_of_an_openai_tool_call_survive_its_format_and_are_dropped_by_another() {
    let entry = |arguments: &str| {
        json!({ "index": 0, "id": "t", "type": "function",
            "function": { "name": "f", "arguments": arguments, "vendor_fn": null },
            "extra_content": { "thought": "x" }, "empty": [] })
    };
    let chunk = |delta: Value, finish: Value| {
        let choice = json!([{ "index": 0, "delta": delta, "finish_reason": finish }]);
        format!(
            "data: {}\n\n",
            json!({ "id": "c", "model": "m", "choices": choice })
        )
    };
    let stream = [
        chunk(json!({ "tool_calls": [entry("")] }), Value::Null),
        chunk(
            json!({ "tool_calls": [{ "index": 0, "function": { "arguments": "{}" } }] }),
            Value::Null,
        ),
        chunk(json!({}), json!("tool_calls")),
        "data: [DONE]\n\n".to_owned(),
    ]
    .concat();
    let whole = json!({ "id": "c", "model": "m", "choices": [{ "index": 0, "finish_reason": "tool_calls",
        "message": { "role": "assistant", "content": null, "tool_calls": [entry("{}")] } }] });
    let call = http::Request::new(()).into_parts().0;
    let decode = |r| {
        OpenAiChat::OPENAI
            .decode_response(r, &call)
            .expect("decoded")
    };
    for (is_stream, text) in [(false, whole.to_string()), (true, stream)] {
        let replies = decoded(decode, is_stream, &text);
        // The choice stays readable events: no opaque choice among them.
        assert!(
            replies
                .iter()
                .any(|r| matches!(r, Output::BlockStart { .. })),
            "stream {is_stream}"
        );
        let out = OpenAiChat::OPENAI
            .encode_response(&request_of(is_stream), &replies)
            .expect("written");
        let body = String::from_utf8_lossy(out.body()).into_owned();
        let found: Value = match is_stream {
            false => serde_json::from_str::<Value>(&body).unwrap_or_default()["choices"][0]["message"]["tool_calls"][0].clone(),
            true => body
                .split("\n\n")
                .filter_map(|e| serde_json::from_str::<Value>(e.strip_prefix("data: ")?).ok())
                .find_map(|c| c["choices"][0]["delta"]["tool_calls"][0].get("id").map(|_| c["choices"][0]["delta"]["tool_calls"][0].clone()))
                .unwrap_or_default(),
        };
        // Even null and empty ones, whose presence says something.
        assert_eq!(
            found["extra_content"],
            json!({ "thought": "x" }),
            "stream {is_stream}: {body}"
        );
        assert_eq!(found["empty"], json!([]), "stream {is_stream}: {body}");
        assert_eq!(found["function"]["vendor_fn"], Value::Null);
        assert!(found["function"].get("vendor_fn").is_some(), "{found}");
        let to_anthropic = AnthropicMessages.encode_response(&request_of(is_stream), &replies);
        let to_anthropic = to_anthropic.expect("translated");
        assert!(!String::from_utf8_lossy(to_anthropic.body()).contains("extra_content"));
    }
}

#[test]
fn common_media_is_typed_and_translated_without_fetching_urls() {
    use respan_gateway_conversation::{MediaSource, ResponseFormat};
    let image = json!({"type":"image_url","image_url":{"url":"data:image/png;base64,aGk="}});
    let body = openai(json!([{"role":"user","content":[image]}]));
    let chat = OPENAI
        .decode_request(Bytes::from(body.to_string()))
        .expect("supported request");
    assert!(matches!(&chat.messages[0].content[0], Content::Image(media)
        if media.mime_type == "image/png" && media.source == MediaSource::Bytes(Bytes::from_static(b"hi"))));
    let translated = sent(OPENAI, ANTHROPIC, &body).expect("supported request");
    assert_eq!(
        translated["messages"][0]["content"][0],
        json!({"type":"image","source":{"type":"base64","media_type":"image/png","data":"aGk="}})
    );
    assert_eq!(
        sent(ANTHROPIC, OPENAI, &translated).expect("supported request")["messages"],
        body["messages"]
    );

    let mut structured = body.clone();
    structured["response_format"] = json!({"type":"json_schema","json_schema":{
        "name":"answer","strict":true,"schema":{"type":"object","properties":{"ok":{"type":"boolean"}},"required":["ok"],"additionalProperties":false}
    }});
    let typed = OPENAI
        .decode_request(Bytes::from(structured.to_string()))
        .expect("supported request");
    assert!(matches!(
        typed.options.response_format,
        Some(ResponseFormat::JsonSchema { .. })
    ));
    assert!(typed.native.is_none());
    assert_eq!(same(OPENAI, &structured), structured);
    let translated = sent(OPENAI, ANTHROPIC, &structured).expect("supported request");
    assert_eq!(
        translated["output_config"]["format"]["schema"],
        structured["response_format"]["json_schema"]["schema"]
    );
    let reverse = sent(ANTHROPIC, OPENAI, &translated).expect("supported request");
    assert_eq!(reverse["response_format"]["json_schema"]["strict"], true);
}

#[test]
fn media_native_fields_survive_locally_and_prevent_lossy_translation() {
    for part in [
        json!({"type":"image_url","image_url":{"url":"https://example.test/image.png","detail":"high"},"vendor":null}),
        json!({"type":"input_audio","input_audio":{"data":"aGk=","format":"wav","vendor":1}}),
        json!({"type":"file","file":{"file_data":"data:application/pdf;base64,aGk=","filename":"doc.pdf","vendor":null}}),
        json!({"type":"file","file":{"file_id":"file_1"}}),
    ] {
        let body = openai(json!([{"role":"user","content":[part]}]));
        assert_eq!(same(OPENAI, &body), body);
        assert!(refused(OPENAI, ANTHROPIC, &body));
    }
    let document = anthropic(json!([{"role":"user","content":[{
        "type":"document","title":"doc.pdf","source":{"type":"base64","media_type":"application/pdf","data":"aGk="}
    }]}]));
    let translated = sent(ANTHROPIC, OPENAI, &document).expect("supported request");
    assert_eq!(
        translated["messages"][0]["content"][0]["file"]["filename"],
        "doc.pdf"
    );
    assert_eq!(
        sent(OPENAI, ANTHROPIC, &translated).expect("supported request")["messages"],
        document["messages"]
    );
}

#[test]
fn reasoning_is_typed_but_signatures_remain_format_specific() {
    let body =
        openai(json!([{"role":"assistant","reasoning_content":"consider","content":"answer"}]));
    let deepseek = OpenAiChat::DEEPSEEK;
    let chat = deepseek.decode_request(Bytes::from(body.to_string()));
    let chat = chat.expect("supported request");
    assert!(
        chat.messages[0]
            .content
            .iter()
            .any(|p| matches!(p, Content::Reasoning {text,..} if text=="consider"))
    );
    assert_eq!(same(deepseek, &body), body);
    let signed = anthropic(
        json!([{"role":"assistant","content":[{"type":"thinking","thinking":"consider","signature":"sig"}]}]),
    );
    assert_eq!(same(ANTHROPIC, &signed), signed);
    assert!(refused(ANTHROPIC, deepseek, &signed));
}

#[test]
fn unknown_structured_output_options_stay_native() {
    let mut body = openai(json!([{"role":"user","content":"hi"}]));
    body["response_format"] =
        json!({"type":"json_schema","json_schema":{"name":"answer","schema":{},"future_option":1}});
    assert_eq!(same(OPENAI, &body), body);
    assert!(refused(OPENAI, ANTHROPIC, &body));
    let mut body = anthropic(json!([{"role":"user","content":"hi"}]));
    body["output_config"] =
        json!({"format":{"type":"json_schema","schema":{"type":"object"}},"effort":"high"});
    assert_eq!(same(ANTHROPIC, &body), body);
    assert!(refused(ANTHROPIC, OPENAI, &body));
}

#[test]
fn structured_output_translation_does_not_drop_wrapper_instructions() {
    for extra in [
        json!({"description":"answer in French"}),
        json!({"strict":false}),
    ] {
        let mut body = openai(json!([{"role":"user","content":"hi"}]));
        let mut spec = json!({"name":"answer","schema":{"type":"object"}});
        spec.as_object_mut()
            .expect("schema object")
            .extend(extra.as_object().expect("options").clone());
        body["response_format"] = json!({"type":"json_schema","json_schema":spec});
        assert_eq!(same(OPENAI, &body), body);
        assert!(refused(OPENAI, ANTHROPIC, &body));
    }
}

fn usage_of(reply: &Output) -> Option<u64> {
    match reply {
        Output::Usage(u) => Some(u.input_tokens).filter(|&n| n > 0),
        _ => None,
    }
}

#[tokio::test]
async fn held_usage_survives_semantic_failure_but_not_a_wire_failure() {
    use respan_gateway_core::futures::{StreamExt, stream};
    let fixtures = [
        (
            "data: {\"id\":\"c1\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"\"}}],\"usage\":{\"prompt_tokens\":2}}\n\n",
            "data: {\"error\":{\"message\":\"semantic\"}}\n\n",
        ),
        (
            "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"m1\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"m\",\"content\":[],\"usage\":{\"input_tokens\":2,\"output_tokens\":0}}}\n\n",
            "event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\",\"message\":\"semantic\"}}\n\n",
        ),
    ];
    for (provider, (start, error)) in fixtures.into_iter().enumerate() {
        for wire in [false, true] {
            let ending = if wire {
                Err(Upstream::fault("wire"))
            } else {
                Ok(Bytes::from(error))
            };
            let body = Body::stream(stream::iter([Ok(Bytes::from(start)), ending]));
            let mut response = http::Response::new(body);
            response
                .headers_mut()
                .insert(CONTENT_TYPE, HeaderValue::from_static("text/event-stream"));
            let request = http::Request::new(Bytes::new());
            let values = if provider == 0 {
                OPENAI.read_response(response, request)
            } else {
                AnthropicMessages.read_response(response, request)
            }
            .collect::<Vec<_>>()
            .await;
            let error = values
                .last()
                .expect("terminal fault")
                .as_ref()
                .expect_err("stream failed");
            assert_eq!(error.message(), if wire { "wire" } else { "semantic" });
            if wire {
                assert_eq!(
                    values.len(),
                    1,
                    "wire failure must not finalize held starts"
                );
            } else {
                assert_eq!(
                    values.len(),
                    2,
                    "semantic failure retains one usage-only event"
                );
                let usage = values[0].as_ref().expect("salvaged usage");
                assert!(!usage.is_content());
                assert_eq!(usage_of(usage), Some(2));
            }
        }
    }
}

#[tokio::test]
async fn finite_and_streamed_response_reads_share_framing_and_errors() {
    use respan_gateway_core::futures::{TryStreamExt, stream};

    for anthropic in [false, true] {
        for streamed in [false, true] {
            let request = http::Request::new(Bytes::from(
                json!({
                    "stream": streamed, "stream_options": {"include_usage": true}
                })
                .to_string(),
            ));
            let replies = [
                Output::start("r", "m"),
                Output::block_start(0, Block::Text),
                Output::text(0, "hello"),
                Output::block_end(0),
                Output::finished(Finish::Stop),
            ];
            let response = if anthropic {
                AnthropicMessages.encode_response(&request, &replies)
            } else {
                OPENAI.encode_response(&request, &replies)
            }
            .expect("encode finite answer");
            let (head, bytes) = response.into_parts();
            let mut refused = head.clone();
            refused.status = StatusCode::TOO_MANY_REQUESTS;
            let mut empty = head.clone();
            empty.status = StatusCode::NO_CONTENT;
            // Whole values, byte-split SSE, incomplete SSE, malformed JSON,
            // HTTP refusal, and bodyless answers all use the same semantics.
            let cases = [
                (head.clone(), bytes.clone()),
                (head.clone(), bytes.slice(..bytes.len() / 2)),
                (head, Bytes::from_static(b"not-json")),
                (
                    refused,
                    Bytes::from_static(b"{\"error\":{\"message\":\"busy\"}}"),
                ),
                (empty, Bytes::from_static(b"unread bodyless garbage")),
            ];
            for (head, bytes) in cases {
                let call = request.clone().into_parts().0;
                let finite = http::Response::from_parts(head.clone(), bytes.clone());
                let finite = if anthropic {
                    AnthropicMessages.decode_response(finite, &call)
                } else {
                    OPENAI.decode_response(finite, &call)
                };
                let chunks: Vec<_> = bytes
                    .iter()
                    .map(|b| Ok(Bytes::copy_from_slice(&[*b])))
                    .collect();
                let live = http::Response::from_parts(head, Body::stream(stream::iter(chunks)));
                let live = if anthropic {
                    AnthropicMessages.read_response(live, request.clone())
                } else {
                    OPENAI.read_response(live, request.clone())
                }
                .try_collect::<Vec<_>>()
                .await;
                match (finite, live) {
                    (Ok(finite), Ok(live)) => assert_eq!(finite, live),
                    (Err(finite), Err(live)) => {
                        assert_eq!(
                            respan_gateway_http::error_code(&finite),
                            respan_gateway_http::error_code(&live)
                        );
                        assert_eq!(finite.message(), live.message());
                    }
                    (finite, live) => panic!("finite/live mismatch: {finite:?} / {live:?}"),
                }
            }
        }
    }
}

#[test]
fn native_context_window_finish_survives_reframing_and_translates_as_length() {
    let request = |stream| http::Request::new(Bytes::from(json!({"stream": stream}).to_string()));
    let call = http::Request::new(()).into_parts().0;
    let native = "model_context_window_exceeded";
    let unary = json!({
        "id": "m", "type": "message", "role": "assistant", "model": "claude-x",
        "content": [{"type": "text", "text": "partial"}],
        "stop_reason": native,
        "usage": {"input_tokens": 4, "output_tokens": 2}
    });
    let as_unary = http::Response::new(Bytes::from(unary.to_string()));
    let replies = AnthropicMessages
        .decode_response(as_unary, &call)
        .expect("decode unary");
    let framed = AnthropicMessages
        .encode_response(&request(true), &replies)
        .expect("SSE");
    let body = String::from_utf8_lossy(framed.body());
    assert!(
        body.contains(&format!("\"stop_reason\":\"{native}\"")),
        "{body}"
    );
    let translated = OPENAI
        .encode_response(&request(false), &replies)
        .expect("translated");
    let translated: Value = serde_json::from_slice(translated.body()).expect("JSON");
    assert_eq!(translated["choices"][0]["finish_reason"], "length");

    let sse = format!(
        "event: message_start\ndata: {{\"type\":\"message_start\",\"message\":{{\"id\":\"m\",\"model\":\"claude-x\",\"usage\":{{\"input_tokens\":4,\"output_tokens\":0}}}}}}\n\n\
         event: content_block_start\ndata: {{\"type\":\"content_block_start\",\"index\":0,\"content_block\":{{\"type\":\"text\",\"text\":\"\"}}}}\n\n\
         event: content_block_delta\ndata: {{\"type\":\"content_block_delta\",\"index\":0,\"delta\":{{\"type\":\"text_delta\",\"text\":\"partial\"}}}}\n\n\
         event: content_block_stop\ndata: {{\"type\":\"content_block_stop\",\"index\":0}}\n\n\
         event: message_delta\ndata: {{\"type\":\"message_delta\",\"delta\":{{\"stop_reason\":\"{native}\"}},\"usage\":{{\"output_tokens\":2}}}}\n\n\
         event: message_stop\ndata: {{\"type\":\"message_stop\"}}\n\n"
    );
    let mut head = http::Response::new(()).into_parts().0;
    head.headers
        .insert(CONTENT_TYPE, HeaderValue::from_static("text/event-stream"));
    let replies = AnthropicMessages
        .decode_response(http::Response::from_parts(head, sse.into()), &call)
        .expect("decode SSE");
    let framed = AnthropicMessages
        .encode_response(&request(false), &replies)
        .expect("unary");
    let body: Value = serde_json::from_slice(framed.body()).expect("reframed unary JSON");
    assert_eq!(body["stop_reason"], native);
    let translated = OPENAI
        .encode_response(&request(false), &replies)
        .expect("translated");
    let translated: Value = serde_json::from_slice(translated.body()).expect("JSON");
    assert_eq!(translated["choices"][0]["finish_reason"], "length");

    let unknown = Output::finished(Finish::native("openai-chat", "unrecognized_native_finish"));
    assert!(
        AnthropicMessages
            .encode_response(&request(false), &[unknown])
            .is_err()
    );
}

#[tokio::test]
async fn non_success_sse_is_a_whole_refusal_in_finite_and_live_reads() {
    use respan_gateway_core::futures::{TryStreamExt, stream};
    let cases = [
        (
            false,
            "openai-chat",
            "{\"error\":{\"type\":\"requests\",\"message\":\"slow down\"}}",
        ),
        (
            true,
            "anthropic-messages",
            "{\"type\":\"error\",\"error\":{\"type\":\"rate_limit_error\",\"message\":\"slow down\"}}",
        ),
    ];
    let call = http::Request::new(Bytes::new());
    for (anthropic, _name, body) in cases {
        let successful_sse = if anthropic {
            "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"m\",\"model\":\"claude-x\",\"usage\":{\"input_tokens\":1,\"output_tokens\":0}}}\n\nevent: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":1}}\n\nevent: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"
        } else {
            "data: {\"id\":\"c\",\"model\":\"gpt-x\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n"
        };
        for body in [
            body,
            successful_sse,
            "event: error\ndata: {\"error\":{\"type\":\"rate_limit_error\",\"message\":\"slow down\"}}\n\n",
        ] {
            let mut response = http::Response::new(Bytes::from(body.to_owned()));
            *response.status_mut() = StatusCode::TOO_MANY_REQUESTS;
            response
                .headers_mut()
                .insert(CONTENT_TYPE, HeaderValue::from_static("text/event-stream"));
            response
                .headers_mut()
                .insert("retry-after", HeaderValue::from_static("7"));
            let finite = if anthropic {
                AnthropicMessages.decode_response(response.clone(), &call.clone().into_parts().0)
            } else {
                OPENAI.decode_response(response.clone(), &call.clone().into_parts().0)
            }
            .expect_err("non-success must not decode as successful SSE");
            let (head, bytes) = response.into_parts();
            let live =
                http::Response::from_parts(head, Body::stream(stream::iter([Ok(bytes.clone())])));
            let live = if anthropic {
                AnthropicMessages.read_response(live, call.clone())
            } else {
                OPENAI.read_response(live, call.clone())
            }
            .try_collect::<Vec<_>>()
            .await
            .expect_err("non-success must refuse");
            for fault in [finite, live] {
                assert_eq!(fault.get::<Status>().map(|s| s.0.as_u16()), Some(429));
                assert_eq!(
                    fault.get::<respan_gateway_core::RetryAfter>().map(|r| r.0),
                    Some(std::time::Duration::from_secs(7))
                );
                assert_eq!(
                    fault
                        .get::<respan_gateway_http::ProviderError>()
                        .map(|e| &e.response)
                        .map(|a| a.body().clone()),
                    Some(bytes.clone())
                );
            }
        }
    }
}

#[test]
fn orphan_tool_arguments_fail_in_unary_and_streaming_responses() {
    // Arguments of a call that never started, in either format's words.
    let orphan = [Output::arguments(0, "{}")];
    let ask = http::Request::new(()).into_parts().0;
    for stream in [false, true] {
        let body: Bytes = json!({"stream": stream, "stream_options": {"include_usage": true}})
            .to_string()
            .into();
        let request = http::Request::from_parts(ask.clone(), body);
        assert!(
            OPENAI.encode_response(&request, &orphan).is_err(),
            "OpenAI stream={stream}"
        );
        assert!(
            AnthropicMessages
                .encode_response(&request, &orphan)
                .is_err(),
            "Anthropic stream={stream}"
        );
    }
}

#[test]
fn interleaved_calls_and_repeated_starts_remain_writable() {
    // As OpenAI streams parallel calls: interleaved, a start repeated.
    let call = |block: u32, id: &str, name: &str| {
        Output::block_start(
            block,
            Block::ToolCall {
                id: id.into(),
                name: name.into(),
            },
        )
    };
    let replies = [
        call(0, "a", "alpha"),
        Output::arguments(0, "{\"a\":"),
        call(1, "b", "beta"),
        Output::arguments(1, "{\"b\":"),
        Output::arguments(0, "1}"),
        call(1, "b", "beta"),
        Output::arguments(1, "2}"),
        Output::block_end(0),
        Output::block_end(1),
    ];
    let ask = http::Request::new(()).into_parts().0;
    for stream in [false, true] {
        let body: Bytes = json!({"stream":stream,"stream_options":{"include_usage":true}})
            .to_string()
            .into();
        let request = http::Request::from_parts(ask.clone(), body);
        let openai = OPENAI.encode_response(&request, &replies);
        assert!(openai.is_ok(), "OpenAI stream={stream}");
        let anthropic = AnthropicMessages.encode_response(&request, &replies);
        let anthropic = anthropic.expect("Anthropic");
        let text = String::from_utf8_lossy(anthropic.body()).into_owned();
        let calls: Vec<Value> = match stream {
            false => serde_json::from_str::<Value>(&text).expect("JSON")["content"]
                .as_array()
                .cloned()
                .unwrap_or_default(),
            true => {
                let mut calls = vec![json!({"input": ""}), json!({"input": ""})];
                for part in text.split("\n\n") {
                    let data = part.lines().find_map(|l| l.strip_prefix("data: "));
                    let v: Value = serde_json::from_str(data.unwrap_or("null")).unwrap_or_default();
                    if let (Some(i), Some(json)) =
                        (v["index"].as_u64(), v["delta"]["partial_json"].as_str())
                    {
                        let was = calls[i as usize]["input"].as_str().unwrap_or("").to_owned();
                        calls[i as usize]["input"] = format!("{was}{json}").into();
                    }
                }
                calls
            }
        };
        let inputs: Vec<Value> = calls
            .iter()
            .map(|c| match &c["input"] {
                Value::String(json) => serde_json::from_str(json).unwrap_or_default(),
                input => input.clone(),
            })
            .collect();
        assert_eq!(
            inputs,
            [json!({"a": 1}), json!({"b": 2})],
            "stream={stream}: {text}"
        );
    }
}

#[test]
fn the_fields_of_an_anthropic_message_are_dropped_by_another_format() {
    let body = json!({ "id": "m", "type": "message", "role": "assistant", "model": "c",
        "content": [{ "type": "text", "text": "hi" }], "stop_reason": "end_turn",
        "stop_sequence": null, "container": { "id": "k" },
        "usage": { "input_tokens": 1, "output_tokens": 1 } });
    let call = http::Request::new(()).into_parts().0;
    let answer = http::Response::new(Bytes::from(body.to_string()));
    let events = ANTHROPIC.decode_response(answer, &call).expect("decoded");
    // It cannot change the answer.
    let ask = http::Request::new(Bytes::from_static(b"{}"));
    assert!(OPENAI.encode_response(&ask, &events).is_ok());
}
