//! What a format has that the kind cannot hold survives a trip through the
//! same format, and another format drops it if that is harmless, else
//! refuses it.

use crate::common::NativeFieldsExt;
use respan_gateway_http::bytes::Bytes;
use respan_gateway_http::http::{self, HeaderValue, header::CONTENT_TYPE};
use serde_json::{Value, json};

use crate::common::{Wire, read_request, write_request};
use crate::conversation::{Content, Finish, Output, Role};
use crate::{AnthropicMessages, OpenAiChat};

/// `body` as a request of format `R`, written as a request of format `W`.
fn sent<R: Wire, W: Wire>(body: &Value) -> Result<Value, respan_gateway_sdk::Fault> {
    let chat = read_request::<R>(Bytes::from(body.to_string()))?;
    Ok(write_request::<W>(&chat)?.into())
}

fn same<W: Wire>(body: &Value) -> Value {
    sent::<W, W>(body).expect("written again")
}

fn refused<R: Wire, W: Wire>(body: &Value) -> bool {
    sent::<R, W>(body).is_err()
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
    assert_eq!(same::<AnthropicMessages>(&body), body);
    let chat = read_request::<AnthropicMessages>(Bytes::from(body.to_string())).expect("chat");
    assert!(chat.messages[0].is_plain());
    assert_eq!(chat.messages[0].text(), "the repoh\u{69}");
    let to_openai = sent::<AnthropicMessages, OpenAiChat>(&body).expect("translated");
    assert_eq!(
        to_openai["messages"][0]["content"],
        json!([{ "type": "text", "text": "the repo" }, { "type": "text", "text": "hi" }])
    );
    // Another field of a text part is not harmless.
    let cited = json!({ "type": "text", "text": "x", "citations": [{ "type": "c" }] });
    let body = anthropic(json!([{ "role": "user", "content": [cited] }]));
    assert_eq!(same::<AnthropicMessages>(&body), body);
    assert!(refused::<AnthropicMessages, OpenAiChat>(&body));
    // The same of OpenAI's own text part fields.
    let part = json!({ "type": "text", "text": "x", "vendor": 1 });
    let body = openai(json!([{ "role": "user", "content": [part] }]));
    assert_eq!(same::<OpenAiChat>(&body), body);
    assert!(refused::<OpenAiChat, AnthropicMessages>(&body));
}

#[test]
fn tool_part_fields_survive_the_same_format_and_are_refused_by_another() {
    let marker = json!({ "type": "ephemeral" });
    let call =
        json!({ "type": "tool_use", "id": "t", "name": "f", "input": {}, "cache_control": marker });
    let result = json!({ "type": "tool_result", "tool_use_id": "t", "content": "x", "cache_control": marker });
    let body = anthropic(json!([
        { "role": "user", "content": "go" },
        { "role": "assistant", "content": [call] },
        { "role": "user", "content": [result] },
    ]));
    assert_eq!(same::<AnthropicMessages>(&body), body);
    let chat = read_request::<AnthropicMessages>(Bytes::from(body.to_string())).expect("chat");
    assert!(
        matches!(&chat.messages[1].content[0], Content::ToolCall { native, .. } if !native.is_empty())
    );
    assert!(
        matches!(&chat.messages[2].content[0], Content::ToolResult { native, .. } if !native.is_empty())
    );
    assert!(refused::<AnthropicMessages, OpenAiChat>(&body));
}

#[test]
fn a_role_and_a_finish_of_a_format_are_written_by_that_format_alone() {
    let body =
        openai(json!([{ "role": "critic", "content": "hm" }, { "role": "user", "content": "hi" }]));
    assert_eq!(same::<OpenAiChat>(&body), body);
    let chat = read_request::<OpenAiChat>(Bytes::from(body.to_string())).expect("chat");
    assert_eq!(chat.messages[0].role, Role::other("openai-chat", "critic"));
    assert!(refused::<OpenAiChat, AnthropicMessages>(&body));

    let ask = http::Request::new(Bytes::from(json!({ "stream": true }).to_string()));
    let ask_whole = http::Request::new(Bytes::from_static(b"{}"));
    let call = http::Request::new(()).into_parts().0;
    let mut head = http::Response::new(()).into_parts().0;
    head.headers
        .insert(CONTENT_TYPE, HeaderValue::from_static("text/event-stream"));
    let stream = "data: {\"id\":\"c\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi\"},\"finish_reason\":\"insufficient_system_resource\"}]}\n\ndata: [DONE]\n\n";
    let replies = OpenAiChat
        .decode_response(http::Response::from_parts(head, Bytes::from(stream)), &call)
        .expect("decoded");
    assert!(replies.contains(&Output::finished(Finish::other(
        "openai-chat",
        "insufficient_system_resource"
    ))));
    let streamed = OpenAiChat
        .encode_response(&ask, &replies)
        .expect("a stream");
    let streamed = String::from_utf8_lossy(streamed.body()).into_owned();
    assert!(
        streamed.contains("\"finish_reason\":\"insufficient_system_resource\""),
        "{streamed}"
    );
    let whole = OpenAiChat
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
    assert_eq!(same::<OpenAiChat>(&body), body);
    let chat = read_request::<OpenAiChat>(Bytes::from(body.to_string())).expect("chat");
    assert_eq!(chat.messages[0].role, Role::Developer);
    assert!(chat.messages[0].is_plain());
    let to_anthropic = sent::<OpenAiChat, AnthropicMessages>(&body).expect("translated");
    assert_eq!(to_anthropic["system"], "be brief");
    // After the conversation starts it has no place in Anthropic.
    let late = openai(json!([
        { "role": "user", "content": "hi" },
        { "role": "developer", "content": "be brief" },
    ]));
    assert!(refused::<OpenAiChat, AnthropicMessages>(&late));
    // Edited to another role, it is written as that role.
    let mut edited = chat;
    edited.messages[0].role = Role::User;
    let again = write_request::<OpenAiChat>(&edited)
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
        assert_eq!(same::<OpenAiChat>(&body), canonical);
        let to_anthropic = sent::<OpenAiChat, AnthropicMessages>(&body).expect("translated");
        assert_eq!(to_anthropic["max_tokens"], 9);
        assert!(to_anthropic.get("max_completion_tokens").is_none());
    }
    let anthropic = anthropic(json!([{ "role": "user", "content": "hi" }]));
    let to_openai = sent::<AnthropicMessages, OpenAiChat>(&anthropic).expect("translated");
    assert_eq!(to_openai["max_completion_tokens"], 5);
}

#[test]
fn anthropic_metadata_beside_the_user_id_survives_its_format() {
    let mut body = anthropic(json!([{ "role": "user", "content": "hi" }]));
    body["metadata"] = json!({ "user_id": "u", "trace": "t" });
    assert_eq!(same::<AnthropicMessages>(&body), body);
    let chat = read_request::<AnthropicMessages>(Bytes::from(body.to_string())).expect("chat");
    assert_eq!(chat.user.as_deref(), Some("u"));
    assert!(refused::<AnthropicMessages, OpenAiChat>(&body));
    // A user id alone translates, and a redacted one is written.
    let mut redacted = chat;
    redacted.user = Some("[x]".into());
    let again = write_request::<AnthropicMessages>(&redacted)
        .map(Value::from)
        .unwrap_or_default();
    assert_eq!(again["metadata"], json!({ "user_id": "[x]", "trace": "t" }));
}

fn request_of(stream: bool) -> http::Request<Bytes> {
    let body = json!({ "stream": stream, "stream_options": { "include_usage": true } });
    http::Request::new(Bytes::from(body.to_string()))
}

fn decoded<W: Wire>(
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
    let decode = |r| OpenAiChat.decode_response(r, &call).expect("decoded");
    for (is_stream, text) in [(false, whole.to_string()), (true, stream)] {
        let replies = decoded::<OpenAiChat>(decode, is_stream, &text);
        let out = OpenAiChat
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
        let replies = decoded::<AnthropicMessages>(decode, is_stream, &text);
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
        let to_openai = OpenAiChat.encode_response(&request_of(is_stream), &replies);
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
            let replies = decoded::<AnthropicMessages>(decode, is_stream, &text);
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
            // Another format drops them, and does not refuse.
            let to_openai = OpenAiChat.encode_response(&request_of(is_stream), &replies);
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
    let decode = |r| OpenAiChat.decode_response(r, &call).expect("decoded");
    for (is_stream, text) in [(false, whole.to_string()), (true, stream)] {
        let replies = decoded::<OpenAiChat>(decode, is_stream, &text);
        // The choice stays readable events: no opaque choice among them.
        assert!(
            replies
                .iter()
                .any(|r| matches!(r, Output::BlockStart { .. })),
            "stream {is_stream}"
        );
        let out = OpenAiChat
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
    let chat =
        read_request::<OpenAiChat>(Bytes::from(body.to_string())).expect("supported request");
    assert!(matches!(&chat.messages[0].content[0], Content::Image(media)
        if media.mime_type == "image/png" && media.source == MediaSource::Bytes(Bytes::from_static(b"hi"))));
    let translated = sent::<OpenAiChat, AnthropicMessages>(&body).expect("supported request");
    assert_eq!(
        translated["messages"][0]["content"][0],
        json!({"type":"image","source":{"type":"base64","media_type":"image/png","data":"aGk="}})
    );
    assert_eq!(
        sent::<AnthropicMessages, OpenAiChat>(&translated).expect("supported request")["messages"],
        body["messages"]
    );

    let mut structured = body.clone();
    structured["response_format"] = json!({"type":"json_schema","json_schema":{
        "name":"answer","strict":true,"schema":{"type":"object","properties":{"ok":{"type":"boolean"}},"required":["ok"],"additionalProperties":false}
    }});
    let typed =
        read_request::<OpenAiChat>(Bytes::from(structured.to_string())).expect("supported request");
    assert!(matches!(
        typed.options.response_format,
        Some(ResponseFormat::JsonSchema { .. })
    ));
    assert!(typed.native.is_none());
    assert_eq!(same::<OpenAiChat>(&structured), structured);
    let translated = sent::<OpenAiChat, AnthropicMessages>(&structured).expect("supported request");
    assert_eq!(
        translated["output_config"]["format"]["schema"],
        structured["response_format"]["json_schema"]["schema"]
    );
    let reverse = sent::<AnthropicMessages, OpenAiChat>(&translated).expect("supported request");
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
        assert_eq!(same::<OpenAiChat>(&body), body);
        assert!(refused::<OpenAiChat, AnthropicMessages>(&body));
    }
    let document = anthropic(json!([{"role":"user","content":[{
        "type":"document","title":"doc.pdf","source":{"type":"base64","media_type":"application/pdf","data":"aGk="}
    }]}]));
    let translated = sent::<AnthropicMessages, OpenAiChat>(&document).expect("supported request");
    assert_eq!(
        translated["messages"][0]["content"][0]["file"]["filename"],
        "doc.pdf"
    );
    assert_eq!(
        sent::<OpenAiChat, AnthropicMessages>(&translated).expect("supported request")["messages"],
        document["messages"]
    );
}

#[test]
fn reasoning_is_typed_but_signatures_remain_format_specific() {
    let body =
        openai(json!([{"role":"assistant","reasoning_content":"consider","content":"answer"}]));
    let chat =
        read_request::<OpenAiChat>(Bytes::from(body.to_string())).expect("supported request");
    assert!(
        chat.messages[0]
            .content
            .iter()
            .any(|p| matches!(p, Content::Reasoning {text,..} if text=="consider"))
    );
    assert_eq!(same::<OpenAiChat>(&body), body);
    let signed = anthropic(
        json!([{"role":"assistant","content":[{"type":"thinking","thinking":"consider","signature":"sig"}]}]),
    );
    assert_eq!(same::<AnthropicMessages>(&signed), signed);
    assert!(refused::<AnthropicMessages, OpenAiChat>(&signed));
}

#[test]
fn unknown_structured_output_options_stay_native() {
    let mut body = openai(json!([{"role":"user","content":"hi"}]));
    body["response_format"] =
        json!({"type":"json_schema","json_schema":{"name":"answer","schema":{},"future_option":1}});
    assert_eq!(same::<OpenAiChat>(&body), body);
    assert!(refused::<OpenAiChat, AnthropicMessages>(&body));
    let mut body = anthropic(json!([{"role":"user","content":"hi"}]));
    body["output_config"] =
        json!({"format":{"type":"json_schema","schema":{"type":"object"}},"effort":"high"});
    assert_eq!(same::<AnthropicMessages>(&body), body);
    assert!(refused::<AnthropicMessages, OpenAiChat>(&body));
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
        assert_eq!(same::<OpenAiChat>(&body), body);
        assert!(refused::<OpenAiChat, AnthropicMessages>(&body));
    }
}
