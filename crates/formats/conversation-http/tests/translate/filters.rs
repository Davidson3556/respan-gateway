//! Translate filters behavior.

use super::*;

#[tokio::test]
async fn a_filter_can_turn_content_of_another_format_into_text() {
    /// Reports that the model thought, from Anthropic's redacted thinking
    /// block, which the kind has no form for.
    struct Thought;

    impl Filter<Output> for Thought {
        fn edit(&mut self, reply: Output, out: &mut Vec<Output>) -> Result<(), Fault> {
            match &reply {
                Output::Native(respan_gateway_core::NativeValue { format, value: raw })
                    if &**format == "anthropic-messages"
                        && raw["content_block"]["type"] == "redacted_thinking" =>
                {
                    out.extend(support::text(5, "[thought] "));
                }
                _ => out.push(reply),
            }
            Ok(())
        }
    }

    #[async_trait]
    impl Plugin for Thought {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            cx.filter(Thought)?;
            Ok(None)
        }
    }

    let h = Harness::new(vec![Arc::new(Thought)]).await;
    let thought = "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":5,\"content_block\":{\"type\":\"redacted_thinking\",\"data\":\"x\"}}\n\n";
    let mut stream = ANTHROPIC_STREAM.to_vec();
    stream.insert(1, thought);
    h.anthropic.answer(sse(&stream));

    let (status, body) = h.text(openai_request("claude-x", true)).await;

    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body.contains("[thought] "), "{body}");
}

#[tokio::test]
async fn a_filter_edits_translated_text() {
    let h = Harness::new(vec![Arc::new(Shout)]).await;
    h.openai.answer(sse(OPENAI_STREAM));

    let (_, body) = h.text(anthropic_request(true)).await;

    let text: String = events(&body)
        .iter()
        .filter_map(|(_, d)| {
            d.pointer("/delta/text")
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .collect();
    assert_eq!(text, "HELLO WORLD");
}

#[tokio::test]
async fn a_filter_edits_an_anthropic_stream_in_its_own_shape() {
    let h = Harness::new(vec![Arc::new(Shout)]).await;
    h.anthropic.answer(sse(ANTHROPIC_STREAM));
    let body = json!({
        "model": "claude-x",
        "max_tokens": 10,
        "stream": true,
        "messages": [{ "role": "user", "content": "hi" }],
    });

    let (_, out) = h.text(call(MESSAGES, body.to_string())).await;

    let expected = ANTHROPIC_STREAM
        .concat()
        .replace("\"text\":\"Hello\"", "\"text\":\"HELLO\"")
        .replace("\"text\":\" world\"", "\"text\":\" WORLD\"");
    assert_eq!(semantic(&out), semantic(&expected));
}

#[tokio::test]
async fn a_filter_edits_events_that_share_a_chunk_while_translating() {
    struct Truncated;

    #[async_trait]
    impl Plugin for Truncated {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            cx.filter(|r: &mut Output| {
                match r {
                    Output::Start(start) => start.model = "alias".into(),
                    Output::Finish(finish) => *finish = Finish::Length,
                    _ => {}
                }
                Ok(())
            })?;
            Ok(None)
        }
    }

    let h = Harness::new(vec![Arc::new(Truncated)]).await;
    let chunk = "data: {\"id\":\"c1\",\"model\":\"gpt-real\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi\"},\"finish_reason\":\"stop\"}]}\n\n";
    h.openai.answer(sse(&[chunk, "data: [DONE]\n\n"]));

    let (_, body) = h.text(anthropic_request(true)).await;

    let events = events(&body);
    assert_eq!(events[0].1["message"]["model"], "alias");
    let stop = events
        .iter()
        .find_map(|(_, d)| d.pointer("/delta/stop_reason"));
    assert_eq!(stop, Some(&json!("max_tokens")));
}

#[tokio::test]
async fn a_filter_restores_text_across_an_anthropic_stream_and_releases_the_rest() {
    let h = Harness::new(vec![Arc::new(Unmask)]).await;
    let stream = masked_anthropic_stream();
    let chunks: Vec<&str> = stream.iter().map(String::as_str).collect();
    h.anthropic.answer(sse(&chunks));
    let body = json!({
        "model": "claude-x", "max_tokens": 10, "stream": true,
        "messages": [{ "role": "user", "content": "hi" }],
    });

    let (status, out) = h.text(call(MESSAGES, body.to_string())).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(anthropic_texts(&out), "Hi Alice, bye <");
    // What was kept back goes into Anthropic's own block, before it closes.
    let names: Vec<String> = events(&out)
        .into_iter()
        .filter_map(|(name, _)| name)
        .collect();
    let tail = &names[names.len() - 4..];
    assert_eq!(
        tail,
        [
            "content_block_delta",
            "content_block_stop",
            "message_delta",
            "message_stop"
        ]
    );
    assert!(!out.contains("\"index\":1"), "{out}");
    assert_eq!(says(h.log.only()), Ok(()));
}

#[tokio::test]
async fn a_filter_restores_text_across_a_translated_stream() {
    let h = Harness::new(vec![Arc::new(Unmask)]).await;
    let stream = masked_anthropic_stream();
    let chunks: Vec<&str> = stream.iter().map(String::as_str).collect();
    h.anthropic.answer(sse(&chunks));

    let (_, body) = h.text(openai_request("claude-x", true)).await;

    let text: String = events(&body)
        .iter()
        .filter_map(|(_, c)| {
            c.pointer("/choices/0/delta/content")
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .collect();
    assert_eq!(text, "Hi Alice, bye <");
    assert!(body.ends_with("data: [DONE]\n\n"));
}

#[tokio::test]
async fn a_filter_releases_what_it_held_into_a_whole_message() {
    let h = Harness::new(vec![Arc::new(Unmask)]).await;
    let reply = json!({
        "id": "msg_1", "type": "message", "role": "assistant", "model": "claude-x",
        "content": [{ "type": "text", "text": "Hi <P1>, bye <", "citations": null }],
        "stop_reason": "end_turn", "usage": { "input_tokens": 5, "output_tokens": 2 },
    });
    h.anthropic
        .answer(json_response(StatusCode::OK, &reply.to_string()));
    let body = json!({
        "model": "claude-x", "max_tokens": 10,
        "messages": [{ "role": "user", "content": "hi" }],
    });

    let (status, out) = h.text(call(MESSAGES, body.to_string())).await;

    assert_eq!(status, StatusCode::OK, "{out}");
    let out: Value = serde_json::from_str(&out).unwrap_or_default();
    assert_eq!(out["content"][0]["text"], "Hi Alice, bye <");
    assert_eq!(out["content"][0]["citations"], Value::Null);
    assert_eq!(out["id"], "msg_1");
}

#[tokio::test]
async fn a_block_a_filter_adds_at_the_end_opens_after_anthropics_own() {
    let h = Harness::new(vec![Arc::new(NewBlock)]).await;
    h.anthropic.answer(sse(ANTHROPIC_STREAM));
    let body = json!({
        "model": "claude-x", "max_tokens": 10, "stream": true,
        "messages": [{ "role": "user", "content": "hi" }],
    });

    let (status, out) = h.text(call(MESSAGES, body.to_string())).await;

    assert_eq!(status, StatusCode::OK, "{out}");
    let starts: Vec<Value> = events(&out)
        .into_iter()
        .filter(|(n, _)| n.as_deref() == Some("content_block_start"))
        .map(|(_, v)| v["index"].clone())
        .collect();
    assert_eq!(starts, [json!(0), json!(1)]);
    assert_eq!(anthropic_texts(&out), "Hello world [footer]");
    let delta = events(&out)
        .into_iter()
        .find(|(n, _)| n.as_deref() == Some("message_delta"));
    let delta = delta.map(|(_, v)| v).unwrap_or_default();
    assert_eq!(
        delta.pointer("/delta/stop_reason"),
        Some(&json!("end_turn"))
    );
}

#[tokio::test]
async fn a_block_a_filter_adds_to_a_whole_message_is_appended() {
    let h = Harness::new(vec![Arc::new(NewBlock)]).await;
    let reply = json!({
        "id": "msg_1", "type": "message", "role": "assistant", "model": "claude-x",
        "content": [{ "type": "text", "text": "Hello" }],
        "stop_reason": "end_turn", "usage": { "input_tokens": 5, "output_tokens": 2 },
    });
    h.anthropic
        .answer(json_response(StatusCode::OK, &reply.to_string()));
    let body = json!({
        "model": "claude-x", "max_tokens": 10,
        "messages": [{ "role": "user", "content": "hi" }],
    });

    let (status, out) = h.text(call(MESSAGES, body.to_string())).await;

    assert_eq!(status, StatusCode::OK, "{out}");
    let out: Value = serde_json::from_str(&out).unwrap_or_default();
    assert_eq!(out["content"][1]["text"], " [footer]");
    assert_eq!(out["stop_reason"], "end_turn");
}

#[test]
fn a_filter_is_tested_on_its_own() {
    let mut footer = Footer(0, None);
    let mut out = Vec::new();
    let _ = footer.edit(Output::finished(Finish::Stop), &mut out);
    // The finish is kept back to the end.
    assert!(out.is_empty());
    let _ = footer.end(&Ok(()), &mut out);
    let mut footed = text(0, " [footer]");
    footed.push(Output::finished(Finish::Stop));
    assert_eq!(out, footed);
}

#[tokio::test]
async fn a_finish_held_to_the_end_still_ends_anthropics_stream() {
    struct Foot;

    #[async_trait]
    impl Plugin for Foot {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            cx.filter(Footer(0, None))?;
            Ok(None)
        }
    }

    let h = Harness::new(vec![Arc::new(Foot)]).await;
    h.anthropic.answer(sse(ANTHROPIC_STREAM));
    let body = json!({
        "model": "claude-x", "max_tokens": 10, "stream": true,
        "messages": [{ "role": "user", "content": "hi" }],
    });

    let (status, out) = h.text(call(MESSAGES, body.to_string())).await;

    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(anthropic_texts(&out), "Hello world [footer]");
    let delta = events(&out)
        .into_iter()
        .find(|(n, _)| n.as_deref() == Some("message_delta"));
    let delta = delta.map(|(_, v)| v).unwrap_or_default();
    assert_eq!(
        delta.pointer("/delta/stop_reason"),
        Some(&json!("end_turn"))
    );
    assert_eq!(delta.pointer("/usage/output_tokens"), Some(&json!(2)));
    assert!(out.ends_with("event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"));
}

#[tokio::test]
async fn text_put_before_anthropics_first_block_goes_into_it() {
    let h = filtered(Note(false)).await;
    h.anthropic.answer(sse(ANTHROPIC_STREAM));
    let body = json!({
        "model": "claude-x", "max_tokens": 10, "stream": true,
        "messages": [{ "role": "user", "content": "hi" }],
    });

    let (status, out) = h.text(call(MESSAGES, body.to_string())).await;

    assert_eq!(status, StatusCode::OK, "{out}");
    let names: Vec<String> = events(&out).into_iter().filter_map(|(n, _)| n).collect();
    let count = |name: &str| names.iter().filter(|n| *n == name).count();
    assert_eq!(
        (count("message_start"), count("content_block_start")),
        (1, 1),
        "{out}"
    );
    assert_eq!(anthropic_texts(&out), "Note: Hello world");
}

#[tokio::test]
async fn a_whole_message_a_filter_took_all_text_of_is_still_sent() {
    let h = filtered(Silence::default()).await;
    let reply = json!({
        "id": "msg_1", "type": "message", "role": "assistant", "model": "claude-x",
        "content": [{ "type": "text", "text": "secret" }],
        "stop_reason": "end_turn", "usage": { "input_tokens": 5, "output_tokens": 2 },
    });
    h.anthropic
        .answer(json_response(StatusCode::OK, &reply.to_string()));
    let body = json!({
        "model": "claude-x", "max_tokens": 10,
        "messages": [{ "role": "user", "content": "hi" }],
    });

    let (status, out) = h.text(call(MESSAGES, body.to_string())).await;

    assert_eq!(status, StatusCode::OK, "{out}");
    let out: Value = serde_json::from_str(&out).unwrap_or_default();
    assert_eq!(out["content"], json!([]));
    assert_eq!(out["stop_reason"], "end_turn");
}

#[tokio::test]
async fn made_text_stays_out_of_a_thinking_block() {
    /// Puts a note, a block of its own, as soon as the response starts.
    #[derive(Clone)]
    struct OwnNote(bool);

    impl Filter<Output> for OwnNote {
        fn edit(&mut self, event: Output, out: &mut Vec<Output>) -> Result<(), Fault> {
            let first = matches!(event, Output::Start(_)) && !std::mem::replace(&mut self.0, true);
            out.push(event);
            if first {
                out.extend(text(9, "Note: "));
            }
            Ok(())
        }
    }

    let h = filtered(OwnNote(false)).await;
    let event = |name: &str, data: Value| format!("event: {name}\ndata: {data}\n\n");
    let stream = [
        ANTHROPIC_STREAM[0].to_owned(),
        event(
            "content_block_start",
            json!({"type": "content_block_start", "index": 0,
            "content_block": {"type": "thinking", "thinking": ""}}),
        ),
        event(
            "content_block_delta",
            json!({"type": "content_block_delta", "index": 0,
            "delta": {"type": "thinking_delta", "thinking": "hmm"}}),
        ),
        event(
            "content_block_stop",
            json!({"type": "content_block_stop", "index": 0}),
        ),
        event(
            "content_block_start",
            json!({"type": "content_block_start", "index": 1,
            "content_block": {"type": "text", "text": ""}}),
        ),
        event(
            "content_block_delta",
            json!({"type": "content_block_delta", "index": 1,
            "delta": {"type": "text_delta", "text": "Hello"}}),
        ),
        event(
            "content_block_stop",
            json!({"type": "content_block_stop", "index": 1}),
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

    let (status, out) = h.text(call(MESSAGES, body.to_string())).await;

    assert_eq!(status, StatusCode::OK, "{out}");
    let all = events(&out);
    let thinking = all
        .iter()
        .find(|(_, v)| v.pointer("/content_block/type") == Some(&json!("thinking")))
        .map(|(_, v)| v["index"].clone());
    assert!(thinking.is_some(), "{out}");
    let in_thinking = all.iter().any(|(_, v)| {
        v["index"] == thinking.clone().unwrap_or_default()
            && v.pointer("/delta/type") == Some(&json!("text_delta"))
    });
    assert!(!in_thinking, "{out}");
    assert!(anthropic_texts(&out).contains("Note: "), "{out}");
}

#[tokio::test]
async fn a_filter_holding_everything_of_anthropics_stream_releases_it_whole() {
    let h = filtered(HoldAll(Vec::new())).await;
    h.anthropic.answer(sse(ANTHROPIC_STREAM));
    let body = json!({
        "model": "claude-x", "max_tokens": 10, "stream": true,
        "messages": [{ "role": "user", "content": "hi" }],
    });

    let (status, out) = h.text(call(MESSAGES, body.to_string())).await;

    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(anthropic_texts(&out), "Hello world");
    let names: Vec<String> = events(&out).into_iter().filter_map(|(n, _)| n).collect();
    assert_eq!(
        names.iter().filter(|n| *n == "message_start").count(),
        1,
        "{out}"
    );
    assert_eq!(names.last().map(String::as_str), Some("message_stop"));
    // A buffering guard's release keeps the provider's framing in order.
    assert_well_formed(&names, &out);
}

#[tokio::test]
async fn text_before_the_start_of_an_anthropic_message_is_written() {
    let h = filtered(Disclaimer).await;
    let reply = json!({
        "id": "msg_1", "type": "message", "role": "assistant", "model": "claude-x",
        "content": [{ "type": "text", "text": "Hello" }],
        "stop_reason": "end_turn", "usage": { "input_tokens": 5, "output_tokens": 2 },
    });
    h.anthropic
        .answer(json_response(StatusCode::OK, &reply.to_string()));
    let body = json!({
        "model": "claude-x", "max_tokens": 10,
        "messages": [{ "role": "user", "content": "hi" }],
    });

    let (status, out) = h.text(call(MESSAGES, body.to_string())).await;

    assert_eq!(status, StatusCode::OK, "{out}");
    let out: Value = serde_json::from_str(&out).unwrap_or_default();
    // The note is a block of its own, before the answer's.
    let texts: Vec<&str> = out["content"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|b| b["text"].as_str())
        .collect();
    assert_eq!(texts, ["NOTE: ", "Hello"]);
}

#[tokio::test]
async fn a_block_that_opens_with_text_a_filter_holds_opens_once() {
    let h = filtered(HoldAll(Vec::new())).await;
    let mut stream: Vec<String> = ANTHROPIC_STREAM.iter().map(|s| s.to_string()).collect();
    stream[1] = "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"Well, \"}}\n\n".to_owned();
    let chunks: Vec<&str> = stream.iter().map(String::as_str).collect();
    h.anthropic.answer(sse(&chunks));
    let body = json!({
        "model": "claude-x", "max_tokens": 10, "stream": true,
        "messages": [{ "role": "user", "content": "hi" }],
    });

    let (status, out) = h.text(call(MESSAGES, body.to_string())).await;

    assert_eq!(status, StatusCode::OK, "{out}");
    let names: Vec<String> = events(&out).into_iter().filter_map(|(n, _)| n).collect();
    let count = |name: &str| names.iter().filter(|n| *n == name).count();
    assert_eq!(
        (count("content_block_start"), count("content_block_stop")),
        (1, 1),
        "{out}"
    );
    // Released as it came, its opening text is the block's first text, and
    // the stream keeps its order.
    assert_eq!(anthropic_texts(&out), "Well, Hello world");
    assert_well_formed(&names, &out);
    let start = events(&out)
        .into_iter()
        .find(|(n, _)| n.as_deref() == Some("message_start"));
    assert_eq!(
        start.map(|(_, v)| v["message"]["id"].clone()),
        Some(json!("msg_1"))
    );
}

#[tokio::test]
async fn text_released_at_the_end_of_a_translated_stream_goes_before_its_finish() {
    let h = filtered(HoldText::default()).await;
    h.anthropic.answer(sse(ANTHROPIC_STREAM));

    let (status, body) = h.text(openai_request("claude-x", true)).await;

    assert_eq!(status, StatusCode::OK, "{body}");
    let chunks: Vec<Value> = events(&body).into_iter().map(|(_, v)| v).collect();
    let text_at = chunks.iter().rposition(|c| {
        c.pointer("/choices/0/delta/content")
            .is_some_and(|t| t != "")
    });
    let finish_at = chunks.iter().position(|c| {
        !c.pointer("/choices/0/finish_reason")
            .is_none_or(Value::is_null)
    });
    assert!(text_at < finish_at, "{body}");
    assert!(body.ends_with("data: [DONE]\n\n"));
}

#[tokio::test]
async fn a_whole_anthropic_message_a_filter_took_the_start_of_is_sent() {
    let h = filtered(Nameless).await;
    let reply = json!({
        "id": "msg_1", "type": "message", "role": "assistant", "model": "claude-x",
        "content": [{ "type": "text", "text": "Hello" }],
        "stop_reason": "end_turn", "usage": { "input_tokens": 5, "output_tokens": 2 },
    });
    h.anthropic
        .answer(json_response(StatusCode::OK, &reply.to_string()));
    let body = json!({
        "model": "claude-x", "max_tokens": 10,
        "messages": [{ "role": "user", "content": "hi" }],
    });

    let (status, out) = h.text(call(MESSAGES, body.to_string())).await;

    assert_eq!(status, StatusCode::OK, "{out}");
    // Without its start the message is sent, unnamed.
    let out: Value = serde_json::from_str(&out).unwrap_or_default();
    assert_eq!(out["content"][0]["text"], "Hello");
    assert_eq!(out["id"], "");
}

#[tokio::test]
async fn azures_filter_annotation_after_the_finish_ends_a_translated_stream_cleanly() {
    let h = Harness::new(vec![Arc::new(To("azure"))]).await;
    let mut stream: Vec<String> = OPENAI_STREAM.iter().map(|s| s.to_string()).collect();
    let annotation = json!({"id": "", "choices": [{"index": 0, "delta": {},
        "content_filter_offsets": {"check_offset": 0, "start_offset": 0, "end_offset": 11},
        "content_filter_results": {"hate": {"filtered": false, "severity": "safe"}}}]});
    stream.insert(4, format!("data: {annotation}\n\n"));
    let chunks: Vec<&str> = stream.iter().map(String::as_str).collect();
    h.openai.answer(sse(&chunks));

    let (status, out) = h.text(anthropic_request(true)).await;

    assert_eq!(status, StatusCode::OK, "{out}");
    assert!(!out.contains("event: error"), "{out}");
    assert!(
        out.ends_with("event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"),
        "{out}"
    );
}

#[tokio::test]
async fn text_held_across_two_anthropic_blocks_stays_in_its_block() {
    let h = filtered(LagByOne::default()).await;
    let event = |name: &str, data: Value| format!("event: {name}\ndata: {data}\n\n");
    let block = |i: u32, text: &str| {
        [
            event(
                "content_block_start",
                json!({"type": "content_block_start", "index": i,
                "content_block": {"type": "text", "text": ""}}),
            ),
            event(
                "content_block_delta",
                json!({"type": "content_block_delta", "index": i,
                "delta": {"type": "text_delta", "text": text}}),
            ),
            event(
                "content_block_stop",
                json!({"type": "content_block_stop", "index": i}),
            ),
        ]
    };
    let mut stream = vec![ANTHROPIC_STREAM[0].to_owned()];
    stream.extend(block(0, "a"));
    stream.extend(block(1, "b"));
    stream.extend([
        ANTHROPIC_STREAM[5].to_owned(),
        ANTHROPIC_STREAM[6].to_owned(),
    ]);
    let chunks: Vec<&str> = stream.iter().map(String::as_str).collect();
    h.anthropic.answer(sse(&chunks));
    let body = json!({
        "model": "claude-x", "max_tokens": 10, "stream": true,
        "messages": [{ "role": "user", "content": "hi" }],
    });

    let (status, out) = h.text(call(MESSAGES, body.to_string())).await;

    assert_eq!(status, StatusCode::OK, "{out}");
    assert!(!out.contains("event: error"), "{out}");
    let deltas: Vec<(Value, Value)> = events(&out)
        .into_iter()
        .filter(|(n, _)| n.as_deref() == Some("content_block_delta"))
        .map(|(_, v)| (v["index"].clone(), v["delta"]["text"].clone()))
        .collect();
    assert_eq!(deltas, [(json!(0), json!("a")), (json!(1), json!("b"))]);
}

#[tokio::test]
async fn text_held_from_two_anthropic_blocks_keeps_its_order() {
    let out = anthropic_through(HoldAll(Vec::new()), &two_text_blocks("first ", "second")).await;

    assert_eq!(anthropic_texts(&out), "first second", "{out}");
}
