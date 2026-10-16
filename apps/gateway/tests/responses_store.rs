//! Conversation state the gateway keeps (`plugins.responses`): a Responses
//! client's `previous_response_id`, to a provider of Chat Completions, which
//! keeps nothing, is sent as the conversation it names; and a Chat client's
//! reasoning between tool calls reaches OpenAI's Responses again.
//!
//! Run: `cargo test -p respan-gateway --test responses_store`.

#![allow(clippy::unwrap_used)]

mod support;

use std::sync::{Arc, Mutex};

use axum::Json;
use axum::routing::post;
use serde_json::{Value, json};
use support::{Gateway, client, json as parse};

/// A Chat Completions provider that answers `reply-<n>` and records what it
/// was sent.
async fn fake() -> (String, Arc<Mutex<Vec<Value>>>) {
    let sent: Arc<Mutex<Vec<Value>>> = Arc::default();
    let seen = sent.clone();
    let app = axum::Router::new().route(
        "/v1/chat/completions",
        post(move |Json(body): Json<Value>| {
            let seen = seen.clone();
            async move {
                let n = {
                    let mut seen = seen.lock().unwrap();
                    seen.push(body);
                    seen.len()
                };
                Json(json!({ "id": format!("chatcmpl-{n}"), "object": "chat.completion", "model": "m",
                    "choices": [{ "index": 0, "finish_reason": "stop",
                        "message": { "role": "assistant", "content": format!("reply-{n}") } }],
                    "usage": { "prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2 } }))
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (base, sent)
}

async fn gateway(base: &str) -> Gateway {
    Gateway::start(&format!(
        "client_keys = [{{ name = \"a\", key = \"ka\" }}, {{ name = \"b\", key = \"kb\" }}]\n\
         formats = [\"openai-chat\", \"openai-responses\"]\n\
         providers = {{ p = {{ formats = \"openai-chat\", base_url = \"{base}/v1\" }} }}\n\
         plugins = {{ responses = {{ keep = \"1h\" }} }}\n"
    ))
    .await
}

/// A Responses request with `key`: its status and body.
async fn respond(gw: &Gateway, key: &str, body: Value) -> (u16, Value) {
    let resp = client()
        .post(gw.url("/v1/responses"))
        .bearer_auth(key)
        .header("content-type", "application/json")
        .body(body.to_string())
        .send()
        .await
        .unwrap();
    (resp.status().as_u16(), parse(&resp.text().await.unwrap()))
}

/// The roles and texts of the messages a Chat request carried.
fn said(request: &Value) -> Vec<(String, String)> {
    let messages = request["messages"].as_array().unwrap();
    let text = |m: &Value| match &m["content"] {
        Value::String(s) => s.clone(),
        parts => parts
            .as_array()
            .into_iter()
            .flatten()
            .map(|p| p["text"].as_str().unwrap_or(""))
            .collect(),
    };
    messages
        .iter()
        .map(|m| (m["role"].as_str().unwrap().to_owned(), text(m)))
        .collect()
}

fn pairs(list: &[(&str, &str)]) -> Vec<(String, String)> {
    list.iter()
        .map(|(r, t)| ((*r).to_owned(), (*t).to_owned()))
        .collect()
}

#[tokio::test]
async fn a_follow_up_is_sent_with_the_conversation_it_names() {
    let (base, sent) = fake().await;
    let gw = gateway(&base).await;
    let (status, first) = respond(
        &gw,
        "ka",
        json!({ "model": "m", "instructions": "Be brief.", "input": [
            { "role": "system", "content": "Speak French." },
            { "role": "user", "content": "one" },
        ] }),
    )
    .await;
    assert_eq!(status, 200, "{first}");
    // The gateway's own id, not the provider's.
    assert!(
        first["id"].as_str().unwrap().starts_with("resp_"),
        "{first}"
    );
    let (status, second) = respond(
        &gw,
        "ka",
        json!({ "model": "m", "instructions": "Be kind.", "input": "two", "previous_response_id": first["id"] }),
    )
    .await;
    assert_eq!(status, 200, "{second}");
    let (status, _) = respond(
        &gw,
        "ka",
        json!({ "model": "m", "input": "three", "previous_response_id": second["id"] }),
    )
    .await;
    assert_eq!(status, 200);

    let sent = sent.lock().unwrap().clone();
    // A response's instructions are its own; the turns before, a system item
    // among them, are carried on.
    assert_eq!(
        said(&sent[1]),
        pairs(&[
            ("system", "Be kind."),
            ("system", "Speak French."),
            ("user", "one"),
            ("assistant", "reply-1"),
            ("user", "two"),
        ])
    );
    assert_eq!(
        said(&sent[2]),
        pairs(&[
            ("system", "Speak French."),
            ("user", "one"),
            ("assistant", "reply-1"),
            ("user", "two"),
            ("assistant", "reply-2"),
            ("user", "three"),
        ])
    );
    assert!(sent.iter().all(|r| r.get("previous_response_id").is_none()));
}

/// Regression: instructions as a list of parts were taken for the
/// conversation's own system message, and replayed into the next turn.
#[tokio::test]
async fn instructions_in_any_form_are_the_requests_own() {
    let (base, sent) = fake().await;
    let gw = gateway(&base).await;
    let parts = json!([{ "type": "input_text", "text": "Be brief." }]);
    let (status, first) = respond(
        &gw,
        "ka",
        json!({ "model": "m", "instructions": parts, "input": "one" }),
    )
    .await;
    assert_eq!(status, 200, "{first}");
    let next = json!({ "model": "m", "input": "two", "previous_response_id": first["id"] });
    assert_eq!(respond(&gw, "ka", next).await.0, 200);
    let sent = sent.lock().unwrap().clone();
    assert_eq!(
        said(&sent[1]),
        pairs(&[("user", "one"), ("assistant", "reply-1"), ("user", "two")])
    );
}

#[tokio::test]
async fn a_response_unknown_or_unkept_is_refused_before_a_call() {
    let (base, sent) = fake().await;
    let gw = gateway(&base).await;
    let (_, unkept) = respond(
        &gw,
        "ka",
        json!({ "model": "m", "input": "one", "store": false }),
    )
    .await;
    let calls = sent.lock().unwrap().len();
    for (key, id) in [("ka", json!("resp_nope")), ("ka", unkept["id"].clone())] {
        let (status, body) = respond(
            &gw,
            key,
            json!({ "model": "m", "input": "two", "previous_response_id": id }),
        )
        .await;
        assert_eq!(status, 400, "{key} {id}: {body}");
        let message = body["error"]["message"].as_str().unwrap_or_default();
        assert!(message.contains("previous_response_id"), "{body}");
    }
    assert_eq!(sent.lock().unwrap().len(), calls, "no call was made");
}

/// A Chat client's turns with OpenAI's Responses, with the plugin on as it
/// is unless turned off: the reasoning a turn's tool calls came of goes back
/// before those calls in the next turn.
#[tokio::test]
async fn a_chat_clients_reasoning_between_tool_calls_reaches_responses_again() {
    let openai = mockllm::FakeOpenAi::start().await;
    let gw = Gateway::start(&format!(
        "client_keys = [{{ name = \"a\", key = \"ka\" }}, {{ name = \"b\", key = \"kb\" }}]\n\
         formats = [\"openai-chat\"]\n\
         providers = {{ p = {{ formats = \"openai-responses\", base_url = \"{}\", key = \"sk\" }} }}\n",
        openai.base_url()
    ))
    .await;
    let tools = json!([{ "type": "function", "function": { "name": "get_weather",
        "parameters": { "type": "object", "properties": { "city": { "type": "string" } } } } }]);
    let chat = |key: &'static str, body: Value| {
        let gw = &gw;
        async move {
            let resp = client()
                .post(gw.url("/v1/chat/completions"))
                .bearer_auth(key)
                .header("content-type", "application/json")
                .body(body.to_string())
                .send()
                .await
                .unwrap();
            let status = resp.status().as_u16();
            let body = parse(&resp.text().await.unwrap());
            assert_eq!(status, 200, "{body}");
            body
        }
    };

    // The first turn: the model reasons, then calls a tool.
    openai.answer(
        mockllm::Answer::tool_call("get_weather", json!({ "city": "Paris" }))
            .reasoning("look it up"),
    );
    let first = chat(
        "ka",
        json!({ "model": "o3", "tools": tools,
            "messages": [{ "role": "user", "content": "weather?" }] }),
    )
    .await;
    let calls = first["choices"][0]["message"]["tool_calls"].clone();
    let id = calls[0]["id"].as_str().unwrap().to_owned();
    let sent = &openai.requests()[0].body;
    assert_eq!(sent["store"], false, "{sent}");

    // The second turn, as the client sends it: its history has no reasoning.
    let second = |key: &'static str| {
        chat(
            key,
            json!({ "model": "o3", "tools": tools, "messages": [
                { "role": "user", "content": "weather?" },
                { "role": "assistant", "content": null, "tool_calls": calls },
                { "role": "tool", "tool_call_id": id, "content": "sunny" },
            ] }),
        )
    };
    second("ka").await;
    let input = openai.requests()[1].body["input"].clone();
    let kinds: Vec<&str> = input
        .as_array()
        .unwrap()
        .iter()
        .map(|i| i["type"].as_str().unwrap_or("message"))
        .collect();
    assert_eq!(
        kinds,
        [
            "message",
            "reasoning",
            "function_call",
            "function_call_output"
        ],
        "{input}"
    );
    assert!(
        input[1]["encrypted_content"]
            .as_str()
            .is_some_and(|c| c.starts_with("gAAAA")),
        "{input}"
    );
}
