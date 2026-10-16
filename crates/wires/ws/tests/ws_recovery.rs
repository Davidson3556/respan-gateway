#![allow(clippy::unwrap_used)]
//! Stateful provider protocols through real sockets, including recovery,
//! tool results and stream failures. mockllm validates the client events.

mod support;

use mockllm::{Answer, FakeGemini, FakeOpenAi};
use serde_json::{Value, json};
use support::*;

fn base(url: &str) -> String {
    url.replace("http://", "ws://")
        .trim_end_matches("/v1")
        .to_owned()
}

fn kind(text: &str, name: &str) -> bool {
    serde_json::from_str::<Value>(text).is_ok_and(|v| v["type"] == name)
}

async fn finish(ws: &mut Ws, server: &Server) {
    close(ws, 1000, "").await;
    assert!(matches!(hear(ws).await, Heard::Closed(_) | Heard::Broken));
    idle(&server.gateway).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn realtime_recovers_from_a_bad_event_then_streams_unicode_tool_arguments() {
    let provider = FakeOpenAi::start().await;
    let args = json!({"text":"雪😀\nquote: \" backslash: \\"});
    provider.answer(Answer::tool_call("echo", args.clone()).pace(1));
    let (gw, ends, calls) = relay(&base(&provider.base_url()));
    let server = serve(gw).await;
    let mut ws = connect(
        server.addr,
        "/v1/realtime?model=gpt-realtime",
        &[("authorization", "Bearer sk-test")],
    )
    .await
    .unwrap();
    assert!(kind(&heard_text(&mut ws).await, "session.created"));
    say(&mut ws, r#"{"type":"not.an.event","event_id":"bad"}"#).await;
    let error: Value = serde_json::from_str(&heard_text(&mut ws).await).unwrap();
    assert_eq!(error["type"], "error");
    assert_eq!(error["error"]["event_id"], "bad");
    say(
        &mut ws,
        r#"{"type":"response.create","response":{"output_modalities":["text"]}}"#,
    )
    .await;
    let events = hear_until(&mut ws, |s| kind(s, "response.done")).await;
    let mut arguments = String::new();
    for event in &events {
        let event: Value = serde_json::from_str(event).unwrap();
        if event["type"] == "response.function_call_arguments.delta" {
            arguments.push_str(event["delta"].as_str().unwrap());
        }
    }
    assert_eq!(serde_json::from_str::<Value>(&arguments).unwrap(), args);
    finish(&mut ws, &server).await;
    assert_eq!(code(&ends.only()), "ok");
    assert_eq!(codes(calls.all()), ["ok"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn gemini_tool_result_resumes_the_same_session_and_preserves_call_ids() {
    let provider = FakeGemini::start().await;
    provider.answer(Answer::tool_call("echo", json!({"text":"雪😀"})));
    provider.answer(Answer::text("tool result accepted"));
    let (gw, ends, _) = relay(&base(&provider.base_url()));
    let server = serve(gw).await;
    let mut ws = connect(server.addr, "/ws/google.ai.generativelanguage.v1beta.GenerativeService.BidiGenerateContent?key=AIza-test", &[]).await.unwrap();
    say(&mut ws, r#"{"setup":{"model":"models/gemini-3.8-live","generationConfig":{"responseModalities":["TEXT"]},"tools":[{"functionDeclarations":[{"name":"echo","parameters":{"type":"OBJECT","properties":{"text":{"type":"STRING"}}}}]}]}}"#).await;
    let setup: Value = serde_json::from_str(&heard_text(&mut ws).await).unwrap();
    assert!(setup.get("setupComplete").is_some());
    say(&mut ws, r#"{"realtimeInput":{"text":"echo 雪😀"}}"#).await;
    let call: Value = serde_json::from_str(&heard_text(&mut ws).await).unwrap();
    let call = &call["toolCall"]["functionCalls"][0];
    assert_eq!(call["name"], "echo");
    assert_eq!(call["args"]["text"], "雪😀");
    let response = json!({"toolResponse":{"functionResponses":[{"id":call["id"],"name":"echo","response":{"result":"雪😀"}}]}});
    say(&mut ws, &response.to_string()).await;
    let turn = hear_until(&mut ws, |s| {
        serde_json::from_str::<Value>(s).unwrap()["serverContent"]["turnComplete"] == true
    })
    .await;
    assert!(turn.iter().any(|s| s.contains("accepted")));
    let requests = provider.requests();
    let result = requests
        .iter()
        .find(|r| r.body.get("toolResponse").is_some())
        .unwrap();
    assert_eq!(result.body, response);
    finish(&mut ws, &server).await;
    assert_eq!(code(&ends.only()), "ok");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_cut_realtime_turn_is_truncated_and_never_replayed() {
    let provider = FakeOpenAi::start().await;
    provider.answer(Answer::text("one two three four").cut_after(2));
    let (gw, ends, calls) = relay(&base(&provider.base_url()));
    let server = serve(gw).await;
    let mut ws = connect(
        server.addr,
        "/v1/realtime?model=gpt-realtime",
        &[("authorization", "Bearer sk-test")],
    )
    .await
    .unwrap();
    assert!(kind(&heard_text(&mut ws).await, "session.created"));
    say(
        &mut ws,
        r#"{"type":"response.create","response":{"output_modalities":["text"]}}"#,
    )
    .await;
    let mut count = 0;
    while let Heard::Text(text) = hear(&mut ws).await {
        assert!(!kind(&text, "response.done"));
        count += 1;
    }
    assert_eq!(count, 2);
    idle(&server.gateway).await;
    assert_eq!(code(&ends.only()), "truncated");
    assert_eq!(codes(calls.all()), ["truncated"]);
    assert_eq!(
        provider
            .requests()
            .iter()
            .filter(|r| r.body["type"] == "response.create")
            .count(),
        1
    );
}
