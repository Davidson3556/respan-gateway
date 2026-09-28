//! Custom transports: a provider quirk, an in-process model, a stall bound.

mod common;

use std::time::Duration;

use common::*;
use respan_gateway_core::http::StatusCode;
use respan_gateway_core::serde_json::{Value, json};
use respan_gateway_core::{Fault, Gateway};
use respan_gateway_formats::{AnthropicMessages, OpenAiChat};
use respan_gateway_routing::fallback::Fallback;
use respan_gateway_routing::inproc::InProcess;
use respan_gateway_routing::quirks::DropFields;
use respan_gateway_routing::retry::Backoff;
use respan_gateway_routing::watchdog::Watchdog;
use respan_gateway_testkit::MockTransport;

fn echo(messages: &[(String, String)]) -> Result<String, Fault> {
    let last = messages.last().map(|(_, t)| t.as_str()).unwrap_or_default();
    Ok(format!("you said {last}"))
}

fn local_gateway() -> Gateway {
    Gateway::new()
        .provider("local", OpenAiChat, InProcess(echo))
        .and_then(|g| g.serve(OpenAiChat)?.serve(AnthropicMessages))
        .and_then(|g| g.target("local"))
        .expect("gateway")
}

#[tokio::test]
async fn an_in_process_model_answers_whole_and_streamed() {
    let gw = local_gateway();

    let (s1, _, whole) = send(&gw, chat("tiny", false, "hi")).await;
    let (s2, _, streamed) = send(&gw, chat("tiny", true, "hi there")).await;

    assert_eq!((s1, s2), (StatusCode::OK, StatusCode::OK));
    assert_eq!(openai_text(&whole.expect("whole")), "you said hi");
    let streamed = streamed.expect("whole");
    assert_eq!(openai_text(&streamed), "you said hi there");
    assert!(streamed.ends_with("data: [DONE]\n\n"));
    // The client did not ask for usage, so the chunk the format asked for
    // is hidden from it.
    assert!(!streamed.contains("\"usage\""), "{streamed}");
}

#[tokio::test]
async fn an_anthropic_client_reaches_the_in_process_model_translated() {
    let gw = local_gateway();

    let (status, _, body) = send(&gw, messages("tiny", true, "bonjour")).await;

    assert_eq!(status, StatusCode::OK);
    let body = body.expect("whole");
    let text: String = data(&body)
        .iter()
        .filter_map(|d| d["delta"]["text"].as_str().map(str::to_owned))
        .collect();
    assert_eq!(text, "you said bonjour", "{body}");
}

#[tokio::test]
async fn the_in_process_model_is_a_fallback_like_any_provider() {
    let (mock, log) = (MockTransport::new(), Log::default());
    let gw = Gateway::new()
        .provider("openai", OpenAiChat, mock.at(OPENAI))
        .and_then(|g| g.provider("local", OpenAiChat, InProcess(echo)))
        .and_then(|g| g.serve(OpenAiChat))
        .and_then(|g| {
            g.plugin("log", log.clone())?.plugin(
                "fallback",
                Fallback::new(Backoff::once()).then("local", None),
            )
        })
        .and_then(|g| g.target("openai"))
        .expect("gateway");
    mock.reply(OPENAI, json_reply(StatusCode::SERVICE_UNAVAILABLE, "{}"));

    let (status, _, body) = send(&gw, chat("gpt-x", false, "hi")).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(openai_text(&body.expect("whole")), "you said hi");
    // Its usage reaches the provider edge, as a remote provider's would.
    let local = log
        .calls()
        .into_iter()
        .find(|c| c.edge == "local")
        .expect("a call");
    assert_eq!((local.end.as_str(), local.output_tokens), ("ok", 3.0));
}

fn quirky(mock: &MockTransport) -> Gateway {
    let drop = DropFields::new(["quirky"], ["stream_options", "user"]);
    Gateway::new()
        .provider("quirky", OpenAiChat, mock.at(OPENAI))
        .and_then(|g| g.provider("openai", OpenAiChat, mock.at(BACKUP)))
        .and_then(|g| g.serve(OpenAiChat)?.plugin("drop", drop))
        .and_then(|g| g.target("quirky"))
        .expect("gateway")
}

#[tokio::test]
async fn a_provider_rejecting_fields_never_sees_them() {
    let mock = MockTransport::new();
    let gw = quirky(&mock);
    mock.reply(OPENAI, sse(&openai_stream(&["ok"])));
    let req = chat_with(json!({"model": "gpt-x", "stream": true, "user": "u-1",
        "temperature": 0.5, "messages": [{"role": "user", "content": "hi"}]}));

    let (status, _, body) = send(&gw, req).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(openai_text(&body.expect("whole")), "ok");
    let sent = sent_json(&mock.sent()[0]);
    assert!(
        sent.get("stream_options").is_none() && sent.get("user").is_none(),
        "{sent}"
    );
    assert_eq!(sent["temperature"], 0.5);
}

/// F5: fixed by `edit_call`. A provider-edge hook removes the fields from
/// the call's final request, after `prepare` wrote `stream_options`; `user`
/// has no place in the kind, so it is out of reach of `input_mut`. Calls to
/// other providers keep them.
#[tokio::test]
async fn a_provider_edge_hook_removes_a_field_the_provider_rejects() {
    let mock = MockTransport::new();
    let gw = quirky(&mock);
    let fallback = Fallback::new(Backoff::once()).then("openai", None);
    let gw = gw.plugin("fallback", fallback).expect("plugin");
    mock.reply(OPENAI, json_reply(StatusCode::SERVICE_UNAVAILABLE, "{}"))
        .reply(BACKUP, sse(&openai_stream(&["ok"])));
    let req = chat_with(json!({"model": "gpt-x", "stream": true, "user": "u-1",
        "messages": [{"role": "user", "content": "hi"}]}));

    let (status, _, _) = send(&gw, req).await;

    assert_eq!(status, StatusCode::OK);
    let (quirky, openai): (Value, Value) = (sent_json(&mock.sent()[0]), sent_json(&mock.sent()[1]));
    assert!(quirky.get("user").is_none(), "{quirky}");
    assert!(quirky.get("stream_options").is_none(), "{quirky}");
    assert_eq!(openai["user"], "u-1");
    assert!(openai.get("stream_options").is_some(), "{openai}");
}

#[tokio::test(start_paused = true)]
async fn a_stalled_stream_ends_in_an_error_in_the_clients_framing() {
    let (mock, log) = (MockTransport::new(), Log::default());
    let stalls = Stalls {
        inner: mock.at(OPENAI),
    };
    let gw = Gateway::new()
        .provider(
            "openai",
            OpenAiChat,
            Watchdog::new(stalls, Duration::from_secs(30)),
        )
        .and_then(|g| {
            g.serve(OpenAiChat)?
                .plugin("log", log.clone())?
                .target("openai")
        })
        .expect("gateway");
    let mut partial = openai_stream(&["Hel", "lo"]);
    partial.truncate(2);
    mock.reply(OPENAI, sse(&partial));

    let (status, _, body) = send(&gw, chat("gpt-x", true, "hi")).await;

    assert_eq!(status, StatusCode::OK);
    let body = body.expect("an error event, not an abort");
    assert_eq!(openai_text(&body), "Hel");
    let last = data(&body).pop().expect("an event");
    assert_eq!(last["error"]["code"], "upstream", "{body}");
    assert_eq!(log.client().end, "upstream");
}

#[tokio::test(start_paused = true)]
async fn a_stream_stalled_before_its_first_byte_is_refused_with_a_status() {
    let mock = MockTransport::new();
    let stalls = Stalls {
        inner: mock.at(OPENAI),
    };
    let gw = Gateway::new()
        .provider(
            "openai",
            OpenAiChat,
            Watchdog::new(stalls, Duration::from_secs(30)),
        )
        .and_then(|g| g.serve(OpenAiChat)?.target("openai"))
        .expect("gateway");
    mock.reply(OPENAI, sse(&[]));

    let (status, _, _) = send(&gw, chat("gpt-x", true, "hi")).await;

    assert_eq!(status, StatusCode::BAD_GATEWAY);
}
