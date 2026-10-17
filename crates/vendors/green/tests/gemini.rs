//! Gemini's quirks.

#[path = "../../tests/support/mod.rs"]
mod support;

use respan_gateway_vendor_kit::Capability;
use respan_gateway_vendors_green::gemini;

#[tokio::test]
async fn model_id_prefix() {
    let list = support::json!({"models": [
        {"name": "models/gemini-2.5-flash", "displayName": "Gemini 2.5 Flash", "inputTokenLimit": 1048576,
         "outputTokenLimit": 65536, "thinking": true, "supportedGenerationMethods": ["generateContent"]},
        {"name": "models/gemini-live-2.5-flash-preview", "supportedGenerationMethods": ["bidiGenerateContent"]},
    ]});
    // Its own API's list, with its key.
    let base = support::listing("/v1beta/models?pageSize=1000", list).await;
    let gemini = gemini::VENDOR.provider("g").base(format!("{base}/v1beta"));
    let gemini = gemini
        .base_of("openai-chat", format!("{base}/v1beta/openai"))
        .expect("its chat")
        .key("k");
    let models = gemini.models().await.expect("a list").expect("models");
    assert_eq!(models[0].display_name.as_deref(), Some("Gemini 2.5 Flash"));
    assert_eq!(models[0].context_window, Some(1048576));
    assert_eq!(
        models[0].capabilities,
        [Capability::Tools, Capability::Reasoning]
    );
    assert!(models[1].capabilities.is_empty());
    assert_eq!(
        support::ids(models),
        ["gemini-2.5-flash", "gemini-live-2.5-flash-preview"]
    );
}

mod thought_signature {
    use super::support::{Vendor, chat, json};
    use respan_gateway_conversation_http::dialect::Dialected;
    use respan_gateway_conversation_http::gemini::GeminiGenerate;
    use respan_gateway_conversation_http::openai::OpenAiChat;
    use respan_gateway_vendors_green::gemini::{Gemini, SIGNATURE_PLACEHOLDER};

    /// A conversation in which the model called `f` twice, then was answered.
    fn history(model: &str) -> serde_json::Value {
        let call = |id: &str| {
            json!({ "id": id, "type": "function",
                "function": { "name": "f", "arguments": "{}" } })
        };
        let mut request = chat(false);
        request["model"] = model.into();
        request["messages"] = json!([
            { "role": "user", "content": "go" },
            { "role": "assistant", "content": null, "tool_calls": [call("a"), call("b")] },
            { "role": "tool", "tool_call_id": "a", "content": "1" },
            { "role": "tool", "tool_call_id": "b", "content": "2" },
        ]);
        request
    }

    fn vendor() -> Vendor {
        let answer = json!({ "candidates": [{
            "content": { "role": "model", "parts": [{ "text": "ok" }] },
            "finishReason": "STOP" }], "modelVersion": "m" });
        Vendor::whole_in(GeminiGenerate, answer)
    }

    #[tokio::test]
    async fn thought_signature_is_a_placeholder_on_the_first_call_of_a_gemini_3_turn() {
        let gemini = vendor();
        gemini.openai(history("gemini-3.5-flash")).await;
        let sent = &gemini.sent()[0];
        let parts = &sent["contents"][1]["parts"];
        assert_eq!(parts[0]["thoughtSignature"], SIGNATURE_PLACEHOLDER);
        assert!(parts[1].get("thoughtSignature").is_none(), "{parts}");
        // The results of the turn are one content, whatever the client sent.
        assert_eq!(
            sent["contents"][2]["parts"].as_array().map(Vec::len),
            Some(2)
        );
    }

    #[tokio::test]
    async fn thought_signature_is_a_placeholder_in_extra_content_over_chat_completions() {
        let answer = json!({ "id": "c", "object": "chat.completion", "model": "m",
            "choices": [{ "index": 0, "finish_reason": "stop",
                "message": { "role": "assistant", "content": "ok" } }] });
        let gemini = Vendor::whole_in(Dialected::new(OpenAiChat, Gemini), answer);
        // A history from a client of another format, which has no signatures.
        let call = |id: &str| json!({ "type": "tool_use", "id": id, "name": "f", "input": {} });
        let result = |id: &str| json!({ "type": "tool_result", "tool_use_id": id, "content": "1" });
        gemini
            .anthropic(
                json!({ "model": "gemini-3-flash-preview", "max_tokens": 10, "messages": [
                { "role": "user", "content": "go" },
                { "role": "assistant", "content": [call("a"), call("b")] },
                { "role": "user", "content": [result("a"), result("b")] },
            ] }),
            )
            .await;
        let calls = &gemini.sent()[0]["messages"][1]["tool_calls"];
        let signature = &calls[0]["extra_content"]["google"]["thought_signature"];
        assert_eq!(signature, SIGNATURE_PLACEHOLDER, "{calls}");
        assert!(calls[1].get("extra_content").is_none(), "{calls}");
    }

    #[tokio::test]
    async fn thought_signature_replaces_a_cache_marker_another_format_put_on_a_call() {
        let gemini = vendor();
        let call = json!({ "type": "tool_use", "id": "a", "name": "f", "input": {},
            "cache_control": { "type": "ephemeral" } });
        gemini
            .anthropic(json!({ "model": "gemini-3-flash-preview", "max_tokens": 10, "messages": [
                { "role": "user", "content": "go" },
                { "role": "assistant", "content": [call] },
                { "role": "user", "content": [{ "type": "tool_result", "tool_use_id": "a", "content": "1" }] },
            ] }))
            .await;
        let parts = &gemini.sent()[0]["contents"][1]["parts"];
        assert_eq!(
            parts[0]["thoughtSignature"], SIGNATURE_PLACEHOLDER,
            "{parts}"
        );
    }

    #[tokio::test]
    async fn an_older_model_is_sent_no_signature() {
        let gemini = vendor();
        gemini.openai(history("gemini-2.5-flash")).await;
        let parts = &gemini.sent()[0]["contents"][1]["parts"];
        assert!(parts[0].get("thoughtSignature").is_none(), "{parts}");
    }
}

mod extra_content {
    use super::support::{Vendor, chat, json, messages};
    use respan_gateway_conversation_http::dialect::Dialected;
    use respan_gateway_conversation_http::openai::OpenAiChat;
    use respan_gateway_vendors_green::gemini::Gemini;

    /// A Gemini 3 reply over Chat Completions, signed in `extra_content`.
    fn signed() -> Vendor {
        Vendor::whole_in(
            Dialected::new(OpenAiChat, Gemini),
            json!({ "id": "c", "object": "chat.completion", "model": "gemini-3-flash-preview",
                "choices": [{ "index": 0, "finish_reason": "stop", "message": {
                    "role": "assistant", "content": "Hello there.",
                    "extra_content": { "google": { "thought_signature": "EsED" } } } }],
                "usage": { "prompt_tokens": 7, "completion_tokens": 3, "total_tokens": 10 } }),
        )
    }

    #[tokio::test]
    async fn extra_content_is_dropped_for_anthropic_and_kept_for_openai_chat() {
        let answer: serde_json::Value =
            serde_json::from_str(&signed().anthropic(messages(false)).await).expect("JSON");
        assert_eq!(answer["content"][0]["text"], "Hello there.", "{answer}");
        assert!(
            !answer.to_string().contains("thought_signature"),
            "{answer}"
        );

        let answer: serde_json::Value =
            serde_json::from_str(&signed().openai(chat(false)).await).expect("JSON");
        let message = &answer["choices"][0]["message"];
        assert_eq!(
            message["extra_content"]["google"]["thought_signature"], "EsED",
            "{answer}"
        );
    }
}
