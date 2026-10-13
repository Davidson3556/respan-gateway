//! Gemini's quirks.

mod support;

use respan_gateway_vendors::gemini;

#[test]
fn model_id_prefix() {
    let id = gemini::VENDOR.model_id("models/gemini-2.5-flash");
    assert_eq!(id, "gemini-2.5-flash");
    assert_eq!(gemini::VENDOR.model_id("gemini-2.5-flash"), id);
}

mod thought_signature {
    use super::support::{Vendor, chat, json};
    use respan_gateway_conversation_http::gemini::GeminiGenerate;
    use respan_gateway_vendors::gemini::{GeminiQuirks, SIGNATURE_PLACEHOLDER};

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
        Vendor::speaking(
            GeminiGenerate,
            &GeminiQuirks,
            "application/json",
            answer.to_string(),
        )
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
    async fn an_older_model_is_sent_no_signature() {
        let gemini = vendor();
        gemini.openai(history("gemini-2.5-flash")).await;
        let parts = &gemini.sent()[0]["contents"][1]["parts"];
        assert!(parts[0].get("thoughtSignature").is_none(), "{parts}");
    }
}
