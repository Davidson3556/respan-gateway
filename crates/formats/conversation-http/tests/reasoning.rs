#![allow(clippy::unwrap_used)]
//! Reasoning controls and `top_k` through the four request formats: each
//! reads and writes its own, a same-format relay writes back what it read,
//! and what another format cannot say is refused.

use respan_gateway_conversation::{Effort, Reasoning};
use respan_gateway_conversation_http::Schema;
use respan_gateway_conversation_http::anthropic::AnthropicMessages;
use respan_gateway_conversation_http::gemini::GeminiGenerate;
use respan_gateway_conversation_http::openai::OpenAiChat;
use respan_gateway_conversation_http::responses::OpenAiResponses;
use serde_json::{Value, json};

fn chat(extra: Value) -> Value {
    let mut v = json!({ "model": "m", "messages": [{ "role": "user", "content": "hi" }] });
    v.as_object_mut()
        .unwrap()
        .extend(extra.as_object().unwrap().clone());
    v
}
fn anthropic(extra: Value) -> Value {
    let mut v = chat(extra);
    v["max_tokens"] = 2048.into();
    v
}
fn responses(extra: Value) -> Value {
    let mut v = json!({ "model": "m", "input": [{ "role": "user", "content": "hi" }] });
    v.as_object_mut()
        .unwrap()
        .extend(extra.as_object().unwrap().clone());
    v
}
fn gemini(config: Value) -> Value {
    json!({ "contents": [{ "role": "user", "parts": [{ "text": "hi" }] }], "generationConfig": config })
}

fn back<S: Schema>(s: S, v: &Value) -> Value {
    let input = s.decode_json(v);
    let mut out = Value::Object(s.encode_request(&input).expect("written"));
    // The model travels beside the body.
    if let (Some(m), true) = (v.get("model"), out.get("model").is_none()) {
        out["model"] = m.clone();
    }
    out
}

/// Same-format fidelity, and the kind's reading of it.
fn reads<S: Schema>(s: S, v: Value, want: Option<Reasoning>) {
    let input = s.decode_json(&v);
    assert_eq!(input.options.reasoning, want, "{v}");
    let again = back(s, &v);
    assert_eq!(again["generationConfig"], v["generationConfig"], "{v}");
    for k in ["reasoning_effort", "thinking", "reasoning"] {
        assert_eq!(again.get(k), v.get(k), "{k} in {v}");
    }
}

fn effort(e: Effort) -> Option<Reasoning> {
    Some(Reasoning::effort(e))
}

#[test]
fn openai_chat_reads_and_writes_reasoning_effort() {
    for (name, e) in [
        ("none", Effort::Off),
        ("minimal", Effort::Minimal),
        ("low", Effort::Low),
        ("medium", Effort::Medium),
        ("high", Effort::High),
    ] {
        reads(
            OpenAiChat,
            chat(json!({ "reasoning_effort": name })),
            effort(e),
        );
    }
    // An effort the kind has no name for stays native, and goes back.
    let v = chat(json!({ "reasoning_effort": "xhigh" }));
    reads(OpenAiChat, v.clone(), None);
    assert!(
        AnthropicMessages
            .encode_request(&OpenAiChat.decode_json(&v))
            .is_err()
    );
}

#[test]
fn responses_reads_effort_and_an_auto_summary() {
    let both = responses(json!({ "reasoning": { "effort": "high", "summary": "auto" } }));
    let mut want = Reasoning::effort(Effort::High);
    want.summary = Some(true);
    reads(OpenAiResponses, both, Some(want));
    reads(
        OpenAiResponses,
        responses(json!({ "reasoning": { "effort": "none" } })),
        effort(Effort::Off),
    );
    // A detailed summary has no form in the kind: native, and refused elsewhere.
    let v = responses(json!({ "reasoning": { "effort": "low", "summary": "detailed" } }));
    let input = OpenAiResponses.decode_json(&v);
    assert_eq!(input.options.reasoning, effort(Effort::Low));
    assert_eq!(back(OpenAiResponses, &v)["reasoning"], v["reasoning"]);
    assert!(OpenAiChat.encode_request(&input).is_err());
}

#[test]
fn anthropic_reads_a_budget_and_disabled() {
    let on = anthropic(json!({ "thinking": { "type": "enabled", "budget_tokens": 1024 } }));
    reads(AnthropicMessages, on, Some(Reasoning::budget(1024)));
    let off = anthropic(json!({ "thinking": { "type": "disabled" } }));
    reads(AnthropicMessages, off, effort(Effort::Off));
    // Adaptive thinking, or a field beside, stays native.
    reads(
        AnthropicMessages,
        anthropic(json!({ "thinking": { "type": "adaptive" } })),
        None,
    );
    let shown = anthropic(
        json!({ "thinking": { "type": "enabled", "budget_tokens": 2000, "display": "omitted" } }),
    );
    reads(AnthropicMessages, shown, None);
}

#[test]
fn gemini_reads_budget_level_and_thoughts() {
    reads(
        GeminiGenerate,
        gemini(json!({ "thinkingConfig": { "thinkingBudget": 512 } })),
        Some(Reasoning::budget(512)),
    );
    reads(
        GeminiGenerate,
        gemini(json!({ "thinkingConfig": { "thinkingBudget": 0 } })),
        effort(Effort::Off),
    );
    let mut want = Reasoning::effort(Effort::Low);
    want.summary = Some(true);
    reads(
        GeminiGenerate,
        gemini(json!({ "thinkingConfig": { "thinkingLevel": "low", "includeThoughts": true } })),
        Some(want),
    );
    // Dynamic thinking (-1) is not a budget in tokens: native.
    reads(
        GeminiGenerate,
        gemini(json!({ "thinkingConfig": { "thinkingBudget": -1 } })),
        None,
    );
    // Uppercase levels read too, and are written as the docs spell them.
    let up = GeminiGenerate.decode_json(&gemini(
        json!({ "thinkingConfig": { "thinkingLevel": "HIGH" } }),
    ));
    assert_eq!(up.options.reasoning, effort(Effort::High));
    let out = GeminiGenerate.encode_request(&up).unwrap();
    assert_eq!(
        out["generationConfig"]["thinkingConfig"]["thinkingLevel"],
        "high"
    );
}

#[test]
fn an_effort_crosses_between_effort_formats() {
    let input = OpenAiChat.decode_json(&chat(json!({ "reasoning_effort": "medium" })));
    let r = OpenAiResponses.encode_request(&input).unwrap();
    assert_eq!(r["reasoning"], json!({ "effort": "medium" }));
    let g = GeminiGenerate.encode_request(&input).unwrap();
    assert_eq!(
        g["generationConfig"]["thinkingConfig"],
        json!({ "thinkingLevel": "medium" })
    );
}

#[test]
fn a_budget_crosses_between_budget_formats() {
    let input = AnthropicMessages.decode_json(&anthropic(
        json!({ "thinking": { "type": "enabled", "budget_tokens": 4096 } }),
    ));
    let g = GeminiGenerate.encode_request(&input).unwrap();
    assert_eq!(
        g["generationConfig"]["thinkingConfig"],
        json!({ "thinkingBudget": 4096 })
    );
    let back = AnthropicMessages
        .encode_request(&GeminiGenerate.decode_json(&Value::Object(g)))
        .unwrap();
    assert_eq!(
        back["thinking"],
        json!({ "type": "enabled", "budget_tokens": 4096 })
    );
}

#[test]
fn off_crosses_everywhere() {
    let input = OpenAiChat.decode_json(&chat(json!({ "reasoning_effort": "none" })));
    let a = AnthropicMessages.encode_request(&input).unwrap();
    assert_eq!(a["thinking"], json!({ "type": "disabled" }));
    let g = GeminiGenerate.encode_request(&input).unwrap();
    assert_eq!(
        g["generationConfig"]["thinkingConfig"],
        json!({ "thinkingBudget": 0 })
    );
    let r = OpenAiResponses.encode_request(&input).unwrap();
    assert_eq!(r["reasoning"], json!({ "effort": "none" }));
}

#[test]
fn an_effort_is_not_a_budget_and_is_refused() {
    let effort = OpenAiChat.decode_json(&chat(json!({ "reasoning_effort": "high" })));
    assert!(AnthropicMessages.encode_request(&effort).is_err());
    let budget = AnthropicMessages.decode_json(&anthropic(
        json!({ "thinking": { "type": "enabled", "budget_tokens": 2048 } }),
    ));
    assert!(OpenAiChat.encode_request(&budget).is_err());
    assert!(OpenAiResponses.encode_request(&budget).is_err());
    let mut both = Reasoning::effort(Effort::Low);
    both.budget_tokens = Some(1);
    let mut input = OpenAiChat.decode_json(&chat(json!({})));
    input.options.reasoning = Some(both);
    for r in [
        OpenAiChat.encode_request(&input),
        GeminiGenerate.encode_request(&input),
    ] {
        assert!(r.is_err());
    }
}

#[test]
fn thoughts_the_target_cannot_return_or_withhold_are_refused() {
    let shown = GeminiGenerate.decode_json(&gemini(
        json!({ "thinkingConfig": { "includeThoughts": true } }),
    ));
    assert!(OpenAiChat.encode_request(&shown).is_err());
    let r = OpenAiResponses.encode_request(&shown).unwrap();
    assert_eq!(r["reasoning"], json!({ "summary": "auto" }));
    let a = AnthropicMessages.encode_request(&shown).unwrap();
    assert!(a.get("thinking").is_none());
    let hidden = GeminiGenerate.decode_json(&gemini(json!({
        "thinkingConfig": { "includeThoughts": false, "thinkingBudget": 1024 }
    })));
    assert!(AnthropicMessages.encode_request(&hidden).is_err());
}

#[test]
fn top_k_is_anthropics_and_geminis() {
    let a = AnthropicMessages.decode_json(&anthropic(json!({ "top_k": 40 })));
    assert_eq!(a.options.top_k, Some(40));
    assert_eq!(AnthropicMessages.encode_request(&a).unwrap()["top_k"], 40);
    let g = GeminiGenerate.encode_request(&a).unwrap();
    assert_eq!(g["generationConfig"]["topK"], 40);
    let again = GeminiGenerate.decode_json(&Value::Object(g));
    assert_eq!(again.options.top_k, Some(40));
    assert!(again.native.is_none());
    assert!(OpenAiChat.encode_request(&a).is_err());
    assert!(OpenAiResponses.encode_request(&a).is_err());
}
