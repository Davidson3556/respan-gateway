//! What the vendors' tests share: a provider's answer, decoded.

use respan_gateway_conversation::Output;
use respan_gateway_conversation_http::Schema;
use respan_gateway_conversation_http::openai::OpenAiChat;
use respan_gateway_http::bytes::Bytes;
use respan_gateway_http::http::{self, header::CONTENT_TYPE};
use serde_json::Value;

/// The events of a whole answer, `body`, in `chat`.
#[allow(dead_code)]
pub fn whole(chat: OpenAiChat, body: Value) -> Vec<Output> {
    decoded(chat, "application/json", body.to_string())
}

/// The events of a stream of `chunks`, then `[DONE]`, in `chat`.
#[allow(dead_code)]
pub fn streamed(chat: OpenAiChat, chunks: &[Value]) -> Vec<Output> {
    let mut stream: String = chunks.iter().map(|c| format!("data: {c}\n\n")).collect();
    stream.push_str("data: [DONE]\n\n");
    decoded(chat, "text/event-stream", stream)
}

fn decoded(chat: OpenAiChat, content_type: &str, body: String) -> Vec<Output> {
    let answer = http::Response::builder()
        .header(CONTENT_TYPE, content_type)
        .body(Bytes::from(body))
        .expect("an answer");
    let call = http::Request::post("/chat/completions")
        .body(())
        .expect("a call")
        .into_parts()
        .0;
    chat.decode_response(answer, &call)
        .expect("the answer's events")
}

/// Whether `events` hold anything the kind has no form for.
#[allow(dead_code)]
pub fn any_native(events: &[Output]) -> bool {
    events.iter().any(|e| matches!(e, Output::Native(_)))
}
