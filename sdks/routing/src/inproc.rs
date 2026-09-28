//! A model in the same process, reached as a provider.

use respan_gateway_core::bytes::Bytes;
use respan_gateway_core::http::{HeaderValue, StatusCode, header::CONTENT_TYPE};
use respan_gateway_core::http_body_util::BodyExt;
use respan_gateway_core::serde_json::{Value, json};
use respan_gateway_core::{Body, Doc, Fault, Transport, async_trait, http};

/// A local model: the conversation as `(role, text)` pairs to its answer.
pub trait LocalModel: Send + Sync + 'static {
    /// Answer `messages`.
    fn answer(&self, messages: &[(String, String)]) -> Result<String, Fault>;
}

impl<F> LocalModel for F
where
    F: Fn(&[(String, String)]) -> Result<String, Fault> + Send + Sync + 'static,
{
    fn answer(&self, messages: &[(String, String)]) -> Result<String, Fault> {
        self(messages)
    }
}

/// A provider that is code in this process, to register with the
/// `OpenAiChat` format: `Provider::new("local", OpenAiChat, InProcess(model))`.
/// It is a full provider, so it can be a router's target or a fallback, and
/// the provider edge (billing, limits) sees its calls.
///
/// The request arrives as a body to collect and a response is bytes, so the model's
/// answer is written as OpenAI JSON or SSE and decoded again by the format
/// (FINDINGS F8).
#[derive(Debug, Clone)]
pub struct InProcess<M>(pub M);

#[async_trait]
impl<M: LocalModel> Transport for InProcess<M> {
    async fn send(&self, call: http::Request<Body>) -> Result<http::Response<Body>, Fault> {
        // A plain call's body is one whole frame; read it back as JSON
        // (FINDINGS N6).
        let body = Doc::from(call.into_body().collect().await?.to_bytes());
        let v = body.json();
        let v = v.ok_or_else(|| Fault::bad_request("not an OpenAI chat request"))?;
        let messages: Vec<(String, String)> = v["messages"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|m| {
                (
                    m["role"].as_str().unwrap_or("user").to_owned(),
                    text_of(&m["content"]),
                )
            })
            .collect();
        let model = v["model"].as_str().unwrap_or("local").to_owned();
        let answer = self.0.answer(&messages)?;
        let prompt_tokens = messages
            .iter()
            .map(|(_, t)| t.split_whitespace().count())
            .sum::<usize>();
        let completion_tokens = answer.split_whitespace().count();
        let usage = json!({"prompt_tokens": prompt_tokens, "completion_tokens": completion_tokens,
            "total_tokens": prompt_tokens + completion_tokens});
        let (content_type, out) = match v["stream"] == true {
            false => {
                let whole = json!({"id": "local-1", "object": "chat.completion", "model": model,
                    "choices": [{"index": 0, "finish_reason": "stop",
                        "message": {"role": "assistant", "content": answer}}],
                    "usage": usage});
                ("application/json", whole.to_string())
            }
            true => {
                let chunk = |delta: Value, finish: Value| {
                    let c = json!({"id": "local-1", "object": "chat.completion.chunk", "model": model,
                        "choices": [{"index": 0, "delta": delta, "finish_reason": finish}]});
                    format!("data: {c}\n\n")
                };
                let mut out = chunk(json!({"role": "assistant", "content": ""}), Value::Null);
                for (i, word) in answer.split(' ').enumerate() {
                    let word = if i == 0 {
                        word.to_owned()
                    } else {
                        format!(" {word}")
                    };
                    out += &chunk(json!({"content": word}), Value::Null);
                }
                out += &chunk(json!({}), "stop".into());
                let tail = json!({"id": "local-1", "object": "chat.completion.chunk",
                    "model": model, "choices": [], "usage": usage});
                out += &format!("data: {tail}\n\ndata: [DONE]\n\n");
                ("text/event-stream", out)
            }
        };
        let mut resp = http::Response::new(Body::from(Doc::from(Bytes::from(out))));
        *resp.status_mut() = StatusCode::OK;
        resp.headers_mut()
            .insert(CONTENT_TYPE, HeaderValue::from_static(content_type));
        Ok(resp)
    }
}

/// A message's text: a string, or its text parts joined.
fn text_of(content: &Value) -> String {
    match content {
        Value::String(s) => s.clone(),
        Value::Array(parts) => parts.iter().filter_map(|p| p["text"].as_str()).collect(),
        _ => String::new(),
    }
}
