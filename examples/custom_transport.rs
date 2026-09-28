//! A model in this process, reached as a provider: a [`Transport`] that
//! answers OpenAI Chat, JSON or SSE, as the request asks. It is a full
//! provider, so it can be a target or a fallback, and every provider-edge
//! plugin (billing, limits, guards) sees its calls.
//!
//! The transport speaks bytes, so it reuses the format as a codec on the
//! provider's side: `request_decoder` reads the call into a
//! [`Conversation`], `response_encoder` writes the answer's [`Reply`]s.
//! Registered with `AnthropicMessages` instead, the same code answers in
//! Anthropic's wire format.
//!
//! `cargo run -p respan-gateway-examples --example custom_transport`

use respan_gateway_core::bytes::{Bytes, BytesMut};
use respan_gateway_core::futures::stream;
use respan_gateway_core::http_body_util::{BodyExt, StreamBody};
use respan_gateway_core::{Body, Fault, Format, Gateway, Transport, Usage, async_trait, http};
use respan_gateway_formats::OpenAiChat;
use respan_gateway_formats::conversation::{Conversation, Finish, Reply, units};
use respan_gateway_testkit::{post, read_whole};

/// Your model: a conversation in, its answer out.
trait LocalModel: Send + Sync + 'static {
    fn answer(&self, chat: &Conversation) -> Result<String, Fault>;
}

/// A toy model that echoes the last message.
struct Echo;

impl LocalModel for Echo {
    fn answer(&self, chat: &Conversation) -> Result<String, Fault> {
        let last = chat.messages.last().map(|m| m.text()).unwrap_or_default();
        Ok(format!("you said: {last}"))
    }
}

/// `model`, reached through `format`'s wire protocol.
struct Local<F, M> {
    format: F,
    model: M,
}

#[async_trait]
impl<F, M> Transport for Local<F, M>
where
    F: Format<Input = Conversation, Output = Reply>,
    M: LocalModel,
{
    async fn send(&self, call: http::Request<Body>) -> Result<http::Response<Body>, Fault> {
        // A whole call's body is its one input.
        let (head, body) = call.into_parts();
        let call = http::Request::from_parts(head, body.collect().await?.to_bytes());
        let mut decoder = self.format.request_decoder(&call);
        let mut chats = Vec::new();
        decoder.decode(call.body().clone(), &mut chats)?;
        decoder.end(&mut chats)?;
        let chat = chats
            .pop()
            .ok_or_else(|| Fault::bad_request("no conversation"))?;
        let answer = self.model.answer(&chat)?;

        // One reply per word, then the finish with usage (a word is a token here).
        let mut usage = Usage::default();
        let words = |s: &str| s.split_whitespace().count() as f64;
        usage.add(
            units::INPUT_TOKENS,
            chat.messages.iter().map(|m| words(&m.text())).sum(),
        );
        usage.add(units::OUTPUT_TOKENS, words(&answer));
        let model = chat.model.clone().unwrap_or_else(|| "local".into());
        let mut replies = vec![Reply::start("local-1", model)];
        // Each word keeps the space before the next: "you ", "said: ", ...
        replies.extend(answer.split_inclusive(' ').map(|w| Reply::text(0, w)));
        let mut last = Reply::finished(Finish::Stop);
        last.usage = usage;
        replies.push(last);

        // The encoder sets the content type: SSE if the call asked to stream.
        let (mut head, ()) = http::Response::new(()).into_parts();
        let mut encoder = self.format.response_encoder(&mut head, &call);
        let mut frames = Vec::new();
        let mut out = BytesMut::new();
        encoder.open(&mut out);
        for reply in &replies {
            encoder.encode(reply, &mut out)?;
            frames.push(out.split().freeze());
        }
        encoder.end(&Ok(()), &mut out)?;
        frames.push(out.freeze());
        let frames = frames.into_iter().filter(|f| !f.is_empty());
        let body =
            stream::iter(frames.map(|f: Bytes| Ok(respan_gateway_core::http_body::Frame::data(f))));
        Ok(http::Response::from_parts(
            head,
            StreamBody::new(body).boxed_unsync(),
        ))
    }
}

fn gateway() -> Result<Gateway, Fault> {
    let local = Local {
        format: OpenAiChat,
        model: Echo,
    };
    Gateway::new()
        .provider("local", OpenAiChat, local)?
        .serve(OpenAiChat)?
        .target("local")
}

const UNARY: &str = r#"{"model":"echo","messages":[{"role":"user","content":"hello there"}]}"#;
const STREAM: &str = r#"{"model":"echo","stream":true,"stream_options":{"include_usage":true},"messages":[{"role":"user","content":"hello there"}]}"#;

#[tokio::main]
async fn main() -> Result<(), Fault> {
    let gw = gateway()?;
    for req in [UNARY, STREAM] {
        let (status, _, body) =
            read_whole(gw.handle(post("/v1/chat/completions", req)).await.0).await;
        println!("{status}\n{}\n", String::from_utf8_lossy(&body?));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use respan_gateway_core::{Plugin, ProviderContext};
    use respan_gateway_formats::AnthropicMessages;
    use respan_gateway_testkit::Calls;

    async fn ask(gw: &Gateway, req: &'static str) -> Result<(http::HeaderMap, String), Fault> {
        let (status, headers, body) =
            read_whole(gw.handle(post("/v1/chat/completions", req)).await.0).await;
        assert_eq!(status, http::StatusCode::OK);
        Ok((headers, String::from_utf8_lossy(&body?).into_owned()))
    }

    #[tokio::test]
    async fn answers_json_or_sse_as_asked() -> Result<(), Fault> {
        let gw = gateway()?;
        let (headers, json) = ask(&gw, UNARY).await?;
        assert_eq!(headers["content-type"], "application/json");
        let v: serde_json::Value =
            serde_json::from_str(&json).map_err(|e| Fault::internal(e.to_string()))?;
        assert_eq!(
            v["choices"][0]["message"]["content"],
            "you said: hello there"
        );
        let (headers, sse) = ask(&gw, STREAM).await?;
        assert_eq!(headers["content-type"], "text/event-stream");
        assert!(sse.contains(r#""content":"hello ""#), "{sse}");
        assert!(sse.ends_with("data: [DONE]\n\n"), "{sse}");
        Ok(())
    }

    #[tokio::test]
    async fn the_same_model_answers_in_anthropics_format() -> Result<(), Fault> {
        let local = Local {
            format: AnthropicMessages,
            model: Echo,
        };
        let gw = Gateway::new()
            .provider("local", AnthropicMessages, local)?
            .serve(AnthropicMessages)?
            .target("local")?;
        let req = r#"{"model":"echo","max_tokens":9,"stream":true,"messages":[{"role":"user","content":"hi"}]}"#;
        let (_, _, body) = read_whole(gw.handle(post("/v1/messages", req)).await.0).await;
        let sse = String::from_utf8_lossy(&body?).into_owned();
        assert!(
            sse.contains("event: content_block_delta") && sse.contains(r#""text":"said: ""#),
            "{sse}"
        );
        Ok(())
    }

    /// Refuses every call to "local", as a limit or guard would.
    struct Closed;

    #[async_trait]
    impl Plugin for Closed {
        async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
            match px.provider() {
                "local" => Err(Fault::new(
                    http::StatusCode::TOO_MANY_REQUESTS,
                    "limit",
                    "closed",
                )),
                _ => Ok(()),
            }
        }
    }

    #[tokio::test]
    async fn the_provider_edge_sees_and_bills_its_calls() -> Result<(), Fault> {
        let calls = Calls::default();
        let gw = gateway()?.plugin("calls", calls.clone())?;
        ask(&gw, STREAM).await?;
        gw.idle().await;
        let usage = calls.usage();
        assert_eq!(usage.get(units::INPUT_TOKENS), Some(2.0));
        assert_eq!(usage.get(units::OUTPUT_TOKENS), Some(4.0));

        let gw = gateway()?.plugin("closed", Closed)?;
        let (status, _, _) =
            read_whole(gw.handle(post("/v1/chat/completions", UNARY)).await.0).await;
        assert_eq!(status, http::StatusCode::TOO_MANY_REQUESTS);
        Ok(())
    }
}
