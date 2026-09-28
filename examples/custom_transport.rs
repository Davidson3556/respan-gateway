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
use respan_gateway_core::http_body::Frame;
use respan_gateway_core::http_body_util::{BodyExt, StreamBody};
use respan_gateway_core::{Body, Fault, Format, Gateway, Transport, async_trait, http};
use respan_gateway_formats::conversation::{Conversation, Finish, Reply, units};
use respan_gateway_formats::{OpenAiChat, Usage};
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
        let chat = read_chat(&self.format, &call)?;
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
        write_replies(&self.format, &call, &replies)
    }
}

/// The call's conversation, read with the format's request decoder.
fn read_chat<F: Format<Input = Conversation>>(
    format: &F,
    call: &http::Request<Bytes>,
) -> Result<Conversation, Fault> {
    let mut decoder = format.request_decoder(call);
    let mut chats = Vec::new();
    decoder.decode(call.body().clone(), &mut chats)?;
    decoder.end(&mut chats)?;
    chats
        .pop()
        .ok_or_else(|| Fault::bad_request("no conversation"))
}

/// The replies as a response, written with the format's response encoder,
/// which also sets the content type: SSE if the call asked to stream.
fn write_replies<F: Format<Output = Reply>>(
    format: &F,
    call: &http::Request<Bytes>,
    replies: &[Reply],
) -> Result<http::Response<Body>, Fault> {
    let (mut head, ()) = http::Response::new(()).into_parts();
    let mut encoder = format.response_encoder(&mut head, call);
    let mut frames = Vec::new();
    let mut out = BytesMut::new();
    encoder.open(&mut out);
    for reply in replies {
        encoder.encode(reply, &mut out)?;
        frames.push(out.split().freeze());
    }
    encoder.end(&Ok(()), &mut out)?;
    frames.push(out.freeze());
    let frames = frames.into_iter().filter(|f| !f.is_empty());
    let body = stream::iter(frames.map(|f| Ok(Frame::data(f))));
    Ok(http::Response::from_parts(
        head,
        StreamBody::new(body).boxed_unsync(),
    ))
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
#[path = "tests/custom_transport.rs"]
mod tests;
