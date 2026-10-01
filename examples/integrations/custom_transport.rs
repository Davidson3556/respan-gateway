//! A model in this process, reached as a provider: a [`Format`] that only
//! dials, its I/O value the shared model. No bytes are written or read: it
//! takes the call's [`Conversation`] and answers with [`Reply`]s, which any
//! client of the conversation kind (OpenAI Chat, Anthropic Messages) gets in
//! its own protocol, JSON or SSE as it asked. It is a full provider, so it
//! can be a target or a fallback, and every provider-edge plugin (billing,
//! limits, guards) sees its calls.
//!
//! A format that keeps the client edge's defaults is dial-only.
//!
//! `cargo run -p respan-gateway-examples --example custom_transport`

use std::sync::Arc;

use respan_gateway_providers::OpenAiChat;
use respan_gateway_sdk::Usage;
use respan_gateway_sdk::conversation::{Conversation, Finish, Reply, units};
use respan_gateway_sdk::futures::FutureExt;
use respan_gateway_sdk::futures::channel::oneshot;
use respan_gateway_sdk::futures::{sink, stream};
use respan_gateway_sdk::{Connection, EventSink, EventStream, Fault, Format, Gateway};
#[allow(dead_code)]
#[path = "../../tests/support/http.rs"]
mod http_support;
use http_support::{post, read_whole, serve};

/// Your model: a conversation in, its answer out.
type Model = Arc<dyn Fn(&Conversation) -> Result<String, Fault> + Send + Sync>;

/// A toy model that echoes the last message.
fn echo(chat: &Conversation) -> Result<String, Fault> {
    let last = chat.messages.last().map(|m| m.text()).unwrap_or_default();
    Ok(format!("you said: {last}"))
}

/// A model in this process: each provider attempt receives a model handle.
struct Local;

impl Format<Model, Reply, Conversation> for Local {
    fn name(&self) -> &str {
        "local"
    }

    fn bind(&self, model: Model) -> Result<Connection<Reply, Conversation>, Fault> {
        let (send, receive) = oneshot::channel();
        let incoming = async move { receive.await.map_err(|_| Fault::abandoned()) };
        let outgoing = EventSink::preparing(move |flow: &mut EventStream<Conversation>| {
            let chat = flow
                .value()
                .transpose()?
                .ok_or_else(|| Fault::bad_request("no conversation"))?;
            let answer = model(chat)?;

            // One reply per word, then the finish with usage (a word is a token here).
            let words = |s: &str| s.split_whitespace().count() as f64;
            let mut usage = Usage::default();
            let prompt = chat.messages.iter().map(|m| words(&m.text())).sum();
            usage.add(units::INPUT_TOKENS, prompt);
            usage.add(units::OUTPUT_TOKENS, words(&answer));
            let model = chat.model.clone().unwrap_or_else(|| "local".into());
            let mut replies = vec![Reply::start("local-1", model)];
            // Each word keeps the space before the next: "you ", "said: ", ...
            replies.extend(answer.split_inclusive(' ').map(|w| Reply::text(0, w)));
            let mut last = Reply::finished(Finish::Stop);
            last.usage = usage;
            replies.push(last);
            let outputs = EventStream::stream(stream::iter(replies.into_iter().map(Ok)));
            Ok(sink::unfold(
                Some((send, outputs)),
                |state, item: Result<Conversation, Fault>| async move {
                    item?;
                    if let Some((send, outputs)) = state {
                        let _ = send.send(outputs);
                    }
                    Ok::<_, Fault>(None)
                },
            ))
        });
        Ok((incoming.boxed(), outgoing))
    }
}

fn gateway() -> Result<Gateway, Fault> {
    let model: Model = Arc::new(echo);
    Gateway::new()
        .provider::<Conversation, _, _>("local", Local, move |_| model.clone())?
        .client::<Conversation, _, _>(OpenAiChat)?
        .target("local")
}

const UNARY: &str = r#"{"model":"echo","messages":[{"role":"user","content":"hello there"}]}"#;
const STREAM: &str = r#"{"model":"echo","stream":true,"stream_options":{"include_usage":true},"messages":[{"role":"user","content":"hello there"}]}"#;

#[tokio::main]
async fn main() -> Result<(), Fault> {
    let gw = gateway()?;
    for req in [UNARY, STREAM] {
        let (status, _, body) =
            read_whole(serve(&gw, post("/v1/chat/completions", req)).await.0).await;
        println!("{status}\n{}\n", String::from_utf8_lossy(&body?));
    }
    Ok(())
}

#[cfg(test)]
#[path = "../tests/custom_transport.rs"]
mod tests;
