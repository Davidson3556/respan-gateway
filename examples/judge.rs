//! Use case 5, a plugin that calls a model: a judge reads the prompt, asks a
//! small model whether it is hard, and targets the strong or the cheap model.
//!
//! The judge's call is a subrequest (`cx.call(p).with_input(prompt)`), so it
//! is a call like any other: it inherits the request's extensions (tenant,
//! trace), every plugin's `on_provider` runs before it (egress rules, keys,
//! budgets), and a plugin that meters calls meters it too. The provider edge
//! cannot call, so a subrequest cannot recurse: the depth limit is one.
//!
//! `cargo run -p respan-gateway-examples --example judge`

use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use respan_gateway_core::futures::TryStreamExt;
use respan_gateway_core::{
    ClientContext, Fault, Gateway, Plugin, ProviderContext, Response, async_trait, http, serde_json,
};
use respan_gateway_formats::OpenAiChat;
use respan_gateway_formats::conversation::{Conversation, Message, Reply, Role};
use respan_gateway_testkit::{MockTransport, mock, post, read_whole};

const JUDGE: &str = "http://judge.test/v1";
const CHEAP: &str = "http://cheap.test/v1";
const STRONG: &str = "http://strong.test/v1";

/// Routes each conversation by a small model's verdict on its last message.
struct Judge;

#[async_trait]
impl Plugin for Judge {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let Ok(chat) = cx.input::<Conversation>() else {
            return Ok(None);
        };
        let last = chat.messages.last().map(Message::text).unwrap_or_default();
        let mut prompt = Conversation::new([
            Message::new(
                Role::System,
                "Reply HARD or EASY: how hard is this request?",
            ),
            Message::new(Role::User, last),
        ]);
        prompt.model = Some("judge-mini".into());
        prompt.max_tokens = Some(1);
        let replies: Vec<Reply> = cx
            .call("judge")
            .with_input(prompt)
            .await?
            .outputs()
            .try_collect()
            .await?;
        let verdict: String = replies.iter().flat_map(Reply::texts).collect();
        cx.set_target(if verdict.contains("HARD") {
            "strong"
        } else {
            "cheap"
        });
        Ok(None)
    }
}

/// A provider-edge plugin: it records every call and refuses calls for a
/// blocked tenant, the judge's included.
#[derive(Clone, Default)]
struct Egress(Arc<Mutex<Vec<String>>>);

#[async_trait]
impl Plugin for Egress {
    async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
        self.seen().push(px.provider().to_owned());
        if px.client().head().headers.contains_key("x-blocked") {
            return Err(Fault::new(
                http::StatusCode::FORBIDDEN,
                "egress",
                "no calls for you",
            ));
        }
        Ok(())
    }
}

impl Egress {
    fn seen(&self) -> MutexGuard<'_, Vec<String>> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

fn gateway(mock: &MockTransport, egress: Egress) -> Result<Gateway, Fault> {
    Gateway::new()
        .provider("judge", OpenAiChat, mock.at(JUDGE))?
        .provider("cheap", OpenAiChat, mock.at(CHEAP))?
        .provider("strong", OpenAiChat, mock.at(STRONG))?
        .serve(OpenAiChat)?
        .plugin("egress", egress)?
        .plugin("judge", Judge)?
        .target("cheap")
}

fn answer(text: &str) -> mock::Reply {
    let body = serde_json::json!({"id": "c", "choices": [{"index": 0, "finish_reason": "stop",
        "message": {"role": "assistant", "content": text}}], "usage": {"prompt_tokens": 9, "completion_tokens": 1}});
    mock::json(http::StatusCode::OK, &body.to_string())
}

const REQUEST: &str =
    r#"{"model":"auto","messages":[{"role":"user","content":"Prove the Riemann hypothesis"}]}"#;

#[tokio::main]
async fn main() -> Result<(), Fault> {
    let mock = MockTransport::new();
    mock.reply(JUDGE, answer("HARD"))
        .reply(STRONG, answer("Working on it."));
    let egress = Egress::default();
    let gw = gateway(&mock, egress.clone())?;
    let (status, _, body) =
        read_whole(gw.handle(post("/v1/chat/completions", REQUEST)).await.0).await;
    println!("{status} {}", String::from_utf8_lossy(&body?));
    for sent in mock.sent() {
        println!(
            "called {}: {}",
            sent.uri(),
            String::from_utf8_lossy(sent.body())
        );
    }
    println!("provider edge saw: {:?}", *egress.seen());
    Ok(())
}

#[cfg(test)]
#[path = "tests/judge.rs"]
mod tests;
