//! Use case 5, a plugin that calls a model: a judge reads the prompt, asks a
//! small model whether it is hard, and targets the strong or the cheap model.
//!
//! The judge's call is a subrequest (`cx.call(p).with_input(prompt)`), so it
//! is a call like any other: it inherits the request's extensions (tenant,
//! trace), every plugin's `on_provider` runs before it (egress rules, keys,
//! budgets), and a plugin that meters calls meters it too. `cx.call` runs
//! provider hooks without rerunning `Judge::on_client`, so this judge's
//! subrequest does not recursively invoke the judge.
//!
//! `cargo run -p respan-gateway-examples --example judge`

use parking_lot::{Mutex, MutexGuard};
use respan_gateway_core::ErrorCode;
use std::sync::Arc;

use mockllm::{Answer, FakeOpenAi};
use respan_gateway_conversation::{Assembled, Input, Message, Output, Role};
use respan_gateway_conversation_http::openai::OpenAiChat;
use respan_gateway_core::futures::TryStreamExt;
use respan_gateway_core::{
    ClientContext, Fault, Gateway, Plugin, Provider, ProviderContext, Response, async_trait,
};
#[cfg(test)]
use respan_gateway_http::http;
use respan_gateway_http::raw::Original;
use respan_gateway_http_faults::Status;
#[allow(dead_code)]
#[path = "../support/request.rs"]
mod http_support;
#[allow(dead_code)]
#[path = "../support/http.rs"]
mod provider_support;
use http_support::{post, read_whole, serve};

/// The models the tiers serve, which replace the client's `auto`.
const STRONG_MODEL: &str = "strong-large";
const CHEAP_MODEL: &str = "cheap-small";

/// Routes each conversation by a small model's verdict on its last message.
struct Judge;

#[async_trait]
impl Plugin for Judge {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let Ok(chat) = cx.input::<Input>() else {
            return Ok(None);
        };
        let last = chat.messages.last().map(Message::text).unwrap_or_default();
        let mut prompt = Input::new([
            Message::new(
                Role::System,
                "Output HARD or EASY: how hard is this request?",
            ),
            Message::new(Role::User, last),
        ]);
        prompt.model = Some("judge-mini".into());
        prompt.options.max_output_tokens = Some(1);
        let replies: Vec<Output> = cx
            .call("judge")
            .with_input(prompt)
            .with_extension(respan_gateway_conversation_http::StreamResponse(false))
            .await?
            .outputs()
            .try_collect()
            .await?;
        let mut answer = Assembled::default();
        replies.iter().try_for_each(|reply| answer.push(reply))?;
        let (target, model) = match answer.text().contains("HARD") {
            true => ("strong", STRONG_MODEL),
            false => ("cheap", CHEAP_MODEL),
        };
        cx.set_target(target);
        cx.filter(move |chat: &mut Input| {
            chat.model = Some(model.into());
            Ok(())
        })?;
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
        let asked = px.extensions().get::<Original>().and_then(Original::get);
        if asked.is_some_and(|a| a.headers().contains_key("x-blocked")) {
            return Err(Fault::new("no calls for you")
                .with(ErrorCode("egress".into()))
                .with(Status(respan_gateway_http::http::StatusCode::FORBIDDEN)));
        }
        Ok(())
    }
}

impl Egress {
    fn seen(&self) -> MutexGuard<'_, Vec<String>> {
        self.0.lock()
    }
}

fn gateway(
    judge: &FakeOpenAi,
    cheap: &FakeOpenAi,
    strong: &FakeOpenAi,
    egress: Egress,
) -> Result<Gateway, Fault> {
    Gateway::new()
        .provider(Provider::new(
            "judge",
            OpenAiChat,
            provider_support::openai(&judge.base_url())?.connect(),
        ))?
        .provider(Provider::new(
            "cheap",
            OpenAiChat,
            provider_support::openai(&cheap.base_url())?.connect(),
        ))?
        .provider(Provider::new(
            "strong",
            OpenAiChat,
            provider_support::openai(&strong.base_url())?.connect(),
        ))?
        .client::<Input, _, _>(OpenAiChat)?
        .plugin(egress)
        .plugin(Judge)
        .target("cheap")
}

fn answer(text: &str) -> Answer {
    let body = serde_json::json!({"id": "c", "choices": [{"index": 0, "finish_reason": "stop",
        "message": {"role": "assistant", "content": text}}], "usage": {"prompt_tokens": 9, "completion_tokens": 1}});
    Answer::raw(body.to_string())
}

const REQUEST: &str =
    r#"{"model":"auto","messages":[{"role":"user","content":"Prove the Riemann hypothesis"}]}"#;

#[tokio::main]
async fn main() -> Result<(), Fault> {
    let (judge, cheap, strong) = tokio::join!(
        FakeOpenAi::start(),
        FakeOpenAi::start(),
        FakeOpenAi::start()
    );
    judge.answer(answer("HARD"));
    strong.answer(answer("Working on it."));
    let egress = Egress::default();
    let gw = gateway(&judge, &cheap, &strong, egress.clone())?;
    let (status, _, body) =
        read_whole(serve(&gw, post("/v1/chat/completions", REQUEST)).await.0).await;
    println!("{status} {}", String::from_utf8_lossy(&body?));
    for (name, fake) in [("judge", &judge), ("strong", &strong)] {
        for sent in fake.requests() {
            println!(
                "called {name}{}: {}",
                sent.path,
                String::from_utf8_lossy(&sent.raw)
            );
        }
    }
    println!("provider edge saw: {:?}", *egress.seen());
    Ok(())
}
