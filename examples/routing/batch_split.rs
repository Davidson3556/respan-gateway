//! Use case 4, one request, many upstream calls: an embeddings batch over a
//! provider's cap is split into calls of at most `cap` inputs, sent
//! concurrently, and answered as one: the vectors in the client's order (a
//! call's vector `i` is input `i` of its chunk, and the chunks are joined in
//! order, so the merged index is the original one) and the usage of every
//! call summed. A batch within the cap is not split: it is sent whole.
//!
//! Each chunk is a subrequest, `cx.call(p).with_input(chunk)`, so it is a
//! call like any other: every plugin's `on_provider` runs before it, and a
//! plugin that meters calls sees each one. A call built with `with_input` is
//! not a relay, so the client's key does not go with it: the provider has
//! its own key (`HttpTransport::new(url)?.with_headers(key)?`), or set the
//! call's `CallHeaders` from the request's `Credential`.
//!
//! The embeddings kind and its format are `custom_format`'s, included as a
//! module.
//!
//! At most eight chunks run concurrently. Each retries transient failures
//! up to three times before any merged response is committed. A permanent
//! failure or exhausted retries fails the request, with no partial answer.
//!
//! `cargo run -p respan-gateway-examples --example batch_split`

// This example includes custom_format, which includes its own test helpers.
#![allow(clippy::duplicate_mod)]

use mockllm::{Answer, FakeOpenAi};
use respan_gateway_core::Upstream;
use respan_gateway_core::futures::{StreamExt, TryStreamExt, stream};
use respan_gateway_core::{ClientContext, Fault, Gateway, Plugin, Response, async_trait};
#[cfg(test)]
use respan_gateway_http::http::StatusCode;
use respan_gateway_http::retryable;
use respan_gateway_plugins::routing::{Backoff, Until, first_success};
#[allow(dead_code)]
#[path = "../../tests/support/http.rs"]
mod http_support;
use http_support::{post, read_whole, serve};
#[allow(dead_code)]
#[path = "../support/http.rs"]
mod provider_support;
use std::time::Duration;

#[allow(dead_code)]
#[path = "../integrations/custom_format.rs"]
mod embeddings;
use embeddings::{EmbedRequest, Embeddings, OpenAiEmbeddings};

/// Splits a batch over `.0` inputs into concurrent calls to the target.
struct Split(usize);

#[async_trait]
impl Plugin for Split {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let Ok(request) = cx.input::<EmbedRequest>().cloned() else {
            return Ok(None);
        };
        let Some(target) = cx.target().map(str::to_owned) else {
            return Ok(None);
        };
        if request.input.len() <= self.0 {
            return Ok(None);
        }
        let calls: Vec<_> = request
            .input
            .chunks(self.0)
            .map(|chunk| {
                let mut part = request.clone();
                part.input = chunk.to_vec();
                (chunk.len(), cx.call(target.clone()).with_input(part))
            })
            .collect();
        let answers = stream::iter(calls)
            .map(|(expected, call)| async move {
                let backoff = Backoff::new(3, Duration::from_millis(10), Duration::from_secs(1));
                let answers = first_success([call], &backoff, Until::End, retryable)
                    .await?
                    .outputs::<Embeddings>()
                    .try_collect::<Vec<_>>()
                    .await?;
                if answers.len() != 1 || answers[0].vectors.len() != expected {
                    return Err(Upstream::fault("the provider omitted embedding vectors"));
                }
                Ok(answers)
            })
            .buffered(8)
            .try_collect::<Vec<_>>()
            .await?;
        let mut merged = Embeddings::default();
        for answer in answers.iter().flatten() {
            merged.vectors.extend(answer.vectors.iter().cloned());
            merged.usage += &answer.usage;
        }
        Ok(Some(Response::stream(stream::iter([Ok(merged)]))))
    }
}

const MODEL: &str = "text-embedding-3-small";

fn gateway(
    provider: impl Fn(respan_gateway_core::Extensions) -> respan_gateway_http::ProviderIo
    + Send
    + Sync
    + 'static,
    cap: usize,
) -> Result<Gateway, Fault> {
    if cap == 0 {
        return Err(Fault::config("the embedding batch cap must be positive"));
    }
    Gateway::new()
        .provider::<EmbedRequest, _, _>("openai", OpenAiEmbeddings, provider)?
        .client::<EmbedRequest, _, _>(OpenAiEmbeddings)?
        .plugin(Split(cap))
        .target("openai")
}

/// An answer with a vector `[n]` for each of `firsts`, using `tokens`.
fn answer(firsts: &[f64], tokens: u64) -> Answer {
    let data: Vec<_> = firsts
        .iter()
        .enumerate()
        .map(|(i, n)| serde_json::json!({ "index": i, "embedding": [n] }))
        .collect();
    let body = serde_json::json!({ "object": "list", "data": data, "model": MODEL,
        "usage": { "prompt_tokens": tokens, "total_tokens": tokens } });
    Answer::raw(body.to_string())
}

#[tokio::main]
async fn main() -> Result<(), Fault> {
    let openai = FakeOpenAi::start().await;
    for scripted in [
        answer(&[0.0, 1.0], 2),
        answer(&[2.0, 3.0], 2),
        answer(&[4.0], 1),
    ] {
        openai.answer(scripted);
    }
    let gw = gateway(provider_support::openai(&openai.base_url())?.connect(), 2)?;
    let body = r#"{"model":"text-embedding-3-small","input":["a","b","c","d","e"]}"#;
    let (status, _, answered) = read_whole(serve(&gw, post("/v1/embeddings", body)).await.0).await;
    println!("{status} {}", String::from_utf8_lossy(&answered?));
    let requests = openai.requests();
    assert_eq!(requests.len(), 3);
    for sent in requests {
        println!("call: {}", String::from_utf8_lossy(&sent.raw));
    }
    Ok(())
}

#[cfg(test)]
#[path = "../tests/cases/routing/batch_split.rs"]
mod tests;
