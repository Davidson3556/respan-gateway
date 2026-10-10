//! Use case 14, the gateway as a library: another Rust service runs the
//! pipeline in-process, with no server and no second hop. It builds a
//! [`Gateway`], hands [`Gateway::handle`] a request on the HTTP wire (an
//! owned `ClientIo` native HTTP request and its whole body), reads the answer
//! and awaits how the request ended. With no HTTP hop, it needs none of the
//! adapter's hop rules (`respan_gateway_http::handle` applies them), but
//! hands the gateway what the `ClientIo` publishes, as the adapter does
//! (`ClientIo::extensions`); no answer means no format took the request,
//! and the end says why.
//!
//! The kernel counts no usage:
//! the embedder tallies it with a plugin that sums what each call's
//! provider reports (`Meter`), as billing would.
//!
//! `cargo run -p respan-gateway-examples --example embedded`

use mockllm::{Answer, FakeOpenAi};
use respan_gateway_conversation::Input;
use respan_gateway_conversation_http::openai::OpenAiChat;
use respan_gateway_core::futures::future::join;
use respan_gateway_core::futures::{StreamExt, stream};
use respan_gateway_core::{Fault, Gateway};
use respan_gateway_examples::LIMIT;
use respan_gateway_examples::Meter;
use respan_gateway_http::bytes::Bytes;
use respan_gateway_http::http::HeaderValue;
use respan_gateway_http::http_body_util::BodyExt;
use respan_gateway_http::{HttpTransport, http};

/// The gateway, with no server. For real use, the transport is
/// `HttpTransport::new("https://api.openai.com/v1")?.with_headers(key)?`.
fn gateway(
    provider: impl Fn(respan_gateway_core::Extensions) -> respan_gateway_http::ProviderIo
    + Send
    + Sync
    + 'static,
    tally: Meter,
) -> Result<Gateway, Fault> {
    Gateway::new()
        .provider::<Input, _, _>("openai", OpenAiChat::OPENAI, provider)?
        .client::<Input, _, _>(OpenAiChat::OPENAI)?
        .plugin(tally)
        .target("openai")
}

fn scripted_provider(openai: &FakeOpenAi) -> Result<HttpTransport, Fault> {
    let mut headers = http::HeaderMap::new();
    headers.insert("authorization", HeaderValue::from_static("Bearer sk-test"));
    HttpTransport::new(openai.base_url())?.with_headers(headers)
}

/// One chat request through the pipeline: the answer's status and body,
/// and how the request ended.
async fn ask(
    gw: &Gateway,
    prompt: &str,
) -> Result<(http::StatusCode, Bytes, Result<(), Fault>), Fault> {
    let body = serde_json::json!({
        "model": "gpt-4o-mini",
        "messages": [{"role": "user", "content": prompt}],
    });
    let (head, ()) = http::Request::post("/v1/chat/completions")
        .header("content-type", "application/json")
        .body(())
        .map_err(respan_gateway_http::built_wrong)?
        .into_parts();
    let body = stream::iter([Ok(Bytes::from(body.to_string()))]).boxed();
    let request = http::Request::from_parts(head, respan_gateway_http::Body::stream(body));
    let (incoming, answer) =
        respan_gateway_http::ClientIo::accept(request, respan_gateway_http::Framing::Whole, LIMIT);
    let extensions = incoming.extensions();
    let handled = gw.handle(incoming, extensions);
    let consume = async {
        let Some((resp, end)) = answer.await else {
            return Ok::<_, Fault>(None);
        };
        let (head, body) = resp.into_parts();
        let body = BodyExt::collect(body).await?.to_bytes();
        Ok(Some((head.status, body, end.await)))
    };
    // The handle includes delivery, so drive it while consuming the reply.
    let (handled, answer) = join(handled, consume).await;
    answer?.ok_or_else(|| {
        handled
            .err()
            .unwrap_or_else(|| Fault::internal("no format answered"))
    })
}

#[tokio::main]
async fn main() -> Result<(), Fault> {
    let openai = FakeOpenAi::start().await;
    openai.answer(Answer::raw(ANSWER));
    let tally = Meter::default();
    let gw = gateway(scripted_provider(&openai)?.connect(), tally.clone())?;
    let (status, body, end) = ask(&gw, "Say hi").await?;
    // End hooks run detached; wait for them before reading the tally.
    gw.idle().await;
    println!("status: {status}, ended: {:?}", end);
    println!("body:   {}", String::from_utf8_lossy(&body));
    println!("usage:  {:?}", tally.usage());
    Ok(())
}

const ANSWER: &str = r#"{"id":"c1","object":"chat.completion","model":"gpt-4o-mini","choices":[{"index":0,"finish_reason":"stop","message":{"role":"assistant","content":"hi"}}],"usage":{"prompt_tokens":3,"completion_tokens":1}}"#;

#[cfg(test)]
#[path = "../tests/cases/hosting/embedded.rs"]
mod tests;
