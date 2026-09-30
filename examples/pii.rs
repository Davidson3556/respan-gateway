//! Use case 2, PII restored across stream deltas. E-mail addresses are
//! hidden behind placeholders in every call's input as the provider
//! receives it, so no call carries them, whichever plugin makes it or adds
//! to it, and restored on the client edge, where a [`Holdback`] keeps
//! `<EMA` back until `IL_1>` says it is a placeholder.
//!
//! Every text of the request is scanned: messages, the system prompt, stop
//! sequences, the end user's id, tools and earlier tool calls (inside their
//! JSON arguments). What it cannot read, such as an image, is refused, as it
//! might hold PII. A tool call in the answer is restored whether it comes
//! whole or streamed: `Holdback` also holds the tail of each streamed call's
//! arguments (OpenAI's chunks, Anthropic's `partial_json` fragments), so a
//! placeholder split across their fragments is restored, and the restored
//! value is escaped as JSON text. Interleaved OpenAI choices retain separate
//! text and tool-argument tails, so one choice finishing does not release another.
//!
//! So it is two plugins, and order is registration order: `pii` registered
//! first restores every response the client gets, whoever answers;
//! `pii-input` registered last redacts. Register any plugin that adds to the
//! input BEFORE `pii-input`, or its text goes out unredacted; plugins in
//! between see the input before redaction and responses before restoring.
//!
//! `cargo run -p respan-gateway-examples --example pii`

use respan_gateway_core::{
    ClientContext, Fault, Gateway, Plugin, ProviderContext, Response, async_trait,
};
use respan_gateway_formats::conversation::Conversation;
use respan_gateway_http::Body;
use respan_gateway_http::Status;
use respan_gateway_http::http::{self, StatusCode};
use respan_gateway_providers::OpenAiChat;
use respan_gateway_testkit::{MockTransport, mock, post, read_whole, serve};
use respan_gateway_text::{Holdback, Placeholders, Readable};
use serde_json::{self, json};

const OPENAI: &str = "http://openai.test/v1";

/// `text` with every e-mail address hidden in `vault`. A plain word scan;
/// put a real detector here (a regex set for phone numbers and more, an
/// NER model), labelling each find.
fn hide_emails(text: &str, vault: &Placeholders) -> String {
    let words = text.split_inclusive(char::is_whitespace);
    words
        .map(|word| {
            let core = word.trim_matches(|c: char| !c.is_alphanumeric());
            match core.split_once('@') {
                Some((user, domain)) if !user.is_empty() && domain.contains('.') => {
                    word.replacen(core, &vault.hide("EMAIL", core), 1)
                }
                _ => word.to_owned(),
            }
        })
        .collect()
}

/// Every text of `chat` with its e-mail addresses hidden in `vault`; `Err`
/// for what it cannot read.
fn redact(chat: &mut Conversation, vault: &Placeholders) -> Result<(), Fault> {
    chat.edit_all(|text| {
        *text = hide_emails(text, vault);
        Ok(())
    })
}

/// The client edge: one vault per request, restored in every response.
struct Pii;

/// The provider edge: every call's input, redacted into the vault.
struct PiiInput;

const UNCOVERED: &str = "pii_uncovered";

fn uncovered() -> Fault {
    Fault::new(UNCOVERED, "no redaction reads this kind").with(Status(403))
}

#[async_trait]
impl Plugin for Pii {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        if !cx.is_kind::<Conversation>() {
            return Err(uncovered());
        }
        let vault = Placeholders::default();
        cx.filter(Holdback::new(vault.clone()))?;
        cx.extensions_mut().insert(vault);
        Ok(None)
    }
}

#[async_trait]
impl Plugin for PiiInput {
    async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
        if !px.is_kind::<Conversation>() {
            return Err(uncovered());
        }
        let vault = match px.extensions().get::<Placeholders>() {
            Some(vault) => vault.clone(),
            // A call a plugin before `pii` makes: restore its response.
            None => {
                let vault = Placeholders::default();
                px.filter(Holdback::new(vault.clone()))?;
                vault
            }
        };
        px.filter(move |chat: &mut Conversation| redact(chat, &vault))
    }
}

/// `pii` first, `pii-input` last, and `middle` between them. For real use,
/// the transport is
/// `HttpTransport::new("https://api.openai.com/v1")?.with_headers(key)?`.
fn gateway_with(
    mock: &MockTransport,
    middle: impl FnOnce(Gateway) -> Result<Gateway, Fault>,
) -> Result<Gateway, Fault> {
    let gw = Gateway::new()
        .provider::<Conversation, _, _>(
            "openai",
            OpenAiChat,
            respan_gateway_http::connect(mock.at(OPENAI)),
        )?
        .client::<Conversation, _, _>(OpenAiChat)?
        .plugin("pii", Pii)?;
    middle(gw)?.plugin("pii-input", PiiInput)?.target("openai")
}

fn gateway(mock: &MockTransport) -> Result<Gateway, Fault> {
    gateway_with(mock, Ok)
}

/// A streamed chat of one user message, with `more` fields.
fn chat(content: serde_json::Value, more: serde_json::Value) -> http::Request<Body> {
    let mut body = json!({ "model": "gpt", "stream": true,
        "messages": [{ "role": "user", "content": content }] });
    for (key, value) in more.as_object().into_iter().flatten() {
        body[key] = value.clone();
    }
    post("/v1/chat/completions", body.to_string())
}

/// The provider streams `deltas` as OpenAI chunks.
fn deltas(deltas: &[&str]) -> mock::Reply {
    let mut chunks: Vec<String> = deltas
        .iter()
        .map(|d| {
            let chunk = json!({ "id": "c1", "model": "gpt",
                "choices": [{ "index": 0, "delta": { "content": d } }] });
            format!("data: {chunk}\n\n")
        })
        .collect();
    chunks.push("data: [DONE]\n\n".into());
    mock::sse(&chunks.iter().map(String::as_str).collect::<Vec<_>>())
}

/// The status and the text the client got, and what the provider was sent.
async fn ask(
    mock: &MockTransport,
    req: http::Request<Body>,
) -> Result<(StatusCode, String, String), Fault> {
    ask_via(&gateway(mock)?, mock, req).await
}

async fn ask_via(
    gw: &Gateway,
    mock: &MockTransport,
    req: http::Request<Body>,
) -> Result<(StatusCode, String, String), Fault> {
    let (status, _, body) = read_whole(serve(gw, req).await.0).await;
    let body = String::from_utf8_lossy(&body?).into_owned();
    let data = body.lines().filter_map(|l| l.strip_prefix("data: "));
    let json = data.filter_map(|d| serde_json::from_str::<serde_json::Value>(d).ok());
    let text = json.filter_map(|v| {
        v["choices"][0]["delta"]["content"]
            .as_str()
            .map(str::to_owned)
    });
    let text = if status.is_success() {
        text.collect()
    } else {
        body
    };
    let sent = mock.sent().last().map(|r| r.body().to_vec());
    let sent = String::from_utf8(sent.unwrap_or_default()).unwrap_or_default();
    Ok((status, text, sent))
}

const PROMPT: &str = "Mail bob@x.io, or <alice@y.org>.";

#[tokio::main]
async fn main() -> Result<(), Fault> {
    let mock = MockTransport::new();
    mock.reply(OPENAI, deltas(&["Sure: <EMA", "IL_", "1> and <EMAIL_2>."]));
    let (status, text, sent) = ask(&mock, chat(PROMPT.into(), json!({}))).await?;
    println!("provider got: {sent}\nclient got:   {status} {text}");
    Ok(())
}

#[cfg(test)]
#[path = "tests/pii.rs"]
mod tests;
