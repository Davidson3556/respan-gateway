//! Use case 2, PII restored across stream deltas. E-mail addresses are
//! hidden behind placeholders on the provider edge, so no call carries them,
//! whichever plugin makes it, and restored on the client edge, where a
//! [`Holdback`] keeps `<EMA` back until `IL_1>` says it is a placeholder.
//! Plugins after this one see only placeholders, both ways.
//!
//! Every text of the request is scanned: messages, the system prompt, stop
//! sequences and the end user's id. What it cannot read, such as an image,
//! is refused, as it might hold PII. A placeholder the model writes into a
//! tool call's arguments is not restored: the kind carries those as other
//! content.
//!
//! `cargo run -p respan-gateway-examples --example pii`

use respan_gateway_core::http::{self, StatusCode};
use respan_gateway_core::serde_json::{self, json};
use respan_gateway_core::{
    Body, ClientContext, Fault, Gateway, Plugin, ProviderContext, Response, async_trait,
};
use respan_gateway_formats::OpenAiChat;
use respan_gateway_formats::conversation::Conversation;
use respan_gateway_testkit::{MockTransport, mock, post, read_whole};
use respan_gateway_text::{Holdback, Placeholders, Readable};

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
    for text in chat.read_all_mut()? {
        *text = hide_emails(text, vault);
    }
    Ok(())
}

struct Pii;

fn uncovered() -> Fault {
    Fault::new(
        StatusCode::FORBIDDEN,
        "pii_uncovered",
        "no redaction reads this kind",
    )
}

#[async_trait]
impl Plugin for Pii {
    /// One vault per request, restored in every response the client gets.
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        if !cx.is_kind::<Conversation>() {
            return Err(uncovered());
        }
        let vault = Placeholders::default();
        cx.filter(Holdback::new(vault.clone()))?;
        cx.head_mut().extensions.insert(vault);
        Ok(None)
    }

    /// Every call's input, redacted into the request's vault.
    async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
        if !px.is_kind::<Conversation>() {
            return Err(uncovered());
        }
        let vault = match px.extensions().get::<Placeholders>() {
            Some(vault) => vault.clone(),
            // A call a plugin before this one makes: restore its response.
            None => {
                let vault = Placeholders::default();
                px.filter(Holdback::new(vault.clone()))?;
                vault
            }
        };
        px.filter(move |chat: &mut Conversation| redact(chat, &vault))
    }
}

/// For real use, the transport is
/// `HttpTransport::new("https://api.openai.com/v1")?.with_headers(key)?`.
fn gateway(mock: &MockTransport) -> Result<Gateway, Fault> {
    Gateway::new()
        .provider("openai", OpenAiChat, mock.at(OPENAI))?
        .serve(OpenAiChat)?
        .plugin("pii", Pii)?
        .target("openai")
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
    let (status, _, body) = read_whole(gateway(mock)?.handle(req).await.0).await;
    let body = String::from_utf8_lossy(&body?).into_owned();
    let data = body.lines().filter_map(|l| l.strip_prefix("data: "));
    let json = data.filter_map(|d| serde_json::from_str::<serde_json::Value>(d).ok());
    let text = json.filter_map(|v| {
        v["choices"][0]["delta"]["content"]
            .as_str()
            .map(str::to_owned)
    });
    let text = match status.is_success() {
        true => text.collect(),
        false => body,
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
mod tests {
    use super::*;

    #[test]
    fn hides_addresses_only() {
        let vault = Placeholders::default();
        let text = "bob@x.io, (a@b.co) or 42; not a@b, @x.io or x.io.";
        assert_eq!(
            hide_emails(text, &vault),
            "<EMAIL_1>, (<EMAIL_2>) or 42; not a@b, @x.io or x.io."
        );
    }

    #[tokio::test]
    async fn a_placeholder_split_across_three_deltas_is_restored() -> Result<(), Fault> {
        let mock = MockTransport::new();
        mock.reply(OPENAI, deltas(&["Sure: <EMA", "IL_", "1> and <EMAIL_2>."]));
        let (status, text, sent) = ask(&mock, chat(PROMPT.into(), json!({}))).await?;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(text, "Sure: bob@x.io and alice@y.org.");
        assert!(sent.contains("Mail <EMAIL_1>, or <<EMAIL_2>>."), "{sent}");
        assert!(!sent.contains('@'), "{sent}");
        Ok(())
    }

    #[tokio::test]
    async fn stop_sequences_and_the_user_id_are_redacted_too() -> Result<(), Fault> {
        let mock = MockTransport::new();
        mock.reply(OPENAI, deltas(&["ok"]));
        let more = json!({ "stop": ["bob@x.io"], "user": "carol@z.io" });
        let (status, _, sent) = ask(&mock, chat("hi".into(), more)).await?;
        assert_eq!(status, StatusCode::OK);
        let sent: serde_json::Value = serde_json::from_str(&sent).unwrap_or_default();
        assert_eq!(
            (&sent["stop"], &sent["user"]),
            (&json!(["<EMAIL_1>"]), &json!("<EMAIL_2>"))
        );
        Ok(())
    }

    #[tokio::test]
    async fn what_it_cannot_read_is_refused() -> Result<(), Fault> {
        let mock = MockTransport::new();
        let image = json!([{ "type": "image_url", "image_url": { "url": "data:," } }]);
        let (status, body, _) = ask(&mock, chat(image, json!({}))).await?;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
        assert!(mock.sent().is_empty());
        Ok(())
    }
}
