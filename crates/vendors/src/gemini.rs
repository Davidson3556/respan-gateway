//! Google's Gemini API: Chat Completions, images and embeddings at its
//! OpenAI-compatible base, and its own API beside it: `generateContent`,
//! Gemini Live and Veo.

use respan_gateway_conversation::{Content, Input, Role};
use respan_gateway_conversation_http::Schema;
use respan_gateway_core::{NativeFields, Plugin, ProviderContext, Result, async_trait};
use serde_json::{Map, Value, json};

use respan_gateway_conversation_http::gemini::GeminiGenerate;
use respan_gateway_conversation_http::openai::OpenAiChat;
use respan_gateway_media_http::gemini::GeminiVeo;
use respan_gateway_media_http::openai::{OpenAiEmbeddings, OpenAiImages};
use respan_gateway_realtime_ws::GeminiLive;

use crate::{
    Base, Body, BoxFuture, Model, Models, Quirk, Speaks, Vendor, VendorProvider, built_wrong, chat,
    http, http::Uri, json as body_json, models,
};

/// The header its own API takes the key in.
const KEY: &str = "x-goog-api-key";

/// Google's Gemini API. Its own API is its OpenAI-compatible base without
/// `/openai`; Live's endpoint is at its origin. Its own `generateContent`
/// comes first, so a client of another format is translated to it, which
/// says what Chat Completions cannot, such as reasoning; a client of Chat
/// Completions is sent to its OpenAI-compatible endpoint as it is.
pub const VENDOR: Vendor = Vendor {
    id: "gemini",
    base_url: Some("https://generativelanguage.googleapis.com/v1beta/openai"),
    key_env: &["GEMINI_API_KEY", "GOOGLE_API_KEY"],
    formats: &[
        &Speaks::http(GeminiGenerate)
            .key_in(KEY)
            .at(Base::Without("/openai")),
        &Speaks::http(OpenAiChat),
        &Speaks::ws(GeminiLive, |base| GeminiLive.transport(base))
            .key_in(KEY)
            .at(Base::Origin),
        &Speaks::http(OpenAiImages),
        &Speaks::http(GeminiVeo)
            .follow(google_storage)
            .key_in(KEY)
            .at(Base::Without("/openai")),
        &Speaks::http(OpenAiEmbeddings),
    ],
    models: list_models,
    quirks: &[
        Quirk {
            name: "model_id_prefix",
            description: "Its model list names each model `models/<id>`.",
        },
        Quirk {
            name: "thought_signature",
            description: "A Gemini 3 model refuses a function call in a request's history that \
                          has no signature (`thoughtSignature`, or over Chat Completions the \
                          call's `extra_content`), as one from another format has none; it takes \
                          its documented placeholder in place of one.",
        },
        Quirk {
            name: "extra_content",
            description: "Over Chat Completions, a Gemini 3 reply carries its thought signature \
                          in the message's `extra_content`, which only Gemini reads back: a \
                          client of another format goes without it.",
        },
    ],
    plugin: Some(&GeminiQuirks),
};

/// What Gemini 3 models accept as the signature of a function call made
/// elsewhere, as its documentation gives.
pub const SIGNATURE_PLACEHOLDER: &str = "skip_thought_signature_validator";

/// Gemini's quirks: a function call in the history needs a thought
/// signature, and a reply's signature over Chat Completions is Gemini's
/// alone.
#[derive(Debug)]
pub struct GeminiQuirks;

#[async_trait]
impl Plugin for GeminiQuirks {
    async fn on_provider(&self, px: &mut ProviderContext) -> Result<()> {
        // Where each format of Gemini's takes a call's signature.
        let (format, key, signature) = match px.format() {
            f if f == GeminiGenerate::NAME => (
                GeminiGenerate::NAME,
                "thoughtSignature",
                json!(SIGNATURE_PLACEHOLDER),
            ),
            f if f == OpenAiChat::NAME => (
                OpenAiChat::NAME,
                "extra_content",
                json!({ "google": { "thought_signature": SIGNATURE_PLACEHOLDER } }),
            ),
            _ => return Ok(()),
        };
        if format == OpenAiChat::NAME {
            chat::inert_message(px, &["extra_content"])?;
        }
        if !px.is_kind::<Input>() {
            return Ok(());
        }
        px.filter(move |input: &mut Input| {
            // Models before Gemini 3 take no signature in the request.
            let older = input.model.as_deref().is_some_and(|m| {
                let m = m.strip_prefix("models/").unwrap_or(m);
                m.starts_with("gemini-1") || m.starts_with("gemini-2")
            });
            if !older {
                sign_calls(input, format, key, &signature);
            }
            Ok(())
        })
    }
}

/// Signs, with `signature` at `key` of `format`'s fields, the first call of
/// each assistant message that has no signature of its own, as Gemini
/// returns one only on the first of a turn's parallel calls.
fn sign_calls(input: &mut Input, format: &'static str, key: &str, signature: &Value) {
    // Another format's fields a translation drops anyway, as a client's
    // item id or cache marker, give way to the signature.
    const DROPPED: &[&str] = &["inert", "cache_control", "thoughtSignature"];
    let own = |native: &mut Option<NativeFields>| match native {
        Some(n) if &*n.format != format => {
            let dropped = n.fields.keys().all(|k| DROPPED.contains(&k.as_str()));
            if dropped {
                *native = None;
            }
            dropped
        }
        _ => true,
    };
    for message in input
        .messages
        .iter_mut()
        .filter(|m| m.role == Role::Assistant)
    {
        let first = message.content.iter_mut().find_map(|part| match part {
            Content::ToolCall { native, .. } => Some(native),
            _ => None,
        });
        let Some(native) = first else {
            continue;
        };
        if !own(native) {
            continue;
        }
        let fields = native
            .get_or_insert_with(|| NativeFields::new(format, Map::new()))
            .fields
            .entry(key);
        fields.or_insert_with(|| signature.clone());
    }
}

/// Its models, as its own API lists them (`{"models": [{"name", ..}]}`),
/// each id without the `models/` it puts before it; as OpenAI lists them for
/// a provider of its OpenAI-compatible formats alone.
fn list_models(p: &VendorProvider) -> BoxFuture<'_, Models> {
    Box::pin(async move {
        // Its own API's base and key, as either format of it takes them.
        let own = match p.client_of(GeminiGenerate::NAME)? {
            Some(own) => Some(own),
            None => p.client_of(GeminiVeo::NAME)?,
        };
        let Some(own) = own else {
            return models::openai(p).await;
        };
        let call = http::Request::get("/models?pageSize=1000").body(Body::default());
        let (answer, _) = own.send(call.map_err(built_wrong)?).await?;
        let answer = body_json(answer).await?;
        let models = answer["models"].as_array().into_iter().flatten();
        let model = |m: &Value| {
            let name = m["name"].as_str()?;
            let mut model = Model::new(name.strip_prefix("models/").unwrap_or(name));
            model.display_name = m["displayName"].as_str().map(str::to_owned);
            model.description = m["description"].as_str().map(str::to_owned);
            model.context_window = m["inputTokenLimit"].as_u64();
            model.max_output_tokens = m["outputTokenLimit"].as_u64();
            if m["thinking"] == true {
                model.capabilities.push("reasoning".to_owned());
            }
            Some(model)
        };
        Ok(models.filter_map(model).collect())
    })
}

/// The redirects its calls follow: to Google's hosts over HTTPS alone, as
/// Veo serves a video's bytes from its storage.
fn google_storage(uri: &Uri) -> bool {
    let host = uri.host().unwrap_or_default();
    let google = [".googleapis.com", ".googleusercontent.com"];
    uri.scheme_str() == Some("https") && google.iter().any(|domain| host.ends_with(domain))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_googles_hosts_over_https_are_followed() {
        let follows = |uri: &str| google_storage(&uri.parse().unwrap_or_default());
        assert!(follows("https://storage.googleapis.com/v/1"));
        assert!(follows("https://x.googleusercontent.com/v"));
        assert!(!follows("http://storage.googleapis.com/v"));
        assert!(!follows("https://evil.test/storage.googleapis.com"));
        assert!(!follows("https://googleapis.com.evil.test/v"));
    }
}
