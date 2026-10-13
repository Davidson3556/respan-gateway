//! Google's Gemini API: Chat Completions, images and embeddings at its
//! OpenAI-compatible base, and its own API beside it: `generateContent`,
//! Gemini Live and Veo.

use respan_gateway_conversation::{Content, Input, Role};
use respan_gateway_conversation_http::Schema;
use respan_gateway_conversation_http::gemini::GeminiGenerate;
use respan_gateway_conversation_http::openai::OpenAiChat;
use respan_gateway_core::{NativeFields, Plugin, ProviderContext, Result, async_trait};
use respan_gateway_media_http::gemini::GeminiVeo;
use respan_gateway_media_http::openai::{OpenAiEmbeddings, OpenAiImages};
use respan_gateway_realtime_ws::GeminiLive;
use serde_json::Map;

use crate::{Base, Format, Quirk, Speaks, Vendor};

/// The header its own API takes the key in.
const KEY: &str = "x-goog-api-key";

/// Google's Gemini API. Its own API is its OpenAI-compatible base without
/// `/openai`; Live's endpoint is at its origin.
pub const VENDOR: Vendor = Vendor {
    id: "gemini",
    base_url: Some("https://generativelanguage.googleapis.com/v1beta/openai"),
    key_env: &["GEMINI_API_KEY", "GOOGLE_API_KEY"],
    example_model: "gemini-2.5-flash",
    formats: &[
        Speaks::new(Format::OpenAiChat(OpenAiChat)),
        Speaks::new(Format::GeminiGenerate(GeminiGenerate))
            .key_in(KEY)
            .at(Base::Without("/openai")),
        Speaks::new(Format::GeminiLive(GeminiLive))
            .key_in(KEY)
            .at(Base::Origin),
        Speaks::new(Format::OpenAiImages(OpenAiImages)),
        Speaks::new(Format::GeminiVeo(GeminiVeo))
            .key_in(KEY)
            .at(Base::Without("/openai")),
        Speaks::new(Format::OpenAiEmbeddings(OpenAiEmbeddings)),
    ],
    model_id_prefix: Some("models/"),
    quirks: &[
        Quirk {
            name: "model_id_prefix",
            description: "Its OpenAI-compatible model list names each model `models/<id>`.",
        },
        Quirk {
            name: "thought_signature",
            description: "A Gemini 3 model refuses a function call in a request's history that \
                          has no `thoughtSignature`, as one from another format has none; it \
                          takes its documented placeholder in place of one.",
        },
    ],
    plugin: Some(&GeminiQuirks),
};

/// What Gemini 3 models accept as the signature of a function call made
/// elsewhere, as its documentation gives.
pub const SIGNATURE_PLACEHOLDER: &str = "skip_thought_signature_validator";

/// Gemini's quirks: a function call in the history needs a thought signature.
#[derive(Debug)]
pub struct GeminiQuirks;

#[async_trait]
impl Plugin for GeminiQuirks {
    async fn on_provider(&self, px: &mut ProviderContext) -> Result<()> {
        if !px.is_kind::<Input>() || px.format() != GeminiGenerate::NAME {
            return Ok(());
        }
        px.filter(|input: &mut Input| {
            // Models before Gemini 3 take no signature in the request.
            let older = input.model.as_deref().is_some_and(|m| {
                let m = m.strip_prefix("models/").unwrap_or(m);
                m.starts_with("gemini-1") || m.starts_with("gemini-2")
            });
            if !older {
                sign_calls(input);
            }
            Ok(())
        })
    }
}

/// Signs, with the placeholder, the first call of each assistant message
/// that has no signature of its own, as Gemini returns one only on the first
/// of a turn's parallel calls.
fn sign_calls(input: &mut Input) {
    let own = |native: &Option<NativeFields>| {
        native
            .as_ref()
            .is_none_or(|n| &*n.format == GeminiGenerate::NAME)
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
        let Some(native) = first.filter(|native| own(native)) else {
            continue;
        };
        let fields = native
            .get_or_insert_with(|| NativeFields::new(GeminiGenerate::NAME, Map::new()))
            .fields
            .entry("thoughtSignature");
        fields.or_insert_with(|| SIGNATURE_PLACEHOLDER.into());
    }
}
