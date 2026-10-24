//! DeepInfra: Chat Completions, images, embeddings, speech and
//! transcriptions, as OpenAI serves them. Not its videos: a finished job
//! says `succeeded`, not `completed`, beside a `data` list of its files.

use respan_gateway_conversation_http::openai::OpenAiChat;
use respan_gateway_media_http::openai::{
    OpenAiEmbeddings, OpenAiImages, OpenAiSpeech, OpenAiTranscriptions,
};

use serde_json::Value;

use respan_gateway_vendor_kit::{
    Body, BoxFuture, Model, Models, Price, Vendor, VendorProvider, built_wrong, http, json,
};

/// Where its API is.
const BASE_URL: &str = "https://api.deepinfra.com/v1/openai";

/// DeepInfra.
pub const VENDOR: Vendor = Vendor {
    id: "deepinfra",
    key_env: &["DEEPINFRA_API_KEY"],
    formats: &[
        (&OpenAiChat, Some(BASE_URL)),
        (&OpenAiImages, Some(BASE_URL)),
        (&OpenAiEmbeddings, Some(BASE_URL)),
        (&OpenAiSpeech, Some(BASE_URL)),
        (&OpenAiTranscriptions, Some(BASE_URL)),
    ],
    models: list_models,
    ..Vendor::DEFAULT
};

/// The kind each of its tags says a model is, the first it has.
const KINDS: &[(&str, &str)] = &[
    ("chat", "chat"),
    ("embed", "embedding"),
    ("image-gen", "image"),
    ("video-gen", "video"),
    ("tts", "speech"),
    ("stt", "transcription"),
];

/// Its models: `GET /models` as OpenAI lists them, each with what it
/// does, its limits and its price in its `metadata`.
fn list_models(p: &VendorProvider) -> BoxFuture<'_, Models> {
    Box::pin(async move {
        let call = http::Request::get("/models").body(Body::default());
        let (answer, _) = p.client()?.send(call.map_err(built_wrong)?).await?;
        Ok(listed(&json(answer).await?))
    })
}

/// The models of its list, each of the kind its tags say; one whose tags
/// say none of [`KINDS`] is none it serves, and is left out.
fn listed(answer: &Value) -> Vec<Model> {
    let entries = answer["data"].as_array().into_iter().flatten();
    entries.filter_map(described).collect()
}

/// One entry of its list: its id, and the rest from its `metadata`. Its
/// `created` is always 0, so says nothing.
fn described(m: &Value) -> Option<Model> {
    let mut model = Model::new(m["id"].as_str()?);
    let about = &m["metadata"];
    let tags = about["tags"].as_array();
    let has = |tag: &str| tags.is_some_and(|t| t.iter().any(|t| t == tag));
    let (_, kind) = KINDS.iter().find(|(tag, _)| has(tag))?;
    model.kind = Some((*kind).to_owned());
    model.description = about["description"]
        .as_str()
        .filter(|d| !d.is_empty())
        .map(str::to_owned);
    model.context_window = about["context_length"].as_u64();
    model.max_output_tokens = about["max_tokens"].as_u64();
    model.price = price(&about["pricing"]);
    Some(model)
}

/// A price by the token, in dollars per million: an embedding model's has
/// no output. Its prices by the image, character or second are not one.
fn price(p: &Value) -> Option<Price> {
    let input = p["input_tokens"].as_f64()?;
    let output = p["output_tokens"].as_f64().unwrap_or_default();
    Some(Price::new(input, output, p["cache_read_tokens"].as_f64()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn its_tags_say_a_models_kind_and_its_metadata_the_rest() {
        let entry = |id: &str, tags: Value, pricing: Value| {
            let about = json!({ "description": "", "context_length": 8192, "max_tokens": 4096,
                                "pricing": pricing, "tags": tags });
            json!({ "id": id, "object": "model", "created": 0, "metadata": about })
        };
        let answer = json!({ "object": "list", "data": [
            entry("Qwen/Qwen3-30B-A3B", json!(["chat", "reasoning"]),
                  json!({ "input_tokens": 0.12, "output_tokens": 0.5, "cache_read_tokens": 0.06 })),
            entry("BAAI/bge-m3", json!(["embed"]), json!({ "input_tokens": 0.01 })),
            entry("black-forest-labs/FLUX-1-dev", json!(["image-gen"]), json!({ "per_image_unit": 0.04 })),
            entry("google/veo-3.1", json!(["video-gen"]), json!({ "output_seconds": 0.4 })),
            entry("hexgrad/Kokoro-82M", json!(["tts"]), json!({ "input_characters": 0.8 })),
            entry("openai/whisper-large-v3", json!(["stt"]), json!({ "input_seconds": 0.00001 })),
            entry("BAAI/bge-reranker-v2-m3", json!(["reranker"]), json!({ "input_tokens": 0.01 })),
        ] });
        let models = listed(&answer);
        let kinds: Vec<_> = models.iter().map(|m| m.kind.as_deref()).collect();
        assert_eq!(
            kinds,
            [
                Some("chat"),
                Some("embedding"),
                Some("image"),
                Some("video"),
                Some("speech"),
                Some("transcription"),
            ],
            "a reranker is none it serves"
        );
        let chat = &models[0];
        assert_eq!(
            (chat.context_window, chat.max_output_tokens),
            (Some(8192), Some(4096))
        );
        assert_eq!(chat.description, None);
        assert_eq!(chat.price, Some(Price::new(0.12, 0.5, Some(0.06))));
        assert_eq!(models[1].price, Some(Price::new(0.01, 0.0, None)));
        assert_eq!(models[2].price, None);
    }
}
