//! Embeddings.

use respan_gateway_core::{ClientContext, Fault, Plugin, Response, async_trait};
use respan_gateway_http::http::StatusCode;
use respan_gateway_media::embedding;
use respan_gateway_media_http::openai::OpenAiEmbeddings;
use serde_json::{Value, json};

use crate::support::{Fake, Rules, embedding_vector, gateway, json_of, json_request, send, unb64};

async fn embed(fake: &Fake, asked: Value) -> (StatusCode, Value) {
    let gateway = gateway(OpenAiEmbeddings, fake);
    let (status, _, body) = send(&gateway, json_request("/v1/embeddings", asked)).await;
    (status, json_of(&body))
}

/// The floats a base64 vector holds.
fn floats(v: &Value) -> Vec<f32> {
    unb64(v)
        .chunks_exact(4)
        .map(|f| f32::from_le_bytes([f[0], f[1], f[2], f[3]]))
        .collect()
}

#[tokio::test]
async fn a_string_is_embedded_and_relayed_as_one() {
    let fake = Fake::openai().await;
    let asked = json!({"model": "text-embedding-3-small", "input": "Hello there General Kenobi"});
    let (status, body) = embed(&fake, asked.clone()).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body,
        json!({
            "object": "list",
            "data": [{"object": "embedding", "index": 0, "embedding": embedding_vector(0, 8)}],
            "model": "text-embedding-3-small",
            "usage": {"prompt_tokens": 4, "total_tokens": 4},
        })
    );
    assert_eq!(Value::Object(fake.seen()[0].fields.clone()), asked);
}

#[tokio::test]
async fn strings_are_embedded_one_vector_each_in_order() {
    let fake = Fake::openai().await;
    let asked = json!({"model": "text-embedding-3-small", "input": ["a b", "c", "d e f"]});
    let (status, body) = embed(&fake, asked.clone()).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let data = body["data"].as_array().expect("data");
    assert_eq!(data.len(), 3);
    for (i, item) in data.iter().enumerate() {
        assert_eq!(item["index"], i);
        assert_eq!(item["embedding"], json!(embedding_vector(i, 8)));
    }
    assert_eq!(body["usage"]["prompt_tokens"], 6);
    assert_eq!(Value::Object(fake.seen()[0].fields.clone()), asked);
}

#[tokio::test]
async fn tokens_are_embedded_as_one_item_or_several() {
    let fake = Fake::openai().await;
    let model = "text-embedding-3-small";
    for (input, vectors, tokens) in [
        (json!([15339, 1917]), 1, 2),
        (json!([[15339], [1, 2, 3]]), 2, 4),
    ] {
        let (status, body) = embed(&fake, json!({"model": model, "input": input})).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["data"].as_array().map(Vec::len), Some(vectors));
        assert_eq!(body["usage"]["total_tokens"], tokens);
    }
    let seen = fake.seen();
    assert_eq!(seen[0].fields["input"], json!([15339, 1917]));
    assert_eq!(seen[1].fields["input"], json!([[15339], [1, 2, 3]]));
}

#[tokio::test]
async fn base64_is_asked_and_answered_as_base64() {
    let fake = Fake::openai().await;
    let asked =
        json!({"model": "text-embedding-3-small", "input": "hi", "encoding_format": "base64"});
    let (status, body) = embed(&fake, asked.clone()).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        floats(&body["data"][0]["embedding"]),
        embedding_vector(0, 8)
    );
    assert_eq!(Value::Object(fake.seen()[0].fields.clone()), asked);
}

#[tokio::test]
async fn floats_are_answered_as_asked_whatever_the_provider_sends() {
    let fake = Fake::start(Rules {
        embedding_encoding: Some("base64"),
        ..Rules::default()
    })
    .await;
    let asked =
        json!({"model": "text-embedding-3-small", "input": "hi", "encoding_format": "float"});
    let (status, body) = embed(&fake, asked).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["data"][0]["embedding"], json!(embedding_vector(0, 8)));
}

#[tokio::test]
async fn base64_is_answered_as_asked_whatever_the_provider_sends() {
    let fake = Fake::start(Rules {
        embedding_encoding: Some("float"),
        ..Rules::default()
    })
    .await;
    let asked =
        json!({"model": "text-embedding-3-small", "input": "hi", "encoding_format": "base64"});
    let (status, body) = embed(&fake, asked).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        floats(&body["data"][0]["embedding"]),
        embedding_vector(0, 8)
    );
}

#[tokio::test]
async fn dimensions_and_user_pass_through() {
    let fake = Fake::openai().await;
    let asked = json!({
        "model": "text-embedding-3-small", "input": "hi", "dimensions": 3, "user": "u-1",
    });
    let (status, body) = embed(&fake, asked.clone()).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["data"][0]["embedding"], json!(embedding_vector(0, 3)));
    assert_eq!(Value::Object(fake.seen()[0].fields.clone()), asked);
}

#[tokio::test]
async fn unknown_fields_survive_in_both_directions() {
    let fake = Fake::start(Rules {
        extra_fields: vec!["input_type"],
        extra_answer: json!({"system_fingerprint": "fp_1"})
            .as_object()
            .cloned()
            .unwrap_or_default(),
        ..Rules::default()
    })
    .await;
    let asked = json!({"model": "text-embedding-3-small", "input": "hi", "input_type": "query"});
    let (status, body) = embed(&fake, asked.clone()).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["system_fingerprint"], "fp_1");
    assert_eq!(Value::Object(fake.seen()[0].fields.clone()), asked);
}

#[tokio::test]
async fn a_providers_error_is_relayed_with_its_status() {
    let fake = Fake::openai().await;
    let asked = json!({"model": "text-embedding-9", "input": "hi"});
    let (status, body) = embed(&fake, asked).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["error"]["code"], "model_not_found");
}

#[tokio::test]
async fn embeddings_without_input_are_refused_before_any_call() {
    let fake = Fake::openai().await;
    let (status, body) = embed(&fake, json!({"model": "text-embedding-3-small"})).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"]["param"], "input");
    let (status, body) = embed(&fake, json!({"model": "m", "input": []})).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"]["param"], "input");
    assert!(fake.seen().is_empty());
}

#[tokio::test]
async fn a_plugin_reads_and_edits_the_typed_input() {
    struct Shorter;

    #[async_trait]
    impl Plugin for Shorter {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            cx.filter(|input: &mut embedding::Input| {
                input.dimensions = Some(2);
                Ok(())
            })?;
            Ok(None)
        }
    }

    let fake = Fake::openai().await;
    let gateway = gateway(OpenAiEmbeddings, &fake).plugin(Shorter);
    let asked = json!({"model": "text-embedding-3-small", "input": "hi"});
    let (status, _, _) = send(&gateway, json_request("/v1/embeddings", asked)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(fake.seen()[0].fields["dimensions"], 2);
}
