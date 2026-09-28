//! Custom format: add a protocol, here OpenAI embeddings, by writing its
//! kind (`EmbedRequest` in, `Embeddings` out) and its `Format`. Plugins
//! then read and edit its values, whatever bytes they came in.
//!
//! Each value keeps the bytes it came in (`Relayed`), so a message no one
//! changed relays byte for byte, and a changed one is written into them.
//! `cargo run -p respan-gateway-examples --example custom_format` runs it
//! against a mock provider.

use std::sync::Arc;

use respan_gateway_core::bytes::Bytes;
use respan_gateway_core::http::{self, HeaderValue, StatusCode, header::CONTENT_TYPE};
use respan_gateway_core::serde_json::{self, Value, json};
use respan_gateway_core::{
    ClientContext, Decoder, Encoder, Fault, Format, Gateway, Plugin, Response, Usage, async_trait,
};
use respan_gateway_formats::conversation::units::INPUT_TOKENS;
use respan_gateway_formats::{self as formats, relayed::Relayed, whole::Relay};
use respan_gateway_testkit::{Calls, MockTransport, mock, post, read_whole};

/// The kind's input: texts to embed.
#[derive(Debug, Clone, Default, PartialEq)]
struct EmbedRequest {
    model: Option<String>,
    input: Vec<String>,
    origin: Relayed<EmbedRequest>,
}

/// The kind's output: a vector per input, and the usage.
#[derive(Debug, Clone, Default, PartialEq)]
struct Embeddings {
    vectors: Vec<Vec<f64>>,
    usage: Usage,
    origin: Relayed<Embeddings>,
}

const NAME: &str = "openai-embeddings";

/// OpenAI's `POST /v1/embeddings`.
struct OpenAiEmbeddings;

impl Format for OpenAiEmbeddings {
    type Input = EmbedRequest;
    type Output = Embeddings;

    fn name(&self) -> &str {
        NAME
    }

    fn accepts(&self, head: &http::request::Parts) -> bool {
        formats::posts_to(head, "/embeddings")
    }

    fn request_decoder(&self, _request: &http::Request<Bytes>) -> Box<dyn Decoder<EmbedRequest>> {
        let name: Arc<str> = Arc::from(NAME);
        formats::message_decoder(move |raw| {
            let v: Value =
                serde_json::from_slice(&raw).map_err(|_| Fault::bad_request("not JSON"))?;
            let mut said = EmbedRequest::default();
            said.model = v["model"].as_str().map(Into::into);
            said.input = match &v["input"] {
                Value::String(one) => vec![one.clone()],
                // Texts; token arrays are relayed but not read here.
                all => serde_json::from_value(all.clone()).unwrap_or_default(),
            };
            said.origin = Relayed::came(&name, raw, said.clone());
            Ok(said)
        })
    }

    fn request_encoder(
        &self,
        request: &mut http::Request<Bytes>,
        _first: Option<&EmbedRequest>,
    ) -> Box<dyn Encoder<EmbedRequest>> {
        formats::set_path(request, "/embeddings");
        let json = HeaderValue::from_static("application/json");
        request.headers_mut().insert(CONTENT_TYPE, json);
        Box::new(Relay::new(write_request, |_: &Fault| Bytes::new(), false))
    }

    fn response_decoder(
        &self,
        head: &http::response::Parts,
        _request: &http::Request<Bytes>,
    ) -> Box<dyn Decoder<Embeddings>> {
        let name: Arc<str> = Arc::from(NAME);
        // A failure is decoded for us: its fault carries the provider's body.
        formats::whole::decoder(NAME, head, move |raw, out| {
            let v: Value = serde_json::from_slice(&raw).map_err(|_| Fault::upstream("not JSON"))?;
            let mut said = Embeddings::default();
            let data = v["data"].as_array().into_iter().flatten();
            // Float vectors; base64 ones are relayed but not read here.
            let vector = |d: &Value| serde_json::from_value(d["embedding"].clone());
            said.vectors = data.map(|d| vector(d).unwrap_or_default()).collect();
            if let Some(tokens) = v["usage"]["prompt_tokens"].as_f64() {
                said.usage.add(INPUT_TOKENS, tokens);
            }
            said.origin = Relayed::came(&name, raw, said.clone());
            out.push(said);
            Ok(())
        })
    }

    fn response_encoder(
        &self,
        head: &mut http::response::Parts,
        _request: &http::Request<Bytes>,
    ) -> Box<dyn Encoder<Embeddings>> {
        formats::default_content_type(head, "application/json");
        let fail = |f: &Fault| match f.raw(NAME) {
            Some(body) => body.clone(),
            None => Bytes::from(json!({ "error": { "message": f.message() } }).to_string()),
        };
        Box::new(Relay::new(write_embeddings, fail, false))
    }

    /// What `Outcome.usage` counts, as decoded.
    fn usage<'a>(&self, output: &'a Embeddings) -> Option<&'a Usage> {
        (!output.usage.is_empty()).then_some(&output.usage)
    }
}

/// The JSON a value came in, to write its changes into.
fn origin_json<T: PartialEq>(origin: &Relayed<T>) -> Value {
    let bytes = origin.origin(NAME).map(|o| &o.bytes[..]).unwrap_or(b"{}");
    serde_json::from_slice(bytes).unwrap_or_default()
}

/// A request: the client's bytes while they say the same.
fn write_request(one: &[EmbedRequest]) -> Result<Bytes, Fault> {
    // None if the client's bytes go unread.
    let [r] = one else {
        return Ok(Bytes::new());
    };
    if let Some(bytes) = r.origin.bytes(NAME, r) {
        return Ok(bytes.clone());
    }
    let mut v = origin_json(&r.origin);
    (v["model"], v["input"]) = (json!(r.model), json!(r.input));
    Ok(v.to_string().into())
}

/// A response: the provider's bytes while they say the same.
fn write_embeddings(one: &[Embeddings]) -> Result<Bytes, Fault> {
    let [e] = one else {
        return Err(Fault::internal("one response, one body"));
    };
    if let Some(bytes) = e.origin.bytes(NAME, e) {
        return Ok(bytes.clone());
    }
    let mut v = origin_json(&e.origin);
    let data = e.vectors.iter().enumerate();
    v["data"] = data
        .map(|(i, v)| json!({ "index": i, "embedding": v }))
        .collect();
    let tokens = e.usage.get(INPUT_TOKENS);
    v["usage"] = json!({ "prompt_tokens": tokens, "total_tokens": tokens });
    Ok(v.to_string().into())
}

/// A plugin on the new kind: every request uses `model`.
struct PinModel(&'static str);

#[async_trait]
impl Plugin for PinModel {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        if cx.is_kind::<EmbedRequest>() {
            let model = self.0;
            cx.filter(move |req: &mut EmbedRequest| {
                req.model = Some(model.into());
                Ok(())
            })?;
        }
        Ok(None)
    }
}

const AT: &str = "https://api.openai.com/v1";
const ASKED: &str = r#"{"model": "large", "input": ["hi"], "dimensions": 2}"#;
const ANSWER: &str = r#"{"object": "list", "data": [{"object": "embedding", "index": 0, "embedding": [0.5, -0.5]}],
 "model": "small", "usage": {"prompt_tokens": 1, "total_tokens": 1}}"#;

/// Posts `body` through a gateway on a mock OpenAI: what was sent, the
/// status and body answered, and the usage counted.
async fn embed(body: &'static str) -> Result<(Bytes, StatusCode, Bytes, Usage), Fault> {
    let (mock, calls) = (MockTransport::new(), Calls::default());
    mock.reply(AT, mock::json(StatusCode::OK, ANSWER));
    let gw = Gateway::new()
        .plugin("calls", calls.clone())?
        .plugin("pin", PinModel("small"))?
        .provider("openai", OpenAiEmbeddings, mock.at(AT))?
        .serve(OpenAiEmbeddings)?
        .target("openai")?;
    let (status, _, answered) = read_whole(gw.handle(post("/v1/embeddings", body)).await.0).await;
    gw.idle().await;
    let sent = mock.sent()[0].body().clone();
    Ok((sent, status, answered?, calls.usage()))
}

#[tokio::main]
async fn main() -> Result<(), Fault> {
    let (sent, status, answered, usage) = embed(ASKED).await?;
    println!("sent:     {}", String::from_utf8_lossy(&sent));
    println!("answered: {status} {}", String::from_utf8_lossy(&answered));
    println!("usage:    {usage:?}");
    Ok(())
}

#[cfg(test)]
#[path = "tests/custom_format.rs"]
mod tests;
