//! Custom format: add a protocol, here OpenAI embeddings, by writing its
//! kind (`EmbedRequest` in, `Embeddings` out) and its `Codec`, which
//! `impl_format!` makes a `Format`. Plugins
//! then read and edit its values, whatever bytes they came in.
//!
//! Each value keeps the bytes it came in (`Relayed`), so a message no one
//! changed relays byte for byte, and a changed one is written into them.
//! `cargo run -p respan-gateway-examples --example custom_format` runs it
//! against a mock provider.
//!
//! `batch_split.rs` builds on this kind (it includes this file as a module).

use std::sync::{Arc, Mutex, PoisonError};

use respan_gateway_core::{
    ClientContext, Fault, Gateway, Plugin, ProviderContext, Response, async_trait,
};
use respan_gateway_formats::conversation::units::INPUT_TOKENS;
use respan_gateway_formats::{self as formats, Usage, relayed::Relayed, whole::Relay};
use respan_gateway_http::bytes::Bytes;
use respan_gateway_http::http::{HeaderValue, StatusCode, header::CONTENT_TYPE};
use respan_gateway_http::{Codec, Decoder, Encoder, RequestHead, ResponseHead, impl_format};
use respan_gateway_testkit::{MockTransport, mock, post, read_whole, serve};
use serde_json::{self, Value, json};

/// The kind's input: texts to embed.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct EmbedRequest {
    pub model: Option<String>,
    pub input: Vec<String>,
    origin: Relayed<EmbedRequest>,
}

/// The kind's output: a vector per input, and the usage.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Embeddings {
    pub vectors: Vec<Vec<f64>>,
    pub usage: Usage,
    origin: Relayed<Embeddings>,
}

const NAME: &str = "openai-embeddings";

/// OpenAI's `POST /v1/embeddings`.
#[derive(Debug, Clone)]
pub struct OpenAiEmbeddings;

impl Codec for OpenAiEmbeddings {
    type Input = EmbedRequest;
    type Output = Embeddings;

    fn name(&self) -> &str {
        NAME
    }

    fn accepts(&self, head: &RequestHead) -> bool {
        formats::posts_to(head, "/embeddings")
    }

    fn request_decoder(&self, _head: &RequestHead) -> Box<dyn Decoder<EmbedRequest>> {
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

    /// The client's head if it relays, which the transport cleans; the
    /// path and content type are this format's.
    fn request_encoder(
        &self,
        client: Option<&RequestHead>,
        _body: &mut Bytes,
        streams: bool,
    ) -> Box<dyn Encoder<EmbedRequest, RequestHead>> {
        let mut head = formats::call_head(client);
        if streams {
            return formats::with_head(head, formats::whole::unstreamable(NAME));
        }
        let head = move |first: Option<&EmbedRequest>| {
            if first.is_some() {
                formats::body_changed(&mut head);
            }
            formats::set_path(&mut head, "/embeddings");
            let json = HeaderValue::from_static("application/json");
            head.headers.insert(CONTENT_TYPE, json);
            head
        };
        let body = Relay::new(write_request, |_: &Fault| Bytes::new(), false);
        formats::headed(head, Box::new(body))
    }

    fn response_decoder(
        &self,
        head: &ResponseHead,
        _call: &RequestHead,
        _: &Bytes,
    ) -> Box<dyn Decoder<Embeddings>> {
        let name: Arc<str> = Arc::from(NAME);
        // A failure is decoded for us: its fault carries the provider's answer.
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
        answer: Option<&ResponseHead>,
        _ask: &RequestHead,
        _body: &Bytes,
    ) -> Box<dyn Encoder<Embeddings, ResponseHead>> {
        let mut head = formats::response_head(answer);
        formats::default_content_type(&mut head, "application/json");
        let body = Relay::new(write_embeddings, error, false);
        formats::with_head(head, Box::new(body))
    }

    /// This protocol's error under the fault's status and `Retry-After`;
    /// the provider's own answer goes as it came.
    fn refusal(&self, fault: &Fault, _body: &Bytes, head: &mut ResponseHead) -> Bytes {
        formats::default_content_type(head, "application/json");
        error(fault)
    }
}

/// The fault as this protocol's error body.
fn error(fault: &Fault) -> Bytes {
    Bytes::from(json!({ "error": { "message": fault.message() } }).to_string())
}

impl_format!(OpenAiEmbeddings);

/// The JSON a value came in, to write its changes into.
fn origin_json<T: PartialEq>(origin: &Relayed<T>) -> Value {
    let bytes = origin.came_in(NAME).map(|o| &o.bytes[..]).unwrap_or(b"{}");
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

/// Adds up the usage the provider reports, with a filter of its kind on
/// each call.
#[derive(Clone, Default)]
struct Metered(Arc<Mutex<Usage>>);

#[async_trait]
impl Plugin for Metered {
    async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
        let total = self.0.clone();
        px.filter(move |e: &mut Embeddings| {
            *total.lock().unwrap_or_else(PoisonError::into_inner) += &e.usage;
            Ok(())
        })
    }
}

const AT: &str = "https://api.openai.com/v1";
const ASKED: &str = r#"{"model": "large", "input": ["hi"], "dimensions": 2}"#;
const ANSWER: &str = r#"{"object": "list", "data": [{"object": "embedding", "index": 0, "embedding": [0.5, -0.5]}],
 "model": "small", "usage": {"prompt_tokens": 1, "total_tokens": 1}}"#;

/// Posts `body` through a gateway on a mock OpenAI: what was sent, the
/// status and body answered, and the usage the provider reported.
async fn embed(body: &'static str) -> Result<(Bytes, StatusCode, Bytes, Usage), Fault> {
    let (mock, metered) = (MockTransport::new(), Metered::default());
    mock.reply(AT, mock::json(StatusCode::OK, ANSWER));
    let gw = Gateway::new()
        .plugin("pin", PinModel("small"))?
        .plugin("metered", metered.clone())?
        .provider("openai", OpenAiEmbeddings, mock.at(AT))?
        .client(OpenAiEmbeddings)?
        .target("openai")?;
    let (status, _, answered) = read_whole(serve(&gw, post("/v1/embeddings", body)).await.0).await;
    gw.idle().await;
    let sent = mock.sent()[0].body().clone();
    let usage = metered
        .0
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .clone();
    Ok((sent, status, answered?, usage))
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
