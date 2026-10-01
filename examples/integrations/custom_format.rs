//! Custom format: add a protocol, here OpenAI embeddings, by writing its
//! kind (`EmbedRequest` in, `Embeddings` out) and implementing `Format`
//! for owned incoming and outgoing HTTP messages. Plugins
//! then read and edit its values, whatever bytes they came in.
//!
//! Each value keeps the bytes it came in (`Relayed`), so a message no one
//! changed relays byte for byte, and a changed one is written into them.
//! `cargo run -p respan-gateway-examples --example custom_format` runs it
//! against a mock provider.
//!
//! `batch_split.rs` builds on this kind (it includes this file as a module).

use std::sync::{Arc, Mutex, PoisonError};

use mockllm::{Answer, FakeOpenAi};
use respan_gateway_http::bytes::Bytes;
use respan_gateway_http::formats::{self as formats, whole::Relay};
use respan_gateway_http::http::{HeaderValue, Method, StatusCode, header::CONTENT_TYPE};
use respan_gateway_http::{self as h, Incoming, Outgoing};
use respan_gateway_sdk::{
    ClientContext, Connection, Fault, Format, Gateway, Plugin, ProviderContext, Response,
    async_trait,
};
use respan_gateway_sdk::{Usage, conversation::units::INPUT_TOKENS, relayed::Relayed};
#[allow(dead_code)]
#[path = "../../tests/support/http.rs"]
mod http_support;
use http_support::{post, read_whole, serve};
#[allow(dead_code)]
#[path = "../tests/support.rs"]
mod provider_support;
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

// Each direction binds owned HTTP I/O directly to typed events. These are
// public core/HTTP APIs, the same ones external format authors use.
impl Format<Incoming, EmbedRequest, Embeddings> for OpenAiEmbeddings {
    fn name(&self) -> &str {
        NAME
    }

    fn matches(&self, io: &Incoming) -> bool {
        io.request().method() == Method::POST
            && formats::clean_path(io.request().uri().path()).ends_with("/embeddings")
    }

    fn bind(&self, io: Incoming) -> Result<Connection<EmbedRequest, Embeddings>, Fault> {
        Ok(io.bind(
            NAME,
            |request| h::messages(request, |_, raw| read_request(raw)),
            |reply, source| {
                let mut head = h::response_head(NAME, source);
                formats::default_content_type(&mut head, "application/json");
                Ok(reply.write(head, Box::new(Relay::new(write_embeddings, error, false))))
            },
            |fault, _, head| {
                formats::default_content_type(head, "application/json");
                error(fault)
            },
        ))
    }
}

impl Format<Outgoing, Embeddings, EmbedRequest> for OpenAiEmbeddings {
    fn name(&self) -> &str {
        NAME
    }

    fn bind(&self, io: Outgoing) -> Result<Connection<Embeddings, EmbedRequest>, Fault> {
        Ok(io.bind(
            NAME,
            |source| {
                if !source.is_single() {
                    return Err(Fault::config(format!(
                        "a call in {NAME} carries one body, not streamed inputs"
                    )));
                }
                let (request, sink) = h::request(
                    NAME,
                    source,
                    Box::new(Relay::new(write_request, |_: &Fault| Bytes::new(), false)),
                )?;
                let (mut head, body) = request.into_parts();
                head.method = Method::POST;
                formats::set_path(&mut head, "/embeddings");
                head.headers
                    .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
                Ok((h::http::Request::from_parts(head, body), sink))
            },
            |response, _| {
                formats::whole::read(NAME, response, |raw, out| {
                    out.push(read_embeddings(raw)?);
                    Ok(())
                })
            },
            |head, _| head.status.is_success(),
        ))
    }
}

fn read_request(raw: Bytes) -> Result<EmbedRequest, Fault> {
    let name: Arc<str> = Arc::from(NAME);
    let v: Value = serde_json::from_slice(&raw).map_err(|_| Fault::bad_request("not JSON"))?;
    let mut said = EmbedRequest::default();
    said.model = v["model"].as_str().map(Into::into);
    said.input = match &v["input"] {
        Value::String(one) => vec![one.clone()],
        // Texts; token arrays are relayed but not read here.
        all => serde_json::from_value(all.clone()).unwrap_or_default(),
    };
    said.origin = Relayed::came(&name, raw, said.clone());
    Ok(said)
}

fn read_embeddings(raw: Bytes) -> Result<Embeddings, Fault> {
    let name: Arc<str> = Arc::from(NAME);
    let v: Value = serde_json::from_slice(&raw).map_err(|_| Fault::upstream("not JSON"))?;
    let mut said = Embeddings::default();
    let malformed = || Fault::upstream("invalid embedding indices or vectors");
    let data = v["data"].as_array().ok_or_else(malformed)?;
    let mut ordered = vec![None; data.len()];
    for item in data {
        let index = item["index"]
            .as_u64()
            .and_then(|index| usize::try_from(index).ok())
            .and_then(|index| ordered.get_mut(index))
            .ok_or_else(malformed)?;
        // Base64 vectors can relay unread, but cannot be edited as floats.
        let vector: Vec<f64> =
            serde_json::from_value(item["embedding"].clone()).map_err(|_| malformed())?;
        if vector.is_empty() || index.replace(vector).is_some() {
            return Err(malformed());
        }
    }
    said.vectors = ordered
        .into_iter()
        .collect::<Option<_>>()
        .ok_or_else(malformed)?;
    if let Some(tokens) = v["usage"]["prompt_tokens"].as_f64() {
        said.usage.add(INPUT_TOKENS, tokens);
    }
    said.origin = Relayed::came(&name, raw, said.clone());
    Ok(said)
}

/// The fault as this protocol's error body.
fn error(fault: &Fault) -> Bytes {
    Bytes::from(json!({ "error": { "message": fault.message() } }).to_string())
}

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

const ASKED: &str = r#"{"model": "text-embedding-3-large", "input": ["hi"], "dimensions": 2}"#;
const ANSWER: &str = r#"{"object": "list", "data": [{"object": "embedding", "index": 0, "embedding": [0.5, -0.5]}],
 "model": "text-embedding-3-small", "usage": {"prompt_tokens": 1, "total_tokens": 1}}"#;
const PINNED: &str = "text-embedding-3-small";

/// Posts `body` through a gateway on a strict fake OpenAI: what was sent, the
/// status and body answered, and the usage the provider reported.
async fn embed(body: &'static str) -> Result<(Bytes, StatusCode, Bytes, Usage), Fault> {
    let (openai, metered) = (FakeOpenAi::start().await, Metered::default());
    openai.answer(Answer::raw(ANSWER));
    let provider = provider_support::openai(&openai.base_url())?;
    let gw = Gateway::new()
        .plugin("pin", PinModel(PINNED))?
        .plugin("metered", metered.clone())?
        .provider::<EmbedRequest, _, _>("openai", OpenAiEmbeddings, provider.connect())?
        .client::<EmbedRequest, _, _>(OpenAiEmbeddings)?
        .target("openai")?;
    let (status, _, answered) = read_whole(serve(&gw, post("/v1/embeddings", body)).await.0).await;
    gw.idle().await;
    let requests = openai.requests();
    assert_eq!(requests.len(), 1, "one provider embedding request");
    let sent = requests[0].raw.clone();
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
#[path = "../tests/custom_format.rs"]
mod tests;
