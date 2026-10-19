//! Custom format: add a protocol, here OpenAI embeddings, by writing its
//! kind (`EmbedRequest` in, `Embeddings` out) and implementing `Format`
//! for owned incoming and outgoing HTTP messages. Plugins
//! then read and edit its values, whatever bytes they came in.
//!
//! Values are plain data and the format writes each one from its fields, so
//! a message relays by what it says, not by its bytes; the fields the kind
//! has no form for ride along in `rest`.
//! `cargo run -p respan-gateway-examples --example custom_format` runs it
//! against a mock provider.
//!
//! `batch_split.rs` builds on this kind (it includes this file as a module).

use parking_lot::Mutex;
use respan_gateway_core::Provider;
use std::sync::Arc;

use mockllm::{Answer, FakeOpenAi};
use respan_gateway_conversation::Usage;
use respan_gateway_core::{
    ClientContext, Connection, Fault, Format, Gateway, Plugin, ProviderContext, Response,
    async_trait,
};
use respan_gateway_http::bytes::Bytes;
use respan_gateway_http::content;
use respan_gateway_http::http::response::Parts as ResponseHead;
use respan_gateway_http::http::{HeaderValue, Method, StatusCode, header::CONTENT_TYPE};
use respan_gateway_http::{self as h, All, ClientIo, ProviderIo, Whole};
#[allow(dead_code)]
#[path = "../support/request.rs"]
mod http_support;
use http_support::{post, read_whole, serve};
#[allow(dead_code)]
#[path = "../support/http.rs"]
mod provider_support;
use serde_json::{self, Map, Value, json};

/// The kind's input: texts to embed.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct EmbedRequest {
    pub model: Option<String>,
    pub input: Vec<String>,
    /// The other fields of the body, such as `dimensions`.
    pub rest: Map<String, Value>,
}

/// The kind's output: a vector per input, and the usage.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Embeddings {
    pub vectors: Vec<Vec<f64>>,
    pub usage: Usage,
    /// The other fields of the body, such as `model`.
    pub rest: Map<String, Value>,
}

const NAME: &str = "openai-embeddings";

/// OpenAI's `POST /v1/embeddings`.
#[derive(Debug, Clone)]
pub struct OpenAiEmbeddings;

// Each direction binds owned HTTP I/O directly to typed events. These are
// public core/HTTP APIs, the same ones external format authors use.
impl Format<ClientIo, EmbedRequest, Embeddings> for OpenAiEmbeddings {
    fn name(&self) -> &str {
        NAME
    }

    fn matches(&self, io: &ClientIo) -> bool {
        io.request().method() == Method::POST
            && respan_gateway_http::path::clean_path(io.request().uri().path())
                .ends_with("/embeddings")
    }

    fn bind(&self, io: ClientIo) -> Result<Connection<EmbedRequest, Embeddings>, Fault> {
        Ok(io.with_error(error).bind(
            // The request's one body, read into its one input.
            |_| {
                Box::new(|raw: Bytes, out: &mut Vec<EmbedRequest>| {
                    out.push(read_request(raw)?);
                    Ok(())
                })
            },
            // The response written whole, once it ended.
            |reply| {
                let mut head = reply.head();
                content::default_content_type(&mut head, "application/json");
                Ok(reply.write(head, Box::new(All::new(write_embeddings))))
            },
        ))
    }
}

impl Format<ProviderIo, Embeddings, EmbedRequest> for OpenAiEmbeddings {
    fn name(&self) -> &str {
        NAME
    }

    fn bind(&self, io: ProviderIo) -> Result<Connection<Embeddings, EmbedRequest>, Fault> {
        Ok(io.bind(
            // The call's head, and its one input written as its body.
            |head, request: Option<&EmbedRequest>| {
                if request.is_none() {
                    return Err(Fault::config(format!(
                        "a call in {NAME} carries one body, not streamed inputs"
                    )));
                }
                head.method = Method::POST;
                respan_gateway_http::path::set_path(head, "/embeddings")?;
                head.headers
                    .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
                Ok(Box::new(|r: &EmbedRequest, out: &mut Vec<Bytes>| {
                    out.push(write_request(r)?);
                    Ok(())
                }))
            },
            // The answer read whole into its one output.
            |_, _| {
                Some(Box::new(Whole::new(
                    |raw: Bytes, out: &mut Vec<Embeddings>| {
                        out.push(read_embeddings(raw)?);
                        Ok(())
                    },
                )))
            },
        ))
    }
}

fn read_request(raw: Bytes) -> Result<EmbedRequest, Fault> {
    let mut v: Map<String, Value> =
        serde_json::from_slice(&raw).map_err(|_| Fault::bad_request("not a JSON object"))?;
    let model = v.remove("model").and_then(|m| m.as_str().map(Into::into));
    let texts_only = || Fault::bad_request("input must be a string or an array of strings");
    let input = match v.remove("input") {
        Some(Value::String(one)) => vec![one],
        // Texts; token arrays are not read here.
        Some(all) => serde_json::from_value(all).map_err(|_| texts_only())?,
        None => return Err(texts_only()),
    };
    // Base64 vectors are not read here either: the provider would be paid
    // for answers this kind cannot hold.
    if v.get("encoding_format").is_some_and(|f| f == "base64") {
        return Err(Fault::bad_request(
            "encoding_format base64 is not supported",
        ));
    }
    Ok(EmbedRequest {
        model,
        input,
        rest: v,
    })
}

fn read_embeddings(raw: Bytes) -> Result<Embeddings, Fault> {
    let mut v: Map<String, Value> =
        serde_json::from_slice(&raw).map_err(|_| Fault::upstream("not a JSON object"))?;
    let mut said = Embeddings::default();
    let malformed = || Fault::upstream("invalid embedding indices or vectors");
    let data = v.remove("data").ok_or_else(malformed)?;
    let data = data.as_array().ok_or_else(malformed)?;
    let mut ordered = vec![None; data.len()];
    for item in data {
        let index = item["index"]
            .as_u64()
            .and_then(|index| usize::try_from(index).ok())
            .and_then(|index| ordered.get_mut(index))
            .ok_or_else(malformed)?;
        // Base64 vectors are not read here: they are malformed for this kind.
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
    let usage = v.remove("usage");
    if let Some(tokens) = usage.and_then(|u| u["prompt_tokens"].as_u64()) {
        said.usage.input_tokens += tokens;
    }
    said.rest = v;
    Ok(said)
}

/// The fault as this protocol's error body: the format's error writer.
fn error(fault: &Fault, _: &h::http::Request<Bytes>, head: &mut ResponseHead) -> Bytes {
    content::default_content_type(head, "application/json");
    Bytes::from(json!({ "error": { "message": fault.message() } }).to_string())
}

/// A request, written from its fields.
fn write_request(r: &EmbedRequest) -> Result<Bytes, Fault> {
    let mut v = r.rest.clone();
    v.insert("model".into(), json!(r.model));
    v.insert("input".into(), json!(r.input));
    Ok(Value::Object(v).to_string().into())
}

/// A response, written from its fields.
fn write_embeddings(one: &[Embeddings]) -> Result<Bytes, Fault> {
    let [e] = one else {
        return Err(Fault::internal("one response, one body"));
    };
    let mut v = e.rest.clone();
    let data = e.vectors.iter().enumerate();
    let data = data.map(|(i, v)| json!({ "object": "embedding", "index": i, "embedding": v }));
    v.insert("data".into(), data.collect());
    let tokens = e.usage.input_tokens;
    v.insert(
        "usage".into(),
        json!({ "prompt_tokens": tokens, "total_tokens": tokens }),
    );
    Ok(Value::Object(v).to_string().into())
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
            *total.lock() += &e.usage;
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
        .plugin(PinModel(PINNED))
        .plugin(metered.clone())
        .provider(Provider::new(
            "openai",
            OpenAiEmbeddings,
            provider.connect(),
        ))?
        .client::<EmbedRequest, _, _>(OpenAiEmbeddings)?
        .target("openai")?;
    let (status, _, answered) = read_whole(serve(&gw, post("/v1/embeddings", body)).await.0).await;
    gw.idle().await;
    let requests = openai.requests();
    assert_eq!(requests.len(), 1, "one provider embedding request");
    let sent = requests[0].raw.clone();
    let usage = *metered.0.lock();
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
