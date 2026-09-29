//! Use case 4, one request, many upstream calls: `GET /v1/models` is answered
//! by asking every configured provider and merging their lists into one.
//!
//! The endpoint is its own format, `Models`, built on `formats::opaque`:
//! its values are the bytes of the request and of each answer, which is all
//! a plugin that never reads a chat needs. Its `MergeModels` plugin answers
//! in `on_client`: it makes a call to each provider (a relay of the client's
//! request, each under its provider's own key, set on its transport), reads
//! each answer's JSON, and returns one list, the first entry of an id
//! winning. A provider that fails is left out of the list; only when every
//! one fails does the request fail.
//!
//! The plugin is given the provider names: the gateway has no list of them
//! for a plugin to read.
//!
//! Not covered: another provider's list shape than OpenAI's
//! (`{"data": [{"id": ..}]}`); prefixing ids with their provider; caching the
//! merged list.
//!
//! `cargo run -p respan-gateway-examples --example models_merge`

use respan_gateway_core::futures::future::join_all;
use respan_gateway_core::futures::{TryStreamExt, stream};
use respan_gateway_core::{ClientContext, Fault, Gateway, Plugin, Response, async_trait};
use respan_gateway_formats as formats;
use respan_gateway_formats::opaque::{self, Got, Sent};
use respan_gateway_formats::whole::Relay;
use respan_gateway_http::bytes::Bytes;
use respan_gateway_http::http::{self, HeaderMap, HeaderValue, StatusCode};
use respan_gateway_http::{Codec, Decoder, Encoder, RequestHead, ResponseHead, impl_format};
use respan_gateway_testkit::{MockTransport, mock, read_whole, serve};
use serde_json::{Value, json};

/// `GET /v1/models`, relayed unread by one provider, merged by the plugin.
#[derive(Clone)]
struct Models;

impl Codec for Models {
    type Input = Sent<Models>;
    type Output = Got<Models>;

    fn name(&self) -> &str {
        "models"
    }

    fn accepts(&self, head: &RequestHead) -> bool {
        head.method == http::Method::GET && head.uri.path() == "/v1/models"
    }

    fn request_decoder(&self, _head: &RequestHead) -> Box<dyn Decoder<Sent<Models>>> {
        opaque::request_decoder()
    }

    fn request_encoder(
        &self,
        client: Option<&RequestHead>,
        _body: &mut Bytes,
        streams: bool,
    ) -> Box<dyn Encoder<Sent<Models>, RequestHead>> {
        opaque::request_encoder(client, streams)
    }

    fn response_decoder(
        &self,
        head: &ResponseHead,
        call: &RequestHead,
        _body: &Bytes,
    ) -> Box<dyn Decoder<Got<Models>>> {
        opaque::response_decoder("models", head, call)
    }

    /// The answer's bytes under a JSON content type, as a merged list has
    /// no provider's head to borrow it from.
    fn response_encoder(
        &self,
        answer: Option<&ResponseHead>,
        _ask: &RequestHead,
        _body: &Bytes,
    ) -> Box<dyn Encoder<Got<Models>, ResponseHead>> {
        let mut head = formats::response_head(answer);
        formats::default_content_type(&mut head, "application/json");
        let write =
            |one: &[Got<Models>]| Ok(one.iter().flat_map(|g| g.0.to_vec()).collect::<Vec<u8>>());
        let fail = |fault: &Fault| {
            json!({ "error": { "message": fault.message() } })
                .to_string()
                .into_bytes()
        };
        formats::with_head(head, Box::new(Relay::new(write, fail, false)))
    }
}

impl_format!(Models);

/// Answers `GET /v1/models` with the lists of `providers` merged.
struct MergeModels(Vec<&'static str>);

#[async_trait]
impl Plugin for MergeModels {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        if !cx.is_kind::<Sent<Models>>() {
            return Ok(None);
        }
        let calls: Vec<_> = self.0.iter().map(|p| cx.call(*p)).collect();
        let lists: Vec<Result<Value, Fault>> = join_all(calls.into_iter().map(|call| async move {
            let frames: Vec<Got<Models>> = call.await?.outputs().try_collect().await?;
            let bytes: Vec<u8> = frames.iter().flat_map(|f| f.0.to_vec()).collect();
            serde_json::from_slice::<Value>(&bytes)
                .map_err(|_| Fault::upstream("a provider's models are not JSON"))
        }))
        .await;
        let (lists, failures): (Vec<_>, Vec<_>) = lists.into_iter().partition(Result::is_ok);
        if lists.is_empty() {
            return Err(failures
                .into_iter()
                .find_map(Result::err)
                .unwrap_or_else(|| Fault::upstream("no provider")));
        }
        let mut data: Vec<Value> = Vec::new();
        for model in lists
            .iter()
            .flatten()
            .flat_map(|l| l["data"].as_array().into_iter().flatten())
        {
            if !data.iter().any(|seen| seen["id"] == model["id"]) {
                data.push(model.clone());
            }
        }
        let merged = json!({ "object": "list", "data": data }).to_string();
        Ok(Some(Response::stream(stream::iter([Ok(
            Got::<Models>::new(merged),
        )]))))
    }
}

const A: &str = "http://a.test";
const B: &str = "http://b.test";

/// A key set on a provider's own transport, so the client's is not sent.
fn key(value: &'static str) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert("authorization", HeaderValue::from_static(value));
    headers
}

fn gateway(mock: &MockTransport) -> Result<Gateway, Fault> {
    Gateway::new()
        .serve(Models)?
        .provider("a", Models, mock.at(A).with_headers(key("Bearer key-a")))?
        .provider("b", Models, mock.at(B).with_headers(key("Bearer key-b")))?
        .plugin("merge", MergeModels(vec!["a", "b"]))
}

fn request() -> http::Request<respan_gateway_http::Body> {
    let body = respan_gateway_http::full("");
    match http::Request::get("/v1/models")
        .header("authorization", "Bearer client")
        .body(body)
    {
        Ok(request) => request,
        Err(_) => unreachable!("a fixed request is valid"),
    }
}

fn list(ids: &[&str]) -> mock::Reply {
    let data: Vec<Value> = ids
        .iter()
        .map(|id| json!({ "id": id, "object": "model" }))
        .collect();
    mock::json(
        StatusCode::OK,
        &json!({ "object": "list", "data": data }).to_string(),
    )
}

#[tokio::main]
async fn main() -> Result<(), Fault> {
    let mock = MockTransport::new();
    mock.reply(A, list(&["gpt-x", "shared"]))
        .reply(B, list(&["llama", "shared"]));
    let gw = gateway(&mock)?;
    let (status, _, body) = read_whole(serve(&gw, request()).await.0).await;
    println!("{status} {}", String::from_utf8_lossy(&body?));
    Ok(())
}

#[cfg(test)]
#[path = "tests/models_merge.rs"]
mod tests;
