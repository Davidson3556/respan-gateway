//! Probes of the round-9 core: whose fault untranslatable content is when a
//! plugin changed the input (V9-1), and `prepare` seeing the input (K9-8),
//! for a protocol that puts the model in the path, as Gemini does.

mod common;

use std::sync::{Arc, Mutex, PoisonError};

use common::*;
use respan_gateway_core::bytes::{Bytes, BytesMut};
use respan_gateway_core::http::{self, StatusCode};
use respan_gateway_core::serde_json::{self, Value, json};
use respan_gateway_core::{
    ClientContext, Decoder, Doc, Encoder, Fault, Format, Gateway, Item, Part, Plugin,
    ProviderContext, Response, async_trait,
};
use respan_gateway_formats::conversation::{Content, Conversation};
use respan_gateway_formats::{AnthropicMessages, OpenAiChat, pass_headers};
use respan_gateway_testkit::{MockTransport, post};

// ----------------------------------------------------- V9-1: whose fault

fn translating(mock: &MockTransport, plugin: impl Plugin) -> Gateway {
    Gateway::new()
        .provider("anthropic", AnthropicMessages, mock.at(ANTHROPIC))
        .and_then(|g| g.serve(OpenAiChat))
        .and_then(|g| g.plugin("edit", plugin))
        .and_then(|g| g.target("anthropic"))
        .expect("gateway")
}

fn file_part() -> Value {
    json!({"type": "file", "file": {"file_id": "file-abc"}})
}

/// Adds a part of its own that no other format can say.
struct AddsFile;

#[async_trait]
impl Plugin for AddsFile {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        cx.filter(|chat: &mut Conversation| {
            if let Some(m) = chat.messages.last_mut() {
                m.content.push(Content::other(file_part()));
            }
            Ok(())
        })?;
        Ok(None)
    }
}

/// Replaces every part the kind has no form for with one of its own, just
/// as untranslatable: the client's part never goes out.
struct SwapsFile;

#[async_trait]
impl Plugin for SwapsFile {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        cx.filter(|chat: &mut Conversation| {
            for m in &mut chat.messages {
                for c in &mut m.content {
                    if matches!(c, Content::Other { .. }) {
                        *c = Content::other(json!({"type": "input_audio", "input_audio": {}}));
                    }
                }
            }
            Ok(())
        })?;
        Ok(None)
    }
}

/// A plugin that brings untranslatable content to a translatable request
/// is the operator's fault (500 `config`).
#[tokio::test]
async fn content_a_plugin_adds_is_the_operators_fault() {
    let mock = MockTransport::new();
    let gw = translating(&mock, AddsFile);

    let (status, _, body) = send(&gw, chat("gpt-x", false, "hi")).await;

    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "{body:?}");
    assert!(body.is_ok_and(|b| b.contains("\"config\"")));
    assert!(mock.sent().is_empty());
}

/// V9-1's counterfactual: the plugin replaced the client's untranslatable
/// part with its own, so what fails is the plugin's content, yet the client
/// is blamed (400), since its own input would have failed too. Pinned as
/// defensible: the client's request could not go to this provider either.
#[tokio::test]
async fn a_plugin_that_swaps_the_clients_bad_part_for_its_own_blames_the_client() {
    let mock = MockTransport::new();
    let gw = translating(&mock, SwapsFile);
    let req = chat_with(
        json!({"model": "gpt-x", "messages": [{"role": "user", "content": [
        {"type": "text", "text": "summarize"}, file_part()]}]}),
    );

    let (status, _, body) = send(&gw, req).await;

    assert_eq!(status, StatusCode::BAD_REQUEST, "{body:?}");
}

// ------------------------------------------- K9-8: the model in the path

/// An embeddings request with the model in its path, as Gemini's
/// `/models/{model}:embedContent`.
#[derive(Debug, Clone, PartialEq)]
struct Embed {
    model: String,
    text: String,
}

#[derive(Debug, Clone, PartialEq)]
struct Vector(Vec<f64>);

/// The model `prepare` got with the input, if any, and whether relayed.
type Seen = (Option<String>, bool);

/// What each `prepare` got.
#[derive(Clone, Default)]
struct Prepared(Arc<Mutex<Vec<Seen>>>);

impl Prepared {
    fn all(&self) -> Vec<Seen> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

struct PathEmbed(Prepared);

fn model_in(path: &str) -> Option<&str> {
    path.strip_prefix("/models/")?.strip_suffix(":embed")
}

impl Format for PathEmbed {
    type Input = Embed;
    type Output = Vector;

    fn name(&self) -> &str {
        "path-embed"
    }

    fn accepts(&self, head: &http::request::Parts) -> bool {
        head.method == http::Method::POST && model_in(head.uri.path()).is_some()
    }

    fn decode_input(&self, head: &http::request::Parts, body: &Doc) -> Result<Embed, Fault> {
        let model = model_in(head.uri.path()).unwrap_or_default().to_owned();
        let text = body.json().and_then(|v| v["text"].as_str());
        let text = text.ok_or_else(|| Fault::bad_request("no text"))?;
        Ok(Embed {
            model,
            text: text.into(),
        })
    }

    fn encode_input(&self, input: &Embed, _: Option<(&Doc, &Embed)>) -> Result<Doc, Fault> {
        Ok(Doc::from(json!({ "text": input.text })))
    }

    fn prepare(
        &self,
        body: Doc,
        input: Option<&Embed>,
        client: &http::request::Parts,
        relayed: bool,
    ) -> Result<http::Request<Doc>, Fault> {
        let seen = (input.map(|i| i.model.clone()), relayed);
        self.0
            .0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(seen);
        // The model is the input's when the kernel holds it; relayed unread,
        // it is the client's path.
        let model = match input {
            Some(input) => input.model.as_str(),
            None => model_in(client.uri.path()).unwrap_or_default(),
        };
        let mut req = http::Request::new(body);
        *req.method_mut() = http::Method::POST;
        *req.uri_mut() = format!("/models/{model}:embed")
            .parse()
            .map_err(|_| Fault::bad_request("bad model"))?;
        pass_headers(&mut req, relayed.then_some(client));
        Ok(req)
    }

    fn decoder(&self, _: &http::response::Parts, _: &http::Request<Doc>) -> Box<dyn Decoder> {
        Box::new(WholeVector(BytesMut::new()))
    }

    fn encoder(
        &self,
        _: &mut http::response::Parts,
        _: &http::Request<Doc>,
        _: bool,
    ) -> Box<dyn Encoder> {
        Box::new(WriteVector)
    }
}

struct WholeVector(BytesMut);

impl Decoder for WholeVector {
    fn decode(&mut self, bytes: Bytes, _: &mut Vec<Part>) {
        self.0.extend_from_slice(&bytes);
    }

    fn end(&mut self, out: &mut Vec<Part>) {
        let raw = self.0.split().freeze();
        let v: Value = serde_json::from_slice(&raw).unwrap_or_default();
        let vector = v["embedding"].as_array().cloned().unwrap_or_default();
        let vector = Vector(vector.iter().filter_map(Value::as_f64).collect());
        out.push(Item::decoded([vector], raw).into());
        out.push(Part::End(Ok(())));
    }
}

struct WriteVector;

impl Encoder for WriteVector {
    fn encode(&mut self, part: &Part, out: &mut BytesMut) -> Result<(), Fault> {
        if let Part::Item(item) = part {
            for v in item.outputs::<Vector>()? {
                out.extend_from_slice(json!({ "embedding": v.0 }).to_string().as_bytes());
            }
        }
        Ok(())
    }
}

const GEMINI: &str = "http://gemini.test";

fn embed_gateway(mock: &MockTransport, prepared: &Prepared, plugin: impl Plugin) -> Gateway {
    Gateway::new()
        .provider("gemini", PathEmbed(prepared.clone()), mock.at(GEMINI))
        .and_then(|g| g.serve(PathEmbed(prepared.clone())))
        .and_then(|g| g.plugin("p", plugin))
        .and_then(|g| g.target("gemini"))
        .expect("gateway")
}

fn embed_req(model: &str) -> http::Request<respan_gateway_core::Body> {
    post(&format!("/models/{model}:embed"), r#"{"text":"hi"}"#)
}

fn sent_path(mock: &MockTransport) -> String {
    mock.sent()[0].uri().path().to_owned()
}

struct Nothing;

#[async_trait]
impl Plugin for Nothing {}

/// Reads the input and changes nothing.
struct Reads;

#[async_trait]
impl Plugin for Reads {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        cx.input::<Embed>()?;
        Ok(None)
    }
}

/// Routes to a bigger model with an input filter.
struct Upgrades;

#[async_trait]
impl Plugin for Upgrades {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        cx.filter(|e: &mut Embed| {
            e.model = "embed-big".into();
            Ok(())
        })?;
        Ok(None)
    }
}

fn reply(mock: &MockTransport) {
    mock.reply(GEMINI, json_reply(StatusCode::OK, r#"{"embedding":[1.0]}"#));
}

/// K9-8 fixed: a model changed by a filter on a same-format relay reaches
/// the path, since `prepare` sees the changed input.
#[tokio::test]
async fn a_model_changed_by_a_filter_reaches_the_path() {
    let (mock, prepared) = (MockTransport::new(), Prepared::default());
    let gw = embed_gateway(&mock, &prepared, Upgrades);
    reply(&mock);

    let (status, _, _) = send(&gw, embed_req("embed-small")).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(sent_path(&mock), "/models/embed-big:embed");
    assert_eq!(prepared.all(), [(Some("embed-big".to_owned()), true)]);
}

/// Was "whether_prepare_sees_the_input_depends_on_another_plugins_read":
/// fixed by T10-6. The same unchanged relay gives `prepare` `None` whether
/// or not some plugin read the input, as its body is the client's own bytes.
#[tokio::test]
async fn whether_a_plugin_read_the_input_does_not_change_what_prepare_sees() {
    let (mock, prepared) = (MockTransport::new(), Prepared::default());
    let unread = embed_gateway(&mock, &prepared, Nothing);
    let read = embed_gateway(&mock, &prepared, Reads);
    reply(&mock);
    reply(&mock);

    assert_eq!(
        send(&unread, embed_req("embed-small")).await.0,
        StatusCode::OK
    );
    assert_eq!(
        send(&read, embed_req("embed-small")).await.0,
        StatusCode::OK
    );

    assert_eq!(prepared.all(), [(None, true), (None, true)]);
    assert_eq!(mock.sent()[1].uri().path(), "/models/embed-small:embed");
}

// ------------------------------------------- round 10: when prepare sees it

/// An input filter that leaves the input equal to the client's.
struct Identity;

#[async_trait]
impl Plugin for Identity {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        cx.filter(|e: &mut Embed| {
            e.model = e.model.clone();
            Ok(())
        })?;
        Ok(None)
    }
}

/// Routes to a bigger model on the provider edge.
struct UpgradesOnProvider;

#[async_trait]
impl Plugin for UpgradesOnProvider {
    async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
        px.filter(|e: &mut Embed| {
            e.model = "embed-big".into();
            Ok(())
        })
    }
}

/// Answers with a call that routes to a bigger model with its own filter.
struct UpgradesOneCall;

#[async_trait]
impl Plugin for UpgradesOneCall {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let call = cx.call("gemini").with_filter(|e: &mut Embed| {
            e.model = "embed-big".into();
            Ok(())
        });
        Ok(Some(call.await?))
    }
}

async fn seen_with(plugin: impl Plugin) -> (Vec<Seen>, String) {
    let (mock, prepared) = (MockTransport::new(), Prepared::default());
    let gw = embed_gateway(&mock, &prepared, plugin);
    reply(&mock);
    assert_eq!(send(&gw, embed_req("embed-small")).await.0, StatusCode::OK);
    (prepared.all(), sent_path(&mock))
}

/// T10-6: a filter that leaves the input equal is no change: the body is
/// the client's, and `prepare` gets `None`, as for an unread relay.
#[tokio::test]
async fn a_filter_that_changes_nothing_leaves_prepare_none() {
    let (seen, path) = seen_with(Identity).await;
    assert_eq!(seen, [(None, true)]);
    assert_eq!(path, "/models/embed-small:embed");
}

/// The model changed on the provider edge, or by one call's own filter,
/// reaches `prepare` just as a client-edge change does (K9-8).
#[tokio::test]
async fn a_model_changed_on_the_provider_edge_or_for_one_call_reaches_the_path() {
    for (seen, path) in [
        seen_with(UpgradesOnProvider).await,
        seen_with(UpgradesOneCall).await,
    ] {
        assert_eq!(seen, [(Some("embed-big".to_owned()), true)]);
        assert_eq!(path, "/models/embed-big:embed");
    }
}

/// N10-R1, low (docs): here the changed input encodes to the client's own
/// bytes (the model is only in the path), yet `prepare` gets `Some` with
/// `relayed = true`. The rule is "the input differs from the client's",
/// not "the body is not the client's bytes" as `prepare`'s rustdoc says,
/// and `relayed`'s "relays the client's own body" is not literally true of
/// a changed relay. The behavior is right; the wording names two tests
/// where there is one.
#[tokio::test]
async fn a_change_that_encodes_to_the_clients_bytes_still_gives_prepare_the_input() {
    let (mock, prepared) = (MockTransport::new(), Prepared::default());
    let gw = embed_gateway(&mock, &prepared, Upgrades);
    reply(&mock);
    assert_eq!(send(&gw, embed_req("embed-small")).await.0, StatusCode::OK);
    assert_eq!(mock.sent()[0].body(), &Bytes::from(r#"{"text":"hi"}"#));
    assert_eq!(prepared.all(), [(Some("embed-big".to_owned()), true)]);
}
