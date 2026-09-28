//! Probes of the round-8 core: the model left the kernel. It is a field of
//! the conversation input, changed by a filter (`conversation::set_model`).

mod common;

use std::sync::{Arc, Mutex, PoisonError};

use common::*;
use respan_gateway_core::bytes::{Bytes, BytesMut};
use respan_gateway_core::futures::TryStreamExt;
use respan_gateway_core::http::header::UPGRADE;
use respan_gateway_core::http::{self, HeaderValue, StatusCode};
use respan_gateway_core::http_body::Frame;
use respan_gateway_core::http_body_util::{BodyExt, StreamBody};
use respan_gateway_core::serde_json::json;
use respan_gateway_core::{
    Body, ClientContext, Decoder, Doc, Encoder, Fault, Format, Gateway, Item, Part, Plugin,
    Response, Transport, async_trait,
};
use respan_gateway_formats::conversation::{Conversation, Message, Reply, Role, set_model};
use respan_gateway_formats::{AnthropicMessages, OpenAiChat};
use respan_gateway_routing::fallback::Fallback;
use respan_gateway_routing::retry::Backoff;
use respan_gateway_testkit::{MockTransport, read_whole, streamed};

// ------------------------------------------- blame for a changed model

fn fallback_gateway(mock: &MockTransport, model: Option<&str>) -> Gateway {
    Gateway::new()
        .provider("openai", OpenAiChat, mock.at(OPENAI))
        .and_then(|g| g.provider("anthropic", AnthropicMessages, mock.at(ANTHROPIC)))
        .and_then(|g| g.serve(OpenAiChat))
        .and_then(|g| {
            g.plugin(
                "fallback",
                Fallback::new(Backoff::once()).then("anthropic", model),
            )
        })
        .and_then(|g| g.target("openai"))
        .expect("gateway")
}

/// A chat with a file part, which the conversation kind keeps in the
/// client's shape and cannot translate.
fn with_file() -> http::Request<respan_gateway_core::Body> {
    chat_with(
        json!({"model": "gpt-x", "messages": [{"role": "user", "content": [
        {"type": "text", "text": "summarize"},
        {"type": "file", "file": {"file_id": "file-abc"}}]}]}),
    )
}

/// A fallback with no model: the client's content cannot be translated,
/// and the client is told so (400).
#[tokio::test]
async fn untranslatable_client_content_is_the_clients_fault() {
    let mock = MockTransport::new();
    let gw = fallback_gateway(&mock, None);
    mock.reply(OPENAI, json_reply(StatusCode::SERVICE_UNAVAILABLE, "{}"));

    let (status, _, body) = send(&gw, with_file()).await;

    assert_eq!(status, StatusCode::BAD_REQUEST, "{body:?}");
}

/// R8-A, fixed by V9-1: the same fallback, asking the backup for a model.
/// Asking for a model changes the input, but the client's own input cannot
/// be translated either, so the fault is still the client's (400).
#[tokio::test]
async fn asking_for_a_model_does_not_make_the_clients_content_the_operators_fault() {
    let mock = MockTransport::new();
    let gw = fallback_gateway(&mock, Some("claude-x"));
    mock.reply(OPENAI, json_reply(StatusCode::SERVICE_UNAVAILABLE, "{}"));

    let (status, _, body) = send(&gw, with_file()).await;

    assert_eq!(status, StatusCode::BAD_REQUEST, "{body:?}");
}

// ------------------------------------- a model in an upgrading head

/// A realtime chat, like OpenAI's Realtime API: the model is in the query
/// of the upgrading head (`/rt?model=...`), one `{"say": ...}` per frame.
struct Realtime;

fn model_of(head: &http::request::Parts) -> Option<&str> {
    head.uri.query()?.strip_prefix("model=")
}

impl Format for Realtime {
    type Input = Conversation;
    type Output = Reply;

    fn name(&self) -> &str {
        "realtime"
    }

    fn accepts(&self, head: &http::request::Parts) -> bool {
        head.uri.path() == "/rt"
    }

    fn decode_input(&self, head: &http::request::Parts, body: &Doc) -> Result<Conversation, Fault> {
        let say = body.json().and_then(|v| v["say"].as_str());
        let say = say.ok_or_else(|| Fault::bad_request("not realtime"))?;
        let mut chat = Conversation::new([Message::new(Role::User, say)]);
        chat.model = model_of(head).map(str::to_owned);
        Ok(chat)
    }

    fn encode_input(
        &self,
        chat: &Conversation,
        original: Option<(&Doc, &Conversation)>,
    ) -> Result<Doc, Fault> {
        // A frame has no place for the model: it was chosen in the head.
        // Relayed, the original tells a changed model, so it is refused
        // rather than dropped; translated in, nothing tells.
        if original.is_some_and(|(_, was)| was.model != chat.model) {
            return Err(Fault::bad_request("the model is chosen in the head"));
        }
        let text = chat.messages.last().map(Message::text).unwrap_or_default();
        Ok(Doc::from(json!({ "say": text })))
    }

    fn prepare(
        &self,
        body: Doc,
        _: Option<&Conversation>,
        client: &http::request::Parts,
        _: bool,
    ) -> Result<http::Request<Doc>, Fault> {
        let mut call = http::Request::new(body);
        let uri = format!("/rt?model={}", model_of(client).unwrap_or_default());
        *call.uri_mut() = uri.parse().map_err(|_| Fault::bad_request("bad model"))?;
        call.headers_mut()
            .insert(UPGRADE, HeaderValue::from_static("websocket"));
        Ok(call)
    }

    fn decoder(&self, _: &http::response::Parts, _: &http::Request<Doc>) -> Box<dyn Decoder> {
        Box::new(Heard)
    }

    fn encoder(
        &self,
        head: &mut http::response::Parts,
        _: &http::Request<Doc>,
        _: bool,
    ) -> Box<dyn Encoder> {
        if head.status == StatusCode::OK {
            head.status = StatusCode::SWITCHING_PROTOCOLS;
            head.headers
                .insert(UPGRADE, HeaderValue::from_static("websocket"));
        }
        Box::new(Write)
    }
}

struct Heard;

impl Decoder for Heard {
    fn decode(&mut self, bytes: Bytes, out: &mut Vec<Part>) {
        out.push(Item::decoded([Reply::text(0, "heard")], bytes).into());
    }

    fn end(&mut self, out: &mut Vec<Part>) {
        out.push(Part::End(Ok(())));
    }
}

struct Write;

impl Encoder for Write {
    fn encode(&mut self, part: &Part, out: &mut BytesMut) -> Result<(), Fault> {
        match part {
            Part::Item(item) => out.extend_from_slice(item.unedited().map_or(&[][..], |b| b)),
            Part::End(Err(f)) => {
                out.extend_from_slice(format!("error: {}", f.message()).as_bytes())
            }
            _ => {}
        }
        Ok(())
    }
}

/// Echoes each frame, noting the call's URI.
#[derive(Clone, Default)]
struct Echo(Arc<Mutex<Vec<String>>>);

#[async_trait]
impl Transport for Echo {
    async fn send(&self, call: http::Request<Body>) -> Result<http::Response<Body>, Fault> {
        let (head, body) = call.into_parts();
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(head.uri.to_string());
        let echoed = body.into_data_stream().map_ok(Frame::data);
        let mut resp = http::Response::new(StreamBody::new(echoed).boxed_unsync());
        *resp.status_mut() = StatusCode::SWITCHING_PROTOCOLS;
        resp.headers_mut()
            .insert(UPGRADE, HeaderValue::from_static("websocket"));
        Ok(resp)
    }
}

/// Asks for `rt-big` with the kind's own helper.
struct Upgrade;

#[async_trait]
impl Plugin for Upgrade {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        cx.filter(set_model("rt-big"))?;
        Ok(None)
    }
}

/// Asks for `rt-big` the only way that works on an upgrading request:
/// rewriting the head in the format's own spelling.
struct UpgradeHead;

#[async_trait]
impl Plugin for UpgradeHead {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let head = cx.head_mut();
        head.uri = format!("{}?model=rt-big", head.uri.path())
            .parse()
            .map_err(|_| Fault::internal("bad uri"))?;
        Ok(None)
    }
}

async fn realtime(plugin: impl Plugin) -> (Vec<String>, String) {
    let echo = Echo::default();
    let gw = Gateway::new()
        .provider("rt", Realtime, echo.clone())
        .and_then(|g| g.serve(Realtime)?.plugin("p", plugin))
        .and_then(|g| g.target("rt"))
        .expect("gateway");
    let (send, mut req) = streamed("/rt?model=rt-small");
    req.headers_mut()
        .insert(UPGRADE, HeaderValue::from_static("websocket"));
    send.unbounded_send(Bytes::from(r#"{"say":"hi"}"#)).ok();
    drop(send);
    let (status, _, body) = read_whole(gw.handle(req).await.0).await;
    gw.idle().await;
    assert_eq!(status, StatusCode::SWITCHING_PROTOCOLS);
    let uris = echo
        .0
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .clone();
    let body = body.map(|b| String::from_utf8_lossy(&b).into_owned());
    (uris, body.unwrap_or_else(|f| format!("aborted: {f}")))
}

/// R8-B (pin): `set_model` on an upgrading request whose model is in its
/// head cannot reach the call: the head went out with the old model before
/// any frame, and the best the format can do is refuse each frame. Before
/// M8-1 the kernel held the model and `prepare` wrote it into the head.
#[tokio::test]
async fn today_set_model_on_a_realtime_request_never_reaches_the_call() {
    let (uris, body) = realtime(Upgrade).await;
    assert_eq!(uris, ["/rt?model=rt-small"]);
    assert!(body.contains("the model is chosen in the head"), "{body}");
}

/// The workaround: the plugin edits the head in the format's spelling,
/// so a model router has to know every upgrading format's query key.
#[tokio::test]
async fn a_realtime_model_is_changed_by_rewriting_the_head() {
    let (uris, body) = realtime(UpgradeHead).await;
    assert_eq!(uris, ["/rt?model=rt-big"]);
    assert_eq!(body, r#"{"say":"hi"}"#);
}
