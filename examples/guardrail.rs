//! Use case 3, a guardrail no request skips. A term list checks every
//! call's input on the provider edge, so a call any plugin makes is
//! covered, and every response the client gets through a client-edge
//! [`Holdback`], so a term split across stream deltas is seen whole, and a
//! response a plugin answers with is covered too. What it cannot read
//! (another kind, an image, a tool call) is refused, never passed: this
//! gateway also serves Anthropic clients, which the guard reads, and the
//! Responses API relayed unread, which it refuses.
//!
//! A plugin that answers skips the plugins after it, so the guard is
//! registered first; a test pins that.
//!
//! `cargo run -p respan-gateway-examples --example guardrail`

use respan_gateway_core::bytes::Bytes;
use respan_gateway_core::futures::stream;
use respan_gateway_core::http::{self, Request, StatusCode, response::Parts};
use respan_gateway_core::serde_json::{Value, json};
use respan_gateway_core::{Body, ClientContext, Decoder, Encoder, Fault, Format, Gateway};
use respan_gateway_core::{Plugin, ProviderContext, Response, async_trait};
use respan_gateway_formats::conversation::{Conversation, Reply};
use respan_gateway_formats::opaque::{self, Got, Sent};
use respan_gateway_formats::{AnthropicMessages, OpenAiChat, posts_to};
use respan_gateway_testkit::{MockTransport, mock, post, read_whole};
use respan_gateway_text::{Holdback, Readable, Rewrite, partial_match};

const OPENAI: &str = "http://openai.test/v1";
const BLOCKED: &str = "content_blocked";
const UNCOVERED: &str = "guard_uncovered";

/// Blocked terms, in lower case, matched case-insensitively. A classifier
/// fits behind the same two methods: `pending` holds what it has not
/// decided on yet.
#[derive(Clone)]
struct Terms(Vec<&'static str>);

impl Terms {
    fn check(&self, text: &str) -> Result<(), Fault> {
        let text = text.to_lowercase();
        match self.0.iter().any(|term| text.contains(term)) {
            true => Err(Fault::new(StatusCode::BAD_REQUEST, BLOCKED, "blocked")),
            false => Ok(()),
        }
    }
}

impl Rewrite for Terms {
    fn pending(&self, text: &str) -> usize {
        partial_match(&text.to_lowercase(), self.0.iter().copied())
    }

    fn rewrite(&mut self, text: &str) -> Result<String, Fault> {
        self.check(text).map(|()| text.to_owned())
    }
}

struct Guard(Terms);

fn uncovered() -> Fault {
    Fault::new(StatusCode::FORBIDDEN, UNCOVERED, "no guard reads this kind")
}

#[async_trait]
impl Plugin for Guard {
    /// Every response the client gets, whoever answers it.
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        if !cx.is_kind::<Conversation>() {
            return Err(uncovered());
        }
        cx.filter(Holdback::new(self.0.clone()))?;
        cx.filter(|reply: &mut Reply| reply.read_all().map(drop))?;
        Ok(None)
    }

    /// Every call's input, a judge's own included.
    async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
        if !px.is_kind::<Conversation>() {
            return Err(uncovered());
        }
        let terms = self.0.clone();
        px.filter(move |chat: &mut Conversation| terms.check(&chat.read_all()?))
    }
}

/// A stand-in for any plugin that answers, such as a cache: it answers the
/// model "cached" from memory, and its memory is poisoned.
struct Cache;

#[async_trait]
impl Plugin for Cache {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        if cx.input::<Conversation>()?.model.as_deref() != Some("cached") {
            return Ok(None);
        }
        let reply = [Reply::start("c0", "cached"), Reply::text(0, "a bomb")];
        Ok(Some(Response::stream(stream::iter(reply.map(Ok)))))
    }
}

/// OpenAI's Responses API, relayed byte for byte: traffic of no kind.
struct Responses;

type Req = Request<Bytes>;

impl Format for Responses {
    type Input = Sent<Responses>;
    type Output = Got<Responses>;

    fn name(&self) -> &str {
        "responses"
    }

    fn accepts(&self, head: &http::request::Parts) -> bool {
        posts_to(head, "/responses")
    }

    fn request_decoder(&self, _: &Req) -> Box<dyn Decoder<Sent<Self>>> {
        opaque::request_decoder()
    }

    fn request_encoder(&self, _: &mut Req, _: Option<&Sent<Self>>) -> Box<dyn Encoder<Sent<Self>>> {
        opaque::request_encoder()
    }

    fn response_decoder(&self, head: &Parts, _: &Req) -> Box<dyn Decoder<Got<Self>>> {
        opaque::response_decoder("responses", head)
    }

    fn response_encoder(&self, head: &mut Parts, _: &Req) -> Box<dyn Encoder<Got<Self>>> {
        opaque::response_encoder("responses", head)
    }
}

/// The guard first. For real use, the transport is
/// `HttpTransport::new("https://api.openai.com/v1")?.with_headers(key)?`.
fn gateway(mock: &MockTransport) -> Result<Gateway, Fault> {
    Gateway::new()
        .provider("openai", OpenAiChat, mock.at(OPENAI))?
        .provider("responses", Responses, mock.at(OPENAI))?
        .serve(OpenAiChat)?
        .serve(AnthropicMessages)?
        .serve(Responses)?
        .plugin("guard", Guard(Terms(vec!["bomb", "launch code"])))?
        .plugin("cache", Cache)?
        .target("openai")?
        .target("responses")
}

/// A streamed OpenAI chat of one user message.
fn chat(model: &str, content: Value) -> Request<Body> {
    let body = json!({ "model": model, "stream": true,
        "messages": [{ "role": "user", "content": content }] });
    post("/v1/chat/completions", body.to_string())
}

/// The provider streams `deltas` as OpenAI chunks.
fn deltas(deltas: &[&str]) -> mock::Reply {
    let chunk = |d| json!({ "id": "c1", "choices": [{ "index": 0, "delta": { "content": d } }] });
    let events: String = deltas
        .iter()
        .map(|d| format!("data: {}\n\n", chunk(d)))
        .collect();
    mock::sse(&[&events, "data: [DONE]\n\n"])
}

async fn send(gw: &Gateway, req: Request<Body>) -> (StatusCode, String) {
    let (status, _, body) = read_whole(gw.handle(req).await.0).await;
    let body = body.unwrap_or_else(|f| Bytes::from(f.to_string()));
    (status, String::from_utf8_lossy(&body).into_owned())
}

#[tokio::main]
async fn main() -> Result<(), Fault> {
    let mock = MockTransport::new();
    let gw = gateway(&mock)?;
    mock.reply(OPENAI, deltas(&["Chemistry is fun. A bo", "mb is not."]));
    let responses = post("/v1/responses", r#"{"input":"hi"}"#);
    for (what, req) in [
        ("term split across deltas", chat("gpt", "chemistry?".into())),
        ("poisoned cache", chat("cached", "hi".into())),
        ("the Responses API", responses),
    ] {
        let (status, body) = send(&gw, req).await;
        println!("{what}: {status}\n{body}\n");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_term_split_across_deltas_is_cut_before_its_start_is_sent() -> Result<(), Fault> {
        let mock = MockTransport::new();
        mock.reply(OPENAI, deltas(&["Chemistry is fun. A bo", "mb is not."]));
        let (status, body) = send(&gateway(&mock)?, chat("gpt", "hi".into())).await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.contains("A \"") && !body.contains("bo\""), "{body}");
        assert!(body.contains(BLOCKED) && !body.contains("[DONE]"), "{body}");
        Ok(())
    }

    #[tokio::test]
    async fn a_blocked_prompt_in_any_format_or_field_never_reaches_the_provider()
    -> Result<(), Fault> {
        let mock = MockTransport::new();
        let gw = gateway(&mock)?;
        let anthropic = json!({ "model": "c", "max_tokens": 9,
            "messages": [{ "role": "user", "content": "the launch code?" }] });
        let stop = json!({ "model": "gpt", "stop": ["BOMB"],
            "messages": [{ "role": "user", "content": "hi" }] });
        for req in [
            chat("gpt", "the LAUNCH CODE?".into()),
            post("/v1/messages", anthropic.to_string()),
            post("/v1/chat/completions", stop.to_string()),
        ] {
            let (status, body) = send(&gw, req).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
            assert!(body.contains(BLOCKED), "{body}");
        }
        assert!(mock.sent().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn what_it_cannot_read_is_refused() -> Result<(), Fault> {
        let mock = MockTransport::new();
        let gw = gateway(&mock)?;
        let image = json!([{ "type": "image_url", "image_url": { "url": "data:," } }]);
        let (status, body) = send(&gw, chat("gpt", image)).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
        // The Responses API is relayed unread, so the guard refuses it.
        let responses = post("/v1/responses", r#"{"input":"the launch code?"}"#);
        let (status, body) = send(&gw, responses).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
        assert!(mock.sent().is_empty());
        // A tool call in the answer, too.
        let call = r#"data: {"id":"c1","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"{}"}}]}}]}"#;
        mock.reply(OPENAI, mock::sse(&[&format!("{call}\n\n")]));
        let (status, body) = send(&gw, chat("gpt", "hi".into())).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
        Ok(())
    }

    // A plugin that answers skips those after it: first, the guard covers
    // the cache's answer; after it, the poisoned answer goes out.
    #[tokio::test]
    async fn the_guard_is_registered_first() -> Result<(), Fault> {
        let mock = MockTransport::new();
        let (status, body) = send(&gateway(&mock)?, chat("cached", "hi".into())).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        let late = gateway(&mock)?.after("guard", ["cache"])?;
        let (status, body) = send(&late, chat("cached", "hi".into())).await;
        assert!(status == StatusCode::OK && body.contains("bomb"), "{body}");
        Ok(())
    }
}
