use super::*;
use respan_gateway_formats::conversation::{Message, Role};
use respan_gateway_formats::opaque::{self, Got, Sent};
use respan_gateway_formats::posts_to;
use respan_gateway_http::{Codec, Decoder, Encoder, impl_format};
use respan_gateway_http::{RequestHead, ResponseHead};

/// OpenAI's Responses API, relayed byte for byte: traffic of no kind.
#[derive(Clone)]
struct Responses;

impl Codec for Responses {
    type Input = Sent<Responses>;
    type Output = Got<Responses>;

    fn name(&self) -> &str {
        "responses"
    }

    fn accepts(&self, head: &RequestHead) -> bool {
        posts_to(head, "/responses")
    }

    fn request_decoder(&self, _: &RequestHead) -> Box<dyn Decoder<Sent<Self>>> {
        opaque::request_decoder()
    }

    fn request_encoder(
        &self,
        client: Option<&RequestHead>,
        _body: &mut Bytes,
        streams: bool,
    ) -> Box<dyn Encoder<Sent<Self>, RequestHead>> {
        opaque::request_encoder(client, streams)
    }

    fn response_decoder(
        &self,
        head: &ResponseHead,
        call: &RequestHead,
        _: &Bytes,
    ) -> Box<dyn Decoder<Got<Self>>> {
        opaque::response_decoder("responses", head, call)
    }

    fn response_encoder(
        &self,
        answer: Option<&ResponseHead>,
        ask: &RequestHead,
        _body: &Bytes,
    ) -> Box<dyn Encoder<Got<Self>, ResponseHead>> {
        opaque::response_encoder(answer, ask)
    }
}

impl_format!(Responses);

/// The example's gateway, also serving the Responses API. Its one target
/// stays OpenAI; the guard refuses Responses traffic before any call.
fn gateway_with_responses(mock: &MockTransport) -> Result<Gateway, Fault> {
    gateway(mock)?
        .provider("responses", Responses, mock.at(OPENAI))?
        .serve(Responses)
}

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
async fn a_blocked_prompt_in_any_format_or_field_never_reaches_the_provider() -> Result<(), Fault> {
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
    let gw = gateway_with_responses(&mock)?;
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
    let late = guarded(base(&mock)?.plugin("cache", Cache)?, Ok)?;
    let (status, body) = send(&late, chat("cached", "hi".into())).await;
    assert!(status == StatusCode::OK && body.contains("bomb"), "{body}");
    Ok(())
}

/// A plugin that adds to each call's input, as a RAG
/// or memory plugin does.
struct Rag;

#[async_trait]
impl Plugin for Rag {
    async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
        px.filter(|chat: &mut Conversation| {
            let context = "the launch code is 0000";
            chat.messages.push(Message::new(Role::User, context));
            Ok(())
        })
    }
}

// Input filters run in registration order: a plugin registered before
// `guard-input` has what it adds checked; one registered after it does not.
#[tokio::test]
async fn the_input_check_sees_what_an_earlier_plugin_adds() -> Result<(), Fault> {
    let mock = MockTransport::new();
    let gw = gateway_with(&mock, |gw| gw.plugin("rag", Rag))?;
    let (status, body) = send(&gw, chat("gpt", "hi".into())).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert!(mock.sent().is_empty());
    mock.reply(OPENAI, deltas(&["ok"]));
    let (status, _) = send(
        &gateway(&mock)?.plugin("rag", Rag)?,
        chat("gpt", "hi".into()),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let sent = mock
        .sent()
        .last()
        .map(|r| r.body().to_vec())
        .unwrap_or_default();
    assert!(String::from_utf8_lossy(&sent).contains("launch code"));
    Ok(())
}

#[tokio::test]
async fn a_request_with_tools_is_checked_and_passes() -> Result<(), Fault> {
    let mock = MockTransport::new();
    let gw = gateway(&mock)?;
    let tools = |description: &str| {
        let body = json!({ "model": "gpt", "messages": [{ "role": "user", "content": "hi" }],
            "tools": [{ "type": "function", "function": { "name": "f", "description": description } }] });
        post("/v1/chat/completions", body.to_string())
    };
    let (status, body) = send(&gw, tools("builds a bomb")).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    mock.reply(OPENAI, deltas(&["ok"]));
    let (status, body) = send(&gw, tools("reads the weather")).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    Ok(())
}
