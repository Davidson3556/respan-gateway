use super::*;
use respan_gateway_conversation::{Message, Role};
use respan_gateway_core::{Connection, Format, Provider};
use respan_gateway_http::{self as h, ClientIo, ProviderIo};
use respan_gateway_passthrough_http as passthrough;

/// OpenAI's Responses API, relayed byte for byte: traffic of no kind.
#[derive(Clone)]
struct Responses;

impl Format<ClientIo, passthrough::Input<Responses>, passthrough::Output<Responses>> for Responses {
    fn name(&self) -> &str {
        "responses"
    }
    fn matches(&self, io: &ClientIo) -> bool {
        io.request().method() == h::http::Method::POST
            && respan_gateway_http_syntax::clean_path(io.request().uri().path())
                .ends_with("/responses")
    }
    fn bind(
        &self,
        io: ClientIo,
    ) -> Result<Connection<passthrough::Input<Responses>, passthrough::Output<Responses>>, Fault>
    {
        Ok(io
            .with_error(h::rules::plain)
            .bind(passthrough::read_request, passthrough::write_response))
    }
}

impl Format<ProviderIo, passthrough::Output<Responses>, passthrough::Input<Responses>>
    for Responses
{
    fn name(&self) -> &str {
        "responses"
    }
    fn bind(
        &self,
        io: ProviderIo,
    ) -> Result<Connection<passthrough::Output<Responses>, passthrough::Input<Responses>>, Fault>
    {
        Ok(io.bind(passthrough::write_request, passthrough::read_response))
    }
}

/// The example's gateway, also serving the Responses API. Its one target
/// stays OpenAI; the guard refuses Responses traffic before any call.
fn gateway_with_responses(
    transport: impl Fn(respan_gateway_core::Extensions) -> respan_gateway_http::ProviderIo
    + Clone
    + Send
    + Sync
    + 'static,
) -> Result<Gateway, Fault> {
    gateway(transport.clone())?
        .provider(Provider::new("responses", Responses, transport))?
        .client::<passthrough::Input<Responses>, _, _>(Responses)
}

#[tokio::test]
async fn a_term_split_across_deltas_is_cut_before_its_start_is_sent() -> Result<(), Fault> {
    let fake = FakeOpenAi::start().await;
    fake.answer(deltas(&["Chemistry is fun. A bo", "mb is not."]));
    let (status, body) = send(
        &gateway(provider_support::openai(&fake.base_url())?.connect())?,
        chat("gpt", "hi".into()),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("A \"") && !body.contains("bo\""), "{body}");
    assert!(body.contains(BLOCKED) && !body.contains("[DONE]"), "{body}");
    assert_eq!(fake.requests().len(), 1);
    Ok(())
}

#[tokio::test]
async fn a_blocked_prompt_in_any_format_or_field_never_reaches_the_provider() -> Result<(), Fault> {
    let fake = FakeOpenAi::start().await;
    let gw = gateway(provider_support::openai(&fake.base_url())?.connect())?;
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
    assert!(fake.requests().is_empty());
    Ok(())
}

#[tokio::test]
async fn what_it_cannot_read_is_refused() -> Result<(), Fault> {
    let fake = FakeOpenAi::start().await;
    let gw = gateway_with_responses(provider_support::openai(&fake.base_url())?.connect())?;
    let image = json!([{ "type": "image_url", "image_url": { "url": "data:," } }]);
    let (status, body) = send(&gw, chat("gpt", image)).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    // The Responses API is relayed unread, so the guard refuses it.
    let responses = post("/v1/responses", r#"{"input":"the launch code?"}"#);
    let (status, body) = send(&gw, responses).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    assert!(fake.requests().is_empty());
    // A tool call in the answer, too.
    let call = r#"data: {"id":"c1","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"t","type":"function","function":{"name":"f","arguments":"{}"}}]}}]}"#;
    fake.answer(provider_support::sse(&[&format!("{call}\n\n")]));
    let (status, body) = send(&gw, chat("gpt", "hi".into())).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert_eq!(fake.requests().len(), 1);
    Ok(())
}

// A plugin that answers skips those after it: first, the guard covers
// the cache's answer; after it, the poisoned answer goes out.
#[tokio::test]
async fn the_guard_is_registered_first() -> Result<(), Fault> {
    let fake = FakeOpenAi::start().await;
    let (status, body) = send(
        &gateway(provider_support::openai(&fake.base_url())?.connect())?,
        chat("cached", "hi".into()),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    let late = guarded(
        base(provider_support::openai(&fake.base_url())?.connect())?.plugin(Cache),
        |gw| gw,
    )?;
    let (status, body) = send(&late, chat("cached", "hi".into())).await;
    assert!(status == StatusCode::OK && body.contains("bomb"), "{body}");
    assert!(fake.requests().is_empty());
    Ok(())
}

/// A plugin that adds to each call's input, as a RAG
/// or memory plugin does.
struct Rag;

#[async_trait]
impl Plugin for Rag {
    async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
        px.filter(|chat: &mut Input| {
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
    let fake = FakeOpenAi::start().await;
    let gw = gateway_with(
        provider_support::openai(&fake.base_url())?.connect(),
        |gw| gw.plugin(Rag),
    )?;
    let (status, body) = send(&gw, chat("gpt", "hi".into())).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert!(fake.requests().is_empty());
    fake.answer(deltas(&["ok"]));
    let (status, _) = send(
        &gateway(provider_support::openai(&fake.base_url())?.connect())?.plugin(Rag),
        chat("gpt", "hi".into()),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let sent = fake.requests();
    assert_eq!(sent.len(), 1);
    assert!(String::from_utf8_lossy(&sent[0].raw).contains("launch code"));
    Ok(())
}

#[tokio::test]
async fn a_request_with_tools_is_checked_and_passes() -> Result<(), Fault> {
    let fake = FakeOpenAi::start().await;
    let gw = gateway(provider_support::openai(&fake.base_url())?.connect())?;
    let tools = |description: &str| {
        let body = json!({ "model": "gpt", "stream": true, "messages": [{ "role": "user", "content": "hi" }],
            "tools": [{ "type": "function", "function": { "name": "f", "description": description } }] });
        post("/v1/chat/completions", body.to_string())
    };
    let (status, body) = send(&gw, tools("builds a bomb")).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    fake.answer(deltas(&["ok"]));
    let (status, body) = send(&gw, tools("reads the weather")).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(fake.requests().len(), 1);
    Ok(())
}
