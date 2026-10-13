//! Whose key a call carries, against mock providers without keys.

use mockllm::{Answer, FakeOpenAi};
use respan_gateway_conversation::Input;
use respan_gateway_conversation_http::openai::OpenAiChat;
use respan_gateway_core::{ClientContext, Fault, Gateway, Plugin, Provider, Response, async_trait};
use respan_gateway_http::http::{HeaderValue, header::AUTHORIZATION};
use respan_gateway_http_transport::HttpTransport;
use respan_gateway_plugin_credentials::ClientCredentials;

#[allow(dead_code)] // The shared helper also exposes streamed requests, unused here.
#[path = "../../../../tests/support/http.rs"]
mod http_support;
use http_support::{LIMIT, post, read_whole, serve};

const ASK: &str = r#"{"model":"m","messages":[{"role":"user","content":"hi"}]}"#;

/// Targets every request at one provider.
struct To(&'static str);

#[async_trait]
impl Plugin for To {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        cx.set_target(self.0);
        Ok(None)
    }
}

/// The `Authorization` the provider `target` heard, of `keyless` named.
async fn heard(keyless: &[&str], target: &'static str) -> Option<String> {
    let fake = FakeOpenAi::start().await;
    fake.answer(Answer::text("ok"));
    let provider = |name: &str| {
        let http = HttpTransport::new(fake.base_url()).unwrap_or_else(|f| panic!("{f}"));
        let send = move |call| {
            let http = http.clone();
            async move { http.send(call).await }
        };
        Provider::new(name, OpenAiChat, respan_gateway_http::connect(send, LIMIT))
    };
    let gateway = Gateway::new()
        .client::<Input, _, _>(OpenAiChat)
        .expect("client")
        .provider(provider("keyless"))
        .expect("keyless")
        .provider(provider("other"))
        .expect("other")
        .plugin(To(target))
        .plugin(ClientCredentials::new(keyless.iter().copied()));
    let mut req = post("/v1/chat/completions", ASK);
    req.headers_mut()
        .insert(AUTHORIZATION, HeaderValue::from_static("Bearer sk-client"));
    let (resp, outcome) = serve(&gateway, req).await;
    // The mock refuses a call without a key; what it heard is the point.
    let _ = read_whole(resp).await;
    let _ = outcome.await;
    let sent = fake.requests();
    sent[0].header("authorization").map(str::to_owned)
}

#[tokio::test]
async fn a_named_provider_gets_the_clients_key() {
    assert_eq!(
        heard(&["keyless"], "keyless").await.as_deref(),
        Some("Bearer sk-client")
    );
}

#[tokio::test]
async fn a_provider_not_named_gets_no_key() {
    assert_eq!(heard(&["keyless"], "other").await, None);
    assert_eq!(heard(&[], "keyless").await, None);
}
