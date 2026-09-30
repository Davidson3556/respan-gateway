//! An OpenAI relay harness, shared by the testkit's `relay.rs` and the
//! http crate's `rules.rs`.

// Each test file uses only some helpers.
#![allow(dead_code)]

use std::sync::Arc;

use respan_gateway_core::{
    ClientContext, Fault, Gateway, Plugin, ProviderContext, Response, async_trait,
};
use respan_gateway_http::http::{HeaderMap, HeaderValue};
use respan_gateway_messages::conversation::Conversation;
use respan_gateway_providers::OpenAiChat;
use respan_gateway_testkit::{MockTransport, Outcomes};

use crate::common::{
    AddProvider, CHAT, Call, Calls, Harness, Metered, OPENAI, add_provider, call, providers,
    with_plugins,
};

pub const BACKUP: &str = "http://backup.test/v1";

pub const STREAM: &[&str] = &[
    "data: {\"id\":\"c1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Hel",
    "lo\"}}]}\n\ndata: {\"id\":\"c1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\" world\"}}]}\n\n",
    "data: {\"id\":\"c1\",\"choices\":[],\"usage\":{\"prompt_tokens\":5,\"completion_tokens\":2}}\n\n",
    "data: [DONE]\n\n",
];

pub const STREAM_REQ: &str = r#"{"model":"gpt-x","stream":true,"stream_options":{"include_usage":true},"messages":[{"role":"user","content":"hi"}]}"#;
pub const UNARY_REQ: &str = r#"{"model":"gpt-x",  "messages":[]}"#;
pub const UNARY: &str = r#"{"id":"c2","choices":[{"message":{"content":"hi"}}],"usage":{"prompt_tokens":4,"completion_tokens":1}}"#;

pub fn chat() -> Call {
    call(CHAT, STREAM_REQ)
}

pub fn unary_chat() -> Call {
    call(CHAT, UNARY_REQ)
}

/// Sets the temperature of every request.
pub struct Cool;

#[async_trait]
impl Plugin for Cool {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        cx.filter(|chat: &mut Conversation| {
            chat.temperature = Some(0.0);
            Ok(())
        })?;
        Ok(None)
    }
}

/// Runs `f` on the provider edge of every call.
pub struct OnProvider(pub fn(&mut ProviderContext) -> Result<(), Fault>);

#[async_trait]
impl Plugin for OnProvider {
    async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
        (self.0)(px)
    }
}

/// A provider at `base_url` with its own key, or with no headers, so the
/// client's credentials pass through.
pub fn provider(
    mock: &MockTransport,
    name: &str,
    base_url: &str,
    key: Option<&'static str>,
) -> AddProvider {
    let mut headers = HeaderMap::new();
    if let Some(k) = key {
        headers.insert("authorization", HeaderValue::from_static(k));
    }
    add_provider(name, OpenAiChat, mock.at(base_url).with_headers(headers))
}

/// A gateway serving OpenAI clients: `providers`, `plugins` in order, and
/// `target`.
pub fn gateway(
    list: impl IntoIterator<Item = AddProvider>,
    plugins: Vec<Arc<dyn Plugin>>,
    target: Option<&str>,
) -> Result<Gateway, Fault> {
    let gw = with_plugins(
        providers(list)?.client::<Conversation, _, _>(OpenAiChat)?,
        plugins,
    )?;
    match target {
        Some(t) => gw.target(t),
        None => Ok(gw),
    }
}

impl Harness {
    /// Recorders of outcomes on both edges, then `chain`, then a meter;
    /// `openai` with a key of its own, `backup` with none; and `target`.
    pub fn new(chain: Vec<Arc<dyn Plugin>>, target: Option<&str>) -> Self {
        let (log, calls) = (Outcomes::default(), Calls::default());
        let mock = MockTransport::new();
        let mut plugins: Vec<Arc<dyn Plugin>> =
            vec![Arc::new(log.clone()), Arc::new(calls.clone())];
        plugins.extend(chain);
        let usage = Metered::default();
        plugins.push(Arc::new(usage.clone()));
        let list = [
            provider(&mock, "openai", OPENAI, Some("Bearer sk-test")),
            provider(&mock, "backup", BACKUP, None),
        ];
        let gw = gateway(list, plugins, target).unwrap_or_else(|e| panic!("gateway: {e}"));
        Self {
            gw,
            mock,
            log,
            calls,
            usage,
        }
    }
}

pub fn relay() -> Harness {
    Harness::new(Vec::new(), Some("openai"))
}
