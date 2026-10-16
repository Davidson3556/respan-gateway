//! Private helpers for formats integration tests.

use std::sync::{Arc, Mutex, PoisonError};

use respan_gateway_core::Connection;
use respan_gateway_core::Format;
use respan_gateway_http::http::{HeaderMap, HeaderValue};
use respan_gateway_http::{Body, ClientIo, ProviderIo, Sent, connect, http};
use respan_gateway_http_transport::HttpTransport;
use respan_gateway_passthrough_http as passthrough;

use crate::http_support::LIMIT;
use respan_gateway_conversation::Usage;
use respan_gateway_core::{Fault, Gateway, Plugin, Provider, ProviderContext, async_trait};

pub type AddProvider = Box<dyn FnOnce(Gateway) -> Result<Gateway, Fault>>;

/// What a test's transport gives for a call.
pub type SendFuture = respan_gateway_core::futures::future::BoxFuture<'static, Sent>;

/// A transport to `base`, sending `header: key` with each call.
pub fn provider(
    base: &str,
    header: &'static str,
    key: &'static str,
) -> impl Clone + Fn(http::Request<Body>) -> SendFuture + use<> {
    let mut headers = HeaderMap::new();
    headers.insert(header, HeaderValue::from_static(key));
    let client = HttpTransport::new(base)
        .and_then(|http| http.with_headers(headers))
        .unwrap_or_else(|e| panic!("provider transport: {e}"));
    move |request| {
        let client = client.clone();
        Box::pin(async move { client.send(request).await })
    }
}

pub fn openai(
    fake: &mockllm::FakeOpenAi,
) -> impl Clone + Fn(http::Request<Body>) -> SendFuture + use<> {
    provider(&fake.base_url(), "authorization", "Bearer sk-test")
}

/// A transport that fails any call: one no test should make.
pub fn unused() -> impl Clone + Fn(http::Request<Body>) -> SendFuture {
    |_| Box::pin(async { Err(Fault::internal("provider should not be called")) })
}

pub fn add_provider<I, O, F, S, Fut>(name: &str, format: F, sender: S) -> AddProvider
where
    I: Clone + Send + Sync + 'static,
    O: Clone + Send + Sync + 'static,
    F: respan_gateway_core::Format<ProviderIo, O, I>,
    S: Fn(http::Request<Body>) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Sent> + Send + 'static,
{
    let name = name.to_owned();
    Box::new(move |gateway| gateway.provider(Provider::new(name, format, connect(sender, LIMIT))))
}

pub fn providers(list: impl IntoIterator<Item = AddProvider>) -> Result<Gateway, Fault> {
    list.into_iter().try_fold(Gateway::new(), |gw, add| add(gw))
}

use respan_gateway_conversation::{Input, Output};

pub fn with_plugins(
    gateway: Gateway,
    plugins: impl IntoIterator<Item = Arc<dyn Plugin>>,
) -> Gateway {
    plugins.into_iter().fold(gateway, Gateway::plugin)
}

/// Usage seen at the provider edge, including a reply subsequently refused by
/// a filter registered before this meter.
#[derive(Clone, Default)]
pub struct Metered(Arc<Mutex<Usage>>);

impl Metered {
    pub fn total(&self) -> Usage {
        *self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

#[async_trait]
impl Plugin for Metered {
    async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
        if !px.is_kind::<Input>() {
            return Ok(());
        }
        let usage = self.0.clone();
        px.filter(move |reply: &mut Output| {
            if let Output::Usage(more) = reply {
                *usage.lock().unwrap_or_else(PoisonError::into_inner) += &*more;
            }
            Ok(())
        })
    }
}

/// A protocol of no kind that accepts any `POST`, relayed as it came.
#[derive(Clone)]
pub struct Anywhere;

impl Format<ClientIo, passthrough::Input<Anywhere>, passthrough::Output<Anywhere>> for Anywhere {
    fn name(&self) -> &str {
        "anywhere"
    }
    fn matches(&self, io: &ClientIo) -> bool {
        io.request().method() == http::Method::POST
    }
    fn bind(
        &self,
        io: ClientIo,
    ) -> Result<Connection<passthrough::Input<Anywhere>, passthrough::Output<Anywhere>>, Fault>
    {
        Ok(io.bind(passthrough::read_request, passthrough::write_response))
    }
}
impl Format<ProviderIo, passthrough::Output<Anywhere>, passthrough::Input<Anywhere>> for Anywhere {
    fn name(&self) -> &str {
        "anywhere"
    }
    fn bind(
        &self,
        io: ProviderIo,
    ) -> Result<Connection<passthrough::Output<Anywhere>, passthrough::Input<Anywhere>>, Fault>
    {
        Ok(io.bind(
            |head, input| {
                let encoder = passthrough::write_request(head, input)?;
                head.method = http::Method::POST;
                respan_gateway_http::path::set_path(head, "/")?;
                Ok(encoder)
            },
            passthrough::read_response,
        ))
    }
}
