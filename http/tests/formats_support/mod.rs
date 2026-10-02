//! Private helpers for formats integration tests.

use std::sync::{Arc, Mutex, PoisonError};

use respan_gateway_http::{Body, Completion, Outgoing, connect, http};
use respan_gateway_sdk::Usage;
use respan_gateway_sdk::{Fault, Gateway, Plugin, ProviderContext, async_trait};

pub type AddProvider = Box<dyn FnOnce(Gateway) -> Result<Gateway, Fault>>;

pub fn add_provider<I, O, F, S, Fut>(name: &str, format: F, sender: S) -> AddProvider
where
    I: Clone + Send + Sync + 'static,
    O: Clone + Send + Sync + 'static,
    F: respan_gateway_sdk::Format<Outgoing, O, I>,
    S: Fn(http::Request<Body>) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Result<(http::Response<Body>, Option<Completion>), Fault>>
        + Send
        + 'static,
{
    let name = name.to_owned();
    Box::new(move |gateway| gateway.provider(name, format, connect(sender)))
}

pub fn providers(list: impl IntoIterator<Item = AddProvider>) -> Result<Gateway, Fault> {
    list.into_iter().try_fold(Gateway::new(), |gw, add| add(gw))
}

use respan_gateway_sdk::conversation::{Conversation, Reply};

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
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

#[async_trait]
impl Plugin for Metered {
    async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
        if !px.is_kind::<Conversation>() {
            return Ok(());
        }
        let usage = self.0.clone();
        px.filter(move |reply: &mut Reply| {
            if let Reply::Usage(more) = reply {
                *usage.lock().unwrap_or_else(PoisonError::into_inner) += &*more;
            }
            Ok(())
        })
    }
}
