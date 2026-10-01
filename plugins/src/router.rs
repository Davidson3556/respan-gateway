use std::any::Any;

use respan_gateway_sdk::async_trait;
use respan_gateway_sdk::{ClientContext, Fault, Plugin, Response};

/// Targets each request at the provider of the first prefix its model
/// matches, read by `model` from its input of type `I`. A request without a
/// readable input of that type keeps its target; later plugins still run.
pub struct ModelRouter<I> {
    model: fn(&I) -> Option<&str>,
    prefixes: Vec<(String, String)>,
}

impl<I> std::fmt::Debug for ModelRouter<I> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ModelRouter")
            .field("prefixes", &self.prefixes)
            .finish()
    }
}

impl<I> ModelRouter<I> {
    /// A router with no routes, reading each request's model with `model`.
    pub fn new(model: fn(&I) -> Option<&str>) -> Self {
        Self {
            model,
            prefixes: Vec::new(),
        }
    }

    /// Sends models starting with `prefix` to `provider`. Prefixes are tried
    /// in the order added.
    pub fn route(mut self, prefix: impl Into<String>, provider: impl Into<String>) -> Self {
        self.prefixes.push((prefix.into(), provider.into()));
        self
    }
}

#[async_trait]
impl<I: Any + Send + Sync> Plugin for ModelRouter<I> {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let Some(model) = cx.input::<I>().ok().and_then(self.model) else {
            return Ok(None);
        };
        let provider = self
            .prefixes
            .iter()
            .find(|(prefix, _)| model.starts_with(prefix.as_str()));
        if let Some((_, provider)) = provider {
            cx.set_target(provider.as_str());
        }
        Ok(None)
    }
}
