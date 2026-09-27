use respan_gateway_core::async_trait;
use respan_gateway_core::{Context, Fault, Plugin, Response, Target};

/// Targets each request at the provider of the first prefix its model
/// matches, over the gateway's default target. Later plugins still run. An
/// unmatched request keeps its target. A provider the request cannot use
/// fails the call with `config_error`; check each with
/// [`Gateway::check_target`](respan_gateway_core::Gateway::check_target).
#[derive(Debug, Clone, Default)]
pub struct ModelRouter {
    prefixes: Vec<(String, Target)>,
}

impl ModelRouter {
    /// A router with no routes, which leaves every target as it is.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sends models starting with `prefix` to `provider`. Prefixes are tried
    /// in the order added.
    pub fn route(mut self, prefix: impl Into<String>, provider: impl Into<Target>) -> Self {
        self.prefixes.push((prefix.into(), provider.into()));
        self
    }
}

#[async_trait]
impl Plugin for ModelRouter {
    async fn on_client(&self, cx: &mut Context) -> Result<Option<Response>, Fault> {
        let Some(model) = cx.model() else {
            return Ok(None);
        };
        let provider = self
            .prefixes
            .iter()
            .find(|(prefix, _)| model.starts_with(prefix.as_str()));
        if let Some((_, provider)) = provider {
            cx.set_target(provider.clone());
        }
        Ok(None)
    }
}
