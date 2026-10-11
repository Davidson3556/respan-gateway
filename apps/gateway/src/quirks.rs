//! Each vendor's quirks ([`Provider::plugin`]), run on the calls of the
//! providers configured with that vendor alone.

use std::collections::HashSet;

use respan_gateway_core::{Fault, Gateway, Plugin, ProviderContext, async_trait};
use respan_gateway_providers::Provider;

use crate::compose::Upstream;

/// A vendor's plugin, for its providers' calls.
pub struct Quirks {
    plugin: &'static dyn Plugin,
    /// The core providers of that vendor.
    providers: HashSet<String>,
}

impl std::fmt::Debug for Quirks {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Quirks")
            .field("providers", &self.providers)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl Plugin for Quirks {
    async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
        match self.providers.contains(px.provider()) {
            true => self.plugin.on_provider(px).await,
            false => Ok(()),
        }
    }

    async fn on_provider_end(&self, px: &ProviderContext, end: &Result<(), Fault>) {
        if self.providers.contains(px.provider()) {
            self.plugin.on_provider_end(px, end).await;
        }
    }
}

/// `gateway` with the quirks of each vendor `upstreams` are of. Registered
/// after every other plugin, they are the provider edge's innermost: their
/// filters see a call's inputs last and its outputs first, so every other
/// plugin sees the kind's events.
pub fn with_quirks(mut gateway: Gateway, upstreams: &[Upstream]) -> Gateway {
    let mut vendors: Vec<&'static Provider> = Vec::new();
    for vendor in upstreams.iter().filter_map(|u| u.preset) {
        if !vendors.iter().any(|v| v.id == vendor.id) {
            vendors.push(vendor);
        }
    }
    for vendor in vendors {
        let Some(plugin) = vendor.plugin else {
            continue;
        };
        let of = upstreams
            .iter()
            .filter(|u| u.preset.is_some_and(|p| p.id == vendor.id));
        let providers = of.flat_map(|u| u.entries.iter().map(|e| e.internal.clone()));
        gateway = gateway.plugin(Quirks {
            plugin,
            providers: providers.collect(),
        });
    }
    gateway
}
