//! The binary's fallback and hedge plugins (`plugins: fallback` and
//! `plugins: hedge`): their chains, which name configured providers, as
//! the core providers a request is targeted at.
//!
//! A provider is one core provider per format it speaks, so each format of
//! a chain's target gets a chain of its own: each provider of the chain by
//! its core provider of that format, else, for a kind that translates, its
//! first of the kind; a provider that serves neither is left out of it.

use std::time::Duration;

use indexmap::IndexMap;
use respan_gateway_core::Fault;
use respan_gateway_http_faults::retryable;
use respan_gateway_plugin_fallback::{Backoff, Fallback, Hedge, Until};

use crate::compose::Upstream;
use crate::config::{Commit, FallbackConfig, HedgeConfig};

/// The most tries per provider.
const MAX_TRIES: u32 = 10;
/// The first wait before a retry, unless the config says.
const BACKOFF: Duration = Duration::from_millis(250);
/// The longest wait before a retry, unless the config says.
const MAX_BACKOFF: Duration = Duration::from_secs(10);
/// The wait before starting the next provider of a hedge, unless the config
/// says.
const STAGGER: Duration = Duration::from_millis(200);

/// The fallback plugin `config` says, for `upstreams`.
pub fn fallback(config: &FallbackConfig, upstreams: &[Upstream]) -> Result<Fallback, Fault> {
    let bad = |e: String| Fault::config(format!("plugins: fallback: {e}"));
    let tries = config.tries.unwrap_or(1);
    if !(1..=MAX_TRIES).contains(&tries) {
        return Err(bad(format!("tries: {tries}; 1 to {MAX_TRIES}")));
    }
    let base = config.backoff.unwrap_or(BACKOFF);
    let max = config.max_backoff.unwrap_or(MAX_BACKOFF);
    if base > max {
        return Err(bad(format!(
            "backoff: {base:?} is longer than max_backoff: {max:?}"
        )));
    }
    let chains = chains(&config.after, upstreams).map_err(bad)?;
    let plugin = Fallback::new(retryable)
        .backoff(Backoff::new(tries, base, max))
        .until(until(config.until));
    Ok(chains.into_iter().fold(plugin, |plugin, (target, chain)| {
        plugin.after(target, chain)
    }))
}

/// The hedge plugin `config` says, for `upstreams`.
pub fn hedge(config: &HedgeConfig, upstreams: &[Upstream]) -> Result<Hedge, Fault> {
    let bad = |e: String| Fault::config(format!("plugins: hedge: {e}"));
    let chains = chains(&config.after, upstreams).map_err(bad)?;
    if let Some((target, _)) = config.after.iter().find(|(_, chain)| chain.is_empty()) {
        return Err(bad(format!(
            "after: '{target}' has no provider to race it against"
        )));
    }
    let plugin = Hedge::new(config.stagger.unwrap_or(STAGGER)).until(until(config.until));
    Ok(chains.into_iter().fold(plugin, |plugin, (target, chain)| {
        plugin.after(target, chain)
    }))
}

fn until(commit: Commit) -> Until {
    match commit {
        Commit::Head => Until::Head,
        Commit::End => Until::End,
    }
}

/// The chains of `after`, by core provider: for each format of each target,
/// the core providers of its chain that serve that format or its kind.
fn chains(
    after: &IndexMap<String, Vec<String>>,
    upstreams: &[Upstream],
) -> Result<Vec<(String, Vec<String>)>, String> {
    let find = |name: &str| {
        upstreams.iter().find(|u| u.name == name).ok_or_else(|| {
            let names: Vec<&str> = upstreams.iter().map(|u| u.name.as_str()).collect();
            format!(
                "after: unknown provider '{name}'; providers: [{}]",
                names.join(", ")
            )
        })
    };
    let mut chains = Vec::new();
    for (target, chain) in after {
        let target = find(target)?;
        let chain = chain
            .iter()
            .map(|name| find(name))
            .collect::<Result<Vec<_>, _>>()?;
        for (i, provider) in chain.iter().enumerate() {
            if provider.name == target.name || chain[..i].iter().any(|p| p.name == provider.name) {
                return Err(format!(
                    "after: {}: '{}' is listed twice",
                    target.name, provider.name
                ));
            }
        }
        for entry in &target.entries {
            let served = chain.iter().filter_map(|provider| {
                let same = provider.entries.iter().find(|e| e.format == entry.format);
                let kin = || {
                    (provider.entries.iter())
                        .find(|e| entry.kind.translates && e.kind == entry.kind)
                };
                same.or_else(kin).map(|e| e.internal.clone())
            });
            chains.push((entry.internal.clone(), served.collect()));
        }
    }
    Ok(chains)
}
