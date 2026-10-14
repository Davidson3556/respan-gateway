//! `plugins.mcp_aggregate` in the config turns on `/mcp`, which fronts the
//! `mcp` servers it names (all of them if it names none) as a single server
//! ([`respan_gateway_plugin_mcp_aggregate`]). This is its config, checked
//! against the `mcp` servers.

use respan_gateway_core::Fault;
use respan_gateway_plugin_mcp_aggregate::Aggregate;
use respan_gateway_plugin_tool_loop::ToolFilter;
use serde::Deserialize;

use crate::mcp::McpServer;

/// `plugins.mcp_aggregate` in `gateway.toml`.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AggregateConfig {
    /// The `mcp` servers behind `/mcp`, in the order lists show them. All
    /// of the servers that take no client's credentials, if unset.
    pub servers: Option<Vec<String>>,
    /// The tools offered, by their names `{server}__{tool}`, with `*` for
    /// any characters; all of them if none.
    #[serde(default)]
    pub allow: Vec<String>,
    /// The tools withheld, which win over `allow`.
    #[serde(default)]
    pub deny: Vec<String>,
}

/// The servers behind `/mcp`, checked: each is an `mcp` server, named
/// once, and none passes on a client's credentials (the servers' sessions
/// are the gateway's own).
pub fn check(config: &AggregateConfig, servers: &[McpServer]) -> Result<Vec<String>, Fault> {
    let bad = |e: String| Fault::config(format!("plugins: mcp_aggregate: {e}"));
    let names: Vec<String> = match &config.servers {
        Some(listed) => listed.clone(),
        None => (servers.iter().filter(|s| !s.forward_auth))
            .map(|s| s.name.clone())
            .collect(),
    };
    if names.is_empty() {
        return Err(bad(
            "no mcp server to front; list some under `mcp` (a server with \
             `auth = \"forward\"` cannot be fronted)"
                .into(),
        ));
    }
    for (i, name) in names.iter().enumerate() {
        if names[..i].contains(name) {
            return Err(bad(format!("servers: '{name}' is listed twice")));
        }
        let Some(server) = servers.iter().find(|s| s.name == *name) else {
            let known: Vec<&str> = servers.iter().map(|s| s.name.as_str()).collect();
            return Err(bad(format!(
                "servers: unknown mcp server '{name}'; mcp servers: [{}]",
                known.join(", ")
            )));
        };
        if server.forward_auth {
            return Err(bad(format!(
                "servers: mcp server '{name}' passes on a client's credentials \
                 (`auth = \"forward\"`); the aggregate's sessions use each server's own"
            )));
        }
    }
    for (key, patterns) in [("allow", &config.allow), ("deny", &config.deny)] {
        if patterns
            .iter()
            .any(|p| p.is_empty() || p.contains(char::is_whitespace))
        {
            return Err(bad(format!(
                "{key}: a pattern is a tool name, with `*` for any characters"
            )));
        }
    }
    Ok(names)
}

/// The aggregate of `servers` (checked by [`check`]) under `config`'s filter.
pub fn aggregate(config: &AggregateConfig, servers: Vec<String>) -> Aggregate {
    let filter = ToolFilter::new(config.allow.clone(), config.deny.clone());
    Aggregate::new(servers, filter)
}
