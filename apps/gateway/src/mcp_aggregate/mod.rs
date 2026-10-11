//! One MCP endpoint for every MCP server: `mcp_aggregate:` in the config
//! turns on `/mcp`, which fronts the `mcp:` servers it names (all of them if
//! it names none) as a single server. A client sees one server and one
//! session; the gateway holds one session with each server behind it, opened
//! as the client's session is and ended with it, over the same providers
//! `/mcp/{name}` uses, with each server's own credentials.
//!
//! What a client sees:
//!
//! - `initialize` answers with the capabilities of the servers merged (a
//!   capability any server has, `listChanged` and `subscribe` if any server
//!   does). A server that does not answer is left out of the session.
//! - **Tools and prompts** are named `{server}__{name}`; a call is sent to
//!   the server with the name it knows. `allow` and `deny` (patterns with
//!   `*`, as the agent's) say which tools are offered, by that name.
//! - **Resources** keep their URIs under the server's: `mcp://{server}/{uri}`
//!   (a template likewise). Nothing is looked up: the URI says the server.
//!   URIs inside tool and prompt results are given the prefix too, so what
//!   a tool links to can be read.
//! - **Lists** are the first page of every server's, merged, in the order
//!   `servers` names them. A server with more pages leaves a `nextCursor`,
//!   which holds the cursors of the servers that have more; it asks for
//!   those servers alone, until none has.
//! - **Notifications**: the `GET` stream is the servers' own, merged: their
//!   `list_changed`, `resources/updated` (URI prefixed) and log messages. A
//!   `notifications/cancelled` from the client goes to every server; progress
//!   notifications ride the answer to the call they belong to.
//! - Sampling, elicitation, roots and completion are not offered: the
//!   gateway tells its servers it is a client with no capabilities.
//!
//! The session belongs to the client key that opened it, as a server's does
//! at `/mcp/{name}`. A client that ends its session (`DELETE`) ends the
//! servers' sessions with it; one that does not leaves them to the servers'
//! own idle timeouts (the gateway forgets its side after thirty idle minutes
//! or past a thousand sessions). A reload ends every session of the build it
//! retires, with the servers, once the requests on that build are done
//! ([`Closer`]); the clients open new ones.
//!
//! The request log has one entry per client request: the method and the
//! tool, prompt or resource it names, and the server a routed call went to
//! (the last one, for a list that asked several).

mod rewrite;
mod run;
mod session;
mod upstream;

use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use respan_gateway_core::futures::stream::BoxStream;
use respan_gateway_core::{ClientContext, Fault, Plugin, Response, async_trait};
use respan_gateway_http::{Wire, framing};
use respan_gateway_mcp::{Input, Output};
use respan_gateway_mcp_http::Owner;
use serde::Deserialize;

use crate::agent::{Filter, PROTOCOL, ToolScope};
use crate::mcp::{FORMAT, McpServer};
use run::Run;
use session::Inner;

pub use session::Closer;

/// Where clients reach the aggregate.
pub const ROOT: &str = "/mcp";
/// The separator between a server's name and a tool's or prompt's.
const SEP: &str = "__";
/// The scheme of a resource's URI under a server's.
const SCHEME: &str = "mcp://";
/// The most client sessions held at once.
const SESSIONS: usize = 1024;
/// How long a session no client uses is kept.
const IDLE: Duration = Duration::from_secs(30 * 60);
/// The protocol versions the aggregate answers with, newest first.
const VERSIONS: [&str; 3] = [PROTOCOL, "2025-03-26", "2024-11-05"];

/// `mcp_aggregate:` in `gateway.yaml`.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AggregateConfig {
    /// The `mcp:` servers behind `/mcp`, in the order lists show them. All
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

/// The servers behind `/mcp`, checked: each is an `mcp:` server, named
/// once, and none passes on a client's credentials (the servers' sessions
/// are the gateway's own).
pub fn check(config: &AggregateConfig, servers: &[McpServer]) -> Result<Vec<String>, Fault> {
    let bad = |e: String| Fault::config(format!("mcp_aggregate: {e}"));
    let names: Vec<String> = match &config.servers {
        Some(listed) => listed.clone(),
        None => (servers.iter().filter(|s| !s.forward_auth))
            .map(|s| s.name.clone())
            .collect(),
    };
    if names.is_empty() {
        return Err(bad(
            "no mcp server to front; list some under `mcp:` (a server with \
             `forward_auth` cannot be fronted)"
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
                 (`forward_auth`); the aggregate's sessions use each server's own"
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

/// Answers the requests to `/mcp` itself.
pub struct Aggregate(Arc<Inner>);

impl Aggregate {
    /// Serves `servers` (checked by [`check`]) under `config`'s filter.
    pub fn new(config: &AggregateConfig, servers: Vec<String>) -> Self {
        Self(Arc::new(Inner {
            servers,
            filter: Filter::new(config.allow.clone(), config.deny.clone()),
            sessions: Mutex::default(),
            next_id: AtomicU64::new(1),
        }))
    }

    /// What ends its sessions once a reload retires it.
    pub fn closer(&self) -> Closer {
        Closer(self.0.clone())
    }
}

#[async_trait]
impl Plugin for Aggregate {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        if cx.format() != FORMAT {
            return Ok(None);
        }
        let path = cx.extensions().get::<Wire>().and_then(|wire| wire.get());
        let path = path.map(|request| framing::clean_path(request.uri().path()));
        if path.as_deref() != Some(ROOT) {
            return Ok(None);
        }
        let input = cx.input::<Input>()?.clone();
        let owner = cx.extensions().get::<Owner>().map(|Owner(id)| id.clone());
        let scope = cx.extensions().get::<ToolScope>().cloned();
        let calls = self.0.servers.iter().map(|s| (s.clone(), cx.call(s)));
        let run = Run {
            inner: self.0.clone(),
            calls: Arc::new(calls.collect()),
            owner,
            scope,
        };
        let outputs = run.handle(input).await?;
        Ok(Some(Response::stream(outputs)))
    }
}

type Outputs = BoxStream<'static, Result<Output, Fault>>;
