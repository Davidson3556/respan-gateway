#![warn(missing_docs)]
//! One MCP endpoint for many MCP servers: [`Aggregate`] answers the MCP
//! format's requests to `/mcp` ([`ROOT`]) itself, fronting the servers it
//! names (core providers of the format) as a single server. A client sees
//! one server and one session; the gateway holds one session with each
//! server behind it, opened as the client's session is and ended with it,
//! through those providers, with each server's own credentials (so give
//! none of them a client's).
//!
//! What a client sees:
//!
//! - `initialize` answers with the capabilities of the servers merged (a
//!   capability any server has, `listChanged` and `subscribe` if any server
//!   does). A server that does not answer is left out of the session.
//! - **Tools and prompts** are named `{server}__{name}`; a call is sent to
//!   the server with the name it knows. A [`ToolFilter`] says which tools
//!   are offered, by that name, and a request's [`ToolScope`] limits it
//!   further.
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
use respan_gateway_http::raw::Original;
use respan_gateway_mcp::{Input, Message, Output, PROTOCOL};
use respan_gateway_mcp_http::FORMAT;
use respan_gateway_plugin_tool_loop::{ToolFilter, ToolScope};
use run::Run;
use serde_json::Value;
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

/// Answers the MCP format's requests to `/mcp` itself; register it before
/// any plugin that routes MCP requests by their path.
pub struct Aggregate(Arc<Inner>);

impl std::fmt::Debug for Aggregate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Aggregate")
            .field("servers", &self.0.servers)
            .finish_non_exhaustive()
    }
}

impl Aggregate {
    /// Front `servers`, core providers of the MCP format, in the order lists
    /// show them, offering the tools `filter` offers.
    pub fn new(servers: Vec<String>, filter: ToolFilter) -> Self {
        Self(Arc::new(Inner {
            servers,
            filter,
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
        let path = cx
            .extensions()
            .get::<Original>()
            .and_then(|wire| wire.get());
        let path = path.map(|request| respan_gateway_http::path::clean_path(request.uri().path()));
        if path.as_deref() != Some(ROOT) {
            return Ok(None);
        }
        let input = cx.input::<Input>()?.clone();
        let scope = cx.extensions().get::<ToolScope>().cloned();
        let calls = self.0.servers.iter().map(|s| (s.clone(), cx.call(s)));
        let run = Run {
            inner: self.0.clone(),
            calls: Arc::new(calls.collect()),
            scope,
        };
        let outputs = run.handle(input).await?;
        Ok(Some(Response::stream(outputs)))
    }
}

type Outputs = BoxStream<'static, Result<Output, Fault>>;

fn message(json: Value) -> Result<Message, Fault> {
    Message::try_from(json).map_err(Fault::internal)
}
