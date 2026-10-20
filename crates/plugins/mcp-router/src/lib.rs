#![warn(missing_docs)]
//! Routes an MCP client's request to the MCP server its path names.
//!
//! [`McpRouter`] targets a request of the MCP format to `/mcp/{name}` at the
//! provider `name`: the last segment of the request's cleaned path. A key
//! limited to some tools (a request carrying a [`ToolScope`]) is refused
//! there with `403 tools_limited`: a server's own endpoint offers all its
//! tools, so such a key reaches them through `/mcp`, which holds it to them.
//! A server turned off in its [`ToolSwitches`] is refused with `403
//! tool_server_off`; a tool turned off is not, since the endpoint relays the
//! server as it is.
//!
//! Register it after `respan-gateway-plugin-mcp-aggregate`'s `Aggregate`,
//! which answers `/mcp` itself; this plugin would otherwise take `/mcp` for
//! a server named `mcp`. It is separate from the `routing` plugin, which
//! routes model requests.

use respan_gateway_core::{ClientContext, ErrorCode, Fault, Plugin, Response, async_trait};
use respan_gateway_http::http::StatusCode;
use respan_gateway_http::path::clean_path;
use respan_gateway_http::raw::Original;
use respan_gateway_http_faults::Status;
use respan_gateway_mcp_http::FORMAT;
use respan_gateway_plugin_tool_loop::{ToolScope, ToolSwitches};
use std::sync::Arc;

/// Targets a client of `/mcp/{name}` at the server `name`, refusing a key
/// limited to some tools or a server turned off. See the [crate docs](crate).
#[derive(Debug, Clone, Default)]
pub struct McpRouter {
    switches: Arc<ToolSwitches>,
}

impl McpRouter {
    /// Refusing the servers `switches` turns off.
    pub fn with_switches(switches: Arc<ToolSwitches>) -> Self {
        Self { switches }
    }
}

/// The server a request path names: the last segment of the cleaned path.
pub fn server_name(path: &str) -> Option<String> {
    clean_path(path).rsplit('/').next().map(str::to_owned)
}

#[async_trait]
impl Plugin for McpRouter {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        if cx.format() != FORMAT {
            return Ok(None);
        }
        let name = cx
            .extensions()
            .get::<Original>()
            .and_then(|wire| wire.get())
            .and_then(|request| server_name(request.uri().path()));
        let name = name.ok_or_else(|| Fault::not_found("no MCP server named"))?;
        if cx.extensions().get::<ToolScope>().is_some() {
            let message = format!(
                "this key is limited to some tools, so it reaches MCP servers at /mcp, not /mcp/{name}"
            );
            return Err(Fault::new(message)
                .with(ErrorCode("tools_limited".into()))
                .with(Status(StatusCode::FORBIDDEN)));
        }
        if !self.switches.server_on(&name) {
            let message = format!("the tool server '{name}' is turned off");
            return Err(Fault::new(message)
                .with(ErrorCode("tool_server_off".into()))
                .with(Status(StatusCode::FORBIDDEN)));
        }
        cx.set_target(name);
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::server_name;

    #[test]
    fn names_the_last_segment() {
        assert_eq!(server_name("/mcp/github").as_deref(), Some("github"));
        assert_eq!(server_name("/v1/mcp/files").as_deref(), Some("files"));
    }

    #[test]
    fn cleans_the_path_first() {
        assert_eq!(server_name("/mcp/github/").as_deref(), Some("github"));
        assert_eq!(server_name("//mcp//github").as_deref(), Some("github"));
    }
}
