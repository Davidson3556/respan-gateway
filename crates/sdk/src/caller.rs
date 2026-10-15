//! Who a request is for.

/// Who a request is for, as its host knows the client: in a request's
/// extensions, for whatever keeps or shows things by client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Caller {
    /// Stable and opaque, never the client's secret: what scopes what is
    /// kept for it, such as its MCP sessions or stored responses.
    pub id: String,
    /// Its name, as a person reads it, such as a client key's.
    pub name: String,
}
