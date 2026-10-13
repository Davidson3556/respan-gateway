# respan-gateway-plugin-credentials

`ClientCredentials` sends a client's own credential headers (its
`Authorization`, `x-api-key` or `x-goog-api-key`, as the HTTP wire lifted them
into the request's `Credential`) on a call to a provider it names, as the
client would have sent them to the vendor. It does so only on a call of the
client's own format: no key crosses a translation. A provider it does not
name never sees a client's key, and a WebSocket subprotocol stays the
session's.

Name the core providers that have no key of their own:

```rust,ignore
use respan_gateway_plugin_credentials::ClientCredentials;

let gateway = gateway
    .plugin(routing)
    .plugin(ClientCredentials::new(["openai-byok", "github-mcp"]));
```

It sets the call's `CallHeaders`, so a plugin registered after it that sets
them too (a tenant's key, say) has the last word. `Relay` forwards a client's
key on its own calls already; this is for a gateway that routes without it.
([tests](tests/credentials.rs))
