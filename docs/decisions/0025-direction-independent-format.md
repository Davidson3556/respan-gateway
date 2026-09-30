# 0025. Format binds messages; the gateway assigns edge roles

- Status: accepted
- Date: 2026-09-29
- Amends the Format interface in [0024](0024-flows-and-symmetric-edges.md).

## Context

The receive-future/send-sink tuple is symmetric. Separate `Format::client`
and `Format::provider` methods nevertheless make formats name the gateway's
roles. Those roles already belong to registration and execution in the core.
The transport holds a handle or connection configuration; it does not need
to identify the peer as the gateway's caller or its provider.

Associated message types fix the receive/send order for each implementation.
Changing the value of an I/O handle cannot change its Rust return type. The
core already knows which order an edge needs and can select an implementation
with that order at registration.

## Decision

Use `Format<T, R, W>`, with one binding method and no associated types:

```rust,ignore
pub trait Format<T, R, W>: Send + Sync + 'static {
    fn name(&self) -> &str;
    fn matches(&self, io: &T) -> bool { false }
    fn is_content(&self, received: &R) -> bool { true }
    fn bind(&self, io: T) -> Result<Connection<R, W>>;
}

pub type Connection<R, W> = (
    BoxFuture<'static, Result<Events<R>>>,
    Pin<Box<dyn Sink<Events<W>, Error = Fault> + Send>>,
);
```

`Connection` is a tuple alias, with no new runtime wrapper. `bind` is a
synchronous factory; its returned operations progress asynchronously.
Format implementations know the protocol messages they decode and encode.
They do not select or name a gateway edge role.

Both gateway registration methods take `<I, O, T>`, with `I` and `O` in
canonical gateway input/output order. `Gateway::client` requires
`Format<T, I, O>`; `Gateway::provider` requires `Format<T, O, I>`.
The core records the role when erasing the implementation. Both execution
paths then call the same erased `bind` method. Kind validation, filters,
wraps and relay comparisons continue to use the canonical `(I, O)` pair.
`is_content` examines received values on a provider binding.

A format can implement both orders for the same `T`. HTTP's `impl_format!`
expands to `Format<Io, Codec::Input, Codec::Output>` and its reverse. The
HTTP handle keeps its existing protocol state; no role flag or new handle
wrapper is added to the public Format contract. The existing Codec contract
requires distinct input/output types, so these implementations are disjoint.

When a concrete format implements both orientations, the registration must
supply enough type information to select one. For example:

```rust,ignore
Gateway::new()
    .client::<Conversation, Reply, _>(OpenAiChat)?
    .provider::<Conversation, Reply, _>("openai", OpenAiChat, connect(transport))?;
```

A format with only one implementation can still infer all three parameters.
Formats over several transports may also need an explicit transport type.

## Consequences

- Remove `Format::client`, `Format::provider`, `ToProvider` and `ToClient`.
  Existing implementations split their binding logic by receive/send types.
- Preserve the transport handle, per-attempt factory, matching, metadata,
  lazy replay, refusal and concurrent delivery contracts from 0024.
- Generic adapters constrain the exact message order they support. Concrete
  registrations specify message types where inference would be ambiguous.
- Opposite bindings with the same name validate as the same gateway kind.
  Registering an inconsistent orientation under that name is rejected.
- The public boundary vocabulary gains one tuple alias, `Connection`, while
  Format loses its associated types and its two role-specific methods.
