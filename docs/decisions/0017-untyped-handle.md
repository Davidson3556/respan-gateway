# 0017. Untyped handle

- Status: superseded by [0020](0020-symmetric-edges.md)
- Date: 2026-09-28

## Context

`Gateway::handle::<Ask, Answer>` erased the request's head at once and
checked `Answer` against the accepting format's at run time, as the kernel
cannot know which format accepts. The type parameters gave no compile-time
safety on a boundary that erases immediately, and made the gateway know the
caller's wire, which is the caller's to know.

## Decision (superseded)

0020 replaced it: `Gateway::handle<W>(wire, extensions)` takes the
caller's wire, one generic, and returns the future of how the request
ended; the response goes through the client edge (`Format::accept`), so the
core holds no `Exchange`, `Head`, `erase` or `downcast`. A served format
`accepts` the wire it is handed, and a format of another wire type never
does. The rest of this record is what 0017 decided.

1. **`handle` is not generic**:
   `handle(&self, req: Exchange<Head>) -> (Result<Exchange<Head>, Fault>, BoxFuture<'static, Result<(), Fault>>)`.
   The kernel takes and checks no expected `Answer`.
2. **`Head` is public**, `Box<dyn Any + Send + Sync>`, an alias: no new
   public type. `Exchange::erase` and `Exchange<Head>::downcast` are public,
   for an adapter to type its own wire.
3. **The adapter types the wire.** `crates/http` erases the request's head
   and downcasts the response's; a response of another wire is `INTERNAL`
   ("a response of another wire than asked") from that downcast. Its public
   API is unchanged.
4. **The compile-time check lives in `Gateway::provider`**, which requires a
   transport's heads to equal its format's (0013). Behaviour is otherwise
   as before: format selection, panic guards, streams and hooks.

## Consequences

- Amends 0013 (`handle::<Ask, Answer>`) and 0015 (`Answer` as a type
  parameter).
- No new public type; `Head` is an alias.
- `Format`, `Decoder`, `Encoder` and `Transport` are unchanged.
