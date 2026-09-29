# 0016. Core fixes

- Status: accepted, amended by [0020](0020-symmetric-edges.md) (the rules hold on the core's event legs and in HTTP's driver, whose writer and reader replaced the byte code in the kernel)
- Date: 2026-09-28

## Context

An independent review of the core, with a toy format on the core's public
API alone (`crates/core/tests`), found five bugs. No decision changes; each
says what an earlier one already meant.

## Decision

1. **A cut or broken request body never looks complete** (B1; 0006). A
   call's inputs, and `take_inputs`, end `Err` when the client's body
   breaks (its own `Err`), when an input cannot be written, or when the call
   or request ends before the inputs do (`abandoned`): the core's event
   legs give `Err(abandoned)` at a cut, never a clean end. In HTTP's driver
   the request encoder's `end` is told that `Err`, never `Ok`; what it
   writes for it goes before the body's `Err`, and nothing follows. Only
   the inputs' own end is a clean one.
2. **Every user-code call is guarded** (B2). The core guards plugins,
   filters and `Format` methods, and every poll of an edge's stream. In
   HTTP's driver a request encoder's later frames run where the transport
   polls the body, possibly in a task of its own. A panic there ends the
   body `Err(internal)` and, since nothing unwinds to the call, marks the
   call, whose response then ends `internal` as it is next read.
3. **`until_content` and `until_end` are linear** (B3; 0011). `read_until`
   notes a batch's end as it reads it. Behaviour is the same.
4. **A request is in flight from `handle`** (B4). `Gateway::idle` covers a
   request still reading its body. A body that fails is refused as before,
   and end hooks run only for plugins whose `on_client` started.
5. **`http::Encoder::open` means the same on a call** (B5; 0014). Its bytes
   come before the body, the client's or the written one; `Ok(true)` means
   nothing more follows: the body is the opening bytes alone.

Also, a decoder's `Err` after values in one chunk of a streaming request
keeps those values, as a response does.

## Consequences

- No public type or method is added or removed; only behaviour and
  rustdoc change (`http::Encoder::open`, `http::Encoder::end`, `Gateway::idle`,
  `ClientContext::take_inputs`, `Response::until_*`,
  `Plugin::on_provider_end`, `http::Transport::send`).
- A transport that ignores its body's `Err` still sends what it read; the
  driver no longer adds a clean end after it.
- Checked and left: a rebuilt gateway counts its own requests only, as
  `idle` says; `until_content` on a response without content holds all of
  it, as it says; a decoder that panics under `until_end` refuses the
  request `internal`, now documented.
