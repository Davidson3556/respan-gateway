# 0011. Read a response until its content or its end

- Status: accepted
- Date: 2026-09-28

## Context

`Response::ready` read a response to its first content and held what it
read, so a plugin could still decide on it or fall back. The name said
nothing about where it stops, and a plugin racing calls for the first
complete answer had no way to hold a response whole: it could read
`outputs`, but then the response is consumed and cannot be sent as it
came. This amends 0003, 0006, 0008, 0009 and 0010, whose rules name
`ready`.

## Decision

1. **`Response::ready` is renamed `Response::until_content`**, same
   signature and behaviour: it reads until the first content
   (`Format::is_content`), a failure, or the end, and holds what it read.
2. **`Response::until_end`** reads to the end and holds everything: the
   head as it came, every output, and the end, `Ok` or a failure. Sent, it
   is delivered as it would have been unread, byte for byte for an
   unedited relay. It is `Err` only where `until_content` is, for a
   success that fails before any content; a later failure is held. A 101
   returns at once, as its body streams only once sent.
3. Both go through one reader and keep what they read as decoded
   batches, so the rule from `until_content` holds for both: a call ends
   when the batch holding its end is sent, read or dropped, and a request
   that closes first ends it `abandoned`.
4. `sdks/routing` takes the stopping point as one neutral argument,
   `Until { Head, Content, End }` (default `Content`), on `first_success`
   and `race`.

## Consequences

- `until_end` keeps a whole response in memory until it is sent or
  dropped; a plugin chooses it knowing the answer's size.
- A response does not say how it ended, so `until_end` cannot tell a
  racer that a success failed after its content: such a response counts
  as finished.
- 13 public core types; `Response` has one more method.
