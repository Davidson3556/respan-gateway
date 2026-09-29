# 0011. Read a response until its content or its end

- Status: accepted, amended by [0012](0012-http-out-of-the-kernel.md), [0014](0014-api-trims.md), [0020](0020-symmetric-edges.md) (a refusal at the provider's head fails at once)
- Date: 2026-09-28

## Context

A read to the first content, held, let a plugin still decide or fall
back. Its name said nothing about where it stops, and a plugin racing calls
for the first complete answer could not hold a response whole.

## Decision

1. **`Response::until_content`** reads until the first content
   (`Format::is_content`), a failure, or the end, and holds what it read.
   A provider's error answer is such a failure, and one refused at its
   head (`ProviderContext::is_head_only`) fails at once.
2. **`Response::until_end`** reads to the end and holds every output and
   the end, `Ok` or a failure. Sent, it is delivered as it would have been
   unread. It is `Err` only where `until_content` is; a later failure is
   held. Both return at once for a call that streams the request's inputs.
3. Both keep what they read as decoded batches: a call ends when the
   batch holding its end is sent, read or dropped, and a request that
   closes first ends it `abandoned`.
4. `sdks/routing` takes the stopping point as `Until { Head, Content, End }`
   (default `Content`) on `first_success` and `race`. `Head` cannot tell an
   error answer from a success.

## Consequences

- `until_end` keeps a whole response in memory until it is sent or
  dropped.
- `until_end` returns how the response ended, so with `Until::End` a
  success that fails after its content does not win.

## History

- 0012: an error answer became a failure; `Head` stopped telling it apart.
- 0014: `until_end` returns the end with the response, in place of a
  separate accessor.
- 0015: routing retries by whether the call got a response, not by a flag
  on the fault.
