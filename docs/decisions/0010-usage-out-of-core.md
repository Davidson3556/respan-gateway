# 0010. Usage out of the core

- Status: accepted, amended by [0011](0011-read-until-content-or-end.md), [0012](0012-http-out-of-the-kernel.md)
- Date: 2026-09-28

## Context

The kernel counted usage per message and summed it into `Outcome.usage`.
But not every kind has usage, and kinds measure it differently: tokens,
bytes, dollars. Counting it was a feature in the core (0001).

## Decision

1. **The kernel counts no usage.** `Usage`, an open unit-to-amount map,
   lives in `respan_gateway_formats`; the chat kind keeps `Reply.usage`
   and its unit names (`conversation::units`).
2. **Filters see every value**, keep-alives and usage alone included. A
   filter passes on at once what it has no reason to hold, so a
   keep-alive still reaches a same-format client in place.
3. **`Format::is_content`** (and `Reply::is_content`) decides only what
   `until_content` commits on; neither a keep-alive nor usage alone is
   content.
4. **Metering is a plugin's.** A plugin meters a call with a provider-edge
   filter of its kind and reads the sum in `on_provider_end`. Output
   filters run the last added first, so it is registered after the
   plugins whose filters may fail (`examples/billing.rs`). A plugin's own
   answer reaches no provider edge; a cache carries its cost in an
   extension.
5. **The ledger stays neutral.** `Accounting`'s quote gets
   `&mut ProviderContext` and the call's `Tally`, and adds its own filter
   pricing what the kind reports. A call sent whose tally was never added
   to, or that was cut, is charged at least its reserve.

## Consequences

- "Usage counts even when a filter fails" is no kernel guarantee: a meter
  misses a message a filter before it failed on or dropped, and what a
  failure cut off. Registering the meter last and a hold (the ledger's
  reserve) cover this.
- A filter that holds every value holds keep-alives too, so the head may
  wait for their release.
- There is no request-level sum; a plugin keeps one in a request
  extension its calls share.

## History

- 0011: `ready` became `until_content`.
- 0012: `Outcome` kept only `end`.
