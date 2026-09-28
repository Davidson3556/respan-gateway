# 0010. Usage out of the core

- Status: accepted
- Date: 2026-09-28

## Context

The kernel counted usage: `Format::usage` reported an increment per
message, the provider edge summed it as decoded, before any filter, and
`Outcome.usage` carried it, summed again on the client edge. But not every
kind has usage (an MCP tool call may not), and kinds measure it
differently: tokens, bytes, dollars. The kernel cannot know what usage is,
so counting it was a feature in the core (0001). This amends 0008 and
0009.

## Decision

1. **The kernel counts no usage.** Core's `Usage`, `Format::usage`,
   `Outcome.usage` and every path that counted or summed it are gone.
   `Usage`, still an open unit-to-amount map, moves to
   `respan_gateway_formats::Usage`; the chat kind keeps `Reply.usage` and
   its unit names (`conversation::units`).
2. **Filters see every value.** A value that is no content, such as a
   keep-alive or a message of usage alone (OpenAI's final usage chunk,
   Anthropic's `message_start` and `message_delta`), no longer skips
   filters, so a provider-edge filter sees every increment a provider
   reports. A filter passes on at once what it has no reason to hold:
   `text::Holdback` lets such a value through unless replies wait before
   it, so a keep-alive still reaches a same-format client in place.
3. **`Format::says` is renamed `Format::is_content`** (and `Reply::says`
   `Reply::is_content`), same signature and default. It decides only what
   `Response::ready` commits on; neither a keep-alive nor usage alone is
   content.
4. **Metering is a plugin's.** A plugin meters a call with a provider-edge
   filter of its kind and reads the sum in `on_provider_end`, after the
   call's filters ended. Output filters run the last added first, so it is
   registered after the plugins whose filters may fail, and then sees each
   message first (`examples::meter`).
5. **The ledger stays neutral.** `Accounting`'s quote gets
   `&mut ProviderContext` and the call's `Tally`, a per-call amount it
   owns; the quote adds its own filter that prices what the kind reports
   into it. `Quote::new(accounts, reserve)` has no price. A call sent whose
   tally was never added to, or that was cut, is still charged at least
   its reserve.

We kept one flag rather than splitting it (filters see keep-alives but
not usage): one rule is simpler than two, and a filter that holds
everything was already able to delay a stream's framing.

## Consequences

- **Given up:** "usage counts even when a filter fails" is no kernel
  guarantee. A meter misses a message a filter before it failed on or
  held and dropped, and anything after a failure in the same frame; a
  failure also ends the stream, so usage the provider would have sent
  later is never seen. Registering the meter last covers the first case;
  a hold (the ledger's reserve) covers the rest.
- Every filter sees keep-alives and usage alone. One that holds all
  values holds them too, so the head may wait for its release: a stream
  that opened with a keep-alive and then failed can now be refused.
- `Outcome` has only `head` and `end`; there is no request-level sum. A
  plugin that wants one keeps it in a request extension its calls share.
- The testkit's `Calls::usage` is gone; tests meter through a filter.
- 13 public core types; `Format` has 9 members.
