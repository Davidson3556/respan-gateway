# respan-gateway-ledger

Reserve before a call and settle once from its usage, against a store and
prices you supply. Amounts are whatever unit you count.

- `Accounting::new(ledger, quote)`: a plugin that holds each call's worst
  case before it is sent (402 if it does not fit) and settles it from the
  call's usage. `.ttl(..)` bounds a hold; `.unreported(Unreported::..)` says
  what to charge when no usage came.
- `Quote::new(accounts, reserve, price)`: who pays, the most the call can
  cost, and its price from usage.
- `Ledger`: the store trait to implement; `MemoryLedger` is an in-memory one
  with nested accounts (`open`, `available`, `spent`).
- Error codes: `INSUFFICIENT_FUNDS`, `UNKNOWN_ACCOUNT`, `NO_ACCOUNT`.

Used by: [wallet](../../examples/wallet.rs), [cost_cap](../../examples/cost_cap.rs).
