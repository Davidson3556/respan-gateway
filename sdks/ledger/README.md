# respan-gateway-ledger

Reserve before a call and settle once from what it cost, against a store
and prices you supply. Amounts are whatever unit you count; the ledger
knows no usage format.

- `Accounting::new(ledger, quote)`: a plugin that holds each call's worst
  case before it is sent (a 402, `Status` from `respan-gateway-http`, if it does not fit) and settles it from the
  call's `Tally`, or at least the hold when its cost may be partial (never
  reported, or cut off); nothing when the provider refused before
  beginning a response (`ProviderContext::is_head_only` and a failure that is
  not a cut-off). `.ttl(..)` bounds a hold.
- `Quote::new(accounts, reserve)`: who pays and the most the call can cost.
  The quote gets the call (`&mut ProviderContext`) and its `Tally`, so it
  can add a filter of its kind that prices each usage increment into it.
- `Tally`: a call's priced cost, `add(amount)`.
- `Ledger`: the store trait to implement; `MemoryLedger` is an in-memory one
  with nested accounts (`open`, `available`, `spent`).
- Error codes: `INSUFFICIENT_FUNDS`, `UNKNOWN_ACCOUNT`, `NO_ACCOUNT`.

Used by: [wallet](../../examples/wallet.rs), [cost_cap](../../examples/cost_cap.rs).
