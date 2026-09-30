# respan-gateway-ledger

Neutral ledger storage and metering primitives. Amounts are whatever unit your
application counts; this SDK does not decide prices, who pays, or when a call is
charged.

- `Ledger`: reserve and settle holds by `HoldId`, with a TTL for abandoned
  reservations. Implement it for your own store.
- `MemoryLedger`: in-memory nested accounts with `open`, `available`, and
  `spent`.
- `Tally`: a shared atomic cost counter. Clones share saturating `add(amount)`
  and the `is_reported` flag, including when zero is added.
- Storage error codes: `INSUFFICIENT_FUNDS` (HTTP status hint 402) and
  `UNKNOWN_ACCOUNT` (HTTP status hint 401).

The storage errors retain their existing HTTP status hints. The
[`Accounting` plugin](../../plugins/README.md) in `respan-gateway-plugins`
provides the first-party quote/hold/settlement policy and its `NO_ACCOUNT`
error. See the [wallet](../../examples/wallet.rs) and
[cost cap](../../examples/cost_cap.rs) integrations.
