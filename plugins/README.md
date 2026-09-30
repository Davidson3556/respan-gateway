# respan-gateway-plugins

Concrete first-party policies built on public core and SDK APIs.

- `ModelRouter` selects the provider target from a model name.
- `Accounting::new(ledger, quote)` reserves each provider call's quoted
  worst-case cost before sending and settles exactly once when the call ends.
  The quote receives `&mut ProviderContext` and a shared
  `respan_gateway_ledger::Tally` so a provider filter can report usage. A
  failed quote refuses the call; a quote without accounts yields `NO_ACCOUNT`
  (HTTP 401). An unsent call or an uncut provider refusal costs nothing;
  a sent call cut off or lacking complete usage costs at least its reserve. `.ttl(..)`
  bounds an unsettled hold. Register Accounting last so its filter sees
  provider outputs before earlier plugins' filters.
- `Quote::new(accounts, reserve)` names who pays and the maximum cost of
  the call.

Ledger backends, holds and the neutral `Tally` remain in
[`respan-gateway-ledger`](../sdks/ledger/README.md). Application-specific
prices, authentication and metering belong in integrations such as
[wallet](../examples/wallet.rs) and [cost cap](../examples/cost_cap.rs).
