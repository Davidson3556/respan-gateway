# respan-gateway-plugins

`ModelRouter` selects a provider from an input model. The public `routing` module
contains transport-neutral attempt orchestration: sequential retry and
fallback, a staggered race, and each attempt's role. Each answer is read
`Until` a stopping point before it is judged: its `Head`, a plugin-chosen
event (`Until::content(predicate)`), or its `End` (`Response::until_end`,
held whole). A success counts only once read that far; nothing is retried
after commitment.

- `first_success(calls, &Backoff, Until, should_retry)`: each call in turn,
  retried only when the caller's classifier permits and
  `Backoff::new(tries, base, max)` allows. The classifier sees every failure
  and a `response_opened` bool: true only if `call.await` returned a response
  before applying `Until`, not when a request was sent, refused, or bytes
  arrived. A false result skips retries of that target, not fallback targets.
  A response's `Retry-After` delay is waited for as asked, but a delay over
  `max` means no retry; a failure before opening supplies no delay.
  `Until::Head` counts a provider's refusal (such as a 429 or 503 answer) as
  a failure. With live (streaming) inputs, `Until::content` and `Until::End`
  cannot read the response and commit as `Head` does.
- `race(calls, stagger, Until)`: start each call `stagger` after the last;
  the first success wins.
- `Attempt` (extension on each call): its `index()` and `Role`.

For HTTP, explicitly pass `respan_gateway_http::retryable` as the classifier.
HTTP's `watchdog(transport, head, idle)` lives in the HTTP protocol module; it bounds a
transport's head wait and polled per-frame body gaps. Other transports can
choose a different classifier without importing HTTP.

Used by: [fallback](../../examples/routing/fallback.rs),
[attachments](../../examples/integrations/attachments.rs).

Billing is application policy, not a supported plugin export. See
[the billing examples](../../examples/README.md#billing-policies) for pricing,
conservative per-attempt accounting, nested wallets and per-turn reservations.
Their support modules are private to the examples, not a replacement billing
crate. Provider usage is the conversation kind's typed `Usage`.
