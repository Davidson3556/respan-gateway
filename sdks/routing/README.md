# respan-gateway-routing

Transport-neutral attempt orchestration for plugins: sequential retry and
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
  A response's `Retry-After` delay is honored up to `max`; a failure before
  opening does not supply a retry delay.
- `race(calls, stagger, Until)`: start each call `stagger` after the last;
  the first success wins.
- `Attempt` (extension on each call): its `index()` and `Role`.

For HTTP, explicitly pass `respan_gateway_http::retryable` as the classifier.
HTTP's `watchdog(transport, head, idle)` lives in the HTTP SDK; it bounds a
transport's head wait and polled per-frame body gaps. Other transports can
choose a different classifier without importing HTTP.

Used by: [fallback](../../examples/fallback.rs),
[attachments](../../examples/attachments.rs).
