# respan-gateway-plugin-fallback

`Fallback` sends a request to its target, then to each provider of the
target's chain in turn, and the client gets the first success; `Hedge` races
them instead, each started a stagger after the last. A request whose target
has no chain is left alone. They are library plugins: the gateway binary
configures attempts per route (`[routes.<model>]`'s `fallback`, `tries`,
`hedge`), sent by the routing crate's `Dispatch` with the helpers below.

```rust,ignore
use respan_gateway_plugin_fallback::{Backoff, Fallback, Until};

let fallback = Fallback::new(respan_gateway_http::rules::retryable)
    .after("openai", ["anthropic"])          // target, then its fallbacks
    .backoff(Backoff::new(2, Duration::from_millis(250), Duration::from_secs(5)))
    .until(Until::content(Output::is_content));
let gateway = gateway.plugin(routing).plugin(fallback);
```

Each provider is tried as `backoff` allows (once by default) and each answer
is read `until` its stopping point (its head by default) before it is judged.
It answers the request itself, so register it after the plugins that target
and edit the request, such as routing.

## Attempt helpers

The plugin is built on transport-neutral attempt orchestration that other
plugins can call from `on_client` too: sequential retry and
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
- `Attempt` (extension on each call): its `index()`, `Role`, and `head()`,
  how long its response took to open.

For HTTP, explicitly pass `respan_gateway_http::rules::retryable` as the classifier.
HTTP's `watchdog(transport, head, idle)` lives in the HTTP protocol module; it bounds a
transport's head wait and polled per-frame body gaps. Other transports can
choose a different classifier without importing HTTP.

Used by: [routing](../routing/README.md)'s `Dispatch`,
[fallback](../../../examples/routing/fallback.rs),
[attachments](../../../examples/integrations/attachments.rs).
