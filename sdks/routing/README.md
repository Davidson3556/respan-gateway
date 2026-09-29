# respan-gateway-routing

Attempt control for plugins: retry with `Retry-After`, fallback, a staggered
race, a stall bound, and each attempt's role. Each answer is read `Until` a
stopping point before it is judged: its `Head`, its first `Content`
(`Response::until_content`, the default), or its `End`
(`Response::until_end`, held whole, to race for completion; a success
that failed on the way, as the end it returns says, does not count). A success
counts only once read that far; nothing is retried after that. A
provider's error answer is a fault with its status and `Retry-After`,
which decide a retry, as does any failure of a response the call got; at
its `Head`, unread, it cannot be told from a success.

- `first_success(calls, &Backoff, Until) -> Result<Response, Fault>`: each
  call in turn, retried as `Backoff::new(tries, base, max)` says.
- `race(calls, stagger, Until) -> Result<Response, Fault>`: start each call
  `stagger` after the last; the first success wins.
- `Watchdog::new(transport, head, idle)`: a `Transport` that fails a call
  whose head is late or whose body goes silent.
- `Attempt` (extension on each call): its `index()` and `Role`.

Used by: [fallback](../../examples/fallback.rs),
[attachments](../../examples/attachments.rs).
