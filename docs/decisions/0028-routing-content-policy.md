# 0028: Content classification belongs to routing policy

`Format` now contains only `name`, `matches`, and `bind`. Neither it nor
HTTP's `Codec` classifies events as content. The erased format contract
has no content callback, and ordinary forwarding never invokes one.

Core exposes `Response::buffer_until(predicate)` as a generic read-ahead
operation. It retains response metadata, buffered events, and call
ownership. Predicates inspect provider-filtered events; each invocation
rechecks buffered events against its own predicate. Errors before a
match are returned; errors after it are retained for replay. A clean end
returns the response even without a match. Duplex calls remain unread.

The routing SDK provides `until_content(response, predicate)` and
`Until::content(predicate)`. Plugins supply the event type and criterion,
for example `Reply::is_content`. There is no universal default predicate.

This intentionally changes the former pre-filter classification: events
removed by a provider filter cannot commit a fallback, and synthesized
outputs follow the caller's predicate too. `until_end` remains a generic
core buffer operation, with errors classified by whether output arrived,
not by a format's definition of content.
