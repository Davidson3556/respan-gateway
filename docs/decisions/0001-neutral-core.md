# 0001. The core is feature-neutral

- Status: accepted
- Date: 2026-09-24

## Context

The gateway this replaces grew one special case per feature. Billing, prompt
handling and logging are woven through every request path, so a change to one
touches all of them. An earlier draft of this design repeated the mistake at
the trait level, with a separate trait for guards, routers, price books,
limiters and sinks.

Users will want things nobody has planned for: translating every prompt into
Chinese, routing by prompt length, billing by request bytes.

## Decision

The core provides generic mechanisms only. PII redaction, guardrails, load
balancing, fallback, billing, caching and logging are plugins, including the
ones that ship with the gateway. First-party plugins use the same public API as
third-party ones.

The test for any proposed abstraction: can an arbitrary user-invented feature
be written with it, the same way a built-in feature is? If not, the core is not
neutral enough.

## Consequences

- A gateway with no plugins configured is a plain relay.
- Nothing is named after a feature in the core crate.
- Built-in plugins cannot use private core APIs, so the public API has to be
  good enough for real features.
