# 0001. The core is feature-neutral

- Status: accepted
- Date: 2026-09-24

## Context

A gateway with a special case, or a trait, per feature (guards, routers,
price books, limiters, sinks) ties every feature to its core: a change to
one touches all of them.

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
