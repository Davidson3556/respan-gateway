#![warn(missing_docs)]
//! A safety SDK written against the public API of the respan-gateway core,
//! as a third party would write it. Its real purpose is adversarial: every
//! place the core makes a safety feature impossible, ambiguous or clumsy is
//! recorded in `FINDINGS.md`.
//!
//! - [`pii`]: redact PII before every provider call, restore it in the
//!   streamed response, placeholders split across deltas included.
//! - [`guard`]: an input guardrail and a streaming output guardrail over any
//!   kind that says how to read it, refusing kinds it cannot read.
//! - [`mcp`]: a user-defined MCP-like kind the core has never seen.
//! - [`retention`]: a per-tenant "no retention" marker set by an auth plugin,
//!   and a content log that honours it while billing records still flow.
//! - [`calls`]: plugins that make their own calls: a fallback and a judge.

pub mod calls;
pub mod guard;
pub mod mcp;
pub mod pii;
pub mod retention;
