#![warn(missing_docs)]
//! Routing for respan-gateway, written against the core's public API only,
//! as a third party would. Its real purpose is adversarial: every place the
//! core made something impossible, ambiguous, clumsy or silently wrong is
//! recorded in `FINDINGS.md`, with a test in `tests/`.
//!
//! - [`smart::SmartRouter`]: a judge call picks the target, which answers.
//! - [`fallback::Fallback`]: providers in turn, across formats, each retried
//!   with backoff that honours `Retry-After` ([`retry::Backoff`]).
//! - [`hedge::Hedge`]: a second provider after a delay; the first success
//!   to send content wins and the other is cancelled.
//! - [`attachments::PdfToText`]: PDFs become text on the provider edge, only
//!   for targets that cannot read them.
//! - [`tenant::Tenants`]: per-tenant target and credentials, updated live,
//!   and [`tenant::ByEndpoint`], a per-tenant base URL.
//! - [`quirks::DropFields`]: a provider that rejects a field, fixed on the
//!   call's final request.
//! - [`inproc::InProcess`]: a model in the same process, as a transport.
//! - [`watchdog::Watchdog`]: a stall bound on a provider's body.

pub mod attachments;
pub mod fallback;
pub mod hedge;
pub mod inproc;
pub mod quirks;
pub mod retry;
pub mod smart;
pub mod tenant;
pub mod watchdog;
