use crate::{Fault, Usage};

/// How a client request ended. Passed to [`crate::Plugin::on_client_close`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    Done,
    /// Refused or failed before anything was sent to the client.
    Refused(Fault),
    /// Failed after the response started.
    Failed(Fault),
    ClientGone,
}

/// How one provider request ended. Passed to
/// [`crate::Plugin::on_provider_close`].
#[derive(Debug, Clone, PartialEq)]
pub struct ProviderOutcome {
    pub provider: String,
    /// Matches [`crate::ProviderRequest::index`].
    pub index: usize,
    pub status: Option<u16>,
    pub end: ProviderEnd,
    /// Usage the provider reported. Empty unless the response was committed
    /// and read.
    pub usage: Usage,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderEnd {
    /// Its response was sent to the client and finished.
    Committed,
    /// It errored, or returned an error status and was not used.
    Failed,
    /// It succeeded but was not used, e.g. a hedge loser.
    Cancelled,
    /// It was committed but the client left before the end.
    Abandoned,
}
