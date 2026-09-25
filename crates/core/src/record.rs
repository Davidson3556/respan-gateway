use std::collections::BTreeMap;
use std::time::Duration;

use crate::{Fault, Usage};

/// Everything known about one client request, emitted exactly once when it
/// closes: on completion, failure, refusal or client disconnect.
#[derive(Debug, Clone)]
pub struct Record {
    pub request_id: String,
    pub route: String,
    pub principal: Option<String>,
    pub tenant: Option<String>,
    pub model: Option<String>,
    pub status: u16,
    pub outcome: Outcome,
    pub attempts: Vec<AttemptRecord>,
    /// Reported usage of the committed provider request.
    pub usage: Usage,
    pub labels: BTreeMap<String, String>,
    pub duration: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    Done,
    /// Refused or failed before anything was sent to the client.
    Refused(Fault),
    /// Failed after the response started.
    Failed(Fault),
    ClientGone,
}

#[derive(Debug, Clone)]
pub struct AttemptRecord {
    pub provider: String,
    pub status: Option<u16>,
    pub end: AttemptEnd,
    pub usage: Usage,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttemptEnd {
    Pending,
    Committed,
    Failed,
    Cancelled,
}
