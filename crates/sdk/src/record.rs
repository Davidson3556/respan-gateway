//! One summary per client request, assembled from the close hooks. Billing
//! and logging plugins can build on it or write their own.

use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use respan_gateway_core::{
    ClientRequest, Fault, Flow, Outcome, Plugin, Principal, ProviderEnd, ProviderOutcome, Usage,
};

#[derive(Debug, Clone)]
pub struct Record {
    pub request_id: String,
    pub route: String,
    pub model: Option<String>,
    /// Who was calling, if an auth plugin said so.
    pub principal: Option<String>,
    pub tenant: Option<String>,
    pub outcome: Outcome,
    /// Every provider request, in the order they ended.
    pub attempts: Vec<ProviderOutcome>,
    pub duration: Duration,
}

impl Record {
    /// Usage of the provider request whose response went to the client.
    pub fn usage(&self) -> Usage {
        let mut total = Usage::default();
        for a in &self.attempts {
            if matches!(a.end, ProviderEnd::Committed | ProviderEnd::Abandoned) {
                total.merge(&a.usage);
            }
        }
        total
    }
}

type Sink = Arc<dyn Fn(&Record) + Send + Sync>;

#[derive(Clone)]
struct Draft {
    started: Instant,
    attempts: Arc<Mutex<Vec<ProviderOutcome>>>,
}

/// Calls `sink` with a [`Record`] when each request ends. Place it first in
/// the chain so it also records requests later plugins refuse.
#[derive(Clone)]
pub struct Recorder {
    sink: Sink,
}

impl std::fmt::Debug for Recorder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Recorder").finish_non_exhaustive()
    }
}

impl Recorder {
    pub fn new(sink: impl Fn(&Record) + Send + Sync + 'static) -> Self {
        Self {
            sink: Arc::new(sink),
        }
    }
}

#[async_trait]
impl Plugin for Recorder {
    async fn on_client_request(&self, req: &ClientRequest) -> Result<Flow, Fault> {
        req.insert_ext(Draft {
            started: Instant::now(),
            attempts: Arc::default(),
        });
        Ok(Flow::Pass)
    }

    async fn on_provider_close(&self, req: &ClientRequest, outcome: &ProviderOutcome) {
        if let Some(d) = req.ext::<Draft>() {
            d.attempts
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(outcome.clone());
        }
    }

    async fn on_client_close(&self, req: &ClientRequest, outcome: &Outcome) {
        let Some(d) = req.ext::<Draft>() else {
            return;
        };
        let principal = req.ext::<Principal>();
        let attempts =
            std::mem::take(&mut *d.attempts.lock().unwrap_or_else(PoisonError::into_inner));
        (self.sink)(&Record {
            request_id: req.id().to_owned(),
            route: req.route().to_owned(),
            model: req.model().map(str::to_owned),
            principal: principal.as_ref().map(|p| p.id.clone()),
            tenant: principal.map(|p| p.tenant),
            outcome: outcome.clone(),
            attempts,
            duration: d.started.elapsed(),
        });
    }
}
