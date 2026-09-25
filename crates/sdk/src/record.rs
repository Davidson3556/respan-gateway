//! One summary per client request, assembled from the kernel's close hooks.
//! Billing and logging plugins can build on it or write their own.

use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use respan_gateway_core::{
    ClientRequest, Fault, Flow, Outcome, Plugin, Principal, ProviderEnd, ProviderOutcome,
    ProviderRequest, Usage,
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
struct Draft(Arc<Mutex<Vec<ProviderOutcome>>>);

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
        let draft = Draft(Arc::default());
        req.insert_ext(draft.clone());
        let started = Instant::now();
        let sink = self.sink.clone();
        let (request_id, route, model) = (
            req.id().to_owned(),
            req.route().to_owned(),
            req.model().map(str::to_owned),
        );
        req.on_close(move |req, outcome| {
            let principal = req.ext::<Principal>();
            let attempts =
                std::mem::take(&mut *draft.0.lock().unwrap_or_else(PoisonError::into_inner));
            sink(&Record {
                request_id,
                route,
                model,
                principal: principal.as_ref().map(|p| p.id.clone()),
                tenant: principal.map(|p| p.tenant),
                outcome: outcome.clone(),
                attempts,
                duration: started.elapsed(),
            });
        });
        Ok(Flow::Pass)
    }

    async fn on_provider_request(&self, req: &ProviderRequest<'_>) -> Result<(), Fault> {
        if let Some(draft) = req.client().ext::<Draft>() {
            req.on_close(move |outcome| {
                draft
                    .0
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .push(outcome.clone());
            });
        }
        Ok(())
    }
}
