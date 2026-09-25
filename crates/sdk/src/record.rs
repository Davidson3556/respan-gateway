//! One summary per client request, assembled from the close hooks. Billing
//! and logging plugins can build on it or write their own.

use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use respan_gateway_core::{
    Context, Fault, Flow, Plugin, Principal, Request, Response, ResponseEnd, Usage,
};

/// One provider request.
#[derive(Debug, Clone, PartialEq)]
pub struct Attempt {
    pub provider: String,
    pub index: usize,
    /// `None` if no response came back.
    pub status: Option<u16>,
    /// `Failed` with the fault if no response came back.
    pub end: ResponseEnd,
    pub usage: Usage,
}

#[derive(Debug, Clone)]
pub struct Record {
    pub request_id: String,
    pub route: String,
    pub model: Option<String>,
    /// Who was calling, if an auth plugin said so.
    pub principal: Option<String>,
    pub tenant: Option<String>,
    /// Status sent to the client, if a response was sent.
    pub status: Option<u16>,
    /// How the client's response ended, or why there was none.
    pub result: Result<ResponseEnd, Fault>,
    /// Every provider request, in the order they ended.
    pub attempts: Vec<Attempt>,
    pub duration: Duration,
}

impl Record {
    /// Usage across every provider request. Unused ones report none.
    pub fn usage(&self) -> Usage {
        let mut total = Usage::default();
        for a in &self.attempts {
            total.merge(&a.usage);
        }
        total
    }
}

fn outcome(resp: Result<&Response, &Fault>) -> (Option<u16>, Result<ResponseEnd, Fault>, Usage) {
    match resp {
        Ok(r) => (
            Some(r.status().as_u16()),
            Ok(r.end().cloned().unwrap_or(ResponseEnd::Done)),
            r.usage().clone(),
        ),
        Err(f) => (None, Err(f.clone()), Usage::default()),
    }
}

type Sink = Arc<dyn Fn(&Record) + Send + Sync>;

#[derive(Clone)]
struct Draft {
    started: Instant,
    attempts: Arc<Mutex<Vec<Attempt>>>,
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
    async fn on_client_request(&self, cx: &Context, _req: &Request) -> Result<Flow, Fault> {
        cx.insert_ext(Draft {
            started: Instant::now(),
            attempts: Arc::default(),
        });
        Ok(Flow::Pass)
    }

    async fn on_provider_close(
        &self,
        cx: &Context,
        req: &Request,
        resp: Result<&Response, &Fault>,
    ) {
        let Some(d) = cx.ext::<Draft>() else {
            return;
        };
        let (status, result, usage) = outcome(resp);
        d.attempts
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(Attempt {
                provider: req.provider().map(|t| t.name.clone()).unwrap_or_default(),
                index: req.index(),
                status,
                end: result.unwrap_or_else(ResponseEnd::Failed),
                usage,
            });
    }

    async fn on_client_close(&self, cx: &Context, req: &Request, resp: Result<&Response, &Fault>) {
        let Some(d) = cx.ext::<Draft>() else {
            return;
        };
        let principal = cx.ext::<Principal>();
        let (status, result, _) = outcome(resp);
        let attempts =
            std::mem::take(&mut *d.attempts.lock().unwrap_or_else(PoisonError::into_inner));
        (self.sink)(&Record {
            request_id: cx.id().to_owned(),
            route: cx.route().to_owned(),
            model: req.model().map(str::to_owned),
            principal: principal.as_ref().map(|p| p.id.clone()),
            tenant: principal.map(|p| p.tenant),
            status,
            result,
            attempts,
            duration: d.started.elapsed(),
        });
    }
}
