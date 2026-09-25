use async_trait::async_trait;

use crate::{Context, Fault, Request, Response};

/// What a plugin decides about a client request.
#[derive(Debug)]
pub enum Flow {
    /// Continue to the next plugin in the chain.
    Pass,
    /// Commit this response to the client. The rest of the chain does not run.
    Respond(Box<Response>),
}

impl Flow {
    pub fn respond(resp: Response) -> Self {
        Flow::Respond(Box::new(resp))
    }
}

/// The only extension point.
///
/// Hooks run in chain order. A plugin gets a close hook only for the requests
/// whose matching request hook it ran. When a client request ends, every
/// provider request that ended is closed first, in order, then the client
/// request. A close hook gets `Ok` with the finished response, or `Err` if
/// there never was one (transport error, refusal).
#[async_trait]
pub trait Plugin: Send + Sync + 'static {
    /// Once per client request. `Err` refuses it; `Flow::Respond` answers it
    /// and ends the chain. If every plugin passes, the route's `target`
    /// answers.
    async fn on_client_request(&self, _cx: &Context, _req: &Request) -> Result<Flow, Fault> {
        Ok(Flow::Pass)
    }

    /// Once per provider request, before it is sent, for every chain entry
    /// that has run so far.
    async fn on_provider_request(&self, _cx: &Context, _req: &Request) -> Result<(), Fault> {
        Ok(())
    }

    async fn on_provider_close(
        &self,
        _cx: &Context,
        _req: &Request,
        _resp: Result<&Response, &Fault>,
    ) {
    }

    async fn on_client_close(
        &self,
        _cx: &Context,
        _req: &Request,
        _resp: Result<&Response, &Fault>,
    ) {
    }
}
