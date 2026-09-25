use async_trait::async_trait;

use crate::{ClientRequest, ClientResponse, Fault, ProviderRequest};

/// What a plugin decides about a client request.
#[derive(Debug)]
pub enum Flow {
    /// Continue to the next plugin in the chain.
    Pass,
    /// Commit this response to the client. The rest of the chain does not run.
    Respond(Box<ClientResponse>),
}

impl Flow {
    pub fn respond(resp: impl Into<ClientResponse>) -> Self {
        Flow::Respond(Box::new(resp.into()))
    }
}

/// The only extension point.
#[async_trait]
pub trait Plugin: Send + Sync + 'static {
    /// Once per client request, in chain order. `Err` refuses the request;
    /// `Flow::Respond` answers it and ends the chain. If every plugin passes,
    /// the route's `target` answers.
    async fn on_client_request(&self, _req: &ClientRequest) -> Result<Flow, Fault> {
        Ok(Flow::Pass)
    }

    /// Once per provider request, before it is sent, for every chain entry
    /// that has run so far.
    async fn on_provider_request(&self, _req: &ProviderRequest<'_>) -> Result<(), Fault> {
        Ok(())
    }
}
