use async_trait::async_trait;

use crate::{ClientRequest, ClientResponse, Fault, ProviderRequest, Providers};

/// The only extension point. Implement the methods you need; which method you
/// implement decides where your code runs.
#[async_trait]
pub trait Plugin: Send + Sync + 'static {
    /// Once per client request, in chain order. Returning `Err` refuses the
    /// request.
    async fn on_client_request(&self, _req: &ClientRequest) -> Result<(), Fault> {
        Ok(())
    }

    /// Once per provider request, in chain order, before it is sent.
    async fn on_provider_request(&self, _req: &ProviderRequest<'_>) -> Result<(), Fault> {
        Ok(())
    }

    /// The route's source: decide which provider requests to make and which
    /// response to send. The returned response is committed to the client.
    async fn dispatch(
        &self,
        _req: &ClientRequest,
        _providers: &Providers<'_>,
    ) -> Result<ClientResponse, Fault> {
        Err(Fault::config("this plugin is not a source"))
    }
}
