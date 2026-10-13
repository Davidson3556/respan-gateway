//! A provider's models: as OpenAI lists them (`GET /models`), unless its
//! vendor lists them its own way ([`Vendor::models`](crate::Vendor::models)).

use std::future::Future;
use std::pin::Pin;

use respan_gateway_core::Fault;
use respan_gateway_http::{Body, http};
use respan_gateway_http_faults::built_wrong;
use respan_gateway_http_transport::json;
use serde_json::Value;

use crate::VendorProvider;

/// How a vendor lists a provider's models: with its client
/// ([`VendorProvider::client`]), as it answers.
pub type ListModels = for<'a> fn(&'a VendorProvider) -> BoxFuture<'a, Models>;

/// A future to come, boxed.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// The models a provider lists.
pub type Models = Result<Vec<Model>, Fault>;

/// A model a provider lists.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Model {
    /// Its id, the name a call takes.
    pub id: String,
    /// When it was made, in seconds since the Unix epoch, if it says.
    pub created: Option<u64>,
}

impl Model {
    /// Model `id`.
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            created: None,
        }
    }

    /// The models of an answer as OpenAI's: `{"data": [{"id", "created"}]}`.
    pub fn openai(answer: &Value) -> Vec<Model> {
        let models = answer["data"].as_array().into_iter().flatten();
        let model = |m: &Value| {
            let mut model = Model::new(m["id"].as_str()?);
            model.created = m["created"].as_u64();
            Some(model)
        };
        models.filter_map(model).collect()
    }
}

/// OpenAI's model list: `GET /models`.
pub fn openai(p: &VendorProvider) -> BoxFuture<'_, Models> {
    Box::pin(async move {
        let call = http::Request::get("/models").body(Body::default());
        let (answer, _) = p.client()?.send(call.map_err(built_wrong)?).await?;
        Ok(Model::openai(&json(answer).await?))
    })
}
