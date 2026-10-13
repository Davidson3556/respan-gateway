//! TypeSafe: System One, its decision models' API.

use respan_gateway_decision_http::SystemOne;

use crate::{
    Body, BoxFuture, Model, Models, Speaks, Vendor, VendorProvider, built_wrong, http, json,
};

/// TypeSafe.
pub const VENDOR: Vendor = Vendor {
    id: "typesafe",
    base_url: Some("https://api.typesafe.ai/v1"),
    key_env: &["TYPESAFE_API_KEY"],
    formats: &[&Speaks::http(SystemOne)],
    models: list_models,
    ..Vendor::DEFAULT
};

/// Its models: `{"models": [{"name", ..}]}`, each model or alias by the
/// name a call takes.
fn list_models(p: &VendorProvider) -> BoxFuture<'_, Models> {
    Box::pin(async move {
        let call = http::Request::get("/models").body(Body::default());
        let (answer, _) = p.client()?.send(call.map_err(built_wrong)?).await?;
        let answer = json(answer).await?;
        let models = answer["models"].as_array().into_iter().flatten();
        Ok(models
            .filter_map(|m| Some(Model::new(m["name"].as_str()?)))
            .collect())
    })
}
