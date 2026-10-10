//! `GET /v1/models`: every provider's models in one OpenAI-shaped list,
//! each named as a client asks for it: as it is if routing sends that name
//! to its provider, else as `provider/model`.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::Json;
use axum::extract::State;
use respan_gateway_core::futures::future::join_all;
use respan_gateway_http::http::HeaderMap;
use respan_gateway_http::reqwest;
use serde_json::{Value, json};

use crate::compose::Upstream;
use crate::formats::Kind;
use crate::routing::Routes;

/// How long a provider may take to list its models.
const TIMEOUT: Duration = Duration::from_secs(5);
/// How long a merged list is reused.
const FRESH: Duration = Duration::from_secs(60);

/// Where one provider lists its chat models.
#[derive(Debug)]
struct Listing {
    provider: String,
    format: String,
    url: String,
    headers: HeaderMap,
}

#[derive(Debug)]
pub struct Models {
    client: reqwest::Client,
    listings: Vec<Listing>,
    routes: Arc<Routes>,
    cached: Mutex<Option<(Instant, Value)>>,
}

impl Models {
    pub fn new(upstreams: &[Upstream], routes: Arc<Routes>) -> Self {
        let listings = upstreams
            .iter()
            .filter_map(|u| {
                let chat = u.entries.iter().find(|e| e.kind == Kind::CHAT)?;
                let base = chat.base_url.trim_end_matches('/');
                let mut headers = chat.auth.clone();
                let url = match chat.format.as_str() {
                    "anthropic-messages" => {
                        headers.insert("anthropic-version", "2023-06-01".parse().ok()?);
                        format!("{base}/v1/models?limit=1000")
                    }
                    _ => format!("{base}/models"),
                };
                Some(Listing {
                    provider: u.name.clone(),
                    format: chat.format.clone(),
                    url,
                    headers,
                })
            })
            .collect();
        Self {
            client: reqwest::Client::new(),
            listings,
            routes,
            cached: Mutex::default(),
        }
    }

    /// The merged list, fetched again once a minute old.
    pub async fn list(&self) -> Value {
        let fresh = |at: &Instant| at.elapsed() < FRESH;
        if let Some((_, list)) = self.lock().as_ref().filter(|(at, _)| fresh(at)) {
            return list.clone();
        }
        let fetched = join_all(self.listings.iter().map(|l| self.fetch(l))).await;
        let data: Vec<Value> = fetched.into_iter().flatten().collect();
        let list = json!({ "object": "list", "data": data });
        *self.lock() = Some((Instant::now(), list.clone()));
        list
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Option<(Instant, Value)>> {
        self.cached.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// One provider's models; none if it fails to say.
    async fn fetch(&self, listing: &Listing) -> Vec<Value> {
        let request = self
            .client
            .get(&listing.url)
            .headers(listing.headers.clone());
        let answered = tokio::time::timeout(TIMEOUT, async {
            let resp = request.send().await.ok()?.error_for_status().ok()?;
            let body = resp.bytes().await.ok()?;
            serde_json::from_slice::<Value>(&body).ok()
        });
        let Ok(Some(body)) = answered.await else {
            return Vec::new();
        };
        let Some(models) = body["data"].as_array() else {
            return Vec::new();
        };
        models
            .iter()
            .filter_map(|m| {
                let id = m["id"].as_str()?;
                // Gemini's OpenAI endpoint names them `models/<id>`.
                let id = id.strip_prefix("models/").unwrap_or(id);
                let mut model = json!({
                    "id": self.public_id(listing, id),
                    "object": "model",
                    "owned_by": listing.provider,
                    "kind": Kind::CHAT.label,
                });
                if let Some(created) = m["created"].as_i64() {
                    model["created"] = created.into();
                }
                Some(model)
            })
            .collect()
    }

    fn public_id(&self, listing: &Listing, id: &str) -> String {
        let route = self.routes.route(Kind::CHAT, &listing.format, Some(id));
        match route {
            Some(route) if route.provider == listing.provider && route.model.is_none() => {
                id.to_owned()
            }
            _ => format!("{}/{id}", listing.provider),
        }
    }
}

pub async fn list(State(models): State<Arc<Models>>) -> Json<Value> {
    Json(models.list().await)
}
