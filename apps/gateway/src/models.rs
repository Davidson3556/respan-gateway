//! `GET /v1/models`: every provider's models in one OpenAI-shaped list,
//! each named as a client asks for it: as it is if routing sends that name
//! to its provider, else as `provider/model`.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::Json;
use axum::extract::State;
use axum::response::{IntoResponse, Response};
use respan_gateway_core::futures::future::join_all;
use respan_gateway_http::http::{HeaderMap, StatusCode};
use respan_gateway_http::reqwest;
use serde_json::{Value, json};

use crate::auth;
use crate::compose::{Entry, Upstream};
use crate::formats::{Kind, first_of_each_kind, unreachable};
use crate::routing::Routes;

/// How long a provider may take to list its models.
const TIMEOUT: Duration = Duration::from_secs(5);
/// How long a merged list is reused.
const FRESH: Duration = Duration::from_secs(60);

/// Where one provider lists its models.
#[derive(Debug)]
struct Listing {
    provider: String,
    url: String,
    headers: HeaderMap,
    /// A format of each kind it serves.
    kinds: Vec<(Kind, String)>,
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
                // OpenAI's media formats share its list of models.
                let openai_media =
                    |e: &&Entry| Kind::MEDIA.contains(&e.kind) && e.format.starts_with("openai-");
                let entries = || u.entries.iter();
                let lister = entries()
                    .find(|e| e.kind == Kind::CHAT)
                    .or_else(|| entries().find(openai_media))?;
                let base = lister.base_url.trim_end_matches('/');
                let mut headers = lister.auth.clone();
                let url = match lister.format.as_str() {
                    "anthropic-messages" => {
                        headers.insert("anthropic-version", "2023-06-01".parse().ok()?);
                        format!("{base}/v1/models?limit=1000")
                    }
                    _ => format!("{base}/models"),
                };
                let kinds = first_of_each_kind(entries().map(|e| (e.kind, e.format.clone())));
                Some(Listing {
                    provider: u.name.clone(),
                    url,
                    headers,
                    kinds,
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

    /// The merged list, fetched again once a minute old; not kept while a
    /// provider fails to list. `Err` says why, when every provider failed.
    pub async fn list(&self) -> Result<Value, String> {
        let fresh = |at: &Instant| at.elapsed() < FRESH;
        if let Some((_, list)) = self.lock().as_ref().filter(|(at, _)| fresh(at)) {
            return Ok(list.clone());
        }
        let fetched = join_all(self.listings.iter().map(|l| self.fetch(l))).await;
        let (listed, failed): (Vec<_>, Vec<_>) = fetched.into_iter().partition(Result::is_ok);
        let failed: Vec<String> = failed.into_iter().filter_map(Result::err).collect();
        if listed.is_empty() && !failed.is_empty() {
            return Err(format!(
                "no provider listed its models: {}",
                failed.join("; ")
            ));
        }
        let data: Vec<Value> = listed.into_iter().flatten().flatten().collect();
        let list = json!({ "object": "list", "data": data });
        if failed.is_empty() {
            *self.lock() = Some((Instant::now(), list.clone()));
        }
        Ok(list)
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Option<(Instant, Value)>> {
        self.cached.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// One provider's models, or why it did not say.
    async fn fetch(&self, listing: &Listing) -> Result<Vec<Value>, String> {
        let failed = |why: String| format!("{}: {why}", listing.provider);
        let request = self
            .client
            .get(&listing.url)
            .headers(listing.headers.clone());
        let answered = tokio::time::timeout(TIMEOUT, async {
            let resp = request.send().await.map_err(|e| unreachable(&e))?;
            if !resp.status().is_success() {
                return Err(format!("answered {}", resp.status().as_u16()));
            }
            let body = resp.bytes().await.map_err(|_| "its answer broke off")?;
            serde_json::from_slice::<Value>(&body).map_err(|_| "its answer is not JSON".to_owned())
        });
        let body = match answered.await {
            Ok(body) => body.map_err(failed)?,
            Err(_) => return Err(failed(format!("no answer within {}s", TIMEOUT.as_secs()))),
        };
        let Some(models) = body["data"].as_array() else {
            return Err(failed("its answer has no `data` list".to_owned()));
        };
        let models = models
            .iter()
            .filter_map(|m| {
                let id = m["id"].as_str()?;
                // Gemini's OpenAI endpoint names them `models/<id>`.
                let id = id.strip_prefix("models/").unwrap_or(id);
                let kind = kind_of(id)?;
                let (_, format) = listing.kinds.iter().find(|(k, _)| *k == kind)?;
                let mut model = json!({
                    "id": self.public_id(listing, kind, format, id),
                    "object": "model",
                    "owned_by": listing.provider,
                    "kind": kind.label,
                });
                if let Some(created) = m["created"].as_i64() {
                    model["created"] = created.into();
                }
                Some(model)
            })
            .collect();
        Ok(models)
    }

    fn public_id(&self, listing: &Listing, kind: Kind, format: &str, id: &str) -> String {
        let route = self.routes.route(kind, format, Some(id));
        match route {
            Some(route) if route.provider == listing.provider && route.model.is_none() => {
                id.to_owned()
            }
            _ => format!("{}/{id}", listing.provider),
        }
    }
}

/// What a model does, by its name as vendors spell them; `None` for one the
/// gateway serves no kind of, such as embeddings.
fn kind_of(id: &str) -> Option<Kind> {
    let id = id.to_ascii_lowercase();
    let has = |words: &[&str]| words.iter().any(|w| id.contains(w));
    let kind = match () {
        _ if has(&["embed"]) => return None,
        _ if has(&["dall-e", "gpt-image", "imagen", "grok-2-image", "flux"]) => Kind::IMAGE,
        _ if has(&["whisper", "transcribe"]) => Kind::TRANSCRIPTION,
        _ if has(&["tts"]) => Kind::SPEECH,
        _ if has(&["sora", "veo"]) => Kind::VIDEO,
        _ if has(&["realtime"]) => Kind::REALTIME,
        _ => Kind::CHAT,
    };
    Some(kind)
}

pub async fn list(State(models): State<Arc<Models>>) -> Response {
    match models.list().await {
        Ok(list) => Json(list).into_response(),
        Err(why) => auth::error(StatusCode::BAD_GATEWAY, "upstream_error", &why),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_models_kind_is_read_from_its_name() {
        let kind = |id| kind_of(id).map(|k| k.label);
        assert_eq!(kind("gpt-4o-mini"), Some("chat"));
        assert_eq!(kind("gpt-image-1"), Some("image"));
        assert_eq!(kind("imagen-4.0-generate-001"), Some("image"));
        assert_eq!(kind("whisper-large-v3"), Some("transcription"));
        assert_eq!(kind("gpt-4o-mini-transcribe"), Some("transcription"));
        assert_eq!(kind("gpt-4o-mini-tts"), Some("speech"));
        assert_eq!(kind("sora-2"), Some("video"));
        assert_eq!(kind("veo-3.0-generate-001"), Some("video"));
        assert_eq!(kind("gpt-realtime"), Some("realtime"));
        assert_eq!(kind("text-embedding-3-small"), None);
    }
}
