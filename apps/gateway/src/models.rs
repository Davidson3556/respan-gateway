//! `GET /v1/models`: every provider's models in one OpenAI-shaped list,
//! each named as a client asks for it: as it is if routing sends that name
//! to its provider, else as `provider/model`. `GET /v1beta/models` is the
//! same list in Gemini's shape, for Gemini SDKs; a request to `/v1/models`
//! with `anthropic-version` gets Anthropic's shape.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};

use axum::Json;
use axum::extract::{Path, State};
use axum::response::{IntoResponse, Response};
use respan_gateway_core::futures::future::join_all;
use respan_gateway_http::http::{HeaderMap, StatusCode};
use respan_gateway_http_transport::reqwest;
use serde_json::{Value, json};

use crate::auth;
use crate::compose::{Entry, Upstream};
use crate::formats::{first_of_each_kind, kind, unreachable};
use crate::requests::rfc3339;
use respan_gateway_plugin_routing::{Kind, Routes};
use respan_gateway_providers::Provider;

/// How long a provider may take to list its models.
const TIMEOUT: Duration = Duration::from_secs(5);
/// How long a merged list is reused.
const FRESH: Duration = Duration::from_secs(60);

/// Where one provider lists its models.
#[derive(Debug)]
struct Listing {
    provider: String,
    /// Its vendor, which may name a model's id its own way.
    vendor: Option<&'static Provider>,
    url: String,
    headers: HeaderMap,
    /// A format of each kind it serves.
    kinds: Vec<(Kind, String)>,
}

/// Why a provider gave no list, and the status it answered with, if any.
struct Refusal {
    status: Option<u16>,
    why: String,
}

impl Refusal {
    fn of(why: String) -> Self {
        Self { status: None, why }
    }

    fn with_status(mut self, status: u16) -> Self {
        self.status = Some(status);
        self
    }
}

/// How a provider answered a call made with its key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reach {
    Reachable,
    /// It refused the key, answering with this status.
    Unauthorized(u16),
    /// It did not answer, or not with a list; why.
    Unreachable(String),
}

/// A provider's [`Reach`] and how long it took to answer.
#[derive(Debug, Clone)]
pub struct Probe {
    pub provider: String,
    pub reach: Reach,
    pub took: Duration,
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
                    |e: &&Entry| kind::MEDIA.contains(&e.kind) && e.format.starts_with("openai-");
                let entries = || u.entries.iter();
                let lister = entries()
                    .find(|e| e.kind == kind::CHAT)
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
                    vendor: u.preset,
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
        let (data, failed) = self.collect().await;
        if data.is_empty() && !failed.is_empty() {
            return Err(format!(
                "no provider listed its models: {}",
                failed.join("; ")
            ));
        }
        let list = json!({ "object": "list", "data": data });
        if failed.is_empty() {
            *self.lock() = Some((Instant::now(), list.clone()));
        }
        Ok(list)
    }

    /// Every provider's models now, and why each provider that did not
    /// list is missing.
    pub async fn collect(&self) -> (Vec<Value>, Vec<String>) {
        let fetched = join_all(self.listings.iter().map(|l| self.fetch(l))).await;
        let (listed, failed): (Vec<_>, Vec<_>) = fetched.into_iter().partition(Result::is_ok);
        let failed = failed.into_iter().filter_map(Result::err).collect();
        (listed.into_iter().flatten().flatten().collect(), failed)
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Option<(Instant, Value)>> {
        self.cached.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// One provider's answer to its list call, or why it gave none.
    async fn get(&self, listing: &Listing) -> Result<Value, Refusal> {
        let request = self
            .client
            .get(&listing.url)
            .headers(listing.headers.clone());
        let answered = tokio::time::timeout(TIMEOUT, async {
            let resp = request
                .send()
                .await
                .map_err(|e| Refusal::of(unreachable(&e)))?;
            let status = resp.status();
            if !status.is_success() {
                let why = format!("answered {}", status.as_u16());
                return Err(Refusal::of(why).with_status(status.as_u16()));
            }
            let body = resp
                .bytes()
                .await
                .map_err(|_| Refusal::of("its answer broke off".to_owned()))?;
            serde_json::from_slice::<Value>(&body)
                .map_err(|_| Refusal::of("its answer is not JSON".to_owned()))
        });
        match answered.await {
            Ok(body) => body,
            Err(_) => Err(Refusal::of(format!(
                "no answer within {}s",
                TIMEOUT.as_secs()
            ))),
        }
    }

    /// One call to each provider that lists its models, with its key: how
    /// each answered and how long it took. A provider that lists nothing
    /// is not called.
    pub async fn probe(&self) -> Vec<Probe> {
        join_all(self.listings.iter().map(|l| self.probe_one(l))).await
    }

    async fn probe_one(&self, listing: &Listing) -> Probe {
        let started = Instant::now();
        let reach = match self.get(listing).await {
            Ok(_) => Reach::Reachable,
            Err(Refusal {
                status: Some(s @ (401 | 403)),
                ..
            }) => Reach::Unauthorized(s),
            Err(Refusal { why, .. }) => Reach::Unreachable(why),
        };
        Probe {
            provider: listing.provider.clone(),
            reach,
            took: started.elapsed(),
        }
    }

    /// One provider's models, or why it did not say.
    async fn fetch(&self, listing: &Listing) -> Result<Vec<Value>, String> {
        let failed = |why: String| format!("{}: {why}", listing.provider);
        let body = self.get(listing).await.map_err(|r| failed(r.why))?;
        let Some(models) = body["data"].as_array() else {
            return Err(failed("its answer has no `data` list".to_owned()));
        };
        let models = models
            .iter()
            .filter_map(|m| {
                let id = m["id"].as_str()?;
                let id = listing.vendor.map_or(id, |v| v.model_id(id));
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
/// gateway serves no kind of.
fn kind_of(id: &str) -> Option<Kind> {
    let id = id.to_ascii_lowercase();
    let has = |words: &[&str]| words.iter().any(|w| id.contains(w));
    let kind = match () {
        _ if has(&["embed"]) => kind::EMBEDDING,
        _ if has(&["dall-e", "gpt-image", "imagen", "grok-2-image", "flux"]) => kind::IMAGE,
        _ if has(&["whisper", "transcribe"]) => kind::TRANSCRIPTION,
        _ if has(&["tts"]) => kind::SPEECH,
        _ if has(&["sora", "veo"]) => kind::VIDEO,
        _ if has(&["realtime"]) => kind::REALTIME,
        _ => kind::CHAT,
    };
    Some(kind)
}

/// `GET /v1/models`: the merged list in OpenAI's shape, or in Anthropic's
/// for a request that carries `anthropic-version`, as Anthropic's SDKs send.
pub async fn list(State(models): State<Arc<Models>>, headers: HeaderMap) -> Response {
    let anthropic = headers.contains_key("anthropic-version");
    match (models.list().await, anthropic) {
        (Ok(list), false) => Json(list).into_response(),
        (Ok(list), true) => Json(anthropic_list(&list)).into_response(),
        (Err(why), false) => auth::error(StatusCode::BAD_GATEWAY, "upstream_error", &why),
        (Err(why), true) => {
            let error = json!({"type": "error", "error": {"type": "api_error", "message": why}});
            (StatusCode::BAD_GATEWAY, Json(error)).into_response()
        }
    }
}

/// The chat models of an OpenAI-shaped `list` as Anthropic lists them, in
/// one page.
fn anthropic_list(list: &Value) -> Value {
    let models = list["data"].as_array().into_iter().flatten();
    let data: Vec<Value> = models
        .filter(|m| m["kind"] == "chat")
        .filter_map(|m| {
            let id = m["id"].as_str()?;
            let created = m["created"].as_u64().unwrap_or_default();
            let created = SystemTime::UNIX_EPOCH + Duration::from_secs(created);
            Some(json!({
                "type": "model",
                "id": id,
                "display_name": id,
                "created_at": rfc3339(created),
            }))
        })
        .collect();
    json!({
        "first_id": data.first().map(|m| &m["id"]),
        "last_id": data.last().map(|m| &m["id"]),
        "has_more": false,
        "data": data,
    })
}

/// A Gemini-shaped error body.
fn gemini_error(status: StatusCode, name: &str, message: &str) -> Response {
    let body = json!({"error": {"code": status.as_u16(), "message": message, "status": name}});
    (status, Json(body)).into_response()
}

/// The chat models of an OpenAI-shaped `list` as Gemini lists them: the ones
/// `generateContent` serves.
fn gemini_models(list: &Value) -> Vec<Value> {
    let models = list["data"].as_array().into_iter().flatten();
    models
        .filter(|m| m["kind"] == "chat")
        .filter_map(|m| {
            let id = m["id"].as_str()?;
            Some(json!({
                "name": format!("models/{id}"),
                "baseModelId": id,
                "displayName": id,
                "supportedGenerationMethods": ["generateContent", "streamGenerateContent"],
            }))
        })
        .collect()
}

/// `GET /v1beta/models`: the merged list in Gemini's shape.
pub async fn list_gemini(State(models): State<Arc<Models>>) -> Response {
    match models.list().await {
        Ok(list) => Json(json!({ "models": gemini_models(&list) })).into_response(),
        Err(why) => gemini_error(StatusCode::BAD_GATEWAY, "UNAVAILABLE", &why),
    }
}

/// `GET /v1beta/models/{model}`: one of the merged list, named with or
/// without `models/`.
pub async fn get_gemini(State(models): State<Arc<Models>>, Path(model): Path<String>) -> Response {
    let list = match models.list().await {
        Ok(list) => list,
        Err(why) => return gemini_error(StatusCode::BAD_GATEWAY, "UNAVAILABLE", &why),
    };
    let wanted = format!("models/{}", model.strip_prefix("models/").unwrap_or(&model));
    match gemini_models(&list)
        .into_iter()
        .find(|m| m["name"] == wanted)
    {
        Some(found) => Json(found).into_response(),
        None => gemini_error(
            StatusCode::NOT_FOUND,
            "NOT_FOUND",
            &format!("{wanted} is not found"),
        ),
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
        assert_eq!(kind("text-embedding-3-small"), Some("embedding"));
        assert_eq!(kind("gemini-embedding-001"), Some("embedding"));
    }
}
