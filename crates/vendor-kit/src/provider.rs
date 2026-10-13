//! A provider of a vendor, ready to register: each format at its base URL,
//! taking the key as the vendor takes it, with the vendor's quirks; and its
//! model list.

use std::sync::Arc;
use std::time::Duration;

use respan_gateway_core::{Extensions, Fault, Gateway, Provider as Core};
use respan_gateway_http::http::header::{AUTHORIZATION, HeaderName, HeaderValue};
use respan_gateway_http::http::{self, HeaderMap, Uri};
use respan_gateway_http::http_body_util::BodyExt;
use respan_gateway_http::{Body, ProviderIo};
use respan_gateway_http_faults::{built_wrong, provider_fault};
use respan_gateway_http_transport::{HttpTransport, reqwest, unreachable};
use respan_gateway_ws::{Limits, WsTransport};
use serde_json::Value;

use crate::{Auth, Format, Speaks, Vendor};

/// A provider of a vendor ([`Vendor::provider`]), or of formats of no
/// vendor, at a base URL with a key, to register
/// ([`register`](VendorProvider::register)).
///
/// ```ignore
/// let openai = respan_gateway_vendors::openai::VENDOR.provider("openai");
/// let gateway = openai.register(Gateway::new())?;
/// let models = openai.models().await;
/// ```
#[derive(Clone)]
pub struct VendorProvider {
    name: String,
    vendor: Option<&'static Vendor>,
    speaks: Vec<Speaks>,
    base: Option<String>,
    key: Key,
    limit: usize,
    sessions: Limits,
}

/// One format's core provider of a [`VendorProvider`] ([`VendorProvider::parts`]).
#[derive(Debug)]
pub struct Part<'a> {
    /// Its name: the provider's for its first format, `name:format` for the
    /// others.
    pub name: String,
    /// Its format, as the vendor speaks it.
    pub speaks: &'a Speaks,
    /// Its base URL.
    pub base_url: String,
    /// What its calls carry to authenticate.
    pub headers: HeaderMap,
}

/// What a provider's calls authenticate with.
#[derive(Clone)]
enum Key {
    /// The client's own credentials.
    Client,
    /// A key, sent as each format takes it.
    Own(String),
    /// These headers, as they are.
    Headers(HeaderMap),
}

impl std::fmt::Debug for VendorProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VendorProvider")
            .field("name", &self.name)
            .field("vendor", &self.vendor.map(|v| v.id))
            .finish_non_exhaustive()
    }
}

impl Vendor {
    /// Provider `name` of this vendor: every format it speaks, at its base
    /// URL, with its key from the first of `key_env` set, else the client's.
    pub fn provider(&'static self, name: impl Into<String>) -> VendorProvider {
        let env = |var: &&str| std::env::var(var).ok().filter(|k| !k.trim().is_empty());
        let key = self.key_env.iter().find_map(env);
        VendorProvider {
            vendor: Some(self),
            speaks: self.formats.to_vec(),
            base: self.base_url.map(Into::into),
            key: key.map_or(Key::Client, Key::Own),
            ..VendorProvider::new(name)
        }
    }
}

impl VendorProvider {
    /// Provider `name` of no vendor and no formats yet
    /// ([`formats`](Self::formats)), with the client's credentials.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            vendor: None,
            speaks: Vec::new(),
            base: None,
            key: Key::Client,
            limit: 32 << 20,
            sessions: Limits::default(),
        }
    }

    /// Speaking `speaks` alone, in place of its vendor's formats.
    pub fn formats(mut self, speaks: impl IntoIterator<Item = Speaks>) -> Self {
        self.speaks = speaks.into_iter().collect();
        self
    }

    /// At `base_url`, the base its formats sit beside.
    pub fn base(mut self, base_url: impl Into<String>) -> Self {
        self.base = Some(base_url.into());
        self
    }

    /// With `key`, sent as each format takes it.
    pub fn key(mut self, key: impl Into<String>) -> Self {
        self.key = Key::Own(key.into());
        self
    }

    /// With `headers` on every call, as they are, in place of a key.
    pub fn headers(mut self, headers: HeaderMap) -> Self {
        self.key = Key::Headers(headers);
        self
    }

    /// Reading up to `bytes` of one answer at once.
    pub fn limit(mut self, bytes: usize) -> Self {
        self.limit = bytes;
        self
    }

    /// Its sessions taking `limits`.
    pub fn sessions(mut self, limits: Limits) -> Self {
        self.sessions = limits;
        self
    }

    /// Its vendor, if it has one.
    pub fn vendor(&self) -> Option<&'static Vendor> {
        self.vendor
    }

    /// Each format's core provider. One format of no vendor sits at the
    /// base URL exactly. `Err` with `config` if it has no base URL, or a
    /// key no header can carry.
    pub fn parts<'a>(&'a self) -> Result<Vec<Part<'a>>, Fault> {
        let base = self.base.as_deref();
        let base = base.ok_or_else(|| Fault::config("needs a `base_url`"))?;
        let exact = self.vendor.is_none() && self.speaks.len() == 1;
        let part = |(i, speaks): (usize, &'a Speaks)| {
            Ok(Part {
                name: match i {
                    0 => self.name.clone(),
                    _ => format!("{}:{}", self.name, speaks.format.name()),
                },
                speaks,
                base_url: match exact {
                    true => base.to_owned(),
                    false => speaks.base_url(base),
                },
                headers: self.auth(speaks.auth)?,
            })
        };
        self.speaks.iter().enumerate().map(part).collect()
    }

    /// `gateway` with a core provider of each of its formats, each carrying
    /// its vendor's quirks ([`Vendor::plugin`]).
    pub fn register(&self, mut gateway: Gateway) -> Result<Gateway, Fault> {
        for part in self.parts()? {
            let format = &part.speaks.format;
            let mut core = self.core(part.name, format, &part.base_url, part.headers)?;
            if let Some(quirks) = self.vendor.and_then(|v| v.plugin) {
                core = core.plugin(quirks);
            }
            gateway = gateway.provider(core)?;
        }
        Ok(gateway)
    }

    /// The models it lists, each as the vendor lists it (OpenAI's
    /// `{"id", "created", ..}`), its `id` the name a call takes
    /// ([`Vendor::model_id`]); by its first chat format, else OpenAI's
    /// media; `None` if it speaks neither. A refusal is
    /// [`provider_fault`]'s.
    pub async fn models(&self) -> Option<Result<Vec<Value>, Fault>> {
        let parts = match self.parts() {
            Ok(parts) => parts,
            Err(fault) => return Some(Err(fault)),
        };
        let lists = |f: &Format| match f {
            Format::OpenAiChat(_)
            | Format::OpenAiResponses(_)
            | Format::AnthropicMessages(_)
            | Format::GeminiGenerate(_) => 0,
            Format::OpenAiImages(_)
            | Format::OpenAiTranscriptions(_)
            | Format::OpenAiSpeech(_)
            | Format::OpenAiEmbeddings(_)
            | Format::OpenAiVideos(_) => 1,
            _ => 2,
        };
        let part = parts
            .into_iter()
            .filter(|p| lists(&p.speaks.format) < 2)
            .min_by_key(|p| lists(&p.speaks.format))?;
        Some(self.list(part).await)
    }

    async fn list(&self, part: Part<'_>) -> Result<Vec<Value>, Fault> {
        let mut headers = part.headers;
        let path = match part.speaks.format {
            Format::AnthropicMessages(_) => {
                let version = HeaderValue::from_static("2023-06-01");
                headers.insert("anthropic-version", version);
                "/v1/models?limit=1000"
            }
            _ => "/models",
        };
        let client = HttpTransport::new(part.base_url)?.with_headers(headers)?;
        let call = http::Request::get(path).body(Body::default());
        let sent = client.send(call.map_err(built_wrong)?).await;
        let (answer, _) = sent.map_err(|f| unreached(&f).map_or(f, Fault::upstream))?;
        let (head, body) = answer.into_parts();
        let body = BodyExt::collect(body).await?.to_bytes();
        if !head.status.is_success() {
            return Err(provider_fault(http::Response::from_parts(head, body)));
        }
        let list: Value =
            serde_json::from_slice(&body).map_err(|_| Fault::upstream("its answer is not JSON"))?;
        let models = list["data"].as_array();
        let models = models.ok_or_else(|| Fault::upstream("its answer has no `data` list"))?;
        let named = |m: &Value| {
            let id = m["id"].as_str()?;
            let id = self.vendor.map_or(id, |v| v.model_id(id));
            let mut m = m.clone();
            m["id"] = id.into();
            Some(m)
        };
        Ok(models.iter().filter_map(named).collect())
    }

    /// The headers its calls of a format that takes the key `auth` carry.
    fn auth(&self, auth: Auth) -> Result<HeaderMap, Fault> {
        let (name, value) = match (&self.key, auth) {
            (Key::Client, _) => return Ok(HeaderMap::new()),
            (Key::Headers(headers), _) => return Ok(headers.clone()),
            (Key::Own(key), Auth::Bearer) => (AUTHORIZATION, format!("Bearer {key}")),
            (Key::Own(key), Auth::Header(name)) => (HeaderName::from_static(name), key.clone()),
        };
        let mut value = HeaderValue::from_str(&value).map_err(|e| Fault::config(e.to_string()))?;
        value.set_sensitive(true);
        Ok(HeaderMap::from_iter([(name, value)]))
    }

    /// The core provider `name` of `format` at `base`, its calls carrying
    /// `auth`: over HTTP, or over WebSocket for a session.
    fn core(
        &self,
        name: String,
        format: &Format,
        base: &str,
        auth: HeaderMap,
    ) -> Result<Core, Fault> {
        let http = || {
            Ok::<_, Fault>(self.reach(HttpTransport::new(base)?.with_headers(auth.clone())?, base))
        };
        let session = |transport: Result<WsTransport, Fault>| {
            let transport = transport?.with_limits(self.sessions);
            Ok::<_, Fault>(transport.with_headers(auth.clone())?.connect())
        };
        Ok(match format.clone() {
            Format::OpenAiChat(f) => Core::new(name, f, http()?),
            Format::OpenAiResponses(f) => Core::new(name, f, http()?),
            Format::AnthropicMessages(f) => Core::new(name, f, http()?),
            Format::GeminiGenerate(f) => Core::new(name, f, http()?),
            Format::OpenAiImages(f) => Core::new(name, f, http()?),
            Format::OpenAiTranscriptions(f) => Core::new(name, f, http()?),
            Format::OpenAiSpeech(f) => Core::new(name, f, http()?),
            Format::OpenAiEmbeddings(f) => Core::new(name, f, http()?),
            Format::OpenAiVideos(f) => Core::new(name, f, http()?),
            Format::Passthrough(f) => Core::new(name, f, http()?),
            // Veo serves a video's bytes by redirect to Google's storage.
            Format::GeminiVeo(f) => {
                let client = HttpTransport::new(base)?
                    .with_connect_timeout(Duration::from_secs(10))?
                    .follow_redirects(google_storage);
                Core::new(name, f, self.reach(client.with_headers(auth)?, base))
            }
            Format::OpenAiRealtime(f) => Core::new(name, f, session(f.transport(base))?),
            Format::GeminiLive(f) => Core::new(name, f, session(f.transport(base))?),
            Format::ResponsesWs(f) => Core::new(name, f, session(f.transport(base))?),
        })
    }

    /// Calls through `client`, a provider it cannot reach named in the fault
    /// with its host, as the transport's names neither.
    fn reach(
        &self,
        client: HttpTransport,
        base: &str,
    ) -> impl Fn(Extensions) -> ProviderIo + Clone + Send + Sync + 'static {
        let client = Arc::new(client);
        let host = base.split_once("://").map_or(base, |(_, rest)| rest);
        let host = host.split('/').next().unwrap_or_default();
        let who: Arc<str> = format!("provider '{}' at {host}", self.name).into();
        respan_gateway_http::connect(
            move |call| {
                let (client, who) = (client.clone(), who.clone());
                async move {
                    client
                        .send(call)
                        .await
                        .map_err(|fault| match unreached(&fault) {
                            Some(why) => Fault::upstream(format!("{who} {why}")).with_source(fault),
                            None => fault,
                        })
                }
            },
            self.limit,
        )
    }
}

/// Why `fault` says its provider could not be reached, if it does.
fn unreached(fault: &Fault) -> Option<String> {
    let e = fault.cause::<reqwest::Error>();
    e.filter(|e| !e.is_builder()).map(unreachable)
}

/// The redirects the Gemini API's calls follow: to Google's hosts over
/// HTTPS alone. The transport leaves the key behind once a call leaves the
/// API's origin.
fn google_storage(uri: &Uri) -> bool {
    let host = uri.host().unwrap_or_default();
    let google = [".googleapis.com", ".googleusercontent.com"];
    uri.scheme_str() == Some("https") && google.iter().any(|domain| host.ends_with(domain))
}

#[cfg(test)]
mod tests {
    use super::*;
    use respan_gateway_passthrough_http::Passthrough;

    #[test]
    fn only_googles_hosts_over_https_are_followed() {
        let follows = |uri: &str| google_storage(&uri.parse().unwrap_or_default());
        assert!(follows("https://storage.googleapis.com/v/1"));
        assert!(follows("https://x.googleusercontent.com/v"));
        assert!(!follows("http://storage.googleapis.com/v"));
        assert!(!follows("https://evil.test/storage.googleapis.com"));
        assert!(!follows("https://googleapis.com.evil.test/v"));
    }

    #[test]
    fn one_format_of_no_vendor_sits_at_its_base_exactly() {
        let passthrough = Speaks::new(Format::Passthrough(Passthrough)).at(crate::Base::Origin);
        let one = VendorProvider::new("p")
            .formats([passthrough])
            .base("https://x.test/v1");
        let parts = one.parts().unwrap_or_default();
        assert_eq!(parts[0].base_url, "https://x.test/v1");
        assert!(VendorProvider::new("p").parts().is_err(), "no base URL");
    }
}
