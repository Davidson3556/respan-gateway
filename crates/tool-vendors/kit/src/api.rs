//! The vendor's HTTP API, with its key and base URL.

use std::time::Duration;

use respan_gateway_core::Fault;
use respan_gateway_http_transport::Network;

use crate::KeyIn;

/// A network's client for tool APIs: two minutes for an answer.
fn client_of(network: &Network) -> Result<reqwest::Client, Fault> {
    network.client("tool API", |builder| {
        Ok(builder
            .timeout(Duration::from_secs(120))
            .user_agent("respan-gateway"))
    })
}

/// The most of a refusal's body an error quotes.
const QUOTED: usize = 2000;

/// A failed tool call, told to the model as the tool's result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolError(pub String);

impl ToolError {
    /// A failure with this message.
    pub fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl std::fmt::Display for ToolError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// A tool vendor's API: its base URL and key, and an HTTP client.
#[derive(Debug, Clone)]
pub struct Api {
    pub(crate) client: reqwest::Client,
    pub(crate) base: String,
    pub(crate) key: String,
    pub(crate) key_in: KeyIn,
}

impl Api {
    /// The API at `base` with `key`, taken as `key_in` says.
    /// It connects through [`Network::shared`], until told otherwise.
    pub fn new(base: impl Into<String>, key: impl Into<String>, key_in: KeyIn) -> Self {
        Self {
            client: client_of(Network::shared()).unwrap_or_default(),
            base: base.into().trim_end_matches('/').to_owned(),
            key: key.into(),
            key_in,
        }
    }

    /// Connect through `network`, sharing its tool APIs' client.
    pub fn with_network(mut self, network: &Network) -> Result<Self, Fault> {
        self.client = client_of(network)?;
        Ok(self)
    }

    /// Send its requests with `client`, one of your own, in place of a
    /// network's.
    pub fn with_client(mut self, client: reqwest::Client) -> Self {
        self.client = client;
        self
    }

    /// Its client, carrying no key: for a request the key has no business
    /// in, such as to a presigned link.
    pub fn client(&self) -> &reqwest::Client {
        &self.client
    }

    /// `path` under the base URL, or `path` itself when it is an absolute URL.
    pub fn url(&self, path: &str) -> String {
        if path.starts_with("http://") || path.starts_with("https://") {
            return path.to_owned();
        }
        let base = self.base.trim_end_matches('/');
        match path.strip_prefix('/') {
            Some(_) => format!("{base}{path}"),
            None => format!("{base}/{path}"),
        }
    }

    /// Its key, as configured, for a vendor whose tools send it themselves
    /// ([`KeyIn::Manual`]) or need parts of it.
    pub fn key(&self) -> &str {
        &self.key
    }

    /// A `method` request to `path` (as [`url`](Self::url) resolves it),
    /// carrying the key as the vendor takes it. Send it with
    /// [`json`](Self::json), [`text`](Self::text) or [`bytes`](Self::bytes).
    pub fn request(&self, method: reqwest::Method, path: &str) -> reqwest::RequestBuilder {
        let request = self.client.request(method, self.url(path));
        match self.key_in {
            KeyIn::Bearer => request.bearer_auth(&self.key),
            KeyIn::Header(name) => request.header(name, &self.key),
            KeyIn::Scheme(scheme) => request.header(
                reqwest::header::AUTHORIZATION,
                format!("{scheme} {}", self.key),
            ),
            KeyIn::Basic => match self.key.split_once(':') {
                Some((user, password)) => request.basic_auth(user, Some(password)),
                None => request.basic_auth(&self.key, None::<&str>),
            },
            KeyIn::Manual => request,
        }
    }

    /// `POST` `body` as JSON to `path` and read the answer as JSON. A failed
    /// request or an answer other than 2xx is a [`ToolError`] saying why.
    pub async fn post_json(
        &self,
        path: &str,
        body: &serde_json::Value,
    ) -> Result<serde_json::Value, ToolError> {
        self.json(self.request(reqwest::Method::POST, path).json(body))
            .await
    }

    /// `GET` `path` with `query` and read the answer as JSON, failing as
    /// [`post_json`](Self::post_json) does.
    pub async fn get_json(
        &self,
        path: &str,
        query: &[(&str, String)],
    ) -> Result<serde_json::Value, ToolError> {
        self.json(self.request(reqwest::Method::GET, path).query(query))
            .await
    }

    /// Send `request` and read its answer as JSON; an empty answer, such as
    /// a `204`'s, is `null`. Fails as [`post_json`](Self::post_json) does.
    pub async fn json(
        &self,
        request: reqwest::RequestBuilder,
    ) -> Result<serde_json::Value, ToolError> {
        let text = self.text(request).await?;
        if text.trim().is_empty() {
            return Ok(serde_json::Value::Null);
        }
        serde_json::from_str(&text).map_err(|e| {
            ToolError(format!(
                "the API answered with no JSON ({e}): {}",
                quote(&text)
            ))
        })
    }

    /// Send `request` and read its answer as text, failing as
    /// [`post_json`](Self::post_json) does.
    pub async fn text(&self, request: reqwest::RequestBuilder) -> Result<String, ToolError> {
        let (_, bytes) = self.bytes(request).await?;
        Ok(String::from_utf8_lossy(&bytes).into_owned())
    }

    /// Send `request` and read its answer's media type and bytes, failing
    /// as [`post_json`](Self::post_json) does.
    pub async fn bytes(
        &self,
        request: reqwest::RequestBuilder,
    ) -> Result<(String, Vec<u8>), ToolError> {
        // Its errors, without their URL, name no header, so not the key.
        let resp = request
            .send()
            .await
            .map_err(|e| ToolError(format!("the API could not be reached: {}", e.without_url())))?;
        let status = resp.status();
        let mime = resp
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default()
            .to_owned();
        let bytes = resp.bytes().await.map_err(|e| {
            ToolError(format!(
                "the API's answer could not be read: {}",
                e.without_url()
            ))
        })?;
        if !status.is_success() {
            return Err(ToolError(format!(
                "the API answered {status}: {}",
                quote(&String::from_utf8_lossy(&bytes))
            )));
        }
        Ok((mime, bytes.to_vec()))
    }
}

/// `text` up to [`QUOTED`] bytes, cut at a character.
fn quote(text: &str) -> String {
    if text.len() <= QUOTED {
        return text.to_owned();
    }
    let mut end = QUOTED;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", &text[..end])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_path_goes_under_the_base_and_a_url_stays() {
        let api = Api::new("https://api.example.com/v1/", "k", KeyIn::Bearer);
        assert_eq!(api.url("/search"), "https://api.example.com/v1/search");
        assert_eq!(api.url("search"), "https://api.example.com/v1/search");
        assert_eq!(api.url("http://other.test/x"), "http://other.test/x");
        assert_eq!(api.url("https://other.test/x"), "https://other.test/x");
    }

    #[test]
    fn a_long_body_is_cut() {
        let long = "é".repeat(QUOTED);
        assert!(quote(&long).len() <= QUOTED + "…".len());
        assert_eq!(quote("short"), "short");
    }
}
