//! The vendor's HTTP API, with its key and base URL.

use std::time::Duration;

use crate::KeyIn;

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
    pub fn new(base: impl Into<String>, key: impl Into<String>, key_in: KeyIn) -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(120))
            .user_agent("respan-gateway")
            .build()
            .unwrap_or_default();
        Self {
            client,
            base: base.into().trim_end_matches('/').to_owned(),
            key: key.into(),
            key_in,
        }
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

    /// `POST` `body` as JSON to `path` and read the answer as JSON. A failed
    /// request or an answer other than 2xx is a [`ToolError`] saying why.
    pub async fn post_json(
        &self,
        path: &str,
        body: &serde_json::Value,
    ) -> Result<serde_json::Value, ToolError> {
        self.send(self.client.post(self.url(path)).json(body)).await
    }

    /// `GET` `path` with `query` and read the answer as JSON, failing as
    /// [`post_json`](Self::post_json) does.
    pub async fn get_json(
        &self,
        path: &str,
        query: &[(&str, String)],
    ) -> Result<serde_json::Value, ToolError> {
        self.send(self.client.get(self.url(path)).query(query))
            .await
    }

    /// Send `request` with the key and read its answer as JSON.
    async fn send(&self, request: reqwest::RequestBuilder) -> Result<serde_json::Value, ToolError> {
        let request = match self.key_in {
            KeyIn::Bearer => request.bearer_auth(&self.key),
            KeyIn::Header(name) => request.header(name, &self.key),
        };
        // Its errors, without their URL, name no header, so not the key.
        let resp = request
            .send()
            .await
            .map_err(|e| ToolError(format!("the API could not be reached: {}", e.without_url())))?;
        let status = resp.status();
        let text = resp.text().await.map_err(|e| {
            ToolError(format!(
                "the API's answer could not be read: {}",
                e.without_url()
            ))
        })?;
        if !status.is_success() {
            return Err(ToolError(format!(
                "the API answered {status}: {}",
                quote(&text)
            )));
        }
        serde_json::from_str(&text).map_err(|e| {
            ToolError(format!(
                "the API answered with no JSON ({e}): {}",
                quote(&text)
            ))
        })
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
