//! OAuth for an HTTP MCP server, machine to machine: `oauth` on an `mcp`
//! server has the gateway get its own access token from a token endpoint and
//! send it as `Authorization: Bearer`, the way `key` sends a fixed one.
//!
//! Two grants (RFC 6749): **client credentials** (`client_id` and
//! `client_secret`), or, when `refresh_token` is set, the **refresh token**
//! grant (a token a person authorized once, by whatever means, and the
//! gateway keeps alive; if the endpoint rotates it, the new one is kept in
//! memory for the life of the process). The access token is cached until it
//! expires, and a `401` from the server drops it, gets a new one and sends
//! the call once more.
//!
//! The browser flow (authorization code with PKCE, and a person to approve
//! it) is not here: a gateway has no person to ask. For such a server, give
//! `key` a personal access token, or run the flow elsewhere and set the
//! `refresh_token` it returns.
//!
//! A token, a secret and a refresh token are never logged, put in a fault,
//! or shown by `Debug`.

use std::time::{Duration, Instant};

use respan_gateway_core::Fault;
use respan_gateway_http::http::{HeaderValue, header};
use respan_gateway_http_transport::reqwest;
use serde::Deserialize;
use serde_json::Value;
use tokio::sync::Mutex;

use crate::compose::secret;

/// How long the token endpoint has to answer.
const TIMEOUT: Duration = Duration::from_secs(15);
/// A token is renewed this long before it expires.
const SKEW: Duration = Duration::from_secs(30);
/// How long a token lives if the endpoint does not say.
const DEFAULT_LIFETIME: Duration = Duration::from_secs(3600);
/// The longest a token may live in the cache, whatever the endpoint says.
const MAX_LIFETIME: Duration = Duration::from_secs(24 * 3600);

/// `oauth` of an `mcp` server.
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OAuthConfig {
    /// The token endpoint, `https://auth.example.com/oauth/token`.
    pub token_url: String,
    /// The gateway's client id, literal or `env:NAME`.
    pub client_id: String,
    /// Its client secret, literal or `env:NAME`.
    pub client_secret: String,
    /// The scopes asked for; the server's default if none.
    #[serde(default)]
    pub scopes: Vec<String>,
    /// A refresh token, literal or `env:NAME`: with it the gateway uses the
    /// refresh token grant, else client credentials.
    pub refresh_token: Option<String>,
}

impl std::fmt::Debug for OAuthConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OAuthConfig")
            .field("token_url", &crate::redact::url(&self.token_url))
            .finish_non_exhaustive()
    }
}

/// A server's token source.
pub struct OAuth {
    token_url: reqwest::Url,
    client_id: String,
    client_secret: String,
    scope: Option<String>,
    http: reqwest::Client,
    state: Mutex<State>,
    /// `env X` or `config`, for a person to read.
    pub source: String,
    refreshing: bool,
}

struct State {
    access: Option<(String, Instant)>,
    refresh: Option<String>,
}

impl std::fmt::Debug for OAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OAuth")
            .field("grant", &self.grant())
            .finish_non_exhaustive()
    }
}

impl OAuth {
    /// The source `config` names, its secrets read. `owner` names the server
    /// in a fault.
    pub fn new(owner: &str, config: &OAuthConfig) -> Result<Self, Fault> {
        let bad = |e: &str| Fault::config(format!("{owner}: oauth: {e}"));
        let token_url = reqwest::Url::parse(config.token_url.trim())
            .map_err(|_| bad("`token_url` is not a URL"))?;
        if !matches!(token_url.scheme(), "http" | "https") || token_url.fragment().is_some() {
            return Err(bad("`token_url` is an http(s) URL with no fragment"));
        }
        let host = token_url.host_str().unwrap_or_default();
        let host = host.trim_start_matches('[').trim_end_matches(']');
        let loopback = host.eq_ignore_ascii_case("localhost")
            || host
                .parse::<std::net::IpAddr>()
                .is_ok_and(|ip| ip.is_loopback());
        if token_url.scheme() == "http" && !loopback {
            return Err(bad(
                "`token_url` must be https: the client secret would be sent in clear \
                 (plain http is for localhost only)",
            ));
        }
        let read = |what: &str, value: &str| {
            let value = secret(&format!("{owner}: oauth {what}"), value)?;
            match value.trim().is_empty() {
                true => Err(bad(&format!("`{what}` is empty"))),
                false => Ok(value),
            }
        };
        let client_id = read("client_id", &config.client_id)?;
        let client_secret = read("client_secret", &config.client_secret)?;
        let refresh = match &config.refresh_token {
            Some(token) => Some(read("refresh_token", token)?),
            None => None,
        };
        if config.scopes.iter().any(|s| {
            s.is_empty() || s.contains(char::is_whitespace) || s.chars().any(char::is_control)
        }) {
            return Err(bad("a scope is one word, with no spaces"));
        }
        let scope = (!config.scopes.is_empty()).then(|| config.scopes.join(" "));
        let http = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(TIMEOUT)
            .build()
            .map_err(|e| Fault::internal("no HTTP client for the token endpoint").with_source(e))?;
        let from = |value: &str| match value.strip_prefix("env:") {
            Some(var) => format!("env {var}"),
            None => "config".to_owned(),
        };
        Ok(Self {
            token_url,
            client_id,
            client_secret,
            scope,
            http,
            refreshing: refresh.is_some(),
            source: from(&config.client_secret),
            state: Mutex::new(State {
                access: None,
                refresh,
            }),
        })
    }

    /// `client credentials` or `refresh token`.
    pub fn grant(&self) -> &'static str {
        match self.refreshing {
            true => "refresh token",
            false => "client credentials",
        }
    }

    /// A token to send: the cached one while it lasts, else a new one. Pass
    /// the token a server refused as `stale` to get a different one: it is
    /// dropped if still the cached one, and replaced.
    pub async fn token(&self, stale: Option<&str>) -> Result<String, Fault> {
        let mut state = self.state.lock().await;
        if let Some((token, expires)) = &state.access
            && Instant::now() < *expires
            && stale != Some(token.as_str())
        {
            return Ok(token.clone());
        }
        state.access = None;
        let (token, lifetime, rotated) = self.fetch(state.refresh.as_deref()).await?;
        let lifetime = lifetime.min(MAX_LIFETIME);
        state.access = Some((
            token.clone(),
            Instant::now() + lifetime.saturating_sub(SKEW),
        ));
        if rotated.is_some() {
            state.refresh = rotated;
        }
        Ok(token)
    }

    /// `Authorization: Bearer <token>`, marked sensitive.
    pub fn header(token: &str) -> Result<HeaderValue, Fault> {
        let mut value = HeaderValue::from_str(&format!("Bearer {token}"))
            .map_err(|_| Fault::internal("the token endpoint gave a token no header can hold"))?;
        value.set_sensitive(true);
        Ok(value)
    }

    /// A new access token, its lifetime, and the refresh token the endpoint
    /// rotated to, if it did.
    async fn fetch(
        &self,
        refresh: Option<&str>,
    ) -> Result<(String, Duration, Option<String>), Fault> {
        let mut form: Vec<(&str, &str)> = vec![
            ("client_id", &self.client_id),
            ("client_secret", &self.client_secret),
        ];
        match refresh {
            Some(token) => {
                form.push(("grant_type", "refresh_token"));
                form.push(("refresh_token", token));
            }
            None => form.push(("grant_type", "client_credentials")),
        }
        if let Some(scope) = &self.scope {
            form.push(("scope", scope));
        }
        // application/x-www-form-urlencoded, as a URL's query is.
        let mut encoder = self.token_url.clone();
        encoder.set_query(None);
        encoder.query_pairs_mut().extend_pairs(&form);
        let body = encoder.query().unwrap_or_default().to_owned();
        let sent = self
            .http
            .post(self.token_url.clone())
            .header(header::ACCEPT, "application/json")
            .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .body(body)
            .send()
            .await;
        // The error would name the URL; the cause alone is for a person.
        let resp = sent.map_err(|e| {
            let cause = respan_gateway_http_transport::unreachable(&e);
            Fault::upstream(format!("the OAuth token endpoint {cause}"))
        })?;
        let status = resp.status();
        let body = resp
            .bytes()
            .await
            .map_err(|_| Fault::upstream("the OAuth token endpoint's answer broke off"))?;
        let json: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
        if !status.is_success() {
            // `error` is a code from RFC 6749 (5.2); anything odd is left out.
            let code = json["error"].as_str().filter(|c| {
                c.len() <= 64 && c.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
            });
            return Err(Fault::upstream(match code {
                Some(code) => format!("the OAuth token endpoint refused: {status} ({code})"),
                None => format!("the OAuth token endpoint refused: {status}"),
            }));
        }
        let Some(token) = json["access_token"].as_str().filter(|t| !t.is_empty()) else {
            return Err(Fault::upstream(
                "the OAuth token endpoint's answer has no access_token",
            ));
        };
        if let Some(kind) = json["token_type"].as_str()
            && !kind.eq_ignore_ascii_case("bearer")
        {
            return Err(Fault::upstream(
                "the OAuth token endpoint gave a token that is not a bearer token",
            ));
        }
        let seconds = match &json["expires_in"] {
            Value::Number(n) => n.as_f64(),
            Value::String(s) => s.parse().ok(),
            _ => None,
        };
        let lifetime = seconds
            .filter(|s| s.is_finite() && *s >= 0.0)
            .map_or(DEFAULT_LIFETIME, Duration::from_secs_f64);
        let rotated = json["refresh_token"]
            .as_str()
            .filter(|t| !t.is_empty())
            .map(str::to_owned);
        Ok((token.to_owned(), lifetime, rotated))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(text: &str) -> Result<OAuth, String> {
        let config: OAuthConfig = toml::from_str(text).map_err(|e| e.to_string())?;
        OAuth::new("mcp 'm'", &config).map_err(|e| e.message().to_owned())
    }

    #[test]
    fn a_grant_is_client_credentials_or_a_refresh_token() {
        let base = "token_url = \"https://a.test/token\"\nclient_id = \"id\"\nclient_secret = \"s3cret\"\n";
        let plain = config(base).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(plain.grant(), "client credentials");
        let refreshed = config(&format!(
            "{base}refresh_token = \"r\"\nscopes = [\"a\", \"b\"]\n"
        ))
        .unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(refreshed.grant(), "refresh token");
        assert_eq!(refreshed.scope.as_deref(), Some("a b"));
    }

    #[test]
    fn debug_shows_no_secret() {
        let base = "token_url = \"https://a.test/token\"\nclient_id = \"id-1\"\nclient_secret = \"s3cret\"\nrefresh_token = \"r3fresh\"\n";
        let shown = format!("{:?}", config(base).unwrap_or_else(|e| panic!("{e}")));
        for secret in ["s3cret", "r3fresh", "id-1"] {
            assert!(!shown.contains(secret), "{shown}");
        }
    }

    #[test]
    fn mistakes_are_named() {
        let said = |text: &str| config(text).err().unwrap_or_default();
        let ok = "client_id = \"i\"\nclient_secret = \"s\"\n";
        assert!(said(&format!("token_url = 'nope'\n{ok}")).contains("not a URL"));
        assert!(said(&format!("token_url = 'ftp://a.test/t'\n{ok}")).contains("http(s)"));
        assert!(said(&format!("token_url = 'http://a.test/t'\n{ok}")).contains("https"));
        for local in ["localhost:1", "127.0.0.1:1", "[::1]:1"] {
            assert_eq!(said(&format!("token_url = 'http://{local}/t'\n{ok}")), "");
        }
        assert!(
            said("token_url = \"https://a.test/t\"\nclient_id = \"\"\nclient_secret = \"s\"\n")
                .contains("client_id")
        );
        assert!(
            said(
                "token_url = \"https://a.test/t\"\nclient_id = \"i\"\nclient_secret = \"env:NO_SUCH_OAUTH_X\"\n"
            )
            .contains("NO_SUCH_OAUTH_X")
        );
        assert!(
            said(&format!(
                "token_url = \"https://a.test/t\"\n{ok}scopes = [\"a b\"]\n"
            ))
            .contains("scope")
        );
        assert!(
            said(&format!(
                "token_url = \"https://a.test/t\"\n{ok}extra = 1\n"
            ))
            .contains("extra")
        );
    }
}
