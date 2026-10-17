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
//! A server a person must authorize (authorization code with PKCE, or a
//! device code) is logged in to once, by `respan-gateway mcp login <name>`
//! ([`crate::mcp_login`]), and named `auth = "login"`: the login it saved,
//! [`Login`], is read at start, and a refresh token the endpoint rotates is
//! written back to it, so a restart keeps it.
//!
//! A token, a secret and a refresh token are never logged, put in a fault,
//! or shown by `Debug`.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::LazyLock;
use std::time::{Duration, Instant};

use respan_gateway_core::Fault;
use respan_gateway_http::http::{HeaderValue, header};
use respan_gateway_http_transport::reqwest;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::Mutex;

use crate::compose::{key_source, secret};

/// How long the token endpoint has to answer.
const TIMEOUT: Duration = Duration::from_secs(15);
/// A token is renewed this long before it expires.
const SKEW: Duration = Duration::from_secs(30);
/// How long a token lives if the endpoint does not say.
const DEFAULT_LIFETIME: Duration = Duration::from_secs(3600);
/// The longest a token may live in the cache, whatever the endpoint says.
const MAX_LIFETIME: Duration = Duration::from_secs(24 * 3600);

/// The refresh token each one a config names was rotated to, for the life
/// of the process: a reload reads the config's again, which the token
/// endpoint has spent.
static ROTATED: LazyLock<parking_lot::Mutex<HashMap<String, String>>> =
    LazyLock::new(Default::default);

/// What `mcp login` saved of a server's authorization, to get its tokens
/// with: the client it registered and the refresh token it was given.
#[derive(Clone, Serialize, Deserialize)]
pub struct Login {
    pub token_url: String,
    pub client_id: String,
    /// None for a public client, as one registered by the login is.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_secret: Option<String>,
    pub refresh_token: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub scopes: Vec<String>,
    /// The server the tokens are for (RFC 8707), as its metadata names it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resource: Option<String>,
}

impl std::fmt::Debug for Login {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Login")
            .field("token_url", &crate::redact::url(&self.token_url))
            .finish_non_exhaustive()
    }
}

/// Where the logins of a config are kept: `.respan/mcp/` beside its file,
/// else in the working directory.
pub fn logins_dir(source: &crate::config::Source) -> PathBuf {
    let base = match source {
        crate::config::Source::File(path) => path.parent().map(Path::to_path_buf),
        _ => None,
    };
    base.unwrap_or_default().join(".respan").join("mcp")
}

/// Writes `login` to `path`, readable by its owner alone, beside a
/// `.gitignore` that keeps `.respan/` out of version control; atomically, so
/// a reader never sees half of it.
pub fn save(path: &Path, login: &Login) -> std::io::Result<()> {
    let dir = path.parent().unwrap_or(Path::new("."));
    std::fs::create_dir_all(dir)?;
    if let Some(respan) = dir.parent().filter(|d| d.ends_with(".respan")) {
        let ignore = respan.join(".gitignore");
        if !ignore.exists() {
            std::fs::write(ignore, "*\n")?;
        }
    }
    let text = serde_json::to_string_pretty(login).map_err(std::io::Error::other)?;
    let temp = path.with_extension("json.tmp");
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
        options.mode(0o600);
        std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))?;
    }
    std::io::Write::write_all(&mut options.open(&temp)?, text.as_bytes())?;
    std::fs::rename(temp, path)
}

/// `auth = { oauth = { .. } }` of an `mcp` server: the gateway's own client.
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
    client_secret: Option<String>,
    scope: Option<String>,
    resource: Option<String>,
    /// The login file a rotated refresh token is written back to, and what
    /// it holds.
    saved: Option<(PathBuf, Login)>,
    /// The refresh token the config names, under which a rotation is kept
    /// in [`ROTATED`].
    configured: Option<String>,
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
        let read = |what: &str, value: &str| {
            let value = secret(&format!("{owner}: oauth {what}"), value)?;
            match value.trim().is_empty() {
                true => Err(bad(&format!("`{what}` is empty"))),
                false => Ok(value),
            }
        };
        let client_id = read("client_id", &config.client_id)?;
        let client_secret = read("client_secret", &config.client_secret)?;
        let configured = match &config.refresh_token {
            Some(token) => Some(read("refresh_token", token)?),
            None => None,
        };
        let refresh = configured
            .as_ref()
            .map(|token| ROTATED.lock().get(token).unwrap_or(token).clone());
        let from = key_source(&config.client_secret);
        let mut oauth = Self::build(
            owner,
            &config.token_url,
            client_id,
            Some(client_secret),
            &config.scopes,
            refresh,
            from,
        )?;
        oauth.configured = configured;
        Ok(oauth)
    }

    /// The source of server `name`'s login at `path`, which `mcp login`
    /// saved.
    pub fn login(owner: &str, name: &str, path: &Path) -> Result<Self, Fault> {
        let text = std::fs::read_to_string(path).map_err(|e| {
            Fault::config(format!(
                "{owner}: oauth: no login at {} ({e}); run `respan-gateway mcp login {name}`",
                path.display()
            ))
        })?;
        let login: Login = serde_json::from_str(&text).map_err(|e| {
            Fault::config(format!(
                "{owner}: oauth: the login at {} is unreadable: {e}",
                path.display()
            ))
        })?;
        let mut oauth = Self::build(
            owner,
            &login.token_url,
            login.client_id.clone(),
            login.client_secret.clone(),
            &login.scopes,
            Some(login.refresh_token.clone()),
            format!("login {}", path.display()),
        )?;
        oauth.resource.clone_from(&login.resource);
        oauth.saved = Some((path.to_owned(), login));
        Ok(oauth)
    }

    fn build(
        owner: &str,
        token_url: &str,
        client_id: String,
        client_secret: Option<String>,
        scopes: &[String],
        refresh: Option<String>,
        source: String,
    ) -> Result<Self, Fault> {
        let bad = |e: &str| Fault::config(format!("{owner}: oauth: {e}"));
        let token_url =
            reqwest::Url::parse(token_url.trim()).map_err(|_| bad("`token_url` is not a URL"))?;
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
        if scopes.iter().any(|s| {
            s.is_empty() || s.contains(char::is_whitespace) || s.chars().any(char::is_control)
        }) {
            return Err(bad("a scope is one word, with no spaces"));
        }
        let scope = (!scopes.is_empty()).then(|| scopes.join(" "));
        let http = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(TIMEOUT)
            .build()
            .map_err(|e| Fault::internal("no HTTP client for the token endpoint").with_source(e))?;
        Ok(Self {
            token_url,
            client_id,
            client_secret,
            scope,
            resource: None,
            saved: None,
            configured: None,
            http,
            refreshing: refresh.is_some(),
            source,
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
        if let Some(rotated) = rotated {
            if let Some((path, login)) = &self.saved {
                // The old one is spent: a restart needs the new one.
                let kept = Login {
                    refresh_token: rotated.clone(),
                    ..login.clone()
                };
                if let Err(e) = save(path, &kept) {
                    eprintln!(
                        "warning: the rotated refresh token could not be saved to {}: {e}; \
                         log in again after a restart",
                        path.display()
                    );
                }
            }
            if let Some(configured) = &self.configured {
                ROTATED.lock().insert(configured.clone(), rotated.clone());
            }
            state.refresh = Some(rotated);
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
        let mut form: Vec<(&str, &str)> = vec![("client_id", &self.client_id)];
        if let Some(secret) = &self.client_secret {
            form.push(("client_secret", secret));
        }
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
        if let Some(resource) = &self.resource {
            form.push(("resource", resource));
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
