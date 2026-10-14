//! `respan-gateway mcp login <name>`: logs the gateway in to an HTTP MCP
//! server that a person must authorize, once, and saves the login
//! ([`Login`]) beside the config, which `auth = "login"` then names.
//!
//! As MCP's authorization spec has it: the server's `401` names its resource
//! metadata (RFC 9728), which names its authorization server, whose metadata
//! (RFC 8414) names its endpoints. The gateway registers itself as a public
//! client (RFC 7591), then asks for the person's approval by a device code
//! (RFC 8628) where the server offers one, else in a browser (authorization
//! code with PKCE, RFC 7636, to a listener on `127.0.0.1`). Tokens are asked
//! for the server alone (`resource`, RFC 8707), with `offline_access` where
//! offered, so a refresh token comes back: what the login keeps.

use std::path::PathBuf;
use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use respan_gateway_http_transport::reqwest;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::config::Config;
use crate::mcp::McpAuth;
use crate::mcp_oauth::{Login, logins_dir, save};

/// How long a person has to approve.
const APPROVAL: Duration = Duration::from_secs(10 * 60);

/// Logs in to server `name` of `config` and saves the login; where it is.
pub async fn login(config: &Config, name: &str) -> Result<PathBuf, String> {
    let server = config
        .mcp
        .get(name)
        .ok_or_else(|| format!("no mcp server '{name}' in {}", config.source()))?;
    let url = server
        .url
        .as_deref()
        .ok_or_else(|| format!("mcp '{name}' runs a `command`; only a `url` is logged in to"))?;
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .map_err(|e| format!("no HTTP client: {e}"))?;
    let at = Endpoints { http: &http };

    let resource = at.resource_metadata(url).await?;
    let issuer = resource["authorization_servers"][0]
        .as_str()
        .ok_or("the server's resource metadata names no authorization server")?;
    let auth = at.authorization_server(issuer).await?;
    let token_url = text(&auth, "token_endpoint")?;
    let resource_id = resource["resource"].as_str().unwrap_or(url).to_owned();
    let scopes = scopes(&resource, &auth);

    let device = auth["device_authorization_endpoint"].as_str().filter(|_| {
        let grants = auth["grant_types_supported"].as_array();
        grants.is_none_or(|g| g.iter().any(|g| g == DEVICE_GRANT))
    });
    let tokens = match device {
        Some(device_url) => {
            // A redirect URI too, which some servers ask of every client.
            let grants = [DEVICE_GRANT, "authorization_code", "refresh_token"];
            let client = at
                .register(&auth, &grants, "http://127.0.0.1/callback")
                .await?;
            match at
                .device_flow(device_url, &token_url, &client, &scopes, &resource_id)
                .await
            {
                Err(Device::Refused(why)) => {
                    // Some servers list the grant but give it no client they
                    // register.
                    println!("No device code ({why}); logging in in a browser instead.");
                    at.browser_flow(&auth, &token_url, &scopes, &resource_id)
                        .await?
                }
                Err(Device::Failed(e)) => return Err(e),
                Ok(tokens) => tokens,
            }
        }
        None => {
            at.browser_flow(&auth, &token_url, &scopes, &resource_id)
                .await?
        }
    };
    let (client, tokens) = tokens;
    let refresh_token = tokens["refresh_token"]
        .as_str()
        .filter(|t| !t.is_empty())
        .ok_or("the server gave no refresh token, so the login cannot be kept")?
        .to_owned();
    let login = Login {
        token_url,
        client_id: client.id,
        client_secret: client.secret,
        refresh_token,
        scopes,
        resource: Some(resource_id),
    };
    let path = logins_dir(config.source()).join(format!("{name}.json"));
    save(&path, &login)
        .map_err(|e| format!("the login could not be saved to {}: {e}", path.display()))?;
    if !matches!(server.auth, Some(McpAuth::Login)) {
        println!("Set `auth = \"login\"` on [mcp.{name}] for the gateway to use it.");
    }
    Ok(path)
}

const DEVICE_GRANT: &str = "urn:ietf:params:oauth:grant-type:device_code";

/// How a device-code login failed: refused a code, so a browser may do, or
/// failed after.
enum Device {
    Refused(String),
    Failed(String),
}

impl From<String> for Device {
    fn from(e: String) -> Self {
        Device::Failed(e)
    }
}

impl From<&str> for Device {
    fn from(e: &str) -> Self {
        Device::Failed(e.to_owned())
    }
}

/// The client the gateway registered.
struct Client {
    id: String,
    secret: Option<String>,
}

/// The scopes to ask for: the server's own, and `offline_access` where the
/// authorization server offers it, for a refresh token.
fn scopes(resource: &Value, auth: &Value) -> Vec<String> {
    let listed = |v: &Value| -> Vec<String> {
        let all = v["scopes_supported"].as_array().into_iter().flatten();
        all.filter_map(|s| s.as_str().map(str::to_owned)).collect()
    };
    let mut scopes = listed(resource);
    if listed(auth).iter().any(|s| s == "offline_access")
        && !scopes.iter().any(|s| s == "offline_access")
    {
        scopes.push("offline_access".to_owned());
    }
    scopes
}

fn text(v: &Value, key: &str) -> Result<String, String> {
    v[key]
        .as_str()
        .map(str::to_owned)
        .ok_or_else(|| format!("the authorization server's metadata has no `{key}`"))
}

struct Endpoints<'a> {
    http: &'a reqwest::Client,
}

impl Endpoints<'_> {
    async fn get(&self, url: &str) -> Result<Option<Value>, String> {
        let resp = self
            .http
            .get(url)
            .send()
            .await
            .map_err(|e| format!("{url}: {e}"))?;
        if !resp.status().is_success() {
            return Ok(None);
        }
        let bytes = resp.bytes().await.map_err(|e| format!("{url}: {e}"))?;
        Ok(serde_json::from_slice(&bytes).ok())
    }

    /// A form `POST`: its status and JSON answer.
    async fn post_form(&self, url: &str, form: &[(&str, &str)]) -> Result<(u16, Value), String> {
        let body = form_urlencoded::Serializer::new(String::new())
            .extend_pairs(form)
            .finish();
        let resp = self
            .http
            .post(url)
            .header("content-type", "application/x-www-form-urlencoded")
            .header("accept", "application/json")
            .body(body)
            .send()
            .await
            .map_err(|e| format!("{url}: {e}"))?;
        let status = resp.status().as_u16();
        let bytes = resp.bytes().await.map_err(|e| format!("{url}: {e}"))?;
        Ok((
            status,
            serde_json::from_slice(&bytes).unwrap_or(Value::Null),
        ))
    }

    /// The server's resource metadata: where its `401` says, else at the
    /// well-known paths beside it.
    async fn resource_metadata(&self, url: &str) -> Result<Value, String> {
        let hello = json!({ "jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {
            "protocolVersion": "2025-06-18", "capabilities": {},
            "clientInfo": { "name": "respan-gateway", "version": env!("CARGO_PKG_VERSION") } } });
        let resp = self
            .http
            .post(url)
            .header("content-type", "application/json")
            .header("accept", "application/json, text/event-stream")
            .body(hello.to_string())
            .send()
            .await
            .map_err(|e| format!("{url}: {e}"))?;
        if resp.status().is_success() {
            return Err("the server answered without a login: it needs none".into());
        }
        let named = resp
            .headers()
            .get_all("www-authenticate")
            .iter()
            .filter_map(|v| v.to_str().ok())
            .find_map(|v| quoted(v, "resource_metadata"));
        let parsed = reqwest::Url::parse(url).map_err(|e| format!("{url}: {e}"))?;
        let origin = parsed.origin().ascii_serialization();
        let path = parsed.path().trim_end_matches('/');
        let mut tried = named.into_iter().collect::<Vec<_>>();
        tried.push(format!(
            "{origin}/.well-known/oauth-protected-resource{path}"
        ));
        tried.push(format!("{origin}/.well-known/oauth-protected-resource"));
        for at in &tried {
            if let Some(metadata) = self.get(at).await? {
                return Ok(metadata);
            }
        }
        Err(format!(
            "the server says nothing of how to log in (no resource metadata at {})",
            tried.join(", ")
        ))
    }

    /// The authorization server `issuer`'s metadata (RFC 8414), else its
    /// OpenID configuration.
    async fn authorization_server(&self, issuer: &str) -> Result<Value, String> {
        let parsed = reqwest::Url::parse(issuer).map_err(|e| format!("{issuer}: {e}"))?;
        let origin = parsed.origin().ascii_serialization();
        let path = parsed.path().trim_end_matches('/');
        let issuer = issuer.trim_end_matches('/');
        for at in [
            format!("{origin}/.well-known/oauth-authorization-server{path}"),
            format!("{issuer}/.well-known/openid-configuration"),
        ] {
            if let Some(metadata) = self.get(&at).await? {
                return Ok(metadata);
            }
        }
        Err(format!("no metadata for the authorization server {issuer}"))
    }

    /// Registers the gateway as a public client for `grants`.
    async fn register(
        &self,
        auth: &Value,
        grants: &[&str],
        redirect: &str,
    ) -> Result<Client, String> {
        let url = auth["registration_endpoint"].as_str().ok_or(
            "the authorization server takes no client registrations: set \
             `auth = { oauth = { .. } }` with a client it issued instead",
        )?;
        let body = json!({
            "client_name": "respan-gateway",
            "grant_types": grants,
            "response_types": ["code"],
            "redirect_uris": [redirect],
            "token_endpoint_auth_method": "none",
        });
        let resp = self
            .http
            .post(url)
            .header("content-type", "application/json")
            .body(body.to_string())
            .send()
            .await
            .map_err(|e| format!("{url}: {e}"))?;
        let status = resp.status();
        let bytes = resp.bytes().await.map_err(|e| format!("{url}: {e}"))?;
        let answer: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
        let id = answer["client_id"]
            .as_str()
            .filter(|_| status.is_success())
            .ok_or_else(|| {
                format!(
                    "the client registration was refused: {status} {}",
                    error_of(&answer)
                )
            })?;
        Ok(Client {
            id: id.to_owned(),
            secret: answer["client_secret"].as_str().map(str::to_owned),
        })
    }

    /// A device code a person approves elsewhere, then its tokens.
    async fn device_flow(
        &self,
        device_url: &str,
        token_url: &str,
        client: &Client,
        scopes: &[String],
        resource: &str,
    ) -> Result<(Client, Value), Device> {
        let scope = scopes.join(" ");
        let mut form = vec![("client_id", client.id.as_str()), ("resource", resource)];
        if !scope.is_empty() {
            form.push(("scope", &scope));
        }
        let (status, device) = self.post_form(device_url, &form).await?;
        let (Some(code), Some(user_code)) =
            (device["device_code"].as_str(), device["user_code"].as_str())
        else {
            return Err(Device::Refused(format!("{status} {}", error_of(&device))));
        };
        let verify = device["verification_uri_complete"]
            .as_str()
            .or(device["verification_uri"].as_str())
            .ok_or("the device code names no page to approve it at")?;
        println!("To log in, open {verify}\nand confirm the code {user_code}.");
        let mut interval = Duration::from_secs(device["interval"].as_u64().unwrap_or(5).max(1));
        let deadline = tokio::time::Instant::now() + APPROVAL;
        let mut form = vec![
            ("grant_type", DEVICE_GRANT),
            ("device_code", code),
            ("client_id", client.id.as_str()),
            ("resource", resource),
        ];
        if let Some(secret) = &client.secret {
            form.push(("client_secret", secret));
        }
        loop {
            if tokio::time::Instant::now() > deadline {
                return Err("the code was not approved in time".into());
            }
            tokio::time::sleep(interval).await;
            let (status, answer) = self.post_form(token_url, &form).await?;
            match answer["error"].as_str() {
                None if (200..300).contains(&status) => {
                    let client = Client {
                        id: client.id.clone(),
                        secret: client.secret.clone(),
                    };
                    return Ok((client, answer));
                }
                Some("authorization_pending") => {}
                Some("slow_down") => interval += Duration::from_secs(5),
                _ => {
                    let why = error_of(&answer);
                    return Err(format!("the login was refused: {status} {why}").into());
                }
            }
        }
    }

    /// An approval in a browser, sent back to a listener on `127.0.0.1`,
    /// then its tokens.
    async fn browser_flow(
        &self,
        auth: &Value,
        token_url: &str,
        scopes: &[String],
        resource: &str,
    ) -> Result<(Client, Value), String> {
        let authorize = text(auth, "authorization_endpoint")?;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .map_err(|e| format!("no port to hear the browser on: {e}"))?;
        let port = listener.local_addr().map_err(|e| e.to_string())?.port();
        let redirect = format!("http://127.0.0.1:{port}/callback");
        let client = self
            .register(auth, &["authorization_code", "refresh_token"], &redirect)
            .await?;
        let verifier = random(32)?;
        let challenge = URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()));
        let state = random(16)?;
        let mut url = reqwest::Url::parse(&authorize).map_err(|e| format!("{authorize}: {e}"))?;
        {
            let mut query = url.query_pairs_mut();
            query
                .append_pair("response_type", "code")
                .append_pair("client_id", &client.id)
                .append_pair("redirect_uri", &redirect)
                .append_pair("code_challenge", &challenge)
                .append_pair("code_challenge_method", "S256")
                .append_pair("state", &state)
                .append_pair("resource", resource);
            if !scopes.is_empty() {
                query.append_pair("scope", &scopes.join(" "));
            }
        }
        println!("To log in, open this page in a browser on this machine:\n{url}");
        let code = tokio::time::timeout(APPROVAL, callback(&listener, &state))
            .await
            .map_err(|_| "the login was not approved in time".to_owned())??;
        let mut form = vec![
            ("grant_type", "authorization_code"),
            ("code", code.as_str()),
            ("redirect_uri", redirect.as_str()),
            ("client_id", client.id.as_str()),
            ("code_verifier", verifier.as_str()),
            ("resource", resource),
        ];
        if let Some(secret) = &client.secret {
            form.push(("client_secret", secret));
        }
        let (status, answer) = self.post_form(token_url, &form).await?;
        if !(200..300).contains(&status) || answer["error"].is_string() {
            return Err(format!(
                "the code was refused: {status} {}",
                error_of(&answer)
            ));
        }
        Ok((client, answer))
    }
}

/// The code the browser brings back to `listener`, if its `state` matches.
async fn callback(listener: &tokio::net::TcpListener, state: &str) -> Result<String, String> {
    loop {
        let (mut stream, _) = listener.accept().await.map_err(|e| e.to_string())?;
        let mut buf = vec![0u8; 8192];
        let n = stream.read(&mut buf).await.map_err(|e| e.to_string())?;
        let head = String::from_utf8_lossy(&buf[..n]);
        let target = head.split_whitespace().nth(1).unwrap_or_default();
        let Some(query) = target.strip_prefix("/callback?") else {
            let _ = stream
                .write_all(b"HTTP/1.1 404 Not Found\r\ncontent-length: 0\r\n\r\n")
                .await;
            continue;
        };
        let pairs: Vec<(String, String)> = form_urlencoded::parse(query.as_bytes())
            .into_owned()
            .collect();
        let get = |k: &str| {
            pairs
                .iter()
                .find(|(key, _)| key == k)
                .map(|(_, v)| v.clone())
        };
        let page = |text: &str| {
            format!(
                "HTTP/1.1 200 OK\r\ncontent-type: text/plain; charset=utf-8\r\ncontent-length: {}\r\n\r\n{text}",
                text.len()
            )
        };
        if get("state").as_deref() != Some(state) {
            let _ = stream
                .write_all(page("This is not the login asked for.").as_bytes())
                .await;
            continue;
        }
        if let Some(error) = get("error") {
            let _ = stream
                .write_all(page("The login was refused.").as_bytes())
                .await;
            return Err(format!("the login was refused: {error}"));
        }
        let code = get("code").ok_or("the browser brought back no code")?;
        let _ = stream
            .write_all(
                page("Logged in: you can close this tab and return to the terminal.").as_bytes(),
            )
            .await;
        return Ok(code);
    }
}

/// `bytes` random bytes, as base64url.
fn random(bytes: usize) -> Result<String, String> {
    let mut buf = vec![0u8; bytes];
    getrandom::fill(&mut buf).map_err(|e| format!("no randomness: {e}"))?;
    Ok(URL_SAFE_NO_PAD.encode(buf))
}

/// The value of `key="…"` in a `WWW-Authenticate` header.
fn quoted(header: &str, key: &str) -> Option<String> {
    let at = header.find(&format!("{key}=\""))? + key.len() + 2;
    let rest = &header[at..];
    Some(rest[..rest.find('"')?].to_owned())
}

/// An OAuth error answer's code and description, for a person.
fn error_of(answer: &Value) -> String {
    let code = answer["error"].as_str().unwrap_or_default();
    let what = answer["error_description"].as_str().unwrap_or_default();
    format!("{code} {what}").trim().to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_challenge_names_its_resource_metadata() {
        let header = r#"Bearer realm="x", resource_metadata="https://a.test/.well-known/oauth-protected-resource/mcp""#;
        assert_eq!(
            quoted(header, "resource_metadata").as_deref(),
            Some("https://a.test/.well-known/oauth-protected-resource/mcp")
        );
        assert_eq!(quoted("Bearer realm=\"x\"", "resource_metadata"), None);
    }

    #[test]
    fn a_refresh_token_is_asked_for_where_offered() {
        let resource = json!({ "scopes_supported": ["openid", "email"] });
        let auth = json!({ "scopes_supported": ["openid", "offline_access"] });
        assert_eq!(
            scopes(&resource, &auth),
            ["openid", "email", "offline_access"]
        );
        assert_eq!(scopes(&json!({}), &json!({})), Vec::<String>::new());
    }
}
