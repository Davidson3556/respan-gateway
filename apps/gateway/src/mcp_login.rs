//! `respan-gateway mcp login <name>`: logs the gateway in to an HTTP MCP
//! server that a person must authorize, once, and saves the login
//! ([`Login`]) beside the config, which `auth = "login"` then names.
//!
//! As MCP's authorization spec has it: the server's `401` names its resource
//! metadata (RFC 9728), which names its authorization server, whose metadata
//! (RFC 8414) names its endpoints. The gateway registers itself as a public
//! client (RFC 7591), then asks for the person's approval by a device code
//! (RFC 8628) where the server offers one, else in a browser (authorization
//! code with PKCE, RFC 7636, to a listener on `127.0.0.1`). Enter opens the
//! page in the default browser; where the browser runs elsewhere (over SSH,
//! with no display), the address it lands on after approving is pasted at
//! the terminal instead. Tokens are asked
//! for the server alone (`resource`, RFC 8707), with `offline_access` where
//! offered, so a refresh token comes back: what the login keeps.

use std::io::IsTerminal;
use std::path::PathBuf;
use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use respan_gateway_http_transport::reqwest;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;

use crate::config::Config;
use crate::mcp::McpAuth;
use crate::mcp_oauth::{Login, logins_dir, save};

/// How long a person has to approve.
const APPROVAL: Duration = Duration::from_secs(10 * 60);

/// The page the browser lands on after approving.
const PAGE: &str = include_str!("mcp_login.html");

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
    let at = Endpoints {
        http: &http,
        server: name,
    };

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
    /// The MCP server's name in the config, for a person.
    server: &'a str,
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
        println!(
            "To log in to '{}', open\n\n  {verify}\n\nand confirm the code {user_code}.",
            self.server
        );
        // Enter opens the page, while the code is polled for.
        let _opener = typed().filter(|_| has_browser()).map(|mut lines| {
            println!("Press Enter to open it in your browser.");
            let verify = verify.to_owned();
            Abort(tokio::spawn(async move {
                while lines.recv().await.is_some() {
                    open(&verify);
                }
            }))
        });
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
        let code = tokio::time::timeout(APPROVAL, self.approval(&listener, &state, url.as_str()))
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

    /// The code of an approval at `url`: brought back to `listener` by the
    /// browser, or the address it landed on pasted at the terminal.
    async fn approval(
        &self,
        listener: &tokio::net::TcpListener,
        state: &str,
        url: &str,
    ) -> Result<String, String> {
        let lines = typed();
        println!(
            "To log in to '{}', approve the gateway at:\n\n  {url}\n",
            self.server
        );
        match (&lines, has_browser()) {
            (Some(_), true) => println!(
                "Press Enter to open it in your browser. If the browser is on another \
                 machine, paste here the address it lands on after you approve."
            ),
            (Some(_), false) => println!(
                "Open it in a browser on any machine. After you approve, the browser goes \
                 to a 127.0.0.1 address that may not load there; paste that address here."
            ),
            (None, _) => println!("Open it in a browser on this machine."),
        }
        let heard = callback(listener, state, self.server);
        tokio::pin!(heard);
        let Some(mut lines) = lines else {
            return heard.await;
        };
        loop {
            tokio::select! {
                code = &mut heard => return code,
                line = lines.recv() => match line.as_deref().map(str::trim) {
                    None => return (&mut heard).await,
                    Some("") => open(url),
                    Some(line) => match pasted(line, state) {
                        Some(Heard::Code(code)) => return Ok(code),
                        Some(Heard::Refused(why)) => {
                            return Err(format!("the login was refused: {why}"));
                        }
                        Some(Heard::Stranger) => println!(
                            "That address is from another login; paste the one this \
                             login's page sent the browser to."
                        ),
                        None => println!(
                            "That is not the address the browser landed on: it starts \
                             with http://127.0.0.1 and has ?code= in it."
                        ),
                    },
                },
            }
        }
    }
}

/// What came back of an approval.
#[derive(Debug, PartialEq)]
enum Heard {
    Code(String),
    /// Refused, and why.
    Refused(String),
    /// The answer to another login: its `state` is not this one's.
    Stranger,
}

/// What the query of a callback says, for the login of `state`.
fn heard(query: &str, state: &str) -> Heard {
    let pairs: Vec<(String, String)> = form_urlencoded::parse(query.as_bytes())
        .into_owned()
        .collect();
    let get = |k: &str| {
        pairs
            .iter()
            .find(|(key, _)| key == k)
            .map(|(_, v)| v.as_str())
    };
    if get("state") != Some(state) {
        return Heard::Stranger;
    }
    if let Some(error) = get("error") {
        let what = get("error_description").unwrap_or_default();
        return Heard::Refused(format!("{error} {what}").trim().to_owned());
    }
    match get("code") {
        Some(code) => Heard::Code(code.to_owned()),
        None => Heard::Refused("the browser brought back no code".into()),
    }
}

/// What a pasted line says: the address the browser landed on, or its
/// query alone; `None` for neither.
fn pasted(line: &str, state: &str) -> Option<Heard> {
    let query = line.split_once('?').map_or(line, |(_, q)| q);
    let query = query.split('#').next().unwrap_or_default();
    let named = |k: &str| query.split('&').any(|p| p.starts_with(&format!("{k}=")));
    (named("code") || named("error")).then(|| heard(query, state))
}

/// The code the browser brings back to `listener`, if its `state` matches;
/// each visit is answered with a page for the person at the browser.
async fn callback(
    listener: &tokio::net::TcpListener,
    state: &str,
    server: &str,
) -> Result<String, String> {
    let server = escape(server);
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
        let (answer, done) = match heard(query, state) {
            Heard::Code(code) => (
                page(
                    "200 OK",
                    true,
                    &format!("Logged in to {server}"),
                    "The gateway has its login. You can close this tab and return to the terminal.",
                ),
                Some(Ok(code)),
            ),
            Heard::Refused(why) => (
                page(
                    "200 OK",
                    false,
                    "Login refused",
                    &format!(
                        "{server} refused the login: <code>{}</code>. Return to the terminal to try again.",
                        escape(&why)
                    ),
                ),
                Some(Err(format!("the login was refused: {why}"))),
            ),
            Heard::Stranger => (
                page(
                    "400 Bad Request",
                    false,
                    "Not this login",
                    "This page is from another login attempt. Return to the terminal and open the page it shows now.",
                ),
                None,
            ),
        };
        let _ = stream.write_all(answer.as_bytes()).await;
        if let Some(done) = done {
            return done;
        }
    }
}

/// An HTTP answer of the login page: `ok` or not, its `title` and `detail`
/// HTML.
fn page(status: &str, ok: bool, title: &str, detail: &str) -> String {
    let html = PAGE
        .replace("{{tone}}", if ok { "ok" } else { "bad" })
        .replace("{{title}}", title)
        .replace("{{detail}}", detail);
    format!(
        "HTTP/1.1 {status}\r\ncontent-type: text/html; charset=utf-8\r\n\
         cache-control: no-store\r\nconnection: close\r\ncontent-length: {}\r\n\r\n{html}",
        html.len()
    )
}

/// `text` as HTML text.
fn escape(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

/// Whether a browser opens here: not over SSH, and on Linux and the BSDs
/// only with a display.
fn has_browser() -> bool {
    let set = |k: &str| std::env::var_os(k).is_some_and(|v| !v.is_empty());
    if set("SSH_CONNECTION") || set("SSH_TTY") {
        return false;
    }
    !cfg!(unix) || cfg!(target_os = "macos") || set("DISPLAY") || set("WAYLAND_DISPLAY")
}

/// Opens `url` in the default browser.
fn open(url: &str) {
    match webbrowser::open(url) {
        Ok(()) => println!("Opened in your browser; waiting for your approval..."),
        Err(_) => println!("No browser opens here: open the page on another machine."),
    }
}

/// The lines a person types, where one is at a terminal: read on a thread
/// of its own, as a read blocked on stdin would keep a runtime from
/// shutting down.
fn typed() -> Option<mpsc::UnboundedReceiver<String>> {
    if !std::io::stdin().is_terminal() {
        return None;
    }
    let (tx, rx) = mpsc::unbounded_channel();
    std::thread::spawn(move || {
        for line in std::io::stdin().lines() {
            let Ok(line) = line else { break };
            if tx.send(line).is_err() {
                break;
            }
        }
    });
    Some(rx)
}

/// A task aborted when this is dropped.
struct Abort(tokio::task::JoinHandle<()>);

impl Drop for Abort {
    fn drop(&mut self) {
        self.0.abort();
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
    fn a_pasted_address_brings_its_code_back() {
        let at = "http://127.0.0.1:4321/callback?code=abc&state=s1";
        assert_eq!(pasted(at, "s1"), Some(Heard::Code("abc".into())));
        assert_eq!(
            pasted("code=abc&state=s1#x", "s1"),
            Some(Heard::Code("abc".into()))
        );
        assert_eq!(pasted(at, "s2"), Some(Heard::Stranger));
        let refused = "/callback?error=access_denied&error_description=no&state=s1";
        assert_eq!(
            pasted(refused, "s1"),
            Some(Heard::Refused("access_denied no".into()))
        );
        assert_eq!(pasted("hello", "s1"), None);
        assert_eq!(
            pasted("http://127.0.0.1:4321/callback?state=s1", "s1"),
            None
        );
    }

    #[test]
    fn the_login_page_escapes_what_the_server_said() {
        let answer = page("200 OK", false, "t", &escape("<script>"));
        assert!(answer.contains("&lt;script&gt;") && !answer.contains("<script>"));
        assert!(answer.contains("class=\"bad\""));
        let body = answer.split("\r\n\r\n").nth(1).expect("a body");
        assert!(answer.contains(&format!("content-length: {}\r\n", body.len())));
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
