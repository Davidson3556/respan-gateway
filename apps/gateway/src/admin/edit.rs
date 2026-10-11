//! Reading, checking and rewriting the config through the admin API.
//!
//! The config is shown with every secret redacted. An edit of that view
//! comes back with the redaction placeholder where a secret was, and the
//! secret the file holds there is kept; see [`restore`].

use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use axum::Json;
use axum::extract::State;
use axum::response::{IntoResponse, Response};
use respan_gateway_http::http::StatusCode;
use serde_json::{Value as Json_, json};
use serde_norway::Value;

use super::Admin;
use crate::app::ReloadError;
use crate::config::{Config, Source};

/// What stands for a secret in the config the admin API shows.
pub const PLACEHOLDER: &str = "…";

/// One save at a time, so the backup and the file stay a pair.
static SAVING: Mutex<()> = Mutex::new(());

fn error(status: StatusCode, kind: &str, message: &str, at: Option<(u64, u64)>) -> Response {
    let (line, column) = at.unzip();
    let body =
        json!({ "error": { "message": message, "type": kind, "line": line, "column": column } });
    (status, Json(body)).into_response()
}

// --- which values are secrets ------------------------------------------

/// Whether `value`, at `path` in the config, is a secret: a client key
/// (the entry, or a named entry's `key`), a provider's or MCP server's key
/// (also inside `auth`), an MCP server's OAuth client secret and refresh
/// token, environment value or program argument (not the program), and an
/// MCP URL that holds credentials. An `env:NAME` reference is no secret; it is
/// shown as written.
fn is_secret(path: &[String], value: &Value) -> bool {
    let path: Vec<&str> = path.iter().map(String::as_str).collect();
    match path.as_slice() {
        ["client_keys", _] => value.is_string(),
        ["client_keys", _, "key"]
        | ["providers", _, "key"]
        | ["providers", _, "auth", "key"]
        | ["mcp", _, "key"]
        | ["mcp", _, "auth", "key"]
        | ["mcp", _, "oauth", "client_secret" | "refresh_token"]
        | ["mcp", _, "env", _] => !value.is_null(),
        ["mcp", _, "command", i] => *i != "0" && !value.is_null(),
        ["mcp", _, "url"] | ["providers", _, "base_url"] => value
            .as_str()
            .is_some_and(|u| u == PLACEHOLDER || url_holds_secret(u)),
        _ => false,
    }
}

fn is_reference(value: &Value) -> bool {
    value.as_str().is_some_and(|s| s.starts_with("env:"))
}

/// A URL with credentials in it: a userinfo, a query or fragment, or a
/// path segment that looks like a token (24 or more characters of letters
/// and digits, with both).
fn url_holds_secret(url: &str) -> bool {
    let rest = url.split_once("://").map_or(url, |(_, rest)| rest);
    let (authority, tail) = rest.split_at(rest.find(['/', '?', '#']).unwrap_or(rest.len()));
    let token = |segment: &str| {
        let plain = |c: char| c.is_ascii_alphanumeric() || c == '-' || c == '_';
        segment.len() >= 24
            && segment.chars().all(plain)
            && segment.chars().any(|c| c.is_ascii_digit())
            && segment.chars().any(|c| c.is_ascii_alphabetic())
    };
    authority.contains('@') || tail.contains(['?', '#']) || tail.split('/').any(token)
}

fn name(key: &Value) -> String {
    match key {
        Value::String(s) => s.clone(),
        other => serde_norway::to_string(other)
            .unwrap_or_default()
            .trim()
            .to_owned(),
    }
}

/// Calls `found` on each secret in `value`, which sits at `path`.
fn secrets(value: &mut Value, path: &mut Vec<String>, found: &mut dyn FnMut(&mut Value)) {
    if is_secret(path, value) {
        return found(value);
    }
    match value {
        Value::Mapping(map) => {
            for (key, child) in map.iter_mut() {
                path.push(name(key));
                secrets(child, path, found);
                path.pop();
            }
        }
        Value::Sequence(items) => {
            for (i, child) in items.iter_mut().enumerate() {
                path.push(i.to_string());
                secrets(child, path, found);
                path.pop();
            }
        }
        _ => {}
    }
}

/// The name a bare client key is logged under, and shown under here.
fn bare_name(index: usize) -> String {
    format!("key-{}", index + 1)
}

/// `config`'s bare literal client keys as `{ name: key-<n>, key }`, so each
/// has the identity a restore matches it by.
fn name_bare_keys(config: &mut Value) {
    let Some(Value::Sequence(items)) = config.get_mut("client_keys") else {
        return;
    };
    for (i, item) in items.iter_mut().enumerate() {
        if let Value::String(key) = item
            && !key.starts_with("env:")
        {
            let mut named = serde_norway::Mapping::new();
            named.insert("name".into(), bare_name(i).into());
            named.insert("key".into(), std::mem::take(key).into());
            *item = Value::Mapping(named);
        }
    }
}

/// `config` with every secret replaced by [`PLACEHOLDER`]. A bare client
/// key is shown as a named one (`key-<n>`, the name it is logged under).
pub fn redact(mut config: Value) -> Value {
    name_bare_keys(&mut config);
    secrets(&mut config, &mut Vec::new(), &mut |secret| {
        if !is_reference(secret) {
            *secret = Value::String(PLACEHOLDER.to_owned());
        }
    });
    config
}

/// Every literal secret in `config`, to keep out of messages.
fn literals(config: &Value) -> Vec<String> {
    let mut found = Vec::new();
    secrets(&mut config.clone(), &mut Vec::new(), &mut |secret| {
        if let Value::String(s) = secret
            && !s.is_empty()
            && !s.starts_with("env:")
            && s != PLACEHOLDER
        {
            found.push(s.clone());
        }
    });
    found
}

/// `message` without any of `secrets`.
fn scrub(message: &str, secrets: &[String]) -> String {
    secrets.iter().fold(message.to_owned(), |message, secret| {
        message.replace(secret.as_str(), PLACEHOLDER)
    })
}

/// `edited`, an edit of the redacted view, with each [`PLACEHOLDER`] at a
/// secret replaced by what `current` holds at that place, so a view edited
/// and sent back leaves its secrets as they are. A client key is matched by
/// its `name` (a bare one is `key-<n>` by its place in the file), so it may
/// be deleted, reordered or inserted; any other list by position, which
/// holds only while its length does, so a list that changed length with a
/// placeholder in it is refused. Returns how many it restored, or the
/// place of one that has nothing to restore.
fn restore(edited: &mut Value, current: Option<&Value>) -> Result<usize, String> {
    fn walk(
        edited: &mut Value,
        current: Option<&Value>,
        path: &mut Vec<String>,
        restored: &mut usize,
    ) -> Result<(), String> {
        if edited.as_str() == Some(PLACEHOLDER) && is_secret(path, edited) {
            let Some(secret) = current else {
                return Err(format!(
                    "{}: there is no current value to keep behind '{PLACEHOLDER}'; \
                     give the value or an env:NAME reference",
                    path.join(".")
                ));
            };
            *edited = secret.clone();
            *restored += 1;
            return Ok(());
        }
        match edited {
            Value::Mapping(map) => {
                for (key, child) in map.iter_mut() {
                    let key = name(key);
                    let before = current.and_then(|c| c.get(key.as_str()));
                    path.push(key);
                    walk(child, before, path, restored)?;
                    path.pop();
                }
            }
            Value::Sequence(items) => {
                let held = current.and_then(Value::as_sequence).map(Vec::len);
                let marked = items.iter().any(|i| i.as_str() == Some(PLACEHOLDER));
                if marked && held.is_some_and(|len| len != items.len()) {
                    return Err(format!(
                        "{}: the list changed length, so '{PLACEHOLDER}' cannot say which \
                         value it keeps; give the values",
                        path.join(".")
                    ));
                }
                for (i, child) in items.iter_mut().enumerate() {
                    let before = current.and_then(|c| c.get(i));
                    path.push(i.to_string());
                    walk(child, before, path, restored)?;
                    path.pop();
                }
            }
            _ => {}
        }
        Ok(())
    }
    let mut restored = restore_client_keys(edited, current)?;
    walk(edited, current, &mut Vec::new(), &mut restored)?;
    Ok(restored)
}

/// Restores the `key` placeholders of `client_keys` entries by `name`.
fn restore_client_keys(edited: &mut Value, current: Option<&Value>) -> Result<usize, String> {
    let Some(Value::Sequence(items)) = edited.get_mut("client_keys") else {
        return Ok(0);
    };
    let mut held: std::collections::HashMap<String, String> = std::collections::HashMap::new();
    let listed = current.and_then(|c| c.get("client_keys"));
    let listed = listed.and_then(Value::as_sequence);
    for (i, entry) in listed.into_iter().flatten().enumerate() {
        let (name, key) = match entry {
            Value::String(key) => (Some(bare_name(i)), Some(key.as_str())),
            entry => (
                entry.get("name").and_then(Value::as_str).map(str::to_owned),
                entry.get("key").and_then(Value::as_str),
            ),
        };
        if let (Some(name), Some(key)) = (name, key) {
            held.entry(name).or_insert_with(|| key.to_owned());
        }
    }
    let mut restored = 0;
    for entry in items.iter_mut() {
        if entry.as_str() == Some(PLACEHOLDER) {
            return Err(format!(
                "client_keys: '{PLACEHOLDER}' alone keeps nothing; write it as \
                 {{ name: <the key's name>, key: '{PLACEHOLDER}' }}"
            ));
        }
        if entry.get("key").and_then(Value::as_str) != Some(PLACEHOLDER) {
            continue;
        }
        let name = entry
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let Some(key) = held.get(name).cloned() else {
            return Err(format!(
                "client_keys: there is no key named '{name}' in the file to keep behind \
                 '{PLACEHOLDER}'; give the key or an env:NAME reference"
            ));
        };
        entry["key"] = Value::String(key);
        restored += 1;
    }
    Ok(restored)
}

/// What the admin API shows of a config that has no file: its providers,
/// from the environment.
pub fn stand_in(config: &Config) -> Value {
    let providers = config.providers.iter().map(|(name, p)| {
        let mut provider = serde_norway::Mapping::new();
        let mut put = |key: &str, value: &str| {
            provider.insert(key.into(), Value::String(value.to_owned()));
        };
        if let Some(vendor) = &p.vendor {
            put("vendor", vendor);
        }
        if let Some(base_url) = &p.base_url {
            put("base_url", base_url);
        }
        if let Some(key) = &p.key {
            put("key", key);
        }
        (Value::from(name.as_str()), Value::Mapping(provider))
    });
    let mut root = serde_norway::Mapping::new();
    root.insert("providers".into(), Value::Mapping(providers.collect()));
    redact(Value::Mapping(root))
}

// --- parsing -----------------------------------------------------------

/// `(line, column)` in an error message of serde's, which says `at line N
/// column M`.
fn location(message: &str) -> Option<(u64, u64)> {
    let rest = message.split_once("at line ")?.1;
    let (line, rest) = rest.split_once(" column ")?;
    let column: String = rest.chars().take_while(char::is_ascii_digit).collect();
    Some((line.trim().parse().ok()?, column.parse().ok()?))
}

struct Invalid {
    message: String,
    at: Option<(u64, u64)>,
}

/// `text` as a document, and as a config that would reload, with nothing
/// changed and nothing reached.
fn check(admin: &Admin, text: &str, secrets: &[String]) -> Result<(), Invalid> {
    let invalid = |message: &str| Invalid {
        message: scrub(message, secrets),
        at: location(message),
    };
    let mut config = Config::parse(text).map_err(|e| invalid(&e.to_string()))?;
    config.source = admin.source.clone();
    admin
        .process
        .check(config)
        .map_err(|e| invalid(&e.to_string()))
}

fn document(text: &str) -> Result<Value, Invalid> {
    serde_norway::from_str::<Value>(text).map_err(|e| Invalid {
        message: e.to_string(),
        at: e.location().map(|l| (l.line() as u64, l.column() as u64)),
    })
}

// --- the endpoints -----------------------------------------------------

/// Why writes are off, or `None` when they are on.
fn why_not_writable(admin: &Admin) -> Option<&'static str> {
    if !admin.writes {
        Some("admin_writes is not set in the config")
    } else if !matches!(admin.source, Source::File(_)) {
        Some("the config did not come from a file")
    } else if !admin.auth_required {
        Some("admin_writes needs client_keys")
    } else {
        None
    }
}

/// The config file's document, read now; or why it cannot be.
fn current(admin: &Admin) -> Result<Value, String> {
    match &admin.source {
        Source::File(path) => {
            let unreadable = |what: String| format!("{}: {what}", path.display());
            let text = std::fs::read_to_string(path).map_err(|e| unreadable(e.to_string()))?;
            document(&text).map_err(|e| unreadable(e.message))
        }
        _ => Ok(admin.stand_in.clone()),
    }
}

/// `GET /ui/api/config`: the config file as it is on disk (an environment
/// or other source with no file: its providers) with every secret
/// redacted.
///
/// ```json
/// {
///   "source": "file",                 // "file" | "environment" | "proxy" | "inline"
///   "path": "gateway.yaml",           // null unless source is "file"
///   "writes_enabled": false,
///   "writes_disabled_reason": "admin_writes is not set in the config",  // null when enabled
///   "placeholder": "…",
///   "yaml": "providers: ...",
///   "config": { "providers": { } }    // the same, as JSON
/// }
/// ```
///
/// Redacted: `client_keys`, every provider's and MCP server's `key` and
/// `auth.key`, MCP `env` values and program arguments (not the program),
/// and an MCP `url` that carries a userinfo, query, fragment or token-like
/// path segment, each as `"…"`; an `env:NAME` reference is shown as it is
/// written. The YAML is re-rendered, so comments are not kept.
pub async fn show(State(admin): State<Arc<Admin>>) -> Response {
    let config = match current(&admin) {
        Ok(config) => redact(config),
        Err(message) => {
            let status = StatusCode::INTERNAL_SERVER_ERROR;
            return error(status, "config_unreadable", &message, None);
        }
    };
    let (source, path) = admin.source_and_path();
    let reason = why_not_writable(&admin);
    let yaml = serde_norway::to_string(&config).unwrap_or_default();
    Json(json!({
        "source": source,
        "path": path,
        "writes_enabled": reason.is_none(),
        "writes_disabled_reason": reason,
        "placeholder": PLACEHOLDER,
        "yaml": yaml,
        "config": serde_json::to_value(&config).unwrap_or(Json_::Null),
    }))
    .into_response()
}

/// `POST /ui/api/config/validate` with a YAML body: parses and composes it
/// as a reload would, without changing or reaching anything. Always `200`:
///
/// ```json
/// { "ok": true }
/// { "ok": false, "error": "...", "line": 3, "column": 5 }   // line and column when known
/// ```
///
/// A secret of the body is kept out of the error. A [`PLACEHOLDER`] where
/// a secret goes is read as what the file holds there.
pub async fn validate(State(admin): State<Arc<Admin>>, body: String) -> Json<Json_> {
    Json(match prepare(&admin, &body) {
        Ok(_) => json!({ "ok": true }),
        Err(e) => {
            json!({ "ok": false, "error": e.message, "line": e.at.map(|a| a.0), "column": e.at.map(|a| a.1) })
        }
    })
}

/// The text to write for `body`, checked: `body` itself, or, if it keeps
/// secrets behind placeholders, the document with them restored.
fn prepare(admin: &Admin, body: &str) -> Result<String, Invalid> {
    let mut edited = document(body)?;
    let literal = literals(&edited);
    let bare = |message: String| Invalid { message, at: None };
    let has_placeholder = serde_norway::to_string(&edited)
        .map(|y| y.contains(PLACEHOLDER))
        .unwrap_or(false);
    let text = match has_placeholder {
        true => {
            let file = current(admin)
                .map_err(|e| bare(format!("'{PLACEHOLDER}' has nothing to keep: {e}")))?;
            restore(&mut edited, Some(&file)).map_err(bare)?;
            serde_norway::to_string(&edited).map_err(|e| bare(e.to_string()))?
        }
        false => body.to_owned(),
    };
    let mut known = literals(&edited);
    known.extend(literal);
    check(admin, &text, &known)?;
    Ok(text)
}

/// `PUT /ui/api/config` with a YAML body: checks it as `validate` does,
/// writes it over the config file (a temporary file renamed over it, the
/// previous file kept as `<file>.bak`, the file's permissions kept) and
/// reloads.
///
/// ```json
/// { "saved": true, "path": "gateway.yaml", "backup": "gateway.yaml.bak",
///   "reloaded_at": "2026-10-11T09:30:00Z", "restart_required": [] }
/// ```
///
/// `403` (`admin_writes_disabled`) unless the config says `admin_writes:
/// true`, came from a file and has `client_keys`; `422` (`invalid_config`,
/// with `line` and `column` where known) for a body that would not reload,
/// leaving the file as it was. A secret may be sent as its real value, an
/// `env:NAME` reference, or the [`PLACEHOLDER`], which keeps the file's
/// value at that place (comments in a body that uses it are not kept).
pub async fn save(State(admin): State<Arc<Admin>>, body: String) -> Response {
    if let Some(reason) = why_not_writable(&admin) {
        return error(StatusCode::FORBIDDEN, "admin_writes_disabled", reason, None);
    }
    let Source::File(path) = &admin.source else {
        return error(
            StatusCode::FORBIDDEN,
            "admin_writes_disabled",
            "no file",
            None,
        );
    };
    let _one = SAVING.lock().unwrap_or_else(|e| e.into_inner());
    let text = match prepare(&admin, &body) {
        Ok(text) => text,
        Err(e) => {
            return error(
                StatusCode::UNPROCESSABLE_ENTITY,
                "invalid_config",
                &e.message,
                e.at,
            );
        }
    };
    let backup = match write_atomically(path, &text) {
        Ok(backup) => backup,
        Err(e) => {
            let message = format!("{}: {e}", path.display());
            return error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "write_failed",
                &message,
                None,
            );
        }
    };
    let process = admin.process.clone();
    match process.reload() {
        Ok(reloaded) => {
            let mut answer = super::reloaded_json(&process, &reloaded);
            answer.insert("saved".into(), true.into());
            answer.insert("path".into(), path.display().to_string().into());
            answer.insert(
                "backup".into(),
                backup.map(|b| b.display().to_string()).into(),
            );
            Json(answer).into_response()
        }
        Err(e @ ReloadError::Unavailable) => error(
            StatusCode::CONFLICT,
            "reload_unavailable",
            &format!("saved; {e}"),
            None,
        ),
        Err(e @ ReloadError::Invalid(_)) => {
            let message = format!("saved, but the reload failed: {e}");
            error(
                StatusCode::UNPROCESSABLE_ENTITY,
                "invalid_config",
                &message,
                None,
            )
        }
    }
}

/// Writes `text` to `path` through a temporary file beside it, renamed
/// over it, after copying the file there now to `<path>.bak`. Returns the
/// backup if there was a file to keep. A symlink is written through, and a
/// file that cannot be replaced (a bind mount: `EBUSY`, or another device:
/// `EXDEV`) is written in place instead, after its backup.
fn write_atomically(path: &Path, text: &str) -> std::io::Result<Option<PathBuf>> {
    write_with(path, text, |from, to| std::fs::rename(from, to))
}

fn write_with(
    path: &Path,
    text: &str,
    rename: impl Fn(&Path, &Path) -> std::io::Result<()>,
) -> std::io::Result<Option<PathBuf>> {
    use std::io::ErrorKind::{CrossesDevices, ResourceBusy};
    let resolved = std::fs::canonicalize(path).unwrap_or_else(|_| path.to_owned());
    let path = resolved.as_path();
    let dir = match path.parent() {
        Some(dir) if !dir.as_os_str().is_empty() => dir,
        _ => Path::new("."),
    };
    let file = path
        .file_name()
        .map_or_else(String::new, |n| n.to_string_lossy().into_owned());
    let temporary = dir.join(format!(".{file}.tmp-{}", std::process::id()));
    let result = (|| {
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create(true).truncate(true);
        #[cfg(unix)]
        std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
        let mut out = options.open(&temporary)?;
        out.write_all(text.as_bytes())?;
        out.sync_all()?;
        drop(out);
        let mut backup = None;
        if let Ok(meta) = std::fs::metadata(path) {
            std::fs::set_permissions(&temporary, meta.permissions())?;
            let copy = PathBuf::from(format!("{}.bak", path.display()));
            std::fs::copy(path, &copy)?;
            backup = Some(copy);
        }
        match rename(&temporary, path) {
            Err(e) if matches!(e.kind(), CrossesDevices | ResourceBusy) => {
                std::fs::write(path, text)?;
            }
            other => other?,
        }
        Ok(backup)
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&temporary);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    fn yaml(text: &str) -> Value {
        serde_norway::from_str(text).unwrap_or_else(|e| panic!("{e}"))
    }

    const FULL: &str = "
client_keys: [gw-secret, env:GATEWAY_KEY, { name: app, key: named-secret }]
providers:
  a: { vendor: openai, key: sk-live-1 }
  b: { formats: openai-chat, base_url: 'http://b.test', auth: { type: bearer, key: sk-live-2 } }
  c: { vendor: groq, key: env:GROQ_API_KEY }
mcp:
  m:
    command: [npx, -y, server, --token, tok-1]
    env: { TOKEN: tok-2, FROM_ENV: 'env:X' }
  h: { url: 'https://h.test/mcp/', key: tok-3 }
  t: { url: 'https://t.test/mcp/abcdef0123456789abcdef0123' }
  q: { url: 'https://q.test/mcp?token=zzz', auth: { type: header, name: x-k, key: tok-4 } }
  o:
    url: 'https://o.test/mcp'
    oauth: { token_url: 'https://o.test/token', client_id: cid, client_secret: oauth-secret, refresh_token: oauth-refresh }
";

    #[test]
    fn redacts_every_kind_of_secret_and_keeps_references_and_the_rest() {
        let shown = serde_norway::to_string(&redact(yaml(FULL))).expect("yaml");
        for secret in [
            "gw-secret",
            "sk-live-1",
            "sk-live-2",
            "tok-1",
            "tok-2",
            "tok-3",
            "tok-4",
            "zzz",
            "abcdef0123",
            "named-secret",
            "oauth-secret",
            "oauth-refresh",
        ] {
            assert!(!shown.contains(secret), "{secret} in {shown}");
        }
        for kept in [
            "env:GATEWAY_KEY",
            "env:GROQ_API_KEY",
            "env:X",
            "https://h.test/mcp/",
            "npx",
            "http://b.test",
            "name: app",
            "client_id: cid",
            "https://o.test/token",
        ] {
            assert!(shown.contains(kept), "{kept} missing in {shown}");
        }
        assert_eq!(shown.matches(PLACEHOLDER).count(), 15, "{shown}");
        assert!(shown.contains("name: key-1"), "{shown}");
    }

    #[test]
    fn restores_secrets_behind_placeholders_by_place() {
        let mut edited = redact(yaml(FULL));
        edited["providers"]["a"]["vendor"] = "anthropic".into();
        edited["providers"]["b"]["key"] = "sk-new".into();
        let restored = restore(&mut edited, Some(&yaml(FULL))).expect("restored");
        assert_eq!(restored, 15);
        let mut want = yaml(FULL);
        want["client_keys"][0] = yaml("{ name: key-1, key: gw-secret }");
        want["providers"]["a"]["vendor"] = "anthropic".into();
        want["providers"]["b"]["key"] = "sk-new".into();
        assert_eq!(edited, want);
    }

    #[test]
    fn a_placeholder_with_nothing_to_keep_is_refused_and_one_elsewhere_is_data() {
        let mut edited = yaml("providers: { new: { vendor: openai, key: '…' } }");
        let e = restore(&mut edited, Some(&yaml(FULL))).expect_err("refused");
        assert!(e.starts_with("providers.new.key"), "{e}");
        let mut plain = yaml("providers: { a: { vendor: '…' } }");
        assert_eq!(restore(&mut plain, Some(&yaml(FULL))), Ok(0));
    }

    fn keys(doc: &Value) -> Vec<String> {
        let list = doc["client_keys"].as_sequence().expect("list").iter();
        list.map(|k| {
            let key = k.get("key").unwrap_or(k);
            key.as_str().expect("string").to_owned()
        })
        .collect()
    }

    const KEYS: &str = "client_keys: [key-a, { name: ops, key: key-b }, key-c]";

    #[test]
    fn a_deleted_reordered_or_inserted_client_key_keeps_its_own_secret() {
        let shown = redact(yaml(KEYS));
        let names: Vec<_> = shown["client_keys"]
            .as_sequence()
            .expect("list")
            .iter()
            .map(|k| k["name"].as_str().expect("named").to_owned())
            .collect();
        assert_eq!(names, ["key-1", "ops", "key-3"]);
        let edit = |f: &dyn Fn(&mut Vec<Value>)| {
            let mut edited = shown.clone();
            if let Some(Value::Sequence(items)) = edited.get_mut("client_keys") {
                f(items);
            }
            restore(&mut edited, Some(&yaml(KEYS))).expect("restored");
            keys(&edited)
        };
        // Delete the first: the others keep theirs.
        assert_eq!(edit(&|k| drop(k.remove(0))), ["key-b", "key-c"]);
        // Delete the middle.
        assert_eq!(edit(&|k| drop(k.remove(1))), ["key-a", "key-c"]);
        // Reorder.
        assert_eq!(edit(&|k| k.reverse()), ["key-c", "key-b", "key-a"]);
        // Insert one with its own value before the others.
        let new = yaml("{ name: new, key: key-new }");
        assert_eq!(
            edit(&|k| k.insert(0, new.clone())),
            ["key-new", "key-a", "key-b", "key-c"]
        );
    }

    #[test]
    fn a_client_key_placeholder_without_a_known_name_is_refused() {
        let mut ghost = yaml("client_keys: [{ name: ghost, key: '…' }]");
        let e = restore(&mut ghost, Some(&yaml(KEYS))).expect_err("refused");
        assert!(e.contains("ghost"), "{e}");
        let mut bare = yaml("client_keys: ['…']");
        assert!(restore(&mut bare, Some(&yaml(KEYS))).is_err());
    }

    #[test]
    fn a_list_that_changed_length_around_a_placeholder_is_refused() {
        let current = yaml("mcp: { m: { command: [npx, a, b] } }");
        let mut edited = redact(current.clone());
        edited["mcp"]["m"]["command"] = yaml("[npx, '…']");
        let e = restore(&mut edited, Some(&current)).expect_err("refused");
        assert!(e.contains("changed length"), "{e}");
        let mut same = redact(current.clone());
        same["mcp"]["m"]["command"][1] = "x".into();
        assert_eq!(restore(&mut same, Some(&current)), Ok(1));
    }

    #[test]
    fn a_save_that_cannot_rename_writes_in_place_after_the_backup() {
        let dir = std::env::temp_dir().join(format!("respan-edit-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("dir");
        let file = dir.join("gateway.yaml");
        std::fs::write(&file, "old: 1\n").expect("write");
        let busy = |_: &Path, _: &Path| Err(std::io::ErrorKind::ResourceBusy.into());
        let backup = write_with(&file, "new: 2\n", busy).expect("written");
        assert_eq!(std::fs::read_to_string(&file).expect("read"), "new: 2\n");
        let backup = backup.expect("backup");
        assert_eq!(std::fs::read_to_string(backup).expect("read"), "old: 1\n");
        let other = |_: &Path, _: &Path| Err(std::io::ErrorKind::PermissionDenied.into());
        assert!(write_with(&file, "x: 3\n", other).is_err());
        assert_eq!(std::fs::read_to_string(&file).expect("read"), "new: 2\n");
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn finds_the_place_of_an_error() {
        assert_eq!(location("providers: bad at line 3 column 5"), Some((3, 5)));
        assert_eq!(location("nothing"), None);
    }
}
