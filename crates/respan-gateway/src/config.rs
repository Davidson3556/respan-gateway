//! The YAML config: providers, and HTTP routes that relay, translate, or pick
//! a provider by model.

use std::collections::BTreeMap;
use std::sync::Arc;

use respan_gateway_core::http::HeaderMap;
use respan_gateway_core::http::header::{AUTHORIZATION, HeaderName, HeaderValue};
use respan_gateway_core::http::uri::PathAndQuery;
use respan_gateway_core::{Fault, Format, Gateway, Provider, Route, Transport};
use respan_gateway_plugins::ModelRouter;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub listen: Option<std::net::SocketAddr>,
    #[serde(default)]
    pub providers: BTreeMap<String, ProviderConfig>,
    /// Keyed by the path each is served at, which also names the route.
    pub routes: BTreeMap<String, RouteConfig>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderConfig {
    pub format: String,
    pub base_url: String,
    pub auth: Option<Auth>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum Auth {
    /// `Authorization: Bearer <key>`
    Bearer { key: String },
    /// `<name>: <key>`
    Header { name: String, key: String },
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RouteConfig {
    pub format: String,
    /// The provider for requests no `models` prefix matches.
    pub target: Option<String>,
    /// Model prefix to provider, tried in order.
    #[serde(default)]
    pub models: Vec<ModelRoute>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelRoute {
    pub prefix: String,
    pub provider: String,
}

impl Config {
    pub fn parse(yaml: &str) -> Result<Self, Fault> {
        serde_norway::from_str(yaml).map_err(|e| Fault::config(e.to_string()))
    }

    /// A gateway from this config, reaching providers through `transport`,
    /// with formats looked up in `formats`.
    pub fn build(
        &self,
        transport: impl Transport,
        formats: &[Arc<dyn Format>],
    ) -> Result<Gateway, Fault> {
        let format = |name: &str, owner: String| {
            formats
                .iter()
                .find(|f| f.name() == name)
                .cloned()
                .ok_or_else(|| {
                    let known: Vec<_> = formats.iter().map(|f| f.name()).collect();
                    Fault::config(format!(
                        "{owner}: unknown format '{name}'; compiled-in formats: [{}]",
                        known.join(", ")
                    ))
                })
        };
        let mut providers = Vec::new();
        for (name, p) in &self.providers {
            let format = format(&p.format, format!("provider '{name}'"))?;
            let auth = auth_headers(name, p.auth.as_ref())?;
            let provider = Provider::new(name.clone(), format, p.base_url.clone());
            providers.push(provider.with_headers(auth));
        }
        let mut routes = Vec::new();
        for (path, r) in &self.routes {
            // Matched exactly, so it must be a bare request path.
            let whole = path.parse::<PathAndQuery>();
            if !whole.is_ok_and(|p| p.query().is_none() && p.as_str() == path) {
                return Err(Fault::config(format!(
                    "route '{path}': not a path, such as '/v1/chat/completions'"
                )));
            }
            let mut route = Route::new(path.clone(), format(&r.format, format!("route '{path}'"))?);
            if !r.models.is_empty() {
                let router = r.models.iter().fold(ModelRouter::new(), |router, m| {
                    router.route(m.prefix.clone(), m.provider.clone())
                });
                route = route.with_plugin(router);
            }
            if let Some(t) = &r.target {
                route = route.with_target(t.clone());
            }
            routes.push(route);
        }
        let gateway = Gateway::new(transport, providers, routes)?;
        // An unreachable model route fails here, not per request.
        for (path, r) in &self.routes {
            for m in &r.models {
                gateway.check_target(path, m.provider.as_str())?;
            }
        }
        Ok(gateway)
    }
}

/// `env:NAME` reads the environment variable; anything else is literal.
fn secret(provider: &str, value: &str) -> Result<String, Fault> {
    match value.strip_prefix("env:") {
        Some(var) => std::env::var(var).map_err(|_| {
            Fault::config(format!(
                "provider '{provider}': environment variable {var} is not set"
            ))
        }),
        None => Ok(value.to_owned()),
    }
}

fn auth_headers(provider: &str, auth: Option<&Auth>) -> Result<HeaderMap, Fault> {
    let bad = |e: String| Fault::config(format!("provider '{provider}': {e}"));
    let (name, value) = match auth {
        None => return Ok(HeaderMap::new()),
        Some(Auth::Bearer { key }) => (AUTHORIZATION, format!("Bearer {}", secret(provider, key)?)),
        Some(Auth::Header { name, key }) => (
            HeaderName::from_bytes(name.as_bytes()).map_err(|e| bad(e.to_string()))?,
            secret(provider, key)?,
        ),
    };
    let mut value = HeaderValue::from_str(&value).map_err(|e| bad(e.to_string()))?;
    value.set_sensitive(true);
    let mut headers = HeaderMap::new();
    headers.insert(name, value);
    Ok(headers)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn resolve(yaml: &str) -> Result<Gateway, String> {
        let transport = respan_gateway_testkit::MockTransport::new();
        Config::parse(yaml)
            .and_then(|c| c.build(transport, &crate::formats()))
            .map_err(|e| e.message().to_owned())
    }

    #[tokio::test]
    async fn the_first_listed_prefix_a_model_matches_wins() {
        use respan_gateway_core::http::StatusCode;
        use respan_gateway_testkit::{MockTransport, mock, post};

        let yaml = "providers:
  a: { format: openai-chat, base_url: 'https://a.test' }
  b: { format: openai-chat, base_url: 'https://b.test' }
routes:
  /chat:
    format: openai-chat
    models: [{ prefix: claude, provider: a }, { prefix: claude-3, provider: b }]
";
        let mock = MockTransport::new();
        let config = Config::parse(yaml).unwrap_or_else(|e| panic!("{e}"));
        let gateway = config
            .build(mock.clone(), &crate::formats())
            .unwrap_or_else(|e| panic!("{e}"));
        mock.reply("https://a.test", mock::json(StatusCode::OK, "{}"));

        let request = r#"{"model":"claude-3-x","messages":[]}"#;
        drop(gateway.handle("/chat", post(request)).await);

        let sent = mock.sent();
        let urls: Vec<String> = sent.iter().map(|s| s.uri().to_string()).collect();
        assert_eq!(urls, ["https://a.test/v1/chat/completions"]);
    }

    #[tokio::test]
    async fn the_example_config_routes_by_model_both_ways() {
        use respan_gateway_testkit::{MockTransport, collect, mock, post};

        let yaml = include_str!("../../../gateway.example.yaml")
            .replace("env:OPENAI_API_KEY", "sk")
            .replace("env:ANTHROPIC_API_KEY", "sk");
        let config = Config::parse(&yaml).unwrap_or_else(|e| panic!("{e}"));
        let mock = MockTransport::new();
        let gateway = config
            .build(mock.clone(), &crate::formats())
            .unwrap_or_else(|e| panic!("gateway: {e}"));
        let claude = r#"{"id":"msg_1","type":"message","role":"assistant","model":"claude-x","content":[{"type":"text","text":"hi"}],"stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":1}}"#;
        let gpt = r#"{"id":"c1","object":"chat.completion","model":"gpt-x","choices":[{"index":0,"message":{"role":"assistant","content":"hi"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1}}"#;
        mock.reply(
            "https://api.anthropic.com",
            mock::json(respan_gateway_core::http::StatusCode::OK, claude),
        );
        mock.reply(
            "https://api.openai.com",
            mock::json(respan_gateway_core::http::StatusCode::OK, gpt),
        );

        let openai_client = r#"{"model":"claude-x","messages":[{"role":"user","content":"hi"}]}"#;
        let anthropic_client =
            r#"{"model":"gpt-x","max_tokens":10,"messages":[{"role":"user","content":"hi"}]}"#;
        let (_, _, to_claude) = collect(
            gateway
                .handle("/v1/chat/completions", post(openai_client))
                .await,
        )
        .await;
        let (_, _, to_gpt) =
            collect(gateway.handle("/v1/messages", post(anthropic_client)).await).await;

        let urls: Vec<String> = mock
            .sent()
            .into_iter()
            .map(|s| s.uri().to_string())
            .collect();
        assert_eq!(
            urls,
            [
                "https://api.anthropic.com/v1/messages",
                "https://api.openai.com/v1/chat/completions"
            ]
        );
        let json = |b: Result<respan_gateway_core::bytes::Bytes, respan_gateway_core::Fault>| -> respan_gateway_core::serde_json::Value {
            b.ok()
                .and_then(|b| respan_gateway_core::serde_json::from_slice(&b).ok())
                .unwrap_or_default()
        };
        assert_eq!(json(to_claude)["object"], "chat.completion");
        assert_eq!(json(to_gpt)["type"], "message");
    }

    #[test]
    fn bearer_auth_is_a_sensitive_header() {
        let auth = Auth::Bearer { key: "sk".into() };
        let headers = auth_headers("p", Some(&auth)).unwrap_or_default();
        let value = headers.get(AUTHORIZATION);
        assert_eq!(value.and_then(|v| v.to_str().ok()), Some("Bearer sk"));
        assert!(value.is_some_and(HeaderValue::is_sensitive));
    }

    #[test]
    fn a_missing_environment_variable_is_an_error() {
        let yaml = "providers: { p: { format: openai-chat, base_url: 'http://x', auth: { type: bearer, key: 'env:RESPAN_TEST_UNSET' } } }\nroutes: {}";
        assert_eq!(
            resolve(yaml).err().as_deref(),
            Some("provider 'p': environment variable RESPAN_TEST_UNSET is not set")
        );
    }

    #[test]
    fn a_model_route_to_an_unknown_provider_is_an_error() {
        let yaml =
            "routes: { /x: { format: openai-chat, models: [{ prefix: c, provider: nope }] } }";
        assert_eq!(
            resolve(yaml).err().as_deref(),
            Some("route '/x': unknown provider 'nope'")
        );
    }

    #[test]
    fn a_listen_address_is_a_socket_address() {
        let bad = Config::parse("listen: 127.0.0.1\nroutes: {}").err();
        assert!(bad.is_some_and(|e| e.message().contains("listen")));
        let good = Config::parse("listen: 127.0.0.1:8080\nroutes: {}");
        assert!(good.is_ok_and(|c| c.listen.is_some_and(|a| a.port() == 8080)));
    }

    #[test]
    fn a_route_is_served_at_a_path() {
        for path in ["x", "/x?y=1", "/x y", "/x#y"] {
            let yaml = format!("routes: {{ '{path}': {{ format: openai-chat, target: p }} }}");
            assert_eq!(
                resolve(&yaml).err(),
                Some(format!(
                    "route '{path}': not a path, such as '/v1/chat/completions'"
                ))
            );
        }
    }

    #[test]
    fn an_unknown_format_names_the_compiled_in_ones() {
        let yaml = "routes: { /x: { format: nope, target: p } }";
        assert_eq!(
            resolve(yaml).err().as_deref(),
            Some(
                "route '/x': unknown format 'nope'; compiled-in formats: [openai-chat, anthropic-messages]"
            )
        );
    }
}
