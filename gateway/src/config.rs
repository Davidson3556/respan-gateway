//! The YAML config: providers, the formats served, plugins in order, and
//! the default target.

use std::collections::BTreeMap;

use respan_gateway_http::http::HeaderMap;
use respan_gateway_http::http::header::{AUTHORIZATION, HeaderName, HeaderValue};

use respan_gateway_core::{ClientContext, Fault, Format, Gateway, Plugin, Response, async_trait};
use respan_gateway_http::{Codec, Transport};
use respan_gateway_messages::conversation::Conversation;
use respan_gateway_plugins::ModelRouter;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub listen: Option<std::net::SocketAddr>,
    #[serde(default)]
    pub providers: BTreeMap<String, ProviderConfig>,
    /// The formats served to clients, by name, tried in order. Serve
    /// `passthrough` last to relay every other request unread to the
    /// provider of format `passthrough`, whose `base_url` is then the
    /// vendor's origin, as clients' paths are sent as they are.
    pub serve: Vec<String>,
    /// Compiled-in plugins, in the order listed unless `after` says otherwise.
    #[serde(default)]
    pub plugins: Vec<PluginConfig>,
    /// The provider that answers when no plugin sets one.
    pub target: Option<String>,
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

/// One plugin: its name, the plugins it runs after, and which plugin it
/// is, by the one field that configures it.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PluginConfig {
    pub name: String,
    #[serde(default)]
    pub after: Vec<String>,
    /// A [`ModelRouter`]: model prefix to provider, tried in order.
    pub models: Option<Vec<ModelRoute>>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelRoute {
    pub prefix: String,
    pub provider: String,
}

/// A format compiled into the binary: its name, and how to add a
/// provider that speaks it or serve its clients.
pub struct KnownFormat {
    pub name: String,
    pub provider: fn(Gateway, name: String, transport: Transport) -> Result<Gateway, Fault>,
    pub serve: fn(Gateway) -> Result<Gateway, Fault>,
}

impl KnownFormat {
    /// An HTTP format, as every provider the binary knows speaks.
    pub fn of<F>() -> Self
    where
        F: Codec
            + Default
            + Format<Transport, F::Input, F::Output>
            + Format<Transport, F::Output, F::Input>,
    {
        Self {
            name: Codec::name(&F::default()).to_owned(),
            provider: |gateway, name, transport| {
                gateway.provider::<F::Input, F::Output, _>(
                    name,
                    F::default(),
                    respan_gateway_http::connect(transport),
                )
            },
            serve: |gateway| gateway.client::<F::Input, F::Output, _>(F::default()),
        }
    }
}

impl Config {
    pub fn parse(yaml: &str) -> Result<Self, Fault> {
        serde_norway::from_str(yaml).map_err(|e| Fault::config(e.to_string()))
    }

    /// Builds the gateway: each provider's transport comes from `transport`,
    /// given its base URL and auth headers; formats are found in `formats`.
    pub fn build<T: Into<Transport>>(
        &self,
        transport: impl Fn(&str, HeaderMap) -> Result<T, Fault>,
        formats: &[KnownFormat],
    ) -> Result<Gateway, Fault> {
        let format = |name: &str, owner: &str| {
            formats.iter().find(|f| f.name == name).ok_or_else(|| {
                let known: Vec<_> = formats.iter().map(|f| f.name.as_str()).collect();
                Fault::config(format!(
                    "{owner}: unknown format '{name}'; compiled-in formats: [{}]",
                    known.join(", ")
                ))
            })
        };
        let mut gateway = Gateway::new();
        for (name, p) in &self.providers {
            let format = format(&p.format, &format!("provider '{name}'"))?;
            let auth = auth_headers(name, p.auth.as_ref())?;
            let reach = transport(&p.base_url, auth)
                .map_err(|e| Fault::config(format!("provider '{name}': {}", e.message())))?;
            gateway = (format.provider)(gateway, name.clone(), reach.into())?;
        }
        let after = (self.serve.iter().skip_while(|n| *n != PASSTHROUGH)).nth(1);
        if let Some(after) = after {
            return Err(Fault::config(format!(
                "serve: '{after}' comes after '{PASSTHROUGH}', which accepts every request first; \
                 serve '{PASSTHROUGH}' last"
            )));
        }
        for name in &self.serve {
            gateway = (format(name, "serve")?.serve)(gateway)?;
        }
        for p in ordered(&self.plugins)? {
            let bad = |e: Fault| Fault::config(format!("plugin '{}': {}", p.name, e.message()));
            let Some(models) = &p.models else {
                return Err(bad(Fault::config(
                    "no plugin given; compiled-in plugins: [models]",
                )));
            };
            // An unknown provider in a route fails here, not per request.
            if let Some(m) = models
                .iter()
                .find(|m| !self.providers.contains_key(&m.provider))
            {
                let unknown = format!("unknown provider '{}'", m.provider);
                return Err(bad(Fault::config(unknown)));
            }
            let router = ModelRouter::new(|c: &Conversation| c.model.as_deref());
            let router = models.iter().fold(router, |router, m| {
                router.route(m.prefix.clone(), m.provider.clone())
            });
            gateway = gateway.plugin(p.name.clone(), router)?;
        }
        if self.serve.iter().any(|name| name == PASSTHROUGH) {
            let mut own = self
                .providers
                .iter()
                .filter(|(_, p)| p.format == PASSTHROUGH);
            let (Some((name, _)), None) = (own.next(), own.next()) else {
                let message =
                    format!("serve: {PASSTHROUGH} needs exactly one provider of that format");
                return Err(Fault::config(message));
            };
            gateway = gateway.plugin(PASSTHROUGH, SendTo(name.clone()))?;
        }
        match &self.target {
            Some(target) => gateway
                .target(target.as_str())
                .map_err(|e| Fault::config(format!("target: {}", e.message()))),
            None => Ok(gateway),
        }
    }
}

/// The name of the format that relays what no other served format accepts.
const PASSTHROUGH: &str = "passthrough";

/// Sends the requests of format `passthrough` to the provider `.0`: a
/// default target serves one kind, and these are of another.
struct SendTo(String);

#[async_trait]
impl Plugin for SendTo {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        if cx.format() == PASSTHROUGH {
            cx.set_target(self.0.as_str());
        }
        Ok(None)
    }
}

/// The plugins in the order they run: next is the earliest listed plugin
/// whose `after` plugins are all placed. `Err` for an unknown name or a
/// cycle.
fn ordered(plugins: &[PluginConfig]) -> Result<Vec<&PluginConfig>, Fault> {
    let index = |name: &str| plugins.iter().position(|p| p.name == name);
    for p in plugins {
        if let Some(first) = p.after.iter().find(|a| index(a).is_none()) {
            return Err(Fault::config(format!(
                "plugin '{}' runs after unknown plugin '{first}'",
                p.name
            )));
        }
    }
    let mut placed = vec![false; plugins.len()];
    let mut order = Vec::with_capacity(plugins.len());
    while order.len() < plugins.len() {
        let ready = (0..plugins.len()).find(|&i| {
            let after = |a: &String| index(a).is_some_and(|a| placed[a]);
            !placed[i] && plugins[i].after.iter().all(after)
        });
        let Some(i) = ready else {
            let stuck: Vec<_> = (0..plugins.len())
                .filter(|&i| !placed[i])
                .map(|i| format!("'{}'", plugins[i].name))
                .collect();
            return Err(Fault::config(format!(
                "plugins run after each other in a cycle: {}",
                stuck.join(", ")
            )));
        };
        placed[i] = true;
        order.push(&plugins[i]);
    }
    Ok(order)
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
    use respan_gateway_http::http::StatusCode;
    use respan_gateway_testkit::{MockTransport, mock, post, read_whole, serve};

    use super::*;

    /// A gateway from `yaml` reaching its providers over HTTP.
    fn resolve(yaml: &str) -> Result<Gateway, String> {
        Config::parse(yaml)
            .and_then(|c| c.build(crate::http, &crate::formats()))
            .map_err(|e| e.message().to_owned())
    }

    /// A gateway from `yaml` whose providers answer from `mock`.
    fn mocked(yaml: &str, mock: &MockTransport) -> Gateway {
        let config = Config::parse(yaml).unwrap_or_else(|e| panic!("{e}"));
        let transport = |url: &str, auth| Ok(mock.at(url).with_headers(auth));
        config
            .build(transport, &crate::formats())
            .unwrap_or_else(|e| panic!("gateway: {e}"))
    }

    const TWO: &str = "providers:
  a: { format: openai-chat, base_url: 'https://a.test/v1' }
  b: { format: openai-chat, base_url: 'https://b.test/v1' }
serve: [openai-chat]
";

    /// The URLs `gateway` called for an OpenAI Chat `request`.
    async fn sent_to(gateway: &Gateway, mock: &MockTransport, request: &str) -> Vec<String> {
        let req = post("/v1/chat/completions", request.to_owned());
        drop(serve(gateway, req).await);
        mock.sent().iter().map(|s| s.uri().to_string()).collect()
    }

    #[tokio::test]
    async fn the_first_listed_prefix_a_model_matches_wins() {
        let yaml = format!(
            "{TWO}plugins:
  - name: models
    models: [{{ prefix: claude, provider: a }}, {{ prefix: claude-3, provider: b }}]
"
        );
        let mock = MockTransport::new();
        let gateway = mocked(&yaml, &mock);
        mock.reply("https://a.test/v1", mock::json(StatusCode::OK, "{}"));

        let urls = sent_to(&gateway, &mock, r#"{"model":"claude-3-x","messages":[]}"#).await;
        assert_eq!(urls, ["https://a.test/v1/chat/completions"]);
    }

    #[tokio::test]
    async fn plugins_run_in_the_order_after_says() {
        // `first` is listed first but runs after `second`, so the target it
        // sets, last, wins.
        let yaml = format!(
            "{TWO}plugins:
  - {{ name: first, after: [second], models: [{{ prefix: gpt, provider: a }}] }}
  - {{ name: second, models: [{{ prefix: gpt, provider: b }}] }}
"
        );
        let mock = MockTransport::new();
        let gateway = mocked(&yaml, &mock);
        mock.reply("https://a.test/v1", mock::json(StatusCode::OK, "{}"));

        let urls = sent_to(&gateway, &mock, r#"{"model":"gpt-x","messages":[]}"#).await;
        assert_eq!(urls, ["https://a.test/v1/chat/completions"]);
    }

    #[tokio::test]
    async fn the_default_target_answers_what_no_router_targets() {
        let yaml = format!(
            "{TWO}target: b
plugins: [{{ name: models, models: [{{ prefix: claude, provider: a }}] }}]
"
        );
        let mock = MockTransport::new();
        let gateway = mocked(&yaml, &mock);
        mock.reply("https://b.test/v1", mock::json(StatusCode::OK, "{}"));

        let urls = sent_to(&gateway, &mock, r#"{"model":"gpt-x","messages":[]}"#).await;
        assert_eq!(urls, ["https://b.test/v1/chat/completions"]);
    }

    #[tokio::test]
    async fn the_example_config_routes_by_model_both_ways() {
        let yaml = include_str!("../../gateway.example.yaml")
            .replace("env:OPENAI_API_KEY", "sk")
            .replace("env:ANTHROPIC_API_KEY", "sk");
        let mock = MockTransport::new();
        let gateway = mocked(&yaml, &mock);
        let claude = r#"{"id":"msg_1","type":"message","role":"assistant","model":"claude-x","content":[{"type":"text","text":"hi"}],"stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":1}}"#;
        let gpt = r#"{"id":"c1","object":"chat.completion","model":"gpt-x","choices":[{"index":0,"message":{"role":"assistant","content":"hi"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1}}"#;
        mock.reply(
            "https://api.anthropic.com",
            mock::json(StatusCode::OK, claude),
        );
        mock.reply("https://api.openai.com/v1", mock::json(StatusCode::OK, gpt));

        let openai_client = r#"{"model":"claude-x","messages":[{"role":"user","content":"hi"}]}"#;
        let anthropic_client =
            r#"{"model":"gpt-x","max_tokens":10,"messages":[{"role":"user","content":"hi"}]}"#;
        let to_claude = serve(&gateway, post("/v1/chat/completions", openai_client));
        let (_, _, to_claude) = read_whole(to_claude.await.0).await;
        let to_gpt = serve(&gateway, post("/v1/messages", anthropic_client));
        let (_, _, to_gpt) = read_whole(to_gpt.await.0).await;

        let urls: Vec<String> = mock.sent().iter().map(|s| s.uri().to_string()).collect();
        assert_eq!(
            urls,
            [
                "https://api.anthropic.com/v1/messages",
                "https://api.openai.com/v1/chat/completions"
            ]
        );
        let json = |b: Result<respan_gateway_http::bytes::Bytes, Fault>| {
            b.ok()
                .and_then(|b| serde_json::from_slice(&b).ok())
                .unwrap_or(serde_json::Value::Null)
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
        let yaml = "providers: { p: { format: openai-chat, base_url: 'http://x', auth: { type: bearer, key: 'env:RESPAN_TEST_UNSET' } } }\nserve: []";
        assert_eq!(
            resolve(yaml).err().as_deref(),
            Some("provider 'p': environment variable RESPAN_TEST_UNSET is not set")
        );
    }

    #[test]
    fn a_bad_base_url_or_auth_header_is_an_error() {
        let provider =
            |p: &str| format!("providers: {{ p: {{ format: openai-chat, {p} }} }}\nserve: []");
        let url = provider("base_url: 'https://k:s@x.test'");
        let said = resolve(&url).err().unwrap_or_default();
        assert!(said.starts_with("provider 'p': bad base URL"), "{said}");
        assert!(!said.contains("k:s"), "{said}");
        let host =
            provider("base_url: 'https://x.test', auth: { type: header, name: host, key: k }");
        assert_eq!(
            resolve(&host).err().as_deref(),
            Some("provider 'p': header 'host' is the connection's")
        );
    }

    #[test]
    fn a_model_route_to_an_unknown_provider_is_an_error() {
        let yaml = "serve: [openai-chat]
plugins: [{ name: models, models: [{ prefix: c, provider: nope }] }]";
        assert_eq!(
            resolve(yaml).err().as_deref(),
            Some("plugin 'models': unknown provider 'nope'")
        );
    }

    /// The names of the plugins in `yaml`'s list, in the order they run.
    fn order_of(yaml: &str) -> Vec<String> {
        let config = Config::parse(&format!("serve: []\nplugins: {yaml}"));
        let config = config.unwrap_or_else(|e| panic!("{}", e.message()));
        let order = ordered(&config.plugins).unwrap_or_else(|e| panic!("{}", e.message()));
        order.iter().map(|p| p.name.clone()).collect()
    }

    #[test]
    fn after_moves_a_plugin_only_as_far_as_it_needs() {
        // Listed d, c, b, a; c after a, b after d. The earliest ready one
        // goes next, so: d, b, a, c.
        let list = "[{name: d}, {name: c, after: [a]}, {name: b, after: [d]}, {name: a}]";
        assert_eq!(order_of(list), ["d", "b", "a", "c"]);
        // Listed a, b, c with a after c: b, c, a.
        let list = "[{name: a, after: [c]}, {name: b}, {name: c}]";
        assert_eq!(order_of(list), ["b", "c", "a"]);
    }

    #[test]
    fn a_bad_plugin_order_or_target_is_an_error() {
        let router = "models: [{ prefix: c, provider: a }]";
        let cases = [
            (
                format!("plugins: [{{ name: x, after: [y], {router} }}]"),
                "plugin 'x' runs after unknown plugin 'y'",
            ),
            (
                format!(
                    "plugins: [{{ name: x, after: [y], {router} }}, {{ name: y, after: [x], {router} }}]"
                ),
                "plugins run after each other in a cycle: 'x', 'y'",
            ),
            (
                format!("plugins: [{{ name: x, after: [x], {router} }}, {{ name: z, {router} }}]"),
                "plugins run after each other in a cycle: 'x'",
            ),
            (
                format!("plugins: [{{ name: x, {router} }}, {{ name: x, {router} }}]"),
                "plugin 'x' is registered twice",
            ),
            (
                "plugins: [{ name: x }]".to_owned(),
                "plugin 'x': no plugin given; compiled-in plugins: [models]",
            ),
            ("target: nope".to_owned(), "target: unknown provider 'nope'"),
        ];
        for (yaml, expected) in cases {
            let yaml = format!("{TWO}{yaml}");
            assert_eq!(resolve(&yaml).err().as_deref(), Some(expected), "{yaml}");
        }
    }

    #[test]
    fn a_listen_address_is_a_socket_address() {
        let bad = Config::parse("listen: 127.0.0.1\nserve: []").err();
        assert!(bad.is_some_and(|e| e.message().contains("listen")));
        let good = Config::parse("listen: 127.0.0.1:8080\nserve: []");
        assert!(good.is_ok_and(|c| c.listen.is_some_and(|a| a.port() == 8080)));
    }

    #[test]
    fn a_format_served_after_passthrough_is_refused() {
        let yaml = "serve: [passthrough, openai-chat]";
        assert_eq!(
            resolve(yaml).err().as_deref(),
            Some(
                "serve: 'openai-chat' comes after 'passthrough', which accepts every request first; serve 'passthrough' last"
            )
        );
    }

    #[test]
    fn an_unknown_format_names_the_compiled_in_ones() {
        let yaml = "serve: [openai-chat, nope]";
        assert_eq!(
            resolve(yaml).err().as_deref(),
            Some(
                "serve: unknown format 'nope'; compiled-in formats: [openai-chat, anthropic-messages, passthrough]"
            )
        );
    }
}
