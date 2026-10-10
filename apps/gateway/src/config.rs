//! The YAML config: providers, the formats served, plugins in order, and
//! the default target.

use std::any::TypeId;
use std::collections::BTreeMap;

use respan_gateway_http::http::HeaderMap;
use respan_gateway_http::http::header::{AUTHORIZATION, HeaderName, HeaderValue};

use respan_gateway_conversation::{Input, Output};
use respan_gateway_core::{
    ClientContext, Extensions, Fault, Format, Gateway, Plugin, Response, async_trait,
};
use respan_gateway_http::{Incoming, Outgoing};
use respan_gateway_plugins::ModelRouter;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub listen: Option<std::net::SocketAddr>,
    /// The most bytes of a client's request, and of a provider's answer
    /// held at once, past which the request fails 413 or the answer 502;
    /// 32 MiB if unset.
    pub body_limit: Option<usize>,
    #[serde(default)]
    pub providers: BTreeMap<String, ProviderConfig>,
    /// The formats served to clients, by name, tried in order. Serve
    /// `passthrough` last to relay every other request unread to the
    /// provider of format `passthrough`, whose `base_url` is then the
    /// vendor's origin, as clients' paths are sent as they are. Clients of
    /// a format no other translates, `passthrough` or a session's, go to
    /// the one provider of that format.
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

/// A provider's name, base URL and auth headers, added to a gateway.
type AddProvider = dyn Fn(Gateway, String, &str, HeaderMap) -> Result<Gateway, Fault>;

/// A format compiled into the binary: its name, and how to add a
/// provider that speaks it or serve its clients.
pub struct KnownFormat {
    pub name: String,
    kind: (TypeId, TypeId),
    provider: Box<AddProvider>,
    serve: Box<dyn Fn(Gateway) -> Result<Gateway, Fault>>,
}

impl KnownFormat {
    /// `format`, its providers reached by `reach`, given one's base URL
    /// and auth headers.
    pub fn of<F, I, O, T>(
        format: F,
        reach: impl Fn(&str, HeaderMap) -> Result<T, Fault> + 'static,
    ) -> Self
    where
        F: Clone + Send + Sync + 'static + Format<Incoming, I, O> + Format<Outgoing, O, I>,
        I: Clone + Send + Sync + 'static,
        O: Clone + Send + Sync + 'static,
        T: Fn(Extensions) -> Outgoing + Send + Sync + 'static,
    {
        let client = format.clone();
        Self {
            name: <F as Format<Incoming, I, O>>::name(&format).to_owned(),
            kind: (TypeId::of::<I>(), TypeId::of::<O>()),
            provider: Box::new(move |gateway, name, base_url, auth| {
                gateway.provider::<I, O, _>(name, format.clone(), reach(base_url, auth)?)
            }),
            serve: Box::new(move |gateway| gateway.client::<I, O, _>(client.clone())),
        }
    }

    /// Whether its kind is `I` in and `O` out.
    pub fn is<I: 'static, O: 'static>(&self) -> bool {
        self.kind == (TypeId::of::<I>(), TypeId::of::<O>())
    }
}

impl Config {
    pub fn parse(yaml: &str) -> Result<Self, Fault> {
        serde_norway::from_str(yaml).map_err(|e| Fault::config(e.to_string()))
    }

    /// Builds the gateway of the formats in `formats`.
    pub fn build(&self, formats: &[KnownFormat]) -> Result<Gateway, Fault> {
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
            gateway = (format.provider)(gateway, name.clone(), &p.base_url, auth)
                .map_err(|e| Fault::config(format!("provider '{name}': {}", e.message())))?;
        }
        let after = (self.serve.iter().skip_while(|n| *n != PASSTHROUGH)).nth(1);
        if let Some(after) = after {
            return Err(Fault::config(format!(
                "serve: '{after}' comes after '{PASSTHROUGH}', which accepts every request first; \
                 serve '{PASSTHROUGH}' last"
            )));
        }
        // Only the conversation kind translates: a client of another kind
        // goes to the one provider of its own format.
        let mut own = BTreeMap::new();
        for name in &self.serve {
            let served = format(name, "serve")?;
            gateway = (served.serve)(gateway)?;
            if served.is::<Input, Output>() {
                continue;
            }
            let mut providers = self.providers.iter().filter(|(_, p)| p.format == *name);
            let (Some((provider, _)), None) = (providers.next(), providers.next()) else {
                let message = format!("serve: {name} needs exactly one provider of that format");
                return Err(Fault::config(message));
            };
            own.insert(name.clone(), provider.clone());
        }
        for p in ordered(&self.plugins)? {
            let bad = |e: Fault| Fault::config(format!("plugin '{}': {}", p.name, e.message()));
            let Some(models) = &p.models else {
                return Err(bad(Fault::config(
                    "no plugin given; compiled-in plugins: [models]",
                )));
            };
            for m in models {
                let Some(provider) = self.providers.get(&m.provider) else {
                    return Err(bad(Fault::config(format!(
                        "unknown provider '{}'",
                        m.provider
                    ))));
                };
                let format = format(&provider.format, &format!("provider '{}'", m.provider))?;
                if !format.is::<Input, Output>() {
                    return Err(bad(Fault::config(format!(
                        "provider '{}' uses format '{}', incompatible with conversation models routes",
                        m.provider, provider.format
                    ))));
                }
            }
            let router = ModelRouter::new(|c: &Input| c.model.as_deref());
            let router = models.iter().fold(router, |router, m| {
                router.route(m.prefix.clone(), m.provider.clone())
            });
            gateway = gateway.plugin(router);
        }
        if !own.is_empty() {
            gateway = gateway.plugin(SendTo(own));
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

/// Sends the clients of each format in `.0` to its provider there: a
/// default target serves one kind, and these are of another.
struct SendTo(BTreeMap<String, String>);

#[async_trait]
impl Plugin for SendTo {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        if let Some(provider) = self.0.get(cx.format()) {
            cx.set_target(provider.as_str());
        }
        Ok(None)
    }
}

/// The plugins in the order they run: next is the earliest listed plugin
/// whose `after` plugins are all placed. `Err` for an unknown name or a
/// cycle.
fn ordered(plugins: &[PluginConfig]) -> Result<Vec<&PluginConfig>, Fault> {
    let index = |name: &str| plugins.iter().position(|p| p.name == name);
    // `after` names a plugin, so a name is one plugin's.
    let mut each = plugins.iter().enumerate();
    if let Some((_, p)) = each.find(|(i, p)| index(&p.name) != Some(*i)) {
        return Err(Fault::config(format!(
            "plugin '{}' is registered twice",
            p.name
        )));
    }
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
#[allow(dead_code, unused_imports)]
#[path = "../../../tests/support/http.rs"]
mod http_helpers;

#[cfg(test)]
mod tests {
    use super::http_helpers::{post, read_whole, serve};
    use mockllm::{Answer, FakeAnthropic, FakeOpenAi};
    use respan_gateway_http::http::StatusCode;

    use super::*;

    /// A gateway from `yaml` reaching its providers over HTTP.
    fn resolve(yaml: &str) -> Result<Gateway, String> {
        Config::parse(yaml)
            .and_then(|c| {
                let limit = c.body_limit.unwrap_or(crate::BODY_LIMIT);
                c.build(&crate::formats(limit))
            })
            .map_err(|e| e.message().to_owned())
    }

    /// Build the actual HTTP configuration against two strict local providers.
    async fn local(yaml: &str) -> (Gateway, FakeOpenAi, FakeOpenAi) {
        let a = FakeOpenAi::start().await;
        let b = FakeOpenAi::start().await;
        a.answer(Answer::text("a"));
        b.answer(Answer::text("b"));
        let yaml = yaml
            .replace("https://a.test/v1", &a.base_url())
            .replace("https://b.test/v1", &b.base_url());
        let gateway = resolve(&yaml).unwrap_or_else(|e| panic!("gateway: {e}"));
        (gateway, a, b)
    }

    const TWO: &str = "providers:
  a: { format: openai-chat, base_url: 'https://a.test/v1', auth: { type: bearer, key: sk-test } }
  b: { format: openai-chat, base_url: 'https://b.test/v1', auth: { type: bearer, key: sk-test } }
serve: [openai-chat]
";

    async fn request(gateway: &Gateway, model: &str) {
        let body = serde_json::json!({
            "model": model,
            "messages": [{"role": "user", "content": "hi"}]
        });
        let (response, _) = serve(gateway, post("/v1/chat/completions", body.to_string())).await;
        let (status, _, body) = read_whole(response).await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.is_ok(), "{body:?}");
        gateway.idle().await;
    }

    #[tokio::test]
    async fn the_first_listed_prefix_a_model_matches_wins() {
        let yaml = format!(
            "{TWO}plugins:
  - name: models
    models: [{{ prefix: claude, provider: a }}, {{ prefix: claude-3, provider: b }}]
"
        );
        let (gateway, a, b) = local(&yaml).await;
        request(&gateway, "claude-3-x").await;
        assert_eq!(a.requests().len(), 1);
        assert_eq!(a.requests()[0].path, "/v1/chat/completions");
        assert!(b.requests().is_empty());
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
        let (gateway, a, b) = local(&yaml).await;
        request(&gateway, "gpt-x").await;
        assert_eq!(a.requests().len(), 1);
        assert_eq!(a.requests()[0].path, "/v1/chat/completions");
        assert!(b.requests().is_empty());
    }

    #[tokio::test]
    async fn the_default_target_answers_what_no_router_targets() {
        let yaml = format!(
            "{TWO}target: b
plugins: [{{ name: models, models: [{{ prefix: claude, provider: a }}] }}]
"
        );
        let (gateway, a, b) = local(&yaml).await;
        request(&gateway, "gpt-x").await;
        assert!(a.requests().is_empty());
        assert_eq!(b.requests().len(), 1);
        assert_eq!(b.requests()[0].path, "/v1/chat/completions");
    }

    #[tokio::test]
    async fn the_example_config_routes_by_model_both_ways() {
        let openai = FakeOpenAi::start().await;
        let anthropic = FakeAnthropic::start().await;
        openai.answer(Answer::text("hi"));
        anthropic.answer(Answer::text("hi"));
        let yaml = include_str!("../../../gateway.example.yaml")
            .replace("env:OPENAI_API_KEY", "sk")
            .replace("env:ANTHROPIC_API_KEY", "sk")
            .replace("https://api.openai.com/v1", &openai.base_url())
            .replace("https://api.anthropic.com", &anthropic.base_url());
        let gateway = resolve(&yaml).unwrap_or_else(|e| panic!("gateway: {e}"));

        let openai_client = r#"{"model":"claude-x","messages":[{"role":"user","content":"hi"}]}"#;
        let anthropic_client =
            r#"{"model":"gpt-x","max_tokens":10,"messages":[{"role":"user","content":"hi"}]}"#;
        let to_claude = serve(&gateway, post("/v1/chat/completions", openai_client));
        let (_, _, to_claude) = read_whole(to_claude.await.0).await;
        let to_gpt = serve(&gateway, post("/v1/messages", anthropic_client));
        let (_, _, to_gpt) = read_whole(to_gpt.await.0).await;

        gateway.idle().await;
        assert_eq!(anthropic.requests().len(), 1);
        assert_eq!(anthropic.requests()[0].path, "/v1/messages");
        assert_eq!(openai.requests().len(), 1);
        assert_eq!(openai.requests()[0].path, "/v1/chat/completions");
        let json = |b: Result<respan_gateway_http::bytes::Bytes, Fault>| {
            b.ok()
                .and_then(|b| serde_json::from_slice(&b).ok())
                .unwrap_or(serde_json::Value::Null)
        };
        let to_claude = json(to_claude);
        let to_gpt = json(to_gpt);
        assert_eq!(to_claude["object"], "chat.completion", "{to_claude}");
        assert_eq!(to_gpt["type"], "message", "{to_gpt}");
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
    fn a_session_provider_is_websocket_and_its_clients_go_to_it_alone() {
        let realtime = "realtime: { format: openai-realtime, base_url: 'wss://api.openai.com/v1' }";
        let cases = [
            (
                "providers: { p: { format: gemini-live, base_url: 'https://x.test' } }\nserve: []"
                    .to_owned(),
                "provider 'p': bad base URL: ws or wss with a host, and no query, fragment or credentials (those go in its headers)",
            ),
            (
                "serve: [openai-realtime]".to_owned(),
                "serve: openai-realtime needs exactly one provider of that format",
            ),
            (
                format!(
                    "providers: {{ {realtime} }}\nserve: [openai-realtime]
plugins: [{{ name: models, models: [{{ prefix: gpt, provider: realtime }}] }}]"
                ),
                "plugin 'models': provider 'realtime' uses format 'openai-realtime', incompatible with conversation models routes",
            ),
        ];
        for (yaml, expected) in cases {
            assert_eq!(resolve(&yaml).err().as_deref(), Some(expected), "{yaml}");
        }
        let both = format!("providers: {{ {realtime} }}\nserve: [openai-realtime]");
        assert!(resolve(&both).is_ok());
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
                "serve: unknown format 'nope'; compiled-in formats: [openai-chat, groq-chat, deepseek-chat, vllm-chat, openrouter-chat, azure-chat, anthropic-messages, passthrough, openai-realtime, responses-ws, gemini-live]"
            )
        );
    }
}
