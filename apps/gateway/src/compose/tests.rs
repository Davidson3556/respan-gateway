use mockllm::{Answer, FakeAnthropic, FakeOpenAi};
use respan_gateway_http::http::StatusCode;

use super::http_helpers::{post, read_whole, serve};
use super::*;
use crate::App;

/// The gateway of `yaml`, its plugins included.
fn resolve(yaml: &str) -> Result<Gateway, String> {
    Config::parse(yaml)
        .and_then(App::new)
        .map(|app| app.gateway().clone())
        .map_err(|e| e.to_string())
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
    assert_eq!(status, StatusCode::OK, "{body:?}");
    assert!(body.is_ok(), "{body:?}");
    gateway.idle().await;
}

/// The model each request to `mock` asked for.
fn models_asked(mock: &FakeOpenAi) -> Vec<String> {
    let model = |r: &mockllm::Request| r.object()?.get("model")?.as_str().map(str::to_owned);
    mock.requests().iter().filter_map(model).collect()
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
async fn without_a_target_the_first_provider_of_the_kind_answers() {
    let (gateway, a, b) = local(TWO).await;
    request(&gateway, "llama").await;
    assert_eq!(a.requests().len(), 1);
    assert!(b.requests().is_empty());
}

#[tokio::test]
async fn a_provider_prefix_picks_the_provider_and_is_not_sent() {
    let (gateway, a, b) = local(TWO).await;
    request(&gateway, "b/gpt-4.1-mini").await;
    assert!(a.requests().is_empty());
    assert_eq!(models_asked(&b), ["gpt-4.1-mini"]);
}

#[tokio::test]
async fn a_preset_routes_its_models_with_its_key() {
    let openai = FakeOpenAi::start().await;
    let anthropic = FakeAnthropic::start().await;
    openai.answer(Answer::text("hi"));
    anthropic.answer(Answer::text("hi"));
    let yaml = format!(
        "providers:
  openai: {{ vendor: openai, base_url: '{}', key: sk-openai }}
  anthropic: {{ vendor: anthropic, base_url: '{}', key: sk-ant }}
",
        openai.base_url(),
        anthropic.base_url()
    );
    let gateway = resolve(&yaml).unwrap_or_else(|e| panic!("gateway: {e}"));
    request(&gateway, "claude-sonnet-4-5").await;
    request(&gateway, "gpt-4.1-mini").await;
    assert_eq!(anthropic.requests().len(), 1);
    assert_eq!(anthropic.requests()[0].header("x-api-key"), Some("sk-ant"));
    assert_eq!(openai.requests().len(), 1);
    let auth = openai.requests()[0]
        .header("authorization")
        .map(str::to_owned);
    assert_eq!(auth.as_deref(), Some("Bearer sk-openai"));
}

#[tokio::test]
async fn the_example_config_routes_by_model_both_ways() {
    let openai = FakeOpenAi::start().await;
    let anthropic = FakeAnthropic::start().await;
    openai.answer(Answer::text("hi"));
    anthropic.answer(Answer::text("hi"));
    // Its presets, at the fakes.
    let at = |vendor: &str, base_url: String| {
        format!("{{ vendor: {vendor}, base_url: '{base_url}', key: sk }}")
    };
    let yaml = include_str!("../../../../gateway.example.yaml")
        .replace("{ vendor: openai }", &at("openai", openai.base_url()))
        .replace(
            "{ vendor: anthropic }",
            &at("anthropic", anthropic.base_url()),
        );
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

/// The plan of `yaml`.
fn plan(yaml: &str) -> Result<Plan, String> {
    let config = Config::parse(yaml).map_err(|e| e.to_string())?;
    let formats = crate::formats::formats(1 << 20);
    let composed = compose(&config, &formats);
    composed
        .map(|(_, plan)| plan)
        .map_err(|e| e.message().to_owned())
}

#[test]
fn a_preset_is_every_format_of_its_vendor() {
    let plan = plan(
        "providers:
  openai: { vendor: openai, key: sk }
  gemini: { vendor: gemini, key: AIza, base_url: 'http://g.test/v1beta/openai' }
",
    )
    .unwrap_or_else(|e| panic!("{e}"));
    let entries = |i: usize| -> Vec<(String, String, String)> {
        let entries = plan.upstreams[i].entries.iter();
        let entry = |e: &Entry| (e.format.clone(), e.internal.clone(), e.base_url.clone());
        entries.map(entry).collect()
    };
    let entry = |f: &str, i: &str, b: &str| (f.to_owned(), i.to_owned(), b.to_owned());
    assert_eq!(
        entries(0),
        [
            entry("openai-chat", "openai", "https://api.openai.com/v1"),
            entry(
                "openai-realtime",
                "openai:openai-realtime",
                "wss://api.openai.com/v1"
            ),
            entry(
                "responses-ws",
                "openai:responses-ws",
                "wss://api.openai.com/v1"
            ),
            entry(
                "openai-images",
                "openai:openai-images",
                "https://api.openai.com/v1"
            ),
            entry(
                "openai-transcriptions",
                "openai:openai-transcriptions",
                "https://api.openai.com/v1"
            ),
            entry(
                "openai-speech",
                "openai:openai-speech",
                "https://api.openai.com/v1"
            ),
            entry(
                "openai-videos",
                "openai:openai-videos",
                "https://api.openai.com/v1"
            ),
            entry(
                "openai-embeddings",
                "openai:openai-embeddings",
                "https://api.openai.com/v1"
            ),
        ]
    );
    assert_eq!(
        entries(1),
        [
            entry("openai-chat", "gemini", "http://g.test/v1beta/openai"),
            entry("gemini-live", "gemini:gemini-live", "ws://g.test"),
            entry(
                "openai-images",
                "gemini:openai-images",
                "http://g.test/v1beta/openai"
            ),
            entry("gemini-veo", "gemini:gemini-veo", "http://g.test/v1beta"),
            entry(
                "openai-embeddings",
                "gemini:openai-embeddings",
                "http://g.test/v1beta/openai"
            ),
        ]
    );
    let header = |i: usize, e: usize, name: &str| {
        let auth = &plan.upstreams[i].entries[e].auth;
        auth.get(name)
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned)
    };
    assert_eq!(
        header(1, 0, "authorization").as_deref(),
        Some("Bearer AIza")
    );
    assert_eq!(header(1, 1, "x-goog-api-key").as_deref(), Some("AIza"));
    assert_eq!(header(1, 3, "x-goog-api-key").as_deref(), Some("AIza"));
    assert!(plan.upstreams.iter().all(|u| u.own_key));
    // Served by default: the chat formats, each media kind's client format
    // and every session's.
    assert_eq!(
        plan.serve,
        [
            "openai-chat",
            "anthropic-messages",
            "openai-images",
            "openai-transcriptions",
            "openai-speech",
            "openai-videos",
            "openai-embeddings",
            "openai-realtime",
            "responses-ws",
            "gemini-live"
        ]
    );
}

#[test]
fn a_list_of_formats_shares_one_base_url_and_key() {
    let plan = plan(
        "providers:
  both: { format: [anthropic-messages, passthrough], base_url: 'https://x.test/', key: k }
serve: [anthropic-messages, passthrough]
",
    )
    .unwrap_or_else(|e| panic!("{e}"));
    let both = &plan.upstreams[0];
    let internal: Vec<&str> = both.entries.iter().map(|e| e.internal.as_str()).collect();
    assert_eq!(internal, ["both", "both:passthrough"]);
    let x_api_key = both.entries[0].auth.get("x-api-key");
    assert_eq!(x_api_key.and_then(|v| v.to_str().ok()), Some("k"));
    assert!(both.entries[1].auth.contains_key("authorization"));
    assert_eq!(both.entries[1].base_url, "https://x.test");
}

/// Vendors share `openai-chat`: each takes its key as its own vendor says,
/// and a provider of no vendor as OpenAI's does.
#[test]
fn a_key_goes_where_its_own_vendor_takes_it() {
    let keys = plan(
        "providers:
  az: { vendor: azure, base_url: 'https://r.test/openai/v1', key: a }
  oa: { vendor: openai, key: o }
  plain: { format: openai-chat, base_url: 'https://p.test/v1', key: p }
",
    )
    .unwrap_or_else(|e| panic!("{e}"));
    let auth = |i: usize, name: &str| {
        let auth = &keys.upstreams[i].entries[0].auth;
        auth.get(name)
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned)
    };
    assert_eq!(auth(0, "api-key").as_deref(), Some("a"));
    assert_eq!(auth(0, "authorization"), None);
    assert_eq!(auth(1, "authorization").as_deref(), Some("Bearer o"));
    assert_eq!(auth(2, "authorization").as_deref(), Some("Bearer p"));
    // A vendor's dialect is no format of its own.
    let dialect =
        plan("providers:\n  g: { format: groq-chat, base_url: 'https://g.test', key: k }");
    assert!(dialect.is_err_and(|e: String| e.contains("unknown format 'groq-chat'")));
}

#[test]
fn a_bad_provider_is_an_error() {
    let cases = [
        ("p: {}", "provider 'p': needs a `vendor` or a `format`"),
        (
            "p: { vendor: nope }",
            "provider 'p': unknown vendor 'nope'; presets: [openai, anthropic, gemini, groq, deepseek, openrouter, xai, mistral, together, fireworks, ollama, azure, vllm]",
        ),
        (
            "p: { format: openai-chat }",
            "provider 'p': needs a `base_url`",
        ),
        (
            "p: { vendor: openai, key: k, auth: { type: bearer, key: k } }",
            "provider 'p': set `key` or `auth`, not both",
        ),
        (
            "p: { vendor: groq, key: 'env:RESPAN_TEST_UNSET' }",
            "provider 'p': environment variable RESPAN_TEST_UNSET is not set",
        ),
        (
            "p: { format: [openai-chat, openai-chat], base_url: 'http://x' }",
            "provider 'p': lists a format twice",
        ),
    ];
    for (provider, expected) in cases {
        let yaml = format!("providers: {{ {provider} }}");
        assert_eq!(plan(&yaml).err().as_deref(), Some(expected), "{yaml}");
    }
}

#[test]
fn client_keys_are_literal_or_from_the_environment() {
    let unset = plan("keys: [literal, 'env:RESPAN_TEST_UNSET']");
    let said = "keys: environment variable RESPAN_TEST_UNSET is not set";
    assert_eq!(unset.err().as_deref(), Some(said));
    let one = "providers: { p: { format: openai-chat, base_url: 'http://x.test' } }";
    let set = plan(&format!("{one}\nkeys: [one, two]")).unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(set.keys, ["one", "two"]);
}

#[test]
fn two_providers_of_a_session_format_are_an_error() {
    let yaml = "providers:
  openai: { vendor: openai, key: k }
  realtime: { format: openai-realtime, base_url: 'wss://x.test/v1' }
";
    assert_eq!(
        plan(yaml).err().as_deref(),
        Some(
            "serve: openai-realtime needs exactly one provider of that format; openai, realtime all speak it"
        )
    );
}

#[test]
fn bearer_auth_is_a_sensitive_header() {
    let auth = Auth::Bearer { key: "sk".into() };
    let headers = auth_headers("p", &auth).unwrap_or_default();
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
    let host = provider("base_url: 'https://x.test', auth: { type: header, name: host, key: k }");
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
    let config = config.unwrap_or_else(|e| panic!("{e}"));
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
            "serve: unknown format 'nope'; compiled-in formats: [openai-chat, openai-realtime, responses-ws, openai-images, openai-transcriptions, openai-speech, openai-videos, openai-embeddings, anthropic-messages, gemini-live, gemini-veo, passthrough]"
        )
    );
}

#[test]
fn a_format_only_providers_speak_cannot_be_served() {
    assert_eq!(
        plan("serve: [gemini-veo]").err().as_deref(),
        Some("serve: gemini-veo is spoken only by providers; no client sends it")
    );
}

#[test]
fn an_empty_key_is_refused_and_keys_are_trimmed() {
    let one = "providers: { p: { format: openai-chat, base_url: 'http://x.test' } }\n";
    for keys in ["['']", "['  ']"] {
        assert_eq!(
            plan(&format!("{one}keys: {keys}")).err().as_deref(),
            Some("keys: a key is empty"),
            "{keys}"
        );
    }
    let trimmed = plan(&format!("{one}keys: [' gk ']")).map(|p| p.keys);
    assert_eq!(trimmed, Ok(vec!["gk".to_owned()]));
    let empty_provider_key =
        "providers: { p: { format: openai-chat, base_url: 'http://x.test', key: '' } }";
    assert_eq!(
        plan(empty_provider_key).err().as_deref(),
        Some("provider 'p': a key is empty")
    );
    let empty_auth = "providers: { p: { format: openai-chat, base_url: 'http://x.test', auth: { type: bearer, key: ' ' } } }";
    assert_eq!(
        plan(empty_auth).err().as_deref(),
        Some("provider 'p': a key is empty")
    );
}

#[test]
fn a_config_without_providers_is_refused() {
    assert_eq!(
        plan("providers: {}").err().as_deref(),
        Some("providers: none configured; add one, such as `openai: { vendor: openai }`")
    );
}

#[test]
fn a_provider_name_is_letters_digits_and_a_little_punctuation() {
    for name in ["'a/b'", "'a~b'", "'a:b'", "'a b'", "''"] {
        let yaml = format!(
            "providers: {{ {name}: {{ format: openai-chat, base_url: 'http://x.test' }} }}"
        );
        let said = plan(&yaml).err().unwrap_or_default();
        assert!(said.ends_with("(no '/', '~' or ':')"), "{name}: {said}");
    }
    let fine = "providers: { my-gw_2.0: { format: openai-chat, base_url: 'http://x.test' } }";
    assert!(plan(fine).is_ok());
}

#[test]
fn a_vendors_model_name_with_a_slash_goes_to_the_provider_that_serves_it() {
    let plan = plan(
        "providers:
  openai: { vendor: openai, key: sk }
  deepseek: { vendor: deepseek, key: sk }
  mistral: { vendor: mistral, key: sk }
  together: { vendor: together, key: sk }
  fireworks: { vendor: fireworks, key: sk }
  openrouter: { vendor: openrouter, key: sk }
",
    )
    .unwrap_or_else(|e| panic!("{e}"));
    let to = |model: &str| {
        let route = plan.routes.route(kind::CHAT, "openai-chat", Some(model));
        route.map(|r| (r.provider, r.model))
    };
    let whole = |provider: &str| Some((provider.to_owned(), None));
    assert_eq!(
        to("meta-llama/Llama-3.3-70B-Instruct-Turbo"),
        whole("together")
    );
    assert_eq!(
        to("deepseek-ai/DeepSeek-V3"),
        whole("together"),
        "not deepseek"
    );
    assert_eq!(
        to("mistralai/Mixtral-8x7B"),
        whole("together"),
        "not mistral"
    );
    assert_eq!(to("deepseek-chat"), whole("deepseek"));
    assert_eq!(
        to("accounts/fireworks/models/llama-v3p3-70b-instruct"),
        whole("fireworks")
    );
    assert_eq!(to("anthropic/claude-3.5-sonnet"), whole("openrouter"));
    assert_eq!(to("qwen/qwen3-32b"), whole("openrouter"));
    // A configured provider's name comes first, and is taken off.
    assert_eq!(
        to("openai/gpt-4o-mini"),
        Some(("openai".to_owned(), Some("gpt-4o-mini".to_owned())))
    );
    assert_eq!(
        to("together/openai/gpt-oss-120b"),
        Some((
            "together".to_owned(),
            Some("openai/gpt-oss-120b".to_owned())
        ))
    );
}
