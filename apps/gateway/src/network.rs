//! `[network]`: how the gateway reaches providers and tool servers, through
//! which proxy and trusting which certificates; and a provider's or tool
//! server's own `proxy`.

use std::path::Path;

use respan_gateway_core::Fault;
use respan_gateway_http_transport::Network;
use serde::{Deserialize, Deserializer};

use crate::compose::secret;
use crate::config::{Config, Source};

/// The variables a program reads its proxy from, and those that exempt
/// hosts, each also in lower case.
const PROXY_VARS: [&str; 3] = ["HTTPS_PROXY", "HTTP_PROXY", "ALL_PROXY"];
const NO_PROXY_VAR: &str = "NO_PROXY";

/// `[network]`.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NetworkConfig {
    /// The proxy every outbound connection goes through: a URL such as
    /// `http://proxy.corp:3128`, with `user:password@` if it asks, literal
    /// or `env:NAME`; `false` for none. Unset: the environment's
    /// (`HTTPS_PROXY`, `HTTP_PROXY`, `ALL_PROXY`, minus `NO_PROXY`).
    pub proxy: Option<Proxy>,
    /// Hosts reached without the proxy: a host, a domain from its leading
    /// `.`, an IP address or a CIDR range, or `*`.
    #[serde(default)]
    pub no_proxy: Vec<String>,
    /// A PEM file of certificate authorities trusted besides the system's,
    /// such as a TLS-inspecting proxy's; relative to the config file.
    pub ca: Option<std::path::PathBuf>,
}

/// A proxy setting: a URL, or none.
#[derive(Clone, PartialEq, Eq)]
pub enum Proxy {
    Url(String),
    Off,
}

// A proxy's URL may hold its password.
impl std::fmt::Debug for Proxy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Url(_) => f.write_str("Url(..)"),
            Self::Off => f.write_str("Off"),
        }
    }
}

impl<'de> Deserialize<'de> for Proxy {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Raw {
            Url(String),
            On(bool),
        }
        match Raw::deserialize(d)? {
            Raw::Url(url) => Ok(Self::Url(url)),
            Raw::On(false) => Ok(Self::Off),
            Raw::On(true) => Err(serde::de::Error::custom(
                "a proxy is a URL, or false for none",
            )),
        }
    }
}

/// The network `config` says, for every provider and tool server without a
/// `proxy` of its own.
pub fn network(config: &Config) -> Result<Network, Fault> {
    let n = &config.network;
    let mut network = Network::default();
    if let Some(ca) = &n.ca {
        let path = match config.source() {
            Source::File(file) => file.parent().unwrap_or(Path::new("")).join(ca),
            _ => ca.clone(),
        };
        let shown = path.display();
        let pem = std::fs::read(&path)
            .map_err(|e| Fault::config(format!("network: ca: {shown}: {e}")))?;
        network = network
            .trust_pem(&pem)
            .map_err(|e| Fault::config(format!("network: ca: {shown}: {}", e.message())))?;
    }
    if n.proxy.is_none() && !n.no_proxy.is_empty() {
        return Err(Fault::config(
            "network: no_proxy is for a `proxy`; the environment's takes NO_PROXY",
        ));
    }
    with_proxy("network", network, n.proxy.as_ref(), &n.no_proxy)
}

/// `base`, the config's, through `proxy` if it names one: a provider's or
/// tool server's own, `owner`'s.
pub fn of(
    owner: &str,
    base: &Network,
    config: &NetworkConfig,
    proxy: Option<&Proxy>,
) -> Result<Network, Fault> {
    match proxy {
        None => Ok(base.clone()),
        Some(proxy) => with_proxy(owner, base.clone(), Some(proxy), &config.no_proxy),
    }
}

fn with_proxy(
    owner: &str,
    network: Network,
    proxy: Option<&Proxy>,
    no_proxy: &[String],
) -> Result<Network, Fault> {
    match proxy {
        None => Ok(network),
        Some(Proxy::Off) => Ok(network.direct()),
        Some(Proxy::Url(url)) => {
            let url = secret(&format!("{owner}: proxy"), url)?;
            (network.proxy(&url, no_proxy))
                .map_err(|e| Fault::config(format!("{owner}: proxy: {}", e.message())))
        }
    }
}

/// The proxy variables a tool server's program is given, which gets nothing
/// of the gateway's environment but `PATH`: the config's proxy, `proxy` if
/// its own, as `HTTPS_PROXY` and the rest; none for `false`; and, if neither
/// says, the gateway's own.
pub fn child_env(
    owner: &str,
    config: &NetworkConfig,
    proxy: Option<&Proxy>,
) -> Result<Vec<(String, String)>, Fault> {
    let both = |name: &str| [name.to_owned(), name.to_lowercase()];
    match proxy.or(config.proxy.as_ref()) {
        Some(Proxy::Off) => Ok(Vec::new()),
        Some(Proxy::Url(url)) => {
            let url = secret(&format!("{owner}: proxy"), url)?;
            let mut env: Vec<(String, String)> = PROXY_VARS
                .iter()
                .flat_map(|v| both(v))
                .map(|v| (v, url.clone()))
                .collect();
            if !config.no_proxy.is_empty() {
                let hosts = config.no_proxy.join(",");
                env.extend(both(NO_PROXY_VAR).map(|v| (v, hosts.clone())));
            }
            Ok(env)
        }
        None => {
            let vars = PROXY_VARS
                .iter()
                .chain([&NO_PROXY_VAR])
                .flat_map(|v| both(v));
            let set = vars.filter_map(|v| std::env::var(&v).ok().map(|value| (v, value)));
            Ok(set.collect())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parsed(text: &str) -> NetworkConfig {
        toml::from_str(text).unwrap_or_else(|e| panic!("{e}"))
    }

    #[test]
    fn a_proxy_is_a_url_or_false() {
        let n = parsed("proxy = \"http://proxy:3128\"");
        assert_eq!(n.proxy, Some(Proxy::Url("http://proxy:3128".into())));
        assert_eq!(parsed("proxy = false").proxy, Some(Proxy::Off));
        assert!(toml::from_str::<NetworkConfig>("proxy = true").is_err());
    }

    #[test]
    fn no_proxy_needs_a_proxy() {
        let config = Config {
            network: parsed("no_proxy = [\"localhost\"]"),
            ..Config::default()
        };
        assert!(network(&config).is_err());
    }

    #[test]
    fn a_bad_proxy_is_named_without_its_url() {
        let config = Config {
            network: parsed("proxy = \"not a url\""),
            ..Config::default()
        };
        let refused = network(&config)
            .map(|_| ())
            .map_err(|f| f.message().to_owned());
        let message = refused.err().unwrap_or_default();
        assert!(message.starts_with("network: proxy: "), "{message}");
        assert!(!message.contains("not a url"));
    }

    #[test]
    fn a_program_gets_the_proxy_it_is_given() {
        let n = parsed("proxy = \"http://p:1\"\nno_proxy = [\"a\", \"b\"]");
        let env = |proxy: Option<&Proxy>| child_env("t", &n, proxy).unwrap_or_default();
        let given = env(None);
        assert!(given.contains(&("HTTPS_PROXY".into(), "http://p:1".into())));
        assert!(given.contains(&("no_proxy".into(), "a,b".into())));
        let own = env(Some(&Proxy::Url("http://q:2".into())));
        assert!(own.contains(&("https_proxy".into(), "http://q:2".into())));
        assert!(env(Some(&Proxy::Off)).is_empty());
    }
}
