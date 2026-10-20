//! [`Network`]: how outbound connections are made, through which proxy and
//! trusting which certificates, and the clients made so.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use respan_gateway_core::Fault;

/// How outbound connections are made: through a proxy or none, and trusting
/// some certificate authorities besides the system's. Every client of the
/// gateway's is built from one ([`builder`](Self::builder)), and kept for
/// each purpose ([`client`](Self::client)), so its connections are pooled;
/// a clone shares them.
///
/// The default takes the environment's proxy (`HTTPS_PROXY`, `HTTP_PROXY`,
/// `ALL_PROXY`, minus `NO_PROXY`, and their lower-case names) and the
/// system's certificates.
///
/// ```ignore
/// let network = Network::default()
///     .proxy("http://proxy.corp:3128", &["localhost".into(), ".corp".into()])?
///     .trust_pem(&std::fs::read("corp-root.pem")?)?;
/// let transport = HttpTransport::new(base_url)?.with_network(&network)?;
/// ```
#[derive(Clone, Default)]
pub struct Network {
    route: Route,
    /// Certificate authorities trusted besides the system's, as PEM.
    pem: Vec<u8>,
    trusted: Vec<reqwest::Certificate>,
    clients: Arc<Mutex<HashMap<&'static str, reqwest::Client>>>,
}

/// Where connections go.
#[derive(Clone, Default)]
enum Route {
    /// Through the environment's proxy, if it names one.
    #[default]
    Environment,
    /// Straight to the host, whatever the environment says.
    Direct,
    /// Through this proxy, except to the hosts it leaves out.
    Proxy(Box<reqwest::Proxy>),
}

impl std::fmt::Debug for Network {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // A proxy's URL may hold its password.
        let route = match self.route {
            Route::Environment => "environment",
            Route::Direct => "direct",
            Route::Proxy(_) => "proxy",
        };
        f.debug_struct("Network")
            .field("route", &route)
            .field("trusted", &self.trusted.len())
            .finish_non_exhaustive()
    }
}

impl Network {
    /// Through the proxy at `url` (`http://` or `https://`, with
    /// `user:password@` if it asks), for every host but those
    /// `no_proxy` names: a host, a domain from its leading `.`, an IP
    /// address or a CIDR range, or `*` for all, as `NO_PROXY` takes them.
    pub fn proxy(self, url: &str, no_proxy: &[String]) -> Result<Self, Fault> {
        // The URL is not quoted: it may hold a password.
        let proxy = reqwest::Proxy::all(url)
            .map_err(|_| Fault::config("a proxy is a URL such as http://proxy:3128"))?;
        let proxy = proxy.no_proxy(reqwest::NoProxy::from_string(&no_proxy.join(",")));
        Ok(self.with_route(Route::Proxy(Box::new(proxy))))
    }

    /// Straight to every host, whatever proxy the environment names.
    pub fn direct(self) -> Self {
        self.with_route(Route::Direct)
    }

    /// Trusting the certificate authorities in `pem` too, such as a TLS-
    /// inspecting proxy's. `Err` with `config` if it holds none.
    pub fn trust_pem(mut self, pem: &[u8]) -> Result<Self, Fault> {
        let bad = || Fault::config("not a PEM file of certificates");
        let certs = reqwest::Certificate::from_pem_bundle(pem).map_err(|_| bad())?;
        if certs.is_empty() {
            return Err(bad());
        }
        self.trusted.extend(certs);
        self.pem.extend_from_slice(pem);
        self.pem.push(b'\n');
        self.clients = Arc::default();
        Ok(self)
    }

    /// The certificate authorities it trusts besides the system's, as PEM,
    /// for a client whose TLS is set up apart from Reqwest's.
    pub fn trusted_pem(&self) -> &[u8] {
        &self.pem
    }

    /// A client builder that connects as it says. A caller sets the rest:
    /// timeouts, redirects, its user agent.
    pub fn builder(&self) -> reqwest::ClientBuilder {
        let builder = reqwest::Client::builder().tls_certs_merge(self.trusted.iter().cloned());
        match &self.route {
            Route::Environment => builder,
            Route::Direct => builder.no_proxy(),
            Route::Proxy(proxy) => builder.proxy(*proxy.clone()),
        }
    }

    /// Its client for `purpose`, built by `build` from its
    /// [`builder`](Self::builder) the first time and shared after, by its
    /// clones too.
    pub fn client(
        &self,
        purpose: &'static str,
        build: impl FnOnce(reqwest::ClientBuilder) -> Result<reqwest::ClientBuilder, Fault>,
    ) -> Result<reqwest::Client, Fault> {
        let mut clients = self.clients.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(client) = clients.get(purpose) {
            return Ok(client.clone());
        }
        let client = build(self.builder())?.build().map_err(|e| {
            Fault::internal(format!("the {purpose} client could not be built")).with_source(e)
        })?;
        clients.insert(purpose, client.clone());
        Ok(client)
    }

    /// The default, one for the process: what a transport given no network
    /// connects through.
    pub fn shared() -> &'static Network {
        static NETWORK: std::sync::OnceLock<Network> = std::sync::OnceLock::new();
        NETWORK.get_or_init(Network::default)
    }

    fn with_route(mut self, route: Route) -> Self {
        self.route = route;
        self.clients = Arc::default();
        self
    }
}

#[cfg(test)]
mod tests {
    use super::Network;

    fn clients(network: &Network) -> usize {
        network.clients.lock().map_or(0, |c| c.len())
    }

    #[test]
    fn a_proxy_is_a_url() {
        assert!(Network::default().proxy("http://proxy:3128", &[]).is_ok());
        let refused = Network::default().proxy("not a url", &[]);
        assert!(refused.is_err_and(|fault| !fault.message().contains("not a url")));
    }

    #[test]
    fn trusts_only_certificates() {
        assert!(Network::default().trust_pem(b"nothing").is_err());
    }

    #[test]
    fn a_purpose_shares_its_client_and_a_change_drops_them() {
        let network = Network::default();
        assert!(network.client("a", Ok).is_ok());
        assert_eq!(clients(&network), 1);
        let clone = network.clone();
        assert!(clone.client("a", Ok).is_ok());
        assert_eq!(clients(&network), 1);
        assert_eq!(clients(&network.clone().direct()), 0);
    }
}
