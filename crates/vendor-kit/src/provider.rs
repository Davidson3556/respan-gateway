//! A provider of a vendor, ready to register: each format at its base URL,
//! taking the key as the vendor takes it, with the vendor's quirks; and its
//! model list.

use std::sync::Arc;
use std::time::Duration;

use crate::{Auth, Base, Connect, Models, Reach, Vendor, Wire};
use respan_gateway_core::{Fault, Gateway};
use respan_gateway_http::http::HeaderMap;
use respan_gateway_http::http::header::{AUTHORIZATION, HeaderName, HeaderValue};
use respan_gateway_http_transport::{HttpTransport, reqwest, unreachable};
use respan_gateway_ws::Limits;

/// A provider of a vendor ([`Vendor::provider`]), or of formats of no
/// vendor, at a base URL with a key, to register
/// ([`register`](VendorProvider::register)).
///
/// ```ignore
/// let openai = respan_gateway_vendors::openai::VENDOR.provider("openai");
/// let gateway = openai.register(Gateway::new())?;
/// let models = openai.models().await;
/// let usage = openai.client()?.send(request).await?; // any call of its own
/// ```
#[derive(Clone)]
pub struct VendorProvider {
    name: String,
    vendor: &'static Vendor,
    formats: Vec<&'static dyn Wire>,
    base: Option<String>,
    key: Key,
    limit: usize,
    sessions: Limits,
}

/// One format's core provider of a [`VendorProvider`] ([`VendorProvider::parts`]).
#[derive(Debug)]
pub struct Part {
    /// Its name: the provider's for its first format, `name:format` for the
    /// others.
    pub name: String,
    /// Its format, as the vendor speaks it.
    pub format: &'static dyn Wire,
    /// Its base URL.
    pub base_url: String,
    /// What its calls carry to authenticate.
    pub headers: HeaderMap,
}

/// What a provider's calls authenticate with.
#[derive(Clone)]
enum Key {
    /// The client's own credentials.
    Client,
    /// A key, sent as each format takes it.
    Own(String),
    /// These headers, as they are.
    Headers(HeaderMap),
}

impl std::fmt::Debug for VendorProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VendorProvider")
            .field("name", &self.name)
            .field("vendor", &self.vendor.id)
            .finish_non_exhaustive()
    }
}

impl Vendor {
    /// Provider `name` of this vendor: every format it speaks, at its base
    /// URL, with its key from the first of `key_env` set, else the client's.
    pub fn provider(&'static self, name: impl Into<String>) -> VendorProvider {
        let env = |var: &&str| std::env::var(var).ok().filter(|k| !k.trim().is_empty());
        let key = self.key_env.iter().find_map(env);
        VendorProvider {
            vendor: self,
            formats: self.formats.to_vec(),
            base: self.located().ok().flatten(),
            key: key.map_or(Key::Client, Key::Own),
            ..VendorProvider::new(name)
        }
    }
}

impl VendorProvider {
    /// Provider `name` of no vendor and no formats yet
    /// ([`formats`](Self::formats)), with the client's credentials.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            vendor: &NONE,
            formats: Vec::new(),
            base: None,
            key: Key::Client,
            limit: 32 << 20,
            sessions: Limits::default(),
        }
    }

    /// Speaking `speaks` alone, in place of its vendor's formats.
    pub fn formats(mut self, speaks: impl IntoIterator<Item = &'static dyn Wire>) -> Self {
        self.formats = speaks.into_iter().collect();
        self
    }

    /// At `base_url`, the base its formats sit beside.
    pub fn base(mut self, base_url: impl Into<String>) -> Self {
        self.base = Some(base_url.into());
        self
    }

    /// With `key`, sent as each format takes it.
    pub fn key(mut self, key: impl Into<String>) -> Self {
        self.key = Key::Own(key.into());
        self
    }

    /// With `headers` on every call, as they are, in place of a key.
    pub fn headers(mut self, headers: HeaderMap) -> Self {
        self.key = Key::Headers(headers);
        self
    }

    /// Reading up to `bytes` of one answer at once.
    pub fn limit(mut self, bytes: usize) -> Self {
        self.limit = bytes;
        self
    }

    /// Its sessions taking `limits`.
    pub fn sessions(mut self, limits: Limits) -> Self {
        self.sessions = limits;
        self
    }

    /// Its vendor: [`Vendor::DEFAULT`]'s, of no id, for one of no vendor.
    pub fn vendor(&self) -> &'static Vendor {
        self.vendor
    }

    /// Each format's core provider. One format of no vendor sits at the
    /// base URL exactly, in its scheme. `Err` with `config` if it has no
    /// base URL, or a key no header can carry.
    pub fn parts(&self) -> Result<Vec<Part>, Fault> {
        let base = self.base.as_deref();
        let base = base.ok_or_else(|| Fault::config("needs a `base_url`"))?;
        let exact = self.vendor.id.is_empty() && self.formats.len() == 1;
        let part = |(i, format): (usize, &&'static dyn Wire)| {
            Ok(Part {
                name: match i {
                    0 => self.name.clone(),
                    _ => format!("{}:{}", self.name, format.name()),
                },
                format: *format,
                base_url: match exact {
                    true => format.scheme(base),
                    false => format.base_url(base),
                },
                headers: self.auth(format.auth())?,
            })
        };
        self.formats.iter().enumerate().map(part).collect()
    }

    /// `gateway` with a core provider of each of its formats, each carrying
    /// its vendor's quirks ([`Vendor::plugin`]).
    pub fn register(&self, mut gateway: Gateway) -> Result<Gateway, Fault> {
        for part in self.parts()? {
            let wire = part.format;
            let connect = self.connect(wire, &part.base_url, part.headers)?;
            let mut core = wire.provider(part.name, connect);
            if let Some(quirks) = self.vendor.plugin {
                core = core.plugin(quirks);
            }
            gateway = gateway.provider(core)?;
        }
        Ok(gateway)
    }

    /// A client of it: each call to its base URL, with its key as its first
    /// format over HTTP takes it.
    pub fn client(&self) -> Result<HttpTransport, Fault> {
        self.client_at(Base::Same)
    }

    /// A client of it at `base`, relative to its base URL, such as an API
    /// beside the one its formats are at.
    pub fn client_at(&self, base: Base) -> Result<HttpTransport, Fault> {
        let part = self.parts()?.into_iter().find(|p| of_api(p.format));
        let part = part.ok_or_else(|| Fault::config("speaks no format of an API over HTTP"))?;
        HttpTransport::new(base.of(&part.base_url))?.with_headers(part.headers)
    }

    /// A client of it at the base URL of its format `format`, if it speaks
    /// it.
    pub fn client_of(&self, format: &str) -> Result<Option<HttpTransport>, Fault> {
        let part = self
            .parts()?
            .into_iter()
            .find(|p| p.format.name() == format);
        let client = |p: Part| HttpTransport::new(&p.base_url)?.with_headers(p.headers);
        part.map(client).transpose()
    }

    /// The models it lists, as its vendor lists them ([`Vendor::models`]);
    /// `None` if it speaks no format whose models are listed, such as only
    /// passthrough or sessions.
    pub async fn models(&self) -> Option<Models> {
        if !self.formats.iter().any(|f| of_api(*f)) {
            return None;
        }
        let listed = (self.vendor.models)(self).await;
        Some(listed.map_err(|f| unreached(&f).map_or(f, Fault::upstream)))
    }

    /// The headers its calls of a format that takes the key `auth` carry.
    fn auth(&self, auth: Auth) -> Result<HeaderMap, Fault> {
        let (name, value) = match (&self.key, auth) {
            (Key::Client, _) => return Ok(HeaderMap::new()),
            (Key::Headers(headers), _) => return Ok(headers.clone()),
            (Key::Own(key), Auth::Bearer) => (AUTHORIZATION, format!("Bearer {key}")),
            (Key::Own(key), Auth::Header(name)) => (HeaderName::from_static(name), key.clone()),
        };
        let mut value = HeaderValue::from_str(&value).map_err(|e| Fault::config(e.to_string()))?;
        value.set_sensitive(true);
        Ok(HeaderMap::from_iter([(name, value)]))
    }

    /// What makes the calls of `wire` at `base`, carrying `auth`: a session
    /// over WebSocket; or calls over HTTP, a provider it cannot reach named
    /// in the fault with its host, as the transport's names neither.
    fn connect(&self, wire: &dyn Wire, base: &str, auth: HeaderMap) -> Result<Connect, Fault> {
        let follow = match wire.reach() {
            Reach::Ws(transport) => {
                let transport = transport(base)?.with_limits(self.sessions);
                return Ok(Arc::new(transport.with_headers(auth)?.connect()));
            }
            Reach::Http { follow } => follow,
        };
        let mut client = HttpTransport::new(base)?;
        if let Some(follow) = follow {
            client = client
                .with_connect_timeout(Duration::from_secs(10))?
                .follow_redirects(follow);
        }
        let client = Arc::new(client.with_headers(auth)?);
        let host = base.split_once("://").map_or(base, |(_, rest)| rest);
        let host = host.split('/').next().unwrap_or_default();
        let who: Arc<str> = format!("provider '{}' at {host}", self.name).into();
        Ok(Arc::new(respan_gateway_http::connect(
            move |call| {
                let (client, who) = (client.clone(), who.clone());
                async move {
                    client
                        .send(call)
                        .await
                        .map_err(|fault| match unreached(&fault) {
                            Some(why) => Fault::upstream(format!("{who} {why}")).with_source(fault),
                            None => fault,
                        })
                }
            },
            self.limit,
        )))
    }
}

/// The vendor of a provider of no vendor.
static NONE: Vendor = Vendor::DEFAULT;

/// Whether `format` is of the provider's API over HTTP: not a session's,
/// nor passthrough's, which sits at the origin beside it.
fn of_api(format: &dyn Wire) -> bool {
    !format.websocket() && format.name() != "passthrough"
}

/// Why `fault` says its provider could not be reached, if it does.
pub(crate) fn unreached(fault: &Fault) -> Option<String> {
    let e = fault.cause::<reqwest::Error>();
    e.filter(|e| !e.is_builder()).map(unreachable)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A format of no kind, over HTTP, or as a session if `.0`.
    struct Fake(bool);

    impl Wire for Fake {
        fn name(&self) -> &str {
            "fake"
        }
        fn reach(&self) -> Reach {
            match self.0 {
                true => Reach::Ws(|_| Err(Fault::config("not dialled"))),
                false => Reach::Http { follow: None },
            }
        }
        fn auth(&self) -> Auth {
            Auth::Bearer
        }
        fn base(&self) -> Base {
            Base::Origin
        }
        fn provider(&self, _: String, _: Connect) -> respan_gateway_core::Provider {
            unreachable!("not registered")
        }
    }

    #[test]
    fn one_format_of_no_vendor_sits_at_its_base_exactly() {
        let http: &'static dyn Wire = &Fake(false);
        let one = VendorProvider::new("p")
            .formats([http])
            .base("https://x.test/v1");
        let parts = one.parts().unwrap_or_default();
        assert_eq!(parts[0].base_url, "https://x.test/v1");
        // A session's, in its scheme.
        let session: &'static dyn Wire = &Fake(true);
        let one = VendorProvider::new("p")
            .formats([session])
            .base("https://x.test/v1");
        let parts = one.parts().unwrap_or_default();
        assert_eq!(parts[0].base_url, "wss://x.test/v1");
        assert!(VendorProvider::new("p").parts().is_err(), "no base URL");
    }

    #[test]
    fn a_base_url_takes_its_names_from_the_environment() {
        let at = |base_url| Vendor {
            base_url,
            ..Vendor::DEFAULT
        };
        let home = std::env::var("HOME").unwrap_or_default();
        assert_eq!(
            at(Some("https://x.test/{HOME}/v1")).located(),
            Ok(Some(format!("https://x.test/{home}/v1")))
        );
        assert_eq!(
            at(Some("https://x.test/{RESPAN_NOT_SET}")).located(),
            Err("RESPAN_NOT_SET")
        );
        assert_eq!(at(None).located(), Ok(None));
    }
}
