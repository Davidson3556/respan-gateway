//! A format as a vendor speaks it ([`Speaks`]): any core [`Format`] of the
//! provider edge, over HTTP or as a session, with where the vendor takes
//! its key and where it sits; its type erased once, where the vendor names
//! it, as a [`Wire`].

use std::marker::PhantomData;
use std::sync::Arc;

use respan_gateway_core::{Extensions, Fault, Format, Provider};
use respan_gateway_http::ProviderIo;
use respan_gateway_http::http::Uri;
use respan_gateway_ws::WsTransport;

use crate::{Auth, Base};

/// What makes a provider's calls, one I/O each.
pub type Connect = Arc<dyn Fn(Extensions) -> ProviderIo + Send + Sync>;

/// How a format's calls reach its provider.
#[derive(Debug, Clone, Copy)]
pub enum Reach {
    /// Over HTTP, following the redirects `follow` allows, if any.
    Http {
        /// The redirects followed: a call that follows one waits at most
        /// 10 s to connect.
        follow: Option<fn(&Uri) -> bool>,
    },
    /// As a WebSocket session, over the transport made for its base URL.
    Ws(fn(&str) -> Result<WsTransport, Fault>),
}

/// A format a vendor speaks ([`Speaks`]), its type erased: its name, how
/// its calls reach the provider, how it takes the key and where it sits,
/// and the core provider of it.
pub trait Wire: Send + Sync {
    /// Its name, as a config names it.
    fn name(&self) -> &str;
    /// How its calls reach the provider.
    fn reach(&self) -> Reach;
    /// How it takes the key.
    fn auth(&self) -> Auth;
    /// Where it sits relative to the vendor's base URL.
    fn base(&self) -> Base;
    /// Whether it is a WebSocket session's.
    fn websocket(&self) -> bool {
        matches!(self.reach(), Reach::Ws(_))
    }
    /// Its base URL, for a provider at `base_url`, where it sits.
    fn base_url(&self, base_url: &str) -> String {
        self.scheme(self.base().of(base_url))
    }
    /// `url` in its scheme: a WebSocket format's `ws`/`wss`, another's
    /// `http`/`https`.
    fn scheme(&self, url: &str) -> String {
        let swap = match self.websocket() {
            true => [("https://", "wss://"), ("http://", "ws://")],
            false => [("wss://", "https://"), ("ws://", "http://")],
        };
        swap.iter()
            .find_map(|(from, to)| url.strip_prefix(from).map(|rest| format!("{to}{rest}")))
            .unwrap_or_else(|| url.to_owned())
    }
    /// The core provider `name` of it, its calls made by `connect`.
    fn provider(&self, name: String, connect: Connect) -> Provider;
}

impl std::fmt::Debug for dyn Wire {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}

/// A core format `F` of the provider edge, with input `I` and output `O`,
/// as a vendor speaks it: over HTTP ([`Speaks::http`]) or as a session
/// ([`Speaks::ws`]), its key a bearer token ([`Speaks::key_in`]), at the
/// vendor's base URL ([`Speaks::at`]). A vendor lists it as a [`Wire`]:
///
/// ```ignore
/// formats: &[&Speaks::http(GeminiGenerate).key_in("x-goog-api-key").at(Base::Without("/openai"))],
/// ```
pub struct Speaks<F, I, O> {
    format: F,
    reach: Reach,
    auth: Auth,
    base: Base,
    kind: PhantomData<fn() -> (I, O)>,
}

impl<F, I, O> std::fmt::Debug for Speaks<F, I, O> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Speaks")
            .field("reach", &self.reach)
            .finish_non_exhaustive()
    }
}

impl<F, I, O> Speaks<F, I, O> {
    /// `format` over HTTP.
    pub const fn http(format: F) -> Self {
        Self {
            format,
            reach: Reach::Http { follow: None },
            auth: Auth::Bearer,
            base: Base::Same,
            kind: PhantomData,
        }
    }

    /// `format` as a WebSocket session, over the transport `transport` makes
    /// for its base URL.
    pub const fn ws(format: F, transport: fn(&str) -> Result<WsTransport, Fault>) -> Self {
        Self {
            format,
            reach: Reach::Ws(transport),
            auth: Auth::Bearer,
            base: Base::Same,
            kind: PhantomData,
        }
    }

    /// Over HTTP, following the redirects `follow` allows, such as to the
    /// storage a provider serves its files from.
    pub const fn follow(mut self, follow: fn(&Uri) -> bool) -> Self {
        self.reach = Reach::Http {
            follow: Some(follow),
        };
        self
    }

    /// Its key in the header `name`.
    pub const fn key_in(mut self, name: &'static str) -> Self {
        self.auth = Auth::Header(name);
        self
    }

    /// Sitting at `base`.
    pub const fn at(mut self, base: Base) -> Self {
        self.base = base;
        self
    }
}

impl<F, I, O> Wire for Speaks<F, I, O>
where
    F: Format<ProviderIo, O, I> + Clone + Sync,
    I: Clone + Send + Sync + 'static,
    O: Clone + Send + Sync + 'static,
{
    fn name(&self) -> &str {
        Format::<ProviderIo, O, I>::name(&self.format)
    }

    fn reach(&self) -> Reach {
        self.reach
    }

    fn auth(&self) -> Auth {
        self.auth
    }

    fn base(&self) -> Base {
        self.base
    }

    fn provider(&self, name: String, connect: Connect) -> Provider {
        Provider::new(name, self.format.clone(), move |x| connect(x))
    }
}
