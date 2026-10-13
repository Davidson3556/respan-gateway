#![warn(missing_docs)]
//! A vendor as a template: where it is, how it takes a key, the formats it
//! speaks, and its quirks ([`Vendor`]); and a provider of it, at a base URL
//! with a key, which registers a core provider of each of its formats and
//! lists its models ([`VendorProvider`]). The vendors themselves are
//! `respan-gateway-vendors`'.
//!
//! A format is a spec and names no vendor. Where a vendor's deployment
//! departs from a spec, its vendor says so: as data, such as the header a
//! key goes in ([`Speaks::key_in`]) or how it lists its models ([`ListModels`]), or as
//! a [`Plugin`] that turns what the vendor sends beyond the spec into the
//! kind's events and back. Each is described as a [`Quirk`].

use respan_gateway_core::Plugin;

pub mod models;
mod provider;
mod wire;

pub use models::{BoxFuture, ListModels, Model, Models};
pub use provider::{Part, VendorProvider};
pub use respan_gateway_http::{Body, http};
pub use respan_gateway_http_faults::built_wrong;
pub use respan_gateway_http_transport::{HttpTransport, json};
pub use wire::{Connect, Reach, Speaks, Wire};

/// A vendor: a template of the providers of it ([`Vendor::provider`]). A
/// vendor says what it is and how it departs from the specs it speaks; the
/// rest is [`Vendor::DEFAULT`]'s:
///
/// ```ignore
/// pub const VENDOR: Vendor = Vendor {
///     id: "mistral",
///     base_url: Some("https://api.mistral.ai/v1"),
///     key_env: &["MISTRAL_API_KEY"],
///     formats: &[&Speaks::http(OpenAiChat), &Speaks::http(OpenAiEmbeddings)],
///     ..Vendor::DEFAULT
/// };
/// ```
pub struct Vendor {
    /// Its id, as the gateway's config names it: `vendor = "<id>"`.
    pub id: &'static str,
    /// The base URL its SDKs take, if it has one of its own; a `{NAME}` in
    /// it is the environment variable `NAME` ([`Vendor::located`]), such as
    /// an account's id.
    pub base_url: Option<&'static str>,
    /// The environment variables its key is read from, the first set; none
    /// for a vendor that needs no key.
    pub key_env: &'static [&'static str],
    /// The formats it speaks, as it speaks them ([`Speaks`]).
    pub formats: &'static [&'static dyn Wire],
    /// How it lists a provider's models: as OpenAI does (`GET /models`,
    /// [`models::openai`]) unless it says.
    pub models: ListModels,
    /// How it departs from the specs it speaks.
    pub quirks: &'static [Quirk],
    /// The plugin of its quirks, a plugin of each of its providers
    /// ([`Provider::plugin`](respan_gateway_core::Provider::plugin)): run on
    /// their calls alone, after the gateway's plugins, so those see the
    /// kind's events. Not optional: a provider of this vendor needs it.
    pub plugin: Option<&'static dyn Plugin>,
}

impl std::fmt::Debug for Vendor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Vendor")
            .field("id", &self.id)
            .field("formats", &self.formats)
            .field("quirks", &self.quirks)
            .finish_non_exhaustive()
    }
}

impl Vendor {
    /// A vendor of no id, base URL, key or formats, listing its models as
    /// OpenAI does, with no quirks: what a vendor does not say.
    pub const DEFAULT: Vendor = Vendor {
        id: "",
        base_url: None,
        key_env: &[],
        formats: &[],
        models: models::openai,
        quirks: &[],
        plugin: None,
    };

    /// Its base URL, each `{NAME}` in it the environment variable `NAME`;
    /// `Err` with the name of one not set.
    pub fn located(&self) -> Result<Option<String>, &'static str> {
        let Some(mut rest) = self.base_url else {
            return Ok(None);
        };
        let mut url = String::new();
        while let Some((before, after)) = rest.split_once('{') {
            let (name, after) = after.split_once('}').unwrap_or((after, ""));
            let value = std::env::var(name).ok().filter(|v| !v.trim().is_empty());
            url += before;
            url += value.as_deref().ok_or(name)?.trim();
            rest = after;
        }
        Ok(Some(url + rest))
    }
}

/// A way a vendor's deployment departs from a spec it speaks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Quirk {
    /// Its name, such as `x_groq_usage`.
    pub name: &'static str,
    /// What the vendor does.
    pub description: &'static str,
}

impl Base {
    /// Where it is for `base_url`.
    pub fn of(self, base_url: &str) -> &str {
        let base_url = base_url.trim_end_matches('/');
        match self {
            Base::Same => base_url,
            Base::Origin => origin(base_url),
            Base::Without(suffix) => base_url.strip_suffix(suffix).unwrap_or(base_url),
        }
    }
}

/// `scheme://host[:port]` of `url`.
fn origin(url: &str) -> &str {
    let after_scheme = url.find("://").map_or(0, |i| i + 3);
    match url[after_scheme..].find('/') {
        Some(slash) => &url[..after_scheme + slash],
        None => url,
    }
}

/// How a format takes a provider's key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Auth {
    /// `Authorization: Bearer <key>`.
    Bearer,
    /// The key alone, in this header.
    Header(&'static str),
}

/// Where a format sits relative to the base URL of a provider with several
/// formats. A WebSocket format takes `ws`/`wss` in place of `http`/`https`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Base {
    /// The base URL itself.
    Same,
    /// Its origin, `scheme://host[:port]`.
    Origin,
    /// The base URL without this suffix, if it ends in it.
    Without(&'static str),
}
