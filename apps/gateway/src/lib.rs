//! The respan-gateway application, as a library: its configuration
//! ([`Config`]) and the HTTP application composed from it ([`App`]), for
//! the `respan-gateway` binary and any host that embeds it.
//!
//! ```no_run
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! use respan_gateway::{App, Config};
//! let app = App::new(Config::discover(None)?)?;
//! let listener = tokio::net::TcpListener::bind(app.listen()).await?;
//! println!("{}", app.banner(listener.local_addr()?));
//! app.serve(listener, respan_gateway::shutdown_signal()).await?;
//! # Ok(()) }
//! ```
//!
//! A custom gateway, with plugins of its own, builds its own
//! `respan_gateway_core::Gateway` instead.

mod admin;
mod app;
mod auth;
mod cli;
mod compose;
mod config;
mod formats;
mod init;
mod models;
mod presets;
mod requests;
mod routing;

pub use app::{App, DEFAULT_LISTEN, shutdown_signal};
pub use cli::run;
pub use config::{
    Auth, Config, DEFAULT_FILE, ModelRoute, PluginConfig, ProviderConfig, ProxyVendor, Source,
    no_keys_message,
};
pub use init::starter_config;

/// A configuration that cannot be read or composed: what is wrong, and
/// where.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Error {
    message: String,
}

impl Error {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for Error {}

impl From<respan_gateway_core::Fault> for Error {
    fn from(fault: respan_gateway_core::Fault) -> Self {
        Self::new(fault.message())
    }
}
