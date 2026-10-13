//! Reloading: the state that outlives a config, and the swap to a new one.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex, OnceLock, RwLock};
use std::time::SystemTime;

use respan_gateway_core::Gateway;
use respan_gateway_core::futures::FutureExt;
use tokio::sync::watch;

use super::Built;
use crate::logging::Level;
use crate::requests::RequestLog;
use crate::{Config, Error};

/// What a reload took in.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reloaded {
    /// What the new config changes that only a restart applies, such as
    /// the listen address; empty when everything took effect.
    pub restart_required: Vec<String>,
}

/// Why a reload did not happen. The gateway keeps serving as it was.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReloadError {
    /// This app has no config source to read again.
    Unavailable,
    /// The config cannot be read, composed or served; why.
    Invalid(Error),
}

impl std::fmt::Display for ReloadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReloadError::Unavailable => f.write_str(
                "this gateway was not started from a config file or the environment, \
                 so there is nothing to read again",
            ),
            ReloadError::Invalid(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for ReloadError {}

type Loader = dyn Fn() -> Result<Config, Error> + Send + Sync;
type Reload = dyn Fn() -> Result<Reloaded, ReloadError> + Send + Sync;

/// What outlives a reload: the request log and totals, the clock, the
/// bound address, the signal that the server is stopping.
pub(crate) struct Process {
    pub log: Arc<RequestLog>,
    /// Set once the server stops: open sessions then close 1001, and open
    /// event streams end.
    pub stopping: Arc<watch::Sender<bool>>,
    /// Where it was bound, once served.
    pub listen: OnceLock<SocketAddr>,
    pub started_at: SystemTime,
    pub started: std::time::Instant,
    /// When the config was last reloaded.
    pub reloaded_at: Mutex<Option<SystemTime>>,
    pub(super) reload: OnceLock<Box<Reload>>,
}

impl Process {
    /// Reads the config again and swaps it in; see [`App::reload`].
    pub fn reload(&self) -> Result<Reloaded, ReloadError> {
        match self.reload.get() {
            Some(reload) => reload(),
            None => Err(ReloadError::Unavailable),
        }
    }
}

/// The app: the current build, swapped on reload, and what it needs.
pub(super) struct Shared {
    pub(super) process: Arc<Process>,
    pub(super) current: RwLock<Arc<Built>>,
    /// The gateways swapped out, which may still have requests open.
    pub(super) retired: Mutex<Vec<Gateway>>,
    pub(super) loader: OnceLock<Box<Loader>>,
    /// One reload at a time.
    pub(super) reloading: Mutex<()>,
}

impl Shared {
    pub(super) fn current(&self) -> Arc<Built> {
        let current = self.current.read().unwrap_or_else(|e| e.into_inner());
        current.clone()
    }

    /// Every gateway that may have work open: the current and the retired.
    pub(super) fn gateways(&self) -> Vec<Gateway> {
        let retired = self.retired.lock().unwrap_or_else(|e| e.into_inner());
        let mut all = vec![self.current().gateway.clone()];
        all.extend(retired.iter().cloned());
        all
    }

    pub(super) fn reload(&self) -> Result<Reloaded, ReloadError> {
        let loader = self.loader.get().ok_or(ReloadError::Unavailable)?;
        let _one_at_a_time = self.reloading.lock().unwrap_or_else(|e| e.into_inner());
        let log = self.current().config.log;
        let done = loader().map_err(ReloadError::Invalid).and_then(|config| {
            let built = Built::new(config, &self.process).map_err(ReloadError::Invalid)?;
            self.swap_in(built)
        });
        match &done {
            Ok(reloaded) => {
                log.say(Level::Info, "config reloaded");
                for note in &reloaded.restart_required {
                    log.say(Level::Warn, &format!("reload: {note}"));
                }
            }
            Err(e) => log.say(
                Level::Warn,
                &format!("reload failed, still serving the previous config: {e}"),
            ),
        }
        done
    }

    fn swap_in(&self, built: Built) -> Result<Reloaded, ReloadError> {
        let process = &self.process;
        // Keys that were set may not be dropped under an address beyond
        // loopback, as at start.
        if let Some(refusal) = process.listen.get().and_then(|a| built.open_refusal(*a)) {
            return Err(ReloadError::Invalid(Error::new(refusal)));
        }
        if let Some(addr) = process.listen.get() {
            built.local_is_on(*addr);
        }
        let old = self.current();
        let mut restart_required = Vec::new();
        if let Some(bound) = process.listen.get()
            && built.listen() != old.listen()
        {
            restart_required.push(format!(
                "the config now says listen {}, but the gateway stays on {bound}; \
                 restart it to move",
                built.listen()
            ));
        }
        process.log.configure(&built.config);
        let built = Arc::new(built);
        let previous = {
            let mut current = self.current.write().unwrap_or_else(|e| e.into_inner());
            std::mem::replace(&mut *current, built)
        };
        {
            let mut retired = self.retired.lock().unwrap_or_else(|e| e.into_inner());
            retired.retain(|g| g.idle().now_or_never().is_none());
            retired.push(previous.gateway.clone());
        }
        // Its tool loop's and its aggregate's sessions end once the requests on
        // it do.
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            let (tool_loop, aggregate) = (previous.closer.clone(), previous.aggregate.clone());
            let gateway = previous.gateway.clone();
            if tool_loop.is_some() || aggregate.is_some() {
                runtime.spawn(async move {
                    gateway.idle().await;
                    if let Some(closer) = tool_loop {
                        closer.close(&gateway).await;
                    }
                    if let Some(closer) = aggregate {
                        closer.close(&gateway).await;
                    }
                });
            }
        }
        *process
            .reloaded_at
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = Some(SystemTime::now());
        Ok(Reloaded { restart_required })
    }
}
