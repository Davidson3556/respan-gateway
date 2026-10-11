//! A configured gateway and the HTTP application around it.

use std::collections::HashMap;
use std::future::{Future, IntoFuture};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, OnceLock, RwLock};
use std::time::{Duration, SystemTime};

use axum::Router;
use axum::extract::Request;
use axum::middleware::{from_fn_with_state, map_response};
use axum::response::Response;
use axum::routing::{MethodRouter, get};
use respan_gateway_core::Gateway;
use respan_gateway_core::futures::FutureExt;
use respan_gateway_core::futures::future::join_all;
use respan_gateway_http::Relay;
use respan_gateway_http::http::StatusCode;
use respan_gateway_http::http::header::CONTENT_TYPE;
use respan_gateway_media::video::LocateContent;
use respan_gateway_media_http::gemini::GeminiVeo;
use respan_gateway_plugins::Routing;
use respan_gateway_ws::Limits;
use serde_json::Value;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::time::Instant;
use tower::ServiceExt;

use crate::admin::{self, Admin};
use crate::auth::{self, ClientCredentials, ClientKeys, LocalOnly};
use crate::compose::{PASSTHROUGH, Plan, compose};
use crate::formats::{KnownFormat, formats, kind};
use crate::jobs::VideoJobs;
use crate::logging::Level;
use crate::mcp::{self, McpRoute};
use crate::models::{self, Models, Probe};
use crate::quirks::with_quirks;
use crate::report;
use crate::requests::{self, KEPT, Recorder, RequestLog};
use crate::{Config, Error, VERSION};

/// Where the gateway listens unless told.
pub const DEFAULT_LISTEN: SocketAddr =
    SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST), 8080);
/// How long open responses and end hooks get, together, after a signal,
/// unless the config's `shutdown_grace` says.
const SHUTDOWN: Duration = Duration::from_secs(30);
/// The most bytes of a client's request, and of a provider's answer held at
/// once, unless the config's `body_limit` says: 32 MiB, the largest request
/// the vendors' JSON APIs take (Anthropic's Messages API), so the gateway
/// refuses nothing a vendor would take, and an answer may be as large as a
/// request.
const BODY_LIMIT: usize = 32 << 20;

/// A gateway built from a [`Config`], and its HTTP application: the
/// formats served, `/healthz`, `/v1/models`, the request log, and the web
/// UI with its admin API.
///
/// Serve it with [`serve`](Self::serve), or hand requests to its
/// [`router`](Self::router) in process:
///
/// ```no_run
/// # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
/// use tower::ServiceExt;
/// let app = respan_gateway::App::new(respan_gateway::Config::from_env()?)?;
/// let request = axum::http::Request::get("/healthz").body(axum::body::Body::empty())?;
/// let response = app.router().oneshot(request).await?;
/// assert_eq!(response.status(), 200);
/// # Ok(()) }
/// ```
///
/// An app given a config source ([`reload_from`](Self::reload_from))
/// [`reload`](Self::reload)s: it builds the new config's gateway and swaps
/// it in for new requests, while requests and sessions open on the old one
/// finish there. Clones share one app.
#[derive(Clone)]
pub struct App {
    shared: Arc<Shared>,
    router: Router,
}

impl std::fmt::Debug for App {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let built = self.shared.current();
        f.debug_struct("App")
            .field("source", &built.config.source)
            .field("gateway", &built.gateway)
            .finish_non_exhaustive()
    }
}

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
    reload: OnceLock<Box<Reload>>,
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
struct Shared {
    process: Arc<Process>,
    current: RwLock<Arc<Built>>,
    /// The gateways swapped out, which may still have requests open.
    retired: Mutex<Vec<Gateway>>,
    loader: OnceLock<Box<Loader>>,
    /// One reload at a time.
    reloading: Mutex<()>,
}

impl Shared {
    fn current(&self) -> Arc<Built> {
        let current = self.current.read().unwrap_or_else(|e| e.into_inner());
        current.clone()
    }

    /// Every gateway that may have work open: the current and the retired.
    fn gateways(&self) -> Vec<Gateway> {
        let retired = self.retired.lock().unwrap_or_else(|e| e.into_inner());
        let mut all = vec![self.current().gateway.clone()];
        all.extend(retired.iter().cloned());
        all
    }

    fn reload(&self) -> Result<Reloaded, ReloadError> {
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
        process
            .log
            .configure(built.config.request_log.unwrap_or(KEPT), built.config.log);
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
        *process
            .reloaded_at
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = Some(SystemTime::now());
        Ok(Reloaded { restart_required })
    }
}

/// One config's gateway and application.
struct Built {
    config: Arc<Config>,
    gateway: Gateway,
    plan: Plan,
    formats: Vec<KnownFormat>,
    router: Router,
    /// Without client keys, what keeps it local.
    local: Option<Arc<LocalOnly>>,
}

impl Built {
    fn new(config: Config, process: &Arc<Process>) -> Result<Built, Error> {
        let limit = config.body_limit.unwrap_or(BODY_LIMIT);
        let sessions = Limits {
            max_message: config
                .ws_message_limit
                .unwrap_or(Limits::default().max_message),
            ..Limits::default()
        };
        let mut formats = formats(limit, sessions);
        let providers: Vec<&str> = config.providers.keys().map(String::as_str).collect();
        let mcp = mcp::servers(&config.mcp, &providers)?;
        if !mcp.is_empty() {
            formats.push(mcp::known(mcp.clone(), limit));
        }
        let (gateway, mut plan) = compose(&config, &formats)?;
        plan.mcp = mcp;
        let gateway = with_plugins(gateway, &config, &plan, &formats, &process.log);
        let keys = ClientKeys::new(plan.keys.clone());
        let relay = relay(
            &gateway,
            &plan,
            &formats,
            limit,
            sessions,
            &process.stopping,
        )
        .layer(from_fn_with_state(keys.clone(), auth::consume))
        .layer(from_fn_with_state(process.log.clone(), requests::track));
        let admin = Arc::new(Admin::new(
            process.clone(),
            config.source.clone(),
            &plan,
            &formats,
        ));
        let local =
            (!keys.required()).then(|| LocalOnly::new(config.listen.unwrap_or(DEFAULT_LISTEN)));
        let router = routes(&config, &plan, relay, admin, keys, local.clone());
        Ok(Built {
            config: Arc::new(config),
            gateway,
            plan,
            formats,
            router,
            local,
        })
    }

    fn listen(&self) -> SocketAddr {
        self.config.listen.unwrap_or(DEFAULT_LISTEN)
    }

    fn local_is_on(&self, addr: SocketAddr) {
        if let Some(local) = &self.local {
            local.serving_on(addr);
        }
    }

    /// What it serves, for a person starting it: the first line says
    /// where it listens.
    fn banner(&self, addr: SocketAddr) -> String {
        let mut lines = vec![format!("respan-gateway {VERSION} listening on {addr}")];
        let shown = match addr.ip().is_unspecified() {
            true => SocketAddr::new(DEFAULT_LISTEN.ip(), addr.port()),
            false => addr,
        };
        let base = format!("http://{shown}");
        // A label, then each value on a line of its own.
        let mut row = |label: &str, values: Vec<String>| {
            for (i, value) in values.into_iter().enumerate() {
                let label = if i == 0 { label } else { "" };
                lines.push(format!("  {label:<10} {value}"));
            }
        };
        row("config", vec![self.config.source.to_string()]);
        let upstreams = &self.plan.upstreams;
        let width = upstreams
            .iter()
            .map(|u| u.name.len())
            .max()
            .unwrap_or_default();
        let providers = upstreams.iter().map(|u| {
            let formats: Vec<&str> = u.entries.iter().map(|e| e.format.as_str()).collect();
            format!("{:<width$}  {}", u.name, formats.join(", "))
        });
        let providers: Vec<String> = providers.collect();
        match (providers.is_empty(), self.plan.mcp.is_empty()) {
            (true, true) => row("providers", vec!["none".to_owned()]),
            (true, false) => {}
            (false, _) => row("providers", providers),
        }
        let width = self.plan.mcp.iter().map(|s| s.name.len()).max();
        let servers: Vec<String> = (self.plan.mcp.iter())
            .map(|s| {
                let (name, kind, at) = (&s.name, s.transport_label(), s.endpoint());
                format!("{name:<w$}  {kind:<5}  {at}", w = width.unwrap_or_default())
            })
            .collect();
        if !servers.is_empty() {
            row("mcp", servers);
        }
        let mut endpoints: Vec<String> = self
            .plan
            .serve
            .iter()
            .filter_map(|name| self.formats.iter().find(|f| f.name == *name))
            .flat_map(|f| f.endpoints.iter().map(|e| (*e).to_owned()))
            .collect();
        if !self.config.raw_relay {
            endpoints.push("GET /v1/models".to_owned());
            if self.plan.serves("gemini-generate") {
                endpoints.push("GET /v1beta/models".to_owned());
            }
        }
        endpoints.push("GET /healthz".to_owned());
        row("endpoints", endpoints);
        if self.config.ui {
            row("web ui", vec![format!("{base}/ui/")]);
        }
        let auth = match self.plan.keys.len() {
            0 => "off: no client keys".to_owned(),
            1 => "on: 1 client key".to_owned(),
            n => format!("on: {n} client keys"),
        };
        row("auth", vec![auth]);
        if let Some(example) = self.example(&base) {
            row("try", vec![example]);
        }
        lines.join("\n")
    }

    fn open_refusal(&self, addr: SocketAddr) -> Option<String> {
        let open = self.plan.keys.is_empty() && !addr.ip().is_loopback();
        (open && !self.config.raw_relay).then(|| {
            format!(
                "refusing to listen on {addr} with no client keys: anyone who can reach it \
                 would spend your provider keys, and without keys it answers only to \
                 localhost anyway.\n  Set `client_keys:` in the config (`respan-gateway init \
                 --client-key` writes a strong one), or listen on 127.0.0.1."
            )
        })
    }

    /// A chat request to the default chat provider, as curl sends it.
    fn example(&self, base: &str) -> Option<String> {
        if !self.plan.serves("openai-chat") {
            return None;
        }
        let route = self.plan.routes.route(kind::CHAT, "openai-chat", None)?;
        let upstream = self
            .plan
            .upstreams
            .iter()
            .find(|u| u.name == route.provider)?;
        let model = upstream.preset.map_or("<model>", |p| p.example_model);
        let key = match self.plan.keys.is_empty() {
            true => "",
            false => " -H 'authorization: Bearer <key>'",
        };
        Some(format!(
            "curl {base}/v1/chat/completions{key} -H 'content-type: application/json' \
             -d '{{\"model\":\"{model}\",\"messages\":[{{\"role\":\"user\",\"content\":\"hi\"}}]}}'"
        ))
    }
}

impl App {
    /// Checks `config` and builds its gateway and application. Needs no
    /// runtime; serving does. It cannot [`reload`](Self::reload) until
    /// given a source ([`reload_from`](Self::reload_from)).
    pub fn new(config: Config) -> Result<App, Error> {
        let process = Arc::new(Process {
            log: RequestLog::new(config.request_log.unwrap_or(KEPT), config.log),
            stopping: Arc::new(watch::channel(false).0),
            listen: OnceLock::new(),
            started_at: SystemTime::now(),
            started: std::time::Instant::now(),
            reloaded_at: Mutex::new(None),
            reload: OnceLock::new(),
        });
        let built = Built::new(config, &process)?;
        let shared = Arc::new(Shared {
            process,
            current: RwLock::new(Arc::new(built)),
            retired: Mutex::default(),
            loader: OnceLock::new(),
            reloading: Mutex::new(()),
        });
        let weak = Arc::downgrade(&shared);
        let _ = shared
            .process
            .reload
            .set(Box::new(move || match weak.upgrade() {
                Some(shared) => shared.reload(),
                None => Err(ReloadError::Unavailable),
            }));
        let router = switch(shared.clone());
        Ok(App { shared, router })
    }

    /// Where [`reload`](Self::reload) reads the config from: `loader` gives
    /// the config as it is now.
    #[must_use]
    pub fn reload_from(
        self,
        loader: impl Fn() -> Result<Config, Error> + Send + Sync + 'static,
    ) -> Self {
        let _ = self.shared.loader.set(Box::new(loader));
        self
    }

    /// Reads the config again, and if it is valid builds its gateway and
    /// swaps it in for every request that starts from now on. Requests and
    /// WebSocket sessions already open finish on the gateway they started
    /// on. The request log and its totals carry over. A config that cannot
    /// be read, composed or served (it would drop the client keys under a
    /// non-loopback address) is an [`Err`], and the previous one keeps
    /// serving. What only a restart applies, the listen address, is named
    /// in [`Reloaded::restart_required`].
    pub fn reload(&self) -> Result<Reloaded, ReloadError> {
        self.shared.reload()
    }

    /// The application, to serve or to hand requests to; it follows
    /// reloads.
    pub fn router(&self) -> Router {
        self.router.clone()
    }

    /// The gateway inside it as of now, such as to await [`Gateway::idle`];
    /// a reload makes a new one.
    pub fn gateway(&self) -> Gateway {
        self.shared.current().gateway.clone()
    }

    /// The config as of now.
    pub fn config(&self) -> Arc<Config> {
        self.shared.current().config.clone()
    }

    /// Where it listens: the config's address, or `127.0.0.1:8080`.
    pub fn listen(&self) -> SocketAddr {
        self.shared.current().listen()
    }

    /// Serves on `listener` until `shutdown` resolves, then shuts down
    /// gracefully: open sessions close 1001, and open responses and then
    /// end hooks share one deadline, the config's `shutdown_grace` (30 s).
    /// Responses open at it are cut and lose their end hooks.
    pub async fn serve(
        &self,
        listener: TcpListener,
        shutdown: impl Future<Output = ()> + Send + 'static,
    ) -> std::io::Result<()> {
        let addr = listener.local_addr()?;
        let process = &self.shared.process;
        let _ = process.listen.set(addr);
        self.shared.current().local_is_on(addr);
        let stopping = process.stopping.clone();
        let shared = self.shared.clone();
        let grace = move || shared.current().config.shutdown_grace.unwrap_or(SHUTDOWN);
        let (deadline_sender, mut deadline) = tokio::sync::oneshot::channel();
        let serve = axum::serve(listener, self.router.clone())
            .with_graceful_shutdown({
                let grace = grace.clone();
                async move {
                    shutdown.await;
                    stopping.send_replace(true);
                    let _ = deadline_sender.send(Instant::now() + grace());
                }
            })
            .into_future();
        let log = self.shared.current().config.log;
        let mut serve = std::pin::pin!(serve);
        let mut at = None;
        let served = tokio::select! {
            served = &mut serve => served,
            Ok(deadline) = &mut deadline => {
                at = Some(deadline);
                match tokio::time::timeout_at(deadline, &mut serve).await {
                    Ok(served) => served,
                    Err(_) => {
                        log.say(Level::Warn, "shutdown: responses still open at the deadline are cut");
                        Ok(())
                    }
                }
            }
        };
        let at = at
            .or_else(|| deadline.try_recv().ok())
            .unwrap_or_else(|| Instant::now() + grace());
        let gateways = self.shared.gateways();
        let idle = join_all(gateways.iter().map(Gateway::idle));
        if tokio::time::timeout_at(at, idle).await.is_err() {
            log.say(
                Level::Warn,
                "shutdown: end hooks still running at the deadline were dropped",
            );
        }
        served
    }

    /// What it serves, for a person starting it: the first line says
    /// where it listens.
    pub fn banner(&self, addr: SocketAddr) -> String {
        self.shared.current().banner(addr)
    }

    /// Why it must not serve on `addr`, if it must not: a gateway with
    /// provider keys of its own, on an address other machines reach, and no
    /// client keys, lets anyone who reaches it spend them. (Its `Host`
    /// guard would also refuse every non-local name.) `proxy` is exempt: it
    /// holds no keys, each client brings its own.
    pub fn open_refusal(&self, addr: SocketAddr) -> Option<String> {
        self.shared.current().open_refusal(addr)
    }

    /// The composed configuration, as `check` prints it.
    pub fn report(&self) -> String {
        let built = self.shared.current();
        report::check(&built.config, &built.plan, &built.formats, built.listen())
    }

    /// Every provider's models, as `/v1/models` lists them (`id`, `kind`,
    /// `owned_by`), asking each provider now; and why each provider that did
    /// not list is missing.
    pub async fn models(&self) -> (Vec<Value>, Vec<String>) {
        let built = self.shared.current();
        let models = Models::new(&built.plan.upstreams, built.plan.routes.clone());
        models.collect().await
    }

    /// The report of `check --online`: one call to each provider with its
    /// key, listing its models, and how it answered; and whether every one
    /// of them did.
    pub async fn probe(&self) -> (String, bool) {
        let built = self.shared.current();
        let models = Models::new(&built.plan.upstreams, built.plan.routes.clone());
        let probes: Vec<Probe> = models.probe().await;
        (report::online(&probes), report::all_reachable(&probes))
    }
}

/// The application of whichever build is current when a request arrives.
fn switch(shared: Arc<Shared>) -> Router {
    Router::new().fallback(move |req: Request| {
        let router = shared.current().router.clone();
        async move {
            match router.oneshot(req).await {
                Ok(response) => response,
                Err(never) => match never {},
            }
        }
    })
}

/// Routing by the plan's routes, of each format.
fn routing(plan: &Plan, formats: &[KnownFormat]) -> Routing {
    formats
        .iter()
        .filter(|f| f.kind != kind::MCP)
        .fold(Routing::new(plan.routes.clone()), |routing, f| {
            routing.format(&f.name, f.kind, f.model.clone())
        })
}

/// The gateway with the binary's plugins: the request log, routing, video
/// jobs, whose credentials go where, and last the vendors' quirks.
fn with_plugins(
    gateway: Gateway,
    config: &Config,
    plan: &Plan,
    formats: &[KnownFormat],
    log: &Arc<RequestLog>,
) -> Gateway {
    let names: HashMap<String, String> = plan
        .upstreams
        .iter()
        .flat_map(|u| {
            u.entries
                .iter()
                .map(|e| (e.internal.clone(), u.name.clone()))
        })
        .chain(plan.mcp.iter().map(|s| (s.name.clone(), s.name.clone())))
        .collect();
    // Veo serves a job's content only where the job says.
    let veo = plan.upstreams.iter().flat_map(|u| &u.entries);
    let veo = veo.filter(|e| e.format == GeminiVeo::NAME);
    let locate = veo.fold(LocateContent::new(), |l, e| l.provider(&e.internal));
    let gateway = gateway
        .plugin(Recorder::new(log.clone(), formats, names))
        .plugin(routing(plan, formats))
        .plugin(McpRoute)
        .plugin(VideoJobs::new(&plan.upstreams, formats))
        .plugin(locate);
    // A raw relay sends each client's credentials itself.
    let gateway = match config.raw_relay {
        true => gateway.plugin(Relay::RAW),
        false => {
            let keyless = plan.upstreams.iter().filter(|u| !u.own_key);
            let keyless = keyless.flat_map(|u| u.entries.iter().map(|e| e.internal.clone()));
            // An MCP server gets a client's credentials only if it says so.
            let forwarded = plan.mcp.iter().filter(|s| s.forward_auth);
            let forwarded = forwarded.map(|s| s.name.clone());
            gateway.plugin(ClientCredentials::new(keyless.chain(forwarded).collect()))
        }
    };
    with_quirks(gateway, &plan.upstreams)
}

/// The router of the formats served: a WebSocket upgrade is a session if a
/// session's format is served. Sessions close 1001 once `stopping` is set.
fn relay(
    gateway: &Gateway,
    plan: &Plan,
    formats: &[KnownFormat],
    limit: usize,
    sessions: Limits,
    stopping: &Arc<watch::Sender<bool>>,
) -> Router {
    let session = |name: &String| formats.iter().any(|f| f.name == *name && f.websocket);
    let host = respan_gateway_serve::Host::new(gateway.clone())
        .limit(move |_| limit)
        .limits(sessions);
    let router = match plan.serve.iter().any(session) {
        true => {
            let stopping = stopping.clone();
            host.stop(async move {
                let _ = stopping.subscribe().wait_for(|stop| *stop).await;
            })
            .router()
        }
        false => host.router(),
    };
    match plan.serves(PASSTHROUGH) {
        true => router,
        false => router.layer(map_response(json_404)),
    }
}

/// The application: `/healthz`, `/v1/models`, the web UI, and the formats
/// served for every other request; without client keys, local alone.
fn routes(
    config: &Config,
    plan: &Plan,
    relay: Router,
    admin: Arc<Admin>,
    keys: Arc<ClientKeys>,
    local: Option<Arc<LocalOnly>>,
) -> Router {
    let mut router = Router::new().route("/healthz", get(|| async { "ok" }));
    // A raw relay lets its vendor answer, with the client's credentials.
    if !config.raw_relay {
        let models = Arc::new(Models::new(&plan.upstreams, plan.routes.clone()));
        let list = get(models::list).with_state(models.clone());
        let list = list.layer(from_fn_with_state(keys.clone(), auth::require));
        router = router.route("/v1/models", list);
        // Gemini SDKs list at `/v1beta`; a POST there is a call of the relay.
        let gemini = |route: MethodRouter<Arc<Models>>| {
            let route = route.fallback_service(relay.clone());
            let route = route.layer(from_fn_with_state(keys.clone(), auth::require));
            route.with_state(models.clone())
        };
        router = router
            .route("/v1beta/models", gemini(get(models::list_gemini)))
            .route("/v1beta/models/{*model}", gemini(get(models::get_gemini)));
    }
    if config.ui {
        router = router.merge(admin::router(admin, keys.clone()));
    }
    let router = router.fallback_service(relay);
    match local {
        Some(local) => router.layer(from_fn_with_state(local, auth::local_only)),
        None => router,
    }
}

/// The gateway's own 404, which no format writes, as JSON in OpenAI's
/// error shape. A 404 a provider sent through passthrough is the vendor's,
/// and passthrough takes every request, so it never has one.
async fn json_404(resp: Response) -> Response {
    let plain = resp
        .headers()
        .get(CONTENT_TYPE)
        .is_some_and(|t| t.as_bytes().starts_with(b"text/plain"));
    if resp.status() != StatusCode::NOT_FOUND || !plain {
        return resp;
    }
    let text = axum::body::to_bytes(resp.into_body(), 64 << 10).await;
    let text = text.map(|b| String::from_utf8_lossy(&b).into_owned());
    let message = text.unwrap_or_default();
    auth::error(StatusCode::NOT_FOUND, "not_found_error", message.trim())
}

/// Resolves on Ctrl-C, or on SIGTERM where there is one.
pub async fn shutdown_signal() {
    #[cfg(unix)]
    let terminate = async {
        use tokio::signal::unix::{SignalKind, signal};
        match signal(SignalKind::terminate()) {
            Ok(mut s) => {
                s.recv().await;
            }
            Err(_) => std::future::pending().await,
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        () = terminate => {}
    }
}
