//! A configured gateway and the HTTP application around it.

use std::collections::HashMap;
use std::future::{Future, IntoFuture};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::middleware::from_fn_with_state;
use axum::routing::get;
use respan_gateway_core::Gateway;
use respan_gateway_http::axum::router_with_limit;
use respan_gateway_http::relay::Relay;
use respan_gateway_media::gemini::GeminiVeo;
use respan_gateway_media::video::LocateContent;
use respan_gateway_ws::Limits;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::time::Instant;

use crate::admin::{self, Admin};
use crate::auth::{self, ClientCredentials, ClientKeys};
use crate::compose::{PASSTHROUGH, Plan, compose};
use crate::formats::{Kind, KnownFormat, formats};
use crate::jobs::VideoJobs;
use crate::models::{self, Models};
use crate::requests::{self, Recorder, RequestLog};
use crate::routing::Routing;
use crate::{Config, Error};

/// Where the gateway listens unless told.
pub const DEFAULT_LISTEN: SocketAddr =
    SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST), 8080);
/// How long open responses and end hooks get, together, after a signal.
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
pub struct App {
    config: Config,
    gateway: Gateway,
    plan: Plan,
    formats: Vec<KnownFormat>,
    router: Router,
    admin: Arc<Admin>,
    /// Set once the server stops: open sessions then close 1001.
    stopping: Arc<watch::Sender<bool>>,
}

impl std::fmt::Debug for App {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("App")
            .field("source", &self.config.source)
            .field("gateway", &self.gateway)
            .finish_non_exhaustive()
    }
}

impl App {
    /// Checks `config` and builds its gateway and application. Needs no
    /// runtime; serving does.
    pub fn new(config: Config) -> Result<App, Error> {
        let limit = config.body_limit.unwrap_or(BODY_LIMIT);
        let formats = formats(limit);
        let (gateway, plan) = compose(&config, &formats)?;

        let log = RequestLog::new(config.log);
        let names: HashMap<String, String> = plan
            .upstreams
            .iter()
            .flat_map(|u| {
                u.entries
                    .iter()
                    .map(|e| (e.internal.clone(), u.name.clone()))
            })
            .collect();
        // Veo serves a job's content only where the job says.
        let veo = plan.upstreams.iter().flat_map(|u| &u.entries);
        let veo = veo.filter(|e| e.format == GeminiVeo::NAME);
        let locate = veo.fold(LocateContent::new(), |l, e| l.provider(&e.internal));
        let mut gateway = gateway
            .plugin(Recorder::new(log.clone(), &formats, names))
            .plugin(Routing::new(plan.routes.clone(), &formats))
            .plugin(VideoJobs::new(&plan.upstreams, &formats))
            .plugin(locate);
        // A raw relay sends each client's credentials itself.
        gateway = match config.raw_relay {
            true => gateway.plugin(Relay::RAW),
            false => {
                let keyless = plan.upstreams.iter().filter(|u| !u.own_key);
                let keyless = keyless.flat_map(|u| u.entries.iter().map(|e| e.internal.clone()));
                gateway.plugin(ClientCredentials::new(keyless.collect()))
            }
        };

        let keys = ClientKeys::new(plan.keys.clone());
        let stopping = Arc::new(watch::channel(false).0);
        let session = |name: &String| formats.iter().any(|f| f.name == *name && f.websocket);
        // A session's format served, a WebSocket upgrade is a session.
        let relay = match plan.serve.iter().any(session) {
            true => {
                let stopping = stopping.clone();
                let stop = async move {
                    let _ = stopping.subscribe().wait_for(|stop| *stop).await;
                };
                let limits = Limits::default();
                respan_gateway_ws::axum::router(gateway.clone(), move |_| limit, limits, stop)
            }
            false => router_with_limit(gateway.clone(), move |_| limit),
        };
        let relay = relay
            .layer(from_fn_with_state(keys.clone(), auth::consume))
            .layer(from_fn_with_state(log.clone(), requests::track));

        let admin = Arc::new(Admin::new(log, config.source.to_string(), &plan, &formats));
        let mut router = Router::new().route("/healthz", get(|| async { "ok" }));
        // A gateway that relays every other request lets its vendor answer.
        if !plan.serves(PASSTHROUGH) {
            let models = Arc::new(Models::new(&plan.upstreams, plan.routes.clone()));
            let list = get(models::list).with_state(models);
            let list = list.layer(from_fn_with_state(keys.clone(), auth::require));
            router = router.route("/v1/models", list);
        }
        if config.ui {
            router = router.merge(admin::router(admin.clone(), keys));
        }
        let router = router.fallback_service(relay);
        Ok(App {
            config,
            gateway,
            plan,
            formats,
            router,
            admin,
            stopping,
        })
    }

    /// The application, to serve or to hand requests to.
    pub fn router(&self) -> Router {
        self.router.clone()
    }

    /// The gateway inside it, such as to await [`Gateway::idle`].
    pub fn gateway(&self) -> &Gateway {
        &self.gateway
    }

    pub fn config(&self) -> &Config {
        &self.config
    }

    /// Where it listens: the config's address, or `127.0.0.1:8080`.
    pub fn listen(&self) -> SocketAddr {
        self.config.listen.unwrap_or(DEFAULT_LISTEN)
    }

    /// Serves on `listener` until `shutdown` resolves, then shuts down
    /// gracefully: open sessions close 1001, and open responses and then
    /// end hooks share one 30 s deadline. Responses open at it are cut and
    /// lose their end hooks.
    pub async fn serve(
        &self,
        listener: TcpListener,
        shutdown: impl Future<Output = ()> + Send + 'static,
    ) -> std::io::Result<()> {
        let _ = self.admin.listen.set(listener.local_addr()?);
        let stopping = self.stopping.clone();
        let (deadline_sender, mut deadline) = tokio::sync::oneshot::channel();
        let serve = axum::serve(listener, self.router.clone())
            .with_graceful_shutdown(async move {
                shutdown.await;
                stopping.send_replace(true);
                let _ = deadline_sender.send(Instant::now() + SHUTDOWN);
            })
            .into_future();
        let mut serve = std::pin::pin!(serve);
        let mut at = None;
        let served = tokio::select! {
            served = &mut serve => served,
            Ok(deadline) = &mut deadline => {
                at = Some(deadline);
                match tokio::time::timeout_at(deadline, &mut serve).await {
                    Ok(served) => served,
                    Err(_) => {
                        eprintln!("shutdown: responses still open at the deadline are cut");
                        Ok(())
                    }
                }
            }
        };
        let at = at
            .or_else(|| deadline.try_recv().ok())
            .unwrap_or_else(|| Instant::now() + SHUTDOWN);
        if tokio::time::timeout_at(at, self.gateway.idle())
            .await
            .is_err()
        {
            eprintln!("shutdown: end hooks still running at the deadline were dropped");
        }
        served
    }

    /// What it serves, for a person starting it: the first line says
    /// where it listens.
    pub fn banner(&self, addr: SocketAddr) -> String {
        let version = env!("CARGO_PKG_VERSION");
        let mut lines = vec![format!("respan-gateway {version} listening on {addr}")];
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
        match providers.is_empty() {
            true => row("providers", vec!["none".to_owned()]),
            false => row("providers", providers),
        }
        let mut endpoints: Vec<String> = self
            .plan
            .serve
            .iter()
            .filter_map(|name| self.formats.iter().find(|f| f.name == *name))
            .flat_map(|f| f.endpoints.iter().map(|e| (*e).to_owned()))
            .collect();
        if !self.plan.serves(PASSTHROUGH) {
            endpoints.push("GET /v1/models".to_owned());
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

    /// A warning for a gateway anyone may reach and spend through.
    pub fn warning(&self, addr: SocketAddr) -> Option<String> {
        let open = self.plan.keys.is_empty() && !addr.ip().is_loopback();
        open.then(|| {
            format!(
                "warning: listening on {addr} with no client keys: anyone who can reach it \
                 spends your provider keys; set `keys:` in the config"
            )
        })
    }

    /// A chat request to the default chat provider, as curl sends it.
    fn example(&self, base: &str) -> Option<String> {
        if !self.plan.serves("openai-chat") {
            return None;
        }
        let route = self.plan.routes.route(Kind::CHAT, "openai-chat", None)?;
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
