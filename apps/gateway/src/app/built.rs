//! One config's gateway and application, and how they are composed.

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;

use axum::Router;
use axum::middleware::{from_fn_with_state, map_response};
use axum::response::Response;
use axum::routing::{MethodRouter, get};
use respan_gateway_core::Gateway;
use respan_gateway_http::http::StatusCode;
use respan_gateway_http::http::header::CONTENT_TYPE;
use respan_gateway_media::video::{LocateContent, VideoJobs};
use respan_gateway_media_http::gemini::GeminiVeo;
use respan_gateway_plugin_credentials::ClientCredentials;
use respan_gateway_plugin_fallback::{Fallback, Hedge};
use respan_gateway_plugin_mcp_aggregate::{Aggregate, Closer as AggregateCloser};
use respan_gateway_plugin_relay::Relay;
use respan_gateway_plugin_routing::Routing;
use respan_gateway_ws::Limits;
use tokio::sync::watch;

use super::{BODY_LIMIT, DEFAULT_LISTEN, Process};
use crate::admin::{self, Admin};
use crate::attempts;
use crate::auth::{self, ClientKeys, Cors, LocalOnly};
use crate::compose::{PASSTHROUGH, Plan, compose};
use crate::formats::{KnownFormat, formats, kind};
use crate::mcp::{self, McpRoute};
use crate::mcp_aggregate;
use crate::models::{self, Models};
use crate::requests::{self, Recorder, RequestLog};
use crate::tool_loop;
use crate::{Config, Error};

/// One config's gateway and application.
pub(super) struct Built {
    pub(super) config: Arc<Config>,
    pub(super) gateway: Gateway,
    pub(super) plan: Plan,
    pub(super) formats: Vec<KnownFormat>,
    pub(super) router: Router,
    /// Without client keys, what keeps it local.
    pub(super) local: Option<Arc<LocalOnly>>,
    /// Ends the tool loop's sessions with its tool servers.
    pub(super) closer: Option<tool_loop::Closer>,
    /// Ends the aggregate's sessions with its servers.
    pub(super) aggregate: Option<AggregateCloser>,
}

impl Built {
    pub(super) fn new(config: Config, process: &Arc<Process>) -> Result<Built, Error> {
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
            formats.push(mcp::known(
                mcp.clone(),
                config.plugins.mcp_aggregate.is_some(),
                limit,
            ));
        }
        let aggregate = match &config.plugins.mcp_aggregate {
            Some(a) => Some(mcp_aggregate::check(a, &mcp)?),
            None => None,
        };
        let (gateway, mut plan) = compose(&config, &formats)?;
        plan.mcp = mcp;
        if let Some(tool_loop) = &config.plugins.tool_loop {
            let serving = plan.serves(mcp::FORMAT);
            tool_loop::check(tool_loop, &plan.mcp, serving)?;
        }
        let attempts = Attempts {
            fallback: (config.plugins.fallback.as_ref())
                .map(|c| attempts::fallback(c, &plan.upstreams))
                .transpose()?,
            hedge: (config.plugins.hedge.as_ref())
                .map(|c| attempts::hedge(c, &plan.upstreams))
                .transpose()?,
        };
        let (gateway, closer, aggregate) = with_plugins(
            gateway,
            &config,
            aggregate,
            attempts,
            &plan,
            &formats,
            &process.log,
        );
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
        let admin = Arc::new(Admin::new(process.clone(), &config, &plan, &formats));
        let cors = match &config.cors {
            Some(c) => Some(Cors::new(&c.origins, keys.required()).map_err(Error::new)?),
            None => None,
        };
        let local = (!keys.required())
            .then(|| LocalOnly::new(config.listen.unwrap_or(DEFAULT_LISTEN), cors.clone()));
        let router = routes(&config, &plan, relay, admin, keys, local.clone(), cors);
        Ok(Built {
            config: Arc::new(config),
            gateway,
            plan,
            formats,
            router,
            local,
            closer,
            aggregate,
        })
    }

    pub(super) fn listen(&self) -> SocketAddr {
        self.config.listen.unwrap_or(DEFAULT_LISTEN)
    }

    pub(super) fn local_is_on(&self, addr: SocketAddr) {
        if let Some(local) = &self.local {
            local.serving_on(addr);
        }
    }

    pub(super) fn open_refusal(&self, addr: SocketAddr) -> Option<String> {
        let open = self.plan.keys.is_empty() && !addr.ip().is_loopback();
        (open && !self.config.relay).then(|| {
            format!(
                "refusing to listen on {addr} with no client keys: anyone who can reach it \
                 would spend your provider keys, and without keys it answers only to \
                 localhost anyway.\n  Set `client_keys` in the config (`respan-gateway init \
                 --client-key` writes a strong one), or listen on 127.0.0.1."
            )
        })
    }
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

/// The gateway with the binary's plugins, and what ends the sessions of the
/// tool loop and of the aggregate.
type Plugins = (Gateway, Option<tool_loop::Closer>, Option<AggregateCloser>);

/// The fallback and hedge plugins the config asks for.
struct Attempts {
    fallback: Option<Fallback>,
    hedge: Option<Hedge>,
}

/// The gateway with the binary's plugins, in a fixed order whatever the
/// config's: the request log, routing, MCP, video jobs, whose credentials go
/// where, the tool loop, then hedge and fallback. The vendors' quirks are
/// the gateway's plugins: they are their providers' own ([`compose`]).
fn with_plugins(
    gateway: Gateway,
    config: &Config,
    aggregate: Option<Vec<String>>,
    attempts: Attempts,
    plan: &Plan,
    formats: &[KnownFormat],
    log: &Arc<RequestLog>,
) -> Plugins {
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
    // The tool loop answers the request itself, so it comes after every plugin
    // that routes or edits it; what ends its sessions comes before them all.
    let tool_loop = config.plugins.tool_loop.as_ref().map(|config| {
        let flags: HashSet<String> = (plan.upstreams.iter())
            .flat_map(|u| &u.entries)
            .filter(|e| e.format == "anthropic-messages")
            .map(|e| e.internal.clone())
            .collect();
        tool_loop::plugin(config, move |provider| flags.contains(provider))
    });
    let gateway = match &tool_loop {
        Some((_, closer)) => gateway.plugin(closer.plugin()),
        None => gateway,
    };
    let aggregate = match (&config.plugins.mcp_aggregate, aggregate) {
        (Some(config), Some(servers)) => Some(mcp_aggregate::aggregate(config, servers)),
        _ => None,
    };
    let aggregate_closer = aggregate.as_ref().map(Aggregate::closer);
    let gateway = match &aggregate_closer {
        Some(closer) => gateway.plugin(closer.plugin()),
        None => gateway,
    };
    let gateway = gateway
        .plugin(Recorder::new(log.clone(), formats, names))
        .plugin(routing(plan, formats));
    // `/mcp` answers itself; `/mcp/{name}` is routed to its server.
    let gateway = match aggregate {
        Some(aggregate) => gateway.plugin(aggregate),
        None => gateway,
    };
    let gateway = gateway.plugin(McpRoute);
    let gateway = gateway.plugin(video_jobs(plan, formats)).plugin(locate);
    // A provider without a key of its own gets the client's.
    let keyless = plan.upstreams.iter().filter(|u| !u.own_key);
    let keyless = keyless.flat_map(|u| u.entries.iter().map(|e| e.internal.clone()));
    // An MCP server gets a client's credentials only if it says so.
    let forwarded = plan.mcp.iter().filter(|s| s.forward_auth);
    let forwarded = forwarded.map(|s| s.name.clone());
    let gateway = gateway.plugin(ClientCredentials::new(keyless.chain(forwarded)));
    // A relay gives its vendor's response heads and error bodies back.
    let mut gateway = match config.relay {
        true => gateway.plugin(Relay),
        false => gateway,
    };
    let closer = tool_loop.as_ref().map(|(_, closer)| closer.clone());
    if let Some((tool_loop, _)) = tool_loop {
        gateway = gateway.plugin(tool_loop.observe(requests::steps(log.clone())));
    }
    // Each answers the request itself: a request the tool loop runs is not
    // hedged or retried, and a hedged one is not retried.
    if let Some(hedge) = attempts.hedge {
        gateway = gateway.plugin(hedge);
    }
    if let Some(fallback) = attempts.fallback {
        gateway = gateway.plugin(fallback);
    }
    (gateway, closer, aggregate_closer)
}

/// Video jobs' follow-ups, to the provider of each format whose job ids
/// the format spells.
fn video_jobs(plan: &Plan, formats: &[KnownFormat]) -> VideoJobs {
    let spelling = |format: &str| {
        let known = formats.iter().find(|f| f.name == format);
        known.and_then(|f| f.job_ids)
    };
    let entries = plan.upstreams.iter();
    let entries = entries.flat_map(|u| u.entries.iter().map(move |e| (u, e)));
    entries.fold(VideoJobs::new(), |jobs, (u, e)| match spelling(&e.format) {
        Some(spelling) => jobs.provider(&u.name, &e.internal, spelling),
        None => jobs,
    })
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
    cors: Option<Arc<Cors>>,
) -> Router {
    let mut router = Router::new().route("/healthz", get(|| async { "ok" }));
    // A relay lets its vendor answer, with the client's credentials.
    if !config.relay {
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
    let router = match local {
        Some(local) => router.layer(from_fn_with_state(local, auth::local_only)),
        None => router,
    };
    // Outermost: a preflight carries no key, and no host check applies.
    match cors {
        Some(cors) => router.layer(from_fn_with_state(cors, auth::cors)),
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
