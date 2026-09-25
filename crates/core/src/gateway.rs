use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use bytes::{Bytes, BytesMut};
use futures::StreamExt;
use futures::stream::{self, BoxStream};
use http::header::{CONTENT_LENGTH, CONTENT_TYPE, HeaderValue, RETRY_AFTER, TRANSFER_ENCODING};
use http::{Method, StatusCode};

use crate::edits::{PartStream, TapFn, apply, apply_head, changes_body, lock, taps};
use crate::providers::Reply;
use crate::request::{ClientRequestInit, ProviderCloseFn};
use crate::{
    Body, ClientRequest, ClientResponse, Decoder, Doc, Encoder, End, Fault, Flow, Format, Inbound,
    Outcome, Part, Plugin, ProviderEnd, ProviderOutcome, ResponseHead, Store, Target, Transport,
    Usage,
};

pub(crate) struct Route {
    pub name: String,
    pub http: Option<(Method, String)>,
    pub format: Arc<dyn Format>,
    pub chain: Vec<Arc<dyn Plugin>>,
    /// Answers when every plugin passes.
    pub target: Option<String>,
}

pub(crate) struct Inner {
    pub routes: HashMap<String, Route>,
    pub formats: HashMap<String, Arc<dyn Format>>,
    pub targets: HashMap<String, Target>,
    pub store: Arc<dyn Store>,
    pub transport: Arc<dyn Transport>,
    pub next_id: AtomicU64,
}

impl Inner {
    pub fn format(&self, name: &str) -> Result<Arc<dyn Format>, Fault> {
        self.formats
            .get(name)
            .cloned()
            .ok_or_else(|| Fault::config(format!("unknown format '{name}'")))
    }
}

/// The gateway. Has no server inside: hand it [`Inbound`] work from an HTTP
/// adapter, an embedding program, or anything else.
#[derive(Clone)]
pub struct Gateway {
    pub(crate) inner: Arc<Inner>,
}

impl std::fmt::Debug for Gateway {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Gateway")
            .field("routes", &self.inner.routes.len())
            .field("providers", &self.inner.targets.len())
            .finish()
    }
}

impl Gateway {
    /// The route an HTTP adapter should use for `method` and `path`.
    pub fn http_route(&self, method: &Method, path: &str) -> Option<&str> {
        self.inner.routes.values().find_map(|r| match &r.http {
            Some((m, p)) if m == method && p == path => Some(r.name.as_str()),
            _ => None,
        })
    }

    pub async fn handle(&self, inbound: Inbound) -> http::Response<Body> {
        let inner = &self.inner;
        let Some(route) = inner.routes.get(&inbound.route) else {
            return plain_error(StatusCode::NOT_FOUND, "no such route");
        };
        let body = Doc::new(inbound.body);
        let n = inner.next_id.fetch_add(1, Ordering::Relaxed);
        let client = Arc::new(ClientRequest::new(ClientRequestInit {
            id: format!("req_{n:016x}"),
            route: route.name.clone(),
            format: route.format.name(),
            envelope: route.format.envelope(&body, &inbound.ext),
            body,
            ext: inbound.ext,
            store: inner.store.clone(),
            gateway: self.inner.clone(),
        }));
        let tap: Tap = Arc::default();
        let mut fin = Finalizer {
            client: client.clone(),
            tap: tap.clone(),
            committed: None,
            closed: false,
        };

        match run(route, &client).await {
            Ok(resp) => commit(route, &client, resp, fin, tap),
            Err(f) => {
                fin.close(Outcome::Refused(f.clone()));
                fault_response(route.format.as_ref(), &f)
            }
        }
    }
}

async fn run(route: &Route, client: &ClientRequest) -> Result<ClientResponse, Fault> {
    for (i, plugin) in route.chain.iter().enumerate() {
        client.set_position(i);
        if let Flow::Respond(resp) = plugin.on_client_request(client).await? {
            return Ok(*resp);
        }
    }
    client.set_position(route.chain.len().saturating_sub(1));
    match &route.target {
        Some(target) => client.call(target).await.map(Into::into),
        None => Err(Fault::config(format!(
            "route '{}': no plugin responded and the route has no target",
            route.name
        ))),
    }
}

fn commit(
    route: &Route,
    client: &ClientRequest,
    resp: ClientResponse,
    mut fin: Finalizer,
    tap: Tap,
) -> http::Response<Body> {
    let to_client = client.to_client().take();
    let client_format = route.format.clone();

    let (mut head, source, provider_format, mut edits) = match resp.0 {
        Reply::Provider(pr) => {
            let c = pr.commit();
            fin.committed = Some(CommittedProvider {
                provider: c.provider,
                status: c.head.status.as_u16(),
                on_close: c.on_close,
            });
            let decoder = c.format.decoder(&c.head);
            (
                c.head,
                BodySource::Bytes(c.body, decoder),
                Some(c.format),
                c.from_provider,
            )
        }
        Reply::Synthetic { head, parts } => (head, BodySource::Parts(parts), None, Vec::new()),
    };
    edits.extend(to_client);
    apply_head(&mut edits, &mut head);
    let in_stream = is_event_stream(&head);
    let relay = !edits.iter().any(changes_body)
        && provider_format.is_some_and(|f| f.name() == client_format.name());

    let body = match source {
        BodySource::Bytes(bytes, decoder) if relay => splice(
            bytes,
            decoder,
            taps(edits),
            client_format,
            in_stream,
            fin,
            tap,
        ),
        BodySource::Bytes(bytes, decoder) => {
            let parts = decode(bytes, decoder, tap);
            encode(apply(parts, edits), client_format.encoder(&head), fin)
        }
        BodySource::Parts(parts) => {
            let parts = observe(stream::iter(parts).boxed(), tap);
            encode(apply(parts, edits), client_format.encoder(&head), fin)
        }
    };

    head.headers.remove(CONTENT_LENGTH);
    head.headers.remove(TRANSFER_ENCODING);
    let mut out = http::Response::new(body);
    *out.status_mut() = head.status;
    *out.headers_mut() = head.headers;
    out
}

enum BodySource {
    Bytes(Body, Box<dyn Decoder>),
    Parts(Vec<Part>),
}

fn is_event_stream(head: &ResponseHead) -> bool {
    head.content_type()
        .is_some_and(|c| c.starts_with("text/event-stream"))
}

type Tap = Arc<Mutex<TapState>>;

/// What the kernel itself reads off the committed stream: the provider's
/// usage and how the stream ended.
#[derive(Default)]
struct TapState {
    usage: Usage,
    end: Option<End>,
}

fn record_parts(tap: &Tap, parts: &[Part]) {
    let mut t = lock(tap);
    for p in parts {
        match p {
            Part::Usage(u) => t.usage.merge(u),
            Part::End(e) if t.end.is_none() => t.end = Some(e.clone()),
            _ => {}
        }
    }
}

/// Relay bytes untouched, decoding a copy for the kernel and for taps.
fn splice(
    bytes: Body,
    decoder: Box<dyn Decoder>,
    taps: Vec<TapFn>,
    format: Arc<dyn Format>,
    in_stream: bool,
    fin: Finalizer,
    tap: Tap,
) -> Body {
    struct State {
        bytes: Body,
        decoder: Box<dyn Decoder>,
        taps: Vec<TapFn>,
        format: Arc<dyn Format>,
        in_stream: bool,
        fin: Finalizer,
        tap: Tap,
        done: bool,
    }
    impl State {
        fn seen(&mut self, parts: &[Part]) {
            record_parts(&self.tap, parts);
            for f in &mut self.taps {
                for p in parts {
                    f(p);
                }
            }
        }
    }
    let state = State {
        bytes,
        decoder,
        taps,
        format,
        in_stream,
        fin,
        tap,
        done: false,
    };
    stream::unfold(state, |mut s| async move {
        if s.done {
            return None;
        }
        let mut seen = Vec::new();
        match s.bytes.next().await {
            Some(Ok(chunk)) => {
                s.decoder.decode(chunk.clone(), &mut seen);
                s.seen(&seen);
                Some((Ok(chunk), s))
            }
            Some(Err(f)) => {
                s.done = true;
                s.seen(&[Part::End(End::Failed(f.clone()))]);
                s.fin.close_from_tap();
                let item = if s.in_stream {
                    Ok(s.format.fault(&f, true))
                } else {
                    Err(f)
                };
                Some((item, s))
            }
            None => {
                s.done = true;
                s.decoder.finish(&mut seen);
                if !seen.iter().any(|p| matches!(p, Part::End(_))) && lock(&s.tap).end.is_none() {
                    seen.push(Part::End(End::Failed(Fault::truncated())));
                }
                s.seen(&seen);
                let end = lock(&s.tap).end.clone();
                s.fin.close_from_tap();
                match end {
                    Some(End::Failed(f)) if s.in_stream => Some((Ok(s.format.fault(&f, true)), s)),
                    _ => None,
                }
            }
        }
    })
    .boxed()
}

fn decode(bytes: Body, decoder: Box<dyn Decoder>, tap: Tap) -> PartStream {
    struct State {
        bytes: Body,
        decoder: Box<dyn Decoder>,
        queue: VecDeque<Part>,
        tap: Tap,
        finished: bool,
    }
    let state = State {
        bytes,
        decoder,
        queue: VecDeque::new(),
        tap,
        finished: false,
    };
    stream::unfold(state, |mut s| async move {
        loop {
            if let Some(p) = s.queue.pop_front() {
                return Some((p, s));
            }
            if s.finished {
                return None;
            }
            let mut out = Vec::new();
            match s.bytes.next().await {
                Some(Ok(chunk)) => s.decoder.decode(chunk, &mut out),
                Some(Err(f)) => {
                    s.finished = true;
                    out.push(Part::End(End::Failed(f)));
                }
                None => {
                    s.finished = true;
                    s.decoder.finish(&mut out);
                }
            }
            record_parts(&s.tap, &out);
            s.queue.extend(out);
        }
    })
    .boxed()
}

fn observe(parts: PartStream, tap: Tap) -> PartStream {
    parts
        .map(move |p| {
            record_parts(&tap, std::slice::from_ref(&p));
            p
        })
        .boxed()
}

fn encode(parts: PartStream, encoder: Box<dyn Encoder>, fin: Finalizer) -> Body {
    struct State {
        parts: BoxStream<'static, Part>,
        encoder: Box<dyn Encoder>,
        fin: Finalizer,
        done: bool,
    }
    let state = State {
        parts,
        encoder,
        fin,
        done: false,
    };
    stream::unfold(state, |mut s| async move {
        loop {
            if s.done {
                return None;
            }
            let mut buf = BytesMut::new();
            match s.parts.next().await {
                Some(p) => s.encoder.encode(&p, &mut buf),
                None => {
                    s.done = true;
                    s.encoder.finish(&mut buf);
                    s.fin.close_from_tap();
                }
            }
            if !buf.is_empty() {
                return Some((Ok(buf.freeze()), s));
            }
        }
    })
    .boxed()
}

struct CommittedProvider {
    provider: String,
    status: u16,
    on_close: Vec<ProviderCloseFn>,
}

/// Runs the close hooks exactly once: when the response finishes, when the
/// request is refused, or when the client goes away (drop). The committed
/// provider request closes first, then the client request.
struct Finalizer {
    client: Arc<ClientRequest>,
    tap: Tap,
    committed: Option<CommittedProvider>,
    closed: bool,
}

impl Finalizer {
    fn close_from_tap(&mut self) {
        let outcome = match lock(&self.tap).end.clone() {
            Some(End::Done) => Outcome::Done,
            Some(End::Failed(f)) => Outcome::Failed(f),
            None => Outcome::Failed(Fault::truncated()),
        };
        self.close(outcome);
    }

    fn close(&mut self, outcome: Outcome) {
        if self.closed {
            return;
        }
        self.closed = true;
        if let Some(c) = self.committed.take() {
            let end = match outcome {
                Outcome::ClientGone => ProviderEnd::Abandoned,
                Outcome::Done => ProviderEnd::Committed,
                _ => ProviderEnd::Failed,
            };
            let po = ProviderOutcome {
                provider: c.provider,
                status: Some(c.status),
                end,
                usage: std::mem::take(&mut lock(&self.tap).usage),
            };
            for f in c.on_close {
                f(&po);
            }
        }
        for f in self.client.take_close_fns() {
            f(&self.client, &outcome);
        }
    }
}

impl Drop for Finalizer {
    fn drop(&mut self) {
        self.close(Outcome::ClientGone);
    }
}

fn fault_response(format: &dyn Format, f: &Fault) -> http::Response<Body> {
    let mut resp = http::Response::new(once(format.fault(f, false)));
    *resp.status_mut() = f.status();
    resp.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static(format.fault_content_type()),
    );
    if let Some(retry_after) = f.retry_after()
        && let Ok(v) = HeaderValue::from_str(&retry_after.as_secs().max(1).to_string())
    {
        resp.headers_mut().insert(RETRY_AFTER, v);
    }
    resp
}

/// For errors outside any route, where no Format is known.
pub fn plain_error(status: StatusCode, message: &str) -> http::Response<Body> {
    let body = serde_json::json!({ "error": { "message": message } }).to_string();
    let mut resp = http::Response::new(once(Bytes::from(body)));
    *resp.status_mut() = status;
    resp.headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    resp
}

fn once(b: Bytes) -> Body {
    stream::once(async move { Ok(b) }).boxed()
}
