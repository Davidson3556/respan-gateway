use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use bytes::{Bytes, BytesMut};
use futures::StreamExt;
use futures::stream::{self, BoxStream};
use http::header::{CONTENT_LENGTH, CONTENT_TYPE, HeaderValue, RETRY_AFTER, TRANSFER_ENCODING};
use http::{Method, StatusCode};

use crate::edits::{PartStream, apply, apply_head, apply_once, is_body_edit, lock};
use crate::providers::{Attempts, Reply};
use crate::request::ClientRequestInit;
use crate::{
    AttemptEnd, Body, ClientRequest, ClientResponse, Decoder, Doc, Encoder, End, Fault, FaultKind,
    Format, Outcome, Part, Plugin, Providers, Record, RequestHead, ResponseHead, Store, Target,
    Transport, Usage,
};

pub(crate) type Sink = Box<dyn Fn(&Record) + Send + Sync>;

pub(crate) struct Route {
    pub name: String,
    pub method: Method,
    pub path: String,
    pub format: Arc<dyn Format>,
    pub chain: Vec<Arc<dyn Plugin>>,
    pub source: Source,
}

pub(crate) enum Source {
    Provider(String),
    Plugin(Arc<dyn Plugin>),
}

pub(crate) struct Inner {
    pub routes: Vec<Route>,
    pub formats: HashMap<String, Arc<dyn Format>>,
    pub targets: HashMap<String, Target>,
    pub store: Arc<dyn Store>,
    pub transport: Arc<dyn Transport>,
    pub sinks: Vec<Sink>,
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

/// The gateway. Has no server inside: feed it requests with
/// [`Gateway::handle`], from an HTTP adapter or embedded in another program.
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
    pub async fn handle(&self, req: http::Request<Bytes>) -> http::Response<Body> {
        let (parts, bytes) = req.into_parts();
        let inner = &self.inner;
        let Some(route_index) = inner
            .routes
            .iter()
            .position(|r| r.method == parts.method && r.path == parts.uri.path())
        else {
            return plain_error(StatusCode::NOT_FOUND, "no route");
        };
        let route = &inner.routes[route_index];

        let head = RequestHead {
            method: parts.method,
            uri: parts.uri,
            headers: parts.headers,
        };
        let body = Doc::new(bytes);
        let n = inner.next_id.fetch_add(1, Ordering::Relaxed);
        let client = Arc::new(ClientRequest::new(ClientRequestInit {
            id: format!("req_{n:016x}"),
            route: route.name.clone(),
            stream: route.format.is_stream(&head, &body),
            model: route.format.model(&head, &body),
            head,
            body,
            store: inner.store.clone(),
        }));
        let attempts: Attempts = Arc::default();
        let tap: Tap = Arc::default();
        let mut fin = Finalizer {
            inner: self.inner.clone(),
            client: client.clone(),
            attempts: attempts.clone(),
            tap: tap.clone(),
            committed: None,
            status: 0,
            closed: false,
        };

        match run(inner, route, &client, attempts).await {
            Ok(resp) => commit(route, &client, resp, fin, tap),
            Err(f) => {
                fin.status = f.status().as_u16();
                fin.close(Outcome::Refused(f.clone()));
                fault_response(route.format.as_ref(), &f)
            }
        }
    }
}

async fn run(
    inner: &Inner,
    route: &Route,
    client: &ClientRequest,
    attempts: Attempts,
) -> Result<ClientResponse, Fault> {
    for plugin in &route.chain {
        plugin.on_client_request(client).await?;
    }
    let edits = client.from_client().take();
    if !edits.is_empty() {
        let mut body = client.body();
        let mut part = Part::Doc(std::mem::replace(&mut *body, Doc::new(Bytes::new())));
        apply_once(&mut part, edits)?;
        if let Part::Doc(d) = part {
            *body = d;
        }
    }
    let providers = Providers {
        inner,
        route,
        client,
        attempts,
    };
    match &route.source {
        Source::Provider(name) => providers.call(name).await.map(Into::into),
        Source::Plugin(p) => p.dispatch(client, &providers).await,
    }
}

fn commit(
    route: &Route,
    client: &ClientRequest,
    resp: ClientResponse,
    mut fin: Finalizer,
    tap: Tap,
) -> http::Response<Body> {
    let mut to_client = client.to_client().take();
    let client_format = route.format.clone();

    let (mut head, source, provider_format, mut edits) = match resp.0 {
        Reply::Provider(pr) => {
            let c = pr.commit();
            fin.committed = Some(c.index);
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
    apply_head(&mut edits, &mut head);
    apply_head(&mut to_client, &mut head);
    edits.extend(to_client);
    edits.retain(is_body_edit);
    fin.status = head.status.as_u16();
    let in_stream = is_event_stream(&head);

    let body = match source {
        BodySource::Bytes(bytes, decoder)
            if edits.is_empty()
                && provider_format.is_some_and(|f| f.name() == client_format.name()) =>
        {
            splice(bytes, decoder, client_format, in_stream, fin, tap)
        }
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

/// Relay bytes untouched, decoding a copy only to read usage and the end.
fn splice(
    bytes: Body,
    decoder: Box<dyn Decoder>,
    format: Arc<dyn Format>,
    in_stream: bool,
    fin: Finalizer,
    tap: Tap,
) -> Body {
    struct State {
        bytes: Body,
        decoder: Box<dyn Decoder>,
        format: Arc<dyn Format>,
        in_stream: bool,
        fin: Finalizer,
        tap: Tap,
        done: bool,
    }
    let state = State {
        bytes,
        decoder,
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
                record_parts(&s.tap, &seen);
                Some((Ok(chunk), s))
            }
            Some(Err(f)) => {
                s.done = true;
                lock(&s.tap).end.get_or_insert(End::Failed(f.clone()));
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
                record_parts(&s.tap, &seen);
                let end = lock(&s.tap)
                    .end
                    .get_or_insert(End::Failed(Fault::truncated()))
                    .clone();
                s.fin.close_from_tap();
                match end {
                    End::Failed(f) if s.in_stream => Some((Ok(s.format.fault(&f, true)), s)),
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

/// Emits the request's Record exactly once: when its response finishes, when
/// it is refused, or when the client goes away (drop).
struct Finalizer {
    inner: Arc<Inner>,
    client: Arc<ClientRequest>,
    attempts: Attempts,
    tap: Tap,
    committed: Option<usize>,
    status: u16,
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
        let usage = std::mem::take(&mut lock(&self.tap).usage);
        let mut attempts = lock(&self.attempts).clone();
        if let Some(i) = self.committed
            && let Some(a) = attempts.get_mut(i)
        {
            a.end = AttemptEnd::Committed;
            a.usage = usage.clone();
        }
        let principal = self.client.principal();
        let record = Record {
            request_id: self.client.id.clone(),
            route: self.client.route.clone(),
            principal: principal.as_ref().map(|p| p.id.clone()),
            tenant: principal.map(|p| p.tenant),
            model: self.client.model().map(str::to_owned),
            status: self.status,
            outcome,
            attempts,
            usage,
            labels: self.client.labels(),
            duration: self.client.started.elapsed(),
        };
        for f in self.client.take_close_fns() {
            f(&record);
        }
        for sink in &self.inner.sinks {
            sink(&record);
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
    if let FaultKind::TooMany { retry_after } = &f.kind
        && let Ok(v) = HeaderValue::from_str(&retry_after.as_secs().max(1).to_string())
    {
        resp.headers_mut().insert(RETRY_AFTER, v);
    }
    resp
}

fn plain_error(status: StatusCode, message: &str) -> http::Response<Body> {
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
