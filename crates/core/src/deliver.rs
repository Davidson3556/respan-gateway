use std::collections::VecDeque;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use futures::FutureExt;
use futures::stream::{self, TryStreamExt};
use http::header::{
    CONNECTION, CONTENT_LENGTH, CONTENT_TYPE, HeaderName, PROXY_AUTHENTICATE, PROXY_AUTHORIZATION,
    RETRY_AFTER, TE, TRAILER, TRANSFER_ENCODING, UPGRADE,
};
use http::{HeaderValue, StatusCode};
use http_body::Frame;
use http_body_util::{BodyExt, StreamBody};

use crate::fault::{PANICKED, guarded};
use crate::format::{AnyEncoder, AnyFormat, Values};
use crate::lifecycle::EndHooks;
use crate::plugin::failed;
use crate::response::{Batch, Source, bodiless, head};
use crate::transport::one_frame;
use crate::{Body, ClientContext, Fault, Response};

/// A refusal: a response that fails at once, in the client's format, with
/// its fault's status.
pub(crate) async fn refuse(
    format: &Arc<dyn AnyFormat>,
    fault: Fault,
    hooks: EndHooks<ClientContext>,
) -> http::Response<Body> {
    let status = fault.status();
    deliver(format, Response::made(status, Err(fault)), hooks).await
}

/// Send `resp` to the client. Its head goes out with its body's first byte,
/// so a success that fails before any is refused instead; a refusal its
/// format cannot write goes as text/plain. A response with no body, or to
/// an upgrading request, goes out at once. Head edits apply to the outgoing
/// head, which the `Outcome` gets.
pub(crate) async fn deliver(
    format: &Arc<dyn AnyFormat>,
    resp: Response,
    mut hooks: EndHooks<ClientContext>,
) -> http::Response<Body> {
    let cx = hooks.cx();
    if let Some(c) = resp.body.call() {
        // A call's response goes only to its own request.
        if !cx.open.holds(c) {
            drop(resp);
            let fault = Fault::internal("a response to another request was sent");
            return Box::pin(refuse(format, fault, hooks)).await;
        }
    }
    // A response of another kind fails, even with no outputs.
    if resp.output.is_some_and(|output| output != format.kind().1) {
        drop(resp);
        let fault = Fault::another_kind("a response of another kind was sent".into());
        return Box::pin(refuse(format, fault, hooks)).await;
    }
    let request = cx.request();
    let upgrading = cx.later.is_some();
    // Only a refusal has no outputs' type.
    let refusal = resp.output.is_none();
    let Response {
        mut head,
        body,
        ahead,
        usage,
        ..
    } = resp;
    strip_named(&mut head.headers);
    // Of another format's head, only the response context that stays true
    // across a translation (RFC 9110 10.2) is kept: `Retry-After`.
    if !body.relayed() {
        let retry = head.headers.remove(RETRY_AFTER);
        head.headers.clear();
        head.headers.extend(retry.map(|v| (RETRY_AFTER, v)));
    }
    let build = || {
        // Judged before the encoder or head edits touch the head.
        let refusable = !refusal && head.status.is_success();
        let mut encoder = format.response_encoder(&mut head, &request);
        let mut opening = BytesMut::new();
        if !refusal {
            encoder.open(&mut opening);
        }
        (encoder, opening, refusable)
    };
    // A format that panics building its encoder cannot write a refusal.
    let Ok((encoder, opening, refusable)) = std::panic::catch_unwind(AssertUnwindSafe(build))
    else {
        return unwritable(hooks, Fault::internal(PANICKED));
    };
    // Its body, if any, is never read; a refusal is read for its fault.
    let to_head = hooks.cx().input.head().method == http::Method::HEAD;
    // An upgrading request's client waits for the head before it sends.
    let now = bodiless(head.status, to_head) || upgrading;
    let mut pipe = Pipe {
        source: body,
        refusal,
        ahead,
        format: format.clone(),
        encoder,
        hooks,
        out: Out {
            upgrading,
            ..Out::default()
        },
        ended: None,
        abort: None,
    };
    pipe.out.push(opening);
    // The usage the response was given counts first.
    pipe.hooks.usage += &usage;
    let first = match refusal || !now {
        true => pipe.first(refusable).await,
        false => Ok(()),
    };
    // An edit that fails or panics, or a 101 unasked (RFC 9110 15.2.2),
    // fails the head before it goes out.
    let first = first.and_then(|()| {
        let edits = std::mem::take(&mut pipe.hooks.cx().edits().head);
        guarded(|| edits.into_iter().rev().try_for_each(|edit| edit(&mut head)))?;
        match head.status == StatusCode::SWITCHING_PROTOCOLS && !upgrading {
            true => Err(Fault::internal(
                "a 101 to a request that did not ask to switch protocols",
            )),
            false => Ok(()),
        }
    });
    match first {
        Ok(()) => {}
        Err(fault) if refusal => return unwritable(pipe.hooks, fault),
        Err(fault) => {
            pipe.source.end(Err(fault.clone()));
            return Box::pin(refuse(format, fault, pipe.hooks)).await;
        }
    }
    let head = outgoing(head);
    pipe.hooks.head = Some(head.clone());
    // With no bytes, it ends as its head goes out.
    if pipe.out.is_empty() {
        pipe.tell_end();
    }
    // A call's body is read to its end unwritten, as its usage counts; a
    // plugin's own, which may never end, ends with the head.
    if bodiless(head.status, to_head) {
        match pipe.source.call() {
            Some(_) => {
                let runtime = pipe.hooks.cx().runtime.clone();
                drop(runtime.spawn(async move { while pipe.next().await.is_some() {} }));
            }
            None => {
                pipe.tell_end();
                pipe.hooks.finish(Ok(()));
            }
        }
        return http::Response::from_parts(head, one_frame(Bytes::new()));
    }
    let body = stream::unfold(pipe, |mut p| async move { p.next().await.map(|b| (b, p)) });
    let body = StreamBody::new(body.map_ok(Frame::data)).boxed_unsync();
    http::Response::from_parts(head, body)
}

/// Turns the response sent to the client into its body.
struct Pipe {
    source: Source,
    /// A refusal carries its fault alone; filters end with the request.
    refusal: bool,
    /// Batches to write before the source's: what [`Response::ready`] read.
    ahead: VecDeque<Batch>,
    /// The client's format.
    format: Arc<dyn AnyFormat>,
    encoder: Box<dyn AnyEncoder>,
    hooks: EndHooks<ClientContext>,
    /// Bytes written and not yet handed out.
    out: Out,
    /// The end once written; told as its last bytes are handed out.
    ended: Option<Result<(), Fault>>,
    /// A failure that could not be written, to abort the body with.
    abort: Option<Fault>,
}

impl Pipe {
    /// Write until the body's first bytes. `Err`, to refuse it, if it failed
    /// before any and is `refusable`, would be aborted, or panicked.
    async fn first(&mut self, refusable: bool) -> Result<(), Fault> {
        while self.out.is_empty() && self.ended.is_none() {
            let Ok(bytes_before_end) = self.step().await else {
                return Err(Fault::internal(PANICKED));
            };
            // A failure before any bytes is refused.
            if refusable
                && !bytes_before_end
                && let Some(Err(f)) = &self.ended
            {
                return Err(f.clone());
            }
            if self.out.is_empty()
                && let Some(f) = &self.abort
            {
                return Err(f.clone());
            }
        }
        Ok(())
    }

    async fn next(&mut self) -> Option<Result<Bytes, Fault>> {
        loop {
            if let Some(bytes) = self.out.pop() {
                if self.out.is_empty() {
                    self.tell_end();
                }
                return Some(Ok(bytes));
            }
            self.tell_end();
            if let Some(f) = self.abort.take() {
                return Some(Err(f));
            }
            if self.hooks.is_finished() {
                return None;
            }
            let _ = self.step().await;
        }
    }

    /// Start the end hooks, if the end is written.
    fn tell_end(&mut self) {
        if let Some(end) = self.ended.take() {
            self.hooks.finish(end);
        }
    }

    /// Write the next batch, returning whether bytes came before an end.
    /// `Err` if a decoder, filter or encoder panicked: the response ends
    /// internal and is aborted.
    async fn step(&mut self) -> Result<bool, ()> {
        match AssertUnwindSafe(self.write()).catch_unwind().await {
            Ok(bytes_before_end) => Ok(bytes_before_end),
            Err(_) => {
                let fault = Fault::internal(PANICKED);
                self.source.end(Err(fault.clone()));
                self.ended = Some(Err(fault.clone()));
                self.abort = Some(fault);
                Err(())
            }
        }
    }

    /// Write the next batch, and at the end what filters kept back and the
    /// end. Returns whether bytes came before the end.
    async fn write(&mut self) -> bool {
        let mut batch = match self.ahead.pop_front() {
            Some(batch) => batch,
            None => self.source.next().await.0,
        };
        batch.read();
        let mut end = batch.end.take();
        let (mut values, usage) = (batch.values.take(), std::mem::take(&mut batch.usage));
        // Usage no call counted counts here.
        self.hooks.usage += &usage;
        if let Some(values) = &mut values {
            let format = self.format.clone();
            let says = |v: &dyn std::any::Any| format.says(v);
            let filtered = self.hooks.cx().edits().outputs.run(values, &says);
            if let Err(f) = filtered.and(self.encode(values)) {
                end = Some(Err(f));
            }
        }
        // What filters release at a failure does not prevent its refusal.
        let bytes_before_end = !self.out.is_empty();
        if let Some(end) = end {
            self.source.end(end.clone());
            // Filters see every end, a failure too.
            let end = self.flush(end);
            self.write_end(end);
        }
        bytes_before_end
    }

    /// Write `values`, each output its own frame in an upgrading request.
    fn encode(&mut self, values: &Values) -> Result<(), Fault> {
        let out = &mut self.out;
        self.encoder.encode(values, &mut |bytes| out.push(bytes))
    }

    /// Write what the filters kept back before `end`; returns the end.
    fn flush(&mut self, end: Result<(), Fault>) -> Result<(), Fault> {
        if self.refusal {
            return end;
        }
        let format = self.format.clone();
        let says = |v: &dyn std::any::Any| format.says(v);
        let mut tail = None;
        let end = self.hooks.cx().edits().outputs.end(end, &mut tail, &says);
        match tail.map(|values| self.encode(&values)) {
            Some(Err(f)) => failed(f, &end),
            _ => end,
        }
    }

    /// Write `end`, else its encoder's fault, else abort the body.
    fn write_end(&mut self, mut end: Result<(), Fault>) {
        let mut one = BytesMut::new();
        if let Err(f) = self.encoder.end(&end, &mut one) {
            end = Err(f.clone());
            one.clear();
            if self.encoder.end(&end, &mut one).is_err() {
                one.clear();
                self.abort = Some(f);
            }
        }
        self.out.push(one);
        self.ended = Some(end);
    }
}

/// Bytes not yet handed out, as data frames: one per output to an upgrading
/// request, else one per batch.
#[derive(Default)]
struct Out {
    upgrading: bool,
    frames: VecDeque<Bytes>,
    batch: BytesMut,
}

impl Out {
    fn push(&mut self, bytes: BytesMut) {
        match self.upgrading {
            _ if bytes.is_empty() => {}
            true => self.frames.push_back(bytes.freeze()),
            false => self.batch.unsplit(bytes),
        }
    }

    fn is_empty(&self) -> bool {
        self.frames.is_empty() && self.batch.is_empty()
    }

    /// The next frame to hand out.
    fn pop(&mut self) -> Option<Bytes> {
        let batch = (!self.batch.is_empty()).then(|| self.batch.split().freeze());
        self.frames.pop_front().or(batch)
    }
}

/// A text/plain response, for what no format can write.
pub(crate) fn plain(status: StatusCode, text: String) -> http::Response<Body> {
    let mut head = head(status);
    let plain = HeaderValue::from_static("text/plain; charset=utf-8");
    head.headers.insert(CONTENT_TYPE, plain);
    http::Response::from_parts(outgoing(head), one_frame(text))
}

/// A text/plain 500 for a response its format could not write.
fn unwritable(mut hooks: EndHooks<ClientContext>, fault: Fault) -> http::Response<Body> {
    let status = StatusCode::INTERNAL_SERVER_ERROR;
    let (head, body) = plain(status, fault.message().to_owned()).into_parts();
    hooks.head = Some(head.clone());
    hooks.finish(Err(fault));
    http::Response::from_parts(head, body)
}

/// Connection headers (RFC 9110 7.6.1), the proxy's own (11.7), `Trailer`,
/// as trailers are not delivered, and the length, which reframing changes.
pub(crate) const HOP_BY_HOP: [HeaderName; 10] = [
    CONNECTION,
    CONTENT_LENGTH,
    HeaderName::from_static("keep-alive"),
    HeaderName::from_static("proxy-connection"),
    PROXY_AUTHENTICATE,
    PROXY_AUTHORIZATION,
    TE,
    TRAILER,
    TRANSFER_ENCODING,
    UPGRADE,
];

/// Remove the headers `Connection` names but `TE` and `Upgrade`.
pub(crate) fn strip_named(headers: &mut http::HeaderMap) {
    let named: Vec<HeaderName> = headers
        .get_all(CONNECTION)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .filter_map(|name| HeaderName::from_bytes(name.trim().as_bytes()).ok())
        .filter(|name| name != TE && name != UPGRADE)
        .collect();
    for name in named {
        headers.remove(name);
    }
}

/// `head` as sent to the client: no connection headers, but a 101's
/// `Upgrade` and its `Connection` option (RFC 9110 7.8).
fn outgoing(mut head: http::response::Parts) -> http::response::Parts {
    let upgrade = head.status == StatusCode::SWITCHING_PROTOCOLS;
    for name in HOP_BY_HOP.iter().filter(|&n| !upgrade || n != UPGRADE) {
        head.headers.remove(name);
    }
    if upgrade {
        let option = HeaderValue::from_static("upgrade");
        head.headers.insert(CONNECTION, option);
    }
    head.version = http::Version::default();
    head
}
