//! External HTTP bodies must stay pull-driven through the standard adapter.

#[allow(dead_code)]
#[path = "../../../../tests/support/http.rs"]
mod http_support;
use http_support::LIMIT;
use std::collections::VecDeque;
use std::marker::PhantomPinned;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};

use h::bytes::Bytes;
use h::http::{HeaderMap, Request, Response};
use h::http_body::{Body as HttpBody, Frame, SizeHint};
use h::http_body_util::BodyExt;
use respan_gateway_core::futures::{Future, Stream};
use respan_gateway_core::{EventStream, Fault};
use respan_gateway_http as h;

enum Step {
    Data(&'static [u8]),
    Wait,
    Trailers,
    Error,
}

#[derive(Default)]
struct State {
    steps: VecDeque<Step>,
    released: bool,
    waiting: Option<Waker>,
}

#[derive(Default)]
struct Probe {
    state: Mutex<State>,
    polls: AtomicUsize,
    drops: AtomicUsize,
    hint_reads: AtomicUsize,
    wakes: AtomicUsize,
}

impl Probe {
    fn release(&self) {
        let waiting = {
            let mut state = self.state.lock().expect("body state");
            state.released = true;
            state.waiting.take().expect("body registered its waker")
        };
        waiting.wake();
    }

    fn polls(&self) -> usize {
        self.polls.load(Ordering::SeqCst)
    }

    fn drops(&self) -> usize {
        self.drops.load(Ordering::SeqCst)
    }
}

impl Wake for Probe {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.wakes.fetch_add(1, Ordering::SeqCst);
    }
}

// Interior state permits a safe poll implementation without making this body Unpin.
struct External {
    probe: Arc<Probe>,
    _pinned: PhantomPinned,
}

impl External {
    fn new(steps: impl IntoIterator<Item = Step>) -> (Self, Arc<Probe>) {
        let probe = Arc::new(Probe::default());
        probe.state.lock().expect("body state").steps = steps.into_iter().collect();
        (
            Self {
                probe: probe.clone(),
                _pinned: PhantomPinned,
            },
            probe,
        )
    }
}

impl HttpBody for External {
    type Data = Bytes;
    type Error = Fault;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Fault>>> {
        let this = self.as_ref().get_ref();
        this.probe.polls.fetch_add(1, Ordering::SeqCst);
        let mut state = this.probe.state.lock().expect("body state");
        if matches!(state.steps.front(), Some(Step::Wait)) {
            if !state.released {
                state.waiting = Some(cx.waker().clone());
                return Poll::Pending;
            }
            state.steps.pop_front();
        }
        Poll::Ready(state.steps.pop_front().map(|step| match step {
            Step::Data(bytes) => Ok(Frame::data(Bytes::from_static(bytes))),
            Step::Trailers => {
                let mut trailers = HeaderMap::new();
                trailers.insert("x-trailer", "discarded".parse().expect("header"));
                Ok(Frame::trailers(trailers))
            }
            Step::Error => Err(Fault::upstream("external body broke")),
            Step::Wait => panic!("the fixture has only one wait"),
        }))
    }

    fn size_hint(&self) -> SizeHint {
        self.probe.hint_reads.fetch_add(1, Ordering::SeqCst);
        // Intentionally loose, but valid: neither an allocation request nor EOF.
        let mut hint = SizeHint::new();
        hint.set_upper(u64::MAX);
        hint
    }

    fn is_end_stream(&self) -> bool {
        // False is allowed even at EOF; consumers must still poll_frame.
        false
    }
}

impl Drop for External {
    fn drop(&mut self) {
        self.probe.state.lock().expect("body state").waiting.take();
        self.probe.drops.fetch_add(1, Ordering::SeqCst);
    }
}

fn adapt(body: External) -> h::Body {
    h::Body::stream(body.map_err(Into::into).into_data_stream())
}

fn opening(
    body: h::Body,
    decoded: Arc<AtomicUsize>,
) -> impl Future<Output = Result<EventStream<Bytes>, Fault>> {
    let (incoming, _answer) = h::ClientIo::accept(Request::new(body), LIMIT);
    let (opening, _sink) = incoming.bind(
        move |_request| {
            {
                Box::new(move |bytes: Bytes, out: &mut Vec<_>| {
                    let value: Result<_, Fault> = {
                        {
                            decoded.fetch_add(1, Ordering::SeqCst);
                            Ok(bytes)
                        }
                    };
                    out.push(value?);
                    Ok(())
                })
            }
        },
        |reply| {
            Ok(reply.write(
                Response::new(()).into_parts().0,
                Box::new(|_: &(), out: &mut Vec<Bytes>| {
                    out.push(Bytes::new());
                    Ok(())
                }),
            ))
        },
    );
    opening
}

#[test]
fn external_adapter_is_lazy_and_preserves_pending_frames_and_errors() {
    let (external, probe) = External::new([
        Step::Data(b"one"),
        Step::Wait,
        Step::Trailers,
        Step::Data(b""),
        Step::Data(b"two"),
        Step::Error,
    ]);
    let mut body = Box::pin(adapt(external));
    let waker = Waker::from(probe.clone());
    let mut cx = Context::from_waker(&waker);
    assert_eq!(probe.polls(), 0, "construction must not poll");
    assert_eq!(HttpBody::size_hint(body.as_ref().get_ref()).upper(), None);

    let first = body.as_mut().poll_next(&mut cx);
    assert!(matches!(first, Poll::Ready(Some(Ok(bytes))) if bytes == "one"));
    assert_eq!(probe.polls(), 1, "do not prefetch the next frame");
    assert!(body.as_mut().poll_next(&mut cx).is_pending());
    assert_eq!(probe.polls(), 2);
    assert_eq!(probe.wakes.load(Ordering::SeqCst), 0);
    probe.release();
    assert_eq!(probe.wakes.load(Ordering::SeqCst), 1);

    let empty = body.as_mut().poll_next(&mut cx);
    assert!(matches!(empty, Poll::Ready(Some(Ok(bytes))) if bytes.is_empty()));
    assert_eq!(probe.polls(), 4, "skip trailers, but retain empty data");
    let second = body.as_mut().poll_next(&mut cx);
    assert!(matches!(second, Poll::Ready(Some(Ok(bytes))) if bytes == "two"));
    assert_eq!(probe.polls(), 5);
    let error = body.as_mut().poll_next(&mut cx);
    assert!(matches!(error, Poll::Ready(Some(Err(fault)))
        if fault.code() == "upstream" && fault.message() == "external body broke"));
    assert_eq!(probe.polls(), 6);
    assert!(matches!(
        body.as_mut().poll_next(&mut cx),
        Poll::Ready(None)
    ));
    assert_eq!(probe.polls(), 7);
    assert_eq!(probe.hint_reads.load(Ordering::SeqCst), 0);
    assert_eq!(probe.drops(), 0);
    drop(body);
    assert_eq!(probe.drops(), 1);
}

#[test]
fn dropping_an_unpolled_adapter_drops_its_external_body_once() {
    let (external, probe) = External::new([Step::Wait]);
    let body = adapt(external);
    assert_eq!(probe.polls(), 0);
    drop(body);
    assert_eq!(probe.polls(), 0);
    assert_eq!(probe.drops(), 1);
}

#[test]
fn cancelling_pending_collection_drops_the_body_without_decoding() {
    let (external, probe) = External::new([Step::Data(b"partial"), Step::Wait]);
    let decoded = Arc::new(AtomicUsize::new(0));
    let mut read = Box::pin(opening(adapt(external), decoded.clone()));
    let waker = Waker::from(probe.clone());
    let mut cx = Context::from_waker(&waker);
    assert_eq!(probe.polls(), 0);
    assert!(read.as_mut().poll(&mut cx).is_pending());
    assert_eq!(probe.polls(), 2);
    assert_eq!(probe.drops(), 0);
    drop(read);
    assert_eq!(probe.polls(), 2, "cancellation must not drain");
    assert_eq!(probe.drops(), 1);
    assert_eq!(decoded.load(Ordering::SeqCst), 0);
}

#[test]
fn collection_resumes_on_wakeup_and_does_not_decode_a_late_wire_error() {
    let (external, probe) = External::new([
        Step::Data(b"partial"),
        Step::Wait,
        Step::Trailers,
        Step::Error,
        Step::Data(b"must not be read"),
    ]);
    let decoded = Arc::new(AtomicUsize::new(0));
    let mut read = Box::pin(opening(adapt(external), decoded.clone()));
    let waker = Waker::from(probe.clone());
    let mut cx = Context::from_waker(&waker);
    assert!(read.as_mut().poll(&mut cx).is_pending());
    probe.release();
    assert_eq!(probe.wakes.load(Ordering::SeqCst), 1);
    let result = read.as_mut().poll(&mut cx);
    assert!(matches!(result, Poll::Ready(Err(fault))
        if fault.code() == "upstream" && fault.message() == "external body broke"));
    assert_eq!(probe.polls(), 4, "never read past the wire error");
    assert_eq!(probe.drops(), 1);
    assert_eq!(decoded.load(Ordering::SeqCst), 0);
    assert_eq!(probe.hint_reads.load(Ordering::SeqCst), 0);
}

#[test]
fn loose_size_hint_and_pending_eof_do_not_change_lazy_whole_body_decoding() {
    let (external, probe) = External::new([
        Step::Data(b"one"),
        Step::Wait,
        Step::Trailers,
        Step::Data(b"two"),
    ]);
    assert_eq!(external.size_hint().upper(), Some(u64::MAX));
    let decoded = Arc::new(AtomicUsize::new(0));
    let mut read = Box::pin(opening(adapt(external), decoded.clone()));
    let waker = Waker::from(probe.clone());
    let mut cx = Context::from_waker(&waker);
    assert!(read.as_mut().poll(&mut cx).is_pending());
    probe.release();
    let Poll::Ready(Ok(mut input)) = read.as_mut().poll(&mut cx) else {
        panic!("request should open once all frames arrive");
    };
    assert_eq!(probe.polls(), 5);
    assert_eq!(probe.drops(), 1);
    assert_eq!(probe.hint_reads.load(Ordering::SeqCst), 1);
    assert!(input.is_single());
    assert_eq!(decoded.load(Ordering::SeqCst), 0);
    assert!(
        matches!(Pin::new(&mut input).poll_next(&mut cx), Poll::Ready(Some(Ok(bytes)))
        if bytes == "onetwo")
    );
    assert!(matches!(
        Pin::new(&mut input).poll_next(&mut cx),
        Poll::Ready(None)
    ));
    assert_eq!(decoded.load(Ordering::SeqCst), 1);
}
