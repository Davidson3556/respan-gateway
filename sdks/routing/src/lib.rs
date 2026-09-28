#![warn(missing_docs)]
//! Attempt control for respan-gateway plugins: retry with `Retry-After`
//! ([`first_success`], [`Backoff`]), a staggered race ([`race`]), a stall
//! bound on a provider's head and body ([`Watchdog`]), and each attempt's [`Role`]
//! ([`Attempt`]).
//!
//! Both helpers read each answer [`Until`] a stopping point, by default its
//! first content ([`Response::until_content`]): they return a success only
//! once it got there, and never retry after that, so a stream that fails
//! later reaches the client cut short, not replayed. A fallback or hedge is
//! a few lines of `on_client`:
//!
//! ```ignore
//! let calls = ["openai", "anthropic"].map(|p| cx.call(p));
//! let backoff = Backoff::new(3, Duration::from_millis(200), Duration::from_secs(10));
//! first_success(calls, &backoff, Until::Content).await.map(Some)
//! ```

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use respan_gateway_core::futures::future::BoxFuture;
use respan_gateway_core::futures::stream::{self, FuturesUnordered};
use respan_gateway_core::futures::{FutureExt, StreamExt};
use respan_gateway_core::http::{self, StatusCode};
use respan_gateway_core::http_body_util::{BodyExt, BodyStream, StreamBody};
use respan_gateway_core::{Body, Call, Fault, Response, Transport, async_trait, codes};

/// How often to try one call, and how long to wait between tries.
#[derive(Debug, Clone, Copy)]
pub struct Backoff {
    tries: u32,
    base: Duration,
    max: Duration,
}

impl Backoff {
    /// Up to `tries` tries per call, waiting `base`, doubled after each
    /// retry, at most `max`. A `Retry-After` is waited for as asked, or,
    /// if longer than `max`, not at all: the call is not retried.
    pub fn new(tries: u32, base: Duration, max: Duration) -> Self {
        Self { tries, base, max }
    }

    /// One try per call: no retries.
    pub fn once() -> Self {
        Self::new(1, Duration::ZERO, Duration::ZERO)
    }

    /// The wait before another try, after `tries` tries of which the last
    /// was answered with `answer` ([`Fault::is_answer`]), or with none if it
    /// failed first; `None` if it should not be retried.
    fn wait(&self, tries: u32, answer: Option<&Fault>) -> Option<Duration> {
        if tries >= self.tries {
            return None;
        }
        let doubled = 2u32.saturating_pow(tries.saturating_sub(1));
        let backoff = self.base.saturating_mul(doubled).min(self.max);
        match answer {
            None => Some(backoff),
            Some(answer) if !retryable(answer.status()) => None,
            Some(answer) => match answer.retry_after() {
                Some(asked) if asked > self.max => None,
                Some(asked) => Some(asked),
                None => Some(backoff),
            },
        }
    }
}

/// Whether another try may be answered otherwise; other client errors would
/// not. 529, "overloaded" from some providers, is as safe to retry as a 503.
fn retryable(status: StatusCode) -> bool {
    [408, 425, 429, 500, 502, 503, 504, 529].contains(&status.as_u16())
}

/// Put on every call [`first_success`] and [`race`] send
/// ([`ProviderContext::extensions`](respan_gateway_core::ProviderContext::extensions)),
/// so provider-edge hooks can tell attempts apart.
#[derive(Debug, Clone)]
pub struct Attempt {
    index: u32,
    won: Arc<AtomicBool>,
}

/// What became of an attempt.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    /// Its response is the one returned.
    Winner,
    /// Any other: failed, superseded, dropped, or not chosen yet.
    Loser,
}

impl Attempt {
    fn new(index: u32) -> Self {
        let won = Arc::new(AtomicBool::new(false));
        Self { index, won }
    }

    /// Its place among the helper's attempts, in the order sent, from 0:
    /// what tells a retry from the first try in `on_provider`.
    pub fn index(&self) -> u32 {
        self.index
    }

    /// What became of it. Every attempt is a loser until the helper returns
    /// its response, promoted before that response is read, so its
    /// `on_provider_end` always sees the final role.
    pub fn role(&self) -> Role {
        if self.won.load(Ordering::SeqCst) {
            Role::Winner
        } else {
            Role::Loser
        }
    }
}

/// How far [`first_success`] and [`race`] read an answer before they judge
/// it: a success that fails before that point is retried, or loses the
/// race; one that fails after it is committed.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Until {
    /// Its head, unread: the first call a provider answered wins, however
    /// its body goes, an error answer too, which shows only once read.
    Head,
    /// Its first content ([`Response::until_content`]): what a client
    /// streaming it would see first.
    #[default]
    Content,
    /// Its end ([`Response::until_end`]), held whole until sent: the first
    /// to finish cleanly wins. A success failing after its first content
    /// ([`Response::end`]) does not count as one.
    End,
}

/// An attempt and its answer, read [`Until`] a point.
type Answer = (Attempt, Result<Response, Fault>);

/// Send `call` as `attempt` and read it `until` a point.
async fn send(call: Call, attempt: Attempt, until: Until) -> Answer {
    let answer = match call.with_extension(attempt.clone()).await {
        Ok(resp) => match until {
            Until::Head => Ok(resp),
            Until::Content => resp.until_content().await,
            Until::End => resp.until_end().await,
        },
        Err(fault) => Err(fault),
    };
    (attempt, answer)
}

/// How `answer` failed, if it did: its fault, or, read to its end, the
/// failure a success ended with.
fn failure(answer: &Result<Response, Fault>, until: Until) -> Option<&Fault> {
    match answer {
        Err(fault) => Some(fault),
        Ok(resp) if until == Until::End => resp.end().and_then(|end| end.as_ref().err()),
        Ok(_) => None,
    }
}

fn is_success(answer: &Result<Response, Fault>, until: Until) -> bool {
    answer.is_ok() && failure(answer, until).is_none()
}

/// Return `answer`, promoting its attempt if it is a response. A fault
/// stays a loser: its call already ended, with nothing to commit.
fn win((attempt, answer): Answer) -> Result<Response, Fault> {
    if answer.is_ok() {
        attempt.won.store(true, Ordering::SeqCst);
    }
    answer
}

/// The last answer; with none, `upstream`.
fn last(answer: Option<Answer>) -> Result<Response, Fault> {
    win(answer.ok_or_else(|| Fault::upstream("no provider to try"))?)
}

/// Each call in turn, retried as `backoff` says, until one's response is a
/// success read `until` its stopping point; else the last answer, so the
/// client sees the last 429 and its `Retry-After`. A provider's error
/// answer ([`Fault::is_answer`]) is retried if its status may be answered
/// otherwise, as long as its `Retry-After` says; a call that cannot be
/// made, such as with a `config` fault, moves on at once; an unreachable
/// provider or a success failing before the point is retried; with
/// [`Until::End`], one failing anywhere before its end.
pub async fn first_success(
    calls: impl IntoIterator<Item = Call>,
    backoff: &Backoff,
    until: Until,
) -> Result<Response, Fault> {
    let (mut previous, mut index) = (None, 0);
    for call in calls {
        for tries in 1.. {
            let (attempt, answer) = send(call.clone(), Attempt::new(index), until).await;
            index += 1;
            if is_success(&answer, until) {
                return win((attempt, answer));
            }
            let wait = match failure(&answer, until) {
                Some(f) if f.is_answer() => backoff.wait(tries, Some(f)),
                Some(f) if f.code() == codes::UPSTREAM || f.code() == codes::TRUNCATED => {
                    backoff.wait(tries, None)
                }
                _ => None,
            };
            let Some(wait) = wait else {
                previous = Some((attempt, answer));
                break;
            };
            tokio::time::sleep(wait).await;
        }
    }
    last(previous)
}

/// Start each call `stagger` after the one before, or at once when every
/// call started has failed. The first success read `until` its stopping
/// point wins, with [`Until::End`] only if it ended cleanly; the others
/// are dropped, so they end `abandoned`. With none, the last answer.
pub async fn race(
    calls: impl IntoIterator<Item = Call>,
    stagger: Duration,
    until: Until,
) -> Result<Response, Fault> {
    // A century stands for "never hedge": `Duration::MAX` would overflow.
    let stagger = stagger.min(Duration::from_secs(100 * 365 * 24 * 3600));
    let mut waiting = calls.into_iter().zip(0..).peekable();
    let mut running = FuturesUnordered::<BoxFuture<'static, Answer>>::new();
    let mut previous = None;
    let next = tokio::time::sleep(Duration::ZERO);
    tokio::pin!(next);
    loop {
        tokio::select! {
            Some((attempt, answer)) = running.next() => {
                if is_success(&answer, until) {
                    return win((attempt, answer));
                }
                previous = Some((attempt, answer));
                if running.is_empty() {
                    next.as_mut().reset(tokio::time::Instant::now());
                }
            }
            () = &mut next, if waiting.peek().is_some() => {
                if let Some((call, index)) = waiting.next() {
                    running.push(send(call, Attempt::new(index), until).boxed());
                }
                next.as_mut().reset(tokio::time::Instant::now() + stagger);
            }
            else => return last(previous),
        }
    }
}

/// A transport that fails a silent provider rather than hanging: one that
/// sends no head within `head` (an `upstream` fault), or no body frame
/// within `idle` of the head or the frame before (the stream ends as an
/// error in the client's framing). Before any content, [`first_success`]
/// retries it.
///
/// A whole (non-streaming) answer sends its head only once it is done, so
/// `head` is the longest answer to wait for; `idle` bounds a stream's gaps.
#[derive(Debug, Clone)]
pub struct Watchdog<T> {
    inner: T,
    head: Duration,
    idle: Duration,
}

impl<T: Transport> Watchdog<T> {
    /// Bound `inner`'s wait for its head by `head`, and each gap in its body by `idle`.
    pub fn new(inner: T, head: Duration, idle: Duration) -> Self {
        Self { inner, head, idle }
    }
}

#[async_trait]
impl<T: Transport> Transport for Watchdog<T> {
    async fn send(&self, call: http::Request<Body>) -> Result<http::Response<Body>, Fault> {
        let stalled = |d: Duration| Fault::upstream(format!("the provider sent nothing for {d:?}"));
        let resp = tokio::time::timeout(self.head, self.inner.send(call)).await;
        let (head, body) = resp.map_err(|_| stalled(self.head))??.into_parts();
        let (frames, idle) = (Some(BodyStream::new(body)), self.idle);
        let bounded = stream::unfold(frames, move |frames| async move {
            let mut frames = frames?;
            match tokio::time::timeout(idle, frames.next()).await {
                Ok(frame) => Some((frame?, Some(frames))),
                Err(_) => Some((Err(stalled(idle)), None)),
            }
        });
        let body = StreamBody::new(bounded).boxed_unsync();
        Ok(http::Response::from_parts(head, body))
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use respan_gateway_core::Fault;
    use respan_gateway_core::http::StatusCode;

    use super::Backoff;

    fn answer(status: StatusCode, retry_after: Option<u64>) -> Fault {
        let fault = Fault::new(status, "x", "x").with_answer("f", ());
        match retry_after {
            Some(secs) => fault.with_retry_after(Duration::from_secs(secs)),
            None => fault,
        }
    }

    #[test]
    fn backoff_doubles_up_to_its_max_and_skips_answers_no_retry_changes() {
        let backoff = Backoff::new(5, Duration::from_millis(400), Duration::from_secs(1));
        let waits: Vec<_> = (1..=5).map(|n| backoff.wait(n, None)).collect();
        let ms = |n| Some(Duration::from_millis(n));
        assert_eq!(waits, [ms(400), ms(800), ms(1000), ms(1000), None]);
        let busy = answer(StatusCode::TOO_MANY_REQUESTS, Some(1));
        assert_eq!(backoff.wait(1, Some(&busy)), ms(1000));
        let too_long = answer(StatusCode::TOO_MANY_REQUESTS, Some(2));
        assert_eq!(backoff.wait(1, Some(&too_long)), None);
        let bad_request = answer(StatusCode::BAD_REQUEST, None);
        assert_eq!(backoff.wait(1, Some(&bad_request)), None);
    }
}
