#![warn(missing_docs)]
//! Attempt control for respan-gateway plugins: retry with `Retry-After`
//! ([`first_success`], [`Backoff`]), a staggered race ([`race`]), a stall
//! bound on a provider's head and body ([`Watchdog`]), and each attempt's [`Role`]
//! ([`Attempt`]).
//!
//! [`Response::ready`] is the commit point: both helpers return a success
//! only once its first content arrived, and never retry after that, so a
//! stream that fails later reaches the client cut short, not replayed.
//! A fallback or hedge is a few lines of `on_client`:
//!
//! ```ignore
//! let calls = ["openai", "anthropic"].map(|p| cx.call(p));
//! let backoff = Backoff::new(3, Duration::from_millis(200), Duration::from_secs(10));
//! first_success(calls, &backoff).await.map(Some)
//! ```

use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::Duration;

use respan_gateway_core::futures::future::BoxFuture;
use respan_gateway_core::futures::stream::{self, FuturesUnordered};
use respan_gateway_core::futures::{FutureExt, StreamExt};
use respan_gateway_core::http::{self, HeaderMap, StatusCode, header::RETRY_AFTER};
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
    /// was answered with `head`, or with none if it failed first; `None`
    /// if it should not be retried.
    fn wait(&self, tries: u32, head: Option<&http::response::Parts>) -> Option<Duration> {
        if tries >= self.tries {
            return None;
        }
        let doubled = 2u32.saturating_pow(tries.saturating_sub(1));
        let backoff = self.base.saturating_mul(doubled).min(self.max);
        match head {
            None => Some(backoff),
            Some(head) if !retryable(head.status) => None,
            Some(head) => match retry_after(&head.headers) {
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

/// `Retry-After` in seconds. An HTTP date counts as none.
fn retry_after(headers: &HeaderMap) -> Option<Duration> {
    let value = headers.get(RETRY_AFTER)?.to_str().ok()?;
    value.trim().parse().ok().map(Duration::from_secs)
}

/// Put on every call [`first_success`] and [`race`] send
/// ([`ProviderContext::extensions`](respan_gateway_core::ProviderContext::extensions)),
/// so provider-edge hooks can tell attempts apart.
#[derive(Debug, Clone)]
pub struct Attempt {
    index: u32,
    role: Arc<AtomicU8>,
}

/// What became of an attempt.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    /// Not decided yet. An end hook sees it only when the client's request
    /// ended before the helper decided.
    Pending,
    /// Its response is the one returned.
    Winner,
    /// Any other: failed, superseded, or dropped as a race's loser.
    Loser,
}

impl Attempt {
    fn new(index: u32) -> Self {
        let role = Arc::new(AtomicU8::new(Role::Pending as u8));
        Self { index, role }
    }

    /// Its place among the helper's attempts, in the order sent, from 0.
    pub fn index(&self) -> u32 {
        self.index
    }

    /// What became of it. Set as the helper decides, before it drops a
    /// loser or returns the winner, so its `on_provider_end` sees it, even
    /// for a whole body.
    pub fn role(&self) -> Role {
        match self.role.load(Ordering::SeqCst) {
            1 => Role::Winner,
            2 => Role::Loser,
            _ => Role::Pending,
        }
    }

    fn set(&self, role: Role) {
        self.role.store(role as u8, Ordering::SeqCst);
    }
}

/// An attempt and its answer, read to its first content.
type Answer = (Attempt, Result<Response, Fault>);

/// Send `call` as `attempt` and read it to its first content.
async fn send(call: Call, attempt: Attempt) -> Answer {
    let answer = match call.with_extension(attempt.clone()).await {
        Ok(resp) => resp.ready().await,
        Err(fault) => Err(fault),
    };
    (attempt, answer)
}

fn is_success(answer: &Result<Response, Fault>) -> bool {
    answer.as_ref().is_ok_and(|r| r.head().status.is_success())
}

/// Drop a superseded answer, a loser.
fn lose(answer: Option<Answer>) {
    if let Some((attempt, _)) = answer {
        attempt.set(Role::Loser);
    }
}

/// The last answer, the winner; with none, `upstream`.
fn last(answer: Option<Answer>) -> Result<Response, Fault> {
    let (attempt, answer) = answer.ok_or_else(|| Fault::upstream("no provider to try"))?;
    attempt.set(Role::Winner);
    answer
}

/// Each call in turn, retried as `backoff` says, until one's response is a
/// success with content ([`Response::ready`]); else the last answer, so the
/// client sees the last 429 and its `Retry-After`. A call that is refused
/// or cannot be made, such as with a `config` fault, moves on at once; an
/// unreachable provider or a success failing before content is retried.
pub async fn first_success(
    calls: impl IntoIterator<Item = Call>,
    backoff: &Backoff,
) -> Result<Response, Fault> {
    let (mut previous, mut index) = (None, 0);
    for call in calls {
        for tries in 1.. {
            lose(previous.take());
            let (attempt, answer) = send(call.clone(), Attempt::new(index)).await;
            index += 1;
            if is_success(&answer) {
                attempt.set(Role::Winner);
                return answer;
            }
            let wait = match &answer {
                Ok(resp) => backoff.wait(tries, Some(resp.head())),
                Err(f) if f.code() == codes::UPSTREAM || f.code() == codes::TRUNCATED => {
                    backoff.wait(tries, None)
                }
                Err(_) => None,
            };
            let Some(wait) = wait else {
                previous = Some((attempt, answer));
                break;
            };
            attempt.set(Role::Loser);
            tokio::time::sleep(wait).await;
        }
    }
    last(previous)
}

/// Start each call `stagger` after the one before, or at once when every
/// call started has failed. The first success with content wins; the
/// others are dropped, so they end `abandoned`. With none, the last answer.
pub async fn race(
    calls: impl IntoIterator<Item = Call>,
    stagger: Duration,
) -> Result<Response, Fault> {
    let mut waiting = calls.into_iter().zip(0..).peekable();
    let mut running = FuturesUnordered::<BoxFuture<'static, Answer>>::new();
    let mut started: Vec<Attempt> = Vec::new();
    let mut previous = None;
    let next = tokio::time::sleep(Duration::ZERO);
    tokio::pin!(next);
    loop {
        tokio::select! {
            Some((attempt, answer)) = running.next() => {
                if is_success(&answer) {
                    started.iter().for_each(|other| other.set(Role::Loser));
                    attempt.set(Role::Winner);
                    return answer;
                }
                lose(previous.replace((attempt, answer)));
                if running.is_empty() {
                    next.as_mut().reset(tokio::time::Instant::now());
                }
            }
            () = &mut next, if waiting.peek().is_some() => {
                if let Some((call, index)) = waiting.next() {
                    let attempt = Attempt::new(index);
                    started.push(attempt.clone());
                    running.push(send(call, attempt).boxed());
                }
                next.as_mut().reset(tokio::time::Instant::now() + stagger);
            }
            else => return last(previous),
        }
    }
}

/// A transport that fails a provider silent for `idle`: before its head
/// (an `upstream` fault) or before or between body frames (the stream ends
/// as an error in the client's framing), rather than hanging. Before any
/// content, [`first_success`] retries it.
#[derive(Debug, Clone)]
pub struct Watchdog<T> {
    inner: T,
    idle: Duration,
}

impl<T: Transport> Watchdog<T> {
    /// Bound `inner`'s silences by `idle`.
    pub fn new(inner: T, idle: Duration) -> Self {
        Self { inner, idle }
    }
}

#[async_trait]
impl<T: Transport> Transport for Watchdog<T> {
    async fn send(&self, call: http::Request<Body>) -> Result<http::Response<Body>, Fault> {
        let idle = self.idle;
        let stalled = move || Fault::upstream(format!("the provider sent nothing for {idle:?}"));
        let resp = tokio::time::timeout(idle, self.inner.send(call)).await;
        let (head, body) = resp.map_err(|_| stalled())??.into_parts();
        let frames = Some(BodyStream::new(body));
        let bounded = stream::unfold(frames, move |frames| async move {
            let mut frames = frames?;
            match tokio::time::timeout(idle, frames.next()).await {
                Ok(frame) => Some((frame?, Some(frames))),
                Err(_) => Some((Err(stalled()), None)),
            }
        });
        let body = StreamBody::new(bounded).boxed_unsync();
        Ok(http::Response::from_parts(head, body))
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use respan_gateway_core::http::{self, StatusCode, header::RETRY_AFTER};

    use super::Backoff;

    fn head(status: StatusCode, retry_after: Option<&'static str>) -> http::response::Parts {
        let mut resp = http::Response::new(());
        *resp.status_mut() = status;
        if let Some(value) = retry_after {
            let value = http::HeaderValue::from_static(value);
            resp.headers_mut().insert(RETRY_AFTER, value);
        }
        resp.into_parts().0
    }

    #[test]
    fn backoff_doubles_up_to_its_max_and_skips_answers_no_retry_changes() {
        let backoff = Backoff::new(5, Duration::from_millis(400), Duration::from_secs(1));
        let waits: Vec<_> = (1..=5).map(|n| backoff.wait(n, None)).collect();
        let ms = |n| Some(Duration::from_millis(n));
        assert_eq!(waits, [ms(400), ms(800), ms(1000), ms(1000), None]);
        let busy = head(StatusCode::TOO_MANY_REQUESTS, Some("1"));
        assert_eq!(backoff.wait(1, Some(&busy)), ms(1000));
        let too_long = head(StatusCode::TOO_MANY_REQUESTS, Some("2"));
        assert_eq!(backoff.wait(1, Some(&too_long)), None);
        let bad_request = head(StatusCode::BAD_REQUEST, None);
        assert_eq!(backoff.wait(1, Some(&bad_request)), None);
    }
}
