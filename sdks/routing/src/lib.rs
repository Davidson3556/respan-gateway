#![warn(missing_docs)]
//! Transport-neutral attempt control for respan-gateway plugins: sequential
//! retries and fallback ([`first_success`], [`Backoff`]), a staggered race
//! ([`race`]), and each attempt's [`Role`] ([`Attempt`]).
//! The caller supplies a retry classifier; HTTP callers can use
//! `respan_gateway_http::retryable`.
//!
//! Both helpers read each answer [`Until`] a stopping point, using a plugin-supplied
//! predicate for [`Until::content`]: they return a success only
//! once it got there, and never retry after that, so a stream that fails
//! later reaches the client cut short, not replayed. A fallback or hedge is
//! a few lines of `on_client`:
//!
//! ```ignore
//! let calls = ["openai", "anthropic"].map(|p| cx.call(p));
//! let backoff = Backoff::new(3, Duration::from_millis(200), Duration::from_secs(10));
//! first_success(calls, &backoff, Until::content(Reply::is_content),
//!     respan_gateway_http::retryable).await.map(Some)
//! ```

use std::any::Any;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use respan_gateway_core::futures::future::BoxFuture;
use respan_gateway_core::futures::stream::FuturesUnordered;
use respan_gateway_core::futures::{FutureExt, StreamExt};
use respan_gateway_core::{Call, Fault, Response};

/// How often to try one call, and how long to wait between tries.
#[derive(Debug, Clone, Copy)]
pub struct Backoff {
    tries: u32,
    base: Duration,
    max: Duration,
}

impl Backoff {
    /// Up to `tries` tries per call, waiting `base`, doubled after each
    /// retry, at most `max`. An optional response-supplied delay is waited
    /// for as asked, or, if longer than `max`, not at all: the call is not retried.
    pub fn new(tries: u32, base: Duration, max: Duration) -> Self {
        Self { tries, base, max }
    }

    /// One try per call: no retries.
    pub fn once() -> Self {
        Self::new(1, Duration::ZERO, Duration::ZERO)
    }

    /// The wait before another try, after `tries` tries; `None` if the
    /// attempt budget is spent or the supplied delay exceeds `max`.
    fn wait(&self, tries: u32, delay: Option<Duration>) -> Option<Duration> {
        if tries >= self.tries {
            return None;
        }
        let doubled = 2u32.saturating_pow(tries.saturating_sub(1));
        let backoff = self.base.saturating_mul(doubled).min(self.max);
        match delay {
            Some(asked) if asked > self.max => None,
            Some(asked) => Some(asked),
            None => Some(backoff),
        }
    }
}

/// Put on every call [`first_success`] and [`race`] send
/// (read as `px.extensions()`),
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
#[derive(Clone)]
pub enum Until {
    /// The response opening, without reading events.
    Head,
    /// A plugin's condition for committing to a response. Use [`Until::content`].
    Content(Arc<dyn Fn(Response) -> BoxFuture<'static, Result<Response, Fault>> + Send + Sync>),
    /// The entire response; only a clean completion wins.
    End,
}

impl Until {
    /// Wait for an event accepted by this plugin's content policy.
    pub fn content<T: Any + Send + Sync>(
        predicate: impl FnMut(&T) -> bool + Clone + Send + Sync + 'static,
    ) -> Self {
        Self::Content(Arc::new(move |response| {
            response.buffer_until(predicate.clone()).boxed()
        }))
    }
}

impl std::fmt::Debug for Until {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Head => "Head",
            Self::Content(_) => "Content",
            Self::End => "End",
        })
    }
}

/// An attempt, its answer read [`Until`] a point, and, read to its end,
/// the failure a success ended with; and whether `call.await` opened a
/// response before applying `Until`.
type Answer = (Attempt, Result<Response, Fault>, Option<Fault>, bool);

/// Send `call` as `attempt` and read it `until` a point.
async fn send(call: Call, attempt: Attempt, until: Until) -> Answer {
    let (answer, late, responded) = match call.with_extension(attempt.clone()).await {
        Ok(resp) => match until {
            Until::Head => (Ok(resp), None, true),
            Until::Content(read) => (read(resp).await, None, true),
            Until::End => match resp.until_end().await {
                Ok((resp, end)) => (Ok(resp), end.err(), true),
                Err(fault) => (Err(fault), None, true),
            },
        },
        Err(fault) => (Err(fault), None, false),
    };
    (attempt, answer, late, responded)
}

/// How `answer` failed, if it did: its fault, or, read to its end, the
/// failure a success ended with.
fn failure((_, answer, late, _): &Answer) -> Option<&Fault> {
    answer.as_ref().err().or(late.as_ref())
}

/// Return `answer`, promoting its attempt if it is a response. A fault
/// stays a loser: its call already ended, with nothing to commit.
fn win((attempt, answer, ..): Answer) -> Result<Response, Fault> {
    if answer.is_ok() {
        attempt.won.store(true, Ordering::SeqCst);
    }
    answer
}

/// The last answer; with none, `upstream`.
fn last(answer: Option<Answer>) -> Result<Response, Fault> {
    win(answer.ok_or_else(|| Fault::upstream("no provider to try"))?)
}

/// Remember `answer` as the one to give if none succeeds: the latest, but
/// what a provider answered is not replaced by a call that was never sent
/// (a fallback that cannot be made says nothing of the request).
fn keep(previous: &mut Option<Answer>, answer: Answer) {
    if answer.3 || previous.as_ref().is_none_or(|p| !p.3) {
        *previous = Some(answer);
    }
}

/// Each call in turn, retried when `should_retry` permits and `backoff`
/// allows, until one's response is a success read `until` its stopping point;
/// else the last answer, preferring a response over a fallback that could not
/// be made. The classifier is called for **every** failed attempt, including
/// failures before opening, while reading content, and at [`Until::End`]. Its
/// `response_opened` argument is true exactly when `call.await` returned
/// `Ok(Response)` before applying `until`; it does not mean the request was
/// sent, a refusal occurred, or bytes were delivered. Returning false skips
/// retries of that target but still permits the next fallback. Only opened
/// responses contribute `Fault::retry_after()` to the backoff wait. HTTP
/// callers can select `respan_gateway_http::retryable`; other transports can
/// supply their own policy. Neither policy nor timing makes an input replayable.
pub async fn first_success(
    calls: impl IntoIterator<Item = Call>,
    backoff: &Backoff,
    until: Until,
    should_retry: impl Fn(&Fault, bool) -> bool,
) -> Result<Response, Fault> {
    let (mut previous, mut index) = (None, 0);
    for call in calls {
        for tries in 1.. {
            let answer = send(call.clone(), Attempt::new(index), until.clone()).await;
            index += 1;
            let wait = match failure(&answer) {
                None => return win(answer),
                Some(f) if should_retry(f, answer.3) => {
                    let delay = if answer.3 { f.retry_after() } else { None };
                    backoff.wait(tries, delay)
                }
                _ => None,
            };
            let Some(wait) = wait else {
                keep(&mut previous, answer);
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
            Some(answer) = running.next() => {
                if failure(&answer).is_none() {
                    return win(answer);
                }
                keep(&mut previous, answer);
                if running.is_empty() {
                    next.as_mut().reset(tokio::time::Instant::now());
                }
            }
            () = &mut next, if waiting.peek().is_some() => {
                if let Some((call, index)) = waiting.next() {
                    running.push(send(call, Attempt::new(index), until.clone()).boxed());
                }
                next.as_mut().reset(tokio::time::Instant::now() + stagger);
            }
            else => return last(previous),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::Backoff;

    #[test]
    fn backoff_doubles_up_to_its_max_and_bounds_supplied_delays() {
        let backoff = Backoff::new(5, Duration::from_millis(400), Duration::from_secs(1));
        let waits: Vec<_> = (1..=5).map(|n| backoff.wait(n, None)).collect();
        let ms = |n| Some(Duration::from_millis(n));
        assert_eq!(waits, [ms(400), ms(800), ms(1000), ms(1000), None]);
        assert_eq!(backoff.wait(1, Some(Duration::from_secs(1))), ms(1000));
        assert_eq!(backoff.wait(1, Some(Duration::from_secs(2))), None);
        assert_eq!(backoff.wait(5, Some(Duration::ZERO)), None);
    }
}
