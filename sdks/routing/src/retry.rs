//! Backoff between attempts, honouring the provider's `Retry-After`.

use std::time::Duration;

use respan_gateway_core::http::{self, HeaderMap, StatusCode, header::RETRY_AFTER};

/// How often and how long to wait before retrying one provider.
#[derive(Debug, Clone, Copy)]
pub struct Backoff {
    /// Attempts per provider, the first included.
    pub attempts: u32,
    /// The wait before the second attempt; doubled for each after it.
    pub base: Duration,
    /// The longest wait computed from `base`.
    pub cap: Duration,
    /// The longest `Retry-After` honoured. A provider asking for longer is
    /// not retried: its answer goes to the next provider, or the client.
    pub max_retry_after: Duration,
}

impl Default for Backoff {
    fn default() -> Self {
        Self {
            attempts: 3,
            base: Duration::from_millis(200),
            cap: Duration::from_secs(5),
            max_retry_after: Duration::from_secs(20),
        }
    }
}

impl Backoff {
    /// No retries: one attempt per provider.
    pub fn once() -> Self {
        Self {
            attempts: 1,
            ..Self::default()
        }
    }

    /// The wait before attempt `next` (1 for the first retry) after an
    /// answer with `headers`, or `None` if it should not be retried.
    pub fn wait(&self, next: u32, headers: Option<&HeaderMap>) -> Option<Duration> {
        if next >= self.attempts {
            return None;
        }
        match headers.and_then(retry_after) {
            Some(asked) if asked > self.max_retry_after => None,
            Some(asked) => Some(asked),
            None => Some(self.cap.min(self.base * 2u32.saturating_pow(next - 1))),
        }
    }
}

/// Whether another attempt may get a different answer to one of `status`.
/// Other client errors would be refused by every provider alike.
pub fn retryable(status: StatusCode) -> bool {
    matches!(
        status.as_u16(),
        408 | 409 | 425 | 429 | 500 | 502 | 503 | 504 | 529
    )
}

/// `Retry-After` in seconds. An HTTP date is not parsed; it counts as no
/// hint, so the computed backoff applies.
pub fn retry_after(headers: &HeaderMap) -> Option<Duration> {
    let value = headers.get(RETRY_AFTER)?.to_str().ok()?.trim();
    value.parse::<u64>().ok().map(Duration::from_secs)
}

/// Whether `head` is worth retrying.
pub(crate) fn should_retry(head: &http::response::Parts) -> bool {
    retryable(head.status)
}
