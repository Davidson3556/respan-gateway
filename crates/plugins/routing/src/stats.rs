//! What the router has seen of each candidate: its own record, kept from
//! the calls it routed, so it needs no request log to pick the fastest or
//! to pass over one that keeps failing.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// How much a new sample weighs in the moving averages.
const WEIGHT: f64 = 0.2;
/// Failures in a row that cool a candidate down.
const STREAK: u32 = 3;
/// How long a cooled candidate goes last.
const COOLING: Duration = Duration::from_secs(30);
/// How old a latency may be and still rank a candidate; an older one is
/// tried again as if unknown, so a recovered provider gets its turn.
const FRESH: Duration = Duration::from_secs(10 * 60);

/// Each candidate's record, by provider and model.
#[derive(Debug, Default)]
pub struct Stats(Mutex<HashMap<(String, String), Stat>>);

#[derive(Debug, Clone)]
struct Stat {
    /// Its moving average time to a head, in milliseconds, and when it was
    /// last measured.
    latency: Option<(f64, Instant)>,
    error_rate: f64,
    calls: u64,
    failures: u64,
    streak: u32,
    last_failure: Option<Instant>,
}

/// A candidate's record, as it is now.
#[derive(Debug, Clone, PartialEq)]
pub struct Seen {
    /// Its time to a response's head, a moving average in milliseconds.
    pub latency_ms: Option<f64>,
    /// Its share of failed calls, a moving average from 0 to 1.
    pub error_rate: f64,
    /// The calls it was sent.
    pub calls: u64,
    /// The calls of those it failed.
    pub failures: u64,
    /// Whether it failed its last few calls just now, so goes last.
    pub cooling: bool,
}

impl Stats {
    /// Records a call to `model` of `provider` that opened after `head`, if
    /// it did, and `failed` or not.
    pub fn record(&self, provider: &str, model: &str, head: Option<Duration>, failed: bool) {
        let mut stats = self.0.lock().unwrap_or_else(|e| e.into_inner());
        let now = Instant::now();
        let stat = stats
            .entry((provider.to_owned(), model.to_owned()))
            .or_insert(Stat {
                latency: None,
                error_rate: 0.0,
                calls: 0,
                failures: 0,
                streak: 0,
                last_failure: None,
            });
        if let Some(head) = head {
            let ms = head.as_secs_f64() * 1000.0;
            let avg = match stat.latency {
                Some((avg, at)) if now.duration_since(at) <= FRESH => avg + WEIGHT * (ms - avg),
                _ => ms,
            };
            stat.latency = Some((avg, now));
        }
        let sample = if failed { 1.0 } else { 0.0 };
        stat.error_rate = match stat.calls {
            0 => sample,
            _ => stat.error_rate + WEIGHT * (sample - stat.error_rate),
        };
        stat.calls += 1;
        if failed {
            stat.failures += 1;
            stat.streak += 1;
            stat.last_failure = Some(now);
        } else {
            stat.streak = 0;
        }
    }

    /// The record of `model` of `provider`, if it was called.
    pub fn seen(&self, provider: &str, model: &str) -> Option<Seen> {
        let stats = self.0.lock().unwrap_or_else(|e| e.into_inner());
        let stat = stats.get(&(provider.to_owned(), model.to_owned()))?;
        let cooling =
            stat.streak >= STREAK && stat.last_failure.is_some_and(|at| at.elapsed() < COOLING);
        Some(Seen {
            latency_ms: stat.latency.map(|(ms, _)| ms),
            error_rate: stat.error_rate,
            calls: stat.calls,
            failures: stat.failures,
            cooling,
        })
    }

    /// Its latency, if recent enough to rank it by.
    pub(crate) fn fresh_latency(&self, provider: &str, model: &str) -> Option<f64> {
        let stats = self.0.lock().unwrap_or_else(|e| e.into_inner());
        let stat = stats.get(&(provider.to_owned(), model.to_owned()))?;
        let (ms, at) = stat.latency?;
        (at.elapsed() < FRESH).then_some(ms)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_streak_of_failures_cools_a_candidate_and_a_success_ends_it() {
        let stats = Stats::default();
        for _ in 0..3 {
            stats.record("a", "m", None, true);
        }
        let seen = stats.seen("a", "m").expect("seen");
        assert!(seen.cooling);
        assert_eq!((seen.calls, seen.failures), (3, 3));
        stats.record("a", "m", Some(Duration::from_millis(100)), false);
        let seen = stats.seen("a", "m").expect("seen");
        assert!(!seen.cooling);
        assert_eq!(seen.latency_ms, Some(100.0));
        assert!(seen.error_rate < 1.0);
    }

    #[test]
    fn latency_is_a_moving_average() {
        let stats = Stats::default();
        stats.record("a", "m", Some(Duration::from_millis(100)), false);
        stats.record("a", "m", Some(Duration::from_millis(200)), false);
        assert_eq!(stats.fresh_latency("a", "m"), Some(120.0));
        assert_eq!(stats.fresh_latency("a", "other"), None);
    }

    /// Regression: any call, a failure that never opened among them, kept
    /// an old latency fresh.
    #[test]
    fn only_a_measured_latency_is_fresh() {
        let stats = Stats::default();
        stats.record("a", "m", Some(Duration::from_millis(100)), false);
        let long_ago = Instant::now().checked_sub(FRESH + Duration::from_secs(1));
        if let Some(at) = long_ago {
            let mut all = stats.0.lock().unwrap_or_else(|e| e.into_inner());
            all.values_mut().for_each(|s| s.latency = Some((100.0, at)));
        }
        stats.record("a", "m", None, true);
        assert_eq!(stats.fresh_latency("a", "m"), None);
    }
}
