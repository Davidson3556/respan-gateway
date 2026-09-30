//! A shared atomic counter for a call's priced cost.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};

use crate::Amount;

/// One call's cost as your code prices it, reported while the call runs,
/// such as by a filter of the call's outputs that prices each usage
/// increment its provider reports. Clones share it.
#[derive(Debug, Clone, Default)]
pub struct Tally(Arc<Counted>);

#[derive(Debug, Default)]
struct Counted {
    spent: AtomicI64,
    reported: AtomicBool,
}

impl Tally {
    /// Add `amount` to what the call cost, saturating; the call's cost now
    /// counts as reported, even if `amount` is 0.
    pub fn add(&self, amount: Amount) {
        let spent = &self.0.spent;
        let add = |now: Amount| Some(now.saturating_add(amount));
        let _ = spent.fetch_update(Ordering::Relaxed, Ordering::Relaxed, add);
        self.0.reported.store(true, Ordering::Relaxed);
    }

    /// What was added so far.
    pub fn spent(&self) -> Amount {
        self.0.spent.load(Ordering::Relaxed)
    }

    /// Whether anything was added.
    pub fn is_reported(&self) -> bool {
        self.0.reported.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::Tally;

    #[test]
    fn clones_share_cost_and_reporting_even_for_zero() {
        let tally = Tally::default();
        let clone = tally.clone();
        assert_eq!(tally.spent(), 0);
        assert!(!tally.is_reported());
        clone.add(0);
        assert!(tally.is_reported());
        clone.add(7);
        assert_eq!(tally.spent(), 7);
    }

    #[test]
    fn additions_saturate_in_both_directions() {
        let tally = Tally::default();
        tally.add(i64::MAX);
        tally.add(1);
        assert_eq!(tally.spent(), i64::MAX);
        tally.add(i64::MIN);
        tally.add(-1);
        assert_eq!(tally.spent(), -2);
        tally.add(i64::MIN);
        assert_eq!(tally.spent(), i64::MIN);
    }
}
