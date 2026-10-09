use std::borrow::Cow;
use std::collections::BTreeMap;

/// An open map of usage units to additive amounts, as a provider reports
/// them. `Usage` is kind-neutral: each kind names its own units (the
/// conversation kind's are in `respan_gateway_conversation::units`).
/// Amounts are integer counts of the unit's smallest measure (tokens,
/// characters, milliseconds); money belongs in the application's own decimal
/// type. Addition saturates, so it never overflows.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Usage(BTreeMap<Cow<'static, str>, u64>);

impl Usage {
    /// Add `amount` of `unit`, saturating at `u64::MAX`.
    pub fn add(&mut self, unit: impl Into<Cow<'static, str>>, amount: u64) {
        let total = self.0.entry(unit.into()).or_insert(0);
        *total = total.saturating_add(amount);
    }

    /// The amount of `unit`; `None` if none was reported.
    pub fn get(&self, unit: &str) -> Option<u64> {
        self.0.get(unit).copied()
    }

    /// Whether no usage was reported.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Every unit and its amount.
    pub fn iter(&self) -> impl Iterator<Item = (Cow<'static, str>, u64)> + '_ {
        self.0.iter().map(|(k, v)| (k.clone(), *v))
    }
}

impl std::ops::AddAssign<&Usage> for Usage {
    fn add_assign(&mut self, other: &Usage) {
        for (unit, amount) in other.iter() {
            self.add(unit, amount);
        }
    }
}

impl std::ops::AddAssign for Usage {
    fn add_assign(&mut self, other: Usage) {
        *self += &other;
    }
}
