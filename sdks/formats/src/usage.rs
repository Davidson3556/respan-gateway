use std::borrow::Cow;
use std::collections::BTreeMap;

/// An open map of usage units to additive amounts, as a provider reports
/// them: the conversation kind names its units in
/// [`conversation::units`](crate::conversation::units). `f64` is exact for
/// counts such as tokens; money belongs in a decimal type.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Usage(BTreeMap<Cow<'static, str>, f64>);

impl Usage {
    /// Add `amount` of `unit`.
    pub fn add(&mut self, unit: impl Into<Cow<'static, str>>, amount: f64) {
        *self.0.entry(unit.into()).or_insert(0.0) += amount;
    }

    /// The amount of `unit`; `None` if none was reported.
    pub fn get(&self, unit: &str) -> Option<f64> {
        self.0.get(unit).copied()
    }

    /// Whether no usage was reported.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Every unit and its amount.
    pub fn iter(&self) -> impl Iterator<Item = (Cow<'static, str>, f64)> + '_ {
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
