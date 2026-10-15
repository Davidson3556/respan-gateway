//! The models a host knows.

use std::collections::HashMap;
use std::sync::{PoisonError, RwLock};

use crate::Model;

/// The models a host knows, by provider and id, shared by whoever reads
/// them, such as a router and a pricing plugin: each provider's, as its
/// list and the host's declarations last said.
#[derive(Debug, Default)]
pub struct Catalog(RwLock<HashMap<(String, String), Model>>);

impl Catalog {
    /// `provider`'s models, as they now are: those it had before go.
    pub fn set(&self, provider: &str, models: impl IntoIterator<Item = Model>) {
        let mut all = self.0.write().unwrap_or_else(PoisonError::into_inner);
        all.retain(|(p, _), _| p != provider);
        for mut model in models {
            model.provider = provider.to_owned();
            all.insert((provider.to_owned(), model.id.clone()), model);
        }
    }

    /// The models `other` knows that this does not, as they are there.
    pub fn fill_from(&self, other: &Catalog) {
        let theirs = other
            .0
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        let mut all = self.0.write().unwrap_or_else(PoisonError::into_inner);
        for (key, model) in theirs {
            all.entry(key).or_insert(model);
        }
    }

    /// `model` of `provider`, if it has it.
    pub fn get(&self, provider: &str, model: &str) -> Option<Model> {
        let all = self.0.read().unwrap_or_else(PoisonError::into_inner);
        all.get(&(provider.to_owned(), model.to_owned())).cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_providers_models_are_replaced_whole() {
        let catalog = Catalog::default();
        catalog.set("p", [Model::new("a"), Model::new("b")]);
        catalog.set("p", [Model::new("b")]);
        assert_eq!(catalog.get("p", "a"), None);
        assert_eq!(catalog.get("p", "b").map(|m| m.provider), Some("p".into()));
        assert_eq!(catalog.get("q", "b"), None);
    }

    #[test]
    fn filling_keeps_its_own_and_adds_what_it_lacks() {
        let (mine, theirs) = (Catalog::default(), Catalog::default());
        let mut declared = Model::new("a");
        declared.display_name = Some("mine".into());
        mine.set("p", [declared]);
        theirs.set("p", [Model::new("a"), Model::new("b")]);
        mine.fill_from(&theirs);
        let name = mine.get("p", "a").and_then(|m| m.display_name);
        assert_eq!(name.as_deref(), Some("mine"));
        assert!(mine.get("p", "b").is_some());
    }
}
