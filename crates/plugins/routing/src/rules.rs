//! Routes: a model name a client asks for, or a glob of them, and the
//! candidates that may serve it, in the order a strategy picks them.

use std::fmt;
use std::time::Duration;

use respan_gateway_plugin_fallback::{Backoff, Until};

use crate::stats::Stats;
use respan_gateway_sdk::Catalog;

/// A route: requests for model `name` go to one of `to`.
#[derive(Debug, Clone)]
pub struct Rule {
    /// The model clients ask for, or a glob of them with `*`, such as
    /// `llama-*`.
    pub name: String,
    /// What may serve it.
    pub to: Vec<Candidate>,
    /// How the candidates are ordered for a request.
    pub pick: Pick,
    /// What a candidate's model must be able to do, as its provider's
    /// listing says: `tools`, `vision`, `reasoning` and the like. One the
    /// listing says nothing of is not passed over.
    pub needs: Vec<String>,
    /// The client keys it applies to, by name; all if empty.
    pub clients: Vec<String>,
    /// The kinds it applies to, such as `chat`; all if empty.
    pub kinds: Vec<String>,
    /// Whether the candidates after the first are tried when it fails.
    pub fallback: bool,
    /// Races the candidates instead, each started this long after the one
    /// before.
    pub hedge: Option<Duration>,
    /// How often each candidate is tried, and the waits between.
    pub backoff: Backoff,
    /// How far an answer is read before it counts as a success.
    pub until: Until,
}

impl Rule {
    /// A route for `name` to `to`, picked in order, falling back to the
    /// rest, each tried once and judged at its head.
    pub fn new(name: impl Into<String>, to: Vec<Candidate>) -> Self {
        Self {
            name: name.into(),
            to,
            pick: Pick::First,
            needs: Vec::new(),
            clients: Vec::new(),
            kinds: Vec::new(),
            fallback: true,
            hedge: None,
            backoff: Backoff::once(),
            until: Until::Head,
        }
    }

    /// Whether its name is a glob (with a `*` or `?`) rather than one model.
    pub fn is_glob(&self) -> bool {
        self.name.contains(['*', '?'])
    }

    /// Whether it routes a request for `model`.
    pub fn matches(&self, model: &str) -> bool {
        glob(&self.name, model)
    }

    /// Whether it applies to a request of `kind` from `client`.
    pub(crate) fn applies(&self, kind: &str, client: Option<&str>) -> bool {
        let kind_ok = self.kinds.is_empty() || self.kinds.iter().any(|k| k == kind);
        let client_ok =
            self.clients.is_empty() || client.is_some_and(|c| self.clients.iter().any(|k| k == c));
        kind_ok && client_ok
    }

    /// Whether more than one call may be sent for a request.
    pub fn sends_many(&self) -> bool {
        self.fallback || self.hedge.is_some()
    }
}

/// A provider that may serve a route, and the model it is sent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Candidate {
    /// The provider, as configured.
    pub provider: String,
    /// The model it is sent, `{model}` standing for the one asked for; the
    /// one asked for if `None`.
    pub model: Option<String>,
    /// Its share of requests, for [`Pick::Weighted`].
    pub weight: u32,
}

impl Candidate {
    /// `provider`, `provider/model` or `provider/{model}`.
    pub fn parse(spec: &str) -> Self {
        let (provider, model) = match spec.split_once('/') {
            Some((provider, model)) => (provider, Some(model.to_owned())),
            None => (spec, None),
        };
        Self {
            provider: provider.to_owned(),
            model,
            weight: 1,
        }
    }

    /// The model it is sent for a request that asked for `asked`.
    pub fn model_for(&self, asked: Option<&str>) -> Option<String> {
        match &self.model {
            Some(model) => Some(model.replace("{model}", asked.unwrap_or_default())),
            None => asked.map(str::to_owned),
        }
    }
}

impl fmt::Display for Candidate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.model {
            Some(model) => write!(f, "{}/{model}", self.provider),
            None => f.write_str(&self.provider),
        }
    }
}

/// How a route orders its candidates for a request. Whichever it is, one
/// that failed its last few calls just now goes last.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Pick {
    /// As listed.
    #[default]
    First,
    /// By price: input plus three times output, per million tokens; one
    /// of unknown price last.
    Cheapest,
    /// By the router's own record of each one's time to a response's head;
    /// one it has no recent record of first, so it gets one.
    Fastest,
    /// At random by weight, then the rest by weight.
    Weighted,
    /// At random.
    Random,
}

impl Pick {
    /// Its name in a config.
    pub fn name(self) -> &'static str {
        match self {
            Self::First => "first",
            Self::Cheapest => "cheapest",
            Self::Fastest => "fastest",
            Self::Weighted => "weighted",
            Self::Random => "random",
        }
    }

    /// The pick named `name`.
    pub fn named(name: &str) -> Option<Self> {
        [
            Self::First,
            Self::Cheapest,
            Self::Fastest,
            Self::Weighted,
            Self::Random,
        ]
        .into_iter()
        .find(|p| p.name() == name)
    }
}

/// The order `pick` gives `candidates`, each with the model it is sent.
pub(crate) fn order<T>(
    pick: Pick,
    mut candidates: Vec<T>,
    of: impl Fn(&T) -> (&Candidate, &str),
    stats: &Stats,
    catalog: &Catalog,
) -> Vec<T> {
    match pick {
        Pick::First => {}
        Pick::Cheapest => {
            let cost = |c: &T| {
                let (candidate, model) = of(c);
                catalog
                    .get(&candidate.provider, model)
                    .and_then(|m| m.price)
                    .map(|p| p.weight())
                    .unwrap_or(f64::INFINITY)
            };
            candidates.sort_by(|a, b| cost(a).total_cmp(&cost(b)));
        }
        Pick::Fastest => {
            let latency = |c: &T| {
                let (candidate, model) = of(c);
                stats
                    .fresh_latency(&candidate.provider, model)
                    .unwrap_or(-1.0)
            };
            candidates.sort_by(|a, b| latency(a).total_cmp(&latency(b)));
        }
        Pick::Weighted | Pick::Random => {
            let weight = |c: &T| match pick {
                Pick::Weighted => u64::from(of(c).0.weight),
                _ => 1,
            };
            let mut drawn = Vec::with_capacity(candidates.len());
            while !candidates.is_empty() {
                let total: u64 = candidates.iter().map(weight).sum();
                let i = match total {
                    0 => 0,
                    total => {
                        let mut at = random() % total;
                        let found = candidates.iter().position(|c| {
                            let w = weight(c);
                            if at < w {
                                return true;
                            }
                            at -= w;
                            false
                        });
                        found.unwrap_or(0)
                    }
                };
                drawn.push(candidates.remove(i));
            }
            candidates = drawn;
        }
    }
    // A stable sort: the cooling go last, in the order they had.
    candidates.sort_by_key(|c| {
        let (candidate, model) = of(c);
        stats
            .seen(&candidate.provider, model)
            .is_some_and(|s| s.cooling)
    });
    candidates
}

fn random() -> u64 {
    let mut bytes = [0u8; 8];
    // The clock, should the system's randomness fail: good enough to spread load.
    if getrandom::fill(&mut bytes).is_err() {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.subsec_nanos())
            .unwrap_or_default();
        return u64::from(nanos);
    }
    u64::from_le_bytes(bytes)
}

/// Whether `name` matches `pattern`, whose `*` stands for any run of
/// characters and `?` for any one.
pub fn glob(pattern: &str, name: &str) -> bool {
    wildmatch::WildMatch::new(pattern).matches(name)
}

#[cfg(test)]
mod tests {
    use super::*;
    use respan_gateway_sdk::{Model, Price};

    #[test]
    fn globs_match_any_run_of_characters() {
        assert!(glob("llama-*", "llama-3.3-70b"));
        assert!(glob("*", "anything"));
        assert!(glob("*-mini", "gpt-4o-mini"));
        assert!(glob("gpt-*-mini", "gpt-4o-mini"));
        assert!(!glob("gpt-*-mini", "gpt-4o"));
        assert!(!glob("llama-*", "meta-llama-3"));
        assert!(glob("smart", "smart"));
        assert!(!glob("smart", "smarter"));
        assert!(!glob("a*a", "a"));
        assert!(glob("gpt-?o", "gpt-4o") && !glob("gpt-?o", "gpt-o"));
        assert!(Rule::new("gpt-?o", Vec::new()).is_glob());
        assert!(!Rule::new("gpt-4o", Vec::new()).is_glob());
    }

    #[test]
    fn a_candidate_names_its_provider_and_model() {
        let c = Candidate::parse("openrouter/openai/{model}");
        assert_eq!(c.provider, "openrouter");
        assert_eq!(
            c.model_for(Some("gpt-4o")).as_deref(),
            Some("openai/gpt-4o")
        );
        assert_eq!(c.to_string(), "openrouter/openai/{model}");
        let kept = Candidate::parse("groq");
        assert_eq!(kept.model_for(Some("llama")).as_deref(), Some("llama"));
        assert_eq!(kept.model_for(None), None);
    }

    /// `cheap` and `dear` list `m` at their prices.
    fn prices() -> Catalog {
        let catalog = Catalog::default();
        for (provider, input, output) in [("cheap", 1.0, 1.0), ("dear", 10.0, 30.0)] {
            let mut model = Model::new("m");
            model.price = Some(Price::new(input, output, None));
            catalog.set(provider, [model]);
        }
        catalog
    }

    fn names(pick: Pick, list: &[&str], stats: &Stats) -> Vec<String> {
        let candidates = list.iter().map(|s| Candidate::parse(s)).collect();
        let ordered = order(pick, candidates, |c| (c, "m"), stats, &prices());
        ordered.into_iter().map(|c| c.provider).collect()
    }

    #[test]
    fn cheapest_ranks_by_price_and_unknown_last() {
        let stats = Stats::default();
        let got = names(Pick::Cheapest, &["dear", "unknown", "cheap"], &stats);
        assert_eq!(got, ["cheap", "dear", "unknown"]);
    }

    #[test]
    fn fastest_tries_the_unknown_first_then_the_quickest() {
        let stats = Stats::default();
        stats.record("slow", "m", Some(Duration::from_millis(900)), false);
        stats.record("quick", "m", Some(Duration::from_millis(90)), false);
        let got = names(Pick::Fastest, &["slow", "quick", "new"], &stats);
        assert_eq!(got, ["new", "quick", "slow"]);
    }

    #[test]
    fn a_cooling_candidate_goes_last() {
        let stats = Stats::default();
        for _ in 0..3 {
            stats.record("a", "m", None, true);
        }
        assert_eq!(names(Pick::First, &["a", "b"], &stats), ["b", "a"]);
    }

    #[test]
    fn weighted_draws_every_candidate_once() {
        let stats = Stats::default();
        let mut got = names(Pick::Weighted, &["a", "b", "c"], &stats);
        got.sort();
        assert_eq!(got, ["a", "b", "c"]);
    }
}
