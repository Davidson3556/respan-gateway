//! Which tools are offered, by name, with `*` for any characters.

/// Which tools are offered: those `allow` names (all, if it names none) but
/// not those `deny` names, by the name the model has. A pattern is a name,
/// with `*` for any characters.
#[derive(Debug, Clone, Default)]
pub struct ToolFilter {
    allow: Vec<String>,
    deny: Vec<String>,
}

impl ToolFilter {
    /// Offer what `allow` names (everything, if empty) but not what `deny`
    /// names.
    pub fn new(allow: Vec<String>, deny: Vec<String>) -> Self {
        Self { allow, deny }
    }

    /// Whether the tool `name` is offered.
    pub fn offers(&self, name: &str) -> bool {
        let named = |patterns: &[String]| patterns.iter().any(|p| glob(p, name));
        (self.allow.is_empty() || named(&self.allow)) && !named(&self.deny)
    }
}

/// The tools a client is limited to, put on its requests' extensions by
/// whatever knows the client. A tool is offered to such a request only if
/// it is within this and the runner's own filter.
#[derive(Debug, Clone)]
pub struct ToolScope(ToolFilter);

impl ToolScope {
    /// Limit a client to the tools `tools` names.
    pub fn new(tools: Vec<String>) -> Self {
        Self(ToolFilter::new(tools, Vec::new()))
    }

    /// Whether the tool `name` is within the scope.
    pub fn offers(&self, name: &str) -> bool {
        self.0.offers(name)
    }
}

/// Whether `text` matches `pattern`, in which `*` stands for any characters.
fn glob(pattern: &str, text: &str) -> bool {
    wildmatch::WildMatch::new(pattern).matches(text)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_pattern_matches_with_stars() {
        assert!(glob("github__*", "github__search"));
        assert!(glob("*__delete_*", "gh__delete_repo"));
        assert!(glob("a*b*c", "aXbYc") && glob("a*b*c", "abc"));
        assert!(glob("*", "") && glob("exact", "exact"));
        assert!(!glob("github__*", "gitlab__search"));
        assert!(!glob("a*b", "ab_"));
        assert!(!glob("a*b*c", "acb"));
        assert!(!glob("exact", "exacts"));
    }

    #[test]
    fn deny_wins_over_allow_and_allow_defaults_to_all() {
        let filter = |allow: &[&str], deny: &[&str]| {
            let names = |p: &[&str]| p.iter().map(|s| (*s).to_owned()).collect();
            ToolFilter::new(names(allow), names(deny))
        };
        let all = filter(&[], &[]);
        assert!(all.offers("gh__search"));
        let some = filter(&["gh__*"], &["gh__delete_*"]);
        assert!(some.offers("gh__search") && !some.offers("gh__delete_repo"));
        assert!(!some.offers("fs__read"));
        let denied = filter(&[], &["fs__write"]);
        assert!(denied.offers("fs__read") && !denied.offers("fs__write"));
    }
}
