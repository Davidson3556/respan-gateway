//! Which tools are offered, by name, with `*` for any characters, and
//! which are switched off.

use std::collections::HashSet;

use parking_lot::RwLock;

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
pub struct ToolScope(Vec<String>);

impl ToolScope {
    /// Limit a client to the tools `tools` names: none, if it names none.
    pub fn new(tools: Vec<String>) -> Self {
        Self(tools)
    }

    /// Whether the tool `name` is within the scope.
    pub fn offers(&self, name: &str) -> bool {
        self.0.iter().any(|p| glob(p, name))
    }
}

/// Tool servers and tools turned off while the gateway runs, held in memory
/// only. Share one through an [`Arc`](std::sync::Arc) with whatever offers
/// tools; a switch takes effect at once. Everything is on until turned off.
#[derive(Debug, Default)]
pub struct ToolSwitches(RwLock<Off>);

#[derive(Debug, Default)]
struct Off {
    servers: HashSet<String>,
    /// A server's tools, by their own names.
    tools: HashSet<(String, String)>,
}

impl ToolSwitches {
    /// Turn the server `name` on or off.
    pub fn set_server(&self, name: &str, on: bool) {
        let servers = &mut self.0.write().servers;
        match on {
            true => servers.remove(name),
            false => servers.insert(name.to_owned()),
        };
    }

    /// Turn the tool `tool` (its own name) of `server` on or off.
    pub fn set_tool(&self, server: &str, tool: &str, on: bool) {
        let key = (server.to_owned(), tool.to_owned());
        let tools = &mut self.0.write().tools;
        match on {
            true => tools.remove(&key),
            false => tools.insert(key),
        };
    }

    /// Whether the server `name` is on.
    pub fn server_on(&self, name: &str) -> bool {
        !self.0.read().servers.contains(name)
    }

    /// Whether the tool `tool` of `server` is on, by its own switch.
    pub fn tool_on(&self, server: &str, tool: &str) -> bool {
        let key = (server.to_owned(), tool.to_owned());
        !self.0.read().tools.contains(&key)
    }

    /// Whether the tool `tool` of `server` is offered: it and its server
    /// are on.
    pub fn offers(&self, server: &str, tool: &str) -> bool {
        self.server_on(server) && self.tool_on(server, tool)
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

    #[test]
    fn a_scope_naming_nothing_offers_nothing() {
        assert!(!ToolScope::new(Vec::new()).offers("gh__search"));
        let scope = ToolScope::new(vec!["gh__*".to_owned()]);
        assert!(scope.offers("gh__search") && !scope.offers("fs__read"));
    }

    #[test]
    fn a_tool_is_offered_while_it_and_its_server_are_on() {
        let switches = ToolSwitches::default();
        assert!(switches.offers("gh", "search"));
        switches.set_tool("gh", "search", false);
        assert!(!switches.tool_on("gh", "search") && !switches.offers("gh", "search"));
        assert!(switches.offers("gh", "read") && switches.offers("fs", "search"));
        switches.set_server("gh", false);
        assert!(!switches.server_on("gh") && !switches.offers("gh", "read"));
        assert!(switches.server_on("gh__x") && switches.offers("gh__x", "read"));
        switches.set_server("gh", true);
        assert!(switches.offers("gh", "read") && !switches.offers("gh", "search"));
        switches.set_tool("gh", "search", true);
        assert!(switches.offers("gh", "search"));
    }
}
