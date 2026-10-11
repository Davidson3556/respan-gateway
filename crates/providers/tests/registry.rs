//! The registry: each vendor once, each quirk tested, and the README's
//! table of them as the code says.

use respan_gateway_providers::{ALL, Provider};

#[test]
fn every_vendor_is_listed_once() {
    let mut ids: Vec<&str> = ALL.iter().map(|p| p.id).collect();
    ids.sort_unstable();
    ids.dedup();
    assert_eq!(ids.len(), ALL.len());
}

/// The tests of each vendor with quirks.
const TESTS: &[(&str, &str)] = &[
    ("azure", include_str!("azure.rs")),
    ("deepseek", include_str!("deepseek.rs")),
    ("gemini", include_str!("gemini.rs")),
    ("groq", include_str!("groq.rs")),
    ("openai", include_str!("openai.rs")),
    ("openrouter", include_str!("openrouter.rs")),
    ("vllm", include_str!("vllm.rs")),
];

/// A quirk is tested by a test of `tests/<vendor>.rs` whose name has it.
#[test]
fn every_quirk_has_a_test_named_for_it() {
    for provider in ALL.iter().filter(|p| !p.quirks.is_empty()) {
        let tests = TESTS.iter().find(|(id, _)| *id == provider.id);
        let (_, tests) = tests.unwrap_or_else(|| panic!("no tests/{}.rs", provider.id));
        for quirk in provider.quirks {
            let named = tests.lines().any(|l| {
                let l = l.trim_start();
                l.starts_with("fn ") || l.starts_with("async fn ")
            } && l.contains(quirk.name));
            assert!(named, "{}: no test named for {}", provider.id, quirk.name);
        }
    }
}

/// The README's table, between its markers, is the one [`ALL`] makes. Run
/// with `BLESS=1` to write it.
#[test]
fn the_readme_table_is_current() {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/README.md");
    let readme = std::fs::read_to_string(path).expect("the README");
    let (start, end) = ("<!-- table -->\n", "<!-- /table -->");
    let (before, rest) = readme.split_once(start).expect("the table's start");
    let (_, after) = rest.split_once(end).expect("the table's end");
    let current = format!("{before}{start}{}{end}{after}", table());
    if std::env::var_os("BLESS").is_some() {
        std::fs::write(path, &current).expect("the README written");
        return;
    }
    assert!(
        readme == current,
        "the README's table is stale; run with BLESS=1"
    );
}

fn table() -> String {
    let mut table = "| Vendor | Formats | Quirks |\n|---|---|---|\n".to_owned();
    for provider in ALL {
        table.push_str(&row(provider));
    }
    table
}

fn row(provider: &Provider) -> String {
    let code = |s: &str| format!("`{s}`");
    let formats: Vec<String> = provider
        .formats
        .iter()
        .map(|s| code(s.format.name()))
        .collect();
    let quirks: Vec<String> = provider
        .quirks
        .iter()
        .map(|q| format!("{}: {}", code(q.name), q.description))
        .collect();
    format!(
        "| {} | {} | {} |\n",
        code(provider.id),
        formats.join(", "),
        quirks.join("<br>")
    )
}
