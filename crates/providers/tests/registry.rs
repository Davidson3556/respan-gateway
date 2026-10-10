//! The registry: each vendor once, its quirks its knobs, and the README's
//! table of them as the code says.

use respan_gateway_providers::{ALL, Provider};

#[test]
fn every_vendor_is_listed_once() {
    let mut ids: Vec<&str> = ALL.iter().map(|p| p.id).collect();
    ids.sort_unstable();
    ids.dedup();
    assert_eq!(ids.len(), ALL.len());
}

#[test]
fn a_vendors_quirks_are_the_knobs_it_sets() {
    for provider in ALL {
        let mut quirks: Vec<&str> = provider.quirks.iter().map(|q| q.name).collect();
        quirks.sort_unstable();
        assert_eq!(quirks, provider.knobs(), "{}", provider.id);
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
