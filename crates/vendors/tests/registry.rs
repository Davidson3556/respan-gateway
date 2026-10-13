//! The registry: each vendor once, each quirk tested, and the README's
//! table of them as the code says.

use respan_gateway_vendors::{ALL, Vendor};

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

fn row(provider: &Vendor) -> String {
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

/// A provider of a vendor sits, and takes its key, as the vendor says.
#[test]
fn each_format_sits_where_and_takes_the_key_as_its_vendor_says() {
    use respan_gateway_vendors::{anthropic, gemini};
    let gemini = gemini::VENDOR
        .provider("g")
        .base("https://a.test/v1beta/openai");
    let parts = gemini.parts().unwrap_or_default();
    let at = |format: &str| {
        let part = parts.iter().find(|p| p.speaks.format.name() == format);
        part.map(|p| (p.name.as_str(), p.base_url.as_str()))
    };
    assert_eq!(
        at("openai-chat"),
        Some(("g", "https://a.test/v1beta/openai"))
    );
    assert_eq!(
        at("gemini-generate"),
        Some(("g:gemini-generate", "https://a.test/v1beta"))
    );
    assert_eq!(at("gemini-live"), Some(("g:gemini-live", "wss://a.test")));
    let claude = anthropic::VENDOR.provider("c").key("k");
    let parts = claude.parts().unwrap_or_default();
    let key = parts[0].headers.get("x-api-key");
    assert_eq!(key.map(|v| v.as_bytes()), Some(&b"k"[..]));
}

/// A format of no vendor is spoken as its own vendor speaks it.
#[test]
fn a_format_of_no_vendor_is_spoken_as_its_own_vendor_speaks_it() {
    use respan_gateway_vendors::{Auth, speaks};
    let messages = speaks(None, "anthropic-messages").map(|s| s.auth);
    assert_eq!(messages, Some(Auth::Header("x-api-key")));
    assert!(speaks(None, "passthrough").is_some());
    assert!(speaks(None, "nope").is_none());
}
