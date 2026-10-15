//! The registry: each vendor once, and the README's table of them as the
//! code says.

use respan_gateway_vendors::{ALL, GREEN, Vendor, YELLOW};

/// Each vendor is listed once, in one tier.
#[test]
fn every_vendor_is_listed_once_in_one_tier() {
    let ids = |vendors: &[&Vendor]| {
        let mut ids: Vec<&str> = vendors.iter().map(|p| p.id).collect();
        ids.sort_unstable();
        ids
    };
    let mut all = ids(ALL);
    all.dedup();
    assert_eq!(all.len(), ALL.len());
    let mut tiers = ids(&[GREEN, YELLOW].concat());
    tiers.dedup();
    assert_eq!(tiers, all);
    assert_eq!(GREEN.len() + YELLOW.len(), ALL.len());
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
    let mut table = "| Vendor | Tier | Formats |\n|---|---|---|\n".to_owned();
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
        .map(|(f, _)| code(f.name()))
        .collect();
    let tier = match GREEN.iter().any(|v| v.id == provider.id) {
        true => "green",
        false => "yellow",
    };
    let (id, formats) = (code(provider.id), formats.join(", "));
    format!("| {id} | {tier} | {formats} |\n")
}

/// A provider of a vendor sits at the URLs the vendor gives, each format
/// as its spec has it reached, and takes its key as each format takes it.
#[test]
fn each_format_sits_where_its_vendor_says_and_takes_the_key_as_its_spec_says() {
    use respan_gateway_vendors::{anthropic, azure, gemini};
    let gemini = gemini::VENDOR.provider("g").key("k");
    let parts = gemini.parts().unwrap_or_default();
    let at = |format: &str| {
        let part = parts.iter().find(|p| p.format.name() == format);
        part.map(|p| (p.name.as_str(), p.base_url.as_str()))
    };
    let google = "https://generativelanguage.googleapis.com";
    assert_eq!(
        at("gemini-generate"),
        Some(("g", &*format!("{google}/v1beta")))
    );
    assert_eq!(
        at("openai-chat"),
        Some(("g:openai-chat", &*format!("{google}/v1beta/openai")))
    );
    // Live's path is whole: it takes the origin, in its scheme.
    let live = "wss://generativelanguage.googleapis.com";
    assert_eq!(at("gemini-live"), Some(("g:gemini-live", live)));
    let key = |parts: &[respan_gateway_vendors::Part], format: &str, header: &str| {
        let part = parts.iter().find(|p| p.format.name() == format);
        part.and_then(|p| p.headers.get(header))
            .map(|v| v.as_bytes().to_vec())
    };
    assert_eq!(
        key(&parts, "gemini-generate", "x-goog-api-key"),
        Some(b"k".to_vec())
    );
    assert_eq!(
        key(&parts, "openai-chat", "authorization"),
        Some(b"Bearer k".to_vec())
    );
    let claude = anthropic::VENDOR
        .provider("c")
        .key("k")
        .parts()
        .unwrap_or_default();
    assert_eq!(
        key(&claude, "anthropic-messages", "x-api-key"),
        Some(b"k".to_vec())
    );
    // Azure's dialect takes the key in a header of its own.
    let azure = azure::VENDOR
        .provider("a")
        .base("https://r.test/openai/v1")
        .key("k");
    let azure = azure.parts().unwrap_or_default();
    assert_eq!(key(&azure, "openai-chat", "api-key"), Some(b"k".to_vec()));
}

/// A format of no vendor is spoken as its spec has it reached.
#[test]
fn a_format_of_no_vendor_is_spoken_as_its_spec_has_it() {
    use respan_gateway_vendors::{KeyIn, format};
    let messages = format(None, "anthropic-messages").map(|(f, url)| (f.key(), url));
    assert_eq!(messages, Some((KeyIn::Header("x-api-key"), None)));
    assert!(format(None, "passthrough").is_some_and(|(f, _)| f.at_origin()));
    assert!(format(None, "nope").is_none());
}
