#![warn(missing_docs)]
//! The tool vendors: HTTP APIs offered as tools, a module each. [`ALL`]
//! lists them; [`find`] looks one up by its id.
//!
//! - [`linkup`]: Linkup's web search, sourced answers and page fetches
//!   (`search`, `answer`, `fetch`), its key from `LINKUP_API_KEY`.
//! - [`tinyfish`]: TinyFish's web search, page fetches and browser agent
//!   (`search`, `fetch`, `run_agent`), its key from `TINYFISH_API_KEY`.

pub use respan_gateway_tool_vendor_kit::*;

mod args;
pub mod linkup;
pub mod tinyfish;

/// Every tool vendor.
pub const ALL: &[&ToolVendor] = &[&linkup::VENDOR, &tinyfish::VENDOR];

/// The tool vendor whose id is `id`.
pub fn find(id: &str) -> Option<&'static ToolVendor> {
    ALL.iter().copied().find(|v| v.id == id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;
    use std::collections::HashSet;

    #[test]
    fn vendors_are_well_formed() {
        let ids: HashSet<_> = ALL.iter().map(|v| v.id).collect();
        assert_eq!(ids.len(), ALL.len(), "vendor ids are unique");
        for vendor in ALL {
            assert_eq!(find(vendor.id).map(|v| v.id), Some(vendor.id));
            let tools = vendor.tools.list();
            assert!(!tools.is_empty(), "{} has tools", vendor.id);
            let names: HashSet<_> = tools.iter().map(|t| t.name).collect();
            assert_eq!(
                names.len(),
                tools.len(),
                "{}'s tool names are unique",
                vendor.id
            );
            for tool in tools {
                let label = format!("{}.{}", vendor.id, tool.name);
                assert!(
                    tool.name
                        .bytes()
                        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_'),
                    "{label} is snake_case"
                );
                assert!(tool.name.len() <= 32, "{label} is short");
                assert!(!tool.description.is_empty(), "{label} is described");
                let schema: Value = serde_json::from_str(tool.input_schema)
                    .unwrap_or_else(|e| panic!("{label}'s schema parses: {e}"));
                assert_eq!(schema["type"], "object", "{label}'s schema is an object");
                let properties = schema["properties"].as_object().expect("properties");
                for required in schema["required"].as_array().into_iter().flatten() {
                    let required = required.as_str().expect("a name");
                    assert!(
                        properties.contains_key(required),
                        "{label} requires {required}, a property"
                    );
                }
            }
        }
        assert!(find("nope").is_none());
    }
}
