//! Gemini's quirks.

use respan_gateway_providers::gemini;

#[test]
fn model_id_prefix() {
    let id = gemini::PROVIDER.model_id("models/gemini-2.5-flash");
    assert_eq!(id, "gemini-2.5-flash");
    assert_eq!(gemini::PROVIDER.model_id("gemini-2.5-flash"), id);
}
