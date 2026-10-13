//! Anthropic: Messages.

use respan_gateway_conversation_http::anthropic::AnthropicMessages;

use crate::{Format, Speaks, Vendor};

/// Anthropic. Its base URL leaves out the version.
pub const VENDOR: Vendor = Vendor {
    id: "anthropic",
    base_url: Some("https://api.anthropic.com"),
    key_env: &["ANTHROPIC_API_KEY"],
    example_model: "claude-sonnet-4-5",
    formats: &[Speaks::new(Format::AnthropicMessages(AnthropicMessages)).key_in("x-api-key")],
    model_id_prefix: None,
    quirks: &[],
    plugin: None,
};
