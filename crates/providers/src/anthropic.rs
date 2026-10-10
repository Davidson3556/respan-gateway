//! Anthropic: Messages.

use respan_gateway_conversation_http::anthropic::AnthropicMessages;

use crate::{Format, Provider, Speaks};

/// Anthropic. Its base URL leaves out the version.
pub const PROVIDER: Provider = Provider {
    id: "anthropic",
    base_url: Some("https://api.anthropic.com"),
    key_env: &["ANTHROPIC_API_KEY"],
    prefixes: &["claude"],
    example_model: "claude-sonnet-4-5",
    formats: &[Speaks::new(Format::AnthropicMessages(AnthropicMessages)).key_in("x-api-key")],
    model_id_prefix: None,
    quirks: &[],
};
