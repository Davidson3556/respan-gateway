//! A tool's result: text, and media MCP carries as content blocks.

use std::collections::BTreeMap;

use base64::Engine as _;
use base64::engine::DecodePaddingMode;
use base64::engine::general_purpose::{GeneralPurpose, GeneralPurposeConfig, STANDARD};
use serde_json::{Value, json};

/// One block of a tool's result, as MCP's `tools/call` has it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Content {
    /// Text for the model.
    Text(String),
    /// An image: its bytes and media type.
    Image {
        /// The image's bytes.
        data: Vec<u8>,
        /// Its media type, such as `image/png`.
        mime_type: String,
    },
    /// Audio: its bytes and media type.
    Audio {
        /// The audio's bytes.
        data: Vec<u8>,
        /// Its media type, such as `audio/mpeg`.
        mime_type: String,
    },
}

impl Content {
    /// The block as MCP's JSON, media in base64.
    pub fn to_json(&self) -> Value {
        match self {
            Self::Text(text) => json!({"type": "text", "text": text}),
            Self::Image { data, mime_type } => {
                json!({"type": "image", "data": STANDARD.encode(data), "mimeType": mime_type})
            }
            Self::Audio { data, mime_type } => {
                json!({"type": "audio", "data": STANDARD.encode(data), "mimeType": mime_type})
            }
        }
    }
}

/// Standard base64, padded or not, whitespace ignored, as APIs send media
/// in JSON; `None` when it isn't base64.
pub fn decode_base64(text: &str) -> Option<Vec<u8>> {
    const LENIENT: GeneralPurpose = GeneralPurpose::new(
        &base64::alphabet::STANDARD,
        GeneralPurposeConfig::new().with_decode_padding_mode(DecodePaddingMode::Indifferent),
    );
    let digits: Vec<u8> = text.bytes().filter(|c| !c.is_ascii_whitespace()).collect();
    LENIENT.decode(digits).ok()
}

/// What a call used, as its vendor measures it, by unit: `credits`,
/// `pages`, `characters`, `usd` and the like.
pub type ToolUsage = BTreeMap<&'static str, f64>;

/// A tool's result: its blocks, in order, and what the call used.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ToolOutput {
    /// Its blocks, for the model.
    pub content: Vec<Content>,
    /// What the call used: read from the vendor's answer, or worked out
    /// from the arguments; empty if neither says.
    pub usage: ToolUsage,
}

impl ToolOutput {
    /// It, having used `amount` of `unit`, such as `("credits", 2.0)`.
    pub fn with_usage(mut self, unit: &'static str, amount: f64) -> Self {
        self.usage.insert(unit, amount);
        self
    }
}

impl From<String> for ToolOutput {
    fn from(text: String) -> Self {
        vec![Content::Text(text)].into()
    }
}

impl From<&str> for ToolOutput {
    fn from(text: &str) -> Self {
        text.to_owned().into()
    }
}

impl From<Content> for ToolOutput {
    fn from(block: Content) -> Self {
        vec![block].into()
    }
}

impl From<Vec<Content>> for ToolOutput {
    fn from(content: Vec<Content>) -> Self {
        Self {
            content,
            usage: ToolUsage::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn blocks_are_mcps_json() {
        assert_eq!(
            Content::Text("hi".into()).to_json(),
            json!({"type": "text", "text": "hi"})
        );
        let audio = Content::Audio {
            data: b"abc".to_vec(),
            mime_type: "audio/mpeg".into(),
        };
        assert_eq!(
            audio.to_json(),
            json!({"type": "audio", "data": "YWJj", "mimeType": "audio/mpeg"})
        );
        assert_eq!(
            ToolOutput::from("x"),
            ToolOutput::from(vec![Content::Text("x".into())])
        );
    }
}
