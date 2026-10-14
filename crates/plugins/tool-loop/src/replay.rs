//! The numbering of what the model said, in each format's natives.

use respan_gateway_conversation::Output;
use respan_gateway_core::NativeValue;
use serde_json::Value;

/// The block an event names, by `index` (Anthropic's) or `block` (the
/// others' natives).
pub(super) fn block_of(raw: &Value) -> Option<u32> {
    let named = raw["index"].as_u64().or_else(|| raw["block"].as_u64());
    u32::try_from(named?).ok()
}

/// `native`, which names `block`, naming `number`.
pub(super) fn renumbered(native: &NativeValue, block: u32, number: u32) -> Output {
    let mut value = (*native.value).clone();
    if let Some(object) = value.as_object_mut() {
        let key = if object.contains_key("index") && object["index"] == block {
            "index"
        } else {
            "block"
        };
        object.insert(key.into(), number.into());
    }
    Output::Native(NativeValue::new(native.format.clone(), value))
}
