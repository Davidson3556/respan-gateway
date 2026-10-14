//! The numbering of what the model said, in each format's natives.

use respan_gateway_conversation::Output;
use respan_gateway_core::NativeValue;

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
