//! Where a value came from, so its format writes it as its bytes while it
//! says the same, and writes what changed into them.

use std::sync::Arc;

use respan_gateway_core::bytes::Bytes;

/// The bytes a value was decoded from, in `format`, and what they said.
#[derive(Debug)]
#[non_exhaustive]
pub struct Origin<T> {
    /// The name of the format that decoded the bytes.
    pub format: Arc<str>,
    /// The message as it came.
    pub bytes: Bytes,
    /// What the bytes said: the value as decoded.
    pub said: T,
}

/// Where a value came from, if decoded; never part of what it says, so
/// always equal.
#[derive(Debug)]
pub struct Relayed<T>(Option<Arc<Origin<T>>>);

/// Came from nowhere: made by a plugin.
impl<T> Default for Relayed<T> {
    fn default() -> Self {
        Self(None)
    }
}

/// Shares the origin.
impl<T> Clone for Relayed<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

/// Where a value came from is not what it says.
impl<T> PartialEq for Relayed<T> {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl<T: PartialEq> Relayed<T> {
    /// Decoded by `format` from `bytes`, which say `said`.
    pub fn came(format: &Arc<str>, bytes: Bytes, said: T) -> Self {
        let format = format.clone();
        Self(Some(Arc::new(Origin {
            format,
            bytes,
            said,
        })))
    }

    /// Its origin if `format` decoded it: the shape to write changes into.
    pub fn origin(&self, format: &str) -> Option<&Origin<T>> {
        self.0.as_deref().filter(|o| *o.format == *format)
    }

    /// The bytes to write for `now` in `format`, while it says what they said.
    pub fn bytes(&self, format: &str, now: &T) -> Option<&Bytes> {
        self.origin(format)
            .filter(|o| o.said == *now)
            .map(|o| &o.bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_value_is_written_as_its_bytes_only_by_its_format_while_unchanged() {
        let format: Arc<str> = Arc::from("f");
        let relayed = Relayed::came(&format, Bytes::from_static(b"x"), 1);
        assert_eq!(relayed.bytes("f", &1), Some(&Bytes::from_static(b"x")));
        assert_eq!(relayed.bytes("f", &2), None);
        assert_eq!(relayed.bytes("g", &1), None);
        assert!(relayed.origin("f").is_some_and(|o| o.said == 1));
        assert!(relayed == Relayed::default());
    }
}
