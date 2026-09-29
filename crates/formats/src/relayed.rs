//! Where a value came from, so its format writes it as its bytes while it
//! says the same, and writes what changed into them.

use std::sync::Arc;

use respan_gateway_core::{Fault, codes};
use respan_gateway_http::bytes::Bytes;

/// The bytes a value was decoded from, in `format`, and what they said.
#[derive(Debug)]
#[non_exhaustive]
pub struct Came<T> {
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
pub struct Relayed<T>(Option<Arc<Came<T>>>);

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
        Self(Some(Arc::new(Came {
            format,
            bytes,
            said,
        })))
    }

    /// Its origin if `format` decoded it: the shape to write changes into.
    pub fn came_in(&self, format: &str) -> Option<&Came<T>> {
        self.0.as_deref().filter(|o| *o.format == *format)
    }

    /// What the bytes said, if decoded by any format.
    pub fn said(&self) -> Option<&T> {
        self.0.as_deref().map(|o| &o.said)
    }

    /// The bytes to write for `now` in `format`, while it says what they said.
    pub fn bytes(&self, format: &str, now: &T) -> Option<&Bytes> {
        self.came_in(format)
            .filter(|o| o.said == *now)
            .map(|o| &o.bytes)
    }

    /// `fault`, of `now` that a format cannot write, as whose it is:
    /// [`codes::BAD_REQUEST`], the client's, if `now` says what its bytes
    /// said, in whichever format; else [`codes::CONFIG`], a plugin's.
    pub fn whose(&self, now: &T, fault: Fault) -> Fault {
        let clients = self.0.as_ref().is_some_and(|o| o.said == *now);
        let code = if clients {
            codes::BAD_REQUEST
        } else {
            codes::CONFIG
        };
        Fault::new(code, fault.message().to_owned()).with_source(fault)
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
        assert!(relayed.came_in("f").is_some_and(|o| o.said == 1));
        assert!(relayed == Relayed::default());
    }

    #[test]
    fn a_value_that_cannot_be_written_is_the_clients_only_as_decoded() {
        let format: Arc<str> = Arc::from("f");
        let relayed = Relayed::came(&format, Bytes::from_static(b"x"), 1);
        let whose = |r: &Relayed<i32>, now| r.whose(&now, Fault::internal("no")).code().to_owned();
        assert_eq!(whose(&relayed, 1), codes::BAD_REQUEST);
        assert_eq!(whose(&relayed, 2), codes::CONFIG);
        assert_eq!(whose(&Relayed::default(), 1), codes::CONFIG);
    }
}
