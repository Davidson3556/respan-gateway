//! Bodies read whole, as unary responses are, and written back.

use std::marker::PhantomData;

use respan_gateway_core::bytes::{Bytes, BytesMut};
use respan_gateway_core::http::{self, StatusCode};
use respan_gateway_core::{Decoder, Encoder, Event, Fault, Part};

/// A decoder for a body read whole. For a success, `on_body` gets all the
/// bytes when the body ends; the response ends `Ok` unless `on_body` pushed
/// an end or returned `Err`. A failure status yields the body as a Frame,
/// then an `Err` end with a relayed fault of that status, the provider's
/// code and message (else `upstream_error` and "the provider answered N"),
/// and its error fields as details. An error inside a success is for
/// `on_body` to judge.
pub fn decoder<F>(head: &http::response::Parts, mut on_body: F) -> Box<dyn Decoder>
where
    F: FnMut(Bytes, &mut Vec<Part>) -> Result<(), Fault> + Send + 'static,
{
    let status = head.status;
    Box::new(Whole::new(move |raw, out: &mut Vec<Part>| {
        match status.is_success() {
            false => {
                // The provider's words, if its body says an error as JSON.
                let v = crate::sse::failure_json(&raw).unwrap_or_default();
                let fault = crate::common::provider_error(status, &v);
                out.push(Part::Frame(raw));
                out.push(Part::End(Err(fault.said())));
            }
            true => {
                if let Err(fault) = on_body(raw, out) {
                    out.push(Part::End(Err(fault)));
                }
            }
        }
    }))
}

/// Hands a whole body to `on_body`, then ends `Ok` unless it pushed an end.
pub(crate) struct Whole<F> {
    buf: BytesMut,
    on_body: F,
}

impl<F> std::fmt::Debug for Whole<F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Whole")
            .field("buffered", &self.buf.len())
            .finish_non_exhaustive()
    }
}

impl<F: FnMut(Bytes, &mut Vec<Part>) + Send + 'static> Whole<F> {
    pub(crate) fn new(on_body: F) -> Self {
        Self {
            buf: BytesMut::new(),
            on_body,
        }
    }
}

impl<F> Decoder for Whole<F>
where
    F: FnMut(Bytes, &mut Vec<Part>) + Send + 'static,
{
    fn decode(&mut self, bytes: Bytes, _out: &mut Vec<Part>) {
        self.buf.extend_from_slice(&bytes);
    }

    fn finish(&mut self, out: &mut Vec<Part>) {
        let raw = self.buf.split().freeze();
        let from = out.len();
        (self.on_body)(raw, out);
        if !out[from..].iter().any(|p| matches!(p, Part::End(_))) {
            out.push(Part::End(Ok(())));
        }
    }
}

/// The one wording of a provider's failure of `status`.
pub(crate) fn answered(status: StatusCode) -> String {
    format!("the provider answered {}", status.as_u16())
}

/// Encodes a response of events of `T`.
///
/// Unedited items and Frames are written as the provider's bytes. Made or
/// edited items go through `write`, which also gets the bytes an edited
/// item came from, so it can keep their shape; it returns `Err` for what it
/// cannot write. A failure goes through `fail`. A stream is written item by
/// item. A whole body is written once, at its end, as one document of all
/// its events; Frames after its first item follow that document. A failed
/// whole body writes its Frames and the failure, not its items.
/// `Part::Usage` is not written.
pub struct Relay<T, W, F, B> {
    write: W,
    fail: F,
    /// What `write` and `fail` return, such as a `String` or `Bytes`.
    bytes: PhantomData<fn() -> B>,
    /// A whole body held until it ends; `None` for a stream.
    whole: Option<Held<T>>,
}

struct Held<T> {
    events: Vec<T>,
    /// The bytes of the first item.
    raw: Option<Bytes>,
    /// Whether `raw` still says the whole body, unedited.
    as_raw: bool,
    /// Whether an item came; later Frames wait for the body.
    seen: bool,
    /// Frames after the first item, written after the body.
    after: BytesMut,
}

impl<T, W, F, B> std::fmt::Debug for Relay<T, W, F, B> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Relay").finish_non_exhaustive()
    }
}

impl<T, W, F, B> Relay<T, W, F, B>
where
    W: FnMut(&[T], Option<&Bytes>) -> Result<B, Fault> + Send + 'static,
    F: FnMut(&Fault) -> B + Send + 'static,
    B: AsRef<[u8]> + 'static,
{
    /// An encoder for a `stream`, or for a whole body if `stream` is false.
    pub fn new(write: W, fail: F, stream: bool) -> Self {
        let held = Held {
            events: Vec::new(),
            raw: None,
            as_raw: true,
            seen: false,
            after: BytesMut::new(),
        };
        Self {
            write,
            fail,
            bytes: PhantomData,
            whole: (!stream).then_some(held),
        }
    }
}

impl<T, W, F, B> Encoder for Relay<T, W, F, B>
where
    T: Event,
    W: FnMut(&[T], Option<&Bytes>) -> Result<B, Fault> + Send + 'static,
    F: FnMut(&Fault) -> B + Send + 'static,
    B: AsRef<[u8]> + 'static,
{
    fn encode(&mut self, part: &Part, out: &mut BytesMut) -> Result<(), Fault> {
        match (part, &mut self.whole) {
            (Part::Frame(raw), Some(held)) if held.seen => held.after.extend_from_slice(raw),
            (Part::Frame(raw), _) => out.extend_from_slice(raw),
            (Part::Item(item), Some(held)) => {
                held.seen = true;
                held.as_raw &= held.raw.is_none() && item.unedited().is_some();
                held.raw = held.raw.take().or_else(|| item.raw().cloned());
                held.events.extend_from_slice(item.events::<T>()?);
            }
            (Part::Item(item), None) => match (item.unedited(), item.events::<T>()?) {
                (Some(raw), _) => out.extend_from_slice(raw),
                // A filter emptied the item: write nothing.
                (None, []) => {}
                (None, events) => out.extend_from_slice((self.write)(events, item.raw())?.as_ref()),
            },
            // A body with no item, such as one passed on as Frames, has
            // nothing to write.
            (Part::End(Ok(())), Some(held)) if held.seen => {
                match (&held.raw, held.as_raw) {
                    (Some(raw), true) => out.extend_from_slice(raw),
                    (raw, _) => {
                        out.extend_from_slice((self.write)(&held.events, raw.as_ref())?.as_ref())
                    }
                }
                out.extend_from_slice(&held.after);
            }
            (Part::End(Err(f)), held) => {
                // Frames after the first item are written before a failure.
                if let Some(held) = held {
                    out.extend_from_slice(&held.after);
                }
                out.extend_from_slice((self.fail)(f).as_ref());
            }
            (Part::Usage(_) | Part::End(Ok(())), _) => {}
            _ => return Err(crate::common::unknown_part()),
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use respan_gateway_core::Item;

    #[test]
    fn a_whole_body_is_written_once_in_order() {
        let write = |events: &[u32], _: Option<&Bytes>| Ok(format!("{events:?}"));
        let mut relay = Relay::new(write, |_: &Fault| String::new(), false);
        let mut out = BytesMut::new();
        let parts = [
            Part::Frame(Bytes::from_static(b"<")),
            Item::decoded([1u32], Bytes::from_static(b"one")).into(),
            Item::new([2u32]).into(),
            Part::Frame(Bytes::from_static(b">")),
            Part::End(Ok(())),
        ];
        for part in &parts {
            assert!(relay.encode(part, &mut out).is_ok());
        }
        assert_eq!(&out[..], b"<[1, 2]>");
    }

    #[test]
    fn a_whole_body_of_frames_alone_is_written_as_it_came() {
        let write = |_: &[u32], _: Option<&Bytes>| Ok(String::from("made up"));
        let mut relay = Relay::new(write, |_: &Fault| String::new(), false);
        let mut out = BytesMut::new();
        for part in [
            Part::Frame(Bytes::from_static(b"as it came")),
            Part::End(Ok(())),
        ] {
            assert!(relay.encode(&part, &mut out).is_ok());
        }
        assert_eq!(&out[..], b"as it came");
    }

    #[test]
    fn a_frame_after_an_emptied_first_item_still_follows_the_body() {
        let write = |events: &[u32], _: Option<&Bytes>| Ok(format!("{events:?}"));
        let mut relay = Relay::new(write, |_: &Fault| String::new(), false);
        let mut out = BytesMut::new();
        let parts = [
            Item::new(Vec::<u32>::new()).into(),
            Part::Frame(Bytes::from_static(b">")),
            Part::End(Ok(())),
        ];
        for part in &parts {
            assert!(relay.encode(part, &mut out).is_ok());
        }
        assert_eq!(&out[..], b"[]>");
    }
}
