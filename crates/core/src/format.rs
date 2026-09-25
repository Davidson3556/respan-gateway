use bytes::{Bytes, BytesMut};

use crate::{Doc, Envelope, Extensions, Fault, Part, ResponseHead};

/// A wire protocol. Stateless and shared; each stream gets its own
/// [`Decoder`] and [`Encoder`].
pub trait Format: Send + Sync + 'static {
    /// Referenced by `format:` in config.
    fn name(&self) -> &'static str;

    /// The model and streaming flag, read from the body or, for protocols
    /// that carry them elsewhere, from the caller's extensions.
    fn envelope(&self, body: &Doc, ext: &Extensions) -> Envelope;

    /// The path to call on a provider speaking this format.
    fn provider_path(&self, envelope: &Envelope) -> String;

    /// Protocol-level changes to a request before it goes to a provider:
    /// the model override, asking for streamed usage, and so on.
    fn prepare_request(
        &self,
        body: &mut Doc,
        model: Option<&str>,
        stream: bool,
    ) -> Result<(), Fault>;

    fn decoder(&self, head: &ResponseHead) -> Box<dyn Decoder>;

    fn encoder(&self, head: &ResponseHead) -> Box<dyn Encoder>;

    /// An error in this protocol's framing. `in_stream` is true once a
    /// streamed response has started, so the error must be a stream frame.
    fn fault(&self, fault: &Fault, in_stream: bool) -> Bytes;

    /// Content type for [`Format::fault`] bodies sent before the commit.
    fn fault_content_type(&self) -> &'static str {
        "application/json"
    }
}

/// Bytes to Parts, for one stream.
pub trait Decoder: Send + 'static {
    fn decode(&mut self, bytes: Bytes, out: &mut Vec<Part>);

    /// The stream ended. Emit anything buffered, and `End(Failed(truncated))`
    /// if the terminal marker never arrived.
    fn finish(&mut self, out: &mut Vec<Part>);
}

/// Parts to bytes, for one stream.
pub trait Encoder: Send + 'static {
    fn encode(&mut self, part: &Part, out: &mut BytesMut);

    fn finish(&mut self, _out: &mut BytesMut) {}
}
