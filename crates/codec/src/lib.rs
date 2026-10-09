#![warn(missing_docs)]
//! What the conversation codecs share. A format supplies its [`Codec`]: its
//! names, how it reads and writes requests, and its response reader and
//! encoders; the HTTP codec around them, on either edge, is the trait's
//! provided methods. Shared message types live in
//! [`respan_gateway_conversation`]; wire helpers in
//! [`respan_gateway_http::formats`].
//!
//! Each codec decodes a request into an [`Input`], keeping the fields the
//! kind has no place for in its `native`, and writes a request from the
//! value alone. A response, streamed or whole, decodes into the events of
//! [`Output`] and is encoded from them, so what survives a relay is what
//! the events and `native` say, not the provider's bytes. Only an explicit
//! raw relay ([`respan_gateway_http::relay::Relay::RAW`]) sends the client's
//! own bytes.
//!
//! What a format has no form for stays in its own terms (optional
//! `NativeFields`, a `Content::Native`, a `Output::Native`) and is written
//! only by that format; translating it is refused where it could change the
//! answer. A request that asks for several choices (`n > 1`) cannot be
//! written and is refused.

mod errors;
mod exchange;
mod values;

pub use errors::{answer, failure_json, merge_details, says, says_error};
pub use exchange::{Asked, Gate, OutputParser, error_event, probe, unreadable_event};
pub use values::{
    Keep, Names, NativeFieldsExt, Totals, count, each_tool, ended, fits, is_inert, kept_fields,
    merge_fields, merge_missing, native_fields, only, read_content, read_message, read_tools,
    remainder, rest_of, set, shows, stop_json, supported_usage, tidy, tool_field, unix_now,
    unsupported_fields, untranslatable, unwritable_content, unwritable_usage, usage_extra,
    write_content, write_message, write_native,
};

use respan_gateway_conversation::{Assembled, Content, Input, Output};
use respan_gateway_http::bytes::Bytes;
use respan_gateway_http::http::{
    self, StatusCode, request::Parts as RequestHead, response::Parts as ResponseHead,
};
use respan_gateway_http::{Body, Encoder, Incoming, Outgoing, Reply, bodiless};
use respan_gateway_sdk::{Connection, EventSink, EventStream, Fault};
use serde_json::{Map, Value};

/// Provider HTTP delivery preference, set in call extensions.
/// Without an override the HTTP binding follows the original client request,
/// or requests a stream when there is no original HTTP request.
/// Standalone semantic request encoding produces a finite request.
/// Client response framing remains determined by the client request.
#[derive(Debug, Clone, Copy)]
pub struct StreamResponse(pub bool);

/// What a conversation format supplies; the HTTP codec around it, at both
/// edges, is shared: these provided methods, which a format's `Format`
/// implementations call ([`Codec::bind_client`], [`Codec::bind_provider`]).
pub trait Codec: Copy + Send + Sync + 'static {
    /// The wire format's name, which tags what its values hold that the
    /// kind has no place for (`NativeFields`, `Content::Native`,
    /// `Output::Native`, a native role or finish). The dialects of one wire
    /// format share it, so each writes what another read.
    const NAME: &'static str;
    /// The path a request is posted to, which a client's request ends with.
    const PATH: &'static str;
    /// Headers a request to a provider gets if it has none of that name.
    const HEADERS: &'static [(&'static str, &'static str)] = &[];
    /// The keys of a conversation's `native` this format writes itself, from
    /// the conversation and the native together.
    const OWN: &'static [&'static str] = &[];
    /// Decodes a response, whole or streamed, into events.
    type Reading: Reader;

    /// This format's name: [`Codec::NAME`], or its dialect's own.
    fn name(&self) -> &'static str {
        Self::NAME
    }
    /// A decoder of one response.
    fn reader(&self) -> Self::Reading;
    /// A JSON request as a conversation, noting what it has no place for.
    fn read(&self, v: &Value) -> Input;
    /// A conversation as a request, but for its own `native` fields and the
    /// model, refusing what this format cannot say.
    fn write(&self, chat: &Input) -> Result<Map<String, Value>, Fault>;
    /// A message part the kind has a form for beyond text, such as media or
    /// a tool block; `None` keeps the part this format's own.
    fn read_part(p: &Value) -> Option<Content>;
    /// A part beyond text and this format's own parts, as it writes it, or
    /// refused.
    fn write_part(c: &Content) -> Result<Value, Fault>;
    /// Whether a successful body is a whole response of this format.
    fn is_response(v: &Value) -> bool;
    /// The events of a response, collected, as one whole body.
    fn render(&self, whole: &Assembled) -> Result<Value, Fault>;
    /// The encoder of a stream to a client that [`Asked`] so.
    fn stream_encoder(&self, asked: Asked) -> Box<dyn Encoder<Output>>;
    /// The error body for `fault`, its details merged by [`merge_details`].
    fn error_body(fault: &Fault) -> Value;
    /// The status HTTP names for what a provider's error answer of `status`
    /// says: `status` itself, unless the vendor uses one of its own.
    fn status(status: StatusCode) -> StatusCode {
        status
    }
    /// Add to a request what this format needs, such as asking for usage.
    fn finish(_v: &mut Map<String, Value>) {}
    /// Spell the fields of `v` as `original`, the client's request in this
    /// format, did, where the format has two spellings of one field.
    fn respell(_original: &Value, _v: &mut Map<String, Value>) {}

    /// Whether a client's request is of this format: a `POST` to a path
    /// ending in [`Codec::PATH`].
    fn accepts<B>(&self, request: &http::Request<B>) -> bool {
        request.method() == http::Method::POST
            && respan_gateway_http::formats::path_ends_with(request.uri(), Self::PATH)
    }

    /// Read a request lazily: the decoder of a client's request.
    fn read_request(&self, request: http::Request<Body>) -> EventStream<Input> {
        let codec = *self;
        respan_gateway_http::messages(request, move |_, raw| codec.decode_request(raw))
    }

    /// Decode a finite JSON request synchronously.
    fn decode_request(&self, raw: Bytes) -> Result<Input, Fault> {
        let v = match serde_json::from_slice::<Value>(&raw) {
            Ok(v) if v.is_object() => v,
            _ => return Err(values::not_an_object()),
        };
        let mut chat = self.read(&v);
        chat.model = v["model"].as_str().map(Into::into);
        Ok(chat)
    }

    /// A conversation as a request's JSON body, if nothing is lost: its
    /// fields the kind has no place for are written only if they are this
    /// format's.
    fn encode_request(&self, chat: &Input) -> Result<Map<String, Value>, Fault> {
        let keep = Keep {
            own: Self::OWN,
            droppable: &[],
        };
        let in_the_request = |key: &str| format!("'{key}' in the request");
        let native = kept_fields(Self::NAME, &chat.native, in_the_request, keep)?;
        let mut v = Map::new();
        set(&mut v, "model", chat.model.clone().map(Into::into));
        v.extend(self.write(chat)?);
        v.extend(native);
        Ok(v)
    }

    /// Read provider response events with fresh per-body parser state: the
    /// decoder of a provider's answer.
    fn read_response(
        &self,
        response: http::Response<Body>,
        call: http::Request<Bytes>,
    ) -> EventStream<Output> {
        exchange::read_response(*self, response, &call.into_parts().0)
    }

    /// Decode a finite answer synchronously, without polling an async stream.
    fn decode_response(
        &self,
        response: http::Response<Bytes>,
        call: &RequestHead,
    ) -> Result<Vec<Output>, Fault> {
        exchange::decode_response(*self, response, call)
    }

    /// Select response framing and its encoder: the client's, whatever the
    /// provider's was.
    fn response_encoder(
        &self,
        head: &mut ResponseHead,
        request: &http::Request<Bytes>,
    ) -> Box<dyn Encoder<Output>> {
        exchange::response_encoder(*self, head, request)
    }

    /// Bind response delivery, its head the one a plugin chose, else a
    /// success's: the encoder of a client's response.
    fn write_response(&self, reply: Reply) -> Result<EventSink<Output>, Fault> {
        let mut head = reply.head();
        let encoder = self.response_encoder(&mut head, reply.request());
        Ok(reply.write(head, encoder))
    }

    /// Render an HTTP refusal in this format's error schema: the error
    /// writer of a client's response.
    fn refusal(
        &self,
        fault: &Fault,
        _request: &http::Request<Bytes>,
        head: &mut ResponseHead,
    ) -> Bytes {
        exchange::refusal::<Self>(fault, head)
    }

    /// Encode a finite answer synchronously, including finalization errors;
    /// one HTTP gives no body ([`bodiless`]) has none.
    fn encode_response(
        &self,
        request: &http::Request<Bytes>,
        replies: &[Output],
    ) -> Result<http::Response<Bytes>, Fault> {
        let mut head = http::Response::new(()).into_parts().0;
        let encoder = self.response_encoder(&mut head, request);
        let bytes = match bodiless(request.method(), head.status) {
            true => Bytes::new(),
            false => respan_gateway_http::encode_all(encoder, replies)?,
        };
        Ok(http::Response::from_parts(head, bytes))
    }

    /// This format at the client edge: requests in, responses out.
    fn bind_client(&self, io: Incoming) -> Result<Connection<Input, Output>, Fault> {
        let codec = *self;
        Ok(io.bind(
            move |request| codec.read_request(request),
            move |reply| codec.write_response(reply),
            move |fault, request, head| codec.refusal(fault, request, head),
        ))
    }

    /// This format at the provider edge: a call out, its response in, as the
    /// call's [`StreamResponse`] and the client's request ask.
    fn bind_provider(&self, io: Outgoing) -> Result<Connection<Output, Input>, Fault> {
        let codec = *self;
        let stream = io.extensions().get::<StreamResponse>().copied();
        let wire = io.extensions().get::<respan_gateway_http::Wire>().cloned();
        Ok(io.bind(
            move |head, input| exchange::post(codec, head, input, wire.as_ref(), stream),
            move |response, call| codec.read_response(response, call),
            move |answer| errors::answer(&codec, answer),
        ))
    }
}

/// A format's decoder of a response into events, which holds what it needs
/// between the events of one body.
pub trait Reader: Send + 'static {
    /// The events of one stream event, `raw`.
    fn event(&mut self, raw: Bytes, out: &mut Vec<Output>) -> Result<(), Fault>;
    /// The events of a stream that has ended, or an error if it was cut.
    fn end(&mut self, out: &mut Vec<Output>) -> Result<(), Fault>;
    /// The events of one whole response body `v`.
    fn body(&mut self, v: &Value, out: &mut Vec<Output>) -> Result<(), Fault>;
}
