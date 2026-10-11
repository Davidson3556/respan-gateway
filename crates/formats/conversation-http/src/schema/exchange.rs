//! The HTTP exchange of a conversation format: requests posted, responses
//! read into events with a hold of their start, and events encoded into a
//! client's response, streamed or as one whole body.
//!
//! Nothing here reuses the bytes a value was decoded from: values are plain
//! data, and an encoder writes what a value says. Only an explicit raw relay
//! sends the client's bytes ([`respan_gateway_http::SendRaw`]).

use std::sync::Arc;

use respan_gateway_conversation::Usage;
use respan_gateway_conversation::{Assembled, Input, Output};
use respan_gateway_core::{BadRequest, EventStream, Fault, Upstream};
use respan_gateway_http::bytes::{BufMut, Bytes, BytesMut};
use respan_gateway_http::formats::sse::{self, content_type, event_data, is_stream};
use respan_gateway_http::formats::{self, whole};
use respan_gateway_http::http::{self, HeaderValue, StatusCode, header::CONTENT_TYPE};
use respan_gateway_http::{
    Body, Encoder, RawReply, Wire, bodiless, http::request::Parts as RequestHead,
    http::response::Parts as ResponseHead,
};
use serde_json::{Map, Value};

use super::errors::{answer, failure_json, said_error, says_error};
use super::values::{shows, unwritable_content};
use super::{Decoder, Schema, StreamResponse};

/// The call of `input` in `schema`'s format, a JSON `POST` to
/// [`Schema::PATH`] with the format's headers ([`Schema::HEADERS`]), and its
/// encoder ([`RequestEncoder`]). It asks for a stream as the call's
/// [`StreamResponse`] says, else as the client's original request
/// ([`Wire`]) did, else for one; the client's request, if of this format,
/// gives its spelling of a field the format spells two ways
/// ([`Schema::respell`]). `Err` with `config` for live input.
pub(crate) fn post<C: Schema>(
    schema: C,
    head: &mut RequestHead,
    input: Option<&Input>,
    wire: Option<&Wire>,
    stream: Option<StreamResponse>,
) -> Result<Box<dyn Encoder<Input>>, Fault> {
    if input.is_none() {
        return Err(Fault::config(format!(
            "{} does not accept streaming inputs",
            C::NAME
        )));
    }
    head.method = http::Method::POST;
    formats::set_path(head, C::PATH)?;
    head.headers
        .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    for (name, value) in C::HEADERS {
        let value = HeaderValue::from_static(value);
        head.headers.entry(*name).or_insert(value);
    }
    let original = wire.and_then(Wire::get);
    let stream = stream
        .map(|s| s.0)
        .unwrap_or_else(|| original.is_none_or(|r| probe(r.body()).stream));
    let original = original
        .filter(|r| r.uri().path().ends_with(C::PATH))
        .map(|r| serde_json::from_slice(r.body()).unwrap_or(Value::Null));
    Ok(Box::new(RequestEncoder {
        schema,
        stream,
        original,
    }))
}

/// Writes a conversation as a request, asking for a stream if `stream`, in
/// the spelling of the client's `original` request, then what the format
/// adds ([`Schema::finish`]). What cannot be written is the client's fault:
/// without the bytes it came as, the gateway cannot tell an edit from the
/// request.
struct RequestEncoder<C> {
    schema: C,
    stream: bool,
    original: Option<Value>,
}

impl<C: Schema> Encoder<Input> for RequestEncoder<C> {
    fn encode(&mut self, chat: &Input, out: &mut BytesMut) -> Result<(), Fault> {
        let mut v = self
            .schema
            .encode_request(chat)
            .map_err(|f| match f.is::<BadRequest>() {
                true => f,
                false => BadRequest::fault(f.message()).with_source(f),
            })?;
        if self.stream {
            v.insert("stream".into(), true.into());
        }
        if let Some(original) = &self.original {
            C::respell(original, &mut v);
        }
        C::prepare_call(&mut v);
        out.extend_from_slice(&to_bytes(&v)?);
        Ok(())
    }
}

/// `v` as JSON bytes.
fn to_bytes(v: &Map<String, Value>) -> Result<Bytes, Fault> {
    let mut out = BytesMut::new().writer();
    let written = serde_json::to_writer(&mut out, v);
    written.map_err(|e| Fault::internal("a request could not be written").with_source(e))?;
    Ok(out.into_inner().freeze())
}

/// What a JSON request asks of its response: a stream, and usage in it.
#[derive(Debug, Default, Clone, Copy)]
pub struct Asked {
    /// Whether it asks for a stream.
    pub stream: bool,
    /// Whether it asks for usage in the stream.
    pub usage: bool,
}

/// [`Asked`] of a JSON body, reading only its two fields.
pub(crate) fn probe(raw: &[u8]) -> Asked {
    #[derive(serde::Deserialize)]
    struct Probe {
        #[serde(default)]
        stream: Value,
        #[serde(default)]
        stream_options: Value,
    }
    match serde_json::from_slice::<Probe>(raw) {
        Ok(p) => Asked {
            stream: p.stream == true,
            usage: p.stream_options["include_usage"] == true,
        },
        Err(_) => Asked::default(),
    }
}

/// A response of `schema`'s format to the call `call`: nothing if it has no
/// body, else the events of a stream, else those of one JSON message, or
/// the provider's error if it is no success ([`answer`]).
pub(crate) fn read_response<C: Schema>(
    schema: C,
    response: http::Response<Body>,
    call: &RequestHead,
) -> EventStream<Output> {
    let (head, body) = response.into_parts();
    if head.status.is_success() && bodiless(&call.method, head.status) {
        return EventStream::empty();
    }
    if is_stream(&head) {
        let parser = OutputParser::new(schema.decoder(), true, body.limit());
        return formats::parse(
            body,
            parser,
            |parser, bytes, out| parser.push(bytes, out),
            |parser, wire, out| {
                wire?;
                parser.finish(out)
            },
        );
    }
    whole::read(body, move |raw, out| {
        if !head.status.is_success() {
            return Err(answer(&schema, http::Response::from_parts(head, raw)));
        }
        out.extend(unary_reply(schema, head.status, raw)?);
        Ok(())
    })
}

/// [`read_response`] of a finite answer, synchronously.
pub(crate) fn decode_response<C: Schema>(
    schema: C,
    response: http::Response<Bytes>,
    call: &RequestHead,
) -> Result<Vec<Output>, Fault> {
    let (head, bytes) = response.into_parts();
    if head.status.is_success() && bodiless(&call.method, head.status) {
        return Ok(Vec::new());
    }
    if is_stream(&head) {
        let mut parser = OutputParser::new(schema.decoder(), true, None);
        let mut out = Vec::new();
        parser.push(bytes, &mut out)?;
        parser.finish(&mut out)?;
        return Ok(out);
    }
    if !head.status.is_success() {
        return Err(answer(&schema, http::Response::from_parts(head, bytes)));
    }
    unary_reply(schema, head.status, bytes)
}

/// One JSON message, decoded whole: the events of a success, else the
/// provider's error.
fn unary_reply<C: Schema>(schema: C, status: StatusCode, raw: Bytes) -> Result<Vec<Output>, Fault> {
    let v = serde_json::from_slice::<Value>(&raw).ok();
    if let Some(v) = v.as_ref().filter(|v| C::is_response(v)) {
        let mut out = Vec::new();
        schema.decoder().body(v, &mut out)?;
        return Ok(out);
    }
    Err(match failure_json(&raw).filter(says_error) {
        Some(v) => said_error(C::NAME, status, raw, &v),
        None => Upstream::fault("the provider's success is not a response of its format"),
    })
}

/// Choose response framing from the explicitly selected head and the
/// client's request, and its encoder: a stream's, as the client [`Asked`],
/// or one whole body's.
pub(crate) fn response_encoder<C: Schema>(
    schema: C,
    head: &mut ResponseHead,
    request: &http::Request<Bytes>,
) -> Box<dyn Encoder<Output>> {
    let asked = probe(request.body());
    // A success head's framing is the client's, whatever the provider's was.
    if is_stream(head) != asked.stream {
        head.headers.remove(CONTENT_TYPE);
    }
    if content_type(head).is_none() {
        let content_type = match asked.stream {
            true => "text/event-stream",
            false => "application/json",
        };
        head.headers
            .insert(CONTENT_TYPE, HeaderValue::from_static(content_type));
    }
    match is_stream(head) {
        true => schema.stream_encoder(asked),
        false => Box::new(UnaryEncoder {
            whole: Assembled::default(),
            schema,
        }),
    }
}

/// A refusal in format `C`: its own words as this format's JSON error.
/// The format has no error under a success, so a refusal under a status
/// that says none is a plain 500, as the adapter writes what no format can.
pub(crate) fn refusal<C: Schema>(fault: &Fault, head: &mut ResponseHead) -> Bytes {
    if !(head.status.is_client_error() || head.status.is_server_error()) {
        head.status = StatusCode::INTERNAL_SERVER_ERROR;
        head.headers.clear();
        let text = HeaderValue::from_static("text/plain; charset=utf-8");
        head.headers.insert(CONTENT_TYPE, text);
        return Bytes::from(fault.message().to_owned());
    }
    let json = HeaderValue::from_static("application/json");
    head.headers.insert(CONTENT_TYPE, json);
    match fault.get::<RawReply>().map(|r| r.0.body()) {
        Some(words) => words.clone(),
        None => C::error_body(fault).to_string().into(),
    }
}

/// Writes one message at its end: the events collected into a whole body. A
/// failure, before any byte, is refused.
struct UnaryEncoder<C> {
    whole: Assembled,
    schema: C,
}

impl<C: Schema> Encoder<Output> for UnaryEncoder<C> {
    fn encode(&mut self, reply: &Output, _out: &mut BytesMut) -> Result<(), Fault> {
        self.whole.push(reply).map_err(|_| unwritable_content())
    }

    fn end(&mut self, end: &Result<(), Fault>, out: &mut BytesMut) -> Result<(), Fault> {
        end.clone()?;
        let body = self.schema.encode_whole(&self.whole)?;
        out.extend_from_slice(body.to_string().as_bytes());
        Ok(())
    }
}

/// What an encoder of a stream has written but keeps back until a reply shows
/// content ([`shows`]), so that a failure before it leaves nothing written
/// and the response is refused, as the decoder holds a start for.
#[derive(Debug, Default)]
pub(crate) struct Gate {
    shown: bool,
    held: BytesMut,
}

impl Gate {
    /// Let through, with what came before, what `reply` made, if it or any
    /// before it shows content.
    pub fn pass(&mut self, reply: &Output, made: &[u8], out: &mut BytesMut) {
        self.held.extend_from_slice(made);
        if self.shown || shows(reply) {
            self.release(out);
        }
    }

    /// Let through all that is held: a stream that ended well shows what it
    /// has.
    pub fn release(&mut self, out: &mut BytesMut) {
        self.shown = true;
        out.extend_from_slice(&self.held.split());
    }
}

/// Incremental provider response parsing, shared by streaming and finite reads.
/// Semantic failures retain held usage; wire failures never finalize the parser.
#[derive(Debug)]
pub(crate) struct OutputParser<R> {
    frames: sse::Parser,
    reading: R,
    held: Vec<Output>,
    through: bool,
}

impl<R: Decoder> OutputParser<R> {
    /// A parser by `reading` that, if `hold_start`, keeps a response's start
    /// and what comes before its first content back until that content
    /// comes, so a failure before it leaves nothing started; it holds up to
    /// `limit` bytes of an event ([`sse::Parser::new`]).
    pub fn new(reading: R, hold_start: bool, limit: Option<usize>) -> Self {
        Self {
            frames: sse::Parser::new(limit),
            reading,
            held: Vec::new(),
            through: !hold_start,
        }
    }

    /// The events of the stream's next `bytes`.
    pub fn push(&mut self, bytes: Bytes, out: &mut Vec<Output>) -> Result<(), Fault> {
        let from = out.len();
        let result = self.frames.push(bytes).and_then(|events| {
            events
                .into_iter()
                .try_for_each(|raw| self.reading.event(raw, out))
        });
        self.hold(out, from, result.is_err());
        result
    }

    /// The events of the stream's end, and all that is held.
    pub fn finish(&mut self, out: &mut Vec<Output>) -> Result<(), Fault> {
        let from = out.len();
        let result = self
            .frames
            .finish()
            .into_iter()
            .try_for_each(|raw| self.reading.event(raw, out))
            .and_then(|()| self.reading.end(out));
        self.hold(out, from, result.is_err());
        out.append(&mut self.held);
        result
    }

    fn hold(&mut self, out: &mut Vec<Output>, from: usize, failed: bool) {
        for reply in out.split_off(from) {
            match reply {
                _ if self.through => out.push(reply),
                ref r if !r.is_content() && self.held.is_empty() => out.push(reply),
                ref r if !r.is_content() || !shows(r) => self.held.push(reply),
                _ => {
                    out.append(&mut self.held);
                    out.push(reply);
                    self.through = true;
                }
            }
        }
        if failed && !self.through {
            let mut usage = Usage::default();
            for held in self.held.drain(..) {
                if let Output::Usage(u) = held {
                    usage += &u;
                }
            }
            if !usage.is_empty() {
                out.push(Output::Usage(usage));
            }
        }
    }
}

/// The events of a stream event that is not its format's JSON, as an
/// [`Output::Native`] of `format` so filters read it; one with no data, such
/// as a comment, says nothing.
pub(crate) fn unreadable_event(format: &Arc<str>, raw: &Bytes, out: &mut Vec<Output>) {
    if let Some(data) = event_data(raw) {
        out.push(Output::native(format.clone(), data));
    }
}

/// The fault of a provider's error event `v`, in a response of format
/// `name`, whose data goes as it came to a client of this format.
pub(crate) fn error_event(name: &str, raw: &[u8], v: &Value) -> Fault {
    let data = Bytes::from(event_data(raw).unwrap_or_default());
    said_error(name, StatusCode::OK, data, v)
}
