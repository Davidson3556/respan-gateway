//! What the HTTP formats of this crate share, OpenAI's and Veo's.

use respan_gateway_conversation_http::read_provider_error;
use respan_gateway_core::{BadRequest, Connection, EventStream, Fault, NativeFields};
use respan_gateway_http::bytes::Bytes;
use respan_gateway_http::framing;
use respan_gateway_http::http::{self, request::Parts as RequestHead};
use respan_gateway_http::{Body, Encoder, ProviderIo};
use respan_gateway_media::video;
use serde_json::{Map, Value};

/// The provider edge of `format`: a call whose head `encode` sets (a `POST`
/// to start with) and whose body it returns, its success read by `decode`,
/// given the call as sent, its failure leniently. A format takes one value
/// per call: live input is refused.
pub(crate) fn bind_provider<I, O, E, D>(
    io: ProviderIo,
    format: &'static str,
    encode: E,
    decode: D,
) -> Connection<O, I>
where
    I: Clone + Send + Sync + 'static,
    O: Clone + Send + Sync + 'static,
    E: FnOnce(&mut RequestHead, &I) -> Result<Box<dyn Encoder<I>>, Fault> + Send + 'static,
    D: FnOnce(http::Response<Body>, http::Request<Bytes>) -> EventStream<O> + Send + 'static,
{
    io.bind(
        move |head, input: Option<&I>| {
            let input = input.ok_or_else(|| {
                Fault::config(format!("{format} does not accept streaming inputs"))
            })?;
            head.method = http::Method::POST;
            encode(head, input)
        },
        decode,
        move |answer| read_provider_error(format, answer),
    )
}

/// The encoder of a call whose body is `body`.
pub(crate) fn sends<I: 'static>(body: Bytes) -> Box<dyn Encoder<I>> {
    framing::each(move |_: &I| Ok(body.clone()))
}

/// The content of a video, chunk by chunk as it comes.
pub(crate) fn content(body: Body) -> EventStream<video::Output> {
    framing::parse(
        body,
        (),
        |(), bytes, out| {
            out.push(video::Output::Content(bytes));
            Ok(())
        },
        |(), wire, _| wire,
    )
}

/// The fields of `native` to write in `format`: all of them if they are
/// its, none if there are none; another format's are refused, as only it
/// knows what they mean.
pub(crate) fn kept(
    format: &str,
    native: &Option<NativeFields>,
) -> Result<Map<String, Value>, Fault> {
    match native {
        Some(n) if &*n.format == format => Ok(n.fields.clone()),
        Some(n) if !n.is_empty() => Err(BadRequest::fault(format!(
            "fields of format {} cannot be written in {format}",
            n.format
        ))),
        _ => Ok(Map::new()),
    }
}
