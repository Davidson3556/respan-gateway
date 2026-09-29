//! A REST API as a format, for a plugin that calls it or rewrites a call's
//! path. Its input, [`Ask`], carries the method, path and body of a request,
//! so the encoder writes what a plugin changed in the path, and a plugin can
//! make a call of its own to any path under the API's prefix:
//! `cx.call("files").with_input(Ask::get("/v1/files/f1/content"))`. Its output
//! is the answer's bytes, a [`Got`]. Used by `attachments.rs` (a files
//! endpoint) and `jobs_background.rs` (Responses).

use respan_gateway_core::Fault;
use respan_gateway_formats::opaque::{self, Got};
use respan_gateway_formats::{body_changed, call_head, headed, message_decoder, set_path};
use respan_gateway_http::bytes::{Bytes, BytesMut};
use respan_gateway_http::http::Method;
use respan_gateway_http::{Codec, Decoder, Encoder, RequestHead, ResponseHead, impl_format};

/// The API named `name` at paths starting with `prefix`.
#[derive(Clone, Debug)]
pub struct Rest {
    name: &'static str,
    prefix: &'static str,
}

impl Rest {
    /// The API `name`, served at `prefix`.
    pub const fn new(name: &'static str, prefix: &'static str) -> Self {
        Self { name, prefix }
    }
}

/// A request: its method, path and body.
#[derive(Clone, PartialEq, Debug)]
pub struct Ask {
    /// The HTTP method.
    pub method: Method,
    /// The path, and the query if there is one.
    pub path: String,
    /// The body.
    pub body: Bytes,
}

impl Ask {
    /// A `GET` of `path`.
    pub fn get(path: impl Into<String>) -> Self {
        Self {
            method: Method::GET,
            path: path.into(),
            body: Bytes::new(),
        }
    }
}

/// Writes an `Ask`'s body; the codec writes its head.
struct WriteBody;

impl Encoder<Ask, ()> for WriteBody {
    fn head(&mut self, _: Option<&Ask>) {}

    fn encode(&mut self, ask: &Ask, out: &mut BytesMut) -> Result<(), Fault> {
        out.extend_from_slice(&ask.body);
        Ok(())
    }

    fn end(&mut self, end: &Result<(), Fault>, _: &mut BytesMut) -> Result<(), Fault> {
        end.clone()
    }
}

impl Codec for Rest {
    type Input = Ask;
    type Output = Got<Rest>;

    fn name(&self) -> &str {
        self.name
    }

    fn accepts(&self, head: &RequestHead) -> bool {
        head.uri.path().starts_with(self.prefix)
    }

    fn request_decoder(&self, head: &RequestHead) -> Box<dyn Decoder<Ask>> {
        let (method, path) = (head.method.clone(), head.uri.path().to_owned());
        message_decoder(move |body| {
            let (method, path) = (method.clone(), path.clone());
            Ok(Ask { method, path, body })
        })
    }

    fn request_encoder(
        &self,
        client: Option<&RequestHead>,
        _body: &mut Bytes,
        _streams: bool,
    ) -> Box<dyn Encoder<Ask, RequestHead>> {
        let mut head = call_head(client);
        let write_head = move |ask: Option<&Ask>| {
            if let Some(ask) = ask {
                head.method = ask.method.clone();
                set_path(&mut head, &ask.path);
                body_changed(&mut head);
            }
            head
        };
        headed(write_head, Box::new(WriteBody))
    }

    fn response_decoder(
        &self,
        head: &ResponseHead,
        call: &RequestHead,
        _: &Bytes,
    ) -> Box<dyn Decoder<Got<Rest>>> {
        opaque::response_decoder(self.name, head, call)
    }

    fn response_encoder(
        &self,
        answer: Option<&ResponseHead>,
        ask: &RequestHead,
        _: &Bytes,
    ) -> Box<dyn Encoder<Got<Rest>, ResponseHead>> {
        opaque::response_encoder(answer, ask)
    }
}

impl_format!(Rest);
