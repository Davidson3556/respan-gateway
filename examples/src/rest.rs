//! A REST API as a format, for a plugin that calls it or rewrites a call's
//! path. Its input, [`Ask`], carries the method, path and body of a request,
//! so the encoder writes what a plugin changed in the path, and a plugin can
//! make a call of its own to any path under the API's prefix:
//! `cx.call("files").with_input(Ask::get("/v1/files/f1/content"))`. Its output
//! is the answer's bytes, a [`Got`]. Used by `attachments.rs` (a files
//! endpoint) and `jobs_background.rs` (Responses). Calls take one finite
//! [`Ask`] (or relay an already complete raw body); live input streams have
//! no method or path to put in the request head and are rejected before dialing.

use respan_gateway_http::bytes::{Bytes, BytesMut};
use respan_gateway_http::formats::opaque::Got;
use respan_gateway_http::formats::opaque::{self};
use respan_gateway_http::formats::set_path;
use respan_gateway_http::http::Method;
use respan_gateway_http::{self as h, Body, Encoder, Incoming, Outgoing};
use respan_gateway_sdk::{Connection, EventSink, EventStream, Fault, Format};

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
    /// The path; the query stays as it came (`Wire` has it).
    pub path: String,
    /// Query parameters retained independently of path edits.
    pub query: Option<String>,
    /// The body.
    pub body: Bytes,
}

impl Ask {
    /// A `GET` of `path`.
    pub fn get(path: impl Into<String>) -> Self {
        Self {
            method: Method::GET,
            path: path.into(),
            query: None,
            body: Bytes::new(),
        }
    }
}

/// Writes an `Ask`'s body; the format writes its head.
struct WriteBody;

impl Encoder<Ask> for WriteBody {
    fn encode(&mut self, ask: &Ask, out: &mut BytesMut) -> Result<(), Fault> {
        out.extend_from_slice(&ask.body);
        Ok(())
    }

    fn end(&mut self, end: &Result<(), Fault>, _: &mut BytesMut) -> Result<(), Fault> {
        end.clone()
    }
}

impl Rest {
    fn request(
        &self,
        source: &mut EventStream<Ask>,
    ) -> Result<(h::http::Request<Body>, EventSink<Ask>), Fault> {
        if !source.is_single() {
            return Err(Fault::config(
                "a REST call carries one finite request, not streamed inputs",
            ));
        }
        let ask = source.value().transpose()?.cloned();
        let (request, sink) = h::request(source, Box::new(WriteBody))?;
        let (mut head, body) = request.into_parts();
        if let Some(ask) = ask {
            head.method = ask.method;
            let path = match ask.query {
                Some(query) => format!("{}?{query}", ask.path),
                None => ask.path,
            };
            set_path(&mut head, &path)?;
        }
        Ok((h::http::Request::from_parts(head, body), sink))
    }
}

impl Format<Incoming, Ask, Got<Rest>> for Rest {
    fn name(&self) -> &str {
        self.name
    }

    fn matches(&self, io: &Incoming) -> bool {
        io.request().uri().path().starts_with(self.prefix)
    }

    fn bind(&self, io: Incoming) -> Result<Connection<Ask, Got<Rest>>, Fault> {
        Ok(io.bind(
            |request| {
                Ok(h::messages(request, |head, body| {
                    Ok(Ask {
                        method: head.method.clone(),
                        path: head.uri.path().to_owned(),
                        query: head.uri.query().map(str::to_owned),
                        body,
                    })
                }))
            },
            |reply, _| Ok(opaque::write_response(reply)),
            h::plain_refusal,
        ))
    }
}

impl Format<Outgoing, Got<Rest>, Ask> for Rest {
    fn name(&self) -> &str {
        self.name
    }

    fn bind(&self, io: Outgoing) -> Result<Connection<Got<Rest>, Ask>, Fault> {
        let (format, name) = (self.clone(), self.name);
        Ok(io.bind(
            move |source| format.request(source),
            move |response, request| opaque::read_response(name, response, &request.into_parts().0),
            |head, _| head.status.is_success(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_rest_call_requires_one_finite_ask_and_preserves_its_method_and_path() {
        let codec = Rest::new("files", "/v1/files");
        let live = codec.request(&mut EventStream::stream(
            respan_gateway_sdk::futures::stream::pending(),
        ));
        assert!(live.is_err(), "live input has no Ask head to encode");

        let ask = Ask {
            method: Method::DELETE,
            path: "/v1/files/file-1?permanent=true".into(),
            body: Bytes::from_static(b"payload"),
            query: None,
        };
        let (request, _) = codec
            .request(&mut EventStream::single(ask))
            .expect("finite Ask is supported");
        assert_eq!(request.method(), Method::DELETE);
        assert_eq!(request.uri().path(), "/v1/files/file-1");
        assert_eq!(request.uri().query(), Some("permanent=true"));
    }
}
