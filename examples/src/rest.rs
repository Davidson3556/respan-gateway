//! A REST API as a format, for a plugin that calls it or rewrites a call's
//! path. Its input, [`Ask`], carries the method, path and body of a request,
//! so the encoder writes what a plugin changed in the path, and a plugin can
//! make a call of its own to any path under the API's prefix:
//! `cx.call("files").with_input(Ask::get("/v1/files/f1/content"))`. Its output
//! is the answer's bytes, an [`passthrough::Output`]. Used by `attachments.rs` (a files
//! endpoint) and `jobs_background.rs` (Responses). Calls take one finite
//! [`Ask`] (or relay an already complete raw body); live input streams have
//! no method or path to put in the request head and are rejected before dialing.

use respan_gateway_core::{Connection, Fault, Format};
use respan_gateway_http::bytes::Bytes;
use respan_gateway_http::framing::{each, passthrough, set_path};
use respan_gateway_http::http::{Method, request::Parts as RequestHead};
use respan_gateway_http::{self as h, ClientIo, Encoder, ProviderIo};

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

/// Write the head of a call of `ask`, its method, path and query, and
/// return the encoder of its body.
fn request(head: &mut RequestHead, ask: Option<&Ask>) -> Result<Box<dyn Encoder<Ask>>, Fault> {
    let streamed = "a REST call carries one finite request, not streamed inputs";
    let ask = ask.ok_or_else(|| Fault::config(streamed))?;
    head.method = ask.method.clone();
    match &ask.query {
        Some(query) => set_path(head, &format!("{}?{query}", ask.path))?,
        None => set_path(head, &ask.path)?,
    }
    Ok(each(|ask: &Ask| Ok(ask.body.clone())))
}

impl Format<ClientIo, Ask, passthrough::Output<Rest>> for Rest {
    fn name(&self) -> &str {
        self.name
    }

    fn matches(&self, io: &ClientIo) -> bool {
        io.request().uri().path().starts_with(self.prefix)
    }

    fn bind(&self, io: ClientIo) -> Result<Connection<Ask, passthrough::Output<Rest>>, Fault> {
        Ok(io.bind(
            |request| {
                h::messages(request, |head, body| {
                    Ok(Ask {
                        method: head.method.clone(),
                        path: head.uri.path().to_owned(),
                        query: head.uri.query().map(str::to_owned),
                        body,
                    })
                })
            },
            passthrough::write_response,
            h::plain,
        ))
    }
}

impl Format<ProviderIo, passthrough::Output<Rest>, Ask> for Rest {
    fn name(&self) -> &str {
        self.name
    }

    fn bind(&self, io: ProviderIo) -> Result<Connection<passthrough::Output<Rest>, Ask>, Fault> {
        let name = self.name;
        Ok(io.bind(request, passthrough::read_response, move |answer| {
            h::provider_fault(name, answer)
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_rest_call_requires_one_finite_ask_and_preserves_its_method_and_path() {
        let mut head = h::http::Request::new(()).into_parts().0;
        assert!(
            request(&mut head, None).is_err(),
            "live input has no Ask head to encode"
        );

        let ask = Ask {
            method: Method::DELETE,
            path: "/v1/files/file-1?permanent=true".into(),
            body: Bytes::from_static(b"payload"),
            query: None,
        };
        assert!(
            request(&mut head, Some(&ask)).is_ok(),
            "one Ask is supported"
        );
        assert_eq!(head.method, Method::DELETE);
        assert_eq!(head.uri.path(), "/v1/files/file-1");
        assert_eq!(head.uri.query(), Some("permanent=true"));
    }
}
