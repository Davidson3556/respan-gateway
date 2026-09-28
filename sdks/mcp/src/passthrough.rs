//! A relay-only format of no kind, for paths a typed format does not
//! cover, such as `/v1/messages/count_tokens` and `/v1/responses` behind a
//! local proxy for Claude Code or Codex. Every byte passes as it came.
//!
//! It has no kind, so no model: a request goes to the first default target
//! that speaks it. A plugin that routes such traffic by model reads the
//! client's body (`cx.body()`) in the protocol's own spelling; none can
//! change the model, as nothing of no kind is written anew.

use respan_gateway_core::http;
use respan_gateway_core::{Decoder, Doc, Encoder, Fault, Format};
use respan_gateway_formats as formats;

/// Relays any method to the paths ending in one of `suffixes`, unchanged.
#[derive(Debug, Clone)]
pub struct Passthrough {
    name: &'static str,
    suffixes: Vec<&'static str>,
}

impl Passthrough {
    /// A passthrough named `name` for paths ending in any of `suffixes`;
    /// `""` matches every path.
    pub fn new(name: &'static str, suffixes: impl IntoIterator<Item = &'static str>) -> Self {
        Self {
            name,
            suffixes: suffixes.into_iter().collect(),
        }
    }
}

impl Format for Passthrough {
    type Input = ();
    type Output = ();

    fn name(&self) -> &str {
        self.name
    }

    fn accepts(&self, head: &http::request::Parts) -> bool {
        let path = head.uri.path();
        self.suffixes.iter().any(|s| path.ends_with(s))
    }

    // Required, never called for a format of no kind (FINDINGS F10).
    fn decode_input(&self, _head: &http::request::Parts, _body: &Doc) -> Result<(), Fault> {
        Ok(())
    }

    // Required, never called for a format of no kind (FINDINGS F10).
    fn encode_input(&self, _input: &(), _original: Option<(&Doc, &())>) -> Result<Doc, Fault> {
        Err(Fault::config("a passthrough has no input to encode"))
    }

    fn prepare(
        &self,
        body: Doc,
        _: Option<&()>,
        client: &http::request::Parts,
        _relayed: bool,
    ) -> Result<http::Request<Doc>, Fault> {
        let uri = client.uri.path_and_query().map_or("/", |p| p.as_str());
        let mut req = http::Request::builder()
            .method(client.method.clone())
            .uri(uri)
            .body(body)?;
        // A call of a format of no kind always relays.
        formats::pass_headers(&mut req, Some(client));
        Ok(req)
    }

    fn decoder(
        &self,
        head: &http::response::Parts,
        _call: &http::Request<Doc>,
    ) -> Box<dyn Decoder> {
        formats::bytes_decoder(head)
    }

    fn encoder(
        &self,
        head: &mut http::response::Parts,
        _client: &http::Request<Doc>,
        relayed: bool,
    ) -> Box<dyn Encoder> {
        formats::bytes_encoder(head, relayed)
    }
}
