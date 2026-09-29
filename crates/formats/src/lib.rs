#![warn(missing_docs)]
//! Wire formats and helpers for writing one. OpenAI Chat Completions and
//! Anthropic Messages translate through the [`conversation`] kind. Every
//! value a format decodes keeps its bytes ([`relayed`]); traffic of no kind
//! relays through [`opaque`].

mod common;
pub mod opaque;
pub mod relayed;
pub mod sse;
pub mod whole;

mod anthropic;
pub mod conversation;
mod openai;
mod tools;
mod usage;

pub use anthropic::AnthropicMessages;
pub use openai::OpenAiChat;
pub use usage::Usage;

use respan_gateway_core::Fault;
use respan_gateway_http::bytes::{Bytes, BytesMut};
use respan_gateway_http::http::{
    self, HeaderValue, header::CONTENT_ENCODING, header::CONTENT_TYPE,
};
use respan_gateway_http::{Decoder, Encoder};
use respan_gateway_http::{RequestHead, ResponseHead};
use serde_json::{self, Value};

/// A fault's extra fields for the client's error body, a JSON object, as a
/// hint ([`Fault::with`]): the formats write them into their error
/// bodies, and the kernel never reads them.
#[derive(Clone, Debug, PartialEq)]
pub struct Details(pub Value);

/// A provider's message as JSON: a whole body, or an event's `data:`.
pub fn json(raw: impl AsRef<[u8]>) -> Option<Value> {
    let raw = raw.as_ref();
    serde_json::from_slice(raw)
        .ok()
        .or_else(|| sse::event_json(raw))
}

/// Whether a request is a `POST` to a path ending in `suffix`: the usual
/// [`Codec::accepts`](respan_gateway_http::Codec::accepts).
pub fn posts_to(head: &RequestHead, suffix: &str) -> bool {
    head.method == http::Method::POST && path_ends_with(head, suffix)
}

/// A path as every front end reads it for matching: percent escapes
/// decoded (a `%2F` too, as a front end might read it a slash, so matching
/// is the liberal reading), `;` parameters dropped, dot-segments removed
/// (RFC 3986 5.2.4) and empty segments collapsed, and a backslash read as a
/// slash (as the outbound client reads it), so `/v1/chat/./completions`,
/// `/v1//chat/completions` and `/x/../v1/chat/completions/` all spell
/// `/v1/chat/completions`.
pub fn clean_path(path: &str) -> String {
    // Match what is forwarded (see `forwarded_path`), so the provider never
    // gets a structure matching did not see.
    let path = &forwarded_path(path);
    let mut kept: Vec<String> = Vec::new();
    // The outbound client reads a backslash as a slash, so a match must too.
    for segment in respan_gateway_http::percent_decode(path).split(['/', '\\']) {
        let segment = segment.split(';').next().unwrap_or_default();
        match segment {
            "" | "." => {}
            ".." => drop(kept.pop()),
            _ => kept.push(segment.to_owned()),
        }
    }
    // Case-insensitive routers (ASP.NET Core, Express) read `/V1/Chat` as
    // `/v1/chat`: matching is the liberal reading.
    kept.iter()
        .fold(String::new(), |out, s| out + "/" + s)
        .to_ascii_lowercase()
}

/// A request's path as a relay sends it on: the segments of the path
/// [`clean_path`] matched, each byte for byte as the client spelled it (a
/// vendor's `%2F`, `:` or `;v=1` mean something), with only what matching
/// drops gone: empty and dot-segments (however escaped, or given `;`
/// parameters), a `..` taking the segment before it, and a backslash a
/// slash. An escaped `%2F` stays inside its segment here, where matching
/// may read it a slash: matching reads at least what the provider does, so
/// the provider never gets a structure the router did not look at.
pub(crate) fn forwarded_path(path: &str) -> String {
    let mut kept: Vec<&str> = Vec::new();
    for raw in path.split(['/', '\\']) {
        let name = raw.split(';').next().unwrap_or_default();
        match respan_gateway_http::percent_decode(name).as_str() {
            "" | "." => {}
            ".." => drop(kept.pop()),
            _ => kept.push(raw),
        }
    }
    match kept.join("/") {
        joined if joined.is_empty() => "/".to_owned(),
        joined => format!("/{joined}"),
    }
}

/// The bytes a passthrough path may hold once percent-decoded once, besides
/// ASCII letters and digits.
///
/// This is an allowlist because readers disagree about special characters,
/// and each loop found another: `;` (servlets), `\` (the URL standard), `%`
/// (double decoders), case, NUL, a decoded `?` or `#` (a front end that
/// decodes and reparses as a URL), Unicode case folding. A denylist is
/// beaten by the next one; here a character some reader treats specially is
/// refused by default. No vendor path needs more: ids, model names and
/// versions are ASCII, a Bedrock ARN needs `:` and an escaped `/`, a Vertex
/// model `@` and `:`, and a base64 id `+`, `/` and `=`. A space is refused
/// (some readers trim it), as is any non-ASCII byte.
const PATH_BYTES: &str = "/-_.~:@+=,!";

/// Whether every reader reads a path one way: percent-decoded once, every
/// character is an ASCII letter or digit or in [`PATH_BYTES`], and no
/// segment is `.`, `..` or ends in `.` (Windows trims trailing dots). With
/// no `;`, `\`, `%`, `?`, `#`, control or non-ASCII byte left to read, and no
/// dot-segment to resolve before or after decoding, every reader reads the
/// same names, as an escaped `/` splits a name without moving it: what
/// [`clean_path`] reads.
pub(crate) fn reads_one_way(path: &str) -> bool {
    let decoded = respan_gateway_http::percent_decode(path);
    decoded
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || PATH_BYTES.contains(c))
        && decoded.split('/').all(|s| !s.ends_with('.'))
}

/// Whether a request's path ends in `suffix`, however it is spelled (see
/// [`clean_path`]).
pub fn path_ends_with(head: &RequestHead, suffix: &str) -> bool {
    clean_path(head.uri.path()).ends_with(&clean_path(suffix))
}

/// Set a call's path, relative to the transport, keeping its query.
pub fn set_path(head: &mut RequestHead, path: &str) {
    let uri = match head.uri.query() {
        Some(query) => format!("{path}?{query}"),
        None => path.to_owned(),
    };
    // A path and a query that parsed parse.
    head.uri = uri.parse().unwrap_or_default();
}

/// Mark a call whose body is not the client's bytes as sent, as when a
/// value was written or its inputs stream: its `Content-Encoding`, the
/// client's, goes.
pub fn body_changed(head: &mut RequestHead) {
    head.headers.remove(CONTENT_ENCODING);
}

/// A call's head: the client's for a relay, else a fresh one.
pub fn call_head(client: Option<&RequestHead>) -> RequestHead {
    client
        .cloned()
        .unwrap_or_else(respan_gateway_http::fresh_request_head)
}

/// A response's head: the provider's for a response in the client's
/// format, else a fresh `200`.
pub fn response_head(answer: Option<&ResponseHead>) -> ResponseHead {
    answer
        .cloned()
        .unwrap_or_else(respan_gateway_http::fresh_response_head)
}

/// Set a response's content type to `value` if it has none (RFC 9110 8.3).
pub fn default_content_type(head: &mut ResponseHead, value: &'static str) {
    let value = HeaderValue::from_static(value);
    head.headers.entry(CONTENT_TYPE).or_insert(value);
}

/// A decoder of one value per body or data frame, made or refused by
/// `on_message`: the usual
/// [`Codec::request_decoder`](respan_gateway_http::Codec::request_decoder).
pub fn message_decoder<T, F>(on_message: F) -> Box<dyn Decoder<T>>
where
    T: 'static,
    F: FnMut(Bytes) -> Result<T, Fault> + Send + 'static,
{
    Box::new(Messages(on_message))
}

struct Messages<F>(F);

/// An encoder whose head is `head`, and whose body `body` writes: the usual
/// way to give a body's encoder ([`Encoder<T, ()>`]) the head a factory
/// made.
pub fn with_head<T, H>(head: H, body: Box<dyn Encoder<T, ()>>) -> Box<dyn Encoder<T, H>>
where
    T: 'static,
    H: Send + 'static,
{
    headed(move |_: Option<&T>| head, body)
}

/// An encoder whose head `head` makes from the first value it is asked
/// with ([`Encoder::head`]), and whose body `body` writes.
pub fn headed<T, H, F>(head: F, body: Box<dyn Encoder<T, ()>>) -> Box<dyn Encoder<T, H>>
where
    T: 'static,
    H: Send + 'static,
    F: FnOnce(Option<&T>) -> H + Send + 'static,
{
    Box::new(Headed {
        head: Some(head),
        body,
    })
}

struct Headed<F, T> {
    head: Option<F>,
    body: Box<dyn Encoder<T, ()>>,
}

impl<T, H, F> Encoder<T, H> for Headed<F, T>
where
    T: 'static,
    H: Send + 'static,
    F: FnOnce(Option<&T>) -> H + Send + 'static,
{
    fn head(&mut self, first: Option<&T>) -> H {
        let head = self.head.take().expect("a head is asked once");
        head(first)
    }

    fn open(&mut self, out: &mut BytesMut) -> Result<bool, Fault> {
        self.body.open(out)
    }

    fn encode(&mut self, value: &T, out: &mut BytesMut) -> Result<(), Fault> {
        self.body.encode(value, out)
    }

    fn end(&mut self, end: &Result<(), Fault>, out: &mut BytesMut) -> Result<(), Fault> {
        self.body.end(end, out)
    }
}

impl<T, F: FnMut(Bytes) -> Result<T, Fault> + Send + 'static> Decoder<T> for Messages<F> {
    fn decode(&mut self, bytes: Bytes, out: &mut Vec<T>) -> Result<(), Fault> {
        out.push((self.0)(bytes)?);
        Ok(())
    }

    fn end(&mut self, _out: &mut Vec<T>) -> Result<(), Fault> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn head(method: http::Method, uri: &str) -> RequestHead {
        let req = http::Request::builder().method(method).uri(uri).body(());
        req.unwrap_or_default().into_parts().0
    }

    #[test]
    fn a_path_is_forwarded_as_spelled_minus_what_matching_drops() {
        for (sent, forwarded) in [
            ("/a/dir%2Ffile:x@y;v=1/%FF", "/a/dir%2Ffile:x@y;v=1/%FF"),
            ("/a//b/./c\\d/", "/a/b/c/d"),
            ("/a/b/%2e%2E/c", "/a/c"),
            ("/a/b/.;x/../c", "/a/c"),
            ("/../..", "/"),
        ] {
            assert_eq!(forwarded_path(sent), forwarded, "{sent}");
        }
    }

    #[test]
    fn a_path_is_matched_however_it_is_spelled() {
        for path in [
            "/v1/chat/completions",
            "/v1/chat/completions/",
            "/v1/chat/completions//?x=1",
            "/v1/chat/completions%2F",
            "/v1/chat/%63ompletions",
            "/v1%2Fchat/completions",
        ] {
            let head = head(http::Method::POST, path);
            assert!(posts_to(&head, "/chat/completions"), "{path}");
        }
        // What a front end would resolve to the chat path is chat, or a
        // passthrough served after it would relay the call past chat plugins.
        for path in [
            "/v1/chat//completions",
            "/v1/chat/./completions",
            "/v1/chat/x/../completions",
            "/x/../v1/chat/completions",
            "/v1/chat/completions/.",
            "/v1/chat/%2e/completions",
            "/v1/chat/completions;jsessionid=1",
            "/v1/chat/x/..;/completions",
            "/../../v1/chat/completions",
        ] {
            let head = head(http::Method::POST, path);
            assert!(posts_to(&head, "/chat/completions"), "{path}");
        }
        let other = head(http::Method::POST, "/v1/chat/completionsx");
        assert!(!posts_to(&other, "/chat/completions"));
        let other = head(http::Method::POST, "/v1/chat/x/completions");
        assert!(!posts_to(&other, "/chat/completions"));
        let other = head(http::Method::POST, "/v1/chat/completions/../x");
        assert!(!posts_to(&other, "/chat/completions"));
        let other = head(http::Method::POST, "/v1/chat/completionsx");
        assert!(!posts_to(&other, "/chat/completions"));
        let get = head(http::Method::GET, "/v1/chat/completions");
        assert!(!posts_to(&get, "/chat/completions"));
    }
}

/// The passthrough forwards a path it never read, so every reader of what
/// it forwards must read what matching read: else a vendor, or a front end
/// before it, could read a chat endpoint where the chat format read none,
/// and a chat call would pass the chat format and its plugins.
#[cfg(test)]
mod path_readers {
    use super::{clean_path, forwarded_path, reads_one_way};
    use respan_gateway_http::percent_decode;

    fn dots(segments: impl Iterator<Item = String>) -> String {
        let mut kept: Vec<String> = Vec::new();
        for s in segments {
            match s.as_str() {
                "" | "." => {}
                ".." => drop(kept.pop()),
                _ => kept.push(s),
            }
        }
        kept.iter().fold(String::new(), |out, s| out + "/" + s)
    }

    /// The URL standard: split raw on `/` and `\`, a dot-segment however
    /// escaped, the rest left escaped.
    fn url_standard(p: &str) -> String {
        dots(
            p.split(['/', '\\'])
                .map(|s| match percent_decode(s).as_str() {
                    d @ ("." | "..") => d.to_owned(),
                    _ => s.to_owned(),
                }),
        )
    }

    /// Servlet containers (Tomcat, Jetty, Spring): `;params` cut from each
    /// raw segment, then decoded; an escaped `/` stays in its segment.
    fn servlet(p: &str) -> String {
        dots(
            p.split('/')
                .map(|s| percent_decode(s.split(';').next().unwrap_or_default())),
        )
    }

    /// A front end that decodes first, then resolves dots (nginx's `$uri`).
    fn decode_first(p: &str) -> String {
        dots(percent_decode(p).split('/').map(str::to_owned))
    }

    /// A C reader: the decoded path ends at its first NUL.
    fn nul_cut(p: &str) -> String {
        let decoded = percent_decode(p);
        dots(
            decoded
                .split('\0')
                .next()
                .unwrap_or_default()
                .split('/')
                .map(str::to_owned),
        )
    }

    /// Windows/IIS: trailing dots and spaces cut from each segment.
    fn trailing(p: &str) -> String {
        dots(
            percent_decode(p)
                .split('/')
                .map(|s| s.trim_end_matches(['.', ' ']).to_owned()),
        )
    }

    /// Case-insensitive routing (ASP.NET Core, Express by default).
    fn case_insensitive(p: &str) -> String {
        decode_first(p).to_ascii_lowercase()
    }

    /// A front end that decodes the path and hands the result on as a URL
    /// (Apache mod_rewrite `[P]`, CVE-2023-25690): a decoded `?` or `#`
    /// ends the path.
    fn query_cut(p: &str) -> String {
        let decoded = percent_decode(p);
        let path = decoded.split(['?', '#']).next().unwrap_or_default();
        dots(path.split('/').map(str::to_owned))
    }

    /// A router folding case per character with Unicode rules (Java
    /// `equalsIgnoreCase`, Go `EqualFold`): `\u{17f}` reads `s`, `\u{131}` `i`.
    fn unicode_case(p: &str) -> String {
        decode_first(p).to_uppercase().to_lowercase()
    }

    /// Every path of up to three more segments after a chat prefix, from
    /// the spellings readers differ on: for each one passthrough accepts,
    /// each reader of the forwarded path reads what matching read, so none
    /// reads a path a client format accepts that matching did not.
    #[test]
    fn every_reader_of_a_forwarded_path_reads_what_matching_read() {
        let parts = [
            "completions",
            "x",
            "..",
            ".",
            "",
            "%2E%2E",
            "%2e",
            ".%2e",
            "x%2Fy",
            "x%5C..",
            "..;p",
            "..%3Bp",
            "completions;x%2F..",
            "completions;%2F..",
            "completions;p",
            "c;p%2F..",
            "%2F..",
            "..%2F",
            "x;%2F",
            "%3B",
            "%252F..",
            "%25",
            "a%2F%2E%2E",
            "chat%2Fcompletions",
            "arn%3Aaws%2Fp",
            "m:generate",
            "%FF",
            "Completions",
            "COMPLETIONS",
            "Chat",
            "completions%00",
            "%00",
            "completions.",
            "completions%20",
            "completions%2e",
            "completions%3Fx",
            "%3F",
            "completions%23x",
            "completion%C5%BF",
            "complet%C4%B1ons",
            "%E2%84%AAx",
        ];
        let (mut bad, mut accepted, mut n) = (Vec::new(), 0, 0);
        for prefix in ["/v1/chat", "/v1/chat/completions"] {
            let mut paths = vec![prefix.to_owned()];
            let mut frontier = paths.clone();
            for _ in 0..3 {
                let next: Vec<String> = frontier
                    .iter()
                    .flat_map(|p| parts.iter().map(move |s| format!("{p}/{s}")))
                    .collect();
                paths.extend(next.iter().cloned());
                frontier = next;
            }
            for p in &paths {
                n += 1;
                if !reads_one_way(p) {
                    continue;
                }
                accepted += 1;
                let (matched, sent) = (clean_path(p), forwarded_path(p));
                for (who, read) in [
                    ("URL-standard", url_standard(&sent)),
                    ("servlet", servlet(&sent)),
                    ("decode-first", decode_first(&sent)),
                    ("NUL-cut", nul_cut(&sent)),
                    ("trailing-dot", trailing(&sent)),
                    ("case-insensitive", case_insensitive(&sent)),
                    ("query-cut", query_cut(&sent)),
                    ("unicode-case", unicode_case(&sent)),
                ] {
                    if clean_path(&read) != matched {
                        bad.push(format!(
                            "{p}: matched {matched:?}; sent {sent}; a {who} reader reads {read:?}"
                        ));
                        break;
                    }
                }
            }
        }
        assert!(accepted > 1_000, "only {accepted} of {n} paths accepted");
        assert!(
            bad.is_empty(),
            "{} of {n}:\n{}",
            bad.len(),
            bad[..bad.len().min(12)].join("\n")
        );
    }

    #[test]
    fn a_path_readers_differ_on_is_refused_and_a_vendors_path_is_not() {
        for path in [
            "/v1/chat/completions/%3B/%2F..",
            "/v1/chat/completions/x%5C../%2F..",
            "/v1/chat/completions;x%2F..",
            "/v1/chat/completions;x%2Fy",
            "/v1/chat%252Fcompletions",
            "/v1/./models",
            "/v1/x/%2e%2E/models",
            "/v1\\models",
            "/v1/chat/completions%3Fx=1",
            "/v1/chat/completions%23frag",
            "/v1/files/%FF",
            "/v1/completion%C5%BF",
            "/v1/my%20file",
            "/v1/a%00b",
            "/v1/a%22b",
            "/v1/a*b",
            "/v1/models.",
        ] {
            assert!(!reads_one_way(path), "{path}");
        }
        for path in [
            "/model/arn%3Aaws%3Abedrock%3Aus-east-1%3A1%3Aapplication-inference-profile%2Fabc/invoke",
            "/v1beta/models/gemini-2.0-flash:generateContent",
            "/v1/models/claude@2024:rawPredict",
            "/v1//files/file-AbC123/",
            "/v1/responses/resp_Ab%2B%2Fg==",
            "/v1/models/ft:gpt-4o:org:custom:9x8y7z",
            "/api/event_logging/batch",
            "/a/b~c_d,e!f",
            "/",
        ] {
            assert!(reads_one_way(path), "{path}");
        }
    }
}
