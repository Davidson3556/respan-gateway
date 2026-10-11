#![warn(missing_docs)]
//! Framing that formats build on: server-sent events ([`sse`]), whole
//! bodies ([`whole`]), an encoder per event, and path helpers. The formats
//! themselves are the `respan-gateway-*-http` crates'; traffic of no kind
//! relays through [`passthrough`](crate::passthrough).

pub mod fields;
pub mod sse;
pub mod whole;

use respan_gateway_core::{EventStream, Fault};
use respan_gateway_http::bytes::Bytes;
use respan_gateway_http::http::{
    self, HeaderValue, header::CONTENT_TYPE, request::Parts as RequestHead,
    response::Parts as ResponseHead,
};
use serde_json::{self, Value};

/// A provider's message as JSON: a whole body, or an event's `data:`.
pub fn json(raw: impl AsRef<[u8]>) -> Option<Value> {
    let raw = raw.as_ref();
    serde_json::from_slice(raw)
        .ok()
        .or_else(|| sse::event_json(raw))
}

/// Whether a request is a `POST` to a path ending in `suffix`: the usual
/// incoming format recognition.
pub fn posts_to(head: &RequestHead, suffix: &str) -> bool {
    head.method == http::Method::POST && path_ends_with(&head.uri, suffix)
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
pub fn forwarded_path(path: &str) -> String {
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

/// Whether a request's path, in `uri`, ends in `suffix`, however it is
/// spelled (see [`clean_path`]).
pub fn path_ends_with(uri: &http::Uri, suffix: &str) -> bool {
    let (path, suffix) = (clean_path(uri.path()), clean_path(suffix));
    // Suffix-pattern routers (Spring MVC's `useSuffixPatternMatch`, Rails'
    // `(.:format)`) read `/chat/completions.json` as `/chat/completions`.
    let (dir, last) = path.rsplit_once('/').unwrap_or_default();
    let bare = match last.split_once('.') {
        Some((name, _)) => format!("{dir}/{name}"),
        None => path.clone(),
    };
    path.ends_with(&suffix) || bare.ends_with(&suffix)
}

/// Set a call's path, relative to the transport, keeping its query. `Err`
/// with `internal` if `path` is no path.
pub fn set_path(head: &mut RequestHead, path: &str) -> Result<(), Fault> {
    let uri = match head.uri.query() {
        Some(query) => format!("{path}?{query}"),
        None => path.to_owned(),
    };
    head.uri = uri.parse().map_err(|e| {
        Fault::internal(format!("a call's path {path:?} is no path")).with_source(e)
    })?;
    Ok(())
}

/// Set a response's content type to `value` if it has none (RFC 9110 8.3).
pub fn default_content_type(head: &mut ResponseHead, value: &'static str) {
    let value = HeaderValue::from_static(value);
    head.headers.entry(CONTENT_TYPE).or_insert(value);
}

/// Encode each value through an ordinary function, with faults propagated at end.
pub fn each<T: 'static, F>(write: F) -> Box<dyn crate::Encoder<T>>
where
    F: FnMut(&T) -> Result<Bytes, Fault> + Send + 'static,
{
    Box::new(Each(write))
}
struct Each<F>(F);
impl<T: 'static, F> crate::Encoder<T> for Each<F>
where
    F: FnMut(&T) -> Result<Bytes, Fault> + Send + 'static,
{
    fn encode(&mut self, value: &T, out: &mut bytes::BytesMut) -> Result<(), Fault> {
        out.extend_from_slice(&(self.0)(value)?);
        Ok(())
    }
}

/// Encode each value through an ordinary function as zero or more messages
/// ([`Encoder::encode_messages`](crate::Encoder::encode_messages)), each a
/// frame of a live body; a body that is not live has them one after another.
pub fn each_message<T: 'static, F>(write: F) -> Box<dyn crate::Encoder<T>>
where
    F: FnMut(&T, &mut Vec<Bytes>) -> Result<(), Fault> + Send + 'static,
{
    Box::new(EachMessage(write))
}
struct EachMessage<F>(F);
impl<T: 'static, F> crate::Encoder<T> for EachMessage<F>
where
    F: FnMut(&T, &mut Vec<Bytes>) -> Result<(), Fault> + Send + 'static,
{
    fn encode(&mut self, value: &T, out: &mut bytes::BytesMut) -> Result<(), Fault> {
        let mut messages = Vec::new();
        (self.0)(value, &mut messages)?;
        messages.iter().for_each(|m| out.extend_from_slice(m));
        Ok(())
    }
    fn encode_messages(&mut self, value: &T, out: &mut Vec<Bytes>) -> Result<(), Fault> {
        let written = out.len();
        (self.0)(value, out).inspect_err(|_| out.truncate(written))
    }
}

/// The most frames that decode to nothing one poll of [`parse`] reads before
/// it yields: what tokio's cooperative budget allows one poll of a task
/// (128), so a body always ready, of comments alone say, cannot hold its
/// thread.
const FRAMES_PER_POLL: usize = 128;

/// Parse wire frames with ordinary stateful functions. Values produced before
/// an error precede it. `end` sees clean EOF or a wire fault, never a semantic
/// `push` failure. The returned stream does not prefetch another frame while
/// decoded values remain, and yields after 128 frames that decode to
/// nothing, as tokio's cooperative budget does a task. What `state` holds
/// between frames is its own to bound, as [`sse::Parser`] holds an event to
/// its body's [`limit`](crate::Body::limit): this holds no bytes, and only
/// the values of one frame.
pub fn parse<T, S, P, E>(body: crate::Body, state: S, push: P, end: E) -> EventStream<T>
where
    T: Send + 'static,
    S: Send + 'static,
    P: FnMut(&mut S, Bytes, &mut Vec<T>) -> Result<(), Fault> + Send + 'static,
    E: FnOnce(&mut S, Result<(), Fault>, &mut Vec<T>) -> Result<(), Fault> + Send + 'static,
{
    EventStream::stream(Parsed {
        body,
        state,
        push,
        end: Some(end),
        ready: std::collections::VecDeque::new(),
        ended: false,
    })
}
struct Parsed<T, S, P, E> {
    body: crate::Body,
    state: S,
    push: P,
    end: Option<E>,
    ready: std::collections::VecDeque<Result<T, Fault>>,
    ended: bool,
}
impl<T, S, P, E> Unpin for Parsed<T, S, P, E> {}
impl<T, S, P, E> respan_gateway_core::futures::Stream for Parsed<T, S, P, E>
where
    T: Send + 'static,
    S: Send + 'static,
    P: FnMut(&mut S, Bytes, &mut Vec<T>) -> Result<(), Fault> + Send + 'static,
    E: FnOnce(&mut S, Result<(), Fault>, &mut Vec<T>) -> Result<(), Fault> + Send + 'static,
{
    type Item = Result<T, Fault>;
    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        use std::task::{Poll, ready};
        let this = self.get_mut();
        let mut frames = 0;
        loop {
            if let Some(value) = this.ready.pop_front() {
                return Poll::Ready(Some(value));
            }
            if this.ended {
                return Poll::Ready(None);
            }
            if frames == FRAMES_PER_POLL {
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            let frame = ready!(std::pin::Pin::new(&mut this.body).poll_next(cx));
            frames += 1;
            let mut values = Vec::new();
            let end = match frame {
                Some(Ok(bytes)) => (this.push)(&mut this.state, bytes, &mut values)
                    .err()
                    .map(Err),
                tail => {
                    let wire = tail.transpose().map(|_| ());
                    let ended = (this.end.take().expect("parser ended once"))(
                        &mut this.state,
                        wire.clone(),
                        &mut values,
                    );
                    Some(wire.and(ended))
                }
            };
            this.ready.extend(values.into_iter().map(Ok));
            if let Some(end) = end {
                this.ended = true;
                if let Err(fault) = end {
                    this.ready.push_back(Err(fault));
                }
            }
        }
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
