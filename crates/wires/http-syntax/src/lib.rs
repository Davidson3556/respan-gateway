#![warn(missing_docs)]
//! How HTTP's paths and headers are read, with no I/O: `%XX` decoding, a
//! path as matching reads it and as a relay forwards it, and a header's
//! tokens and a hop's own headers. A router or a format that recognises a
//! request depends on this crate alone, not on the HTTP wire.

use http::header::{
    self, CONNECTION, CONTENT_LENGTH, HeaderName, PROXY_AUTHENTICATE, PROXY_AUTHORIZATION, TE,
    TRAILER, TRANSFER_ENCODING, UPGRADE,
};
use http::{HeaderMap, request::Parts as RequestHead};
use respan_gateway_core::Fault;

/// `s` with its `%XX` escapes decoded, a bad UTF-8 sequence replaced: how a
/// path or a parameter's name is compared, so a spelling cannot dodge a match.
pub fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        let hex = |b: u8| char::from(b).to_digit(16);
        let escaped = match bytes[i..] {
            [b'%', hi, lo, ..] => hex(hi).zip(hex(lo)).map(|(h, l)| (h * 16 + l) as u8),
            _ => None,
        };
        out.push(escaped.unwrap_or(bytes[i]));
        i += if escaped.is_some() { 3 } else { 1 };
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// A hop's own headers: the connection's (RFC 9110 7.6.1), the proxy's
/// (11.7), `Trailer`, as trailers are not delivered, and the length.
pub const HOP_BY_HOP: [HeaderName; 10] = [
    CONNECTION,
    CONTENT_LENGTH,
    HeaderName::from_static("keep-alive"),
    HeaderName::from_static("proxy-connection"),
    PROXY_AUTHENTICATE,
    PROXY_AUTHORIZATION,
    TE,
    TRAILER,
    TRANSFER_ENCODING,
    UPGRADE,
];

/// Whether `name` is the connection's, never a transport's to send: a hop's
/// own, `Expect` or `Host`.
pub fn is_connection_header(name: &HeaderName) -> bool {
    HOP_BY_HOP.contains(name) || name == header::EXPECT || name == header::HOST
}

/// The comma-separated tokens of every `name` header (RFC 9110 5.6.1).
pub fn header_tokens(headers: &HeaderMap, name: HeaderName) -> impl Iterator<Item = &str> {
    let values = headers.get_all(name).into_iter();
    values
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .map(str::trim)
}

/// Whether `bytes` are a token (RFC 9110 5.6.2), as a subprotocol is.
pub fn is_token(bytes: &[u8]) -> bool {
    !bytes.is_empty()
        && (bytes.iter()).all(|b| b.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(b))
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
    for segment in percent_decode(path).split(['/', '\\']) {
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
        match percent_decode(name).as_str() {
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
