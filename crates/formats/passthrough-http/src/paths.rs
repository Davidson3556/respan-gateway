//! The paths a passthrough takes: those every reader reads one way.

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
///
/// [`clean_path`]: respan_gateway_http_syntax::clean_path
pub(crate) fn reads_one_way(path: &str) -> bool {
    let decoded = respan_gateway_http_syntax::percent_decode(path);
    decoded
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || PATH_BYTES.contains(c))
        && decoded.split('/').all(|s| !s.ends_with('.'))
}

/// The passthrough forwards a path it never read, so every reader of what
/// it forwards must read what matching read: else a vendor, or a front end
/// before it, could read a chat endpoint where the chat format read none,
/// and a chat call would pass the chat format and its plugins.
#[cfg(test)]
mod path_readers {
    use super::reads_one_way;
    use respan_gateway_http_syntax::percent_decode;
    use respan_gateway_http_syntax::{clean_path, forwarded_path};

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
