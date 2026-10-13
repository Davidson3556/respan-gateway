//! `multipart/form-data` (RFC 7578), read from a whole body a client sent
//! and written to a provider. A client's body is untrusted: reading it never
//! panics, holds no more than the body, and refuses what it cannot read
//! one way with a 400.

use memchr::memmem;
use respan_gateway_core::{BadRequest, Fault};
use respan_gateway_http::bytes::{BufMut, Bytes, BytesMut};

/// The most parts a body may have.
const MAX_PARTS: usize = 1024;
/// The most bytes of one part's headers.
const MAX_HEAD: usize = 16 << 10;

/// One part of a form.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Part {
    /// Its field name.
    pub name: String,
    /// The file name it was sent as, if it is a file.
    pub filename: Option<String>,
    /// Its `Content-Type`, if it has one.
    pub content_type: Option<String>,
    /// Its content.
    pub body: Bytes,
}

/// The boundary of a `multipart/form-data` content type, if it is one.
pub(crate) fn boundary(content_type: &str) -> Option<String> {
    let mut params = split_params(content_type).into_iter();
    let essence = params.next()?;
    if !essence.trim().eq_ignore_ascii_case("multipart/form-data") {
        return None;
    }
    let boundary = params.find_map(|p| param(&p, "boundary"))?;
    // RFC 2046 5.1.1: 1 to 70 characters.
    (1..=70).contains(&boundary.len()).then_some(boundary)
}

/// The parts of `body`, delimited by `boundary`.
pub(crate) fn parse(boundary: &str, body: &Bytes) -> Result<Vec<Part>, Fault> {
    let delimiter = format!("--{boundary}");
    let first = first_delimiter(body, delimiter.as_bytes()).ok_or_else(|| bad("has no parts"))?;
    let next = format!("\r\n--{boundary}");
    let next = memmem::Finder::new(next.as_bytes());
    let mut at = first + delimiter.len();
    let mut parts = Vec::new();
    loop {
        match body.get(at..at + 2) {
            Some(b"--") => return Ok(parts),
            _ => at = after_line(body, at).ok_or_else(|| bad("ends early"))?,
        }
        if parts.len() == MAX_PARTS {
            return Err(bad("has too many parts"));
        }
        let head_end = memmem::find(&body[at..body.len().min(at + MAX_HEAD + 4)], b"\r\n\r\n")
            .ok_or_else(|| bad("has a part whose headers do not end"))?;
        let head = &body[at..at + head_end];
        let start = at + head_end + 4;
        let end = start + next.find(&body[start..]).ok_or_else(|| bad("ends early"))?;
        parts.push(part(head, body.slice(start..end))?);
        at = end + next.needle().len();
    }
}

/// Where the first delimiter starts: at the start of the body or of a line.
fn first_delimiter(body: &[u8], delimiter: &[u8]) -> Option<usize> {
    if body.starts_with(delimiter) {
        return Some(0);
    }
    let mut line = b"\r\n".to_vec();
    line.extend_from_slice(delimiter);
    memmem::find(body, &line).map(|i| i + 2)
}

/// Past the transport padding and line end after a delimiter that ends at
/// `at` (RFC 2046 5.1.1).
fn after_line(body: &[u8], mut at: usize) -> Option<usize> {
    while matches!(body.get(at), Some(b' ' | b'\t')) {
        at += 1;
    }
    (body.get(at..at + 2) == Some(b"\r\n")).then_some(at + 2)
}

/// A part of headers `head` and content `body`.
fn part(head: &[u8], body: Bytes) -> Result<Part, Fault> {
    let head = std::str::from_utf8(head).map_err(|_| bad("has headers that are not UTF-8"))?;
    let (mut disposition, mut content_type) = (None, None);
    for line in head.split("\r\n") {
        let (name, value) = line
            .split_once(':')
            .ok_or_else(|| bad("has a header line with no name"))?;
        let value = value.trim().to_owned();
        let slot = match name.trim().to_ascii_lowercase().as_str() {
            "content-disposition" => &mut disposition,
            "content-type" => &mut content_type,
            _ => continue,
        };
        if slot.replace(value).is_some() {
            return Err(bad("has a part with a header twice"));
        }
    }
    let disposition = disposition.ok_or_else(|| bad("has a part with no Content-Disposition"))?;
    let mut params = split_params(&disposition).into_iter();
    if !params
        .next()
        .is_some_and(|kind| kind.trim().eq_ignore_ascii_case("form-data"))
    {
        return Err(bad("has a part that is not form-data"));
    }
    let params: Vec<String> = params.collect();
    let named = |key: &str| params.iter().find_map(|p| param(p, key));
    let name = named("name").ok_or_else(|| bad("has a part with no name"))?;
    // RFC 7578 4.2 deprecates `filename*`, but clients still send it.
    let filename = named("filename*")
        .and_then(|f| extended(&f))
        .or_else(|| named("filename"));
    Ok(Part {
        name,
        filename,
        content_type,
        body,
    })
}

/// A header value split at each `;` outside quotes.
fn split_params(value: &str) -> Vec<String> {
    let (mut out, mut current, mut quoted, mut escaped) = (Vec::new(), String::new(), false, false);
    for c in value.chars() {
        match c {
            _ if escaped => escaped = false,
            '\\' if quoted => escaped = true,
            '"' => quoted = !quoted,
            ';' if !quoted => {
                out.push(std::mem::take(&mut current));
                continue;
            }
            _ => {}
        }
        current.push(c);
    }
    out.push(current);
    out
}

/// The value of parameter `p` if it is `key`'s, unquoted.
fn param(p: &str, key: &str) -> Option<String> {
    let (name, value) = p.split_once('=')?;
    if !name.trim().eq_ignore_ascii_case(key) {
        return None;
    }
    let value = value.trim();
    let Some(quoted) = value.strip_prefix('"').and_then(|v| v.strip_suffix('"')) else {
        return Some(value.to_owned());
    };
    let (mut out, mut escaped) = (String::new(), false);
    for c in quoted.chars() {
        match c {
            '\\' if !escaped => escaped = true,
            c => {
                out.push(c);
                escaped = false;
            }
        }
    }
    Some(out)
}

/// An RFC 8187 value, `UTF-8''name%20escaped`, decoded.
fn extended(value: &str) -> Option<String> {
    let (charset, rest) = value.split_once('\'')?;
    let (_, encoded) = rest.split_once('\'')?;
    charset
        .eq_ignore_ascii_case("utf-8")
        .then(|| respan_gateway_http_syntax::percent_decode(encoded))
}

/// A 400 for a body that `what`.
fn bad(what: &str) -> Fault {
    BadRequest::fault(format!("the multipart body {what}"))
}

/// A random boundary, which no content holds but by a chance of 2^-128.
pub(crate) fn random_boundary() -> Result<String, Fault> {
    let mut bits = [0u8; 16];
    getrandom::fill(&mut bits)
        .map_err(|e| Fault::internal("no secure random source").with_source(e))?;
    let hex: String = bits.iter().map(|b| format!("{b:02x}")).collect();
    Ok(format!("respan-{hex}"))
}

/// Writes a form.
#[derive(Debug)]
pub(crate) struct Writer {
    boundary: String,
    body: BytesMut,
}

impl Writer {
    /// A form delimited by `boundary`.
    pub fn with_boundary(boundary: String) -> Self {
        Self {
            boundary,
            body: BytesMut::new(),
        }
    }

    /// Its content type, with its boundary.
    pub fn content_type(&self) -> String {
        format!("multipart/form-data; boundary={}", self.boundary)
    }

    /// Add the text field `name`.
    pub fn text(&mut self, name: &str, value: &str) {
        self.head(name, None, None);
        self.body.put_slice(value.as_bytes());
        self.body.put_slice(b"\r\n");
    }

    /// Add the file `filename` of type `content_type` as field `name`.
    pub fn file(&mut self, name: &str, filename: &str, content_type: &str, content: &[u8]) {
        self.head(name, Some(filename), Some(content_type));
        self.body.put_slice(content);
        self.body.put_slice(b"\r\n");
    }

    fn head(&mut self, name: &str, filename: Option<&str>, content_type: Option<&str>) {
        let mut head = format!(
            "--{}\r\nContent-Disposition: form-data; name=\"{}\"",
            self.boundary,
            escape(name)
        );
        if let Some(filename) = filename {
            head.push_str(&format!("; filename=\"{}\"", escape(filename)));
        }
        if let Some(content_type) = content_type {
            head.push_str(&format!("\r\nContent-Type: {}", line(content_type)));
        }
        head.push_str("\r\n\r\n");
        self.body.put_slice(head.as_bytes());
    }

    /// The whole body, closed.
    pub fn finish(mut self) -> Bytes {
        self.body
            .put_slice(format!("--{}--\r\n", self.boundary).as_bytes());
        self.body.freeze()
    }
}

/// A name as a quoted string holds it, as browsers escape one (HTML's
/// multipart/form-data encoding): `"`, CR and LF percent-escaped.
fn escape(name: &str) -> String {
    name.replace('"', "%22")
        .replace('\r', "%0D")
        .replace('\n', "%0A")
}

/// A header value with no line ends, so it cannot start another header.
fn line(value: &str) -> String {
    value.replace(['\r', '\n'], " ")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parts(body: &'static [u8]) -> Result<Vec<Part>, Fault> {
        parse("XyZ", &Bytes::from_static(body))
    }

    #[test]
    fn a_form_is_read_as_written() {
        let mut form = Writer::with_boundary("XyZ".into());
        form.text("prompt", "a \"cat\"\r\n--XyQ");
        form.file("image[]", "c\"at.png", "image/png", b"\x89PNG\r\n\r\n--Xy");
        let content_type = form.content_type();
        let body = form.finish();
        let boundary = boundary(&content_type);
        assert_eq!(boundary.as_deref(), Some("XyZ"));
        let read = parse("XyZ", &body).expect("a form");
        assert_eq!(read.len(), 2);
        assert_eq!(read[0].name, "prompt");
        assert_eq!(&read[0].body[..], b"a \"cat\"\r\n--XyQ");
        assert_eq!(read[1].name, "image[]");
        assert_eq!(read[1].filename.as_deref(), Some("c%22at.png"));
        assert_eq!(read[1].content_type.as_deref(), Some("image/png"));
        assert_eq!(&read[1].body[..], b"\x89PNG\r\n\r\n--Xy");
    }

    #[test]
    fn a_boundary_is_read_however_it_is_quoted() {
        for (content_type, want) in [
            ("multipart/form-data; boundary=abc", Some("abc")),
            ("Multipart/Form-Data;boundary=\"a b;c\"", Some("a b;c")),
            ("multipart/form-data; charset=utf-8; BOUNDARY=x", Some("x")),
            ("multipart/form-data", None),
            ("multipart/form-data; boundary=", None),
            ("multipart/mixed; boundary=abc", None),
            ("application/json", None),
        ] {
            assert_eq!(boundary(content_type).as_deref(), want, "{content_type}");
        }
        let long = format!("multipart/form-data; boundary={}", "x".repeat(71));
        assert_eq!(boundary(&long), None);
    }

    #[test]
    fn a_preamble_padding_and_extended_file_names_are_read() {
        let read = parts(
            b"preamble\r\n--XyZ  \r\ncontent-disposition: form-data; name=file; \
              filename=\"a.wav\"; filename*=UTF-8''%C3%A9t%C3%A9.wav\r\n\r\nRIFF\r\n--XyZ--\r\nepilogue",
        )
        .expect("a form");
        assert_eq!(read.len(), 1);
        assert_eq!(read[0].filename.as_deref(), Some("été.wav"));
        assert_eq!(&read[0].body[..], b"RIFF");
    }

    #[test]
    fn what_cannot_be_read_one_way_is_refused() {
        for body in [
            &b""[..],
            b"--XyZ",
            b"--XyZ\r\n",
            b"--XyZ\r\nContent-Disposition: form-data; name=a\r\n\r\nno end",
            b"--XyZ\r\nContent-Disposition: form-data; name=a\r\n",
            b"--XyZ\r\nContent-Type: text/plain\r\n\r\nx\r\n--XyZ--",
            b"--XyZ\r\nContent-Disposition: attachment; name=a\r\n\r\nx\r\n--XyZ--",
            b"--XyZ\r\nContent-Disposition: form-data\r\n\r\nx\r\n--XyZ--",
            b"--XyZ\r\nno colon\r\n\r\nx\r\n--XyZ--",
            b"--XyZ\r\nContent-Disposition: form-data; name=a\r\nContent-Disposition: form-data; name=b\r\n\r\nx\r\n--XyZ--",
            b"--XyZ\r\nContent-Disposition: form-data; name=a\r\n\r\nx\r\n--XyZjunk",
            b"--XyZ\r\nContent-Disposition: form-data; name=\xff\r\n\r\nx\r\n--XyZ--",
        ] {
            let read = parts(body);
            assert!(
                read.as_ref().is_err_and(|f| f.is::<BadRequest>()),
                "{:?}: {read:?}",
                String::from_utf8_lossy(body)
            );
        }
        assert_eq!(parts(b"--XyZ--\r\n").map(|p| p.len()).ok(), Some(0));
    }

    #[test]
    fn every_cut_of_a_form_is_read_or_refused_without_panicking() {
        let mut form = Writer::with_boundary("XyZ".into());
        form.text("a", "1");
        form.file("f", "x.bin", "application/octet-stream", b"\r\n-\r\n--X");
        let body = form.finish();
        for end in 0..body.len() {
            for start in 0..end.min(8) {
                let _ = parse("XyZ", &body.slice(start..end));
            }
        }
        assert_eq!(parse("XyZ", &body).map(|p| p.len()).ok(), Some(2));
    }

    #[test]
    fn a_header_cannot_end_its_line_early() {
        let mut form = Writer::with_boundary("b".into());
        form.file("x\r\nX-Evil: 1", "y\nz", "image/png\r\nX-Evil: 1", b"");
        let read = parse("b", &form.finish()).expect("a form");
        assert_eq!(read[0].name, "x%0D%0AX-Evil: 1");
        assert_eq!(
            read[0].content_type.as_deref(),
            Some("image/png  X-Evil: 1")
        );
    }
}
