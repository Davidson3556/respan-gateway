//! Cloudinary (<https://cloudinary.com>): image and video uploads, search,
//! asset details, deletion and transformation URLs, from its Upload API
//! (`POST /{cloud}/{resource_type}/upload`, `POST /{cloud}/{resource_type}/destroy`)
//! and Admin API (`POST /{cloud}/resources/search`,
//! `GET /{cloud}/resources/{resource_type}/{type}/{public_id}`) at
//! `https://api.cloudinary.com/v1_1`, with HTTP Basic auth.
//!
//! The key is Cloudinary's own `CLOUDINARY_URL`,
//! `cloudinary://<api_key>:<api_secret>@<cloud_name>`: it names the cloud as
//! well as the credentials, so the tools send it themselves
//! ([`KeyIn::Manual`]), the API key and secret as Basic auth and the cloud
//! name in each path. Delivery URLs are built locally, on
//! `https://res.cloudinary.com`, and unsigned.

use async_trait::async_trait;
use respan_gateway_tool_vendor_kit::args::{
    object, optional_bool, optional_enum, optional_int, optional_str, optional_str_list, pretty,
    required_str,
};
use respan_gateway_tool_vendor_kit::reqwest::{Method, RequestBuilder, multipart};
use respan_gateway_tool_vendor_kit::{
    Api, KeyIn, ToolError, ToolOutput, ToolSpec, ToolVendor, Tools,
};
use serde_json::{Map, Value, json};

/// Cloudinary.
pub const VENDOR: ToolVendor = ToolVendor {
    id: "cloudinary",
    key_env: &["CLOUDINARY_URL"],
    key_in: KeyIn::Manual,
    base_url: "https://api.cloudinary.com/v1_1",
    tools: &CloudinaryTools,
};

/// Where Cloudinary delivers assets.
pub const DELIVERY_URL: &str = "https://res.cloudinary.com";

/// Cloudinary's tools: `upload`, `search`, `get_asset`, `delivery_url` and
/// `delete_asset`.
#[derive(Debug, Clone, Copy)]
pub struct CloudinaryTools;

const STORED: &[&str] = &["image", "video", "raw"];
const PRIVACY: &[&str] = &["upload", "private", "authenticated"];

/// The most of a result returned, in bytes.
const MAX_OUTPUT: usize = 20_000;

const TOOLS: &[ToolSpec] = &[
    ToolSpec {
        name: "upload",
        description: "Upload an image, video or other file to the Cloudinary account from a \
            public URL (or a data: URI) and get its public ID, size and delivery URL. Creates a \
            stored asset, counting toward the account's storage and credits; with `overwrite` \
            it replaces any asset of the same public ID. `eager` transformations are generated \
            now and cost transformation credits.",
        input_schema: r#"{
  "type": "object",
  "properties": {
    "file": {"type": "string", "description": "The file: an http(s) URL Cloudinary downloads, or a base64 data: URI."},
    "resource_type": {"type": "string", "enum": ["auto", "image", "video", "raw"], "default": "auto", "description": "image, video (also audio), raw (any other file), or auto to detect it."},
    "public_id": {"type": "string", "description": "The asset's ID, without extension (with it for raw files); random if not given."},
    "asset_folder": {"type": "string", "description": "The folder to put it in, e.g. \"products/shoes\"."},
    "tags": {"type": "array", "items": {"type": "string"}, "description": "Tags for the asset."},
    "overwrite": {"type": "boolean", "default": true, "description": "Replace an existing asset with the same public ID."},
    "eager": {"type": "string", "description": "Transformations to generate now, pipe-separated, e.g. \"c_fill,w_400,h_300|w_100\"."}
  },
  "required": ["file"]
}"#,
    },
    ToolSpec {
        name: "search",
        description: "Search the Cloudinary account's assets with a Lucene-like expression and \
            get each one's public ID, type, size, tags and delivery URL. Each call is one Admin \
            API request, from an hourly limit (500 on the free plan).",
        input_schema: r#"{
  "type": "object",
  "properties": {
    "expression": {"type": "string", "description": "The query, e.g. \"resource_type:image AND tags=shoe AND uploaded_at>1d\", \"asset_folder:products/*\", \"format:png AND bytes>1m\". Every asset, newest first, if not given."},
    "sort_by": {"type": "string", "description": "A field to sort by, e.g. created_at, public_id, bytes. Newest first if not given."},
    "order": {"type": "string", "enum": ["asc", "desc"], "default": "desc", "description": "The sort direction."},
    "max_results": {"type": "integer", "minimum": 1, "maximum": 500, "default": 20, "description": "At most this many assets."},
    "next_cursor": {"type": "string", "description": "The previous result's next_cursor, for the next page."}
  }
}"#,
    },
    ToolSpec {
        name: "get_asset",
        description: "Get one Cloudinary asset's details: size, format, folder, tags, context, \
            metadata and its derived (transformed) versions, optionally its colors and embedded \
            metadata. Each call is one Admin API request, from an hourly limit.",
        input_schema: r#"{
  "type": "object",
  "properties": {
    "public_id": {"type": "string", "description": "The asset's public ID."},
    "resource_type": {"type": "string", "enum": ["image", "video", "raw"], "default": "image", "description": "image, video (also audio) or raw."},
    "type": {"type": "string", "enum": ["upload", "private", "authenticated"], "default": "upload", "description": "Its delivery type."},
    "colors": {"type": "boolean", "default": false, "description": "Include its predominant colors."},
    "media_metadata": {"type": "boolean", "default": false, "description": "Include its Exif, IPTC and XMP metadata."}
  },
  "required": ["public_id"]
}"#,
    },
    ToolSpec {
        name: "delivery_url",
        description: "Build a Cloudinary delivery URL that serves an asset transformed (resized, \
            cropped, converted, filtered...) on the fly, from a transformation string such as \
            \"c_fill,w_300,h_300,g_auto/f_auto/q_auto\". Makes no API call; Cloudinary \
            generates the result when the URL is first opened, using transformation credits. \
            With type \"fetch\" it transforms a remote image by its URL, if the account allows \
            fetch. The URL is unsigned, so not for private or authenticated assets.",
        input_schema: r#"{
  "type": "object",
  "properties": {
    "public_id": {"type": "string", "description": "The asset's public ID, or for type fetch the remote image's http(s) URL."},
    "transformation": {"type": "string", "description": "Cloudinary transformation parameters, chained with \"/\", e.g. \"c_fill,w_300,h_300/e_grayscale\" or \"so_2,w_640/f_jpg\" for a video frame."},
    "resource_type": {"type": "string", "enum": ["image", "video", "raw"], "default": "image", "description": "image, video (also audio) or raw."},
    "type": {"type": "string", "enum": ["upload", "fetch"], "default": "upload", "description": "upload: an asset in the account; fetch: a remote image by URL."},
    "format": {"type": "string", "description": "The file extension to deliver as, e.g. jpg, webp, mp4, gif (not for fetch: use f_<format> in transformation)."},
    "version": {"type": "integer", "minimum": 1, "description": "The asset's version, from upload or search, to bypass cached copies."}
  },
  "required": ["public_id"]
}"#,
    },
    ToolSpec {
        name: "delete_asset",
        description: "Permanently delete one asset from the Cloudinary account (destructive: \
            only backups, if enabled, keep it). Cached copies may still be served by the CDN \
            unless `invalidate` is set.",
        input_schema: r#"{
  "type": "object",
  "properties": {
    "public_id": {"type": "string", "description": "The asset's public ID."},
    "resource_type": {"type": "string", "enum": ["image", "video", "raw"], "default": "image", "description": "image, video (also audio) or raw."},
    "type": {"type": "string", "enum": ["upload", "private", "authenticated"], "default": "upload", "description": "Its delivery type."},
    "invalidate": {"type": "boolean", "default": false, "description": "Also purge its cached copies from the CDN."}
  },
  "required": ["public_id"]
}"#,
    },
];

#[async_trait]
impl Tools for CloudinaryTools {
    fn list(&self) -> &'static [ToolSpec] {
        TOOLS
    }

    async fn call(&self, api: &Api, name: &str, arguments: Value) -> Result<ToolOutput, ToolError> {
        let args = object(&arguments)?;
        let account = Account::parse(api.key())?;
        match name {
            "upload" => {
                let (resource_type, fields) = upload_fields(args)?;
                let request = account
                    .request(api, Method::POST, &format!("{resource_type}/upload"))
                    .multipart(form(fields));
                Ok(render_asset(&api.json(request).await?).into())
            }
            "search" => {
                let body = search_body(args)?;
                let request = account
                    .request(api, Method::POST, "resources/search")
                    .json(&body);
                Ok(render_search(&api.json(request).await?).into())
            }
            "get_asset" => {
                let (path, query) = asset_request(args)?;
                let request = account.request(api, Method::GET, &path).query(&query);
                Ok(render_asset(&api.json(request).await?).into())
            }
            "delivery_url" => delivery_url(account.cloud, args).map(Into::into),
            "delete_asset" => {
                let (resource_type, fields) = destroy_fields(args)?;
                let request = account
                    .request(api, Method::POST, &format!("{resource_type}/destroy"))
                    .multipart(form(fields));
                render_destroy(&api.json(request).await?).map(Into::into)
            }
            _ => Err(ToolError::new(format!("no tool named {name:?}"))),
        }
    }
}

/// The cloud and credentials a `CLOUDINARY_URL` names.
#[derive(Debug, PartialEq, Eq)]
struct Account<'a> {
    cloud: &'a str,
    api_key: &'a str,
    api_secret: &'a str,
}

impl<'a> Account<'a> {
    /// `cloudinary://<api_key>:<api_secret>@<cloud_name>`, the scheme
    /// optional and any query ignored. A bad one is an error that does not
    /// quote it.
    fn parse(key: &'a str) -> Result<Self, ToolError> {
        let bad = || {
            ToolError::new(
                "the Cloudinary key must be a CLOUDINARY_URL: \
                 cloudinary://<api_key>:<api_secret>@<cloud_name>",
            )
        };
        let key = key.trim();
        let rest = key.strip_prefix("cloudinary://").unwrap_or(key);
        let rest = rest.split(['?', '#']).next().unwrap_or_default();
        let (credentials, cloud) = rest.rsplit_once('@').ok_or_else(bad)?;
        let (api_key, api_secret) = credentials.split_once(':').ok_or_else(bad)?;
        let cloud = cloud.trim_matches('/');
        if cloud.is_empty() || cloud.contains('/') || api_key.is_empty() || api_secret.is_empty() {
            return Err(bad());
        }
        Ok(Self {
            cloud,
            api_key,
            api_secret,
        })
    }

    /// A `method` request to `path` under the cloud, with Basic auth.
    fn request(&self, api: &Api, method: Method, path: &str) -> RequestBuilder {
        api.request(method, &format!("{}/{path}", self.cloud))
            .basic_auth(self.api_key, Some(self.api_secret))
    }
}

/// Form fields or query parameters, by name.
type Fields = Vec<(&'static str, String)>;

/// Form fields as a multipart body, as the Upload API's examples send them.
fn form(fields: Fields) -> multipart::Form {
    fields
        .into_iter()
        .fold(multipart::Form::new(), |form, (k, v)| form.text(k, v))
}

/// `s` percent-encoded, but for unreserved characters and those in `keep`.
fn encode(s: &str, keep: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || b"-_.~".contains(&b) || keep.as_bytes().contains(&b) {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

/// A public ID as a URL path, its `/`s kept.
fn id_path(public_id: &str) -> String {
    encode(public_id.trim_matches('/'), "/:")
}

fn bool_field(v: bool) -> String {
    if v { "true" } else { "false" }.into()
}

/// The upload's resource type and form fields from the tool's arguments.
fn upload_fields(args: &Map<String, Value>) -> Result<(&str, Fields), ToolError> {
    let file = required_str(args, "file")?;
    if !(file.starts_with("http://") || file.starts_with("https://") || file.starts_with("data:")) {
        return Err(ToolError::new(
            "`file` must be an http(s) URL or a data: URI",
        ));
    }
    let resource_type =
        optional_enum(args, "resource_type", &["auto", "image", "video", "raw"])?.unwrap_or("auto");
    let mut fields = vec![("file", file.to_owned())];
    for key in ["public_id", "asset_folder", "eager"] {
        if let Some(v) = optional_str(args, key)? {
            fields.push((key, v.to_owned()));
        }
    }
    if let Some(v) = optional_str_list(args, "tags")? {
        fields.push(("tags", v.join(",")));
    }
    if let Some(v) = optional_bool(args, "overwrite")? {
        fields.push(("overwrite", bool_field(v)));
    }
    Ok((resource_type, fields))
}

/// `POST /resources/search`'s body from the tool's arguments.
fn search_body(args: &Map<String, Value>) -> Result<Value, ToolError> {
    let mut body = Map::new();
    if let Some(v) = optional_str(args, "expression")? {
        body.insert("expression".into(), v.into());
    }
    let order = optional_enum(args, "order", &["asc", "desc"])?;
    match (optional_str(args, "sort_by")?, order) {
        (Some(field), order) => {
            body.insert(
                "sort_by".into(),
                json!([{ field: order.unwrap_or("desc") }]),
            );
        }
        (None, Some(order)) => {
            body.insert("sort_by".into(), json!([{ "created_at": order }]));
        }
        (None, None) => {}
    }
    let max = optional_int(args, "max_results", 1, 500)?.unwrap_or(20);
    body.insert("max_results".into(), max.into());
    if let Some(v) = optional_str(args, "next_cursor")? {
        body.insert("next_cursor".into(), v.into());
    }
    body.insert("with_field".into(), json!(["tags"]));
    Ok(Value::Object(body))
}

/// The resource type, delivery type and public ID's path.
fn asset_path(args: &Map<String, Value>) -> Result<(&str, &str, String), ToolError> {
    let public_id = required_str(args, "public_id")?;
    let resource_type = optional_enum(args, "resource_type", STORED)?.unwrap_or("image");
    let kind = optional_enum(args, "type", PRIVACY)?.unwrap_or("upload");
    Ok((resource_type, kind, id_path(public_id)))
}

/// `GET /resources/{resource_type}/{type}/{public_id}`'s path and query.
fn asset_request(args: &Map<String, Value>) -> Result<(String, Fields), ToolError> {
    let (resource_type, kind, id) = asset_path(args)?;
    let mut query = Vec::new();
    for key in ["colors", "media_metadata"] {
        if optional_bool(args, key)? == Some(true) {
            query.push((key, "true".to_owned()));
        }
    }
    Ok((format!("resources/{resource_type}/{kind}/{id}"), query))
}

/// The destroy's resource type and form fields from the tool's arguments.
fn destroy_fields(args: &Map<String, Value>) -> Result<(&str, Fields), ToolError> {
    let public_id = required_str(args, "public_id")?;
    let resource_type = optional_enum(args, "resource_type", STORED)?.unwrap_or("image");
    let kind = optional_enum(args, "type", PRIVACY)?.unwrap_or("upload");
    let mut fields = vec![
        ("public_id", public_id.to_owned()),
        ("type", kind.to_owned()),
    ];
    if let Some(v) = optional_bool(args, "invalidate")? {
        fields.push(("invalidate", bool_field(v)));
    }
    Ok((resource_type, fields))
}

/// The delivery URL of the asset the arguments name, in `cloud`.
fn delivery_url(cloud: &str, args: &Map<String, Value>) -> Result<String, ToolError> {
    let public_id = required_str(args, "public_id")?;
    let resource_type = optional_enum(args, "resource_type", STORED)?.unwrap_or("image");
    let kind = optional_enum(args, "type", &["upload", "fetch"])?.unwrap_or("upload");
    let mut url = format!("{DELIVERY_URL}/{cloud}/{resource_type}/{kind}");
    if let Some(t) = optional_str(args, "transformation")? {
        let t = t.trim().trim_matches('/');
        if t.contains(|c: char| c.is_whitespace() || "?#".contains(c)) {
            return Err(ToolError::new(
                "`transformation` must be Cloudinary parameters such as \
                 \"c_fill,w_300,h_300/f_auto\", with no spaces, ? or #",
            ));
        }
        url.push('/');
        url.push_str(t);
    }
    if let Some(v) = optional_int(args, "version", 1, u64::MAX)? {
        url.push_str(&format!("/v{v}"));
    }
    let format = optional_str(args, "format")?.map(|f| f.trim_start_matches('.'));
    if kind == "fetch" {
        if !(public_id.starts_with("http://") || public_id.starts_with("https://")) {
            return Err(ToolError::new(
                "for type fetch, `public_id` must be the remote image's http(s) URL",
            ));
        }
        if format.is_some() {
            return Err(ToolError::new(
                "`format` does not apply to fetch; add f_<format> to `transformation`",
            ));
        }
        url.push('/');
        url.push_str(&encode(public_id, ""));
        return Ok(url);
    }
    url.push('/');
    url.push_str(&id_path(public_id));
    if let Some(f) = format {
        if !f.bytes().all(|b| b.is_ascii_alphanumeric()) {
            return Err(ToolError::new(format!(
                "`format` must be a file extension such as jpg or mp4, not {f:?}"
            )));
        }
        url.push('.');
        url.push_str(f);
    }
    Ok(url)
}

fn array_of<'a>(v: &'a Value, key: &str) -> &'a [Value] {
    v.get(key)
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or_default()
}

/// `asset`'s fields named in `keep`, those present and not null or empty.
fn kept(asset: &Value, keep: &[&str]) -> Map<String, Value> {
    keep.iter()
        .filter_map(|k| {
            asset
                .get(*k)
                .filter(|v| match v {
                    Value::Null => false,
                    Value::String(s) => !s.is_empty(),
                    Value::Array(a) => !a.is_empty(),
                    Value::Object(o) => !o.is_empty(),
                    _ => true,
                })
                .map(|v| ((*k).to_owned(), v.clone()))
        })
        .collect()
}

/// Derived or eager versions as their transformation and URL.
fn versions(list: &[Value]) -> Value {
    list.iter()
        .map(|d| {
            Value::Object(kept(
                d,
                &["transformation", "format", "bytes", "secure_url"],
            ))
        })
        .collect()
}

/// `text` up to [`MAX_OUTPUT`] bytes, cut at a character.
fn capped(text: String) -> String {
    if text.len() <= MAX_OUTPUT {
        return text;
    }
    let mut end = MAX_OUTPUT;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}\n… (cut)", &text[..end])
}

/// An uploaded or looked-up asset as JSON: its identity, size, folder,
/// tags, metadata and URLs. Signatures, ETags, uploader keys and plain
/// http URLs are dropped; colors are cut to the ten strongest.
fn render_asset(response: &Value) -> String {
    const KEEP: &[&str] = &[
        "public_id",
        "asset_id",
        "resource_type",
        "type",
        "format",
        "version",
        "width",
        "height",
        "duration",
        "pages",
        "bytes",
        "asset_folder",
        "display_name",
        "original_filename",
        "created_at",
        "status",
        "access_mode",
        "secure_url",
        "tags",
        "context",
        "metadata",
        "faces",
        "predominant",
        "image_metadata",
        "media_metadata",
        "video",
        "audio",
    ];
    let mut out = kept(response, KEEP);
    if let Some(Value::Array(colors)) = response.get("colors") {
        out.insert(
            "colors".into(),
            Value::Array(colors.iter().take(10).cloned().collect()),
        );
    }
    for key in ["eager", "derived"] {
        let list = array_of(response, key);
        if !list.is_empty() {
            out.insert(key.into(), versions(list));
        }
    }
    capped(pretty(&Value::Object(out)))
}

/// Search results as JSON: the total, each asset in brief, and the cursor
/// for the next page.
fn render_search(response: &Value) -> String {
    const KEEP: &[&str] = &[
        "public_id",
        "resource_type",
        "type",
        "format",
        "version",
        "width",
        "height",
        "duration",
        "bytes",
        "asset_folder",
        "created_at",
        "secure_url",
        "tags",
    ];
    let assets: Vec<Value> = array_of(response, "resources")
        .iter()
        .map(|r| {
            let mut a = kept(r, KEEP);
            if let Some(status) = r.get("status").and_then(Value::as_str)
                && status != "active"
            {
                a.insert("status".into(), status.into());
            }
            Value::Object(a)
        })
        .collect();
    if assets.is_empty() {
        return "No assets.".into();
    }
    let mut out = Map::new();
    if let Some(total) = response.get("total_count") {
        out.insert("total_count".into(), total.clone());
    }
    out.insert("assets".into(), Value::Array(assets));
    if let Some(cursor) = response.get("next_cursor").filter(|c| c.is_string()) {
        out.insert("next_cursor".into(), cursor.clone());
    }
    capped(pretty(&Value::Object(out)))
}

/// A destroy's outcome; anything but `ok` is a [`ToolError`].
fn render_destroy(response: &Value) -> Result<String, ToolError> {
    match response.get("result").and_then(Value::as_str) {
        Some("ok") => Ok("Deleted.".into()),
        Some(other) => Err(ToolError::new(format!("Not deleted: {other}."))),
        None => Err(ToolError::new(format!("Not deleted: {}", pretty(response)))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(v: Value) -> Map<String, Value> {
        match v {
            Value::Object(m) => m,
            _ => Map::new(),
        }
    }

    #[test]
    fn parses_cloudinary_url() {
        let a = Account::parse("cloudinary://123:s3cr-et@demo").expect("valid");
        assert_eq!(
            a,
            Account {
                cloud: "demo",
                api_key: "123",
                api_secret: "s3cr-et"
            }
        );
        assert_eq!(
            Account::parse(" 123:abc@demo/?secure=true ").map(|a| a.cloud),
            Ok("demo")
        );
        for bad in [
            "",
            "123",
            "cloudinary://123@demo",
            "cloudinary://:x@demo",
            "123:x@",
        ] {
            assert!(Account::parse(bad).is_err(), "{bad:?}");
        }
        let err = Account::parse("cloudinary://123:topsecret").expect_err("no cloud");
        assert!(!err.0.contains("topsecret"));
    }

    #[test]
    fn upload_fields_map_and_validate() {
        let a = args(json!({
            "file": "https://a.b/c.jpg", "resource_type": "image", "public_id": "shoes/red",
            "tags": ["sale", "red"], "overwrite": false, "eager": "w_100"
        }));
        let (rt, fields) = upload_fields(&a).expect("valid");
        assert_eq!(rt, "image");
        assert_eq!(
            fields,
            vec![
                ("file", "https://a.b/c.jpg".to_owned()),
                ("public_id", "shoes/red".to_owned()),
                ("eager", "w_100".to_owned()),
                ("tags", "sale,red".to_owned()),
                ("overwrite", "false".to_owned()),
            ]
        );
        assert_eq!(
            upload_fields(&args(json!({"file": "data:image/png;base64,AAAA"})))
                .expect("valid")
                .0,
            "auto"
        );
        assert!(upload_fields(&args(json!({}))).is_err());
        assert!(upload_fields(&args(json!({"file": "/tmp/x.jpg"}))).is_err());
        assert!(
            upload_fields(&args(json!({"file": "https://a", "resource_type": "pdf"}))).is_err()
        );
    }

    #[test]
    fn search_body_maps_arguments() {
        assert_eq!(
            search_body(&args(json!({
                "expression": "tags=shoe", "sort_by": "bytes", "order": "asc",
                "max_results": 5, "next_cursor": "c"
            })))
            .expect("valid"),
            json!({
                "expression": "tags=shoe", "sort_by": [{"bytes": "asc"}], "max_results": 5,
                "next_cursor": "c", "with_field": ["tags"]
            })
        );
        assert_eq!(
            search_body(&args(json!({}))).expect("valid"),
            json!({"max_results": 20, "with_field": ["tags"]})
        );
        assert!(search_body(&args(json!({"max_results": 501}))).is_err());
        assert!(search_body(&args(json!({"order": "up"}))).is_err());
    }

    #[test]
    fn asset_and_destroy_requests() {
        let (path, query) = asset_request(&args(json!({
            "public_id": "my folder/a+b", "resource_type": "video", "colors": true
        })))
        .expect("valid");
        assert_eq!(path, "resources/video/upload/my%20folder/a%2Bb");
        assert_eq!(query, vec![("colors", "true".to_owned())]);
        assert!(asset_request(&args(json!({"public_id": "x", "type": "fetch"}))).is_err());
        let a = args(json!({"public_id": "x", "invalidate": true}));
        let (rt, fields) = destroy_fields(&a).expect("valid");
        assert_eq!(rt, "image");
        assert_eq!(
            fields,
            vec![
                ("public_id", "x".to_owned()),
                ("type", "upload".to_owned()),
                ("invalidate", "true".to_owned()),
            ]
        );
        assert!(destroy_fields(&args(json!({}))).is_err());
    }

    #[test]
    fn builds_delivery_urls() {
        assert_eq!(
            delivery_url(
                "demo",
                &args(json!({
                    "public_id": "shoes/red shoe", "transformation": "/c_fill,w_300,h_300/f_auto/",
                    "version": 1719307544, "format": ".webp"
                }))
            ),
            Ok("https://res.cloudinary.com/demo/image/upload/c_fill,w_300,h_300/f_auto/v1719307544/shoes/red%20shoe.webp".into())
        );
        assert_eq!(
            delivery_url(
                "demo",
                &args(json!({
                    "public_id": "https://x.org/a.png?s=1", "type": "fetch", "transformation": "w_100"
                }))
            ),
            Ok("https://res.cloudinary.com/demo/image/fetch/w_100/https%3A%2F%2Fx.org%2Fa.png%3Fs%3D1".into())
        );
        assert!(
            delivery_url(
                "demo",
                &args(json!({"public_id": "a", "transformation": "w 1"}))
            )
            .is_err()
        );
        assert!(delivery_url("demo", &args(json!({"public_id": "a", "type": "fetch"}))).is_err());
        assert!(
            delivery_url(
                "demo",
                &args(json!({"public_id": "https://a", "type": "fetch", "format": "jpg"}))
            )
            .is_err()
        );
        assert!(delivery_url("demo", &args(json!({"public_id": "a", "format": "j/pg"}))).is_err());
    }

    #[test]
    fn renders_asset() {
        let text = render_asset(&json!({
            "asset_id": "3515", "public_id": "gotj", "version": 1719307544, "version_id": "7d2c",
            "signature": "d0b1", "width": 1920, "height": 1441, "format": "jpg",
            "resource_type": "image", "created_at": "2024-06-25T09:25:44Z", "tags": [],
            "bytes": 896838, "type": "upload", "etag": "2a2d", "placeholder": false,
            "url": "http://res.cloudinary.com/d/image/upload/v1/gotj.jpg",
            "secure_url": "https://res.cloudinary.com/d/image/upload/v1/gotj.jpg",
            "asset_folder": "", "created_by": {"access_key": "6143"},
            "eager": [{"transformation": "c_pad,h_300,w_400", "width": 400, "height": 300,
                "url": "http://e", "secure_url": "https://e"}]
        }));
        let parsed: Value = serde_json::from_str(&text).expect("json");
        assert_eq!(
            parsed,
            json!({
                "public_id": "gotj", "asset_id": "3515", "resource_type": "image", "type": "upload",
                "format": "jpg", "version": 1719307544, "width": 1920, "height": 1441,
                "bytes": 896838, "created_at": "2024-06-25T09:25:44Z",
                "secure_url": "https://res.cloudinary.com/d/image/upload/v1/gotj.jpg",
                "eager": [{"transformation": "c_pad,h_300,w_400", "secure_url": "https://e"}]
            })
        );
        let colors: Vec<Value> = (0..32).map(|i| json!([format!("#{i:06X}"), 1.0])).collect();
        let text = render_asset(&json!({"public_id": "p", "colors": colors}));
        let parsed: Value = serde_json::from_str(&text).expect("json");
        assert_eq!(parsed["colors"].as_array().map(Vec::len), Some(10));
    }

    #[test]
    fn renders_search_and_destroy() {
        let text = render_search(&json!({
            "total_count": 201, "time": 1021, "next_cursor": "b16b",
            "resources": [{
                "asset_id": "0916", "public_id": "blue_sweater", "asset_folder": "Clothing Sale",
                "filename": "blue_sweater", "format": "jpg", "version": 1719316754,
                "resource_type": "image", "type": "upload", "bytes": 71063, "width": 4800,
                "height": 6400, "aspect_ratio": 0.75,
                "url": "http://res.cloudinary.com/cld-docs/image/upload/v1719316754/blue_sweater.jpg",
                "secure_url": "https://res.cloudinary.com/cld-docs/image/upload/v1719316754/blue_sweater.jpg",
                "status": "active", "created_by": {"access_key": "6143"}, "tags": ["sale"]
            }, {"public_id": "gone", "status": "not_found"}]
        }));
        let parsed: Value = serde_json::from_str(&text).expect("json");
        assert_eq!(
            parsed,
            json!({
                "total_count": 201,
                "assets": [{
                    "public_id": "blue_sweater", "resource_type": "image", "type": "upload",
                    "format": "jpg", "version": 1719316754, "width": 4800, "height": 6400,
                    "bytes": 71063, "asset_folder": "Clothing Sale",
                    "secure_url": "https://res.cloudinary.com/cld-docs/image/upload/v1719316754/blue_sweater.jpg",
                    "tags": ["sale"]
                }, {"public_id": "gone", "status": "not_found"}],
                "next_cursor": "b16b"
            })
        );
        assert_eq!(
            render_search(&json!({"total_count": 0, "resources": []})),
            "No assets."
        );
        assert_eq!(
            render_destroy(&json!({"result": "ok"})),
            Ok("Deleted.".into())
        );
        assert_eq!(
            render_destroy(&json!({"result": "not found"})),
            Err(ToolError::new("Not deleted: not found."))
        );
    }
}
