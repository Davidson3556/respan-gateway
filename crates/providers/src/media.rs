//! Translation of common media semantics; foreign native fields never disappear.
use crate::anthropic::ANTHROPIC;
use crate::common::{Keep, kept_fields, native_fields, untranslatable};
use crate::openai::OPENAI;
use base64::{Engine, engine::general_purpose::STANDARD};
use respan_gateway_conversation::{Content, Media, MediaSource};
use respan_gateway_sdk::Fault;
use serde_json::{Map, Value, json};

fn remainder(v: &Value, keys: &[&str]) -> Map<String, Value> {
    v.as_object()
        .into_iter()
        .flatten()
        .filter(|(k, _)| !keys.contains(&k.as_str()))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}
fn source_url(url: &str, fallback: &str) -> Option<(String, MediaSource)> {
    if let Some(data) = url.strip_prefix("data:") {
        let (mime, data) = data.split_once(";base64,")?;
        Some((
            mime.into(),
            MediaSource::Bytes(STANDARD.decode(data).ok()?.into()),
        ))
    } else {
        Some((fallback.into(), MediaSource::Url(url.into())))
    }
}
fn source(v: &Value) -> Option<(String, MediaSource)> {
    match v["type"].as_str()? {
        "base64" => Some((
            v["media_type"].as_str()?.into(),
            MediaSource::Bytes(STANDARD.decode(v["data"].as_str()?).ok()?.into()),
        )),
        "url" => Some((
            "image/*".into(),
            MediaSource::Url(v["url"].as_str()?.into()),
        )),
        _ => None,
    }
}
/// Decode only shapes whose common semantics are understood.
pub(crate) fn read(format: &str, v: &Value) -> Option<Content> {
    let typ = v["type"].as_str()?;
    let (kind, mime, source, mut fields, name) = match (format, typ) {
        (OPENAI, "image_url") => {
            let image = &v["image_url"];
            let (mime, src) = source_url(image["url"].as_str()?, "image/*")?;
            let mut fields = remainder(v, &["type", "image_url"]);
            let nested = remainder(image, &["url"]);
            if !nested.is_empty() {
                fields.insert("image_url".into(), nested.into());
            }
            (0, mime, src, fields, None)
        }
        (OPENAI, "input_audio") => {
            let audio = &v["input_audio"];
            let mime = match audio["format"].as_str()? {
                "wav" => "audio/wav",
                "mp3" => "audio/mpeg",
                _ => return None,
            };
            let src = MediaSource::Bytes(STANDARD.decode(audio["data"].as_str()?).ok()?.into());
            let mut fields = remainder(v, &["type", "input_audio"]);
            let nested = remainder(audio, &["data", "format"]);
            if !nested.is_empty() {
                fields.insert("input_audio".into(), nested.into());
            }
            (1, mime.into(), src, fields, None)
        }
        (OPENAI, "file") => {
            let file = &v["file"];
            if file.get("filename").is_some_and(|v| !v.is_string()) {
                return None;
            }
            let data = file["file_data"].as_str()?;
            if !data.starts_with("data:") {
                return None;
            }
            let (mime, src) = source_url(data, "application/octet-stream")?;
            let mut fields = remainder(v, &["type", "file"]);
            let nested = remainder(file, &["file_data", "filename"]);
            if !nested.is_empty() {
                fields.insert("file".into(), nested.into());
            }
            (
                2,
                mime,
                src,
                fields,
                file["filename"].as_str().map(str::to_owned),
            )
        }
        (ANTHROPIC, "image" | "document") => {
            if v.get("title").is_some_and(|v| !v.is_string()) {
                return None;
            }
            let (mut mime, src) = source(&v["source"])?;
            if typ == "document" && mime == "image/*" {
                mime = "application/pdf".into();
            }
            let mut fields = remainder(v, &["type", "source", "title"]);
            let known = if v["source"]["type"] == "base64" {
                &["type", "media_type", "data"][..]
            } else {
                &["type", "url"][..]
            };
            let nested = remainder(&v["source"], known);
            if !nested.is_empty() {
                fields.insert("source".into(), nested.into());
            }
            (
                if typ == "image" { 0 } else { 2 },
                mime,
                src,
                fields,
                v["title"].as_str().map(str::to_owned),
            )
        }
        (ANTHROPIC, "thinking") => {
            return Some(Content::Reasoning {
                text: v["thinking"].as_str()?.into(),
                native: native_fields(format, remainder(v, &["type", "thinking"])),
            });
        }
        _ => return None,
    };
    let mut media = Media::new(mime, source);
    media.name = name;
    media.native = native_fields(format, std::mem::take(&mut fields));
    Some(match kind {
        0 => Content::Image(media),
        1 => Content::Audio(media),
        _ => Content::Document(media),
    })
}
fn merge(base: &mut Value, fields: Value) {
    if let (Some(base), Value::Object(fields)) = (base.as_object_mut(), fields) {
        for (key, value) in fields {
            match base.get_mut(&key) {
                Some(current) if current.is_object() && value.is_object() => merge(current, value),
                Some(_) => {} // Semantic fields take precedence over native edits.
                None => {
                    base.insert(key, value);
                }
            }
        }
    }
}
/// Encode a typed media part, refusing unsupported source representations.
pub(crate) fn write(format: &str, content: &Content) -> Result<Value, Fault> {
    if let Content::Reasoning { text, native } = content {
        if format != ANTHROPIC {
            return Err(untranslatable("reasoning outside an assistant message"));
        }
        let mut v = json!({"type":"thinking", "thinking":text});
        let fields = kept_fields(
            format,
            native,
            |k| format!("reasoning field '{k}'"),
            Keep::default(),
        )?;
        merge(&mut v, fields.into());
        return Ok(v);
    }
    let (kind, media) = match content {
        Content::Image(m) => ("image", m),
        Content::Audio(m) => ("audio", m),
        Content::Document(m) => ("document", m),
        _ => return Err(untranslatable("unsupported media content")),
    };
    if media.name.is_some() && kind != "document" {
        return Err(untranslatable("a display name on image or audio content"));
    }
    let native = kept_fields(
        format,
        &media.native,
        |k| format!("media field '{k}'"),
        Keep::default(),
    )?;
    let url = || match &media.source {
        MediaSource::Url(url) => url.clone(),
        MediaSource::Bytes(data) => {
            format!("data:{};base64,{}", media.mime_type, STANDARD.encode(data))
        }
    };
    let mut v = match (format, kind) {
        (OPENAI, "image") => json!({"type":"image_url","image_url":{"url":url()}}),
        (OPENAI, "document") => {
            if matches!(media.source, MediaSource::Url(_)) {
                return Err(untranslatable("a document URL in OpenAI Chat"));
            }
            let mut v = json!({"type":"file","file":{"file_data":url()}});
            if let Some(name) = &media.name {
                v["file"]["filename"] = name.clone().into();
            }
            v
        }
        (OPENAI, "audio") => {
            let MediaSource::Bytes(bytes) = &media.source else {
                return Err(untranslatable("an audio URL in OpenAI Chat"));
            };
            let encoding = match media.mime_type.as_str() {
                "audio/wav" | "audio/x-wav" => "wav",
                "audio/mpeg" => "mp3",
                _ => return Err(untranslatable("this audio MIME type")),
            };
            json!({"type":"input_audio","input_audio":{"data":STANDARD.encode(bytes),"format":encoding}})
        }
        (ANTHROPIC, "image" | "document") => {
            let source = match &media.source {
                MediaSource::Url(url) => json!({"type":"url","url":url}),
                MediaSource::Bytes(bytes) => {
                    json!({"type":"base64","media_type":media.mime_type,"data":STANDARD.encode(bytes)})
                }
            };
            let mut v = json!({"type":kind,"source":source});
            if let Some(name) = &media.name {
                if kind != "document" {
                    return Err(untranslatable("an image display name in Anthropic"));
                }
                v["title"] = name.clone().into();
            }
            v
        }
        _ => return Err(untranslatable("this media kind")),
    };
    merge(&mut v, native.into());
    Ok(v)
}
