//! ElevenLabs (<https://elevenlabs.io>): speech, voices, transcripts and
//! sound effects, from its API at `https://api.elevenlabs.io`
//! (`POST /v1/text-to-speech/{voice_id}`, `GET /v2/voices`,
//! `POST /v1/speech-to-text` as multipart, `POST /v1/sound-generation`), the
//! key in `xi-api-key`. The base URL is the host, not `/v1`, since voices
//! are under `/v2`; a residency host (`api.eu.residency.elevenlabs.io`)
//! takes its place.

use async_trait::async_trait;
use respan_gateway_tool_vendor_kit::args::{
    object, optional_bool, optional_enum, optional_int, optional_str, required_str,
};
use respan_gateway_tool_vendor_kit::reqwest::{Method, multipart};
use respan_gateway_tool_vendor_kit::{
    Api, Content, KeyIn, ToolError, ToolOutput, ToolSpec, ToolVendor, Tools,
};
use serde_json::{Map, Value};

/// ElevenLabs.
pub const VENDOR: ToolVendor = ToolVendor {
    id: "elevenlabs",
    key_env: &["ELEVENLABS_API_KEY"],
    key_in: KeyIn::Header("xi-api-key"),
    base_url: "https://api.elevenlabs.io",
    tools: &ElevenLabsTools,
};

/// ElevenLabs's tools: `text_to_speech`, `search_voices`, `speech_to_text`
/// and `sound_effect`.
#[derive(Debug, Clone, Copy)]
pub struct ElevenLabsTools;

/// The output formats offered: those a client can play as they are (no raw
/// PCM, no telephony codecs), the compact one first and the default.
const FORMATS: &[&str] = &[
    "mp3_22050_32",
    "mp3_44100_64",
    "mp3_44100_128",
    "opus_48000_32",
    "opus_48000_64",
    "wav_16000",
    "wav_24000",
];

/// The most text one speech call takes (Flash v2.5's limit).
const MAX_TEXT: usize = 40_000;

/// The most transcript a result carries.
const MAX_TRANSCRIPT: usize = 50_000;

const TOOLS: &[ToolSpec] = &[
    ToolSpec {
        name: "text_to_speech",
        description: "Speak text in an ElevenLabs voice and get the audio (MP3 by default), \
            plus a line saying what it is. Billed to the ElevenLabs account in credits per \
            character of `text`; Flash models cost about half as much per character. Find a \
            `voice_id` with `search_voices`.",
        input_schema: r#"{
  "type": "object",
  "properties": {
    "text": {"type": "string", "description": "The text to speak. eleven_v3 takes up to 5,000 characters, eleven_multilingual_v2 10,000, eleven_flash_v2_5 40,000."},
    "voice_id": {"type": "string", "description": "The voice, e.g. \"JBFqnCBsd6RMkjVDRZzb\", from search_voices."},
    "model_id": {"type": "string", "default": "eleven_multilingual_v2", "description": "eleven_multilingual_v2 (lifelike, 29 languages), eleven_flash_v2_5 (fast, cheaper), eleven_v3 (most expressive), or another text-to-speech model."},
    "language_code": {"type": "string", "description": "ISO 639-1 language to enforce, e.g. \"de\". Not for multilingual_v2."},
    "output_format": {"type": "string", "enum": ["mp3_22050_32", "mp3_44100_64", "mp3_44100_128", "opus_48000_32", "opus_48000_64", "wav_16000", "wav_24000"], "default": "mp3_22050_32", "description": "codec_samplerate_bitrate; the default is the smallest."},
    "stability": {"type": "number", "minimum": 0, "maximum": 1, "description": "Higher is steadier, lower more emotive. The voice's own setting if unset."},
    "similarity_boost": {"type": "number", "minimum": 0, "maximum": 1, "description": "How closely to keep to the original voice."},
    "style": {"type": "number", "minimum": 0, "maximum": 1, "description": "Style exaggeration; 0 is none and fastest."},
    "speed": {"type": "number", "minimum": 0.7, "maximum": 1.2, "description": "1 is normal speed."},
    "seed": {"type": "integer", "minimum": 0, "maximum": 4294967295, "description": "Repeat a result, best effort."}
  },
  "required": ["text", "voice_id"]
}"#,
    },
    ToolSpec {
        name: "search_voices",
        description: "Search the ElevenLabs account's voices (its own, the premade ones and \
            those saved from the library) and get each one's voice_id, name, category, labels \
            (gender, age, accent, use case), description and preview URL. Free.",
        input_schema: r#"{
  "type": "object",
  "properties": {
    "search": {"type": "string", "description": "Words to match in a voice's name, description, labels or category."},
    "category": {"type": "string", "enum": ["premade", "cloned", "generated", "professional"], "description": "Only voices of this kind."},
    "voice_type": {"type": "string", "enum": ["personal", "community", "default", "workspace", "non-default", "saved"], "description": "Only voices of this origin."},
    "page_size": {"type": "integer", "minimum": 1, "maximum": 100, "default": 10, "description": "At most this many voices."},
    "next_page_token": {"type": "string", "description": "The token a previous call returned, for its next page."}
  }
}"#,
    },
    ToolSpec {
        name: "speech_to_text",
        description: "Transcribe audio or video at a URL (a file over HTTPS, or a YouTube or \
            TikTok page) with ElevenLabs Scribe, and get the text, its language and, with \
            `diarize`, who said what when. Billed to the ElevenLabs account per second of \
            audio.",
        input_schema: r#"{
  "type": "object",
  "properties": {
    "url": {"type": "string", "description": "The audio or video's http(s) URL; a pre-signed storage URL works."},
    "model_id": {"type": "string", "enum": ["scribe_v2", "scribe_v1"], "default": "scribe_v2", "description": "The transcription model."},
    "language_code": {"type": "string", "description": "ISO 639-1 or 639-3 language, e.g. \"en\"; detected if unset."},
    "diarize": {"type": "boolean", "default": false, "description": "Tell speakers apart, and give the transcript as timed lines by speaker."},
    "num_speakers": {"type": "integer", "minimum": 1, "maximum": 32, "description": "At most this many speakers, to help diarization."},
    "tag_audio_events": {"type": "boolean", "default": true, "description": "Mark sounds such as (laughter) in the text."}
  },
  "required": ["url"]
}"#,
    },
    ToolSpec {
        name: "sound_effect",
        description: "Generate a sound effect from a description with ElevenLabs and get the \
            audio (MP3 by default), plus a line saying what it is. Billed to the ElevenLabs \
            account in credits, more for a longer `duration_seconds`.",
        input_schema: r#"{
  "type": "object",
  "properties": {
    "text": {"type": "string", "description": "The sound, e.g. \"rain on a tin roof, distant thunder\"."},
    "duration_seconds": {"type": "number", "minimum": 0.5, "maximum": 30, "description": "Its length; chosen from the description if unset."},
    "prompt_influence": {"type": "number", "minimum": 0, "maximum": 1, "default": 0.3, "description": "Higher keeps closer to the description, with less variety."},
    "loop": {"type": "boolean", "default": false, "description": "Make it loop seamlessly."},
    "output_format": {"type": "string", "enum": ["mp3_22050_32", "mp3_44100_64", "mp3_44100_128", "opus_48000_32", "opus_48000_64"], "default": "mp3_22050_32", "description": "codec_samplerate_bitrate; the default is the smallest."}
  },
  "required": ["text"]
}"#,
    },
];

#[async_trait]
impl Tools for ElevenLabsTools {
    fn list(&self) -> &'static [ToolSpec] {
        TOOLS
    }

    async fn call(&self, api: &Api, name: &str, arguments: Value) -> Result<ToolOutput, ToolError> {
        let args = object(&arguments)?;
        match name {
            "text_to_speech" => {
                let (voice, format, body) = speech_body(args)?;
                let req = api
                    .request(Method::POST, &format!("v1/text-to-speech/{voice}"))
                    .query(&[("output_format", format)])
                    .json(&body);
                let (mime, data) = api.bytes(req).await?;
                let chars = text_len(&body);
                let what = format!("Speech of {chars} characters");
                // Billed per character of `text`
                // (<https://elevenlabs.io/pricing/api>); the `character-cost`
                // header says so too, but `Api::bytes` drops headers.
                Ok(audio(what, format, &mime, data).with_usage("characters", chars as f64))
            }
            "search_voices" => {
                let query = voices_query(args)?;
                Ok(render_voices(&api.get_json("v2/voices", &query).await?).into())
            }
            "speech_to_text" => {
                let mut form = multipart::Form::new();
                for (k, v) in transcribe_fields(args)? {
                    form = form.text(k, v);
                }
                let req = api
                    .request(Method::POST, "v1/speech-to-text")
                    .multipart(form);
                let response = api.json(req).await?;
                Ok(transcribed(render_transcript(&response), &response))
            }
            "sound_effect" => {
                let (format, body) = sound_body(args)?;
                let req = api
                    .request(Method::POST, "v1/sound-generation")
                    .query(&[("output_format", format)])
                    .json(&body);
                let (mime, data) = api.bytes(req).await?;
                let out = audio(
                    format!(
                        "A sound effect of {:?}",
                        body["text"].as_str().unwrap_or_default()
                    ),
                    format,
                    &mime,
                    data,
                );
                // Billed per second generated (<https://elevenlabs.io/pricing/api>):
                // `duration_seconds` when given; unknown when it is chosen.
                Ok(match body["duration_seconds"].as_f64() {
                    Some(secs) => out.with_usage("seconds", secs),
                    None => out,
                })
            }
            _ => Err(ToolError::new(format!("no tool named {name:?}"))),
        }
    }
}

/// The number `key`, if there, within `min..=max`.
fn optional_num(
    args: &Map<String, Value>,
    key: &str,
    min: f64,
    max: f64,
) -> Result<Option<f64>, ToolError> {
    match args.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(v) => match v.as_f64() {
            Some(n) if (min..=max).contains(&n) => Ok(Some(n)),
            _ => Err(ToolError::new(format!(
                "`{key}` must be a number from {min} to {max}"
            ))),
        },
    }
}

/// A voice id, which goes into the path: letters and digits only.
fn voice_id(args: &Map<String, Value>) -> Result<&str, ToolError> {
    let id = required_str(args, "voice_id")?.trim();
    if id.bytes().all(|b| b.is_ascii_alphanumeric()) {
        Ok(id)
    } else {
        Err(ToolError::new(format!(
            "`voice_id` must be a voice's id, letters and digits, not {id:?}"
        )))
    }
}

/// The voice, output format and `POST /v1/text-to-speech/{voice_id}`'s body
/// from the tool's arguments.
fn speech_body(args: &Map<String, Value>) -> Result<(&str, &'static str, Value), ToolError> {
    let text = required_str(args, "text")?;
    if text.chars().count() > MAX_TEXT {
        return Err(ToolError::new(format!(
            "`text` must be at most {MAX_TEXT} characters; split it"
        )));
    }
    let voice = voice_id(args)?;
    let format = output_format(args, FORMATS)?;
    let mut body = Map::new();
    body.insert("text".into(), text.into());
    let model = optional_str(args, "model_id")?.unwrap_or("eleven_multilingual_v2");
    body.insert("model_id".into(), model.into());
    if let Some(v) = optional_str(args, "language_code")? {
        body.insert("language_code".into(), v.into());
    }
    let mut settings = Map::new();
    for key in ["stability", "similarity_boost", "style"] {
        if let Some(v) = optional_num(args, key, 0.0, 1.0)? {
            settings.insert(key.into(), v.into());
        }
    }
    if let Some(v) = optional_num(args, "speed", 0.7, 1.2)? {
        settings.insert("speed".into(), v.into());
    }
    if !settings.is_empty() {
        body.insert("voice_settings".into(), Value::Object(settings));
    }
    if let Some(v) = optional_int(args, "seed", 0, u64::from(u32::MAX))? {
        body.insert("seed".into(), v.into());
    }
    Ok((voice, format, Value::Object(body)))
}

/// `output_format`, one of `allowed`, the first of them by default.
fn output_format(
    args: &Map<String, Value>,
    allowed: &[&'static str],
) -> Result<&'static str, ToolError> {
    let format = optional_enum(args, "output_format", allowed)?.unwrap_or(allowed[0]);
    Ok(allowed
        .iter()
        .copied()
        .find(|f| *f == format)
        .unwrap_or(allowed[0]))
}

fn text_len(body: &Value) -> usize {
    body["text"].as_str().map_or(0, |t| t.chars().count())
}

/// `GET /v2/voices`'s query string from the tool's arguments.
fn voices_query(args: &Map<String, Value>) -> Result<Vec<(&'static str, String)>, ToolError> {
    let mut q = Vec::new();
    if let Some(v) = optional_str(args, "search")? {
        q.push(("search", v.to_owned()));
    }
    let categories = ["premade", "cloned", "generated", "professional"];
    if let Some(v) = optional_enum(args, "category", &categories)? {
        q.push(("category", v.to_owned()));
    }
    let types = [
        "personal",
        "community",
        "default",
        "workspace",
        "non-default",
        "saved",
    ];
    if let Some(v) = optional_enum(args, "voice_type", &types)? {
        q.push(("voice_type", v.to_owned()));
    }
    let size = optional_int(args, "page_size", 1, 100)?.unwrap_or(10);
    q.push(("page_size", size.to_string()));
    if let Some(v) = optional_str(args, "next_page_token")? {
        q.push(("next_page_token", v.to_owned()));
    }
    Ok(q)
}

/// `POST /v1/speech-to-text`'s form fields from the tool's arguments.
fn transcribe_fields(args: &Map<String, Value>) -> Result<Vec<(&'static str, String)>, ToolError> {
    let url = required_str(args, "url")?;
    if !(url.starts_with("http://") || url.starts_with("https://")) {
        return Err(ToolError::new("`url` must be an http(s) URL"));
    }
    let model =
        optional_enum(args, "model_id", &["scribe_v2", "scribe_v1"])?.unwrap_or("scribe_v2");
    let mut f = vec![
        ("model_id", model.to_owned()),
        ("source_url", url.to_owned()),
    ];
    if let Some(v) = optional_str(args, "language_code")? {
        f.push(("language_code", v.to_owned()));
    }
    if let Some(v) = optional_bool(args, "diarize")? {
        f.push(("diarize", v.to_string()));
    }
    if let Some(v) = optional_int(args, "num_speakers", 1, 32)? {
        f.push(("num_speakers", v.to_string()));
    }
    if let Some(v) = optional_bool(args, "tag_audio_events")? {
        f.push(("tag_audio_events", v.to_string()));
    }
    Ok(f)
}

/// The output format and `POST /v1/sound-generation`'s body from the
/// tool's arguments.
fn sound_body(args: &Map<String, Value>) -> Result<(&'static str, Value), ToolError> {
    let mut body = Map::new();
    body.insert("text".into(), required_str(args, "text")?.into());
    if let Some(v) = optional_num(args, "duration_seconds", 0.5, 30.0)? {
        body.insert("duration_seconds".into(), v.into());
    }
    if let Some(v) = optional_num(args, "prompt_influence", 0.0, 1.0)? {
        body.insert("prompt_influence".into(), v.into());
    }
    if let Some(v) = optional_bool(args, "loop")? {
        body.insert("loop".into(), v.into());
    }
    let format = output_format(args, &FORMATS[..5])?;
    Ok((format, Value::Object(body)))
}

/// The media type of audio in `format`, for an answer that names none
/// better than `application/octet-stream`.
fn mime_of(format: &str, answered: &str) -> String {
    let answered = answered.split(';').next().unwrap_or_default().trim();
    if answered.starts_with("audio/") {
        return answered.to_owned();
    }
    match format.split('_').next() {
        Some("opus") => "audio/ogg",
        Some("wav") => "audio/wav",
        _ => "audio/mpeg",
    }
    .into()
}

/// Audio, after a line saying what it is.
fn audio(what: String, format: &str, answered: &str, data: Vec<u8>) -> ToolOutput {
    let mime_type = mime_of(format, answered);
    let kb = data.len().div_ceil(1024);
    ToolOutput::from(vec![
        Content::Text(format!("{what}, as {format} ({mime_type}, {kb} KB).")),
        Content::Audio { data, mime_type },
    ])
}

fn str_of<'a>(v: &'a Value, key: &str) -> Option<&'a str> {
    v.get(key).and_then(Value::as_str).filter(|s| !s.is_empty())
}

fn array_of<'a>(v: &'a Value, key: &str) -> &'a [Value] {
    v.get(key)
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or_default()
}

/// Voices as JSON lines of what picks one: id, name, category, labels,
/// description and preview. Samples, settings and sharing are dropped.
fn render_voices(response: &Value) -> String {
    let voices = array_of(response, "voices");
    if voices.is_empty() {
        return "No voices.".into();
    }
    let mut out: Vec<String> = voices
        .iter()
        .map(|v| {
            let mut voice = Map::new();
            for key in ["voice_id", "name", "category", "description", "preview_url"] {
                if let Some(s) = str_of(v, key) {
                    voice.insert(key.into(), s.into());
                }
            }
            if let Some(labels) = v
                .get("labels")
                .and_then(Value::as_object)
                .filter(|l| !l.is_empty())
            {
                voice.insert("labels".into(), Value::Object(labels.clone()));
            }
            Value::Object(voice).to_string()
        })
        .collect();
    if let Some(n) = response.get("total_count").and_then(Value::as_u64) {
        out.push(format!("{} of {n} voices.", voices.len()));
    }
    if response.get("has_more").and_then(Value::as_bool) == Some(true)
        && let Some(token) = str_of(response, "next_page_token")
    {
        out.push(format!("More: next_page_token {token:?}"));
    }
    out.join("\n")
}

/// A transcript: its language, then the text, or timed lines by speaker
/// when the words name speakers. Capped at [`MAX_TRANSCRIPT`].
fn render_transcript(response: &Value) -> String {
    let chunks = match array_of(response, "transcripts") {
        [] => std::slice::from_ref(response),
        many => many,
    };
    let mut out = String::new();
    for (i, chunk) in chunks.iter().enumerate() {
        if chunks.len() > 1 {
            out.push_str(&format!("## Channel {i}\n"));
        }
        if let Some(lang) = str_of(chunk, "language_code") {
            let p = chunk
                .get("language_probability")
                .and_then(Value::as_f64)
                .map(|p| format!(" ({:.0}%)", p * 100.0))
                .unwrap_or_default();
            out.push_str(&format!("Language: {lang}{p}\n"));
        }
        if let Some(secs) = response.get("audio_duration_secs").and_then(Value::as_f64) {
            out.push_str(&format!("Duration: {secs:.1}s\n"));
        }
        out.push('\n');
        let words = array_of(chunk, "words");
        if words.iter().any(|w| str_of(w, "speaker_id").is_some()) {
            out.push_str(&by_speaker(words));
        } else {
            out.push_str(str_of(chunk, "text").unwrap_or("(no speech)").trim());
        }
        out.push('\n');
    }
    if let Some(id) = str_of(response, "transcription_id") {
        out.push_str(&format!("\nTranscription: {id}"));
    }
    let out = out.trim_end();
    if out.len() <= MAX_TRANSCRIPT {
        return out.to_owned();
    }
    let mut end = MAX_TRANSCRIPT;
    while !out.is_char_boundary(end) {
        end -= 1;
    }
    format!(
        "{}\n… (transcript cut at {MAX_TRANSCRIPT} bytes)",
        &out[..end]
    )
}

/// `text`, having used the seconds of audio the response's
/// `audio_duration_secs` says, which Scribe bills by
/// (<https://elevenlabs.io/docs/api-reference/speech-to-text/convert>).
fn transcribed(text: String, response: &Value) -> ToolOutput {
    let out = ToolOutput::from(text);
    match response.get("audio_duration_secs").and_then(Value::as_f64) {
        Some(secs) => out.with_usage("seconds", secs),
        None => out,
    }
}

/// Words as one line per turn: `[start] speaker: text`.
fn by_speaker(words: &[Value]) -> String {
    let mut lines: Vec<String> = Vec::new();
    let mut current: Option<&str> = None;
    for w in words {
        let text = str_of(w, "text").unwrap_or_default();
        let speaker = str_of(w, "speaker_id");
        if speaker.is_some() && speaker != current && !text.trim().is_empty() {
            current = speaker;
            let start = w.get("start").and_then(Value::as_f64).unwrap_or_default();
            lines.push(format!("[{start:.1}s] {}: ", speaker.unwrap_or("?")));
        }
        match lines.last_mut() {
            Some(line) => line.push_str(text),
            None => lines.push(text.to_owned()),
        }
    }
    lines
        .iter()
        .map(|l| l.trim_end())
        .collect::<Vec<_>>()
        .join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn args(v: Value) -> Map<String, Value> {
        match v {
            Value::Object(m) => m,
            _ => Map::new(),
        }
    }

    #[test]
    fn speech_body_maps_arguments() {
        let a = args(json!({
            "text": "Hello", "voice_id": "JBFqnCBsd6RMkjVDRZzb", "stability": 0.4,
            "speed": 1.1, "seed": 7, "output_format": "wav_16000"
        }));
        let (voice, format, body) = speech_body(&a).expect("valid");
        assert_eq!((voice, format), ("JBFqnCBsd6RMkjVDRZzb", "wav_16000"));
        assert_eq!(
            body,
            json!({
                "text": "Hello", "model_id": "eleven_multilingual_v2",
                "voice_settings": {"stability": 0.4, "speed": 1.1}, "seed": 7
            })
        );
        let (_, format, body) = speech_body(&args(
            json!({"text": "Hi", "voice_id": "abc", "model_id": "eleven_flash_v2_5"}),
        ))
        .expect("valid");
        assert_eq!(format, "mp3_22050_32");
        assert_eq!(body, json!({"text": "Hi", "model_id": "eleven_flash_v2_5"}));
        // Speech is billed by character, not byte.
        assert_eq!(text_len(&json!({"text": "héllo 👋"})), 7);
    }

    #[test]
    fn speech_body_rejects_bad_arguments() {
        assert!(speech_body(&args(json!({"voice_id": "abc"}))).is_err());
        assert!(speech_body(&args(json!({"text": "x"}))).is_err());
        assert!(speech_body(&args(json!({"text": "x", "voice_id": "../v1/user"}))).is_err());
        assert!(speech_body(&args(json!({"text": "x", "voice_id": "a", "stability": 2}))).is_err());
        assert!(
            speech_body(&args(
                json!({"text": "x", "voice_id": "a", "speed": "fast"})
            ))
            .is_err()
        );
        assert!(
            speech_body(&args(
                json!({"text": "x", "voice_id": "a", "output_format": "pcm_16000"})
            ))
            .is_err()
        );
        let long = "a".repeat(MAX_TEXT + 1);
        assert!(speech_body(&args(json!({"text": long, "voice_id": "a"}))).is_err());
    }

    #[test]
    fn voices_query_maps_and_validates() {
        assert_eq!(
            voices_query(&args(
                json!({"search": "calm", "category": "premade", "page_size": 5})
            ))
            .expect("valid"),
            vec![
                ("search", "calm".to_owned()),
                ("category", "premade".to_owned()),
                ("page_size", "5".to_owned())
            ]
        );
        assert!(voices_query(&args(json!({"category": "famous"}))).is_err());
        assert!(voices_query(&args(json!({"page_size": 500}))).is_err());
    }

    #[test]
    fn transcribe_fields_map_and_validate() {
        assert_eq!(
            transcribe_fields(&args(
                json!({"url": "https://a.b/x.mp3", "diarize": true, "num_speakers": 2})
            ))
            .expect("valid"),
            vec![
                ("model_id", "scribe_v2".to_owned()),
                ("source_url", "https://a.b/x.mp3".to_owned()),
                ("diarize", "true".to_owned()),
                ("num_speakers", "2".to_owned())
            ]
        );
        assert!(transcribe_fields(&args(json!({"url": "file:///etc/passwd"}))).is_err());
        assert!(transcribe_fields(&args(json!({}))).is_err());
        assert!(
            transcribe_fields(&args(json!({"url": "https://a", "model_id": "whisper"}))).is_err()
        );
    }

    #[test]
    fn sound_body_maps_and_validates() {
        assert_eq!(
            sound_body(&args(
                json!({"text": "rain", "duration_seconds": 3, "loop": true})
            ))
            .expect("valid"),
            (
                "mp3_22050_32",
                json!({"text": "rain", "duration_seconds": 3.0, "loop": true})
            )
        );
        assert!(sound_body(&args(json!({"text": "rain", "duration_seconds": 60}))).is_err());
        assert!(sound_body(&args(json!({"text": "rain", "output_format": "wav_16000"}))).is_err());
        assert!(sound_body(&args(json!({}))).is_err());
    }

    #[test]
    fn audio_is_a_line_and_the_bytes() {
        let out = audio(
            "Speech".into(),
            "mp3_22050_32",
            "application/octet-stream",
            vec![0; 2000],
        );
        assert_eq!(
            out,
            ToolOutput::from(vec![
                Content::Text("Speech, as mp3_22050_32 (audio/mpeg, 2 KB).".into()),
                Content::Audio {
                    data: vec![0; 2000],
                    mime_type: "audio/mpeg".into()
                },
            ])
        );
        assert_eq!(
            mime_of("wav_16000", "audio/x-wav; charset=binary"),
            "audio/x-wav"
        );
        assert_eq!(mime_of("opus_48000_32", ""), "audio/ogg");
    }

    #[test]
    fn renders_voices() {
        let text = render_voices(&json!({
            "voices": [{
                "voice_id": "JBFqnCBsd6RMkjVDRZzb", "name": "George", "category": "premade",
                "labels": {"accent": "british", "gender": "male"}, "description": "",
                "preview_url": "https://storage.googleapis.com/george.mp3",
                "samples": null, "settings": {"stability": 0.5}, "is_owner": false
            }],
            "has_more": true, "total_count": 30, "next_page_token": "tok"
        }));
        let mut lines = text.lines();
        let first: Value = serde_json::from_str(lines.next().expect("a voice")).expect("json");
        assert_eq!(
            first,
            json!({
                "voice_id": "JBFqnCBsd6RMkjVDRZzb", "name": "George", "category": "premade",
                "labels": {"accent": "british", "gender": "male"},
                "preview_url": "https://storage.googleapis.com/george.mp3"
            })
        );
        assert_eq!(
            lines.collect::<Vec<_>>(),
            ["1 of 30 voices.", "More: next_page_token \"tok\""]
        );
        assert_eq!(render_voices(&json!({"voices": []})), "No voices.");
    }

    #[test]
    fn renders_transcripts() {
        let plain = render_transcript(&json!({
            "language_code": "en", "language_probability": 0.98, "text": " Hello there. ",
            "words": [{"text": "Hello", "type": "word", "start": 0.1, "end": 0.4}]
        }));
        assert_eq!(plain, "Language: en (98%)\n\nHello there.");
        let diarized = render_transcript(&json!({
            "language_code": "en", "text": "Hi. Hey.", "transcription_id": "t1",
            "words": [
                {"text": "Hi.", "type": "word", "start": 0.0, "speaker_id": "speaker_0"},
                {"text": " ", "type": "spacing", "start": 0.5, "speaker_id": "speaker_0"},
                {"text": "Hey.", "type": "word", "start": 1.3, "speaker_id": "speaker_1"}
            ]
        }));
        assert_eq!(
            diarized,
            "Language: en\n\n[0.0s] speaker_0: Hi.\n[1.3s] speaker_1: Hey.\n\nTranscription: t1"
        );
    }

    #[test]
    fn transcripts_use_their_seconds() {
        let response = json!({"text": "Hi.", "audio_duration_secs": 12.5});
        let out = transcribed(render_transcript(&response), &response);
        assert_eq!(out.usage.get("seconds"), Some(&12.5));
        let unknown = transcribed("x".into(), &json!({"text": "x"}));
        assert!(unknown.usage.is_empty());
    }
}
