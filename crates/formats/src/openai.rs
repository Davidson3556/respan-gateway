//! OpenAI Chat Completions: `POST /v1/chat/completions`, JSON or SSE.

use bytes::{Bytes, BytesMut};
use respan_gateway_core::{
    Decoder, Delta, DeltaKind, Doc, Encoder, End, Envelope, Extensions, Fault, Format, Part,
    ResponseHead, Usage, units,
};
use serde_json::{Value, json};

#[derive(Debug, Clone, Copy, Default)]
pub struct OpenAiChat;

impl Format for OpenAiChat {
    fn name(&self) -> &'static str {
        "openai-chat"
    }

    fn envelope(&self, body: &Doc, _ext: &Extensions) -> Envelope {
        let v = body.json();
        Envelope {
            model: v
                .and_then(|v| v.get("model"))
                .and_then(Value::as_str)
                .map(str::to_owned),
            stream: v
                .and_then(|v| v.get("stream"))
                .and_then(Value::as_bool)
                .unwrap_or(false),
        }
    }

    fn provider_path(&self, _envelope: &Envelope) -> String {
        "/v1/chat/completions".into()
    }

    fn prepare_request(
        &self,
        body: &mut Doc,
        model: Option<&str>,
        stream: bool,
    ) -> Result<(), Fault> {
        let wants_usage = body
            .json()
            .and_then(|v| v.pointer("/stream_options/include_usage"))
            .is_some();
        if model.is_none() && (!stream || wants_usage) {
            return Ok(());
        }
        let v = body
            .json_mut()
            .and_then(Value::as_object_mut)
            .ok_or_else(|| Fault::bad_request("request body is not a JSON object"))?;
        if let Some(m) = model {
            v.insert("model".into(), m.into());
        }
        if stream && !wants_usage {
            let opts = v.entry("stream_options").or_insert_with(|| json!({}));
            if let Some(o) = opts.as_object_mut() {
                o.insert("include_usage".into(), true.into());
            }
        }
        Ok(())
    }

    fn decoder(&self, head: &ResponseHead) -> Box<dyn Decoder> {
        if is_sse(head) {
            Box::<SseDecoder>::default()
        } else {
            Box::<UnaryDecoder>::default()
        }
    }

    fn encoder(&self, head: &ResponseHead) -> Box<dyn Encoder> {
        if is_sse(head) {
            Box::new(SseEncoder)
        } else {
            Box::new(UnaryEncoder)
        }
    }

    fn fault(&self, fault: &Fault, in_stream: bool) -> Bytes {
        let body = json!({
            "error": { "message": fault.message, "type": fault.code(), "code": fault.code() }
        });
        if in_stream {
            Bytes::from(format!("data: {body}\n\n"))
        } else {
            Bytes::from(body.to_string())
        }
    }
}

fn is_sse(head: &ResponseHead) -> bool {
    head.content_type()
        .is_some_and(|c| c.starts_with("text/event-stream"))
}

fn usage_of(v: &Value) -> Option<Usage> {
    let u = v.get("usage").filter(|u| u.is_object())?;
    let mut usage = Usage::default();
    let mut add = |unit: &str, ptr: &str| {
        if let Some(n) = u.pointer(ptr).and_then(Value::as_f64) {
            usage.add(unit, n);
        }
    };
    add(units::INPUT_TOKENS, "/prompt_tokens");
    add(units::OUTPUT_TOKENS, "/completion_tokens");
    add(
        units::CACHED_INPUT_TOKENS,
        "/prompt_tokens_details/cached_tokens",
    );
    add(
        units::REASONING_TOKENS,
        "/completion_tokens_details/reasoning_tokens",
    );
    Some(usage)
}

#[derive(Debug, Default)]
struct SseDecoder {
    buf: BytesMut,
    saw_end: bool,
}

impl SseDecoder {
    fn event(&mut self, raw: Bytes, out: &mut Vec<Part>) {
        let text = String::from_utf8_lossy(&raw);
        let data: Vec<&str> = text
            .lines()
            .filter_map(|l| l.strip_prefix("data:"))
            .map(|d| d.strip_prefix(' ').unwrap_or(d))
            .collect();
        if data.is_empty() {
            out.push(Part::Frame(raw));
            return;
        }
        let data = data.join("\n");
        if data.trim() == "[DONE]" {
            self.saw_end = true;
            out.push(Part::End(End::Done));
            return;
        }
        let Ok(v) = serde_json::from_str::<Value>(&data) else {
            out.push(Part::Frame(raw));
            return;
        };
        let usage = usage_of(&v);
        if let Some(err) = v.get("error") {
            let msg = err
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("provider error");
            out.push(Part::Frame(raw));
            self.saw_end = true;
            out.push(Part::End(End::Failed(Fault::upstream(msg))));
            return;
        }
        match delta_of(&v) {
            Some((block, kind, text)) => out.push(Part::Delta(Delta::decoded(
                block,
                kind,
                text,
                raw,
                v.clone(),
            ))),
            None => out.push(Part::Frame(raw)),
        }
        if let Some(u) = usage {
            out.push(Part::Usage(u));
        }
    }
}

/// The single piece of content in a chunk, if it has exactly one: text as
/// block 0, tool call `i` arguments as block `i + 1`.
fn delta_of(v: &Value) -> Option<(u32, DeltaKind, String)> {
    let choices = v.get("choices")?.as_array()?;
    let [choice] = choices.as_slice() else {
        return None;
    };
    let delta = choice.get("delta")?;
    if let Some(s) = delta.get("content").and_then(Value::as_str) {
        return Some((0, DeltaKind::Text, s.to_owned()));
    }
    let calls = delta.get("tool_calls")?.as_array()?;
    let [call] = calls.as_slice() else {
        return None;
    };
    let index = call.get("index").and_then(Value::as_u64).unwrap_or(0);
    let args = call.pointer("/function/arguments")?.as_str()?;
    Some((
        u32::try_from(index + 1).ok()?,
        DeltaKind::Args,
        args.to_owned(),
    ))
}

fn next_event(buf: &mut BytesMut) -> Option<Bytes> {
    let pos = buf
        .windows(2)
        .position(|w| w == b"\n\n")
        .map(|p| p + 2)
        .or_else(|| buf.windows(4).position(|w| w == b"\r\n\r\n").map(|p| p + 4))?;
    Some(buf.split_to(pos).freeze())
}

impl Decoder for SseDecoder {
    fn decode(&mut self, bytes: Bytes, out: &mut Vec<Part>) {
        self.buf.extend_from_slice(&bytes);
        while let Some(event) = next_event(&mut self.buf) {
            self.event(event, out);
        }
    }

    fn finish(&mut self, out: &mut Vec<Part>) {
        if !self.buf.is_empty() {
            let rest = self.buf.split().freeze();
            self.event(rest, out);
        }
        if !self.saw_end {
            out.push(Part::End(End::Failed(Fault::truncated())));
        }
    }
}

#[derive(Debug, Default)]
struct UnaryDecoder {
    buf: BytesMut,
}

impl Decoder for UnaryDecoder {
    fn decode(&mut self, bytes: Bytes, _out: &mut Vec<Part>) {
        self.buf.extend_from_slice(&bytes);
    }

    fn finish(&mut self, out: &mut Vec<Part>) {
        let doc = Doc::new(self.buf.split().freeze());
        let usage = doc.json().and_then(usage_of);
        let parsed = doc.json().is_some();
        out.push(Part::Doc(doc));
        if let Some(u) = usage {
            out.push(Part::Usage(u));
        }
        out.push(Part::End(if parsed {
            End::Done
        } else {
            End::Failed(Fault::upstream("response body is not JSON"))
        }));
    }
}

#[derive(Debug)]
struct SseEncoder;

impl Encoder for SseEncoder {
    fn encode(&mut self, part: &Part, out: &mut BytesMut) {
        match part {
            Part::Frame(b) => out.extend_from_slice(b),
            Part::Delta(d) => match d.raw() {
                Some(raw) => out.extend_from_slice(raw),
                None => {
                    let chunk = rebuild(d);
                    out.extend_from_slice(format!("data: {chunk}\n\n").as_bytes());
                }
            },
            Part::End(End::Done) => out.extend_from_slice(b"data: [DONE]\n\n"),
            Part::End(End::Failed(f)) => {
                out.extend_from_slice(&OpenAiChat.fault(f, true));
            }
            Part::Head(_) | Part::Doc(_) | Part::Usage(_) => {}
        }
    }
}

/// Re-render an edited delta in the shape of the chunk it came from.
fn rebuild(d: &Delta) -> Value {
    let mut chunk = d.frame().cloned().unwrap_or_else(
        || json!({ "object": "chat.completion.chunk", "choices": [{ "index": 0, "delta": {} }] }),
    );
    let delta = chunk.pointer_mut("/choices/0/delta");
    if let Some(delta) = delta {
        match d.kind() {
            DeltaKind::Args => {
                if let Some(a) = delta.pointer_mut("/tool_calls/0/function/arguments") {
                    *a = d.text().into();
                }
            }
            _ => delta["content"] = d.text().into(),
        }
    }
    chunk
}

#[derive(Debug)]
struct UnaryEncoder;

impl Encoder for UnaryEncoder {
    fn encode(&mut self, part: &Part, out: &mut BytesMut) {
        if let Part::Doc(doc) = part {
            out.extend_from_slice(&doc.to_bytes());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decode_all(chunks: &[&str]) -> Vec<Part> {
        let mut d = SseDecoder::default();
        let mut out = Vec::new();
        for c in chunks {
            d.decode(Bytes::from(c.to_string()), &mut out);
        }
        d.finish(&mut out);
        out
    }

    #[test]
    fn events_split_across_reads_decode_once() {
        let out = decode_all(&[
            "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"He",
            "llo\"}}]}\n\ndata: {\"choices\":[],\"usage\":{\"prompt_tokens\":3,\"completion_tokens\":1}}\n\n",
            "data: [DONE]\n\n",
        ]);
        let text: Vec<_> = out
            .iter()
            .filter_map(|p| match p {
                Part::Delta(d) => Some(d.text().to_owned()),
                _ => None,
            })
            .collect();
        assert_eq!(text, vec!["Hello"]);
        let usage = out.iter().find_map(|p| match p {
            Part::Usage(u) => Some(u.clone()),
            _ => None,
        });
        assert_eq!(usage.map(|u| u.get(units::INPUT_TOKENS)), Some(3.0));
        assert!(matches!(out.last(), Some(Part::End(End::Done))));
    }

    #[test]
    fn missing_done_is_truncated() {
        let out =
            decode_all(&["data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"a\"}}]}\n\n"]);
        assert!(matches!(
            out.last(),
            Some(Part::End(End::Failed(f))) if f.kind == respan_gateway_core::FaultKind::Truncated
        ));
    }

    #[test]
    fn edited_delta_keeps_its_chunk_shape() {
        let out = decode_all(&[
            "data: {\"id\":\"x\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi\"}}]}\n\n",
            "data: [DONE]\n\n",
        ]);
        let Some(Part::Delta(d)) = out.into_iter().next() else {
            panic!("expected a delta");
        };
        let mut d = d;
        *d.text_mut() = "HI".into();
        let mut buf = BytesMut::new();
        SseEncoder.encode(&Part::Delta(d), &mut buf);
        assert_eq!(
            &buf[..],
            b"data: {\"id\":\"x\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"HI\"}}]}\n\n"
        );
    }

    #[test]
    fn streaming_requests_ask_for_usage() {
        let mut doc = Doc::new(Bytes::from_static(b"{\"model\":\"a\",\"stream\":true}"));
        OpenAiChat.prepare_request(&mut doc, None, true).ok();
        assert_eq!(
            &doc.to_bytes()[..],
            b"{\"model\":\"a\",\"stream\":true,\"stream_options\":{\"include_usage\":true}}"
        );
    }
}
