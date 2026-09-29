//! A user-defined kind with two formats, unknown to the kernel.

mod common;
use bytes::{Bytes, BytesMut};
use common::{AddProvider, add_provider, providers, with_plugins};
use http::header::CONTENT_TYPE;
use http::{HeaderValue, StatusCode};
use respan_gateway_core::async_trait;
use respan_gateway_core::serde_json;
use respan_gateway_core::{
    ClientContext, Decoder, Encoder, Fault, Filter, Format, Gateway, Plugin, Response, codes,
};
use respan_gateway_core::{bytes, futures};
use respan_gateway_formats::opaque::{self, Got, Sent};
use respan_gateway_formats::relayed::Relayed;
use respan_gateway_formats::whole::Relay;
use respan_gateway_formats::{self as formats, default_content_type, sse};
use respan_gateway_http::Asked;
use respan_gateway_http::http;
use respan_gateway_http::{Answer as Head, Ask};
use respan_gateway_testkit::{MockTransport, Outcomes, mock, post, read_whole, serve};
use serde_json::{Value, json};
use std::sync::Arc;

/// A `POST` of `body` to `/{format}`, where that format's clients send.
fn to(format: &str, body: impl Into<Bytes>) -> http::Request<respan_gateway_http::Body> {
    post(&format!("/{format}"), body)
}

/// A gateway serving `format`: `providers`, `plugins` in order, `target`.
fn serving(
    format: impl Format,
    list: impl IntoIterator<Item = AddProvider>,
    plugins: Vec<Arc<dyn Plugin>>,
    target: Option<&str>,
) -> Result<Gateway, Fault> {
    let gw = with_plugins(providers(list)?.serve(format)?, plugins)?;
    match target {
        Some(t) => gw.target(t),
        None => Ok(gw),
    }
}

/// The jev kind's request: a prompt in, an answer out.
#[derive(Clone, PartialEq)]
struct Jev {
    prompt: String,
    model: Option<String>,
    /// Fields of the body jev has no place for, by name.
    unknown: Vec<String>,
    /// The client's body it was decoded from.
    origin: Relayed<Jev>,
}

impl Jev {
    /// Decoded by `format` from `raw`.
    fn came(mut self, format: &Arc<str>, raw: Bytes) -> Self {
        let said = Jev {
            origin: Relayed::default(),
            ..self.clone()
        };
        self.origin = Relayed::came(format, raw, said);
        self
    }
}

/// A jev response message: an answer, or, with no text, one that says
/// nothing, such as a keep-alive comment.
#[derive(Clone, PartialEq, Default)]
struct Answer {
    text: Option<String>,
    /// The provider's message it was decoded from.
    origin: Relayed<Answer>,
}

impl Answer {
    fn new(text: impl Into<String>) -> Self {
        Answer {
            text: Some(text.into()),
            origin: Relayed::default(),
        }
    }

    /// Decoded by `format` from `raw`.
    fn came(mut self, format: &Arc<str>, raw: Bytes) -> Self {
        let said = self.clone();
        self.origin = Relayed::came(format, raw, said);
        self
    }
}

/// Two dialects of jev, which name the prompt and the answer differently:
/// v1 says `input`/`output`, v2 says `prompt`/`text`.
#[derive(Clone, Copy)]
struct JevFormat {
    name: &'static str,
    prompt: &'static str,
    answer: &'static str,
}

const V1: JevFormat = JevFormat {
    name: "jev-v1",
    prompt: "input",
    answer: "output",
};

const V2: JevFormat = JevFormat {
    name: "jev-v2",
    prompt: "prompt",
    answer: "text",
};

impl Format for JevFormat {
    type Ask = Ask;
    type Answer = Head;
    type Input = Jev;
    type Output = Answer;

    fn name(&self) -> &str {
        self.name
    }

    fn accepts(&self, head: &Ask) -> bool {
        formats::posts_to(head, &format!("/{}", Format::name(self)))
    }

    fn request_decoder(&self, _request: &Ask) -> Box<dyn Decoder<Jev>> {
        let (jev, name) = (*self, Arc::from(self.name));
        formats::message_decoder(move |raw: Bytes| Ok(jev.read(&raw)?.came(&name, raw)))
    }

    fn request_encoder(
        &self,
        client: Option<&Ask>,
        _body: &mut Bytes,
        _streams: bool,
    ) -> Box<dyn Encoder<Jev, Ask>> {
        let mut head = formats::call_head(client);
        head.method = http::Method::POST;
        formats::set_path(&mut head, "/complete");
        let json = HeaderValue::from_static("application/json");
        head.headers.insert(CONTENT_TYPE, json);
        formats::with_head(head, Box::new(Prompts(*self)))
    }

    fn response_decoder(
        &self,
        head: &Head,
        _call: &Ask,
        _sent: &Bytes,
    ) -> Box<dyn Decoder<Answer>> {
        // A jev answer is whole, or a stream of one-answer SSE events.
        let (answer, name): (_, Arc<str>) = (self.answer, Arc::from(self.name));
        sse::or_whole(self.name, head, move |raw: Bytes, out: &mut Vec<Answer>| {
            // A keep-alive comment is no content.
            if raw.starts_with(b":") {
                out.push(Answer::default().came(&name, raw));
                return Ok(());
            }
            let v = formats::json(&raw);
            // An error inside a success is the provider's, relayed.
            if let Some(message) = v.as_ref().and_then(|v| v["error"]["message"].as_str()) {
                return Err(Fault::upstream(message).with_answer(&name, raw, false));
            }
            let text = v.as_ref().and_then(|v| v[answer].as_str());
            let text = text.ok_or_else(|| Fault::upstream("not a jev answer"))?;
            out.push(Answer::new(text).came(&name, raw));
            Ok(())
        })
    }

    fn response_encoder(
        &self,
        answer: Option<&Head>,
        _ask: &Ask,
        _body: &Bytes,
        refusal: Option<&Fault>,
    ) -> Box<dyn Encoder<Answer, Head>> {
        let mut head = formats::answer_head(answer);
        let body = self.response_body(&mut head, refusal);
        formats::with_head(head, body)
    }

    fn is_content(&self, answer: &Answer) -> bool {
        answer.text.is_some()
    }
}

impl JevFormat {
    fn response_body(
        &self,
        head: &mut Head,
        refusal: Option<&Fault>,
    ) -> Box<dyn Encoder<Answer, ()>> {
        // A refusal is the provider's answer as it came, else jev's error
        // under the fault's status.
        if let Some(fault) = refusal
            && let Some(body) = respan_gateway_http::answered(self.name, fault, head)
        {
            return formats::whole::written(body);
        }
        default_content_type(head, "application/json");
        // Answers are written as they came, edits in their shape; others and
        // failures in jev: in a stream, as an event shaped like the original.
        let (key, name, stream) = (self.answer, self.name, sse::is_stream(head));
        let frame = move |like: Option<&Bytes>, v: Value| -> Bytes {
            match stream {
                true => sse::event_as(like, None, v).into(),
                false => v.to_string().into(),
            }
        };
        let write = move |answers: &[Answer]| {
            if let [one] = answers
                && let Some(bytes) = one.origin.bytes(name, one)
            {
                return Ok(bytes.clone());
            }
            let texts: Vec<&str> = answers.iter().filter_map(|a| a.text.as_deref()).collect();
            if texts.is_empty() && answers.iter().all(|a| a.text.is_none()) {
                // What is no content, from another format, writes nothing.
                return Ok(Bytes::new());
            }
            let like = answers.iter().find_map(|a| a.origin.origin(name));
            let like = like.map(|o| &o.bytes);
            Ok(frame(like, json!({ key: texts.concat() })))
        };
        let fail = move |f: &Fault| match f.answer::<Bytes>(name) {
            Some(raw) => raw.clone(),
            None => frame(None, json!({ "error": f.message() })),
        };
        Box::new(Relay::new(write, fail, stream))
    }

    /// A body with no prompt is not jev at all; other fields are noted.
    fn read(&self, raw: &[u8]) -> Result<Jev, Fault> {
        let v: Option<Value> = serde_json::from_slice(raw).ok();
        let v = v.as_ref().and_then(Value::as_object);
        let prompt = v
            .and_then(|v| v.get(self.prompt)?.as_str())
            .ok_or_else(|| Fault::bad_request("no prompt"))?;
        let known = [self.prompt, "model"];
        let unknown = v.into_iter().flatten().map(|(k, _)| k);
        Ok(Jev {
            prompt: prompt.to_owned(),
            model: v.and_then(|v| v.get("model")?.as_str()).map(Into::into),
            unknown: unknown
                .filter(|k| !known.contains(&k.as_str()))
                .cloned()
                .collect(),
            origin: Relayed::default(),
        })
    }

    /// The client's body `raw`, which read `before`, with the prompt
    /// replaced, else a new body if no field is lost.
    fn write(&self, jev: &Jev, original: Option<(&Bytes, &Jev)>) -> Result<Value, Fault> {
        let original = original
            .and_then(|(raw, before)| Some((serde_json::from_slice::<Value>(raw).ok()?, before)));
        match original {
            Some((mut v, before)) => {
                v[self.prompt] = jev.prompt.clone().into();
                if before.model != jev.model {
                    v["model"] = jev.model.clone().into();
                }
                Ok(v)
            }
            None if jev.unknown.is_empty() => {
                let mut v = json!({ self.prompt: jev.prompt });
                if let Some(model) = &jev.model {
                    v["model"] = model.clone().into();
                }
                Ok(v)
            }
            None => Err(Fault::bad_request(format!(
                "'{}' cannot be translated",
                jev.unknown[0]
            ))),
        }
    }
}

/// Writes jev requests: the client's bytes as they came, with an edit
/// written into them, or a new body.
struct Prompts(JevFormat);

impl Encoder<Jev, ()> for Prompts {
    fn head(&mut self, _first: Option<&Jev>) {}

    fn encode(&mut self, jev: &Jev, out: &mut BytesMut) -> Result<(), Fault> {
        let origin = jev.origin.origin(self.0.name);
        if let Some(bytes) = jev.origin.bytes(self.0.name, jev) {
            out.extend_from_slice(bytes);
            return Ok(());
        }
        let v = self.0.write(jev, origin.map(|o| (&o.bytes, &o.said)))?;
        out.extend_from_slice(v.to_string().as_bytes());
        Ok(())
    }

    fn end(&mut self, _end: &Result<(), Fault>, _out: &mut BytesMut) -> Result<(), Fault> {
        Ok(())
    }
}

/// An encoder whose head `.1` edits after `.0` wrote it.
struct EditedHead<T, F>(Box<dyn Encoder<T, Head>>, F);

impl<T: 'static, F: Fn(&mut Head) + Send + 'static> Encoder<T, Head> for EditedHead<T, F> {
    fn head(&mut self, first: Option<&T>) -> Head {
        let mut head = self.0.head(first);
        (self.1)(&mut head);
        head
    }

    fn open(&mut self, out: &mut BytesMut) -> Result<bool, Fault> {
        self.0.open(out)
    }

    fn encode(&mut self, value: &T, out: &mut BytesMut) -> Result<(), Fault> {
        self.0.encode(value, out)
    }

    fn end(&mut self, end: &Result<(), Fault>, out: &mut BytesMut) -> Result<(), Fault> {
        self.0.end(end, out)
    }
}

/// Works on any jev format through the kind's types.
struct Polite;

#[async_trait]
impl Plugin for Polite {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        cx.filter(|jev: &mut Jev| {
            jev.prompt = format!("please, {}", jev.prompt);
            Ok(())
        })?;
        cx.filter(|a: &mut Answer| {
            if let Some(text) = &mut a.text {
                text.push_str(", thanks");
            }
            Ok(())
        })?;
        Ok(None)
    }
}

#[tokio::test]
async fn a_kind_the_kernel_has_never_seen_translates_between_its_formats() {
    let mock = MockTransport::new();
    let gw = serving(
        V1,
        [add_provider("v2", V2, mock.at("http://v2.test"))],
        Vec::new(),
        Some("v2"),
    )
    .unwrap_or_else(|e| panic!("{e}"));
    mock.reply(
        "http://v2.test",
        mock::json(StatusCode::OK, r#"{"text":"hello"}"#),
    );

    let request = r#"{"input":"hi","model":"j1"}"#;
    let (_, _, body) = read_whole(serve(&gw, to("jev-v1", request)).await.0).await;

    let sent: Value = serde_json::from_slice(mock.sent()[0].body()).unwrap_or_default();
    assert_eq!(sent, json!({ "prompt": "hi", "model": "j1" }));
    assert_eq!(body.ok().as_deref(), Some(&br#"{"output":"hello"}"#[..]));
}

#[tokio::test]
async fn a_field_jev_has_no_place_for_is_kept_or_refused() {
    let mock = MockTransport::new();
    let gw = |plugins: Vec<Arc<dyn Plugin>>, target| {
        let list = [
            add_provider("v1", V1, mock.at("http://v1.test")),
            add_provider("v2", V2, mock.at("http://v2.test")),
        ];
        serving(V2, list, plugins, Some(target)).unwrap_or_else(|e| panic!("{e}"))
    };
    let (same, other) = (gw(vec![Arc::new(Polite)], "v2"), gw(Vec::new(), "v1"));
    mock.reply(
        "http://v2.test",
        mock::json(StatusCode::OK, r#"{"text":"hello"}"#),
    );
    let request = r#"{"prompt":"hi","seed":7}"#;

    // Untranslated, the edited body keeps the field.
    let _ = read_whole(serve(&same, to("jev-v2", request)).await.0).await;
    let sent: Value = serde_json::from_slice(mock.sent()[0].body()).unwrap_or_default();
    assert_eq!(sent, json!({ "prompt": "please, hi", "seed": 7 }));

    // Translation would lose it, so it is refused.
    let (status, _, _) = read_whole(serve(&other, to("jev-v2", request)).await.0).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(mock.sent().len(), 1);
}

#[tokio::test]
async fn a_plugin_works_on_every_format_of_its_kind() {
    let mock = MockTransport::new();
    let gw = serving(
        V1,
        [add_provider("v2", V2, mock.at("http://v2.test"))],
        vec![Arc::new(Polite)],
        Some("v2"),
    )
    .unwrap_or_else(|e| panic!("{e}"));
    mock.reply(
        "http://v2.test",
        mock::json(StatusCode::OK, r#"{"text":"hello"}"#),
    );

    let (_, _, body) = read_whole(serve(&gw, to("jev-v1", r#"{"input":"hi"}"#)).await.0).await;

    let sent: Value = serde_json::from_slice(mock.sent()[0].body()).unwrap_or_default();
    assert_eq!(sent, json!({ "prompt": "please, hi" }));
    assert_eq!(
        body.ok().as_deref(),
        Some(&br#"{"output":"hello, thanks"}"#[..])
    );
}

#[tokio::test]
async fn a_default_target_of_another_kind_is_not_used() {
    let mock = MockTransport::new();
    let openai = add_provider(
        "openai",
        respan_gateway_formats::OpenAiChat,
        mock.at("http://o.test"),
    );
    let gw = serving(V1, [openai], Vec::new(), Some("openai")).expect("builds");

    let (resp, ended) = serve(&gw, to("jev-v1", r#"{"input":"hi"}"#)).await;
    let (status, _, _) = read_whole(resp).await;

    // A target exists, so the setup, not the request, is at fault.
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    let end = ended.await.end.map_err(|f| f.code().to_owned());
    assert_eq!(end, Err(codes::CONFIG.to_owned()));
    assert!(mock.sent().is_empty());
}

// One default target, which a later `target` replaces; a
// request it cannot take fails CONFIG, and routing by format is a plugin.
#[tokio::test]
async fn a_later_default_target_replaces_the_first_and_a_plugin_routes_by_format() {
    /// Sends chat requests to OpenAI; the rest keep the default target.
    struct ByFormat;

    #[async_trait]
    impl Plugin for ByFormat {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            if cx.format() == respan_gateway_formats::OpenAiChat.name() {
                cx.set_target("openai");
            }
            Ok(None)
        }
    }

    let mock = MockTransport::new();
    let gw = |plugins: Vec<Arc<dyn Plugin>>| {
        let openai = add_provider(
            "openai",
            respan_gateway_formats::OpenAiChat,
            mock.at("http://o.test"),
        );
        providers([openai, add_provider("v2", V2, mock.at("http://v2.test"))])
            .and_then(|gw| gw.serve(V1)?.serve(respan_gateway_formats::OpenAiChat))
            .and_then(|gw| {
                plugins
                    .into_iter()
                    .try_fold(gw, |gw, p| gw.plugin("by-format", p))
            })
            .and_then(|gw| gw.target("openai")?.target("v2"))
            .unwrap_or_else(|e| panic!("{e}"))
    };
    let chat = || post("/v1/chat/completions", r#"{"model":"m","messages":[]}"#);

    // Without the plugin, a chat request goes to `v2`, which cannot take it.
    let (resp, ended) = serve(&gw(Vec::new()), chat()).await;
    assert_eq!(read_whole(resp).await.0, StatusCode::INTERNAL_SERVER_ERROR);
    let end = ended.await.end.map_err(|f| f.code().to_owned());
    assert_eq!(end, Err(codes::CONFIG.to_owned()));
    assert!(mock.sent().is_empty());

    let gw = gw(vec![Arc::new(ByFormat)]);
    mock.reply(
        "http://v2.test",
        mock::json(StatusCode::OK, r#"{"text":"jev"}"#),
    )
    .reply(
        "http://o.test",
        mock::json(StatusCode::OK, r#"{"id":"c","choices":[]}"#),
    );
    let jev = read_whole(serve(&gw, to("jev-v1", r#"{"input":"hi"}"#)).await.0).await;
    let chat = read_whole(serve(&gw, chat()).await.0).await;

    assert_eq!((jev.0, chat.0), (StatusCode::OK, StatusCode::OK));
    let sent: Vec<String> = mock.sent().iter().map(|r| r.uri().to_string()).collect();
    assert_eq!(
        sent,
        ["http://v2.test/complete", "http://o.test/chat/completions"]
    );
}

// A format of no kind is a kind of one, so serving it needs no provider.
#[tokio::test]
async fn a_format_of_no_kind_is_served_like_any_other() {
    assert!(providers([]).and_then(|gw| gw.serve(Anywhere)).is_ok());
}

#[tokio::test]
async fn a_call_to_a_provider_of_another_kind_is_refused_when_made() {
    struct Stray;

    #[async_trait]
    impl Plugin for Stray {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            Ok(Some(cx.call("openai").await?))
        }
    }

    let mock = MockTransport::new();
    let openai = add_provider(
        "openai",
        respan_gateway_formats::OpenAiChat,
        mock.at("http://o.test"),
    );
    let gw = serving(V1, [openai], vec![Arc::new(Stray)], None).expect("builds");

    let (resp, ended) = serve(&gw, to("jev-v1", r#"{"input":"hi"}"#)).await;
    let (status, _, body) = read_whole(resp).await;

    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    let body = String::from_utf8(body.unwrap_or_default().to_vec()).unwrap_or_default();
    assert!(body.contains("cannot use"), "{body}");
    let end = ended.await.end.map_err(|f| f.code().to_owned());
    assert_eq!(end, Err(respan_gateway_core::codes::CONFIG.to_owned()));
    assert!(mock.sent().is_empty(), "nothing left for the provider");
}

#[tokio::test]
async fn a_plugin_for_another_kind_is_refused_not_ignored() {
    struct Redact;

    #[async_trait]
    impl Plugin for Redact {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            cx.filter(|_: &mut respan_gateway_formats::conversation::Reply| Ok(()))?;
            Ok(None)
        }
    }

    let mock = MockTransport::new();
    let gw = serving(
        V2,
        [add_provider("v2", V2, mock.at("http://v2.test"))],
        vec![Arc::new(Redact)],
        Some("v2"),
    )
    .unwrap_or_else(|e| panic!("{e}"));

    let (status, _, body) =
        read_whole(serve(&gw, to("jev-v2", r#"{"prompt":"hi"}"#)).await.0).await;

    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    let body = String::from_utf8_lossy(&body.unwrap_or_default()).into_owned();
    assert!(body.contains("another kind"), "{body}");
}

#[tokio::test]
async fn a_plugin_made_response_of_another_kind_fails_loudly() {
    struct Wrong;

    #[async_trait]
    impl Plugin for Wrong {
        async fn on_client(&self, _cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let wrong = [Ok(respan_gateway_formats::conversation::Reply::text(
                0, "hi",
            ))];
            Ok(Some(Response::stream(futures::stream::iter(wrong))))
        }
    }

    let gw = serving(V2, [], vec![Arc::new(Wrong)], None).unwrap_or_else(|e| panic!("{e}"));

    let (_, _, body) = read_whole(serve(&gw, to("jev-v2", r#"{"prompt":"hi"}"#)).await.0).await;

    let body = String::from_utf8_lossy(&body.unwrap_or_default()).into_owned();
    assert!(body.contains("another kind"), "{body}");
}

#[tokio::test]
async fn a_plugin_asking_for_another_kind_is_told_so_before_decoding() {
    struct Chatty;

    #[async_trait]
    impl Plugin for Chatty {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            type Chat = respan_gateway_formats::conversation::Conversation;
            // `is_kind` sets it apart from an undecodable body.
            assert!(!cx.is_kind::<Chat>());
            cx.input::<Chat>()?;
            Ok(None)
        }
    }

    let gw = serving(V2, [], vec![Arc::new(Chatty)], None).unwrap_or_else(|e| panic!("{e}"));

    // The plugin's kind, not the bad body, is the fault.
    let (status, _, body) = read_whole(serve(&gw, to("jev-v2", "not json")).await.0).await;

    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    let body = String::from_utf8_lossy(&body.unwrap_or_default()).into_owned();
    assert!(body.contains("another kind"), "{body}");
    // The Rust type name goes to logs only.
    assert!(!body.contains("respan_gateway"), "{body}");
}

#[tokio::test]
async fn a_request_a_plugin_only_looked_at_keeps_its_bytes() {
    struct Look;

    #[async_trait]
    impl Plugin for Look {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            cx.filter(|jev: &mut Jev| {
                jev.prompt = jev.prompt.clone();
                Ok(())
            })?;
            Ok(None)
        }
    }

    let mock = MockTransport::new();
    let gw = serving(
        V2,
        [add_provider("v2", V2, mock.at("http://v2.test"))],
        vec![Arc::new(Look)],
        Some("v2"),
    )
    .unwrap_or_else(|e| panic!("{e}"));
    mock.reply(
        "http://v2.test",
        mock::json(StatusCode::OK, r#"{"text":"hello"}"#),
    );
    // Bytes a re-encoding would not reproduce.
    let request = r#"{"prompt":"hi",  "seed":7}"#;

    let _ = read_whole(serve(&gw, to("jev-v2", request)).await.0).await;

    assert_eq!(&mock.sent()[0].body()[..], request.as_bytes());
}

#[test]
fn formats_of_one_name_and_different_kinds_do_not_build() {
    // A jev format named like OpenAI's.
    let impostor = JevFormat {
        name: "openai-chat",
        ..V1
    };
    let mock = MockTransport::new();
    let err = serving(
        respan_gateway_formats::OpenAiChat,
        [add_provider("jev", impostor, mock.at("http://jev.test"))],
        Vec::new(),
        Some("jev"),
    )
    .err();
    assert_eq!(
        err.as_ref().map(Fault::message),
        Some("two formats named openai-chat are of different kinds")
    );
}

#[test]
fn a_kind_of_one_type_for_both_directions_does_not_build() {
    /// Jev whose kind names `Jev` as both its input and output.
    struct OneWay;

    impl Format for OneWay {
        type Ask = Ask;
        type Answer = Head;
        type Input = Jev;
        type Output = Jev;

        fn name(&self) -> &str {
            "jev-one-way"
        }

        fn accepts(&self, head: &Ask) -> bool {
            formats::posts_to(head, &format!("/{}", Format::name(self)))
        }

        fn request_decoder(&self, request: &Ask) -> Box<dyn Decoder<Jev>> {
            V1.request_decoder(request)
        }

        fn request_encoder(
            &self,
            client: Option<&Ask>,
            body: &mut Bytes,
            streams: bool,
        ) -> Box<dyn Encoder<Jev, Ask>> {
            V1.request_encoder(client, body, streams)
        }

        fn response_decoder(
            &self,
            _head: &Head,
            call: &Ask,
            _sent: &Bytes,
        ) -> Box<dyn Decoder<Jev>> {
            V1.request_decoder(call)
        }

        fn response_encoder(
            &self,
            _answer: Option<&Head>,
            _ask: &Ask,
            _body: &Bytes,
            _refusal: Option<&Fault>,
        ) -> Box<dyn Encoder<Jev, Head>> {
            let head = respan_gateway_http::fresh_answer();
            formats::with_head(head, Box::new(Prompts(V1)))
        }
    }

    let mock = MockTransport::new();
    let err = serving(
        OneWay,
        [add_provider("jev", OneWay, mock.at("http://jev.test"))],
        Vec::new(),
        Some("jev"),
    )
    .err();
    assert_eq!(
        err.as_ref().map(Fault::message),
        Some("the kind of format jev-one-way has one type for both directions")
    );
}

#[tokio::test]
async fn a_format_of_no_kind_relays_and_has_no_input_type() {
    /// A protocol the gateway only passes on.
    struct Opaque;

    impl Format for Opaque {
        type Ask = Ask;
        type Answer = Head;
        type Input = Sent<Opaque>;
        type Output = Got<Opaque>;

        fn name(&self) -> &str {
            "opaque"
        }

        fn accepts(&self, head: &Ask) -> bool {
            formats::posts_to(head, &format!("/{}", Format::name(self)))
        }

        fn request_decoder(&self, _request: &Ask) -> Box<dyn Decoder<Self::Input>> {
            opaque::request_decoder()
        }

        fn request_encoder(
            &self,
            client: Option<&Ask>,
            _body: &mut Bytes,
            streams: bool,
        ) -> Box<dyn Encoder<Self::Input, Ask>> {
            let mut head = formats::call_head(client);
            head.method = http::Method::POST;
            formats::set_path(&mut head, "/");
            opaque::request_encoder(Some(&head), streams)
        }

        fn response_decoder(
            &self,
            head: &Head,
            call: &Ask,
            _sent: &Bytes,
        ) -> Box<dyn Decoder<Self::Output>> {
            opaque::response_decoder("opaque", head, call)
        }

        fn response_encoder(
            &self,
            answer: Option<&Head>,
            ask: &Ask,
            _body: &Bytes,
            refusal: Option<&Fault>,
        ) -> Box<dyn Encoder<Self::Output, Head>> {
            opaque::response_encoder("opaque", answer, ask, refusal)
        }
    }

    /// A guardrail that asks for the jev request.
    struct Guard;

    /// A rate limit, refusing every request.
    struct Limit;

    #[async_trait]
    impl Plugin for Limit {
        async fn on_client(&self, _cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            Err(Fault::new(
                StatusCode::TOO_MANY_REQUESTS,
                "limited",
                "slow down",
            ))
        }
    }

    #[async_trait]
    impl Plugin for Guard {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            cx.input::<Jev>()?;
            Ok(None)
        }
    }

    let mock = MockTransport::new();
    let gw = |plugins: Vec<Arc<dyn Plugin>>| {
        let key = http::HeaderMap::from_iter([(
            http::HeaderName::from_static("x-key"),
            HeaderValue::from_static("provider"),
        )]);
        let transport = mock.at("http://opaque.test").with_headers(key);
        let list = [add_provider("opaque", Opaque, transport)];
        serving(Opaque, list, plugins, Some("opaque")).unwrap_or_else(|e| panic!("{e}"))
    };
    let (open, guarded) = (gw(Vec::new()), gw(vec![Arc::new(Guard)]));
    let limited = gw(vec![Arc::new(Limit)]);
    mock.reply(
        "http://opaque.test",
        http::Response::new(vec![Ok(Bytes::from("anything at all"))]),
    )
    .reply(
        "http://opaque.test",
        mock::json(StatusCode::SERVICE_UNAVAILABLE, "busy"),
    )
    .reply(
        "http://opaque.test",
        http::Response::new(vec![Ok(Bytes::from("half")), Err(Fault::upstream("cut"))]),
    );

    // Client headers reach the provider, save hop-by-hop, host, credential
    // and encoding ones, and the cookies the transport strips.
    let mut ping = to("opaque", "ping");
    let headers = ping.headers_mut();
    headers.insert("host", HeaderValue::from_static("gateway.example"));
    headers.insert("x-trace", HeaderValue::from_static("7"));
    headers.insert("te", HeaderValue::from_static("gzip, Trailers"));
    headers.insert("connection", HeaderValue::from_static("X-Hop, x-key"));
    headers.insert("x-hop", HeaderValue::from_static("1"));
    headers.insert("expect", HeaderValue::from_static("100-continue"));
    headers.insert("content-length", HeaderValue::from_static("4"));
    headers.insert("authorization", HeaderValue::from_static("Bearer gateway"));
    headers.insert("proxy-authorization", HeaderValue::from_static("Basic x"));
    headers.insert("cookie", HeaderValue::from_static("session=1"));
    headers.insert("accept-encoding", HeaderValue::from_static("gzip"));
    let (_, head, body) = read_whole(serve(&open, ping).await.0).await;
    assert_eq!(body.ok().as_deref(), Some(&b"anything at all"[..]));
    // The provider's head is unchanged; no content type is invented.
    assert!(head.get("content-type").is_none());
    let sent = &mock.sent()[0];
    assert_eq!(&sent.body()[..], b"ping");
    assert_eq!(sent.headers()["x-trace"], "7");
    assert!(sent.headers().get("host").is_none());
    for gone in ["x-hop", "expect", "content-length"] {
        assert!(sent.headers().get(gone).is_none(), "{gone}");
    }
    // `TE` goes like every connection header, as trailers
    // are not delivered.
    assert!(sent.headers().get("te").is_none());
    // A header `Connection` names is dropped; the provider's own still goes.
    assert_eq!(sent.headers()["x-key"], "provider");
    for credential in [
        "authorization",
        "proxy-authorization",
        "cookie",
        "accept-encoding",
    ] {
        assert!(sent.headers().get(credential).is_none(), "{credential}");
    }

    // A provider's failure is relayed as is and ends `Err`.
    let (resp, ended) = serve(&open, to("opaque", "ping")).await;
    let (status, _, body) = read_whole(resp).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body.ok().as_deref(), Some(&b"busy"[..]));
    let end = ended.await.end.map_err(|f| f.status());
    assert_eq!(end, Err(StatusCode::SERVICE_UNAVAILABLE.as_u16()));

    // A body cut short upstream is cut short for the client.
    let (status, _, body) = read_whole(serve(&open, to("opaque", "ping")).await.0).await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.is_err(), "{body:?}");

    // A refusal says why, in plain text.
    let (status, headers, body) = read_whole(serve(&limited, to("opaque", "ping")).await.0).await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(headers["content-type"], "text/plain; charset=utf-8");
    assert_eq!(body.ok().as_deref(), Some(&b"slow down"[..]));

    // A plugin for jev that does not skip other traffic refuses it.
    let (status, _, _) = read_whole(serve(&guarded, to("opaque", "ping")).await.0).await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(mock.sent().len(), 3, "the guarded request never left");
}

#[tokio::test]
async fn a_format_that_writes_errors_as_200_is_refused_once() {
    /// Jev over a protocol that puts errors in a 200, as JSON-RPC does.
    struct InBand;

    impl Format for InBand {
        type Ask = Ask;
        type Answer = Head;
        type Input = Jev;
        type Output = Answer;

        fn name(&self) -> &str {
            V2.name()
        }

        fn accepts(&self, head: &Ask) -> bool {
            formats::posts_to(head, &format!("/{}", Format::name(self)))
        }

        fn request_decoder(&self, request: &Ask) -> Box<dyn Decoder<Jev>> {
            V2.request_decoder(request)
        }

        fn request_encoder(
            &self,
            client: Option<&Ask>,
            body: &mut Bytes,
            streams: bool,
        ) -> Box<dyn Encoder<Jev, Ask>> {
            V2.request_encoder(client, body, streams)
        }

        fn response_decoder(
            &self,
            head: &Head,
            call: &Ask,
            sent: &Bytes,
        ) -> Box<dyn Decoder<Answer>> {
            V2.response_decoder(head, call, sent)
        }

        fn response_encoder(
            &self,
            answer: Option<&Head>,
            ask: &Ask,
            body: &Bytes,
            refusal: Option<&Fault>,
        ) -> Box<dyn Encoder<Answer, Head>> {
            // A format writes a refusal's status, so this one
            // overrides it after.
            let encoder = V2.response_encoder(answer, ask, body, refusal);
            Box::new(EditedHead(encoder, |head: &mut Head| {
                head.status = StatusCode::OK;
            }))
        }
    }

    struct Deny;

    #[async_trait]
    impl Plugin for Deny {
        async fn on_client(&self, _cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            Err(Fault::new(StatusCode::FORBIDDEN, "denied", "no"))
        }
    }

    let gw = serving(InBand, [], vec![Arc::new(Deny)], None).unwrap_or_else(|e| panic!("{e}"));

    let (status, _, _) = read_whole(serve(&gw, to("jev-v2", r#"{"prompt":"hi"}"#)).await.0).await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn a_format_that_panics_building_its_encoder_is_a_plain_500() {
    struct Broken;

    impl Format for Broken {
        type Ask = Ask;
        type Answer = Head;
        type Input = Jev;
        type Output = Answer;

        fn name(&self) -> &str {
            V2.name()
        }

        fn accepts(&self, head: &Ask) -> bool {
            formats::posts_to(head, &format!("/{}", Format::name(self)))
        }

        fn request_decoder(&self, request: &Ask) -> Box<dyn Decoder<Jev>> {
            V2.request_decoder(request)
        }

        fn request_encoder(
            &self,
            client: Option<&Ask>,
            body: &mut Bytes,
            streams: bool,
        ) -> Box<dyn Encoder<Jev, Ask>> {
            V2.request_encoder(client, body, streams)
        }

        fn response_decoder(
            &self,
            head: &Head,
            call: &Ask,
            sent: &Bytes,
        ) -> Box<dyn Decoder<Answer>> {
            V2.response_decoder(head, call, sent)
        }

        fn response_encoder(
            &self,
            answer: Option<&Head>,
            ask: &Ask,
            body: &Bytes,
            _refusal: Option<&Fault>,
        ) -> Box<dyn Encoder<Answer, Head>> {
            let _ = (answer, ask, body);
            panic!("a buggy format")
        }
    }

    let log = Outcomes::default();
    let gw =
        serving(Broken, [], vec![Arc::new(log.clone())], None).unwrap_or_else(|e| panic!("{e}"));

    let (status, headers, _) =
        read_whole(serve(&gw, to("jev-v2", r#"{"prompt":"hi"}"#)).await.0).await;
    gw.idle().await;

    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(headers["content-type"], "text/plain; charset=utf-8");
    let end = log.only().end;
    assert!(matches!(end, Err(f) if f.code() == respan_gateway_core::codes::INTERNAL));
}

#[tokio::test]
async fn a_providers_error_body_is_relayed_once() {
    let mock = MockTransport::new();
    let gw = serving(
        V2,
        [add_provider("v2", V2, mock.at("http://v2.test"))],
        Vec::new(),
        Some("v2"),
    )
    .expect("builds");
    let refusal = r#"{"error":"no"}"#;
    mock.reply(
        "http://v2.test",
        mock::json(StatusCode::TOO_MANY_REQUESTS, refusal),
    );

    let (status, _, body) =
        read_whole(serve(&gw, to("jev-v2", r#"{"prompt":"hi"}"#)).await.0).await;

    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(body.ok().as_deref(), Some(refusal.as_bytes()));
}

#[tokio::test]
async fn a_jev_stream_is_relayed_and_edited_event_by_event() {
    /// Drops the answer `b`.
    struct Mute;

    #[async_trait]
    impl Plugin for Mute {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            struct NoB;
            impl Filter<Answer> for NoB {
                fn edit(&mut self, a: Answer, out: &mut Vec<Answer>) -> Result<(), Fault> {
                    if a.text.as_deref() != Some("b") {
                        out.push(a);
                    }
                    Ok(())
                }
            }
            cx.filter(NoB)?;
            Ok(None)
        }
    }

    let mock = MockTransport::new();
    let gw = |plugins: Vec<Arc<dyn Plugin>>| {
        let list = [add_provider("v2", V2, mock.at("http://v2.test"))];
        serving(V2, list, plugins, Some("v2")).unwrap_or_else(|e| panic!("{e}"))
    };
    let (plain, polite, drop) = (
        gw(Vec::new()),
        gw(vec![Arc::new(Polite)]),
        gw(vec![Arc::new(Mute)]),
    );
    let stream = [
        "id: 1\ndata: {\"text\":\"a\"}\n\n",
        "data: {\"text\":\"b\"}\n\n",
    ];
    mock.reply("http://v2.test", mock::sse(&stream))
        .reply("http://v2.test", mock::sse(&stream))
        .reply("http://v2.test", mock::sse(&stream));

    let hi = || to("jev-v2", r#"{"prompt":"hi"}"#);
    let (_, _, plain) = read_whole(serve(&plain, hi()).await.0).await;
    let (_, _, polite) = read_whole(serve(&polite, hi()).await.0).await;
    let (_, _, dropped) = read_whole(serve(&drop, hi()).await.0).await;

    assert_eq!(dropped.ok().as_deref(), Some(stream[0].as_bytes()));

    assert_eq!(plain.ok().as_deref(), Some(stream.concat().as_bytes()));
    let polite = String::from_utf8(polite.unwrap_or_default().to_vec()).unwrap_or_default();
    assert_eq!(
        polite,
        "id: 1\ndata: {\"text\":\"a, thanks\"}\n\ndata: {\"text\":\"b, thanks\"}\n\n"
    );
}

#[tokio::test]
async fn a_translated_jev_stream_and_a_whole_body_with_a_tail_are_one_document_each() {
    struct Tail;

    #[async_trait]
    impl Plugin for Tail {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            struct Sign;
            impl Filter<Answer> for Sign {
                fn edit(&mut self, a: Answer, out: &mut Vec<Answer>) -> Result<(), Fault> {
                    out.push(a);
                    Ok(())
                }
                fn end(
                    &mut self,
                    _end: &Result<(), Fault>,
                    out: &mut Vec<Answer>,
                ) -> Result<(), Fault> {
                    out.push(Answer::new("!"));
                    Ok(())
                }
            }
            cx.filter(Sign)?;
            Ok(None)
        }
    }

    let mock = MockTransport::new();
    let v2 = || [add_provider("v2", V2, mock.at("http://v2.test"))];
    let v1 = serving(V1, v2(), Vec::new(), Some("v2")).unwrap_or_else(|e| panic!("{e}"));
    let tail = serving(V2, v2(), vec![Arc::new(Tail)], Some("v2"));
    let tail = tail.unwrap_or_else(|e| panic!("{e}"));
    let stream = ["data: {\"text\":\"a\"}\n\n", "data: {\"text\":\"b\"}\n\n"];
    mock.reply("http://v2.test", mock::sse(&stream)).reply(
        "http://v2.test",
        mock::json(StatusCode::OK, r#"{"text":"hi"}"#),
    );

    let (_, _, v1) = read_whole(serve(&v1, to("jev-v1", r#"{"input":"hi"}"#)).await.0).await;
    let (_, _, tail) = read_whole(serve(&tail, to("jev-v2", r#"{"prompt":"hi"}"#)).await.0).await;

    assert_eq!(v1.ok().as_deref(), Some(&br#"{"output":"ab"}"#[..]));
    assert_eq!(tail.ok().as_deref(), Some(&br#"{"text":"hi!"}"#[..]));
}

#[tokio::test]
async fn a_redirect_is_passed_on_as_it_came_not_read_as_an_answer() {
    let mock = MockTransport::new();
    let gw = serving(
        V2,
        [add_provider("v2", V2, mock.at("http://v2.test"))],
        vec![Arc::new(Polite)],
        Some("v2"),
    )
    .unwrap_or_else(|e| panic!("{e}"));
    let moved = http::Response::builder()
        .status(StatusCode::MOVED_PERMANENTLY)
        .header("content-type", "text/html")
        .body(vec![Ok(Bytes::from("<html>moved</html>"))])
        .unwrap_or_default();
    mock.reply("http://v2.test", moved);

    let (resp, ended) = serve(&gw, to("jev-v2", r#"{"prompt":"hi"}"#)).await;
    let (status, headers, body) = read_whole(resp).await;

    assert_eq!(status, StatusCode::MOVED_PERMANENTLY);
    assert_eq!(headers["content-type"], "text/html");
    assert_eq!(body.ok().as_deref(), Some(&b"<html>moved</html>"[..]));
    // The redirect is a fault, a 502 to any other format, that
    // carries the provider's answer, 301 and all.
    let end = ended.await.end.err();
    assert_eq!(
        end.as_ref().map(Fault::status),
        Some(StatusCode::BAD_GATEWAY.as_u16())
    );
    let answer = end
        .as_ref()
        .and_then(|f| f.answer::<http::Response<Bytes>>("jev-v2"));
    assert_eq!(
        answer.map(|a| a.status()),
        Some(StatusCode::MOVED_PERMANENTLY)
    );
}

#[tokio::test]
async fn a_user_formats_failure_keeps_the_providers_words() {
    let mock = MockTransport::new();
    let gw = serving(
        V2,
        [add_provider("v2", V2, mock.at("http://v2.test"))],
        Vec::new(),
        Some("v2"),
    )
    .unwrap_or_else(|e| panic!("{e}"));
    let error = r#"{"error":{"type":"invalid_request_error","message":"bad prompt"}}"#;
    mock.reply("http://v2.test", mock::json(StatusCode::BAD_REQUEST, error));

    let (resp, ended) = serve(&gw, to("jev-v2", r#"{"prompt":"hi"}"#)).await;
    let (status, _, body) = read_whole(resp).await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body.ok().as_deref(), Some(error.as_bytes()));
    let end = ended.await.end;
    let end = end.map_err(|f| (f.code().to_owned(), f.message().to_owned()));
    let said = ("invalid_request_error".to_owned(), "bad prompt".to_owned());
    assert_eq!(end, Err(said));
}

#[tokio::test]
async fn a_jev_stream_keeps_its_comments_and_says_a_providers_error() {
    let mock = MockTransport::new();
    let gw = serving(
        V2,
        [add_provider("v2", V2, mock.at("http://v2.test"))],
        Vec::new(),
        Some("v2"),
    )
    .unwrap_or_else(|e| panic!("{e}"));
    let kept = [": keep-alive\n\n", "data: {\"text\":\"a\"}\n\n"];
    let failed = [
        "data: {\"text\":\"a\"}\n\n",
        "data: {\"error\":{\"message\":\"overloaded\"}}\n\n",
    ];
    mock.reply("http://v2.test", mock::sse(&kept))
        .reply("http://v2.test", mock::sse(&failed))
        .reply(
            "http://v2.test",
            mock::json(StatusCode::OK, r#"{"error":{"message":"overloaded"}}"#),
        );

    let mut ends = Vec::new();
    for _ in 0..3 {
        let (resp, ended) = serve(&gw, to("jev-v2", r#"{"prompt":"hi"}"#)).await;
        let (status, _, body) = read_whole(resp).await;
        let end = ended.await.end.map_err(|f| f.message().to_owned());
        ends.push((status, body.unwrap_or_default(), end));
    }

    // SSE comments pass through; a provider's error after content is
    // relayed as it came, and one before any content refused. r14 (D4): the
    // kernel strips nothing, so the refusal, in the provider's own format,
    // carries its error body as it came; translated, it is the client's words
    // (a_jev_error_inside_a_success_translated_is_refused_in_the_clients_words).
    let (ok, overloaded) = (StatusCode::OK, Err("overloaded".to_owned()));
    assert_eq!(ends[0], (ok, Bytes::from(kept.concat()), Ok(())));
    let failed = Bytes::from(failed.concat());
    assert_eq!(ends[1], (ok, failed, overloaded.clone()));
    let refused = Bytes::from_static(br#"{"error":{"message":"overloaded"}}"#);
    assert_eq!(ends[2], (StatusCode::BAD_GATEWAY, refused, overloaded));
}

#[tokio::test]
async fn a_jev_error_inside_a_success_translated_is_refused_in_the_clients_words() {
    let mock = MockTransport::new();
    let gw = serving(
        V1,
        [add_provider("v2", V2, mock.at("http://v2.test"))],
        Vec::new(),
        Some("v2"),
    )
    .unwrap_or_else(|e| panic!("{e}"));
    mock.reply(
        "http://v2.test",
        mock::json(StatusCode::OK, r#"{"error":{"message":"overloaded"}}"#),
    );

    let (status, _, body) = read_whole(serve(&gw, to("jev-v1", r#"{"input":"hi"}"#)).await.0).await;

    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert_eq!(
        body.ok().as_deref(),
        Some(&br#"{"error":"overloaded"}"#[..])
    );
}

#[tokio::test]
async fn the_input_is_read_from_the_head_as_it_arrived() {
    /// Jev whose prompt is in the `x-prompt` header, which it passes on.
    struct Headed;

    impl Format for Headed {
        type Ask = Ask;
        type Answer = Head;
        type Input = Jev;
        type Output = Answer;

        fn name(&self) -> &str {
            "jev-headed"
        }

        fn accepts(&self, head: &Ask) -> bool {
            formats::posts_to(head, &format!("/{}", Format::name(self)))
        }

        fn request_decoder(&self, request: &Ask) -> Box<dyn Decoder<Jev>> {
            let prompt = request
                .headers
                .get("x-prompt")
                .and_then(|p| p.to_str().ok());
            let prompt = prompt.map(String::from);
            let name: Arc<str> = Arc::from("jev-headed");
            formats::message_decoder(move |raw: Bytes| match &prompt {
                Some(prompt) => {
                    let jev = Jev {
                        prompt: prompt.clone(),
                        model: None,
                        unknown: Vec::new(),
                        origin: Relayed::default(),
                    };
                    Ok(jev.came(&name, raw))
                }
                None => Err(Fault::bad_request("no prompt")),
            })
        }

        fn request_encoder(
            &self,
            client: Option<&Ask>,
            body: &mut Bytes,
            streams: bool,
        ) -> Box<dyn Encoder<Jev, Ask>> {
            V2.request_encoder(client, body, streams)
        }

        fn response_decoder(
            &self,
            head: &Head,
            call: &Ask,
            sent: &Bytes,
        ) -> Box<dyn Decoder<Answer>> {
            V2.response_decoder(head, call, sent)
        }

        fn response_encoder(
            &self,
            answer: Option<&Head>,
            ask: &Ask,
            body: &Bytes,
            refusal: Option<&Fault>,
        ) -> Box<dyn Encoder<Answer, Head>> {
            V2.response_encoder(answer, ask, body, refusal)
        }
    }

    /// Reads the input, and edits the header on the call.
    #[derive(Clone, Default)]
    struct Edit(std::sync::Arc<std::sync::Mutex<String>>);

    #[async_trait]
    impl Plugin for Edit {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let prompt = cx.input::<Jev>()?.prompt.clone();
            self.0.lock().map(|mut seen| *seen = prompt).ok();
            Ok(None)
        }

        async fn on_provider(
            &self,
            px: &mut respan_gateway_core::ProviderContext,
        ) -> Result<(), Fault> {
            px.edit_call(|call: &mut Ask, _: &mut Bytes| {
                let edited = HeaderValue::from_static("edited");
                call.headers.insert("x-prompt", edited);
                Ok(())
            });
            Ok(())
        }
    }

    let edit = Edit::default();
    let mock = MockTransport::new();
    let gw = serving(
        Headed,
        [add_provider("h", Headed, mock.at("http://h.test"))],
        vec![Arc::new(edit.clone())],
        Some("h"),
    )
    .unwrap_or_else(|e| panic!("{e}"));
    mock.reply(
        "http://h.test",
        mock::json(StatusCode::OK, r#"{"text":"hello"}"#),
    );
    let mut req = to("jev-headed", r#"{"model":"j1"}"#);
    let arrived = HeaderValue::from_static("arrived");
    req.headers_mut().insert("x-prompt", arrived);

    assert_eq!(read_whole(serve(&gw, req).await.0).await.0, StatusCode::OK);

    let seen = edit.0.lock().map(|s| s.clone()).unwrap_or_default();
    assert_eq!(seen, "arrived");
    // A relay passes the head as edited; the body goes as it came.
    let sent = mock.sent();
    assert_eq!(sent[0].headers()["x-prompt"], "edited");
    assert_eq!(sent[0].body(), &br#"{"model":"j1"}"#[..]);
}

/// A protocol of no kind that accepts any `POST`, relayed as it came.
struct Anywhere;

impl Format for Anywhere {
    type Ask = Ask;
    type Answer = Head;
    type Input = Sent<Anywhere>;
    type Output = Got<Anywhere>;

    fn name(&self) -> &str {
        "anywhere"
    }

    fn accepts(&self, head: &Ask) -> bool {
        head.method == http::Method::POST
    }

    fn request_decoder(&self, _request: &Ask) -> Box<dyn Decoder<Self::Input>> {
        opaque::request_decoder()
    }

    fn request_encoder(
        &self,
        client: Option<&Ask>,
        body: &mut Bytes,
        streams: bool,
    ) -> Box<dyn Encoder<Self::Input, Ask>> {
        let _ = body;
        let mut head = formats::call_head(client);
        head.method = http::Method::POST;
        formats::set_path(&mut head, "/");
        opaque::request_encoder(Some(&head), streams)
    }

    fn response_decoder(
        &self,
        head: &Head,
        call: &Ask,
        _sent: &Bytes,
    ) -> Box<dyn Decoder<Self::Output>> {
        opaque::response_decoder("anywhere", head, call)
    }

    fn response_encoder(
        &self,
        answer: Option<&Head>,
        ask: &Ask,
        body: &Bytes,
        refusal: Option<&Fault>,
    ) -> Box<dyn Encoder<Self::Output, Head>> {
        let _ = body;
        opaque::response_encoder("anywhere", answer, ask, refusal)
    }
}

/// Notes the client's format, then refuses the request.
#[derive(Clone, Default)]
struct NoteFormat(Arc<std::sync::Mutex<Vec<String>>>);

#[async_trait]
impl Plugin for NoteFormat {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        self.0
            .lock()
            .map(|mut n| n.push(cx.format().to_owned()))
            .ok();
        Err(Fault::new(StatusCode::FORBIDDEN, "noted", "noted"))
    }
}

#[tokio::test]
async fn the_first_format_served_that_accepts_a_request_serves_it() {
    let serving_by = |gw: Gateway, anywhere_first: bool| match anywhere_first {
        true => gw.serve(Anywhere)?.serve(V1),
        false => gw.serve(V1)?.serve(V2)?.serve(Anywhere),
    };
    let served = async |anywhere_first: bool| {
        let note = NoteFormat::default();
        let any = add_provider("any", Anywhere, MockTransport::new().at("http://any.test"));
        let gw = providers([any])
            .and_then(|gw| serving_by(gw, anywhere_first)?.plugin("note", note.clone()))
            .unwrap_or_else(|e| panic!("{e}"));
        let mut get = to("jev-v1", "{}");
        *get.method_mut() = http::Method::GET;
        let mut statuses = Vec::new();
        for req in [
            to("jev-v1", "{}"),
            to("jev-v2", "{}"),
            to("other", "{}"),
            get,
        ] {
            statuses.push(read_whole(serve(&gw, req).await.0).await.0.as_u16());
        }
        let noted = note.0.lock().map(|n| n.clone()).unwrap_or_default();
        (noted, statuses)
    };

    let (noted, statuses) = served(false).await;
    assert_eq!(noted, ["jev-v1", "jev-v2", "anywhere"]);
    assert_eq!(statuses, [403, 403, 403, 404]);
    let (noted, statuses) = served(true).await;
    assert_eq!(noted, ["anywhere", "anywhere", "anywhere"]);
    assert_eq!(statuses, [403, 403, 403, 404]);
}

#[tokio::test]
async fn a_router_overrides_the_default_target() {
    let mock = MockTransport::new();
    let list = [
        add_provider("v1", V1, mock.at("http://v1.test")),
        add_provider("v2", V2, mock.at("http://v2.test")),
    ];
    let router = respan_gateway_plugins::ModelRouter::new(|j: &Jev| j.model.as_deref());
    let router = router.route("old", "v1");
    let gw = serving(V2, list, vec![Arc::new(router)], Some("v2")).expect("builds");
    let answer = |key| mock::json(StatusCode::OK, &format!(r#"{{"{key}":"hello"}}"#));
    mock.reply("http://v1.test", answer("output"))
        .reply("http://v2.test", answer("text"));

    for model in ["old-1", "new-1"] {
        let request = format!(r#"{{"prompt":"hi","model":"{model}"}}"#);
        let (status, _, body) = read_whole(serve(&gw, to("jev-v2", request)).await.0).await;
        assert_eq!(status, StatusCode::OK, "{model}");
        assert_eq!(body.ok().as_deref(), Some(&br#"{"text":"hello"}"#[..]));
    }

    let sent: Vec<String> = mock.sent().iter().map(|s| s.uri().to_string()).collect();
    assert_eq!(sent, ["http://v1.test/complete", "http://v2.test/complete"]);
}

#[tokio::test]
async fn a_plugin_for_one_kind_lets_other_traffic_pass_when_it_says_so() {
    /// [`Polite`], for jev only: other traffic passes untouched.
    struct PoliteJev;

    #[async_trait]
    impl Plugin for PoliteJev {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            match cx.is_kind::<Jev>() {
                false => Ok(None),
                true => Polite.on_client(cx).await,
            }
        }
    }

    let mock = MockTransport::new();
    mock.reply(
        "http://v2.test",
        mock::json(StatusCode::OK, r#"{"text":"hi"}"#),
    )
    .reply(
        "http://any.test",
        http::Response::new(vec![Ok(Bytes::from("pong"))]),
    );

    // Each client targets the provider of its own format, as an adapter's
    // mark would say.
    let mut jev = to("jev-v2", r#"{"prompt":"hi"}"#);
    jev.headers_mut()
        .insert("x-to", HeaderValue::from_static("v2"));
    let mut other = to("other", "ping");
    other
        .headers_mut()
        .insert("x-to", HeaderValue::from_static("any"));
    let gw = providers([
        add_provider("v2", V2, mock.at("http://v2.test")),
        add_provider("any", Anywhere, mock.at("http://any.test")),
    ])
    .and_then(|gw| {
        let gw = gw.serve(V2)?.serve(Anywhere)?;
        gw.plugin("to", ToHeader)?.plugin("polite", PoliteJev)
    })
    .unwrap_or_else(|e| panic!("{e}"));

    let (_, _, jev) = read_whole(serve(&gw, jev).await.0).await;
    let (status, _, other) = read_whole(serve(&gw, other).await.0).await;

    assert_eq!(jev.ok().as_deref(), Some(&br#"{"text":"hi, thanks"}"#[..]));
    assert_eq!(status, StatusCode::OK);
    assert_eq!(other.ok().as_deref(), Some(&b"pong"[..]));
    let sent = mock.sent();
    let prompt: Value = serde_json::from_slice(sent[0].body()).unwrap_or_default();
    assert_eq!(prompt["prompt"], "please, hi");
    assert_eq!(&sent[1].body()[..], b"ping");
}

/// Targets the provider the adapter named in `x-to`.
struct ToHeader;

#[async_trait]
impl Plugin for ToHeader {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let asked = cx.extensions().get::<Asked>();
        let to = asked.and_then(|a| a.headers.get("x-to")?.to_str().ok());
        if let Some(to) = to.map(str::to_owned) {
            cx.set_target(to);
        }
        Ok(None)
    }
}

/// Runs `plugin` for a jev-v2 client whose provider speaks jev-v2 too.
fn jev_relay(plugin: impl Plugin, mock: &MockTransport) -> Gateway {
    let v2 = add_provider("v2", V2, mock.at("http://v2.test"));
    serving(V2, [v2], vec![Arc::new(plugin)], Some("v2")).unwrap_or_else(|e| panic!("{e}"))
}

#[tokio::test]
async fn what_filters_kept_back_goes_before_a_relayed_providers_error() {
    /// Keeps every answer back until the end; passes a keep-alive at once,
    /// as every value reaches a filter.
    struct Late;

    #[async_trait]
    impl Plugin for Late {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            cx.filter(KeepAll(Vec::new()))?;
            Ok(None)
        }
    }

    struct KeepAll(Vec<Answer>);

    impl Filter<Answer> for KeepAll {
        fn edit(&mut self, event: Answer, out: &mut Vec<Answer>) -> Result<(), Fault> {
            match event.text {
                None => out.push(event),
                Some(_) => self.0.push(event),
            }
            Ok(())
        }

        fn end(&mut self, _end: &Result<(), Fault>, out: &mut Vec<Answer>) -> Result<(), Fault> {
            out.append(&mut self.0);
            Ok(())
        }
    }

    let mock = MockTransport::new();
    let gw = jev_relay(Late, &mock);
    let error = "data: {\"error\":{\"message\":\"overloaded\"}}\n\n";
    // The comment goes out at once, so the error cannot be refused.
    mock.reply(
        "http://v2.test",
        mock::sse(&[": ping\n\n", "data: {\"text\":\"a\"}\n\n", error]),
    );

    let (status, _, body) =
        read_whole(serve(&gw, to("jev-v2", r#"{"prompt":"hi"}"#)).await.0).await;

    assert_eq!(status, StatusCode::OK);
    let body = String::from_utf8(body.unwrap_or_default().to_vec()).unwrap_or_default();
    assert!(body.contains("\"a\"") && body.ends_with(error), "{body}");
}

#[tokio::test]
async fn an_input_that_cannot_be_translated_is_the_fault_of_whoever_brought_it() {
    /// Adds a field jev-v2 cannot say.
    struct Add;

    #[async_trait]
    impl Plugin for Add {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            cx.filter(|jev: &mut Jev| {
                jev.unknown.push("extra".into());
                Ok(())
            })?;
            Ok(None)
        }
    }

    let mock = MockTransport::new();
    let v2 = || add_provider("v2", V2, mock.at("http://v2.test"));
    let plain = serving(V1, [v2()], Vec::new(), Some("v2")).unwrap_or_else(|e| panic!("{e}"));
    let adding = serving(V1, [v2()], vec![Arc::new(Add)], Some("v2"));
    let adding = adding.unwrap_or_else(|e| panic!("{e}"));

    let sent = read_whole(
        serve(&plain, to("jev-v1", r#"{"input":"hi","extra":1}"#))
            .await
            .0,
    )
    .await;
    let added = read_whole(serve(&adding, to("jev-v1", r#"{"input":"hi"}"#)).await.0).await;

    assert_eq!(sent.0, StatusCode::BAD_REQUEST);
    assert_eq!(added.0, StatusCode::INTERNAL_SERVER_ERROR);
    assert!(mock.sent().is_empty());
}

#[tokio::test]
async fn traffic_of_no_kind_is_a_kind_of_its_own() {
    /// Notes whether the request is of anywhere's kind, then of jev's kind
    /// by its input and its output type, then refuses.
    #[derive(Clone, Default)]
    struct Kinds(Arc<std::sync::Mutex<Vec<(bool, bool, bool)>>>);

    #[async_trait]
    impl Plugin for Kinds {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            // Traffic of no kind is a kind of its own, which no plugin for
            // another kind reads.
            let other = cx.input::<Jev>().err().map(|f| f.code().to_owned());
            let jev = cx.is_kind::<Jev>();
            assert_eq!(other.is_some_and(|c| c == codes::INTERNAL), !jev);
            let kinds = (
                cx.is_kind::<Sent<Anywhere>>(),
                cx.is_kind::<Jev>(),
                cx.is_kind::<Answer>(),
            );
            self.0.lock().map(|mut k| k.push(kinds)).ok();
            Err(Fault::new(StatusCode::FORBIDDEN, "noted", "noted"))
        }
    }

    let (kinds, mock) = (Kinds::default(), MockTransport::new());
    let any = add_provider("any", Anywhere, mock.at("http://any.test"));
    let gw = providers([any])
        .and_then(|gw| {
            gw.serve(V1)?
                .serve(Anywhere)?
                .plugin("kinds", kinds.clone())
        })
        .unwrap_or_else(|e| panic!("{e}"));

    let _ = read_whole(serve(&gw, to("any", "ping")).await.0).await;
    let _ = read_whole(serve(&gw, to("jev-v1", r#"{"input":"hi"}"#)).await.0).await;

    assert_eq!(
        kinds.0.lock().map(|k| k.clone()).ok(),
        Some(vec![(true, false, false), (false, true, true)])
    );
}

#[tokio::test]
async fn a_refusal_goes_out_with_its_faults_status_even_a_success() {
    /// Refuses as JSON-RPC does: an error in a 200.
    struct Rpc;

    #[async_trait]
    impl Plugin for Rpc {
        async fn on_client(&self, _cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            Err(Fault::new(StatusCode::OK, "rpc_error", "no"))
        }
    }

    let mock = MockTransport::new();
    let gw = jev_relay(Rpc, &mock);

    let (status, _, body) =
        read_whole(serve(&gw, to("jev-v2", r#"{"prompt":"hi"}"#)).await.0).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body.ok().as_deref(), Some(&br#"{"error":"no"}"#[..]));
    assert!(mock.sent().is_empty());
}

/// A provider whose stream sends its first bytes and stays open.
struct Open;

#[async_trait]
impl respan_gateway_core::Transport for Open {
    type Ask = Ask;
    type Answer = Head;

    async fn send(
        &self,
        _call: respan_gateway_core::Exchange<Ask>,
    ) -> Result<respan_gateway_core::Exchange<Head>, Fault> {
        use futures::StreamExt;
        let first = Bytes::from("data: hello\n\n");
        let body = futures::stream::iter([Ok(first)]).chain(futures::stream::pending());
        let head = respan_gateway_http::fresh_answer();
        Ok(respan_gateway_core::Exchange::new(head, body.boxed()))
    }
}

#[tokio::test]
async fn until_content_returns_a_response_of_no_kind_at_its_first_bytes_or_its_failure() {
    /// Sends the call's answer read to its content, or refuses with its failure.
    struct Race;

    #[async_trait]
    impl Plugin for Race {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            Ok(Some(cx.call("any").await?.until_content().await?))
        }
    }

    let open = add_provider("any", Anywhere, Open);
    let gw = serving(Anywhere, [open], vec![Arc::new(Race)], None);
    let gw = gw.unwrap_or_else(|e| panic!("{e}"));
    let started = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        serve(&gw, to("any", "ping")),
    );
    assert!(
        started.await.is_ok(),
        "until_content waits for an open stream's end"
    );

    let mock = MockTransport::new();
    let failing = add_provider("any", Anywhere, mock.at("http://any.test"));
    let gw = serving(Anywhere, [failing], vec![Arc::new(Race)], None);
    let gw = gw.unwrap_or_else(|e| panic!("{e}"));
    mock.reply(
        "http://any.test",
        mock::json(StatusCode::SERVICE_UNAVAILABLE, "busy"),
    );

    let (status, _, body) = read_whole(serve(&gw, to("any", "ping")).await.0).await;

    // The provider's error response, as the client gets it when sent.
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body.ok().as_deref(), Some(&b"busy"[..]));
}

// A translated success's head is the client format's alone; a
// `Retry-After` crosses a translation only on a fault (`retry_after`).
#[tokio::test]
async fn a_translating_encoder_gets_none_of_the_providers_head() {
    /// jev v1, noting the headers its encoder is given.
    #[derive(Clone, Default)]
    struct Noting(Arc<std::sync::Mutex<Option<http::HeaderMap>>>);

    impl Format for Noting {
        type Ask = Ask;
        type Answer = Head;
        type Input = Jev;
        type Output = Answer;

        fn name(&self) -> &str {
            V1.name
        }

        fn accepts(&self, head: &Ask) -> bool {
            V1.accepts(head)
        }

        fn request_decoder(&self, request: &Ask) -> Box<dyn Decoder<Jev>> {
            V1.request_decoder(request)
        }

        fn request_encoder(
            &self,
            client: Option<&Ask>,
            body: &mut Bytes,
            streams: bool,
        ) -> Box<dyn Encoder<Jev, Ask>> {
            V1.request_encoder(client, body, streams)
        }

        fn response_decoder(
            &self,
            head: &Head,
            call: &Ask,
            sent: &Bytes,
        ) -> Box<dyn Decoder<Answer>> {
            V1.response_decoder(head, call, sent)
        }

        fn response_encoder(
            &self,
            answer: Option<&Head>,
            ask: &Ask,
            body: &Bytes,
            refusal: Option<&Fault>,
        ) -> Box<dyn Encoder<Answer, Head>> {
            self.0
                .lock()
                .map(|mut h| *h = Some(formats::answer_head(answer).headers))
                .ok();
            V1.response_encoder(answer, ask, body, refusal)
        }
    }

    let mock = MockTransport::new();
    let noting = Noting::default();
    let v2 = [add_provider("v2", V2, mock.at("http://v2.test"))];
    let gw = serving(noting.clone(), v2, Vec::new(), Some("v2")).unwrap_or_else(|e| panic!("{e}"));
    let mut answer = mock::json(StatusCode::OK, r#"{"text":"hi"}"#);
    let headers = answer.headers_mut();
    headers.insert("x-jev-trace", HeaderValue::from_static("t1"));
    headers.insert("retry-after", HeaderValue::from_static("7"));
    mock.reply("http://v2.test", answer);

    let (status, headers, body) =
        read_whole(serve(&gw, to("jev-v1", r#"{"input":"hi"}"#)).await.0).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body.ok().as_deref(), Some(&br#"{"output":"hi"}"#[..]));
    let given = noting
        .0
        .lock()
        .map(|h| h.clone())
        .ok()
        .flatten()
        .unwrap_or_default();
    // It describes the provider's protocol, which is not the client's.
    assert!(given.get("x-jev-trace").is_none());
    assert!(given.get("retry-after").is_none());
    assert!(headers.get("x-jev-trace").is_none());
}

// A message that is no content, such as an SSE comment, reaches filters, and
// goes out where it came when they pass it on.
#[tokio::test]
async fn a_keep_alive_reaches_filters_and_goes_where_it_came() {
    /// Counts the keep-alives it is shown, passing everything on.
    #[derive(Clone, Default)]
    struct Counts(Arc<std::sync::atomic::AtomicUsize>);

    #[async_trait]
    impl Plugin for Counts {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let seen = self.0.clone();
            cx.filter(move |a: &mut Answer| {
                if a.text.is_none() {
                    seen.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                Ok(())
            })?;
            Ok(None)
        }
    }

    let mock = MockTransport::new();
    let counts = Counts::default();
    let gw = jev_relay(counts.clone(), &mock);
    let stream = [
        "data: {\"text\":\"a\"}\n\n",
        ": keep-alive\n\n",
        "data: {\"text\":\"b\"}\n\n",
    ];
    mock.reply("http://v2.test", mock::sse(&stream));

    let (resp, ended) = serve(&gw, to("jev-v2", r#"{"prompt":"hi"}"#)).await;
    let (_, _, body) = read_whole(resp).await;

    assert_eq!(body.ok().as_deref(), Some(stream.concat().as_bytes()));
    assert_eq!(ended.await.end, Ok(()));
    assert_eq!(counts.0.load(std::sync::atomic::Ordering::Relaxed), 1);
}
