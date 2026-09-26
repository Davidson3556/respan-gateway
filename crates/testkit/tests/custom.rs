//! A user-defined kind with two formats, unknown to the kernel.

use bytes::Bytes;
use http::header::CONTENT_TYPE;
use http::{HeaderValue, StatusCode};
use respan_gateway_core::async_trait;
use respan_gateway_core::serde_json;
use respan_gateway_core::{
    Closed, Context, Decoder, Doc, Encoder, Fault, Filter, Format, Gateway, Item, Kind, Outcome,
    Part, Plugin, Provider, RequestView, Response, Route,
};
use respan_gateway_core::{bytes, futures, http};
use respan_gateway_formats::whole::Relay;
use respan_gateway_formats::{self as formats, default_content_type, sse};
use respan_gateway_testkit::{MockTransport, Outcomes, collect, mock, post};
use serde_json::{Value, json};

/// The jev kind's request: a prompt in, an answer out.
#[derive(Clone, PartialEq)]
struct Jev {
    prompt: String,
    /// Fields of the body jev has no place for, by name.
    unknown: Vec<String>,
}

/// A jev response.
#[derive(Clone, PartialEq)]
struct Answer {
    text: String,
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
    fn name(&self) -> &str {
        self.name
    }

    fn model(&self, _head: &http::request::Parts, body: &Doc) -> Option<String> {
        formats::json_model(body)
    }

    fn prepare(
        &self,
        mut body: Doc,
        client: RequestView<'_>,
        _provider: &http::HeaderMap,
    ) -> Result<http::Request<Doc>, Fault> {
        formats::write_model(&mut body, client.model())?;
        let req = http::Request::post("/complete").header(CONTENT_TYPE, "application/json");
        Ok(req.body(body)?)
    }

    fn decoder(&self, head: &http::response::Parts, _client: RequestView<'_>) -> Box<dyn Decoder> {
        // A jev answer is whole, or a stream of one-answer events.
        let answer = self.answer;
        sse::at_eof(sse::or_whole(
            head,
            move |raw: Bytes, out: &mut Vec<Part>| {
                let v = formats::json(&raw);
                // An error inside a success is the provider's, relayed.
                if let Some(said) = v.as_ref().and_then(|v| v["error"]["message"].as_str()) {
                    let fault = Fault::upstream(said).relayed();
                    out.extend([Part::Frame(raw), Part::End(Err(fault))]);
                    return Ok(());
                }
                let text = v.as_ref().and_then(|v| v[answer].as_str());
                let text = text.ok_or_else(|| Fault::upstream("not a jev answer"))?;
                let answer = Answer { text: text.into() };
                out.push(Item::decoded(vec![answer], raw).into());
                Ok(())
            },
        ))
    }

    fn encoder(
        &self,
        head: &mut http::response::Parts,
        _client: RequestView<'_>,
        _relayed: bool,
    ) -> Box<dyn Encoder> {
        default_content_type(head, "application/json");
        // Other answers and failures are written in jev: in a stream, as an
        // event shaped like the original.
        let (key, stream) = (self.answer, sse::is_stream(head));
        let frame = move |like: Option<&Bytes>, v: Value| match stream {
            true => sse::event_as(like, None, v),
            false => v.to_string(),
        };
        let write = move |answers: &[Answer], like: Option<&Bytes>| {
            let text: String = answers.iter().map(|a| a.text.as_str()).collect();
            Ok(frame(like, json!({ key: text })))
        };
        let fail = move |f: &Fault| frame(None, json!({ "error": f.message() }));
        Box::new(Relay::new(write, fail, stream))
    }

    fn kind(&self) -> Option<Kind> {
        let this = *self;
        Some(Kind::new::<Jev, Answer>(
            move |head, body| this.decode(head, body),
            move |jev, original| this.encode(jev, original),
        ))
    }
}

impl JevFormat {
    /// A body with no prompt is not jev at all; other fields are noted.
    fn decode(&self, _head: &http::request::Parts, body: &Doc) -> Result<Jev, Fault> {
        let v = body.json().and_then(Value::as_object);
        let prompt = v
            .and_then(|v| v.get(self.prompt)?.as_str())
            .ok_or_else(|| Fault::bad_request("no prompt"))?;
        let known = [self.prompt, "model"];
        let unknown = v.into_iter().flatten().map(|(k, _)| k);
        Ok(Jev {
            prompt: prompt.to_owned(),
            unknown: unknown
                .filter(|k| !known.contains(&k.as_str()))
                .cloned()
                .collect(),
        })
    }

    /// The client's body with the prompt replaced, else a new body if no
    /// field is lost.
    fn encode(&self, jev: &Jev, original: Option<(&Doc, &Jev)>) -> Result<Doc, Fault> {
        match original.and_then(|(body, _)| body.json()) {
            Some(v) => {
                let mut v = v.clone();
                v[self.prompt] = jev.prompt.clone().into();
                Ok(Doc::from(v))
            }
            None if jev.unknown.is_empty() => Ok(Doc::from(json!({ self.prompt: jev.prompt }))),
            None => Err(Fault::bad_request(format!(
                "'{}' cannot be translated",
                jev.unknown[0]
            ))),
        }
    }
}

/// Works on any jev format through the kind's types.
struct Polite;

#[async_trait]
impl Plugin for Polite {
    async fn on_request(&self, cx: &mut Context) -> Result<Option<Response>, Fault> {
        let jev = cx.request_mut::<Jev>()?;
        jev.prompt = format!("please, {}", jev.prompt);
        cx.filter(|a: &mut Answer| {
            a.text.push_str(", thanks");
            Ok(())
        })?;
        Ok(None)
    }
}

#[tokio::test]
async fn a_kind_the_kernel_has_never_seen_translates_between_its_formats() {
    let mock = MockTransport::new();
    let gw = Gateway::new(
        mock.clone(),
        [Provider::new("v2", V2, "http://v2.test")],
        [Route::new("v1", V1).with_target("v2")],
    )
    .unwrap_or_else(|e| panic!("{e}"));
    mock.reply(
        "http://v2.test",
        mock::json(StatusCode::OK, r#"{"text":"hello"}"#),
    );

    let request = r#"{"input":"hi","model":"j1"}"#;
    let (_, _, body) = collect(gw.handle("v1", post(request)).await).await;

    let sent: Value = serde_json::from_slice(mock.sent()[0].body()).unwrap_or_default();
    assert_eq!(sent, json!({ "prompt": "hi", "model": "j1" }));
    assert_eq!(body.ok().as_deref(), Some(&br#"{"output":"hello"}"#[..]));
}

#[tokio::test]
async fn a_field_jev_has_no_place_for_is_kept_or_refused() {
    let mock = MockTransport::new();
    let gw = Gateway::new(
        mock.clone(),
        [
            Provider::new("v1", V1, "http://v1.test"),
            Provider::new("v2", V2, "http://v2.test"),
        ],
        [
            Route::new("same", V2).with_plugin(Polite).with_target("v2"),
            Route::new("other", V2).with_target("v1"),
        ],
    )
    .unwrap_or_else(|e| panic!("{e}"));
    mock.reply(
        "http://v2.test",
        mock::json(StatusCode::OK, r#"{"text":"hello"}"#),
    );
    let request = r#"{"prompt":"hi","seed":7}"#;

    // Untranslated, the edited body keeps the field.
    let _ = collect(gw.handle("same", post(request)).await).await;
    let sent: Value = serde_json::from_slice(mock.sent()[0].body()).unwrap_or_default();
    assert_eq!(sent, json!({ "prompt": "please, hi", "seed": 7 }));

    // Translation would lose it, so it is refused.
    let (status, _, _) = collect(gw.handle("other", post(request)).await).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(mock.sent().len(), 1);
}

#[tokio::test]
async fn a_plugin_works_on_every_format_of_its_kind() {
    let mock = MockTransport::new();
    let gw = Gateway::new(
        mock.clone(),
        [Provider::new("v2", V2, "http://v2.test")],
        [Route::new("v1", V1).with_plugin(Polite).with_target("v2")],
    )
    .unwrap_or_else(|e| panic!("{e}"));
    mock.reply(
        "http://v2.test",
        mock::json(StatusCode::OK, r#"{"text":"hello"}"#),
    );

    let (_, _, body) = collect(gw.handle("v1", post(r#"{"input":"hi"}"#)).await).await;

    let sent: Value = serde_json::from_slice(mock.sent()[0].body()).unwrap_or_default();
    assert_eq!(sent, json!({ "prompt": "please, hi" }));
    assert_eq!(
        body.ok().as_deref(),
        Some(&br#"{"output":"hello, thanks"}"#[..])
    );
}

#[tokio::test]
async fn a_target_of_another_kind_does_not_build() {
    let openai = || {
        Provider::new(
            "openai",
            respan_gateway_formats::OpenAiChat,
            "http://o.test",
        )
    };
    let err = Gateway::new(
        MockTransport::new(),
        [openai()],
        [Route::new("v1", V1).with_target("openai")],
    )
    .err();
    assert_eq!(
        err.as_ref().map(Fault::message),
        Some(
            "route 'v1': it speaks jev-v1, provider 'openai' speaks openai-chat, and they share no kind"
        )
    );
}

#[tokio::test]
async fn a_target_a_plugin_may_set_is_checked_on_asking() {
    let openai = Provider::new(
        "openai",
        respan_gateway_formats::OpenAiChat,
        "http://o.test",
    );
    let gw = Gateway::new(
        MockTransport::new(),
        [openai, Provider::new("v2", V2, "http://v2.test")],
        [Route::new("v1", V1).with_plugin(Polite)],
    )
    .expect("builds");
    assert!(gw.check_target("v1", "v2").is_ok());
    let err = |provider| {
        gw.check_target("v1", provider)
            .err()
            .map(|f| f.message().to_owned())
    };
    assert!(err("openai").is_some_and(|m| m.contains("share no kind")));
    assert!(err("nope").is_some_and(|m| m.contains("unknown provider 'nope'")));
}

#[tokio::test]
async fn a_call_to_a_provider_of_another_kind_is_refused_when_made() {
    struct Stray;

    #[async_trait]
    impl Plugin for Stray {
        async fn on_request(&self, cx: &mut Context) -> Result<Option<Response>, Fault> {
            Ok(Some(cx.call("openai").await?))
        }
    }

    let mock = MockTransport::new();
    let openai = Provider::new(
        "openai",
        respan_gateway_formats::OpenAiChat,
        "http://o.test",
    );
    let gw = Gateway::new(
        mock.clone(),
        [openai],
        [Route::new("v1", V1).with_plugin(Stray)],
    )
    .expect("builds");

    let resp = gw.handle("v1", post(r#"{"input":"hi"}"#)).await;
    let closed = Closed::of(&resp);
    let (status, _, body) = collect(resp).await;

    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    let body = String::from_utf8(body.unwrap_or_default().to_vec()).unwrap_or_default();
    assert!(body.contains("cannot use"), "{body}");
    let end = closed.await.map(|o| o.end.map_err(|f| f.code().to_owned()));
    assert_eq!(
        end,
        Some(Err(respan_gateway_core::codes::CONFIG.to_owned()))
    );
    assert!(mock.sent().is_empty(), "nothing left for the provider");
}

#[tokio::test]
async fn a_format_that_panics_reading_the_model_is_a_plain_500_with_no_outcome() {
    struct Broken;

    impl Format for Broken {
        fn name(&self) -> &str {
            "broken"
        }

        fn model(&self, _head: &http::request::Parts, _body: &Doc) -> Option<String> {
            panic!("a buggy format")
        }

        fn prepare(
            &self,
            body: Doc,
            _client: RequestView<'_>,
            _provider: &http::HeaderMap,
        ) -> Result<http::Request<Doc>, Fault> {
            Ok(http::Request::new(body))
        }

        fn decoder(
            &self,
            head: &http::response::Parts,
            _client: RequestView<'_>,
        ) -> Box<dyn Decoder> {
            formats::frames_decoder(head)
        }

        fn encoder(
            &self,
            head: &mut http::response::Parts,
            _client: RequestView<'_>,
            relayed: bool,
        ) -> Box<dyn Encoder> {
            formats::frames_encoder(head, relayed)
        }
    }

    let gw = Gateway::new(
        MockTransport::new(),
        [Provider::new("b", Broken, "http://b.test")],
        [Route::new("broken", Broken).with_target("b")],
    )
    .expect("builds");

    let resp = gw.handle("broken", post("ping")).await;
    let closed = Closed::of(&resp);
    let (status, headers, _) = collect(resp).await;

    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(headers["content-type"], "text/plain; charset=utf-8");
    assert!(closed.await.is_none());
}

#[tokio::test]
async fn a_plugin_for_another_kind_is_refused_not_ignored() {
    struct Redact;

    #[async_trait]
    impl Plugin for Redact {
        async fn on_request(&self, cx: &mut Context) -> Result<Option<Response>, Fault> {
            cx.filter(|_: &mut respan_gateway_formats::conversation::Event| Ok(()))?;
            Ok(None)
        }
    }

    let gw = Gateway::new(
        MockTransport::new(),
        [Provider::new("v2", V2, "http://v2.test")],
        [Route::new("v2", V2).with_plugin(Redact).with_target("v2")],
    )
    .unwrap_or_else(|e| panic!("{e}"));

    let (status, _, body) = collect(gw.handle("v2", post(r#"{"prompt":"hi"}"#)).await).await;

    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    let body = String::from_utf8_lossy(&body.unwrap_or_default()).into_owned();
    assert!(body.contains("another kind"), "{body}");
}

#[tokio::test]
async fn a_plugin_made_response_of_another_kind_fails_loudly() {
    struct Wrong;

    #[async_trait]
    impl Plugin for Wrong {
        async fn on_request(&self, _cx: &mut Context) -> Result<Option<Response>, Fault> {
            let wrong = [Ok(respan_gateway_formats::conversation::Event::text(
                0, "hi",
            ))];
            Ok(Some(Response::stream(http::Response::new(
                futures::stream::iter(wrong),
            ))))
        }
    }

    let gw = Gateway::new(
        MockTransport::new(),
        [],
        [Route::new("v2", V2).with_plugin(Wrong)],
    )
    .unwrap_or_else(|e| panic!("{e}"));

    let (_, _, body) = collect(gw.handle("v2", post(r#"{"prompt":"hi"}"#)).await).await;

    let body = String::from_utf8_lossy(&body.unwrap_or_default()).into_owned();
    assert!(body.contains("another kind"), "{body}");
}

#[tokio::test]
async fn a_plugin_asking_for_another_kind_is_told_so_before_decoding() {
    struct Chatty;

    #[async_trait]
    impl Plugin for Chatty {
        async fn on_request(&self, cx: &mut Context) -> Result<Option<Response>, Fault> {
            let asked = cx.request::<respan_gateway_formats::conversation::Conversation>();
            // Its code sets it apart from an undecodable body.
            let code = asked.as_ref().err().map(|f| f.code().to_owned());
            assert_eq!(
                code.as_deref(),
                Some(respan_gateway_core::codes::WRONG_KIND)
            );
            asked?;
            Ok(None)
        }
    }

    let gw = Gateway::new(
        MockTransport::new(),
        [],
        [Route::new("v2", V2).with_plugin(Chatty)],
    )
    .unwrap_or_else(|e| panic!("{e}"));

    // The plugin's kind, not the bad body, is the fault.
    let (status, _, body) = collect(gw.handle("v2", post("not json")).await).await;

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
        async fn on_request(&self, cx: &mut Context) -> Result<Option<Response>, Fault> {
            let jev = cx.request_mut::<Jev>()?;
            jev.prompt = jev.prompt.clone();
            Ok(None)
        }
    }

    let mock = MockTransport::new();
    let gw = Gateway::new(
        mock.clone(),
        [Provider::new("v2", V2, "http://v2.test")],
        [Route::new("v2", V2).with_plugin(Look).with_target("v2")],
    )
    .unwrap_or_else(|e| panic!("{e}"));
    mock.reply(
        "http://v2.test",
        mock::json(StatusCode::OK, r#"{"text":"hello"}"#),
    );
    // Bytes a re-encoding would not reproduce.
    let request = r#"{"prompt":"hi",  "seed":7}"#;

    let _ = collect(gw.handle("v2", post(request)).await).await;

    assert_eq!(&mock.sent()[0].body()[..], request.as_bytes());
}

#[test]
fn formats_of_one_name_and_different_kinds_do_not_build() {
    // A jev format named like OpenAI's.
    let impostor = JevFormat {
        name: "openai-chat",
        ..V1
    };
    let err = Gateway::new(
        MockTransport::new(),
        [Provider::new("jev", impostor, "http://jev.test")],
        [Route::new("chat", respan_gateway_formats::OpenAiChat).with_target("jev")],
    )
    .err();
    assert_eq!(
        err.as_ref().map(Fault::message),
        Some("two formats named openai-chat are of different kinds")
    );
}

#[tokio::test]
async fn a_format_of_no_kind_relays_and_has_no_request_type() {
    /// A protocol the gateway only passes on.
    struct Opaque;

    impl Format for Opaque {
        fn name(&self) -> &str {
            "opaque"
        }

        fn model(&self, _head: &http::request::Parts, _body: &Doc) -> Option<String> {
            None
        }

        fn prepare(
            &self,
            body: Doc,
            client: RequestView<'_>,
            _provider: &http::HeaderMap,
        ) -> Result<http::Request<Doc>, Fault> {
            let mut req = http::Request::new(body);
            *req.method_mut() = http::Method::POST;
            formats::pass_headers(&mut req, client, &[""]);
            Ok(req)
        }

        fn decoder(
            &self,
            head: &http::response::Parts,
            _client: RequestView<'_>,
        ) -> Box<dyn Decoder> {
            formats::frames_decoder(head)
        }

        fn encoder(
            &self,
            head: &mut http::response::Parts,
            _client: RequestView<'_>,
            relayed: bool,
        ) -> Box<dyn Encoder> {
            formats::frames_encoder(head, relayed)
        }
    }

    /// A guardrail that asks for the jev request.
    struct Guard;

    /// A rate limit, refusing every request.
    struct Limit;

    #[async_trait]
    impl Plugin for Limit {
        async fn on_request(&self, _cx: &mut Context) -> Result<Option<Response>, Fault> {
            Err(Fault::new(
                StatusCode::TOO_MANY_REQUESTS,
                "limited",
                "slow down",
            ))
        }
    }

    #[async_trait]
    impl Plugin for Guard {
        async fn on_request(&self, cx: &mut Context) -> Result<Option<Response>, Fault> {
            cx.request::<Jev>()?;
            Ok(None)
        }
    }

    let mock = MockTransport::new();
    let gw = Gateway::new(
        mock.clone(),
        [
            Provider::new("opaque", Opaque, "http://opaque.test").with_headers(
                http::HeaderMap::from_iter([(
                    http::HeaderName::from_static("x-key"),
                    HeaderValue::from_static("provider"),
                )]),
            ),
        ],
        [
            Route::new("open", Opaque).with_target("opaque"),
            Route::new("guarded", Opaque)
                .with_plugin(Guard)
                .with_target("opaque"),
            Route::new("limited", Opaque)
                .with_plugin(Limit)
                .with_target("opaque"),
        ],
    )
    .unwrap_or_else(|e| panic!("{e}"));
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
    // and encoding ones.
    let mut ping = post("ping");
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
    let (_, head, body) = collect(gw.handle("open", ping).await).await;
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
    assert_eq!(sent.headers()["te"], "trailers");
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
    let resp = gw.handle("open", post("ping")).await;
    let closed = Closed::of(&resp);
    let (status, _, body) = collect(resp).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body.ok().as_deref(), Some(&b"busy"[..]));
    let end = closed.await.map(|o| o.end.map_err(|f| f.status()));
    assert_eq!(end, Some(Err(StatusCode::SERVICE_UNAVAILABLE)));

    // A body cut short upstream is cut short for the client.
    let (status, _, body) = collect(gw.handle("open", post("ping")).await).await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.is_err(), "{body:?}");

    // A refusal says why, in plain text.
    let (status, headers, body) = collect(gw.handle("limited", post("ping")).await).await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(headers["content-type"], "text/plain; charset=utf-8");
    assert_eq!(body.ok().as_deref(), Some(&b"slow down"[..]));

    let (status, _, _) = collect(gw.handle("guarded", post("ping")).await).await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(mock.sent().len(), 3, "the guarded request never left");
}

#[tokio::test]
async fn a_format_that_writes_errors_as_200_is_refused_once() {
    /// Jev over a protocol that puts errors in a 200, as JSON-RPC does.
    struct InBand;

    impl Format for InBand {
        fn name(&self) -> &str {
            V2.name()
        }

        fn model(&self, head: &http::request::Parts, body: &Doc) -> Option<String> {
            V2.model(head, body)
        }

        fn prepare(
            &self,
            body: Doc,
            client: RequestView<'_>,
            provider: &http::HeaderMap,
        ) -> Result<http::Request<Doc>, Fault> {
            V2.prepare(body, client, provider)
        }

        fn decoder(
            &self,
            head: &http::response::Parts,
            client: RequestView<'_>,
        ) -> Box<dyn Decoder> {
            V2.decoder(head, client)
        }

        fn encoder(
            &self,
            head: &mut http::response::Parts,
            client: RequestView<'_>,
            relayed: bool,
        ) -> Box<dyn Encoder> {
            head.status = StatusCode::OK;
            V2.encoder(head, client, relayed)
        }

        fn kind(&self) -> Option<Kind> {
            V2.kind()
        }
    }

    struct Deny;

    #[async_trait]
    impl Plugin for Deny {
        async fn on_request(&self, _cx: &mut Context) -> Result<Option<Response>, Fault> {
            Err(Fault::new(StatusCode::FORBIDDEN, "denied", "no"))
        }
    }

    let gw = Gateway::new(
        MockTransport::new(),
        [],
        [Route::new("rpc", InBand).with_plugin(Deny)],
    )
    .unwrap_or_else(|e| panic!("{e}"));

    let (status, _, _) = collect(gw.handle("rpc", post(r#"{"prompt":"hi"}"#)).await).await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn a_format_that_panics_building_its_encoder_is_a_plain_500() {
    struct Broken;

    impl Format for Broken {
        fn name(&self) -> &str {
            V2.name()
        }

        fn model(&self, head: &http::request::Parts, body: &Doc) -> Option<String> {
            V2.model(head, body)
        }

        fn prepare(
            &self,
            body: Doc,
            client: RequestView<'_>,
            provider: &http::HeaderMap,
        ) -> Result<http::Request<Doc>, Fault> {
            V2.prepare(body, client, provider)
        }

        fn decoder(
            &self,
            head: &http::response::Parts,
            client: RequestView<'_>,
        ) -> Box<dyn Decoder> {
            V2.decoder(head, client)
        }

        fn encoder(
            &self,
            _: &mut http::response::Parts,
            _: RequestView<'_>,
            _: bool,
        ) -> Box<dyn Encoder> {
            panic!("a buggy format")
        }

        fn kind(&self) -> Option<Kind> {
            V2.kind()
        }
    }

    let log = Outcomes::default();
    let gw = Gateway::new(
        MockTransport::new(),
        [],
        [Route::new("v2", Broken).with_plugin(log.clone())],
    )
    .unwrap_or_else(|e| panic!("{e}"));

    let (status, headers, _) = collect(gw.handle("v2", post(r#"{"prompt":"hi"}"#)).await).await;
    gw.idle().await;

    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(headers["content-type"], "text/plain; charset=utf-8");
    let end = log.only().end;
    assert!(matches!(end, Err(f) if f.code() == respan_gateway_core::codes::INTERNAL));
}

#[tokio::test]
async fn a_providers_error_body_is_relayed_once() {
    let mock = MockTransport::new();
    let gw = Gateway::new(
        mock.clone(),
        [Provider::new("v2", V2, "http://v2.test")],
        [Route::new("v2", V2).with_target("v2")],
    )
    .expect("builds");
    let refusal = r#"{"error":"no"}"#;
    mock.reply(
        "http://v2.test",
        mock::json(StatusCode::TOO_MANY_REQUESTS, refusal),
    );

    let (status, _, body) = collect(gw.handle("v2", post(r#"{"prompt":"hi"}"#)).await).await;

    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(body.ok().as_deref(), Some(refusal.as_bytes()));
}

#[tokio::test]
async fn a_jev_stream_is_relayed_and_edited_event_by_event() {
    /// Drops the answer `b`.
    struct Mute;

    #[async_trait]
    impl Plugin for Mute {
        async fn on_request(&self, cx: &mut Context) -> Result<Option<Response>, Fault> {
            struct NoB;
            impl Filter<Answer> for NoB {
                fn event(&mut self, a: Answer, out: &mut Vec<Answer>) -> Result<(), Fault> {
                    if a.text != "b" {
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
    let gw = Gateway::new(
        mock.clone(),
        [Provider::new("v2", V2, "http://v2.test")],
        [
            Route::new("plain", V2).with_target("v2"),
            Route::new("polite", V2)
                .with_plugin(Polite)
                .with_target("v2"),
            Route::new("drop", V2).with_plugin(Mute).with_target("v2"),
        ],
    )
    .unwrap_or_else(|e| panic!("{e}"));
    let stream = [
        "id: 1\ndata: {\"text\":\"a\"}\n\n",
        "data: {\"text\":\"b\"}\n\n",
    ];
    mock.reply("http://v2.test", mock::sse(&stream))
        .reply("http://v2.test", mock::sse(&stream))
        .reply("http://v2.test", mock::sse(&stream));

    let (_, _, plain) = collect(gw.handle("plain", post(r#"{"prompt":"hi"}"#)).await).await;
    let (_, _, polite) = collect(gw.handle("polite", post(r#"{"prompt":"hi"}"#)).await).await;
    let (_, _, dropped) = collect(gw.handle("drop", post(r#"{"prompt":"hi"}"#)).await).await;

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
        async fn on_request(&self, cx: &mut Context) -> Result<Option<Response>, Fault> {
            struct Sign;
            impl Filter<Answer> for Sign {
                fn event(&mut self, a: Answer, out: &mut Vec<Answer>) -> Result<(), Fault> {
                    out.push(a);
                    Ok(())
                }
                fn end(&mut self, _: &Outcome, out: &mut Vec<Answer>) -> Result<(), Fault> {
                    out.push(Answer { text: "!".into() });
                    Ok(())
                }
            }
            cx.filter(Sign)?;
            Ok(None)
        }
    }

    let mock = MockTransport::new();
    let gw = Gateway::new(
        mock.clone(),
        [Provider::new("v2", V2, "http://v2.test")],
        [
            Route::new("v1", V1).with_target("v2"),
            Route::new("tail", V2).with_plugin(Tail).with_target("v2"),
        ],
    )
    .unwrap_or_else(|e| panic!("{e}"));
    let stream = ["data: {\"text\":\"a\"}\n\n", "data: {\"text\":\"b\"}\n\n"];
    mock.reply("http://v2.test", mock::sse(&stream)).reply(
        "http://v2.test",
        mock::json(StatusCode::OK, r#"{"text":"hi"}"#),
    );

    let (_, _, v1) = collect(gw.handle("v1", post(r#"{"input":"hi"}"#)).await).await;
    let (_, _, tail) = collect(gw.handle("tail", post(r#"{"prompt":"hi"}"#)).await).await;

    assert_eq!(v1.ok().as_deref(), Some(&br#"{"output":"ab"}"#[..]));
    assert_eq!(tail.ok().as_deref(), Some(&br#"{"text":"hi!"}"#[..]));
}

#[tokio::test]
async fn a_redirect_is_passed_on_as_it_came_not_read_as_an_answer() {
    let mock = MockTransport::new();
    let gw = Gateway::new(
        mock.clone(),
        [Provider::new("v2", V2, "http://v2.test")],
        [Route::new("polite", V2)
            .with_plugin(Polite)
            .with_target("v2")],
    )
    .unwrap_or_else(|e| panic!("{e}"));
    let moved = http::Response::builder()
        .status(StatusCode::MOVED_PERMANENTLY)
        .header("content-type", "text/html")
        .body(vec![Ok(Bytes::from("<html>moved</html>"))])
        .unwrap_or_default();
    mock.reply("http://v2.test", moved);

    let resp = gw.handle("polite", post(r#"{"prompt":"hi"}"#)).await;
    let closed = Closed::of(&resp);
    let (status, headers, body) = collect(resp).await;

    assert_eq!(status, StatusCode::MOVED_PERMANENTLY);
    assert_eq!(headers["content-type"], "text/html");
    assert_eq!(body.ok().as_deref(), Some(&b"<html>moved</html>"[..]));
    let end = closed.await.map(|o| o.end.map_err(|f| f.status()));
    assert_eq!(end, Some(Err(StatusCode::MOVED_PERMANENTLY)));
}

#[tokio::test]
async fn a_user_formats_failure_keeps_the_providers_words() {
    let mock = MockTransport::new();
    let gw = Gateway::new(
        mock.clone(),
        [Provider::new("v2", V2, "http://v2.test")],
        [Route::new("v2", V2).with_target("v2")],
    )
    .unwrap_or_else(|e| panic!("{e}"));
    let error = r#"{"error":{"type":"invalid_request_error","message":"bad prompt"}}"#;
    mock.reply("http://v2.test", mock::json(StatusCode::BAD_REQUEST, error));

    let resp = gw.handle("v2", post(r#"{"prompt":"hi"}"#)).await;
    let closed = Closed::of(&resp);
    let (status, _, body) = collect(resp).await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body.ok().as_deref(), Some(error.as_bytes()));
    let end = closed.await.map(|o| {
        o.end
            .map_err(|f| (f.code().to_owned(), f.message().to_owned()))
    });
    let said = ("invalid_request_error".to_owned(), "bad prompt".to_owned());
    assert_eq!(end, Some(Err(said)));
}

#[tokio::test]
async fn a_jev_stream_keeps_its_comments_and_says_a_providers_error() {
    let mock = MockTransport::new();
    let gw = Gateway::new(
        mock.clone(),
        [Provider::new("v2", V2, "http://v2.test")],
        [Route::new("v2", V2).with_target("v2")],
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
        let resp = gw.handle("v2", post(r#"{"prompt":"hi"}"#)).await;
        let closed = Closed::of(&resp);
        let (status, _, body) = collect(resp).await;
        let end = closed
            .await
            .map(|o| o.end.map_err(|f| f.message().to_owned()));
        assert_eq!(status, StatusCode::OK, "relayed as it came");
        ends.push((body.unwrap_or_default(), end));
    }

    // SSE comments pass through; a provider's error is relayed whole.
    let overloaded = Some(Err("overloaded".to_owned()));
    assert_eq!(ends[0], (Bytes::from(kept.concat()), Some(Ok(()))));
    assert_eq!(ends[1], (Bytes::from(failed.concat()), overloaded.clone()));
    let whole = Bytes::from_static(br#"{"error":{"message":"overloaded"}}"#);
    assert_eq!(ends[2], (whole, overloaded));
}

#[tokio::test]
async fn a_jev_error_inside_a_success_translated_is_refused_in_the_clients_words() {
    let mock = MockTransport::new();
    let gw = Gateway::new(
        mock.clone(),
        [Provider::new("v2", V2, "http://v2.test")],
        [Route::new("v1", V1).with_target("v2")],
    )
    .unwrap_or_else(|e| panic!("{e}"));
    mock.reply(
        "http://v2.test",
        mock::json(StatusCode::OK, r#"{"error":{"message":"overloaded"}}"#),
    );

    let (status, _, body) = collect(gw.handle("v1", post(r#"{"input":"hi"}"#)).await).await;

    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert_eq!(
        body.ok().as_deref(),
        Some(&br#"{"error":"overloaded"}"#[..])
    );
}
