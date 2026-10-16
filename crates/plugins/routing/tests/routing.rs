//! The router's promises, against a mock provider.

use respan_gateway_conversation::Input;
use std::sync::{Arc, Mutex, PoisonError};

use mockllm::{Answer, FakeOpenAi};
use respan_gateway_conversation_http::openai::OpenAiChat;
use respan_gateway_core::futures::FutureExt;
use respan_gateway_core::futures::future::BoxFuture;
use respan_gateway_core::{ClientContext, Fault, Gateway, Plugin, Provider, Response, async_trait};
use respan_gateway_http::Body;
use respan_gateway_http::http::{self, HeaderMap, HeaderValue, StatusCode, header::AUTHORIZATION};
use respan_gateway_http_transport::HttpTransport;
use respan_gateway_passthrough_http::Passthrough;
use respan_gateway_plugin_routing::{
    Candidate, Dispatch, Model, Pick, Routes, Routing, Rule, Speaks,
};

/// An end by its message, for comparing: `Fault` has no equality.
fn ended(end: Result<(), Fault>) -> Result<(), String> {
    end.map_err(|f| f.message().to_owned())
}

#[allow(dead_code)] // The shared helper also exposes streamed requests, unused here.
#[path = "../../../../tests/support/http.rs"]
mod http_support;
use http_support::{LIMIT, post, read_whole, serve};

type Answered = respan_gateway_http::Sent;

const A: &str = "http://a.test/v1";
const B: &str = "http://b.test/v1";

struct Fakes {
    a: FakeOpenAi,
    b: FakeOpenAi,
    sent: Arc<Mutex<Vec<String>>>,
}

impl Fakes {
    async fn start() -> Self {
        Self {
            a: FakeOpenAi::start().await,
            b: FakeOpenAi::start().await,
            sent: Arc::default(),
        }
    }

    fn reply(&self, provider: &str, answer: Answer) -> &Self {
        match provider {
            A => self.a.answer(answer),
            B => self.b.answer(answer),
            _ => panic!("unknown provider fixture {provider}"),
        }
        self
    }

    fn at(
        &self,
        provider: &str,
    ) -> impl Fn(http::Request<Body>) -> BoxFuture<'static, Answered> + Clone + Send + Sync + 'static
    {
        let (fake, label) = match provider {
            A => (&self.a, "a.test"),
            B => (&self.b, "b.test"),
            _ => panic!("unknown provider fixture {provider}"),
        };
        let mut headers = HeaderMap::new();
        headers.insert(AUTHORIZATION, HeaderValue::from_static("Bearer sk-test"));
        let http = HttpTransport::new(fake.base_url())
            .and_then(|transport| transport.with_headers(headers))
            .unwrap_or_else(|fault| panic!("{fault}"));
        let sent = self.sent.clone();
        move |call| {
            let http = http.clone();
            let sent = sent.clone();
            async move {
                sent.lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .push(label.to_owned());
                http.send(call).await
            }
            .boxed()
        }
    }

    fn sent(&self) -> Vec<String> {
        self.sent
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

const CHAT: &str = "chat";

/// Providers `a` and `b` of OpenAI Chat, `b` the target, and `routes`.
fn model_gateway(mock: &Fakes, routes: Routes) -> Gateway {
    routed_gateway(mock, routes).0
}

/// [`model_gateway`], dispatching what a rule routes, and its routes.
fn routed_gateway(mock: &Fakes, routes: Routes) -> (Gateway, Arc<Routes>) {
    let speaks = |internal: &str| {
        vec![Speaks {
            format: "openai-chat".into(),
            kind: CHAT,
            internal: internal.into(),
        }]
    };
    let routes = routes.provider("a", speaks("a")).provider("b", speaks("b"));
    let routes = Arc::new(routes.target("b"));
    let routing = Routing::new(routes.clone()).format(
        "openai-chat",
        CHAT,
        Model::of(Input::model, Input::set_model),
    );
    let gateway = Gateway::new()
        .client::<Input, _, _>(OpenAiChat)
        .expect("chat client")
        .provider(Provider::new(
            "a",
            OpenAiChat,
            respan_gateway_http::connect(mock.at(A), LIMIT),
        ))
        .expect("a provider")
        .provider(Provider::new(
            "b",
            OpenAiChat,
            respan_gateway_http::connect(mock.at(B), LIMIT),
        ))
        .expect("b provider")
        .plugin(routing)
        .plugin(Dispatch::new(|_, _| true));
    (gateway, routes)
}

async fn routed_post(gw: &Gateway, body: &'static str) -> (StatusCode, String) {
    let (resp, outcome) = serve(gw, post("/v1/chat/completions", body)).await;
    let (status, _, bytes) = read_whole(resp).await;
    let text = String::from_utf8_lossy(&bytes.expect("response body")).into_owned();
    assert_eq!(ended(outcome.await), Ok(()));
    (status, text)
}

#[tokio::test]
async fn routing_sends_a_named_provider_its_model_alone() {
    const ASK: &str = r#"{"model":"a/gpt-4o","messages":[{"role":"user","content":"hi"}]}"#;
    let mock = Fakes::start().await;
    mock.reply(A, Answer::text("named provider"));
    let gw = model_gateway(&mock, Routes::new());
    let (status, answer) = routed_post(&gw, ASK).await;
    assert_eq!(status, StatusCode::OK);
    assert!(answer.contains("named provider"), "{answer}");
    let sent = mock.a.requests();
    assert_eq!(sent[0].body["model"], "gpt-4o");
}

// A model a request names outside its input, as a session names it in its
// handshake: read there, and written as the model the input asks for.
#[tokio::test]
async fn routing_reads_and_writes_a_model_named_outside_the_input() {
    const ASK: &str = r#"{"model":"unread","messages":[{"role":"user","content":"hi"}]}"#;
    let mock = Fakes::start().await;
    mock.reply(A, Answer::text("named in the head"));
    let head = |cx: &ClientContext| {
        let wire = cx
            .extensions()
            .get::<respan_gateway_http::raw::Original>()?;
        let model = wire.get()?.headers().get("x-model")?.to_str().ok()?;
        Some(model.to_owned())
    };
    let written = Arc::new(Mutex::new(Vec::new()));
    let seen = written.clone();
    let model = Model::new(head, move |cx, model| {
        seen.lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(model.clone());
        cx.filter(move |input: &mut Input| {
            input.set_model(model.clone());
            Ok(())
        })
    });
    let routes = Routes::new()
        .provider(
            "a",
            vec![Speaks {
                format: "openai-chat".into(),
                kind: CHAT,
                internal: "a".into(),
            }],
        )
        .provider(
            "b",
            vec![Speaks {
                format: "openai-chat".into(),
                kind: CHAT,
                internal: "b".into(),
            }],
        );
    let routing = Routing::new(Arc::new(routes.target("b"))).format("openai-chat", CHAT, model);
    let gw = Gateway::new()
        .client::<Input, _, _>(OpenAiChat)
        .expect("chat client")
        .provider(Provider::new(
            "a",
            OpenAiChat,
            respan_gateway_http::connect(mock.at(A), LIMIT),
        ))
        .expect("a provider")
        .provider(Provider::new(
            "b",
            OpenAiChat,
            respan_gateway_http::connect(mock.at(B), LIMIT),
        ))
        .expect("b provider")
        .plugin(routing);
    let mut request = post("/v1/chat/completions", ASK);
    request
        .headers_mut()
        .insert("x-model", HeaderValue::from_static("a/gpt-4o"));
    let (resp, outcome) = serve(&gw, request).await;
    let (status, _, _) = read_whole(resp).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(ended(outcome.await), Ok(()));
    assert_eq!(mock.a.requests()[0].body["model"], "gpt-4o");
    assert_eq!(
        *written.lock().unwrap_or_else(PoisonError::into_inner),
        ["gpt-4o"]
    );
}

#[tokio::test]
async fn routing_sends_unmatched_and_missing_models_to_the_target() {
    const UNMATCHED: &str = r#"{"model":"other","messages":[{"role":"user","content":"hi"}]}"#;
    const MISSING: &str = r#"{"messages":[{"role":"user","content":"hi"}]}"#;
    let mock = Fakes::start().await;
    mock.reply(B, Answer::text("unmatched default"));
    let gw = model_gateway(&mock, Routes::new());
    let (status, answer) = routed_post(&gw, UNMATCHED).await;
    assert_eq!(status, StatusCode::OK);
    assert!(answer.contains("unmatched default"), "{answer}");
    // The pinned fake rejects the missing model, but only the target
    // receives it: routing does not invent a model.
    let (resp, outcome) = serve(&gw, post("/v1/chat/completions", MISSING)).await;
    let _ = read_whole(resp).await;
    assert!(outcome.await.is_err());
    assert_eq!(mock.sent(), ["b.test", "b.test"]);
}

#[tokio::test]
async fn routing_sends_nothing_of_unreadable_input() {
    let mock = Fakes::start().await;
    let gw = model_gateway(&mock, Routes::new());
    let (resp, outcome) = serve(&gw, post("/v1/chat/completions", "not-json")).await;
    let _ = read_whole(resp).await;
    let _ = outcome.await;
    assert!(mock.sent().is_empty());
}

#[tokio::test]
async fn routing_leaves_other_formats_on_their_target() {
    let routes = Routes::new().provider(
        "a",
        vec![Speaks {
            format: "openai-chat".into(),
            kind: CHAT,
            internal: "a".into(),
        }],
    );
    let routing = Routing::new(Arc::new(routes)).format(
        "openai-chat",
        CHAT,
        Model::of(Input::model, Input::set_model),
    );
    let gw = Gateway::new()
        .client::<Input, _, _>(OpenAiChat)
        .expect("chat client")
        .client::<respan_gateway_passthrough_http::Input<Passthrough>, _, _>(Passthrough)
        .expect("opaque client")
        .provider(Provider::new(
            "raw",
            Passthrough,
            respan_gateway_http::connect(
                |_| async { Ok((http::Response::new(Body::full("opaque answer")), None)) },
                LIMIT,
            ),
        ))
        .expect("opaque provider")
        .plugin(routing)
        .target("raw")
        .expect("default target");
    let (resp, outcome) = serve(&gw, post("/other", "opaque input")).await;
    let (status, _, body) = read_whole(resp).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(&body.expect("opaque body")[..], b"opaque answer");
    assert_eq!(ended(outcome.await), Ok(()));
}

#[tokio::test]
async fn routing_refuses_a_model_read_from_another_input_type() {
    let mock = Fakes::start().await;
    let routing = Routing::new(Arc::new(Routes::new())).format(
        "openai-chat",
        CHAT,
        Model::of::<String>(|s| Some(s.as_str()), |s, model| *s = model),
    );
    let gw = Gateway::new()
        .client::<Input, _, _>(OpenAiChat)
        .expect("chat client")
        .provider(Provider::new(
            "a",
            OpenAiChat,
            respan_gateway_http::connect(mock.at(A), LIMIT),
        ))
        .expect("a provider")
        .plugin(routing)
        .target("a")
        .expect("default target");
    let body = r#"{"model":"gpt","messages":[{"role":"user","content":"hi"}]}"#;
    let (resp, outcome) = serve(&gw, post("/v1/chat/completions", body)).await;
    let (status, _, _) = read_whole(resp).await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert!(outcome.await.is_err_and(|f| f.code() == "internal"));
    assert!(mock.sent().is_empty());
}

/// Targets provider `b` before routing sees a request.
struct TargetsB;

#[async_trait]
impl Plugin for TargetsB {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        cx.set_target("b");
        Ok(None)
    }
}

/// Providers `a` and `b` of OpenAI Chat and routing that sends `gpt-4o`
/// to `a` as `model-a`, after a plugin that targets `b` if `earlier`, else over a gateway
/// whose default target is `b`.
fn targeted_gateway(mock: &Fakes, earlier: bool) -> Gateway {
    let speaks = |internal: &str| {
        vec![Speaks {
            format: "openai-chat".into(),
            kind: CHAT,
            internal: internal.into(),
        }]
    };
    let routes = Routes::new()
        .provider("a", speaks("a"))
        .provider("b", speaks("b"))
        .rule(Rule::new("gpt-4o", vec![Candidate::parse("a/model-a")]));
    let mut routing = Routing::new(Arc::new(routes)).format(
        "openai-chat",
        CHAT,
        Model::of(Input::model, Input::set_model),
    );
    if !earlier {
        routing = routing.over("b");
    }
    let gw = Gateway::new()
        .client::<Input, _, _>(OpenAiChat)
        .expect("chat client")
        .provider(Provider::new(
            "a",
            OpenAiChat,
            respan_gateway_http::connect(mock.at(A), LIMIT),
        ))
        .expect("a provider")
        .provider(Provider::new(
            "b",
            OpenAiChat,
            respan_gateway_http::connect(mock.at(B), LIMIT),
        ))
        .expect("b provider");
    match earlier {
        true => gw.plugin(TargetsB).plugin(routing),
        false => gw.plugin(routing).target("b").expect("default target"),
    }
}

#[tokio::test]
async fn routing_keeps_a_target_an_earlier_plugin_set() {
    const ASK: &str = r#"{"model":"gpt-4o","messages":[{"role":"user","content":"hi"}]}"#;
    let mock = Fakes::start().await;
    mock.reply(B, Answer::text("an earlier plugin's choice"));
    let (status, answer) = routed_post(&targeted_gateway(&mock, true), ASK).await;
    assert_eq!(status, StatusCode::OK);
    assert!(answer.contains("an earlier plugin's choice"), "{answer}");
    assert_eq!(mock.sent(), ["b.test"]);
    // Nor is the route's model name sent to the provider it did not choose.
    assert_eq!(mock.b.requests()[0].body["model"], "gpt-4o");
}

#[tokio::test]
async fn routing_overrides_the_gateways_default_target_it_was_told_of() {
    const ASK: &str = r#"{"model":"gpt-4o","messages":[{"role":"user","content":"hi"}]}"#;
    let mock = Fakes::start().await;
    mock.reply(A, Answer::text("routed"));
    let (status, answer) = routed_post(&targeted_gateway(&mock, false), ASK).await;
    assert_eq!(status, StatusCode::OK);
    assert!(answer.contains("routed"), "{answer}");
    assert_eq!(mock.sent(), ["a.test"]);
}

fn smart(pick: Pick) -> Rule {
    let mut rule = Rule::new(
        "smart",
        vec![Candidate::parse("a/model-a"), Candidate::parse("b/model-b")],
    );
    rule.pick = pick;
    rule
}

#[tokio::test]
async fn a_rule_falls_back_to_its_next_candidate_with_that_ones_model() {
    const ASK: &str = r#"{"model":"smart","messages":[{"role":"user","content":"hi"}]}"#;
    let mock = Fakes::start().await;
    mock.reply(A, Answer::error(503))
        .reply(B, Answer::text("from b"));
    let (gw, routes) = routed_gateway(&mock, Routes::new().rule(smart(Pick::First)));
    let (status, answer) = routed_post(&gw, ASK).await;
    assert_eq!(status, StatusCode::OK);
    assert!(answer.contains("from b"), "{answer}");
    assert_eq!(mock.sent(), ["a.test", "b.test"]);
    assert_eq!(mock.a.requests()[0].body["model"], "model-a");
    assert_eq!(mock.b.requests()[0].body["model"], "model-b");
    // The router's own record of each, once their end hooks ran.
    gw.idle().await;
    let a = routes.stats().seen("a", "model-a").expect("a seen");
    assert_eq!((a.calls, a.failures), (1, 1));
    let b = routes.stats().seen("b", "model-b").expect("b seen");
    assert_eq!((b.calls, b.failures), (1, 0));
    assert!(b.latency_ms.is_some());
}

#[tokio::test]
async fn fastest_goes_to_the_quicker_candidate_once_it_knows() {
    const ASK: &str = r#"{"model":"smart","messages":[{"role":"user","content":"hi"}]}"#;
    let mock = Fakes::start().await;
    let (gw, routes) = routed_gateway(&mock, Routes::new().rule(smart(Pick::Fastest)));
    routes.stats().record(
        "a",
        "model-a",
        Some(std::time::Duration::from_millis(900)),
        false,
    );
    routes.stats().record(
        "b",
        "model-b",
        Some(std::time::Duration::from_millis(50)),
        false,
    );
    mock.reply(B, Answer::text("quick"));
    let (status, answer) = routed_post(&gw, ASK).await;
    assert_eq!(status, StatusCode::OK);
    assert!(answer.contains("quick"), "{answer}");
    assert_eq!(mock.sent(), ["b.test"]);
}
