//! Calls from every hook, shadow calls, targets of another kind, usage.

mod common;

use std::sync::{Arc, Mutex, PoisonError};

use common::*;
use respan_gateway_core::futures::{TryStreamExt, stream};
use respan_gateway_core::http::{self, StatusCode};
use respan_gateway_core::{
    Call, ClientContext, Decoder, Doc, Encoder, Fault, Format, Gateway, Outcome, Plugin,
    ProviderContext, Response, async_trait, codes,
};
use respan_gateway_formats::conversation::{Conversation, Message, Reply, Role};
use respan_gateway_formats::{OpenAiChat, bytes_decoder, bytes_encoder, posts_to};
use respan_gateway_testkit::MockTransport;

type Codes = Arc<Mutex<Vec<String>>>;

fn push<T>(codes: &Codes, result: &Result<T, Fault>) {
    let code = match result {
        Ok(_) => "ok".to_owned(),
        Err(f) => f.code().to_owned(),
    };
    codes
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .push(code);
}

fn gateway(mock: &MockTransport, plugin: impl Plugin, log: &Log) -> Gateway {
    Gateway::new()
        .provider("openai", OpenAiChat, mock.at(OPENAI))
        .and_then(|g| g.provider("raw", Raw, mock.at(BACKUP)))
        .and_then(|g| g.serve(OpenAiChat))
        .and_then(|g| g.plugin("log", log.clone())?.plugin("p", plugin))
        .and_then(|g| g.target("openai"))
        .expect("gateway")
}

// F2: a call from the provider edge (`px.client().call`) or from an end
// hook is now a compile error (`call` takes `&mut self`); the core pins it
// with `compile_fail` doctests, so the run-time test is gone.

/// Keeps a call's response past `on_client`, to read it later as a shadow.
#[derive(Default)]
struct Shadow {
    kept: Mutex<Option<Response>>,
    read: Codes,
}

#[async_trait]
impl Plugin for Shadow {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let shadow = cx.call("openai").await?;
        *self.kept.lock().unwrap_or_else(PoisonError::into_inner) = Some(shadow);
        Ok(None)
    }

    async fn on_client_end(&self, _cx: &ClientContext, _o: &Outcome) {
        let kept = self
            .kept
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if let Some(resp) = kept {
            let read = resp.outputs::<Reply>().try_collect::<Vec<_>>().await;
            push(&self.read, &read);
        }
    }
}

/// A shadow call cannot outlive its request: once the plugins have
/// answered, its response cannot be read, and it ends abandoned when the
/// request closes (promised: operation records).
#[tokio::test]
async fn a_shadow_call_cannot_be_read_after_the_request_answered() {
    let (mock, log) = (MockTransport::new(), Log::default());
    let shadow = Arc::new(Shadow::default());
    let gw = gateway(&mock, shadow.clone(), &log);
    mock.reply(OPENAI, json_reply(StatusCode::OK, &openai_json("shadow")))
        .reply(OPENAI, json_reply(StatusCode::OK, &openai_json("answer")));

    let (status, _, body) = send(&gw, chat("gpt-x", false, "hi")).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(openai_text(&body.expect("whole")), "answer");
    let read = shadow
        .read
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .clone();
    // By the end hook the call has ended, so it reads `abandoned`.
    assert_eq!(read, [codes::ABANDONED]);
    let ends: Vec<_> = log.calls().into_iter().map(|c| c.end).collect();
    assert!(ends.contains(&"abandoned".to_owned()), "{ends:?}");
}

/// Answers from a call it read whole, such as to check the whole answer
/// before committing.
struct Buffered;

#[async_trait]
impl Plugin for Buffered {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let outputs: Vec<Reply> = cx.call("openai").await?.outputs().try_collect().await?;
        Ok(Some(Response::stream(stream::iter(
            outputs.into_iter().map(Ok),
        ))))
    }
}

/// F1b, fixed by usage that never overlaps: a call read whole through
/// `outputs` counts its usage on its provider edge, once, and the client
/// edge counts only usage no call reported, so nothing is lost and a biller
/// adds both edges.
#[tokio::test]
async fn a_buffered_answer_counts_its_usage_once_on_the_provider_edge() {
    let (mock, log) = (MockTransport::new(), Log::default());
    let gw = gateway(&mock, Buffered, &log);
    mock.reply(OPENAI, json_reply(StatusCode::OK, &openai_json("ok")));

    let (status, _, body) = send(&gw, chat("gpt-x", false, "hi")).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(openai_text(&body.expect("whole")), "ok");
    assert_eq!(log.calls()[0].output_tokens, 2.0);
    assert_eq!(log.client().output_tokens, 0.0);
}

/// Sets a target no conversation request can use.
struct ToRaw;

#[async_trait]
impl Plugin for ToRaw {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        cx.set_target("raw");
        Ok(None)
    }
}

/// F12: a router cannot ask whether a request can use a provider; setting
/// one of another kind is found out when the kernel calls it, as a 500.
#[tokio::test]
async fn a_target_of_another_kind_is_a_500_config_error() {
    let (mock, log) = (MockTransport::new(), Log::default());
    let gw = gateway(&mock, ToRaw, &log);

    let (status, _, body) = send(&gw, chat("gpt-x", false, "hi")).await;

    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert!(body.expect("whole").contains(codes::CONFIG));
    assert!(mock.sent().is_empty());
    // No provider hook ran: the check comes before the provider edge.
    assert!(log.calls().is_empty());
}

/// Marks its judge call so a billing plugin can tell it from the target's.
struct TaggedJudge;

#[derive(Clone, Debug)]
struct JudgeCall;

#[async_trait]
impl Plugin for TaggedJudge {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let prompt = Conversation::new([Message::new(Role::User, "easy or hard?")]);
        let judge = cx
            .call("openai")
            .with_input(prompt)
            .with_extension(JudgeCall);
        let _: Vec<Reply> = judge.await?.outputs().try_collect().await?;
        Ok(None)
    }
}

#[derive(Default)]
struct Roles(Codes);

#[async_trait]
impl Plugin for Roles {
    async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
        let role = match px.extensions().get::<JudgeCall>() {
            Some(_) => "judge",
            None => "target",
        };
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(role.into());
        Ok(())
    }
}

#[tokio::test]
async fn a_call_is_tagged_with_its_own_extension() {
    let mock = MockTransport::new();
    let roles = Arc::new(Roles::default());
    let gw = Gateway::new()
        .provider("openai", OpenAiChat, mock.at(OPENAI))
        .and_then(|g| g.serve(OpenAiChat)?.plugin("roles", roles.clone()))
        .and_then(|g| g.plugin("judge", TaggedJudge)?.target("openai"))
        .expect("gateway");
    mock.reply(OPENAI, json_reply(StatusCode::OK, &openai_json("easy")))
        .reply(OPENAI, json_reply(StatusCode::OK, &openai_json("ok")));

    let (status, _, _) = send(&gw, chat("gpt-x", false, "hi")).await;

    assert_eq!(status, StatusCode::OK);
    let roles = roles
        .0
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .clone();
    assert_eq!(roles, ["judge", "target"]);
}

/// A format of no kind, for a provider no conversation can reach.
struct Raw;

impl Format for Raw {
    type Input = ();
    type Output = ();

    fn name(&self) -> &str {
        "raw"
    }

    fn accepts(&self, head: &http::request::Parts) -> bool {
        posts_to(head, "/raw")
    }

    fn decode_input(&self, _head: &http::request::Parts, _body: &Doc) -> Result<(), Fault> {
        Ok(())
    }

    fn encode_input(&self, _input: &(), _original: Option<(&Doc, &())>) -> Result<Doc, Fault> {
        Ok(Doc::default())
    }

    fn prepare(
        &self,
        body: Doc,
        _: Option<&()>,
        _client: &http::request::Parts,
        _relayed: bool,
    ) -> Result<http::Request<Doc>, Fault> {
        let mut req = http::Request::new(body);
        *req.method_mut() = http::Method::POST;
        *req.uri_mut() = http::Uri::from_static("/raw");
        Ok(req)
    }

    fn decoder(
        &self,
        head: &http::response::Parts,
        _call: &http::Request<Doc>,
    ) -> Box<dyn Decoder> {
        bytes_decoder(head)
    }

    fn encoder(
        &self,
        head: &mut http::response::Parts,
        _client: &http::Request<Doc>,
        relayed: bool,
    ) -> Box<dyn Encoder> {
        bytes_encoder(head, relayed)
    }
}

/// Keeps a `Call` made in `on_client` in the request's extensions, then
/// sends a clone of it from the provider edge: a `Call` is 'static and
/// Clone, so a provider-edge plugin can call a model this way.
struct StashedJudge(Codes);

#[async_trait]
impl Plugin for StashedJudge {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let judge = cx.call("openai");
        cx.head_mut().extensions.insert(judge);
        Ok(None)
    }

    async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
        let Some(judge) = px.extensions().get::<Call>().cloned() else {
            return Ok(());
        };
        // The judge's own call was made before the stash, so it has none.
        let mut prompt = Conversation::new([Message::new(Role::User, "is this safe?")]);
        prompt.model = Some("judge-x".into());
        let judged = judge.with_input(prompt).await;
        push(&self.0, &judged);
        Ok(())
    }
}

#[tokio::test]
async fn a_call_made_in_on_client_can_be_sent_from_the_provider_edge() {
    let (mock, log) = (MockTransport::new(), Log::default());
    let codes = Codes::default();
    let gw = gateway(&mock, StashedJudge(codes.clone()), &log);
    mock.reply(OPENAI, json_reply(StatusCode::OK, &openai_json("safe")))
        .reply(OPENAI, json_reply(StatusCode::OK, &openai_json("ok")));

    let (status, _, _) = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        send(&gw, chat("gpt-x", false, "hi")),
    )
    .await
    .expect("deadlocked");

    assert_eq!(status, StatusCode::OK);
    let codes = codes.lock().unwrap_or_else(PoisonError::into_inner).clone();
    assert_eq!(codes, ["ok"]);
}
