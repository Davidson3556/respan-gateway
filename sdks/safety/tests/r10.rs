//! Probes of the round-10 core: a panic in a filter or a head edit is an
//! internal fault, like the same edit's `Err` (T10-5).

mod common;

use common::*;
use respan_gateway_core::http::StatusCode;
use respan_gateway_core::{ClientContext, Fault, Filter, Plugin, Response, async_trait, codes};
use respan_gateway_formats::conversation::{Conversation, Reply};

/// Where a buggy safety check fails, and how.
#[derive(Clone, Copy)]
enum At {
    Input,
    FirstOutput,
    Head,
}

struct Buggy(At, bool);

fn fail(panics: bool) -> Result<(), Fault> {
    match panics {
        true => panic!("a buggy check"),
        false => Err(Fault::internal("a plugin or format panicked")),
    }
}

#[async_trait]
impl Plugin for Buggy {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let panics = self.1;
        match self.0 {
            At::Input => cx.filter(move |_: &mut Conversation| fail(panics))?,
            At::FirstOutput => cx.filter(move |_: &mut Reply| fail(panics))?,
            At::Head => cx.edit_response_head(move |_| fail(panics)),
        }
        Ok(None)
    }
}

async fn run(at: At, panics: bool) -> (StatusCode, String, Option<String>, usize) {
    let h = Harness::new(vec![("buggy", plugin(Buggy(at, panics)))]);
    h.reply(OPENAI, oa_stream(&["Sure"]));
    let (status, body, outcome) = h.text(openai_request("hi", true)).await;
    let end = outcome.end.err().map(|f| f.code().to_owned());
    (status, body, end, h.mock.sent().len())
}

/// A panicking input filter, output filter on the first item, or head edit
/// gives the client exactly what its `Err` twin gives: a 500 `internal`
/// refusal before any byte, with nothing of the provider's answer.
#[tokio::test]
async fn a_panicking_check_refuses_as_its_err_would() {
    for at in [At::Input, At::FirstOutput, At::Head] {
        let panicked = run(at, true).await;
        assert_eq!(panicked, run(at, false).await);
        let (status, body, end, sent) = panicked;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "{body}");
        assert!(!body.contains("Sure"), "{body}");
        assert_eq!(end.as_deref(), Some(codes::INTERNAL));
        // An input check stops the call before it is sent.
        assert_eq!(sent, usize::from(!matches!(at, At::Input)));
    }
}

/// Holds every output, then fails at its end: a check of the whole answer.
struct HoldThenFail(Vec<Reply>, bool);

impl Filter<Reply> for HoldThenFail {
    fn edit(&mut self, value: Reply, _: &mut Vec<Reply>) -> Result<(), Fault> {
        self.0.push(value);
        Ok(())
    }

    fn end(&mut self, _: &Result<(), Fault>, out: &mut Vec<Reply>) -> Result<(), Fault> {
        out.append(&mut self.0);
        fail(self.1)
    }
}

struct WholeAnswerCheck(bool);

#[async_trait]
impl Plugin for WholeAnswerCheck {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        cx.filter(HoldThenFail(Vec::new(), self.0))?;
        Ok(None)
    }
}

async fn whole(panics: bool) -> (StatusCode, String, Option<String>) {
    let h = Harness::new(vec![("whole", plugin(WholeAnswerCheck(panics)))]);
    h.reply(OPENAI, oa_stream(&["Sure"]));
    let (status, body, outcome) = h.text(openai_request("hi", true)).await;
    (status, body, outcome.end.err().map(|f| f.code().to_owned()))
}

/// A holding filter whose end panics ends the held answer as its `Err`
/// twin does: what it released is dropped and the end is `internal`. Both
/// still send the relayed role chunk and a 200 first: that is F10 (formats
/// crate), not the panic.
#[tokio::test]
async fn a_holding_filter_whose_end_panics_ends_as_its_err_would() {
    let panicked = whole(true).await;
    assert_eq!(panicked, whole(false).await);
    let (status, body, end) = panicked;
    assert_eq!(status, StatusCode::OK, "F10: {body}");
    assert!(!body.contains("Sure"), "{body}");
    assert!(body.contains("\"internal\""), "{body}");
    assert_eq!(end.as_deref(), Some(codes::INTERNAL));
}
