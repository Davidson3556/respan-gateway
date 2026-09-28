//! Probes of the core contract that the SDK's features ran into. Each test
//! names its finding in `FINDINGS.md`.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use common::*;
use respan_gateway_core::http::StatusCode;
use respan_gateway_core::serde_json::json;
use respan_gateway_core::{
    ClientContext, Fault, Filter, Plugin, ProviderContext, Response, async_trait,
};
use respan_gateway_formats::conversation::Reply;
use respan_gateway_testkit::post;
use respan_safety::calls::Judge;
use respan_safety::guard::{BLOCKED, Guard, Policy};
use respan_safety::pii::{Pii, Redactor};
use respan_safety::retention::{ContentLog, Tenant, TenantAuth};

const SECRET: &str = "I am Alice, mail me at alice@example.com";

fn pii() -> Pii {
    Pii::new(Redactor::new(["Alice"]))
}

/// F1 (fixed by D4, now through `Fault::with_raw`): what filters held is
/// written before a relayed provider error's body, so the stream still
/// ends with its error event.
#[tokio::test]
async fn held_text_goes_before_a_relayed_error_body() {
    let h = Harness::new(vec![("pii", plugin(pii()))]);
    h.reply(OPENAI, oa_error_event(&["Hi <PERSON"]));
    let (_, _, body, outcome) = h.send(openai_request(SECRET, true)).await;
    let body = String::from_utf8_lossy(&body.expect("body")).into_owned();
    let said = said(&body);
    assert!(matches!(said.last(), Some(Said::Error(_))), "{body}");
    assert!(body.contains("overloaded"), "{body}");
    assert!(outcome.end.is_err());
}

/// F3: the only per-request slot a plugin's hooks share is the head's
/// extensions, which every plugin can read and change, and which reach the
/// transport with every call: here the tenant mark and the content log's
/// private completion buffer.
#[tokio::test]
async fn a_plugins_private_request_state_reaches_the_transport() {
    let auth = TenantAuth::new().tenant("k", "acme", false);
    let h = Harness::new(vec![
        ("auth", plugin(auth)),
        ("content", plugin(ContentLog::default())),
    ]);
    h.reply(OPENAI, oa_whole("ok"));
    let mut req = openai_request("hi", false);
    req.headers_mut()
        .insert("authorization", "Bearer k".parse().expect("value"));
    h.text(req).await;
    let sent = h.requests_to(OPENAI);
    assert_eq!(
        sent[0].extensions().get::<Tenant>(),
        Some(&Tenant("acme".into()))
    );
    // Tenant plus the log's private buffer, which the test cannot name.
    assert!(sent[0].extensions().len() >= 2);
}

/// A filter that fails on a word and releases what it held at its end.
struct FailsThenReleases(Vec<Reply>);

impl Filter<Reply> for FailsThenReleases {
    fn edit(&mut self, event: Reply, _out: &mut Vec<Reply>) -> Result<(), Fault> {
        if matches!(&event, Reply::Text { text, .. } if text.contains("stop")) {
            return Err(Fault::new(StatusCode::BAD_REQUEST, BLOCKED, "blocked"));
        }
        self.0.push(event);
        Ok(())
    }
    fn end(&mut self, _end: &Result<(), Fault>, out: &mut Vec<Reply>) -> Result<(), Fault> {
        out.append(&mut self.0);
        Ok(())
    }
}

struct AddFilter;

#[async_trait]
impl Plugin for AddFilter {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        cx.filter(FailsThenReleases(Vec::new()))?;
        Ok(None)
    }
}

/// S7-1, fixed by Z8-1: a filter whose own `edit` failed still gets its
/// `end`, but what it releases there is dropped, so a filter that holds on
/// suspicion no longer leaks what it held back, and need not remember
/// failing (the guard's workaround is gone).
#[tokio::test]
async fn a_filter_whose_event_failed_releases_nothing_at_its_end() {
    let h = Harness::new(vec![("f", plugin(AddFilter))]);
    h.reply(OPENAI, oa_stream(&["held ", "stop"]));
    let (_, body, _) = h.text(openai_request("hi", true)).await;
    assert!(!text_of(&body).contains("held"), "{body}");
    assert!(error_of(&body).is_some(), "{body}");
}

/// Counts its `end`s: a provider-edge filter holding state until the end.
struct CountEnds(Arc<AtomicUsize>);

impl Filter<Reply> for CountEnds {
    fn edit(&mut self, event: Reply, out: &mut Vec<Reply>) -> Result<(), Fault> {
        out.push(event);
        Ok(())
    }
    fn end(&mut self, _end: &Result<(), Fault>, _out: &mut Vec<Reply>) -> Result<(), Fault> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

struct ProviderEnds(Arc<AtomicUsize>);

#[async_trait]
impl Plugin for ProviderEnds {
    async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
        px.filter(CountEnds(self.0.clone()))
    }
}

/// F17 (fixed by D2): when a client-edge filter fails the response
/// mid-stream, the sent call's provider-edge filters still get their `end`.
#[tokio::test]
async fn a_provider_filter_gets_its_end_when_a_client_filter_fails() {
    let ends = Arc::new(AtomicUsize::new(0));
    let guard = Guard::new(Policy::new(["bomb"])).conversations();
    let h = Harness::new(vec![
        ("guard", plugin(guard)),
        ("ends", plugin(ProviderEnds(ends.clone()))),
    ]);
    h.reply(OPENAI, oa_stream(&["a ", "bomb", " and more"]));
    let (_, body, _) = h.text(openai_request("hi", true)).await;
    assert!(error_of(&body).is_some(), "{body}");
    assert_eq!(ends.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn a_provider_filter_gets_its_end_on_a_clean_stream() {
    let ends = Arc::new(AtomicUsize::new(0));
    let h = Harness::new(vec![("ends", plugin(ProviderEnds(ends.clone())))]);
    h.reply(OPENAI, oa_stream(&["a"]));
    h.text(openai_request("hi", true)).await;
    assert_eq!(ends.load(Ordering::SeqCst), 1);
}

/// F16: OpenAI's `user` field is known to the format and not held by the
/// kind; `Pii` scrubs it from the call as sent (`edit_call`).
#[tokio::test]
async fn pii_in_the_user_field_can_be_redacted() {
    let h = Harness::new(vec![("pii", plugin(pii()))]);
    h.reply(OPENAI, oa_whole("ok"));
    let body = json!({ "model": "gpt-x", "user": "alice@example.com",
        "messages": [{ "role": "user", "content": "hi Alice" }] });
    h.text(post(CHAT, body.to_string())).await;
    let sent = h.sent_to(OPENAI)[0].to_string();
    assert!(!sent.contains("alice@example.com"), "{sent}");
}

/// F16 (fixed by A10): a judge's own input is a new message, so nothing of
/// the client's body rides into the judge call.
#[tokio::test]
async fn a_judges_own_input_carries_nothing_of_the_clients_body() {
    let h = Harness::new(vec![
        ("pii", plugin(pii())),
        ("judge", plugin(Judge::new("judge", "openai"))),
    ]);
    h.reply(JUDGE, oa_whole("FINE"));
    h.reply(OPENAI, oa_whole("ok"));
    let body = json!({ "model": "gpt-x", "user": "alice@example.com",
        "messages": [{ "role": "user", "content": "hi" }] });
    h.text(post(CHAT, body.to_string())).await;
    let judged = h.sent_to(JUDGE)[0].to_string();
    assert!(!judged.contains("alice@example.com"), "{judged}");
}

/// F16: a header carrying PII is scrubbed on the provider edge, from the
/// call as sent (`edit_call`).
#[tokio::test]
async fn a_pii_header_is_scrubbed_from_the_call_on_the_provider_edge() {
    let h = Harness::new(vec![("pii", plugin(pii()))]);
    h.reply(OPENAI, oa_whole("ok"));
    let mut req = openai_request("hi", false);
    req.headers_mut()
        .insert("x-user-email", "alice@example.com".parse().expect("value"));
    h.text(req).await;
    let sent = h.requests_to(OPENAI);
    assert_eq!(
        sent[0].headers().get("x-user-email").map(|v| v.as_bytes()),
        Some(&b"<EMAIL_1>"[..])
    );
}

/// Refuses, on the call as sent, a body carrying an address: a check that
/// only the final bytes can make, such as after a format wrote fields the
/// kind does not hold.
struct FinalBytesCheck;

#[async_trait]
impl Plugin for FinalBytesCheck {
    async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
        px.edit_call(|call| {
            let bytes = call.body().to_bytes();
            match String::from_utf8_lossy(&bytes).contains("alice@example.com") {
                true => Err(Fault::new(StatusCode::FORBIDDEN, "pii", "PII would leave")),
                false => Ok(()),
            }
        });
        Ok(())
    }
}

/// N2 (fixed by r2 #10): an edit of the call as sent refuses the call with
/// the plugin's own status and code, and nothing is sent.
#[tokio::test]
async fn a_check_on_the_call_as_sent_refuses_with_its_own_status() {
    let h = Harness::new(vec![("final", plugin(FinalBytesCheck))]);
    let body = json!({ "model": "gpt-x", "user": "alice@example.com",
        "messages": [{ "role": "user", "content": "hi" }] });
    let (status, _, outcome) = h.text(post(CHAT, body.to_string())).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(outcome.end.as_ref().err().map(Fault::code), Some("pii"));
    assert!(h.mock.sent().is_empty());
}

/// N3 (fixed by r2 #1 and #2): an OpenAI stream's role chunk is held with
/// the next item, so `ready` falls back from a 200 that breaks right after
/// it.
#[tokio::test]
async fn a_fallback_falls_back_from_a_stream_that_breaks_before_content() {
    use respan_safety::calls::Fallback;
    let h = Harness::new(vec![(
        "fallback",
        plugin(Fallback(vec!["openai", "backup"])),
    )]);
    h.reply(OPENAI, oa_broken(&[]));
    h.reply(BACKUP, oa_stream(&["from backup"]));
    let (status, body, _) = h.text(openai_request("hi", true)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(text_of(&body), "from backup", "{body}");
}

/// Drops, on the provider edge, text deltas of "ping" (a provider's
/// keep-alive written as content), and starts (with their empty text) too
/// if `starts`.
struct DropPings {
    starts: bool,
}

/// Dropping takes a named type: only an in-place edit is a closure filter.
#[derive(Clone, Copy)]
struct Pings(bool);

impl Filter<Reply> for Pings {
    fn edit(&mut self, reply: Reply, out: &mut Vec<Reply>) -> Result<(), Fault> {
        let ping = matches!(&reply, Reply::Text { text, .. } if text == "ping");
        let start = match &reply {
            Reply::Start { .. } => true,
            Reply::Text { text, .. } => text.is_empty(),
            _ => false,
        };
        if !(ping || start && self.0) {
            out.push(reply);
        }
        Ok(())
    }
}

#[async_trait]
impl Plugin for DropPings {
    async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
        px.filter(Pings(self.starts))
    }
}

fn fallback_behind(drop: DropPings) -> Harness {
    use respan_safety::calls::Fallback;
    let h = Harness::new(vec![
        ("drop", plugin(drop)),
        ("fallback", plugin(Fallback(vec!["openai", "backup"]))),
    ]);
    h.reply(OPENAI, oa_broken(&["ping"]));
    h.reply(BACKUP, oa_stream(&["from backup"]));
    h
}

/// S-N1 (fixed by Q-4): `ready` judges the provider's items before any
/// filter, so a start is never content whatever a filter drops: dropping
/// the starts or not, the provider's "ping" is its first content, and the
/// fallback relays the stream that then breaks.
///
/// The price (S-R1): a plugin that knows "ping" is a keep-alive written as
/// content, and drops it, can no longer make `ready` skip it, as it could
/// in round 3; the fallback no longer reaches the backup.
#[tokio::test]
async fn ready_counts_what_the_provider_sent_whatever_a_filter_drops() {
    for starts in [true, false] {
        let h = fallback_behind(DropPings { starts });
        let (status, body, outcome) = h.text(openai_request("hi", true)).await;
        assert_eq!(status, StatusCode::OK);
        assert!(error_of(&body).is_some(), "{starts}: {body}");
        assert!(outcome.end.is_err());
        assert!(h.requests_to(BACKUP).is_empty());
    }
}

/// A judge that notes what its verdict cost, through a filter on its own
/// call (`Call::with_filter`), reading only the first reply it needs.
#[derive(Clone, Default)]
struct CostedJudge(Arc<std::sync::Mutex<respan_gateway_core::Usage>>);

#[async_trait]
impl Plugin for CostedJudge {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        use respan_gateway_core::futures::StreamExt;
        use respan_gateway_formats::conversation::{Conversation, Message, Role};
        let prompt = Conversation::new([Message::new(Role::User, "safe?")]);
        let cost = self.0.clone();
        let judge = cx.call("judge").with_input(prompt).with_filter(
            move |u: &mut respan_gateway_core::Usage| {
                *cost.lock().unwrap_or_else(|e| e.into_inner()) += &*u;
                Ok(())
            },
        );
        let mut replies = judge.await?.outputs::<Reply>();
        // Only the first reply is needed; the rest is dropped unread.
        let _first = replies.next().await.transpose()?;
        Ok(None)
    }
}

/// E5-3 (fixed): a plugin reading a call's response sees its usage, even
/// when it stops at the first reply, as a decoder pushes usage before its
/// item.
#[tokio::test]
async fn a_judge_sees_what_its_verdict_cost() {
    let judge = CostedJudge::default();
    let h = Harness::new(vec![("judge", plugin(judge.clone()))]);
    h.reply(JUDGE, oa_whole("yes"));
    h.reply(OPENAI, oa_whole("hi"));
    let (status, _, _) = h.text(openai_request("hi", false)).await;
    assert_eq!(status, StatusCode::OK);
    let cost = judge.0.lock().map(|u| u.clone()).unwrap_or_default();
    assert_eq!(cost.get("input_tokens"), Some(3.0), "{cost:?}");
    assert_eq!(cost.get("output_tokens"), Some(2.0), "{cost:?}");
}
