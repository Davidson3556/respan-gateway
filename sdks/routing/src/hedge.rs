//! Race a second provider against a slow first one.

use std::future::Future;
use std::time::Duration;

use respan_gateway_core::futures::FutureExt;
use respan_gateway_core::{Call, ClientContext, Fault, Plugin, Response, async_trait};
use respan_gateway_formats::conversation::set_model;

/// Calls `primary`; if it has not sent content within `after`, or fails
/// first, calls `backup` too. The first success to send content wins and
/// the other call is dropped, which cancels it. With neither a success, the
/// last answer goes to the client.
///
/// Each call is tagged with its [`Leg`], so provider-edge hooks can tell
/// the hedge from the primary.
#[derive(Debug, Clone)]
pub struct Hedge {
    primary: String,
    backup: String,
    backup_model: Option<String>,
    after: Duration,
}

impl Hedge {
    /// Hedge `primary` with `backup` after `after`.
    pub fn new(primary: impl Into<String>, backup: impl Into<String>, after: Duration) -> Self {
        Self {
            primary: primary.into(),
            backup: backup.into(),
            backup_model: None,
            after,
        }
    }

    /// Ask the backup for `model`, such as when it speaks another format.
    /// The model is a conversation's: on a request of another kind the
    /// backup call fails with `internal`, and the primary's answer stands.
    pub fn backup_model(mut self, model: impl Into<String>) -> Self {
        self.backup_model = Some(model.into());
        self
    }

    /// The backup call, unsent until awaited.
    fn backup(&self, cx: &mut ClientContext) -> Call {
        let call = cx.call(&self.backup).with_extension(Leg::Backup);
        match &self.backup_model {
            Some(model) => call.with_filter(set_model(model)),
            None => call,
        }
    }
}

/// Which leg of a hedge a call is, in its extensions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Leg {
    /// The primary.
    Primary,
    /// The backup, started after the delay.
    Backup,
}

/// An answer, and whether it won: a success whose first content arrived.
type Answer = (Result<Response, Fault>, bool);

/// `call`'s answer once its first content arrived, or it failed.
async fn first_content(call: impl Future<Output = Result<Response, Fault>>) -> Answer {
    match call.await {
        Ok(resp) if resp.head().status.is_success() => {
            let ready = resp.ready().await;
            let won = ready.is_ok();
            (ready, won)
        }
        answer => (answer, false),
    }
}

#[async_trait]
impl Plugin for Hedge {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let primary = cx.call(&self.primary).with_extension(Leg::Primary);
        let mut primary = first_content(primary).boxed();
        // Made now, sent only if the race needs it.
        let backup = self.backup(cx);
        let delay = tokio::time::sleep(self.after);
        tokio::pin!(delay);
        // Phase one: the primary alone, until it answers or `after` passes.
        let lost = tokio::select! {
            (answer, won) = &mut primary => {
                if won {
                    return answer.map(Some);
                }
                answer
            }
            () = &mut delay => {
                // Phase two: both race; the first success wins.
                let mut backup = first_content(backup).boxed();
                let ((first, won), rest) = tokio::select! {
                    a = &mut primary => (a, backup),
                    b = &mut backup => (b, primary),
                };
                if won {
                    return first.map(Some);
                }
                // The other may still win; the failed answer is dropped.
                drop(first);
                return rest.await.0.map(Some);
            }
        };
        // The primary failed before `after`: the backup goes at once.
        match first_content(backup).await.0 {
            Ok(resp) => Ok(Some(resp)),
            Err(_) => lost.map(Some),
        }
    }
}
