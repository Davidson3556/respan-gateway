//! Providers tried in turn, across formats, each retried with backoff.

use respan_gateway_core::{ClientContext, Fault, Plugin, Response, async_trait, codes};
use respan_gateway_formats::conversation::{Conversation, set_model};

use crate::retry::{Backoff, should_retry};

/// One provider to try, and the model to ask it for.
#[derive(Debug, Clone)]
struct Attempt {
    target: Option<String>,
    model: Option<String>,
}

/// Answers from the first provider that succeeds. Each is retried while it
/// answers a retryable status or cannot be reached, waiting as
/// [`Backoff`] says; a `Retry-After` longer than it allows moves on at once.
/// A success that fails before any content, such as Anthropic's `200`
/// then `overloaded_error`, counts as a failure of its status. A client
/// error that no retry fixes goes straight to the client, as does a hook's
/// refusal of a call.
///
/// It answers, so the plugins after it never run: register it last
/// (FINDINGS F3).
#[derive(Debug, Clone)]
pub struct Fallback {
    attempts: Vec<Attempt>,
    backoff: Backoff,
}

impl Fallback {
    /// A fallback that first tries the request's target, as routers and the
    /// gateway set it.
    pub fn new(backoff: Backoff) -> Self {
        Self {
            attempts: vec![Attempt {
                target: None,
                model: None,
            }],
            backoff,
        }
    }

    /// Then try `provider`, asking for `model` if given; a provider of
    /// another format of the kind gets the request translated. The model is
    /// a conversation's: an attempt with one is skipped for a request of
    /// another kind, which has no model this plugin can set.
    pub fn then(mut self, provider: impl Into<String>, model: Option<&str>) -> Self {
        self.attempts.push(Attempt {
            target: Some(provider.into()),
            model: model.map(Into::into),
        });
        self
    }
}

#[async_trait]
impl Plugin for Fallback {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let mut last: Option<Result<Response, Fault>> = None;
        for attempt in &self.attempts {
            let target = attempt.target.as_deref().or(cx.target());
            let Some(target) = target.map(str::to_owned) else {
                continue;
            };
            if attempt.model.is_some() && !cx.is_kind::<Conversation>() {
                continue;
            }
            for n in 1..=self.backoff.attempts {
                let mut call = cx.call(&target);
                if let Some(model) = &attempt.model {
                    call = call.with_filter(set_model(model));
                }
                let (answered, wait) = match call.await {
                    // A success counts once its first content arrives.
                    Ok(resp) if resp.head().status.is_success() => match resp.ready().await {
                        Ok(resp) => return Ok(Some(resp)),
                        Err(f) => (Err(f), self.backoff.wait(n, None)),
                    },
                    Ok(resp) if !should_retry(resp.head()) => return Ok(Some(resp)),
                    Ok(resp) => {
                        let wait = self.backoff.wait(n, Some(&resp.head().headers));
                        (Ok(resp), wait)
                    }
                    // Unreachable: worth another try. Anything else, such as
                    // a hook's refusal, is not the provider's to fix.
                    Err(f) if f.code() == codes::UPSTREAM => (Err(f), self.backoff.wait(n, None)),
                    Err(f) => return Err(f),
                };
                // Dropping the last answer before waiting would free its
                // connection, but it may be the one the client gets.
                last = Some(answered);
                match wait {
                    Some(wait) => tokio::time::sleep(wait).await,
                    None => break,
                }
            }
        }
        match last {
            Some(last) => last.map(Some),
            None => Err(Fault::not_found("no provider to fall back on")),
        }
    }
}
