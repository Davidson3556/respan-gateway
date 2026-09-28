//! Plugins that make calls of their own, which every provider-edge safety
//! plugin must cover: a fallback and a judge.

use std::sync::{Arc, Mutex, PoisonError};

use respan_gateway_core::futures::TryStreamExt;
use respan_gateway_core::{ClientContext, Fault, Plugin, Response, async_trait};
use respan_gateway_formats::conversation::{Conversation, Message, Reply, Role};

/// Answers by calling providers in turn until one succeeds, a 200 that
/// fails before any content included (`ready`), as in
/// `docs/ARCHITECTURE.md`. It answers, so it runs last.
#[derive(Debug, Clone)]
pub struct Fallback(pub Vec<&'static str>);

#[async_trait]
impl Plugin for Fallback {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let mut last = Err(Fault::upstream("no providers"));
        for name in &self.0 {
            last = match cx.call(*name).await {
                Ok(resp) if resp.head().status.is_success() => resp.ready().await,
                other => other,
            };
            if last.as_ref().is_ok_and(|r| r.head().status.is_success()) {
                break;
            }
        }
        last.map(Some)
    }
}

/// Asks a judge model whether to escalate, with its own prompt built from
/// the user's last message, then targets `escalate` if the verdict says so.
/// Records each verdict it read.
#[derive(Debug, Clone)]
pub struct Judge {
    judge: String,
    escalate: String,
    verdicts: Arc<Mutex<Vec<Result<String, String>>>>,
}

impl Judge {
    /// A judge calling `judge`, escalating to `escalate`.
    pub fn new(judge: &str, escalate: &str) -> Self {
        Self {
            judge: judge.into(),
            escalate: escalate.into(),
            verdicts: Arc::default(),
        }
    }

    /// Every verdict read, or the code of the fault instead.
    pub fn verdicts(&self) -> Vec<Result<String, String>> {
        self.verdicts
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

#[async_trait]
impl Plugin for Judge {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        if !cx.is_kind::<Conversation>() {
            return Ok(None);
        }
        let chat = cx.input::<Conversation>()?;
        let last = chat.messages.last().map(Message::text).unwrap_or_default();
        let prompt = Conversation::new([Message::new(
            Role::User,
            format!("Answer ESCALATE or FINE. The user said: {last}"),
        )]);
        let verdict = async {
            let reply = cx.call(&self.judge).with_input(prompt).await?;
            let replies: Vec<Reply> = reply.outputs().try_collect().await?;
            let text: String = replies
                .iter()
                .filter_map(|e| match e {
                    Reply::Text { text, .. } => Some(text.as_str()),
                    _ => None,
                })
                .collect();
            Ok::<_, Fault>(text)
        }
        .await;
        let escalate = verdict.as_ref().is_ok_and(|v| v.contains("ESCALATE"));
        self.verdicts
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(verdict.map_err(|f| f.code().to_owned()));
        if escalate {
            cx.set_target(&self.escalate);
        }
        Ok(None)
    }
}
