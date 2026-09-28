//! A judge model picks which provider answers.

use respan_gateway_core::futures::TryStreamExt;
use respan_gateway_core::{ClientContext, Fault, Plugin, Response, async_trait};
use respan_gateway_formats::conversation::{Conversation, Message, Reply, Role, set_model};

/// One choice the judge can make.
#[derive(Debug, Clone)]
struct Route {
    label: String,
    target: String,
    model: Option<String>,
}

/// Asks `judge` which of its routes suits the client's last message, then
/// sets that route's target and model and passes, so the plugins after it
/// still run and the target answers. A judge that fails, or answers with no
/// known label, leaves the target as it was. Other kinds pass untouched.
#[derive(Debug, Clone)]
pub struct SmartRouter {
    judge: String,
    judge_model: String,
    routes: Vec<Route>,
}

impl SmartRouter {
    /// A router asking `judge` for `model`.
    pub fn new(judge: impl Into<String>, model: impl Into<String>) -> Self {
        Self {
            judge: judge.into(),
            judge_model: model.into(),
            routes: Vec::new(),
        }
    }

    /// When the judge answers `label`, send the request to `provider`,
    /// asking for `model` if given.
    pub fn route(
        mut self,
        label: impl Into<String>,
        provider: impl Into<String>,
        model: Option<&str>,
    ) -> Self {
        self.routes.push(Route {
            label: label.into(),
            target: provider.into(),
            model: model.map(Into::into),
        });
        self
    }

    fn prompt(&self, question: String) -> Conversation {
        let labels: Vec<&str> = self.routes.iter().map(|r| r.label.as_str()).collect();
        let system = format!(
            "Classify the user's message. Answer with exactly one of: {}.",
            labels.join(", ")
        );
        let mut prompt = Conversation::new([
            Message::new(Role::System, system),
            Message::new(Role::User, question),
        ]);
        prompt.max_tokens = Some(5);
        prompt
    }
}

#[async_trait]
impl Plugin for SmartRouter {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        if !cx.is_kind::<Conversation>() {
            return Ok(None);
        }
        let chat = cx.input::<Conversation>()?;
        let question = chat
            .messages
            .iter()
            .rev()
            .find(|m| m.role == Role::User)
            .map(Message::text)
            .unwrap_or_default();
        let mut prompt = self.prompt(question);
        prompt.model = Some(self.judge_model.clone());
        let verdict = match cx.call(&self.judge).with_input(prompt).await {
            Ok(resp) if resp.head().status.is_success() => resp.outputs().try_collect().await,
            // Fail open: the judge is advice, not a guard.
            _ => return Ok(None),
        };
        let Ok::<Vec<Reply>, _>(events) = verdict else {
            return Ok(None);
        };
        let said: String = events
            .iter()
            .filter_map(|e| match e {
                Reply::Text { text, .. } => Some(text.as_str()),
                _ => None,
            })
            .collect();
        let said = said.trim().to_lowercase();
        if let Some(route) = self.routes.iter().find(|r| said == r.label.to_lowercase()) {
            cx.set_target(&route.target);
            if let Some(model) = &route.model {
                cx.filter(set_model(model))?;
            }
        }
        Ok(None)
    }
}
