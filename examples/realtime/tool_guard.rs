//! A tool guard for realtime sessions, whatever their formats: a call of a
//! tool outside an allowed list never reaches the client, the model hears
//! that it is not allowed and goes on, and a session offers the model the
//! allowed tools alone. It reads the kind's events, so it guards an OpenAI
//! Realtime client answered by Gemini Live as it would any other session.
//! Set ALLOWED_TOOLS (comma-separated) and GEMINI_API_KEY; clients name
//! their model at `?model=` (`/v1/realtime?model=gemini-live-2.5-flash`).
#[path = "../support/live/serve.rs"]
mod serve;
use std::sync::Arc;

use respan_gateway_core::futures::channel::mpsc;
use respan_gateway_core::futures::{Stream, StreamExt, future, stream};
use respan_gateway_core::{
    ClientContext, EventStream, Fault, Filter, Gateway, Plugin, Provider, Response, async_trait,
};
use respan_gateway_realtime::{Input, Output, ToolResult};
use respan_gateway_realtime_ws::{GeminiLive, OpenAiRealtime};
use respan_gateway_ws::WsTransport;

/// What the model hears of a call it may not make.
const REFUSED: &str = r#"{"error":"this tool is not allowed"}"#;

/// The guard: the tools a session may call.
#[derive(Clone)]
struct ToolGuard {
    allowed: Arc<[String]>,
}

#[async_trait]
impl Plugin for ToolGuard {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let allowed = self.allowed.clone();
        cx.filter(move |input: &mut Input| {
            if let Input::Session(session) = input
                && let Some(tools) = &mut session.tools
            {
                tools.retain(|tool| allowed.contains(&tool.name));
            }
            Ok(())
        })?;
        let (tell, told) = mpsc::unbounded();
        cx.wrap(move |inputs: EventStream<Input>| among(inputs, told))?;
        cx.filter(Refuse {
            allowed: self.allowed.clone(),
            tell,
            refused: false,
            called: false,
        })?;
        Ok(None)
    }
}

/// What the client says, and what the guard tells the model among it, until
/// the client's end.
fn among(
    inputs: EventStream<Input>,
    told: mpsc::UnboundedReceiver<Input>,
) -> impl Stream<Item = Result<Input, Fault>> {
    let inputs = inputs.map(Some).chain(stream::once(future::ready(None)));
    stream::select(inputs, told.map(|input| Some(Ok(input))))
        .take_while(|input| future::ready(input.is_some()))
        .filter_map(future::ready)
}

/// Drops the calls of tools not allowed, answering each for the client; a
/// response whose calls were all refused is answered again, without them.
struct Refuse {
    allowed: Arc<[String]>,
    tell: mpsc::UnboundedSender<Input>,
    /// Whether the response being made called a tool not allowed.
    refused: bool,
    /// Whether it called one allowed, which its client answers.
    called: bool,
}

impl Filter<Output> for Refuse {
    fn edit(&mut self, output: Output, out: &mut Vec<Output>) -> Result<(), Fault> {
        match &output {
            Output::ToolCall { id, name, .. } if !self.allowed.contains(name) => {
                let answer = ToolResult::new(id.clone(), REFUSED);
                let _ = self.tell.unbounded_send(Input::ToolResult(answer));
                self.refused = true;
                return Ok(());
            }
            Output::ToolCall { .. } => self.called = true,
            Output::ResponseDone { .. } => {
                let called = std::mem::take(&mut self.called);
                if std::mem::take(&mut self.refused) && !called {
                    let _ = self.tell.unbounded_send(Input::Respond { native: None });
                }
            }
            _ => {}
        }
        out.push(output);
        Ok(())
    }
}

/// Clients of OpenAI Realtime and Gemini Live, answered by Gemini Live at
/// `transport`, guarded to `allowed`.
fn gateway(transport: WsTransport, allowed: &[&str]) -> Result<Gateway, Fault> {
    let allowed = allowed.iter().map(|t| (*t).to_owned()).collect();
    Gateway::new()
        .client::<Input, _, _>(OpenAiRealtime)?
        .client::<Input, _, _>(GeminiLive)?
        .provider(Provider::new("gemini", GeminiLive, transport.connect()))?
        .plugin(ToolGuard { allowed })
        .target("gemini")
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut headers = respan_gateway_http::http::HeaderMap::new();
    headers.insert("x-goog-api-key", std::env::var("GEMINI_API_KEY")?.parse()?);
    let allowed = std::env::var("ALLOWED_TOOLS")?;
    let allowed: Vec<&str> = allowed.split(',').map(str::trim).collect();
    let transport = GeminiLive
        .transport("wss://generativelanguage.googleapis.com")?
        .with_headers(headers)?;
    serve::run(gateway(transport, &allowed)?, "127.0.0.1:8083").await?;
    Ok(())
}
#[cfg(test)]
#[path = "../tests/cases/realtime/tool_guard.rs"]
mod tests;
