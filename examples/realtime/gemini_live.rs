//! Gemini Live through the gateway, with explicit turn ownership, over the
//! realtime kind's events. Clients send their setup, then `clientContent`
//! turns or manual activity boundaries. Automatic activity detection and
//! proactive audio are turned off, so every generation has a reservation
//! first. Tool calls, audio, interruptions, goAway and session-resumption
//! handles pass as they came. Configure MODEL and MODEL_INPUT_LIMIT for the
//! model's complete input bound. Sessions end after 30 minutes, before the
//! one-hour hold expires.
//! Protocol: https://ai.google.dev/api/live
#[path = "../support/live/budget.rs"]
mod budget;
#[path = "../support/live/serve.rs"]
mod serve;
use budget::{Budget, OUTPUT};
use respan_gateway_core::futures::{StreamExt, TryStreamExt};
use respan_gateway_core::{BadRequest, ErrorCode, NativeFields};
use respan_gateway_core::{
    ClientContext, EventStream, Fault, Gateway, Plugin, Response, async_trait,
};
#[path = "../support/billing/ledger.rs"]
mod ledger;
use ledger::MemoryLedger;
use respan_gateway_realtime::{Input, Output, Session, Status, Turns};
use respan_gateway_realtime_ws::GeminiLive;
use respan_gateway_ws::{CloseCode, WsTransport};
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};
#[derive(Clone)]
struct Sessions {
    ledger: MemoryLedger,
    model: String,
    input_limit: i64,
    completed: Arc<Mutex<Vec<Value>>>,
}

/// What the Live API's setup keeps that the kind has no place for: each must
/// be an object if it is set.
const CONFIGURATION: &[&str] = &[
    "/setup/generationConfig",
    "/setup/realtimeInputConfig",
    "/setup/realtimeInputConfig/automaticActivityDetection",
    "/setup/proactivity",
];

/// The setup the budget allows: the configured `model`, capped output,
/// manual turns and no proactive audio.
fn configure(setup: &mut Session, model: &str) -> Result<(), Fault> {
    let native = setup
        .native
        .get_or_insert_with(|| NativeFields::new(GeminiLive::NAME, Default::default()));
    let mut kept = Value::Object(std::mem::take(&mut native.fields));
    if CONFIGURATION
        .iter()
        .any(|path| kept.pointer(path).is_some_and(|v| !v.is_object()))
    {
        return Err(BadRequest::fault("setup configuration must be an object"));
    }
    if setup.model.as_deref().is_some_and(|m| m != model) {
        return Err(BadRequest::fault(
            "model is outside the configured budget policy",
        ));
    }
    kept["setup"]["proactivity"]["proactiveAudio"] = false.into();
    if let Value::Object(kept) = kept {
        native.fields = kept;
    }
    setup.model = Some(model.to_owned());
    setup.max_output_tokens = u64::try_from(OUTPUT).ok();
    setup.turns = Some(Turns::Manual);
    Ok(())
}

/// What a turn's events said, until it ends.
#[derive(Default)]
struct Turn {
    used: Option<i64>,
    called: bool,
    interrupted: bool,
}

/// What `event` adds to its turn; at the turn's end, what it used and its
/// record. A response that calls tools goes on, in the same turn, once they
/// answer.
fn said(turn: &Mutex<Turn>, event: &Output) -> Option<(Option<i64>, Value)> {
    let mut turn = budget::lock(turn);
    match event {
        Output::Usage(used) => {
            let tokens = used.tokens.input_tokens + used.tokens.output_tokens;
            let tokens = i64::try_from(tokens).unwrap_or(i64::MAX);
            turn.used = Some(turn.used.unwrap_or(0).saturating_add(tokens));
        }
        Output::SpeechStarted { .. } => turn.interrupted = true,
        Output::ToolCall { .. } => turn.called = true,
        Output::ResponseDone { .. } if turn.called => turn.called = false,
        Output::ResponseDone { status, .. } => {
            let Turn {
                used, interrupted, ..
            } = std::mem::take(&mut *turn);
            let interrupted = interrupted || *status == Status::Interrupted;
            let status = if interrupted {
                "interrupted"
            } else {
                "complete"
            };
            return Some((used, json!({"status": status, "usage": used})));
        }
        _ => {}
    }
    None
}

#[async_trait]
impl Plugin for Sessions {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        if self.model.trim().is_empty() {
            return Err(Fault::config("configure an allowed model"));
        }
        let session = Arc::new(Budget::new(
            self.ledger.clone(),
            self.completed.clone(),
            self.input_limit,
        )?);
        let incoming = session.clone();
        let model = self.model.trim_start_matches("models/").to_owned();
        cx.wrap(move |events: EventStream<Input>| {
            events.and_then(move |mut event| {
                let session = incoming.clone();
                let model = model.clone();
                async move {
                    let active = session.active("turn");
                    match &mut event {
                        Input::Session(setup) => configure(setup, &model)?,
                        // A turn begins at its activity, or at what asks
                        // for an answer: a complete turn, a tool's answer.
                        Input::Native(native)
                            if native.value["realtimeInput"].get("activityStart").is_some() =>
                        {
                            session.begin("turn").await?
                        }
                        Input::Respond { .. } | Input::ToolResult(_) if !active => {
                            session.begin("turn").await?
                        }
                        Input::Audio { .. } | Input::Commit { .. } if !active => {
                            return Err(Fault::new("send activityStart before realtime input")
                                .with(ErrorCode("audio_policy".into()))
                                .with(CloseCode(1008)));
                        }
                        _ => {}
                    }
                    Ok(event)
                }
            })
        })?;
        let outgoing = session.clone();
        let turn = Arc::new(Mutex::new(Turn::default()));
        cx.wrap(move |events: EventStream<Output>| {
            events.and_then(move |event| {
                let session = outgoing.clone();
                let turn = turn.clone();
                async move {
                    if let Some((used, record)) = said(&turn, &event) {
                        session.finish("turn", used, record).await?;
                    }
                    Ok(event)
                }
            })
        })?;
        cx.wrap(|events: EventStream<Output>| {
            events.take_until(tokio::time::sleep(budget::SESSION_MAX))
        })?;
        cx.extensions_mut().insert(session);
        Ok(None)
    }
    async fn on_client_end(&self, cx: &ClientContext, _: &Result<(), Fault>) {
        if let Some(session) = cx.extensions().get::<Arc<Budget>>() {
            session.end().await;
        }
    }
}
fn gateway(transport: WsTransport, sessions: Sessions) -> Result<Gateway, Fault> {
    Gateway::new()
        .client::<Input, _, _>(GeminiLive)?
        .provider::<Input, _, _>("gemini", GeminiLive, transport.connect())?
        .plugin(sessions)
        .target("gemini")
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let key = std::env::var("GEMINI_API_KEY")?;
    let ledger = MemoryLedger::new();
    ledger.open("demo", None, 5_000_000);
    let sessions = Sessions {
        ledger,
        model: std::env::var("MODEL")?,
        input_limit: std::env::var("MODEL_INPUT_LIMIT")?.parse()?,
        completed: Arc::default(),
    };
    let mut headers = respan_gateway_http::http::HeaderMap::new();
    headers.insert("x-goog-api-key", key.parse()?);
    let gw = gateway(
        GeminiLive
            .transport("wss://generativelanguage.googleapis.com")?
            .with_headers(headers)?,
        sessions,
    )?;
    serve::run(gw, "127.0.0.1:8082").await?;
    Ok(())
}
#[cfg(test)]
#[path = "../tests/cases/realtime/gemini_live.rs"]
mod tests;
