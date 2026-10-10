//! Gemini Live through the same JSON-frame transport, with explicit turn ownership.
//! Clients send setup, clientContent turns or manual realtimeInput activity boundaries.
//! Automatic/proactive generation is disabled so every generation has a reservation first.
//! Tool calls, audio, interruptions, goAway and session-resumption handles remain ordinary JSON.
//! Configure MODEL and MODEL_INPUT_LIMIT for the model's complete input bound.
//! Sessions end after 30 minutes, before the one-hour hold expires.
//! Protocol: https://ai.google.dev/api/live
#[path = "../support/live/budget.rs"]
mod budget;
#[path = "../support/live/serve.rs"]
mod serve;
use budget::{Budget, OUTPUT};
use respan_gateway_core::futures::{StreamExt, TryStreamExt};
use respan_gateway_core::{BadRequest, ErrorCode};
use respan_gateway_core::{
    ClientContext, EventStream, Fault, Gateway, Plugin, Response, async_trait,
};
#[path = "../support/billing/ledger.rs"]
mod ledger;
use ledger::MemoryLedger;
use respan_gateway_realtime::JsonEvents;
use respan_gateway_realtime::{Input, Output};
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
        let model = self.model.clone();
        cx.wrap(move |events: EventStream<Input>| {
            events.and_then(move |mut event| {
                let session = incoming.clone();
                let model = model.clone();
                async move {
                    if let Some(setup) = event.json_mut().get_mut("setup") {
                        if !setup.is_object() {
                            return Err(BadRequest::fault("setup must be an object"));
                        }
                        for path in [
                            "/generationConfig",
                            "/realtimeInputConfig",
                            "/realtimeInputConfig/automaticActivityDetection",
                            "/proactivity",
                        ] {
                            if setup.pointer(path).is_some_and(|value| !value.is_object()) {
                                return Err(BadRequest::fault(
                                    "setup configuration must be an object",
                                ));
                            }
                        }
                        if setup
                            .get("model")
                            .is_some_and(|v| v.as_str() != Some(&model))
                        {
                            return Err(BadRequest::fault(
                                "model is outside the configured budget policy",
                            ));
                        }
                        setup["model"] = model.clone().into();
                        setup["generationConfig"]["maxOutputTokens"] = OUTPUT.into();
                        setup["realtimeInputConfig"]["automaticActivityDetection"]["disabled"] =
                            true.into();
                        setup["proactivity"]["proactiveAudio"] = false.into();
                    }
                    if event.json()["clientContent"]["turnComplete"] == true
                        || event.json()["realtimeInput"].get("activityStart").is_some()
                    {
                        session.begin("turn").await?;
                    }
                    if event.json().get("toolResponse").is_some() && !session.active("turn") {
                        session.begin("turn").await?;
                    }
                    if event.json().get("realtimeInput").is_some() && !session.active("turn") {
                        return Err(Fault::new("send activityStart before realtime input")
                            .with(ErrorCode("audio_policy".into()))
                            .with(CloseCode(1008)));
                    }
                    Ok(event)
                }
            })
        })?;
        let outgoing = session.clone();
        let usage = Arc::new(Mutex::new((Value::Null, false)));
        cx.wrap(move |events: EventStream<Output>| {
            events.and_then(move |event| {
                let session = outgoing.clone();
                let usage = usage.clone();
                async move {
                    if let Some(value) = event.json().get("usageMetadata") {
                        budget::lock(&usage).0 = value.clone();
                    }
                    if event.json()["serverContent"]["interrupted"] == true {
                        budget::lock(&usage).1 = true;
                    }
                    if event.json()["serverContent"]["turnComplete"] == true {
                        let (used, interrupted) = std::mem::take(&mut *budget::lock(&usage));
                        session
                            .finish(
                                "turn",
                                used["totalTokenCount"].as_i64(),
                                json!({"status": if interrupted {"interrupted"} else {"complete"},"usage":used}),
                            )
                            .await?;
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
        .client::<Input, _, _>(JsonEvents::GEMINI_LIVE)?
        .provider::<Input, _, _>("gemini", JsonEvents::GEMINI_LIVE, transport.connect())?
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
        JsonEvents::GEMINI_LIVE
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
