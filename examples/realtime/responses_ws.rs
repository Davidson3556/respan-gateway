//! Responses over WebSocket: independent named lanes, continuation IDs, and per-turn billing.
//! Run against a real provider: `OPENAI_API_KEY=... cargo run -p respan-gateway-examples --example responses_ws`.
//! The proxy listens at 127.0.0.1:8081; clients send normal response.create events.
//! MODEL and MODEL_INPUT_LIMIT bind the allowed model to its full input-context quote.
//! Sessions end after 30 minutes, before the one-hour hold expires.
//! Unknown fields and previous_response_id pass through. A socket failure never replays a turn.
//! Protocol: https://developers.openai.com/api/docs/guides/websocket-mode
#[path = "../support/live/budget.rs"]
mod budget;
#[path = "../support/live/serve.rs"]
mod serve;
use budget::{Budget, OUTPUT};
use respan_gateway_core::futures::{StreamExt, TryStreamExt};
use respan_gateway_core::{
    ClientContext, EventStream, Fault, Gateway, Plugin, Provider, Response, async_trait,
};
#[path = "../support/billing/ledger.rs"]
mod ledger;
use ledger::MemoryLedger;
use respan_gateway_realtime::{Input, Output};
use respan_gateway_realtime_ws::ResponsesWs;
use respan_gateway_ws::WsTransport;
use serde_json::json;
use std::sync::Arc;
#[derive(Clone)]
struct Sessions {
    ledger: MemoryLedger,
    model: String,
    input_limit: i64,
    completed: Arc<std::sync::Mutex<Vec<serde_json::Value>>>,
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
                    let Input::Native(native) = &mut event else {
                        return Ok(event);
                    };
                    let json = Arc::make_mut(&mut native.value);
                    if json["type"] == "response.create" {
                        if json
                            .get("model")
                            .is_some_and(|v| v.as_str() != Some(&model))
                        {
                            return Err(Fault::bad_request(
                                "model is outside the configured budget policy",
                            ));
                        }
                        json["model"] = model.clone().into();
                    }
                    if json["type"] == "response.create" && json["generate"] != false {
                        let cap = &mut json["max_output_tokens"];
                        if cap.as_i64().is_none_or(|n| !(1..=OUTPUT).contains(&n)) {
                            *cap = OUTPUT.into();
                        }
                        let lane = json["stream_id"].as_str().unwrap_or("default");
                        session.begin(lane).await?;
                    }
                    Ok(event)
                }
            })
        })?;
        let outgoing = session.clone();
        cx.wrap(move |events: EventStream<Output>| {
            events.and_then(move |event| {
                let session = outgoing.clone();
                async move {
                    let Output::Native(native) = &event else {
                        return Ok(event);
                    };
                    let json = &native.value;
                    let kind = json["type"].as_str().unwrap_or_default();
                    if matches!(
                        kind,
                        "response.completed" | "response.failed" | "response.incomplete"
                    ) {
                        let lane = json["stream_id"].as_str().unwrap_or("default");
                        let response = &json["response"];
                        let usage = response["usage"]["total_tokens"].as_i64();
                        let record = json!({
                            "lane": lane, "id": response["id"],
                            "status": kind, "usage": response["usage"]
                        });
                        if session.active(lane) {
                            session.finish(lane, usage, record).await?;
                        }
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
    async fn on_client_end(&self, cx: &ClientContext, _end: &Result<(), Fault>) {
        if let Some(session) = cx.extensions().get::<Arc<Budget>>() {
            session.end().await;
        }
    }
}
fn gateway(transport: WsTransport, sessions: Sessions) -> Result<Gateway, Fault> {
    Gateway::new()
        .client::<Input, _, _>(ResponsesWs)?
        .provider(Provider::new("responses", ResponsesWs, transport.connect()))?
        .plugin(sessions)
        .target("responses")
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let key = std::env::var("OPENAI_API_KEY")?;
    let ledger = MemoryLedger::new();
    ledger.open("demo", None, 5_000_000);
    let sessions = Sessions {
        ledger,
        model: std::env::var("MODEL")?,
        input_limit: std::env::var("MODEL_INPUT_LIMIT")?.parse()?,
        completed: Arc::default(),
    };
    let mut headers = respan_gateway_http::http::HeaderMap::new();
    headers.insert("authorization", format!("Bearer {key}").parse()?);
    let gw = gateway(
        ResponsesWs
            .transport("wss://api.openai.com/v1")?
            .with_headers(headers)?,
        sessions,
    )?;
    serve::run(gw, "127.0.0.1:8081").await?;
    Ok(())
}
#[cfg(test)]
#[path = "../tests/cases/realtime/responses_ws.rs"]
mod tests;
