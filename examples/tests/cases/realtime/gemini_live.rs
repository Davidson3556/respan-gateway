#![allow(clippy::unwrap_used)]
use super::*;
#[path = "../../../support/live/test.rs"]
mod support;
use support::*;
/// Where a Gemini Live client connects ([`GeminiLive`]).
const PATH: &str = "/ws/google.ai.generativelanguage.v1beta.GenerativeService.BidiGenerateContent";
/// The setup every session opens with.
fn setup() -> Value {
    json!({"setup": {"model": "models/test"}})
}
/// Answers a setup as the Live API does, and anything else with `then`.
fn after_setup(event: &Value, then: Vec<Value>) -> Vec<Value> {
    match event.get("setup") {
        Some(_) => vec![json!({"setupComplete": {}})],
        None => then,
    }
}
/// A session past its setup.
async fn set_up(live: &mut Live) {
    send(live, setup()).await;
    assert!(next(live).await.unwrap().get("setupComplete").is_some());
}
fn configured(balance: i64) -> Sessions {
    let ledger = MemoryLedger::new();
    ledger.open("demo", None, balance);
    Sessions {
        ledger,
        model: "models/test".into(),
        input_limit: 128_000,
        completed: Arc::default(),
    }
}
#[tokio::test]
async fn setup_audio_tools_and_resumption_pass_while_turns_are_metered() {
    let sessions = configured(1_000_000);
    let observed = sessions.clone();
    let mut live=start(PATH,|event|{
        if event.get("setup").is_some(){return vec![json!({"setupComplete":{}})]}
        if event["clientContent"]["turnComplete"]==true||event["realtimeInput"].get("activityEnd").is_some(){return vec![
            json!({"toolCall":{"functionCalls":[{"id":"f","name":"echo","args":{}}]}}),
            json!({"serverContent":{"modelTurn":{"parts":[{"inlineData":{"mimeType":"audio/pcm;rate=24000","data":"AAAA"}}]}}}),
            json!({"serverContent":{"turnComplete":true},"usageMetadata":{"promptTokenCount":13,"responseTokenCount":10,"totalTokenCount":23,"promptTokensDetails":[{"modality":"AUDIO","tokenCount":10}]}}),
            json!({"sessionResumptionUpdate":{"newHandle":"resume1","resumable":true}}),
        ]}vec![]
    },move|base|gateway(WsTransport::new(base).unwrap(),sessions).unwrap()).await;
    send(&mut live,json!({"setup":{"model":"models/test","sessionResumption":{},"generationConfig":{"maxOutputTokens":9000}}})).await;
    next(&mut live).await.unwrap();
    send(&mut live,json!({"clientContent":{"turns":[{"role":"user","parts":[{"text":"hello"}]}],"turnComplete":true}})).await;
    for _ in 0..4 {
        next(&mut live).await.unwrap();
    }
    send(&mut live, json!({"realtimeInput":{"activityStart":{}}})).await;
    send(
        &mut live,
        json!({"realtimeInput":{"audio":{"mimeType":"audio/pcm;rate=16000","data":"AAAA"}}}),
    )
    .await;
    send(&mut live, json!({"realtimeInput":{"activityEnd":{}}})).await;
    next(&mut live).await.unwrap();
    let audio = next(&mut live).await.unwrap();
    assert_eq!(
        audio["serverContent"]["modelTurn"]["parts"][0]["inlineData"]["data"],
        "AAAA"
    );
    let done = next(&mut live).await.unwrap();
    assert_eq!(done["usageMetadata"]["totalTokenCount"], 23);
    next(&mut live).await.unwrap();
    close(&mut live).await;
    assert_eq!(observed.ledger.spent("demo"), 46);
    let sent = live.received.lock();
    assert_eq!(
        sent[0]["setup"]["generationConfig"]["maxOutputTokens"],
        OUTPUT
    );
    assert_eq!(
        sent[0]["setup"]["realtimeInputConfig"]["automaticActivityDetection"]["disabled"],
        true
    );
    assert_eq!(sent[3]["realtimeInput"]["audio"]["data"], "AAAA");
}
#[tokio::test]
async fn interruptions_and_separate_usage_finish_once() {
    let sessions = configured(1_000_000);
    let observed = sessions.clone();
    let mut live = start(
        PATH,
        |event| {
            after_setup(
                &event,
                vec![
                    json!({"usageMetadata":{"promptTokenCount":4,"responseTokenCount":7}}),
                    json!({"serverContent":{"interrupted":true}}),
                    json!({"serverContent":{"turnComplete":true}}),
                ],
            )
        },
        move |base| gateway(WsTransport::new(base).unwrap(), sessions).unwrap(),
    )
    .await;
    set_up(&mut live).await;
    send(&mut live, json!({"clientContent":{"turnComplete":true}})).await;
    for _ in 0..3 {
        next(&mut live).await.unwrap();
    }
    close(&mut live).await;
    assert_eq!(observed.ledger.spent("demo"), 11);
    assert_eq!(observed.completed.lock().len(), 1);
}
#[tokio::test]
async fn unreserved_audio_and_unaffordable_turns_are_refused() {
    for (balance, event) in [
        (
            1_000_000,
            json!({"realtimeInput":{"audio":{"mimeType":"audio/pcm;rate=16000","data":"AAAA"}}}),
        ),
        (1, json!({"clientContent":{"turnComplete":true}})),
    ] {
        let sessions = configured(balance);
        let observed = sessions.clone();
        let mut live = start(
            PATH,
            |event| after_setup(&event, vec![]),
            move |base| gateway(WsTransport::new(base).unwrap(), sessions).unwrap(),
        )
        .await;
        set_up(&mut live).await;
        send(&mut live, event).await;
        assert!(next(&mut live).await.is_none());
        close(&mut live).await;
        assert_eq!(live.received.lock().len(), 1, "the setup alone");
        assert_eq!(observed.ledger.spent("demo"), 0);
    }
}

#[tokio::test]
async fn going_away_passes_unchanged_and_a_cut_turn_charges_its_reservation() {
    let sessions = configured(1_000_000);
    let observed = sessions.clone();
    let mut live = start(
        PATH,
        |event| {
            after_setup(
                &event,
                vec![json!({"goAway":{"timeLeft":"10s"},"future":"preserved"})],
            )
        },
        move |base| gateway(WsTransport::new(base).unwrap(), sessions).unwrap(),
    )
    .await;
    set_up(&mut live).await;
    send(&mut live, json!({"clientContent":{"turnComplete":true}})).await;
    let notice = next(&mut live).await.unwrap();
    assert_eq!(notice["goAway"]["timeLeft"], "10s");
    assert_eq!(notice["future"], "preserved");
    close(&mut live).await;
    assert_eq!(observed.ledger.spent("demo"), budget::QUOTE);
    assert_eq!(observed.completed.lock()[0]["status"], "cut");
}

#[tokio::test]
async fn setup_cannot_switch_to_a_model_outside_its_quote() {
    let sessions = configured(1_000_000);
    let mut live = start(
        PATH,
        |_| vec![],
        move |base| gateway(WsTransport::new(base).unwrap(), sessions).unwrap(),
    )
    .await;
    send(&mut live, json!({"setup":{"model":"models/unquoted"}})).await;
    assert!(next(&mut live).await.is_none());
    close(&mut live).await;
    assert!(live.received.lock().is_empty());
}

#[tokio::test]
async fn malformed_setup_configuration_is_refused_without_panicking() {
    for setup in [
        json!({"generationConfig":4}),
        json!({"realtimeInputConfig":false}),
        json!({"realtimeInputConfig":{"automaticActivityDetection":[]}}),
        json!({"proactivity":null}),
    ] {
        let sessions = configured(1_000_000);
        let mut live = start(
            PATH,
            |_| vec![],
            move |base| gateway(WsTransport::new(base).unwrap(), sessions).unwrap(),
        )
        .await;
        send(&mut live, json!({"setup":setup})).await;
        assert!(next(&mut live).await.is_none());
        close(&mut live).await;
        assert!(live.received.lock().is_empty());
    }
}

#[tokio::test]
async fn a_tool_response_without_an_active_turn_reserves_before_generation() {
    let sessions = configured(1);
    let observed = sessions.clone();
    let mut live = start(
        PATH,
        |event| after_setup(&event, vec![]),
        move |base| gateway(WsTransport::new(base).unwrap(), sessions).unwrap(),
    )
    .await;
    set_up(&mut live).await;
    send(
        &mut live,
        json!({"toolResponse":{"functionResponses":[{"id":"f","response":{"result":"x"}}]}}),
    )
    .await;
    assert!(next(&mut live).await.is_none());
    close(&mut live).await;
    assert_eq!(live.received.lock().len(), 1, "the setup alone");
    assert_eq!(observed.ledger.spent("demo"), 0);
}
