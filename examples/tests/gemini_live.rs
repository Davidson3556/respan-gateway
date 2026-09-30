#![allow(clippy::unwrap_used)]
use super::*;
#[path = "../live/test.rs"]
mod support;
use support::*;
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
            json!({"serverContent":{"modelTurn":{"parts":[{"inlineData":{"mimeType":"audio/pcm;rate=24000","data":"AAAA"}}]},"turnComplete":true},"usageMetadata":{"totalTokenCount":23,"promptTokensDetails":[{"modality":"AUDIO","tokenCount":10}]}}),
            json!({"sessionResumptionUpdate":{"newHandle":"resume1","resumable":true}}),
        ]}vec![]
    },move|base|gateway(WsTransport::new(base).unwrap(),sessions).unwrap()).await;
    send(&mut live,json!({"setup":{"model":"models/test","sessionResumption":{},"generationConfig":{"maxOutputTokens":9000}}})).await;
    next(&mut live).await.unwrap();
    send(&mut live,json!({"clientContent":{"turns":[{"role":"user","parts":[{"text":"hello"}]}],"turnComplete":true}})).await;
    for _ in 0..3 {
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
    next(&mut live).await.unwrap();
    close(&mut live).await;
    assert_eq!(observed.ledger.spent("demo"), 46);
    let sent = live.received.lock().unwrap();
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
        |_| {
            vec![
                json!({"usageMetadata":{"totalTokenCount":11}}),
                json!({"serverContent":{"interrupted":true}}),
                json!({"serverContent":{"turnComplete":true}}),
            ]
        },
        move |base| gateway(WsTransport::new(base).unwrap(), sessions).unwrap(),
    )
    .await;
    send(&mut live, json!({"clientContent":{"turnComplete":true}})).await;
    for _ in 0..3 {
        next(&mut live).await.unwrap();
    }
    close(&mut live).await;
    assert_eq!(observed.ledger.spent("demo"), 11);
    assert_eq!(budget::lock(&observed.completed).len(), 1);
}
#[tokio::test]
async fn unreserved_audio_and_unaffordable_turns_are_refused() {
    for (balance, event) in [
        (
            1_000_000,
            json!({"realtimeInput":{"audio":{"data":"AAAA"}}}),
        ),
        (1, json!({"clientContent":{"turnComplete":true}})),
    ] {
        let sessions = configured(balance);
        let observed = sessions.clone();
        let mut live = start(
            PATH,
            |_| vec![],
            move |base| gateway(WsTransport::new(base).unwrap(), sessions).unwrap(),
        )
        .await;
        send(&mut live, event).await;
        assert!(next(&mut live).await.is_none());
        close(&mut live).await;
        assert!(live.received.lock().unwrap().is_empty());
        assert_eq!(observed.ledger.spent("demo"), 0);
    }
}

#[tokio::test]
async fn going_away_passes_unchanged_and_a_cut_turn_charges_its_reservation() {
    let sessions = configured(1_000_000);
    let observed = sessions.clone();
    let mut live = start(
        PATH,
        |_| vec![json!({"goAway":{"timeLeft":"10s"},"future":"preserved"})],
        move |base| gateway(WsTransport::new(base).unwrap(), sessions).unwrap(),
    )
    .await;
    send(&mut live, json!({"clientContent":{"turnComplete":true}})).await;
    let notice = next(&mut live).await.unwrap();
    assert_eq!(notice["goAway"]["timeLeft"], "10s");
    assert_eq!(notice["future"], "preserved");
    close(&mut live).await;
    assert_eq!(observed.ledger.spent("demo"), budget::QUOTE);
    assert_eq!(budget::lock(&observed.completed)[0]["status"], "cut");
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
    assert!(live.received.lock().unwrap().is_empty());
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
        assert!(live.received.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn a_tool_response_without_an_active_turn_reserves_before_generation() {
    let sessions = configured(1);
    let observed = sessions.clone();
    let mut live = start(
        PATH,
        |_| vec![],
        move |base| gateway(WsTransport::new(base).unwrap(), sessions).unwrap(),
    )
    .await;
    send(
        &mut live,
        json!({"toolResponse":{"functionResponses":[{"id":"f","response":{"result":"x"}}]}}),
    )
    .await;
    assert!(next(&mut live).await.is_none());
    close(&mut live).await;
    assert!(live.received.lock().unwrap().is_empty());
    assert_eq!(observed.ledger.spent("demo"), 0);
}
