#![allow(clippy::unwrap_used)]
use super::*;
use serde_json::Value;
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
async fn an_unrelated_error_does_not_release_the_running_turn() {
    let sessions = configured(1_000_000);
    let observed = sessions.clone();
    let mut live = start(PATH, |_| vec![
        json!({"type":"response.created","response":{"id":"r1"}}),
        json!({"type":"error","error":{"code":"invalid_cancel","event_id":"other"}}),
        json!({"type":"response.completed","response":{"id":"r1","usage":{"total_tokens":37}}}),
    ], move |base| gateway(WsTransport::new(base).unwrap(), sessions).unwrap()).await;
    send(&mut live, json!({"type":"response.create","input":"x"})).await;
    for _ in 0..3 {
        next(&mut live).await.unwrap();
    }
    close(&mut live).await;
    assert_eq!(observed.ledger.spent("demo"), 37);
    assert_eq!(budget::lock(&observed.completed).len(), 1);
}
#[tokio::test]
async fn continuations_keep_ids_and_cap_each_turn_while_metering_lanes() {
    let sessions = configured(1_000_000);
    let observed = sessions.clone();
    let mut live=start(PATH,|event|{
        if event["type"]!="response.create"{return vec![]}
        let lane=&event["stream_id"];
        vec![json!({"type":"response.completed","stream_id":lane,"response":{"id":format!("r{}",event["input"]),"usage":{"total_tokens":17}},"future":{"kept":true}})]
    },move|base|gateway(WsTransport::new(base).unwrap(),sessions).unwrap()).await;
    for (lane, previous) in [("a", Value::Null), ("b", Value::Null), ("a", json!("r1"))] {
        send(&mut live,json!({"type":"response.create","stream_id":lane,"input":1,"previous_response_id":previous,"max_output_tokens":9000,"future":{"keep":"x"}})).await;
        let out = next(&mut live).await.unwrap();
        assert_eq!(out["future"]["kept"], true);
    }
    close(&mut live).await;
    let sent = live.received.lock().unwrap();
    assert_eq!(sent[2]["previous_response_id"], "r1");
    assert!(sent.iter().all(|e| e["max_output_tokens"] == OUTPUT));
    assert_eq!(observed.ledger.spent("demo"), 51);
    assert_eq!(budget::lock(&observed.completed).len(), 3);
}
#[tokio::test]
async fn failed_and_cancelled_turns_settle_usage_without_replaying() {
    let sessions = configured(1_000_000);
    let observed = sessions.clone();
    let mut live = start(
        PATH,
        |event| {
            let kind = if event["input"] == "cancel" {
                "response.incomplete"
            } else {
                "response.failed"
            };
            vec![json!({"type":kind,"response":{"id":"r","usage":{"total_tokens":9}}})]
        },
        move |base| gateway(WsTransport::new(base).unwrap(), sessions).unwrap(),
    )
    .await;
    for text in ["fail", "cancel"] {
        send(&mut live, json!({"type":"response.create","input":text})).await;
        next(&mut live).await.unwrap();
    }
    close(&mut live).await;
    assert_eq!(live.received.lock().unwrap().len(), 2);
    assert_eq!(observed.ledger.spent("demo"), 18);
}
#[tokio::test]
async fn budget_refusal_never_reaches_provider_and_a_cut_turn_keeps_its_charge() {
    for (balance, sent) in [(1, false), (1_000_000, true)] {
        let sessions = configured(balance);
        let observed = sessions.clone();
        let mut live = start(
            PATH,
            |_| vec![json!({"type":"response.in_progress"})],
            move |base| gateway(WsTransport::new(base).unwrap(), sessions).unwrap(),
        )
        .await;
        send(&mut live, json!({"type":"response.create","input":"x"})).await;
        assert_eq!(next(&mut live).await.is_some(), sent);
        close(&mut live).await;
        assert_eq!(!live.received.lock().unwrap().is_empty(), sent);
        assert_eq!(
            observed.ledger.spent("demo"),
            if sent { budget::QUOTE } else { 0 }
        );
    }
}

#[tokio::test]
async fn concurrent_lanes_finish_in_reverse_order_and_warmup_has_no_charge() {
    let sessions = configured(1_000_000);
    let observed = sessions.clone();
    let mut live=start(PATH,|event|{
        if event["generate"]==false{return vec![json!({"type":"response.completed","stream_id":"warm","response":{"id":"warm1"}})]}
        if event["stream_id"]=="a"{return vec![json!({"type":"response.in_progress","stream_id":"a"})]}
        vec![json!({"type":"response.completed","stream_id":"b","response":{"id":"b1","usage":{"total_tokens":4}}}),
            json!({"type":"response.completed","stream_id":"a","response":{"id":"a1","usage":{"total_tokens":7}}})]
    },move|base|gateway(WsTransport::new(base).unwrap(),sessions).unwrap()).await;
    send(
        &mut live,
        json!({"type":"response.create","stream_id":"warm","generate":false}),
    )
    .await;
    next(&mut live).await.unwrap();
    send(&mut live, json!({"type":"response.create","stream_id":"a"})).await;
    next(&mut live).await.unwrap();
    send(&mut live, json!({"type":"response.create","stream_id":"b"})).await;
    next(&mut live).await.unwrap();
    next(&mut live).await.unwrap();
    assert_eq!(observed.ledger.spent("demo"), 11);
    assert_eq!(
        budget::lock(&observed.completed).len(),
        2,
        "records visible while socket stays open"
    );
    close(&mut live).await;
    assert_eq!(budget::lock(&observed.completed)[0]["lane"], "b");
}

#[tokio::test]
async fn model_switches_are_rejected_before_admission() {
    let sessions = configured(1_000_000);
    let mut live = start(
        PATH,
        |_| vec![],
        move |base| gateway(WsTransport::new(base).unwrap(), sessions).unwrap(),
    )
    .await;
    send(
        &mut live,
        json!({"type":"response.create","model":"unquoted-expensive-model"}),
    )
    .await;
    assert!(next(&mut live).await.is_none());
    close(&mut live).await;
    assert!(live.received.lock().unwrap().is_empty());
}

#[tokio::test]
async fn live_turns_end_before_their_reservations_expire() {
    let sessions = configured(1_000_000);
    let observed = sessions.clone();
    let mut live = start(
        PATH,
        |_| vec![json!({"type":"response.in_progress"})],
        move |base| gateway(WsTransport::new(base).unwrap(), sessions).unwrap(),
    )
    .await;
    send(&mut live, json!({"type":"response.create"})).await;
    next(&mut live).await.unwrap();
    tokio::time::pause();
    tokio::time::advance(budget::SESSION_MAX + std::time::Duration::from_secs(1)).await;
    tokio::time::resume();
    assert!(next(&mut live).await.is_none());
    close(&mut live).await;
    assert_eq!(observed.ledger.spent("demo"), budget::QUOTE);
}
