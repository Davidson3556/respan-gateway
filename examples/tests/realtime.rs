use super::*;
use mockllm::{Answer, FakeOpenAi};

/// A session against a scripted provider: the gateway's address, the
/// provider, and what the plugins recorded.
struct Session {
    openai: FakeOpenAi,
    gateway: Gateway,
    meter: Meter,
    ends: Outcomes,
    addr: std::net::SocketAddr,
}

async fn start() -> Result<Session, Fault> {
    let openai = FakeOpenAi::start().await;
    let provider = openai.base_url().replace("http://", "ws://");
    let (meter, ends) = (Meter::default(), Outcomes::default());
    let gateway = gateway(provider.trim_end_matches("/v1"), &meter, &ends)?;
    let addr = listen(&gateway).await;
    Ok(Session {
        openai,
        gateway,
        meter,
        ends,
        addr,
    })
}

/// The events the provider read, in order.
fn read_by_provider(openai: &FakeOpenAi) -> Vec<Value> {
    let all = openai.requests();
    all.iter()
        .filter(|r| r.method == "WS")
        .map(|r| r.body.clone())
        .collect()
}

/// The next event, which must come.
async fn heard(ws: &mut Client) -> Value {
    match hear(ws).await {
        Some(event) => event,
        None => panic!("the session ended"),
    }
}

/// The first event of `kind` in `events`.
fn the<'a>(events: &'a [Value], kind: &str) -> &'a Value {
    match events.iter().find(|e| e["type"] == kind) {
        Some(event) => event,
        None => panic!("no {kind} in {events:?}"),
    }
}

fn last(events: &[Value]) -> &Value {
    match events.last() {
        Some(event) => event,
        None => panic!("no events"),
    }
}

fn usage(input: u64, output: u64) -> Value {
    json!({ "input_tokens": input, "output_tokens": output, "total_tokens": input + output })
}

async fn close(ws: &mut Client) {
    use respan_gateway_core::futures::SinkExt;
    let _ = ws
        .send(tokio_tungstenite::tungstenite::Message::Close(None))
        .await;
    while hear_any(ws).await {}
}

/// Whether anything more came, of any kind.
async fn hear_any(ws: &mut Client) -> bool {
    use respan_gateway_core::futures::StreamExt;
    let next = tokio::time::timeout(std::time::Duration::from_secs(5), ws.next()).await;
    matches!(next, Ok(Some(Ok(_))))
}

#[tokio::test(flavor = "multi_thread")]
async fn an_address_the_user_says_never_reaches_the_provider_and_comes_back_across_deltas()
-> Result<(), Fault> {
    let s = start().await?;
    // The provider repeats a placeholder split across two deltas, and a
    // second one whole, in the deltas and in every event that says the text again.
    let reply = script(&["Noted: <EMA", "IL_1> and <EMAIL_2>."], usage(40, 9));
    s.openai.answer(Answer::events(reply));
    let mut ws = client(s.addr).await;
    assert_eq!(heard(&mut ws).await["type"], "session.created");
    say(&mut ws, user_says("Mail bob@x.io or <alice@y.org> soon")).await;
    assert_eq!(heard(&mut ws).await["type"], "conversation.item.added");
    say(&mut ws, json!({ "type": "response.create" })).await;
    let events = turn(&mut ws).await;

    // What the provider read has placeholders and no address.
    let sent = read_by_provider(&s.openai);
    let text = &sent[0]["item"]["content"][0]["text"];
    assert_eq!(text, "Mail <EMAIL_1> or <<EMAIL_2>> soon");
    assert!(
        sent.iter().all(|e| !e.to_string().contains('@')),
        "{sent:?}"
    );

    // The client hears the addresses back, whole, wherever the text is.
    assert_eq!(said(&events), "Noted: bob@x.io and alice@y.org.");
    let all = events.iter().map(Value::to_string).collect::<String>();
    assert!(!all.contains("<EMAIL"), "{all}");
    let done = the(&events, "response.output_text.done");
    assert_eq!(done["text"], "Noted: bob@x.io and alice@y.org.");
    let last = last(&events);
    assert_eq!(
        last["response"]["output"][0]["content"][0]["text"],
        "Noted: bob@x.io and alice@y.org."
    );
    close(&mut ws).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn text_that_holds_no_address_passes_as_it_came() -> Result<(), Fault> {
    let s = start().await?;
    // Text ending in "<" and "<EM", which may start a placeholder, is held
    // and released as it was when the next delta shows it is not one.
    let reply = script(&["a < b, <EM", "PTY> and </EMA"], usage(1, 1));
    s.openai.answer(Answer::events(reply));
    let mut ws = client(s.addr).await;
    hear(&mut ws).await;
    say(&mut ws, json!({ "type": "response.create" })).await;
    let events = turn(&mut ws).await;
    assert_eq!(said(&events), "a < b, <EMPTY> and </EMA");
    // Nothing in the session's other events changed either.
    assert_eq!(events[0]["type"], "response.created");
    assert_eq!(last(&events)["response"]["usage"], usage(1, 1));
    close(&mut ws).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn each_turn_is_metered_when_it_completes_not_when_the_session_ends() -> Result<(), Fault> {
    let s = start().await?;
    s.openai
        .answer(Answer::events(script(&["one"], usage(10, 2))));
    s.openai
        .answer(Answer::events(script(&["two"], usage(30, 5))));
    let mut ws = client(s.addr).await;
    hear(&mut ws).await;
    say(&mut ws, json!({ "type": "response.create" })).await;
    turn(&mut ws).await;
    // The first turn is metered while the session goes on.
    let one = Turn {
        session: 1,
        status: "completed".into(),
        input_tokens: 10,
        output_tokens: 2,
    };
    assert_eq!(s.meter.turns(), std::slice::from_ref(&one));
    assert!(s.meter.ended().is_empty());
    say(&mut ws, json!({ "type": "response.create" })).await;
    turn(&mut ws).await;
    let two = Turn {
        session: 1,
        status: "completed".into(),
        input_tokens: 30,
        output_tokens: 5,
    };
    assert_eq!(s.meter.turns(), [one, two]);
    close(&mut ws).await;
    s.gateway.idle().await;
    // The end is recorded once, with the turns it metered.
    assert_eq!(
        s.meter.ended(),
        [Ended {
            session: 1,
            code: "ok".into(),
            turns: 2
        }]
    );
    assert_eq!(s.ends.only(), Ok(()));
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn sessions_are_metered_apart() -> Result<(), Fault> {
    let s = start().await?;
    for n in 1..=3 {
        s.openai.answer(Answer::events(script(&["x"], usage(n, n))));
    }
    let mut sessions = Vec::new();
    for _ in 0..3 {
        sessions.push(client(s.addr).await);
    }
    for ws in &mut sessions {
        hear(ws).await;
        say(ws, json!({ "type": "response.create" })).await;
        turn(ws).await;
    }
    for ws in &mut sessions {
        close(ws).await;
    }
    s.gateway.idle().await;
    let mut by_session: Vec<(u64, usize)> = s
        .meter
        .ended()
        .iter()
        .map(|e| (e.session, e.turns))
        .collect();
    by_session.sort();
    assert_eq!(by_session, [(1, 1), (2, 1), (3, 1)]);
    let mut used: Vec<u64> = s.meter.turns().iter().map(|t| t.input_tokens).collect();
    used.sort();
    assert_eq!(used, [1, 2, 3]);
    Ok(())
}

// A response cut before its `response.done` is still billed, from an
// estimate of the text it sent (use case 9), and the session says how it ended.
#[tokio::test(flavor = "multi_thread")]
async fn a_turn_cut_before_it_is_done_is_billed_from_an_estimate() -> Result<(), Fault> {
    let s = start().await?;
    s.openai
        .answer(Answer::events(script(&["one", "two"], usage(10, 2))).cut_after(3));
    let mut ws = client(s.addr).await;
    hear(&mut ws).await;
    say(&mut ws, json!({ "type": "response.create" })).await;
    let mut heard = 0;
    while hear(&mut ws).await.is_some() {
        heard += 1;
    }
    assert_eq!(heard, 3, "the events before the cut");
    s.gateway.idle().await;
    let turns = s.meter.turns();
    assert_eq!(turns.len(), 1, "{turns:?}");
    assert_eq!(turns[0].status, "cut");
    assert!(turns[0].output_tokens > 0, "billed at zero: {turns:?}");
    assert_eq!(
        s.meter.ended(),
        [Ended {
            session: 1,
            code: "truncated".into(),
            turns: 1
        }]
    );
    Ok(())
}

// A turn cut before any text came may have cost all it was allowed: it is
// charged its hold as output, and its input from what the client said.
#[tokio::test(flavor = "multi_thread")]
async fn a_turn_cut_before_any_delta_is_billed_at_least_its_hold() -> Result<(), Fault> {
    let s = start().await?;
    s.openai
        .answer(Answer::events(script(&["one", "two"], usage(10, 2))).cut_after(1));
    let mut ws = client(s.addr).await;
    hear(&mut ws).await;
    say(&mut ws, user_says(&"a long question ".repeat(10))).await;
    assert_eq!(heard(&mut ws).await["type"], "conversation.item.added");
    say(&mut ws, json!({ "type": "response.create" })).await;
    while hear(&mut ws).await.is_some() {}
    s.gateway.idle().await;
    let turns = s.meter.turns();
    assert_eq!(turns.len(), 1, "{turns:?}");
    assert_eq!(turns[0].status, "cut");
    assert!(turns[0].output_tokens >= HOLD, "under its hold: {turns:?}");
    assert!(
        turns[0].input_tokens >= 40,
        "input not estimated: {turns:?}"
    );
    Ok(())
}

// A client that leaves in the middle of a turn: recorded abandoned.
#[tokio::test(flavor = "multi_thread")]
async fn a_client_that_leaves_is_recorded_abandoned() -> Result<(), Fault> {
    let s = start().await?;
    s.openai
        .answer(Answer::events(script(&["one"], usage(10, 2))));
    let mut ws = client(s.addr).await;
    hear(&mut ws).await;
    drop(ws);
    s.gateway.idle().await;
    assert_eq!(
        s.meter.ended(),
        [Ended {
            session: 1,
            code: "abandoned".into(),
            turns: 0
        }]
    );
    Ok(())
}
