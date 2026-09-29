use super::*;
use mockllm::{Answer, FakeOpenAi};

/// A session against a scripted provider: the gateway's address, the
/// provider, and what the plugins recorded.
struct Session {
    openai: FakeOpenAi,
    gateway: Gateway,
    meter: Meter,
    ends: Outcomes,
    wallets: MemoryLedger,
    addr: std::net::SocketAddr,
}

async fn start() -> Result<Session, Fault> {
    start_for(HOUR).await
}

/// A session that may last `longest`.
async fn start_for(longest: Duration) -> Result<Session, Fault> {
    start_with(longest, 100_000).await
}

/// A session that may last `longest`, for a wallet of `tokens`.
async fn start_with(longest: Duration, tokens: Amount) -> Result<Session, Fault> {
    let openai = FakeOpenAi::start().await;
    let provider = openai.base_url().replace("http://", "ws://");
    let (meter, ends) = (Meter::default(), Outcomes::default());
    let wallets = MemoryLedger::new();
    wallets.open("sk-demo", None, tokens);
    let provider = provider.trim_end_matches("/v1");
    let gateway = gateway(provider, &meter, &ends, &wallets, longest)?;
    let addr = listen(&gateway).await;
    Ok(Session {
        openai,
        gateway,
        meter,
        ends,
        wallets,
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

/// The Close the client is sent: its code and reason. Events before it are
/// skipped; `None` if the connection broke without one.
async fn closed(ws: &mut Client) -> Option<(u16, String)> {
    use respan_gateway_core::futures::StreamExt;
    use tokio_tungstenite::tungstenite::Message;
    loop {
        let next = tokio::time::timeout(Duration::from_secs(5), ws.next()).await;
        match next.expect("a close in time") {
            Some(Ok(Message::Close(Some(frame)))) => {
                return Some((frame.code.into(), frame.reason.to_string()));
            }
            Some(Ok(_)) => {}
            _ => return None,
        }
    }
}

// A session past its maximum length is closed by its plugin, with a normal
// close; what it said before is still restored and metered.
#[tokio::test(flavor = "multi_thread")]
async fn a_session_is_closed_at_its_maximum_length() -> Result<(), Fault> {
    let s = start_for(Duration::from_millis(500)).await?;
    let reply = script(&["Noted: <EMA", "IL_1> ok."], usage(4, 2));
    s.openai.answer(Answer::events(reply));
    let mut ws = client(s.addr).await;
    let began = std::time::Instant::now();
    assert_eq!(heard(&mut ws).await["type"], "session.created");
    say(&mut ws, user_says("mail bob@x.io")).await;
    hear(&mut ws).await;
    say(&mut ws, json!({ "type": "response.create" })).await;
    assert_eq!(said(&turn(&mut ws).await), "Noted: bob@x.io ok.");

    // The client says nothing more: the session is cut at its deadline,
    // with a normal close.
    assert_eq!(closed(&mut ws).await, Some((1000, "max_length".into())));
    assert!(
        began.elapsed() < Duration::from_secs(3),
        "{:?}",
        began.elapsed()
    );
    s.gateway.idle().await;
    assert_eq!(s.meter.turns().len(), 1);
    // The call's end is `abandoned`, as for a client that left; the request's
    // end is what tells the cut apart.
    assert_eq!(
        s.meter.ended(),
        [Ended {
            session: 1,
            code: "abandoned".into(),
            turns: 1
        }]
    );
    assert_eq!(
        s.ends.only().map_err(|f| f.code().to_owned()),
        Err("max_length".into())
    );
    Ok(())
}

// The provider's head stays: a client that asks for a subprotocol (a browser
// must) gets the provider's answer echoed while a session is capped.
#[tokio::test(flavor = "multi_thread")]
async fn a_capped_session_keeps_the_subprotocol_the_provider_chose() -> Result<(), Fault> {
    use axum::extract::ws::{Message, WebSocketUpgrade};
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    let app = axum::Router::new().route(
        "/v1/realtime",
        axum::routing::get(|ws: WebSocketUpgrade| async move {
            ws.protocols(["realtime"])
                .on_upgrade(|mut socket| async move {
                    let hello = json!({ "type": "session.created" }).to_string();
                    let _ = socket.send(Message::Text(hello.into())).await;
                    while let Some(Ok(_)) = socket.recv().await {}
                })
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("a port");
    let at = listener.local_addr().expect("an address");
    tokio::spawn(async move { axum::serve(listener, app).await });
    let (meter, ends) = (Meter::default(), Outcomes::default());
    let wallets = MemoryLedger::new();
    wallets.open("sk-demo", None, 100_000);
    let gw = gateway(
        &format!("ws://{at}"),
        &meter,
        &ends,
        &wallets,
        Duration::from_millis(300),
    )?;
    let addr = listen(&gw).await;

    let mut req = format!("ws://{addr}/v1/realtime?model=gpt-realtime")
        .into_client_request()
        .expect("a request");
    req.headers_mut()
        .insert("authorization", "Bearer sk-demo".parse().expect("a value"));
    req.headers_mut().insert(
        "sec-websocket-protocol",
        "realtime".parse().expect("a value"),
    );
    let (mut ws, head) = tokio_tungstenite::connect_async(req)
        .await
        .expect("a handshake");
    assert_eq!(head.headers()["sec-websocket-protocol"], "realtime");
    assert_eq!(heard(&mut ws).await["type"], "session.created");
    assert_eq!(closed(&mut ws).await, Some((1000, "max_length".into())));
    Ok(())
}

// Use case 9: a turn is held on the client's wallet before the provider sees
// it, and settled at what it used. Eight sessions ask at once of a wallet
// that holds three turns: three are answered, and the wallet never goes
// negative at any moment, sampled, not only at the end.
#[tokio::test(flavor = "multi_thread")]
async fn parallel_sessions_cannot_overspend_one_wallet() -> Result<(), Fault> {
    let s = start_with(HOUR, 3 * TURN).await?;
    let sampler = sample(&s.wallets);
    for _ in 0..8 {
        let slow = Answer::events(script(&["ok"], usage(40, 9))).delay_first(300);
        s.openai.answer(slow);
    }
    let mut sessions = Vec::new();
    for _ in 0..8 {
        let addr = s.addr;
        sessions.push(tokio::spawn(async move {
            let mut ws = client(addr).await;
            hear(&mut ws).await;
            say(&mut ws, json!({ "type": "response.create" })).await;
            match hear(&mut ws).await {
                Some(_) => Ok(turn(&mut ws).await),
                None => Err(closed(&mut ws).await),
            }
        }));
    }
    let (mut answered, mut refused) = (0, 0);
    for session in sessions {
        match session.await.expect("a session") {
            Ok(_) => answered += 1,
            Err(_) => refused += 1,
        }
    }
    assert_eq!((answered, refused), (3, 5));
    let asked = read_by_provider(&s.openai);
    assert_eq!(
        asked.len(),
        3,
        "the provider saw only the turns held: {asked:?}"
    );
    s.gateway.idle().await;
    // Each answered turn settled at the 49 tokens it used.
    assert_eq!(s.wallets.spent("sk-demo"), 3 * 49);
    assert_eq!(s.wallets.available("sk-demo"), 3 * TURN - 3 * 49);
    let min = sampler.finish().await;
    assert!(min >= 0, "went negative: {min}");
    Ok(())
}

// A refused turn closes its client with 1008, and the wallet is untouched.
#[tokio::test(flavor = "multi_thread")]
async fn a_turn_the_wallet_cannot_cover_closes_the_session_with_1008() -> Result<(), Fault> {
    let s = start_with(HOUR, TURN / 2).await?;
    let mut ws = client(s.addr).await;
    hear(&mut ws).await;
    say(&mut ws, json!({ "type": "response.create" })).await;
    let close = closed(&mut ws).await;
    assert_eq!(close, Some((1008, "insufficient_funds".into())));
    assert!(read_by_provider(&s.openai).is_empty());
    s.gateway.idle().await;
    assert_eq!(s.wallets.available("sk-demo"), TURN / 2);
    Ok(())
}

// A turn cut before its `response.done` is charged its whole hold.
#[tokio::test(flavor = "multi_thread")]
async fn a_cut_turn_is_charged_what_it_held() -> Result<(), Fault> {
    let s = start_with(HOUR, 3 * TURN).await?;
    let cut = Answer::events(script(&["one", "two"], usage(10, 2))).cut_after(3);
    s.openai.answer(cut);
    let mut ws = client(s.addr).await;
    hear(&mut ws).await;
    say(&mut ws, json!({ "type": "response.create" })).await;
    while hear(&mut ws).await.is_some() {}
    s.gateway.idle().await;
    assert_eq!(s.wallets.spent("sk-demo"), TURN);
    Ok(())
}

// Use case 3 on a session: a remote classifier vets each input before the
// provider reads it; a flagged one closes the client with the fault's code.
#[tokio::test(flavor = "multi_thread")]
async fn a_flagged_input_never_reaches_the_provider_and_closes_the_session() -> Result<(), Fault> {
    let s = start().await?;
    let mut ws = client(s.addr).await;
    hear(&mut ws).await;
    say(&mut ws, user_says("a fine sentence")).await;
    assert_eq!(heard(&mut ws).await["type"], "conversation.item.added");
    say(&mut ws, user_says("an evil plan")).await;
    assert_eq!(closed(&mut ws).await, Some((1008, "flagged".into())));
    s.gateway.idle().await;
    let read = read_by_provider(&s.openai);
    assert_eq!(read.len(), 1, "{read:?}");
    assert!(!read[0].to_string().contains("evil"));
    assert_eq!(
        s.ends.only().map_err(|f| f.code().to_owned()),
        Err("flagged".into())
    );
    Ok(())
}

/// The events a raw provider read.
#[derive(Clone, Default)]
struct Raw {
    read: Arc<Mutex<Vec<Value>>>,
}

/// A provider that keeps to `max_output_tokens`, unlike a script: a
/// `response.create` is answered with a turn that would say `wants` tokens,
/// cut at the cap. An `input_audio_buffer.commit` starts a turn of `wants`
/// tokens by itself, as server voice detection does. A turn's usage is
/// its output tokens alone.
async fn raw_provider(raw: Raw, wants: u64) -> std::net::SocketAddr {
    use axum::extract::ws::{Message, WebSocketUpgrade};
    let app = axum::Router::new().route(
        "/v1/realtime",
        axum::routing::get(move |ws: WebSocketUpgrade| {
            let raw = raw.clone();
            async move {
                ws.protocols(["realtime"])
                    .on_upgrade(move |mut socket| async move {
                        let hello = json!({ "type": "session.created" }).to_string();
                        let _ = socket.send(Message::Text(hello.into())).await;
                        while let Some(Ok(Message::Text(text))) = socket.recv().await {
                            let event: Value = serde_json::from_str(&text).unwrap_or_default();
                            lock(&raw.read).push(event.clone());
                            let cap = event["response"]["max_output_tokens"].as_u64();
                            let tokens = match event["type"].as_str() {
                                Some("response.create") => wants.min(cap.unwrap_or(u64::MAX)),
                                Some("input_audio_buffer.commit") => wants,
                                _ => continue,
                            };
                            tokio::time::sleep(Duration::from_millis(100)).await;
                            for event in script(&["ok"], usage(0, tokens)) {
                                let _ = socket.send(Message::Text(event.to_string().into())).await;
                            }
                        }
                    })
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("a port");
    let at = listener.local_addr().expect("an address");
    tokio::spawn(async move { axum::serve(listener, app).await });
    at
}

/// The example's gateway in front of a raw provider, with a wallet of `tokens`.
async fn raw_session(
    wants: u64,
    tokens: Amount,
) -> (Raw, Gateway, Meter, MemoryLedger, std::net::SocketAddr) {
    let raw = Raw::default();
    let at = raw_provider(raw.clone(), wants).await;
    let (meter, ends) = (Meter::default(), Outcomes::default());
    let wallets = MemoryLedger::new();
    wallets.open("sk-demo", None, tokens);
    let provider = format!("ws://{at}");
    let gateway = gateway(&provider, &meter, &ends, &wallets, HOUR).expect("a gateway");
    let addr = listen(&gateway).await;
    (raw, gateway, meter, wallets, addr)
}

/// Every turn asked for is capped and held, whatever the client asks: with
/// three sessions of turns that would say 2,500 tokens against a wallet of
/// three caps, the balance is never below zero, and all three are answered.
#[tokio::test(flavor = "multi_thread")]
async fn turns_that_would_say_more_than_the_cap_cannot_overdraw_the_wallet() {
    let (raw, gw, _meter, wallets, addr) = raw_session(2_500, 3 * TURN).await;
    let sampler = sample(&wallets);
    let mut sessions = Vec::new();
    for _ in 0..3 {
        sessions.push(tokio::spawn(async move {
            let mut ws = client(addr).await;
            hear(&mut ws).await;
            say(&mut ws, json!({ "type": "response.create" })).await;
            let events = turn(&mut ws).await;
            close(&mut ws).await;
            last(&events)["response"]["usage"]["total_tokens"].clone()
        }));
    }
    for session in sessions {
        assert_eq!(session.await.expect("a session"), TURN);
    }
    gw.idle().await;
    assert_eq!(wallets.spent("sk-demo"), 3 * TURN);
    let min = sampler.finish().await;
    assert!(min >= 0, "went negative: {min}");
    let asked: Vec<_> = lock(&raw.read)
        .iter()
        .map(|e| e["response"].clone())
        .collect();
    assert_eq!(asked, vec![json!({ "max_output_tokens": TURN }); 3]);
}

/// A client's own smaller cap stays; a larger one, or none at all
/// (`"inf"`), becomes the cap.
#[tokio::test(flavor = "multi_thread")]
async fn a_response_create_asks_for_at_most_the_cap() {
    let (raw, gw, _meter, _wallets, addr) = raw_session(5, 100_000).await;
    let mut ws = client(addr).await;
    hear(&mut ws).await;
    let caps = [json!(200), json!(5_000), json!("inf"), json!(null)];
    for cap in caps {
        let response = json!({ "max_output_tokens": cap, "instructions": "brief" });
        say(
            &mut ws,
            json!({ "type": "response.create", "response": response }),
        )
        .await;
        turn(&mut ws).await;
    }
    close(&mut ws).await;
    gw.idle().await;
    let asked: Vec<_> = lock(&raw.read)
        .iter()
        .map(|e| e["response"]["max_output_tokens"].clone())
        .collect();
    assert_eq!(asked, [json!(200), json!(TURN), json!(TURN), json!(TURN)]);
    let all = lock(&raw.read);
    assert_eq!(all[0]["response"]["instructions"], "brief");
}

/// A turn holds what the session has said so far, plus the cap: a wallet
/// that covers only the cap refuses it once the session has a context.
#[tokio::test(flavor = "multi_thread")]
async fn a_turn_holds_what_the_session_has_said_plus_the_cap() {
    let (raw, gw, _meter, wallets, addr) = raw_session(5, TURN + 100).await;
    let mut ws = client(addr).await;
    hear(&mut ws).await;
    say(&mut ws, json!({ "type": "response.create" })).await;
    turn(&mut ws).await;
    let long = "a long question ".repeat(20);
    say(&mut ws, user_says(&long)).await;
    say(&mut ws, json!({ "type": "response.create" })).await;
    assert_eq!(
        closed(&mut ws).await,
        Some((1008, "insufficient_funds".into()))
    );
    gw.idle().await;
    assert_eq!(lock(&raw.read).len(), 2, "the second turn never reached it");
    assert_eq!(wallets.spent("sk-demo"), 5);
}

/// A turn the provider starts on its own is held when it begins: a funded
/// wallet is charged what it used, an empty one is cut with 1008 before
/// it hears the turn.
#[tokio::test(flavor = "multi_thread")]
async fn a_turn_the_provider_starts_is_held_and_charged() {
    let (_raw, gw, meter, wallets, addr) = raw_session(49, 3 * TURN).await;
    let mut ws = client(addr).await;
    hear(&mut ws).await;
    for _ in 0..3 {
        say(&mut ws, json!({ "type": "input_audio_buffer.commit" })).await;
        assert_eq!(last(&turn(&mut ws).await)["type"], "response.done");
    }
    close(&mut ws).await;
    gw.idle().await;
    assert_eq!(meter.turns().len(), 3);
    assert_eq!(wallets.spent("sk-demo"), 3 * 49);
    assert_eq!(wallets.available("sk-demo"), 3 * TURN - 3 * 49);

    let (_raw, gw, _meter, wallets, addr) = raw_session(49, 0).await;
    let mut ws = client(addr).await;
    hear(&mut ws).await;
    say(&mut ws, json!({ "type": "input_audio_buffer.commit" })).await;
    assert_eq!(
        closed(&mut ws).await,
        Some((1008, "insufficient_funds".into()))
    );
    gw.idle().await;
    assert_eq!(wallets.spent("sk-demo"), 0);
}

/// The key is read from whatever credential the client sends: a browser
/// puts it in the subprotocol, a script in the query; none is a 401.
#[tokio::test(flavor = "multi_thread")]
async fn the_wallet_is_found_by_any_credential_form() {
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    let (_raw, gw, _meter, wallets, addr) = raw_session(7, 3 * TURN).await;
    let forms = [
        (
            "/v1/realtime",
            Some("realtime, openai-insecure-api-key.sk-demo"),
        ),
        ("/v1/realtime?api_key=sk-demo", Some("realtime")),
    ];
    for (path, protocol) in forms {
        let mut req = format!("ws://{addr}{path}")
            .into_client_request()
            .expect("a request");
        if let Some(protocol) = protocol {
            req.headers_mut()
                .insert("sec-websocket-protocol", protocol.parse().expect("a value"));
        }
        let (mut ws, _) = tokio_tungstenite::connect_async(req)
            .await
            .expect("a session");
        hear(&mut ws).await;
        say(&mut ws, json!({ "type": "response.create" })).await;
        assert_eq!(last(&turn(&mut ws).await)["type"], "response.done");
        close(&mut ws).await;
    }
    gw.idle().await;
    assert_eq!(wallets.spent("sk-demo"), 2 * 7);

    let req = format!("ws://{addr}/v1/realtime")
        .into_client_request()
        .expect("a request");
    assert!(
        tokio_tungstenite::connect_async(req).await.is_err(),
        "no key, no session"
    );
}

/// The balance of `wallets`' account, sampled until finished.
struct Sampler(Arc<AtomicBool>, tokio::task::JoinHandle<Amount>);

fn sample(wallets: &MemoryLedger) -> Sampler {
    let (wallets, stop) = (wallets.clone(), Arc::new(AtomicBool::new(false)));
    let stopped = stop.clone();
    Sampler(
        stop,
        tokio::spawn(async move {
            let mut min = Amount::MAX;
            while !stopped.load(Ordering::SeqCst) {
                min = min.min(wallets.available("sk-demo"));
                tokio::time::sleep(Duration::from_micros(200)).await;
            }
            min
        }),
    )
}

impl Sampler {
    /// The lowest balance seen.
    async fn finish(self) -> Amount {
        self.0.store(true, Ordering::SeqCst);
        self.1.await.expect("a sampler")
    }
}
