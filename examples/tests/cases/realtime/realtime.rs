use super::*;
use mockllm::{Answer, FakeOpenAi};
use respan_gateway_realtime::Input;

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

/// A wallet that covers `turns` turns of a short session: a turn holds the
/// cap plus the bytes seen so far (the scripts here send under 1,000).
fn wallet_of(turns: Amount) -> Amount {
    turns * (TURN + 1_000)
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
    let gateway = gateway.plugin(respan_gateway_http::relay::Relay::RAW);
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

/// The events the provider read, in order, but for the `session.update` the
/// gateway opens each session with.
fn read_by_provider(openai: &FakeOpenAi) -> Vec<Value> {
    let all = openai.requests();
    all.iter()
        .filter(|r| r.method == "WS" && r.body["type"] != "session.update")
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
        input_audio_tokens: 0,
        output_audio_tokens: 0,
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
        input_audio_tokens: 0,
        output_audio_tokens: 0,
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
    assert_eq!(s.ends.only().map_err(|f| f.message().to_owned()), Ok(()));
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

/// The Close the client is sent: its code and reason. EventStream before it are
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
    // The cut's fault ends the call too, so the meter tells it apart from a
    // client that left.
    assert_eq!(
        s.meter.ended(),
        [Ended {
            session: 1,
            code: "max_length".into(),
            turns: 1
        }]
    );
    assert_eq!(
        s.ends
            .only()
            .map_err(|f| respan_gateway_http::error_code(&f).to_owned()),
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
    let gw = gw.plugin(respan_gateway_http::relay::Relay::RAW);
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
    let s = start_with(HOUR, wallet_of(3)).await?;
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
    assert_eq!(s.wallets.available("sk-demo"), wallet_of(3) - 3 * 49);
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
    let s = start_with(HOUR, wallet_of(3)).await?;
    let cut = Answer::events(script(&["one", "two"], usage(10, 2))).cut_after(3);
    s.openai.answer(cut);
    let mut ws = client(s.addr).await;
    hear(&mut ws).await;
    say(&mut ws, json!({ "type": "response.create" })).await;
    while hear(&mut ws).await.is_some() {}
    s.gateway.idle().await;
    // The cap, plus the bytes of the events the session had seen.
    let spent = s.wallets.spent("sk-demo");
    assert!((TURN..TURN + 1_000).contains(&spent), "spent {spent}");
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
        s.ends
            .only()
            .map_err(|f| respan_gateway_http::error_code(&f).to_owned()),
        Err("flagged".into())
    );
    Ok(())
}

/// The events a raw provider read.
#[derive(Clone, Default)]
struct Raw {
    read: Arc<Mutex<Vec<Value>>>,
    /// The `session.update`s, apart.
    sessions: Arc<Mutex<Vec<Value>>>,
}

/// A provider that keeps to `max_output_tokens`, unlike a script: a
/// `response.create` is answered with a turn that would say `wants` tokens,
/// cut at the cap of the response or of the session. An
/// `input_audio_buffer.commit` starts a turn of `wants` tokens by itself, as
/// server voice detection does, cut at the session's cap. A turn's usage is
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
                        let mut session_cap = u64::MAX;
                        while let Some(Ok(Message::Text(text))) = socket.recv().await {
                            let event: Value = serde_json::from_str(&text).unwrap_or_default();
                            if event["type"] == "session.update" {
                                let cap = event["session"]["max_output_tokens"].as_u64();
                                session_cap = cap.unwrap_or(session_cap);
                                lock(&raw.sessions).push(event);
                                let ack = json!({ "type": "session.updated" }).to_string();
                                let _ = socket.send(Message::Text(ack.into())).await;
                                continue;
                            }
                            lock(&raw.read).push(event.clone());
                            let cap = event["response"]["max_output_tokens"].as_u64();
                            let cap = cap.unwrap_or(u64::MAX).min(session_cap);
                            let tokens = match event["type"].as_str() {
                                Some("response.create") => wants.min(cap),
                                Some("input_audio_buffer.commit") => wants.min(session_cap),
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
    let gateway = gateway.plugin(respan_gateway_http::relay::Relay::RAW);
    let addr = listen(&gateway).await;
    (raw, gateway, meter, wallets, addr)
}

/// Every turn asked for is capped and held, whatever the client asks: with
/// three sessions of turns that would say 2,500 tokens against a wallet of
/// three caps, the balance is never below zero, and all three are answered.
#[tokio::test(flavor = "multi_thread")]
async fn turns_that_would_say_more_than_the_cap_cannot_overdraw_the_wallet() {
    let (raw, gw, _meter, wallets, addr) = raw_session(2_500, wallet_of(3)).await;
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
    let (raw, gw, _meter, wallets, addr) = raw_session(5, TURN + 1_500).await;
    let mut ws = client(addr).await;
    hear(&mut ws).await;
    say(&mut ws, json!({ "type": "response.create" })).await;
    turn(&mut ws).await;
    let long = "a long question ".repeat(200);
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
    let (_raw, gw, meter, wallets, addr) = raw_session(49, wallet_of(3)).await;
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
    assert_eq!(wallets.available("sk-demo"), wallet_of(3) - 3 * 49);

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
    let (_raw, gw, _meter, wallets, addr) = raw_session(7, wallet_of(3)).await;
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

/// A ledger across a network: a `reserve` lands at once and is acknowledged
/// after `ack`; a `settle` lands after `lag`, so one dropped before then
/// never lands.
#[derive(Clone)]
struct Remote {
    books: MemoryLedger,
    ack: Duration,
    lag: Duration,
}

#[respan_gateway_core::async_trait]
impl Ledger for Remote {
    async fn reserve(
        &self,
        id: &HoldId,
        accounts: &[String],
        amount: Amount,
        ttl: Duration,
    ) -> Result<(), Fault> {
        let held = self.books.reserve(id, accounts, amount, ttl).await;
        tokio::time::sleep(self.ack).await;
        held
    }

    async fn settle(&self, id: &HoldId, spent: Amount) -> Result<(), Fault> {
        tokio::time::sleep(self.lag).await;
        self.books.settle(id, spent).await
    }
}

async fn remote_session(
    ack: u64,
    lag: u64,
) -> Result<(FakeOpenAi, Gateway, MemoryLedger, std::net::SocketAddr), Fault> {
    let openai = FakeOpenAi::start().await;
    let provider = openai.base_url().replace("http://", "ws://");
    let books = MemoryLedger::new();
    books.open("sk-demo", None, 100_000);
    let remote = Remote {
        books: books.clone(),
        ack: Duration::from_millis(ack),
        lag: Duration::from_millis(lag),
    };
    let gw = Gateway::new()
        .client::<Input, _, _>(OpenAiRealtime)?
        .provider::<Input, _, _>(
            "openai",
            OpenAiRealtime,
            WsTransport::new(provider.trim_end_matches("/v1"))?.connect(),
        )?
        .target("openai")?
        .plugin(Reserve(remote));
    let gw = gw.plugin(respan_gateway_http::relay::Relay::RAW);
    let addr = listen(&gw).await;
    Ok((openai, gw, books, addr))
}

/// A client that leaves while its turn's hold is being acknowledged: the
/// hold landed, so the session's end must settle it (charge or release).
/// Left, it stays held for `HOUR`, and the wallet cannot spend it.
#[tokio::test(flavor = "multi_thread")]
async fn l5_a_hold_that_landed_as_the_client_left_is_settled() -> Result<(), Fault> {
    let (openai, gw, books, addr) = remote_session(300, 0).await?;
    openai.answer(Answer::events(script(&["ok"], usage(40, 9))));
    let mut ws = client(addr).await;
    hear(&mut ws).await;
    say(&mut ws, json!({ "type": "response.create" })).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    drop(ws);
    gw.idle().await;
    tokio::time::sleep(Duration::from_millis(600)).await;
    let (spent, left) = (books.spent("sk-demo"), books.available("sk-demo"));
    eprintln!(
        "spent {spent} left {left} provider read {:?}",
        read_by_provider(&openai)
    );
    assert_eq!(
        spent + left,
        100_000,
        "a hold outlived its session: spent {spent}, available {left}"
    );
    Ok(())
}

/// A client that leaves while its turn's `response.done` is being settled:
/// the turn must be settled once, not left held.
#[tokio::test(flavor = "multi_thread")]
async fn l5_a_turn_being_settled_as_the_client_left_is_settled() -> Result<(), Fault> {
    let (openai, gw, books, addr) = remote_session(0, 300).await?;
    openai.answer(Answer::events(script(&["ok"], usage(40, 9))));
    let mut ws = client(addr).await;
    hear(&mut ws).await;
    say(&mut ws, json!({ "type": "response.create" })).await;
    // The turn's events up to its `response.done`, which waits on the ledger.
    assert_eq!(heard(&mut ws).await["type"], "response.created");
    tokio::time::sleep(Duration::from_millis(100)).await;
    drop(ws);
    gw.idle().await;
    tokio::time::sleep(Duration::from_millis(600)).await;
    let (spent, left) = (books.spent("sk-demo"), books.available("sk-demo"));
    assert_eq!(
        spent + left,
        100_000,
        "a hold outlived its session: spent {spent}, available {left}"
    );
    Ok(())
}
/// When a request ends, for timing.
#[derive(Clone, Default)]
struct EndAt(Arc<Mutex<Vec<(std::time::Instant, String)>>>);

#[respan_gateway_core::async_trait]
impl Plugin for EndAt {
    async fn on_client_end(&self, _: &ClientContext, end: &Result<(), Fault>) {
        let code = end.as_ref().err().map_or("ok".into(), |f| {
            respan_gateway_http::error_code(f).to_owned()
        });
        lock(&self.0).push((std::time::Instant::now(), code));
    }
}

/// A client that leaves while an input wrap waits (a remote check, a
/// hold): the session must end when the client leaves, and the input it
/// waited on must not reach the provider afterwards.
#[tokio::test(flavor = "multi_thread")]
async fn l5_a_client_that_leaves_during_a_wrap_ends_its_session_then() -> Result<(), Fault> {
    struct Slow;
    #[respan_gateway_core::async_trait]
    impl Plugin for Slow {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            cx.wrap(|events: EventStream<Input>| {
                events.and_then(|e| async move {
                    tokio::time::sleep(Duration::from_millis(400)).await;
                    Ok(e)
                })
            })?;
            Ok(None)
        }
    }
    let openai = FakeOpenAi::start().await;
    let provider = openai.base_url().replace("http://", "ws://");
    let at = EndAt::default();
    let gw = Gateway::new()
        .client::<Input, _, _>(OpenAiRealtime)?
        .provider::<Input, _, _>(
            "openai",
            OpenAiRealtime,
            WsTransport::new(provider.trim_end_matches("/v1"))?.connect(),
        )?
        .target("openai")?
        .plugin(at.clone())
        .plugin(Slow);
    let gw = gw.plugin(respan_gateway_http::relay::Relay::RAW);
    let addr = listen(&gw).await;
    let mut ws = client(addr).await;
    hear(&mut ws).await;
    say(&mut ws, json!({ "type": "response.create" })).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    let left = std::time::Instant::now();
    drop(ws);
    gw.idle().await;
    let ended = lock(&at.0).clone();
    let after = ended
        .first()
        .map(|(t, c)| (t.duration_since(left).as_millis(), c.clone()));
    eprintln!(
        "ended {after:?}; provider read {:?}",
        read_by_provider(&openai)
    );
    assert!(
        read_by_provider(&openai).is_empty(),
        "the provider was sent a turn after its client left"
    );
    assert!(
        after.as_ref().is_some_and(|(ms, _)| *ms < 200),
        "the session ended {after:?} after the client left"
    );
    Ok(())
}

/// A provider that answers each `response.create` with `answer(event)`.
async fn answering(answer: fn(&Value) -> Vec<Value>) -> std::net::SocketAddr {
    use axum::extract::ws::{Message, WebSocketUpgrade};
    let app = axum::Router::new().route(
        "/v1/realtime",
        axum::routing::get(move |ws: WebSocketUpgrade| async move {
            ws.protocols(["realtime"])
                .on_upgrade(move |mut socket| async move {
                    let hello = json!({ "type": "session.created" }).to_string();
                    let _ = socket.send(Message::Text(hello.into())).await;
                    while let Some(Ok(Message::Text(text))) = socket.recv().await {
                        let event: Value = serde_json::from_str(&text).unwrap_or_default();
                        if event["type"] == "response.create" {
                            for reply in answer(&event) {
                                let _ = socket.send(Message::Text(reply.to_string().into())).await;
                            }
                        }
                    }
                })
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("a port");
    let at = listener.local_addr().expect("an address");
    tokio::spawn(async move { axum::serve(listener, app).await });
    at
}

/// The example's gateway in front of `at`, with a wallet of `tokens`.
async fn front(
    at: std::net::SocketAddr,
    tokens: Amount,
) -> (Gateway, MemoryLedger, std::net::SocketAddr) {
    let (meter, ends) = (Meter::default(), Outcomes::default());
    let wallets = MemoryLedger::new();
    wallets.open("sk-demo", None, tokens);
    let gw = gateway(&format!("ws://{at}"), &meter, &ends, &wallets, HOUR).expect("a gateway");
    let gw = gw.plugin(respan_gateway_http::relay::Relay::RAW);
    let addr = listen(&gw).await;
    (gw, wallets, addr)
}

/// The `max_output_tokens` of each `session.update` a raw provider read.
fn session_caps(raw: &Raw) -> Vec<Value> {
    let sessions = lock(&raw.sessions);
    let caps = sessions
        .iter()
        .map(|e| e["session"]["max_output_tokens"].clone());
    caps.collect()
}

/// Turns the provider starts (voice detection) are bounded by the session's
/// cap, which the gateway sets before the client says anything: three
/// sessions whose turns would say 2,500 tokens, against three caps, are all
/// answered and the wallet never goes negative. The client never hears the
/// provider's `session.updated` for it.
#[tokio::test(flavor = "multi_thread")]
async fn turns_the_provider_starts_cannot_overdraw_the_wallet() {
    let (raw, gw, _meter, wallets, addr) = raw_session(2_500, wallet_of(3)).await;
    let sampler = sample(&wallets);
    let mut sessions = Vec::new();
    for _ in 0..3 {
        sessions.push(tokio::spawn(async move {
            let mut ws = client(addr).await;
            hear(&mut ws).await;
            say(&mut ws, json!({ "type": "input_audio_buffer.commit" })).await;
            let events = turn(&mut ws).await;
            close(&mut ws).await;
            assert!(events.iter().all(|e| e["type"] != "session.updated"));
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
    assert_eq!(session_caps(&raw), vec![json!(TURN); 3]);
}

/// A client's own `session.update` cannot lift the cap either.
#[tokio::test(flavor = "multi_thread")]
async fn a_session_update_cannot_lift_the_cap() {
    let (raw, gw, _meter, _wallets, addr) = raw_session(5, 100_000).await;
    let mut ws = client(addr).await;
    hear(&mut ws).await;
    let lift = json!({ "type": "session.update", "session": { "max_output_tokens": "inf" } });
    say(&mut ws, lift).await;
    assert_eq!(heard(&mut ws).await["type"], "session.updated");
    close(&mut ws).await;
    gw.idle().await;
    assert_eq!(session_caps(&raw), [json!(TURN), json!(TURN)]);
}

/// A `response.create` the provider refuses (an `error` naming its event)
/// never ran: its hold is released at once, and a turn after it is held as
/// usual.
#[tokio::test(flavor = "multi_thread")]
async fn a_response_create_the_provider_refuses_is_not_charged() {
    let at = answering(|create| match create["response"]["voice"] == "bad" {
        true => vec![json!({ "type": "error", "error": {
            "type": "invalid_request_error", "event_id": create["event_id"] } })],
        false => script(&["ok"], usage(0, 5)),
    })
    .await;
    let (gw, wallets, addr) = front(at, 100_000).await;
    let mut ws = client(addr).await;
    hear(&mut ws).await;
    let bad = json!({ "type": "response.create", "response": { "voice": "bad" } });
    say(&mut ws, bad).await;
    assert_eq!(heard(&mut ws).await["type"], "error");
    assert_eq!(wallets.available("sk-demo"), 100_000, "released at once");
    say(&mut ws, json!({ "type": "response.create" })).await;
    assert_eq!(last(&turn(&mut ws).await)["type"], "response.done");
    close(&mut ws).await;
    gw.idle().await;
    assert_eq!(wallets.spent("sk-demo"), 5);
}

/// What a tool returns enters the context as much as what the user says: a
/// 40 KB `function_call_output` is held, so a wallet of two caps refuses the
/// turn that reads it.
#[tokio::test(flavor = "multi_thread")]
async fn a_tool_output_is_held_like_text() {
    let (raw, gw, _meter, wallets, addr) = raw_session(5, wallet_of(2)).await;
    let mut ws = client(addr).await;
    hear(&mut ws).await;
    let output = "x".repeat(40_000);
    let item = json!({ "type": "function_call_output", "call_id": "call_1", "output": output });
    let create = json!({ "type": "conversation.item.create", "item": item });
    say(&mut ws, create).await;
    say(&mut ws, json!({ "type": "response.create" })).await;
    let refused = closed(&mut ws).await;
    assert_eq!(refused, Some((1008, "insufficient_funds".into())));
    gw.idle().await;
    assert!(
        lock(&raw.read)
            .iter()
            .all(|e| e["type"] != "response.create")
    );
    assert_eq!(wallets.spent("sk-demo"), 0);
}

/// Audio is bounded by decoded duration, so after 60 KB of base64 audio
/// a small wallet still holds the next turn.
#[tokio::test(flavor = "multi_thread")]
async fn audio_deltas_are_not_counted_as_text() {
    let at = answering(|_| {
        let audio = "A".repeat(60_000);
        vec![
            json!({ "type": "response.created", "response": { "id": "r" } }),
            json!({ "type": "response.output_audio.delta", "response_id": "r", "delta": audio }),
            json!({ "type": "response.output_audio_transcript.delta", "delta": "hi" }),
            json!({ "type": "response.done", "response": { "usage": usage(20, 10) } }),
        ]
    })
    .await;
    let (gw, wallets, addr) = front(at, 5_000).await;
    let mut ws = client(addr).await;
    hear(&mut ws).await;
    for _ in 0..2 {
        say(&mut ws, json!({ "type": "response.create" })).await;
        assert_eq!(last(&turn(&mut ws).await)["type"], "response.done");
    }
    close(&mut ws).await;
    gw.idle().await;
    assert_eq!(wallets.spent("sk-demo"), 2 * 30);
}

// ===== loop 6 audit, on 8b0c012 =====

/// A `response.create` the provider refuses, whose release (`settle(0)`)
/// takes the ledger a while, and a client that leaves meanwhile: the
/// `error` branch of `Wallet::after` removes the hold from `holds` before
/// awaiting `settle`, so when the client's leaving drops that await,
/// `Wallet::end` no longer sees the hold: it stays held for `HOUR` (loop 5's
/// bug 2, back in the new branch). Fix: settle, then remove.
#[tokio::test(flavor = "multi_thread")]
async fn l6_a_refused_turn_being_released_as_the_client_left_is_settled() -> Result<(), Fault> {
    let at = answering(|create| {
        vec![json!({ "type": "error", "error": {
            "type": "invalid_request_error", "event_id": create["event_id"] } })]
    })
    .await;
    let books = MemoryLedger::new();
    books.open("sk-demo", None, 100_000);
    let remote = Remote {
        books: books.clone(),
        ack: Duration::from_millis(0),
        lag: Duration::from_millis(300),
    };
    let gw = Gateway::new()
        .client::<Input, _, _>(OpenAiRealtime)?
        .provider::<Input, _, _>(
            "openai",
            OpenAiRealtime,
            WsTransport::new(format!("ws://{at}"))?.connect(),
        )?
        .target("openai")?
        .plugin(Reserve(remote));
    let gw = gw.plugin(respan_gateway_http::relay::Relay::RAW);
    let addr = listen(&gw).await;
    let mut ws = client(addr).await;
    hear(&mut ws).await;
    say(&mut ws, json!({ "type": "response.create" })).await;
    // The provider's `error` is being released when the client leaves.
    tokio::time::sleep(Duration::from_millis(100)).await;
    drop(ws);
    gw.idle().await;
    tokio::time::sleep(Duration::from_millis(600)).await;
    let (spent, left) = (books.spent("sk-demo"), books.available("sk-demo"));
    assert_eq!(
        spent + left,
        100_000,
        "a hold outlived its session: spent {spent}, available {left}"
    );
    Ok(())
}

/// A client that leaves while its turn's hold is being acknowledged: the
/// hold landed, but the `response.create` never reached the provider, so
/// the turn cost nothing. `Wallet::end` settles every hold left at all it
/// held, so the wallet is charged a full turn (1000) for it. The module doc
/// says a turn "cut before its response.done may have cost all it was held
/// for"; this one provably cost nothing. Low: a billing policy of the
/// example, shown by a test.
#[tokio::test(flavor = "multi_thread")]
async fn l6_a_turn_the_provider_never_saw_is_not_charged() -> Result<(), Fault> {
    let (openai, gw, books, addr) = remote_session(300, 0).await?;
    openai.answer(Answer::events(script(&["ok"], usage(40, 9))));
    let mut ws = client(addr).await;
    hear(&mut ws).await;
    say(&mut ws, json!({ "type": "response.create" })).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    drop(ws);
    gw.idle().await;
    tokio::time::sleep(Duration::from_millis(600)).await;
    let read = read_by_provider(&openai);
    assert!(read.is_empty(), "the provider read {read:?}");
    assert_eq!(
        books.spent("sk-demo"),
        0,
        "a turn the provider never saw was charged"
    );
    Ok(())
}

// ===== loop 6 audit =====

/// A session with a slow input wrap (400 ms per input): the client says
/// `items` `conversation.item.create`s then a `response.create`, back to
/// back, and leaves 50 ms later. How long after it left the session ended,
/// and what the provider was sent.
async fn l6_leave_during_a_wrap(
    items: usize,
) -> Result<(Option<(u128, String)>, Vec<Value>), Fault> {
    struct Slow;
    #[respan_gateway_core::async_trait]
    impl Plugin for Slow {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            cx.wrap(|events: EventStream<Input>| {
                events.and_then(|e| async move {
                    tokio::time::sleep(Duration::from_millis(400)).await;
                    Ok(e)
                })
            })?;
            Ok(None)
        }
    }
    let openai = FakeOpenAi::start().await;
    let provider = openai.base_url().replace("http://", "ws://");
    let at = EndAt::default();
    let gw = Gateway::new()
        .client::<Input, _, _>(OpenAiRealtime)?
        .provider::<Input, _, _>(
            "openai",
            OpenAiRealtime,
            WsTransport::new(provider.trim_end_matches("/v1"))?.connect(),
        )?
        .target("openai")?
        .plugin(at.clone())
        .plugin(Slow);
    let gw = gw.plugin(respan_gateway_http::relay::Relay::RAW);
    let addr = listen(&gw).await;
    let mut ws = client(addr).await;
    hear(&mut ws).await;
    let item = json!({ "type": "conversation.item.create", "item": { "type": "message", "role": "user", "content": [{ "type": "input_text", "text": "hi" }] } });
    for _ in 0..items {
        say(&mut ws, item.clone()).await;
    }
    say(&mut ws, json!({ "type": "response.create" })).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    let left = std::time::Instant::now();
    drop(ws);
    gw.idle().await;
    let ended = lock(&at.0).clone();
    let after = ended
        .first()
        .map(|(t, c)| (t.duration_since(left).as_millis(), c.clone()));
    Ok((after, read_by_provider(&openai)))
}

/// Loop 5's bug 1 with one more frame: the client says two things back to
/// back and leaves while the wrap holds the first. `hear` is parked in
/// `input.send(second)` (its flush waits until the gateway pulls it), so it
/// reads nothing from the socket and never sees the client go: the session
/// lives until the wrap lets the first through (~350 ms). Fixed by watching
/// the socket during the send (one frame of lookahead).
#[tokio::test(flavor = "multi_thread")]
async fn l6_a_client_that_says_two_things_and_leaves_during_a_wrap_ends_then() -> Result<(), Fault>
{
    let (after, read) = l6_leave_during_a_wrap(1).await?;
    assert!(
        read.is_empty() && after.as_ref().is_some_and(|(ms, _)| *ms < 200),
        "the session ended {after:?} after the client left; the provider was sent {read:?}"
    );
    Ok(())
}

/// A tool's definition is read by every turn, so it is held like text: a
/// 40 KB description in a wallet of two caps is refused, and never read.
#[tokio::test(flavor = "multi_thread")]
async fn a_tool_definition_is_held_like_text() {
    let (raw, gw, _meter, wallets, addr) = raw_session(5, wallet_of(2)).await;
    let mut ws = client(addr).await;
    hear(&mut ws).await;
    let tool = json!({ "type": "function", "name": "lookup",
        "description": "x".repeat(40_000), "parameters": { "type": "object" } });
    say(
        &mut ws,
        json!({ "type": "session.update", "session": { "tools": [tool] } }),
    )
    .await;
    assert_eq!(heard(&mut ws).await["type"], "session.updated");
    say(&mut ws, json!({ "type": "response.create" })).await;
    assert_eq!(
        closed(&mut ws).await,
        Some((1008, "insufficient_funds".into()))
    );
    gw.idle().await;
    assert!(
        lock(&raw.read)
            .iter()
            .all(|e| e["type"] != "response.create")
    );
    assert_eq!(wallets.spent("sk-demo"), 0);
    assert!(wallets.available("sk-demo") >= 0);
}

/// A provider that refuses every event with an `error` naming it, as one
/// does that does not know the gateway's `session.type`.
async fn provider_that_refuses_the_cap() -> std::net::SocketAddr {
    use axum::extract::ws::{Message, WebSocketUpgrade};
    let app = axum::Router::new().route(
        "/v1/realtime",
        axum::routing::get(|ws: WebSocketUpgrade| async move {
            ws.protocols(["realtime"])
                .on_upgrade(|mut socket| async move {
                    let hello = json!({ "type": "session.created" }).to_string();
                    let _ = socket.send(Message::Text(hello.into())).await;
                    while let Some(Ok(Message::Text(text))) = socket.recv().await {
                        let event: Value = serde_json::from_str(&text).unwrap_or_default();
                        let error = json!({ "type": "error", "error": {
                        "code": "unknown_parameter", "event_id": event["event_id"] } });
                        let _ = socket.send(Message::Text(error.to_string().into())).await;
                    }
                })
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("a port");
    let at = listener.local_addr().expect("an address");
    tokio::spawn(async move { axum::serve(listener, app).await });
    at
}

/// A session whose cap the provider refuses is closed with a fault, not
/// left uncapped; the client never hears the error for an event it never sent.
#[tokio::test(flavor = "multi_thread")]
async fn a_session_whose_cap_is_refused_is_closed() {
    let at = provider_that_refuses_the_cap().await;
    let (gw, _wallets, addr) = front(at, 100_000).await;
    let mut ws = client(addr).await;
    hear(&mut ws).await;
    let close = closed(&mut ws).await.expect("a close");
    assert_eq!(close.0, 1011);
    gw.idle().await;
}

// ===== loop 7 audit =====

/// A provider that begins each `response.create` it reads (it is billed
/// from then on) but tells `response.created` only `late` after, then runs
/// the turn to its `response.done`. `read` counts the creates it read.
async fn slow_to_tell(late: Duration, read: Arc<AtomicUsize>) -> std::net::SocketAddr {
    use axum::extract::ws::{Message, WebSocketUpgrade};
    let app = axum::Router::new().route(
        "/v1/realtime",
        axum::routing::get(move |ws: WebSocketUpgrade| async move {
            ws.protocols(["realtime"])
                .on_upgrade(move |mut socket| async move {
                    let hello = json!({ "type": "session.created" }).to_string();
                    let _ = socket.send(Message::Text(hello.into())).await;
                    while let Some(Ok(Message::Text(text))) = socket.recv().await {
                        let event: Value = serde_json::from_str(&text).unwrap_or_default();
                        if event["type"] == "response.create" {
                            read.fetch_add(1, Ordering::SeqCst);
                            tokio::time::sleep(late).await;
                            for reply in [
                                json!({ "type": "response.created", "response": { "id": "r" } }),
                                json!({ "type": "response.output_text.delta", "delta": "ok" }),
                                json!({ "type": "response.done", "response": { "usage": usage(40, 9) } }),
                            ] {
                                let _ = socket.send(Message::Text(reply.to_string().into())).await;
                            }
                        }
                    }
                })
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("a port");
    let at = listener.local_addr().expect("an address");
    tokio::spawn(async move { axum::serve(listener, app).await });
    at
}

/// A client says `response.create` and leaves once the provider has read
/// it, before the provider's `response.created` comes back. The provider
/// began (and bills) the turn; `Wallet::end` counts it "not begun" because
/// no `response.created` was seen, and settles its hold at 0: the turn is
/// free, and a client can repeat it at will.
#[tokio::test(flavor = "multi_thread")]
async fn l7_a_turn_the_provider_read_is_not_free_when_the_client_leaves() -> Result<(), Fault> {
    let read = Arc::new(AtomicUsize::new(0));
    let at = slow_to_tell(Duration::from_millis(300), read.clone()).await;
    let (gw, books, addr) = front(at, 100_000).await;
    let mut ws = client(addr).await;
    hear(&mut ws).await;
    say(&mut ws, json!({ "type": "response.create" })).await;
    let start = std::time::Instant::now();
    while read.load(Ordering::SeqCst) == 0 {
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "the provider never read it"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    drop(ws);
    gw.idle().await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let (spent, left) = (books.spent("sk-demo"), books.available("sk-demo"));
    assert_eq!(spent + left, 100_000, "a hold outlived its session");
    assert!(
        spent > 0,
        "the provider read and began a turn, and it cost the wallet {spent}"
    );
    Ok(())
}

/// A provider with voice detection: each `conversation.item.create` it
/// reads starts a turn of its own `after` later (`response.created`, a
/// delta, `response.done`).
async fn starts_turns(after: Duration) -> std::net::SocketAddr {
    use axum::extract::ws::{Message, WebSocketUpgrade};
    use futures::{SinkExt as _, StreamExt as _};
    let app = axum::Router::new().route(
        "/v1/realtime",
        axum::routing::get(move |ws: WebSocketUpgrade| async move {
            ws.protocols(["realtime"])
                .on_upgrade(move |socket| async move {
                    let (mut tx, mut rx) = socket.split();
                    let hello = json!({ "type": "session.created" }).to_string();
                    let _ = tx.send(Message::Text(hello.into())).await;
                    let (go, mut went) = tokio::sync::mpsc::unbounded_channel::<()>();
                    tokio::spawn(async move {
                        while went.recv().await.is_some() {
                            tokio::time::sleep(after).await;
                            for reply in [
                                json!({ "type": "response.created", "response": { "id": "vad" } }),
                                json!({ "type": "response.output_text.delta", "delta": "free" }),
                                json!({ "type": "response.done", "response": { "usage": usage(40, 9) } }),
                            ] {
                                if tx.send(Message::Text(reply.to_string().into())).await.is_err() {
                                    return;
                                }
                            }
                        }
                    });
                    while let Some(Ok(Message::Text(text))) = rx.next().await {
                        let event: Value = serde_json::from_str(&text).unwrap_or_default();
                        if event["type"] == "conversation.item.create" {
                            let _ = go.send(());
                        }
                    }
                })
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("a port");
    let at = listener.local_addr().expect("an address");
    tokio::spawn(async move { axum::serve(listener, app).await });
    at
}

/// A wallet that cannot cover a turn. The client says an item (the
/// provider's voice detection starts a turn on it) and a `response.create`,
/// whose reserve is being acknowledged when the provider's turn begins.
/// That turn must be held (and refused, as the wallet cannot cover it): the
/// client's own turn is not begun-able until its reserve succeeds, so the
/// provider's `response.created` cannot take it.
#[tokio::test(flavor = "multi_thread")]
async fn l7_a_turn_the_provider_starts_during_a_reserve_is_still_held() -> Result<(), Fault> {
    let at = starts_turns(Duration::from_millis(50)).await;
    let books = MemoryLedger::new();
    books.open("sk-demo", None, 500);
    let remote = Remote {
        books: books.clone(),
        ack: Duration::from_millis(300),
        lag: Duration::from_millis(0),
    };
    let gw = Gateway::new()
        .client::<Input, _, _>(OpenAiRealtime)?
        .provider::<Input, _, _>(
            "openai",
            OpenAiRealtime,
            WsTransport::new(format!("ws://{at}"))?.connect(),
        )?
        .target("openai")?
        .plugin(Reserve(remote));
    let gw = gw.plugin(respan_gateway_http::relay::Relay::RAW);
    let addr = listen(&gw).await;
    let mut ws = client(addr).await;
    hear(&mut ws).await;
    let item = json!({ "type": "conversation.item.create", "item": { "type": "message", "role": "user", "content": [{ "type": "input_text", "text": "hi" }] } });
    say(&mut ws, item).await;
    say(&mut ws, json!({ "type": "response.create" })).await;
    let mut heard_kinds = Vec::new();
    while let Some(event) = hear(&mut ws).await {
        heard_kinds.push(event["type"].as_str().unwrap_or_default().to_owned());
    }
    gw.idle().await;
    assert!(
        !heard_kinds
            .iter()
            .any(|k| k == "response.output_text.delta"),
        "the client heard a turn its wallet (500) cannot cover: {heard_kinds:?}; spent {}",
        books.spent("sk-demo")
    );
    Ok(())
}

/// Drops at every moment of a turn against a slow ledger (acks and settles
/// 60 ms late): every hold is settled, none outlives its session, and the
/// wallet never goes below 0.
#[tokio::test(flavor = "multi_thread")]
async fn l7_a_client_that_leaves_at_any_moment_leaves_no_hold() -> Result<(), Fault> {
    let mut bad = Vec::new();
    for at_ms in (0..400).step_by(15) {
        for refused in [false, true] {
            let openai = FakeOpenAi::start().await;
            let provider = openai.base_url().replace("http://", "ws://");
            let books = MemoryLedger::new();
            books.open("sk-demo", None, 100_000);
            let remote = Remote {
                books: books.clone(),
                ack: Duration::from_millis(60),
                lag: Duration::from_millis(60),
            };
            let gw = Gateway::new()
                .client::<Input, _, _>(OpenAiRealtime)?
                .provider::<Input, _, _>(
                    "openai",
                    OpenAiRealtime,
                    WsTransport::new(provider.trim_end_matches("/v1"))?.connect(),
                )?
                .target("openai")?
                .plugin(Reserve(remote));
            let gw = gw.plugin(respan_gateway_http::relay::Relay::RAW);
            let addr = listen(&gw).await;
            openai.answer(Answer::events(script(&["a", "b", "c"], usage(40, 9))));
            let sampler = sample(&books);
            let mut ws = client(addr).await;
            hear(&mut ws).await;
            say(&mut ws, json!({ "type": "response.create" })).await;
            if !refused {
                say(&mut ws, json!({ "type": "response.create" })).await;
            }
            tokio::time::sleep(Duration::from_millis(at_ms)).await;
            drop(ws);
            gw.idle().await;
            tokio::time::sleep(Duration::from_millis(300)).await;
            let low = sampler.finish().await;
            let (spent, left) = (books.spent("sk-demo"), books.available("sk-demo"));
            if spent + left != 100_000 || low < 0 {
                bad.push(format!(
                    "drop at {at_ms} ms: spent {spent} available {left} lowest {low}"
                ));
            }
        }
    }
    assert!(bad.is_empty(), "{}", bad.join("\n"));
    Ok(())
}

// ===== loop 8 audit =====

/// A provider that bills each image the conversation holds at 765 input
/// tokens (a 1024x1024 image at high detail, OpenAI's tile rule), whatever
/// its bytes, plus 5 output tokens, on each `response.create`.
async fn bills_images() -> std::net::SocketAddr {
    use axum::extract::ws::{Message, WebSocketUpgrade};
    let app = axum::Router::new().route(
        "/v1/realtime",
        axum::routing::get(|ws: WebSocketUpgrade| async move {
            ws.protocols(["realtime"])
                .on_upgrade(|mut socket| async move {
                    let hello = json!({ "type": "session.created" }).to_string();
                    let _ = socket.send(Message::Text(hello.into())).await;
                    let mut images = 0u64;
                    while let Some(Ok(Message::Text(text))) = socket.recv().await {
                        let event: Value = serde_json::from_str(&text).unwrap_or_default();
                        match event["type"].as_str() {
                            Some("session.update") => {
                                let ack = json!({ "type": "session.updated" }).to_string();
                                let _ = socket.send(Message::Text(ack.into())).await;
                            }
                            Some("conversation.item.create") => {
                                let parts = event["item"]["content"].as_array();
                                let parts = parts.map_or(&[][..], |parts| &parts[..]);
                                let seen = parts.iter().filter(|p| p["type"] == "input_image");
                                images += seen.count() as u64;
                            }
                            Some("response.create") => {
                                for e in script(&["ok"], usage(765 * images, 5)) {
                                    let _ = socket.send(Message::Text(e.to_string().into())).await;
                                }
                            }
                            _ => {}
                        }
                    }
                })
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("a port");
    let at = listener.local_addr().expect("an address");
    tokio::spawn(async move { axum::serve(listener, app).await });
    at
}

/// Content the provider adds in an event that is not a delta (an
/// `mcp_call`'s output in an item event) is context too: after one of 40 KB,
/// a wallet that covered the first turn refuses the second.
#[tokio::test(flavor = "multi_thread")]
async fn provider_content_in_a_non_delta_event_is_held() {
    let at = answering(|_| {
        let output = "x".repeat(40_000);
        let item = json!({ "type": "mcp_call", "name": "search", "output": output });
        vec![
            json!({ "type": "response.created", "response": { "id": "r" } }),
            json!({ "type": "conversation.item.done", "item": item }),
            json!({ "type": "response.done", "response": { "usage": usage(20, 10) } }),
        ]
    })
    .await;
    let (gw, wallets, addr) = front(at, 5_000).await;
    let mut ws = client(addr).await;
    hear(&mut ws).await;
    say(&mut ws, json!({ "type": "response.create" })).await;
    assert_eq!(last(&turn(&mut ws).await)["type"], "response.done");
    say(&mut ws, json!({ "type": "response.create" })).await;
    let refused = closed(&mut ws).await;
    assert_eq!(refused, Some((1008, "insufficient_funds".into())));
    gw.idle().await;
    assert_eq!(wallets.spent("sk-demo"), 30);
}

// ===== loop 8 audit =====

/// A ledger across a network whose first `reserve` lands `slow` late (a
/// retried request, jitter); later ones land at once. A settle lands at once.
#[derive(Clone)]
struct Jitter {
    books: MemoryLedger,
    slow: Duration,
    first: Arc<AtomicUsize>,
}

#[respan_gateway_core::async_trait]
impl Ledger for Jitter {
    async fn reserve(
        &self,
        id: &HoldId,
        accounts: &[String],
        amount: Amount,
        ttl: Duration,
    ) -> Result<(), Fault> {
        if self.first.fetch_add(1, Ordering::SeqCst) == 0 {
            tokio::time::sleep(self.slow).await;
        }
        self.books.reserve(id, accounts, amount, ttl).await
    }

    async fn settle(&self, id: &HoldId, spent: Amount) -> Result<(), Fault> {
        self.books.settle(id, spent).await
    }
}

/// The client says an item (the provider's voice detection starts a turn on
/// it 50 ms later) and a `response.create`, whose reserve lands late. The
/// provider's turn is held and done while the client's reserve is in flight.
/// `response.done` settles the oldest hold, which is the client's, not the
/// turn's: that settle lands before its reserve (a no-op), the reserve then
/// lands and is never settled again, and outlives the session for `HOUR`.
#[tokio::test(flavor = "multi_thread")]
async fn l8_a_turn_done_during_another_reserve_settles_its_own_hold() -> Result<(), Fault> {
    let at = starts_turns(Duration::from_millis(50)).await;
    let books = MemoryLedger::new();
    books.open("sk-demo", None, 100_000);
    let jitter = Jitter {
        books: books.clone(),
        slow: Duration::from_millis(400),
        first: Arc::new(AtomicUsize::new(0)),
    };
    let gw = Gateway::new()
        .client::<Input, _, _>(OpenAiRealtime)?
        .provider::<Input, _, _>(
            "openai",
            OpenAiRealtime,
            WsTransport::new(format!("ws://{at}"))?.connect(),
        )?
        .target("openai")?
        .plugin(Reserve(jitter));
    let gw = gw.plugin(respan_gateway_http::relay::Relay::RAW);
    let addr = listen(&gw).await;
    let mut ws = client(addr).await;
    hear(&mut ws).await;
    let item = json!({ "type": "conversation.item.create", "item": { "type": "message", "role": "user", "content": [{ "type": "input_text", "text": "hi" }] } });
    say(&mut ws, item).await;
    say(&mut ws, json!({ "type": "response.create" })).await;
    tokio::time::sleep(Duration::from_millis(800)).await;
    close(&mut ws).await;
    gw.idle().await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let (spent, left) = (books.spent("sk-demo"), books.available("sk-demo"));
    assert_eq!(
        spent + left,
        100_000,
        "a hold outlived its session: spent {spent}, available {left}"
    );
    Ok(())
}

/// Inline input_audio receives the same duration bound as streamed audio;
/// encoding 60 KB of base64 does not pretend it is 60,000 text tokens.
#[tokio::test(flavor = "multi_thread")]
async fn audio_the_client_sends_in_an_item_is_bounded_by_duration() {
    let at = answering(|_| script(&["ok"], usage(20, 10))).await;
    let (gw, wallets, addr) = audio_front(at, 5_000).await;
    let mut ws = client(addr).await;
    hear(&mut ws).await;
    let audio = "A".repeat(60_000);
    let part = json!({ "type": "input_audio", "audio": audio });
    let item = json!({ "type": "message", "role": "user", "content": [part] });
    say(
        &mut ws,
        json!({ "type": "conversation.item.create", "item": item }),
    )
    .await;
    say(&mut ws, json!({ "type": "response.create" })).await;
    let outcome = done_or_closed(&mut ws).await;
    if outcome.is_ok() {
        close(&mut ws).await;
    }
    gw.idle().await;
    assert_eq!(
        outcome,
        Ok(()),
        "a turn costing 30 was refused on a wallet of 5,000; spent {}",
        wallets.spent("sk-demo")
    );
}

/// "A token is at least a byte" does not hold for images: providers bill an
/// image by its size in pixels, not its bytes. A 2048x768 black-and-white
/// PNG is 270 bytes (360 in base64), and OpenAI's tile rule bills it
/// 1,445 tokens (the mock here bills 765). Eight of them fit a hold on a
/// wallet of 5,000, and the turn spends 6,125: the wallet is overspent.
#[tokio::test(flavor = "multi_thread")]
async fn a_small_image_billed_more_than_its_bytes_cannot_overspend() {
    const PNG: &str = "iVBORw0KGgoAAAANSUhEUgAACAAAAAMAAQAAAACY27pnAAAA1UlEQVR42u3BAQ0AAADCoPdP7ewBFAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAANwMtAAGZvVjvAAAAAElFTkSuQmCC";
    let at = bills_images().await;
    let (gw, wallets, addr) = front(at, 5_000).await;
    let mut ws = client(addr).await;
    hear(&mut ws).await;
    let url = format!("data:image/png;base64,{PNG}");
    let parts: Vec<Value> = (0..8)
        .map(|_| json!({ "type": "input_image", "image_url": url }))
        .collect();
    let item = json!({ "type": "message", "role": "user", "content": parts });
    say(
        &mut ws,
        json!({ "type": "conversation.item.create", "item": item }),
    )
    .await;
    say(&mut ws, json!({ "type": "response.create" })).await;
    if done_or_closed(&mut ws).await.is_ok() {
        close(&mut ws).await;
    }
    gw.idle().await;
    let balance = wallets.available("sk-demo");
    assert!(
        balance >= 0,
        "the wallet overspent: balance {balance}, spent {}",
        wallets.spent("sk-demo")
    );
}

/// `Ok` at a `response.done`, or the Close the session ended with.
async fn done_or_closed(ws: &mut Client) -> Result<(), Option<(u16, String)>> {
    use respan_gateway_core::futures::StreamExt;
    use tokio_tungstenite::tungstenite::Message;
    loop {
        let next = tokio::time::timeout(Duration::from_secs(5), ws.next()).await;
        match next.expect("an event in time") {
            Some(Ok(Message::Text(text))) if text.contains("\"response.done\"") => return Ok(()),
            Some(Ok(Message::Close(frame))) => {
                return Err(frame.map(|f| (f.code.into(), f.reason.to_string())));
            }
            Some(Ok(_)) => {}
            _ => return Err(None),
        }
    }
}

/// Drops at every moment of a session whose turns the provider starts (voice
/// detection) while the client's own `response.create` is held, against a
/// slow ledger: every hold is settled once, none outlives its session, and
/// the wallet never goes below 0.
#[tokio::test(flavor = "multi_thread")]
async fn l9_provider_turns_and_a_client_that_leaves_at_any_moment_leave_no_hold()
-> Result<(), Fault> {
    let mut bad = Vec::new();
    for at_ms in (0..450).step_by(10) {
        let at = starts_turns(Duration::from_millis(30)).await;
        let books = MemoryLedger::new();
        books.open("sk-demo", None, 100_000);
        let remote = Remote {
            books: books.clone(),
            ack: Duration::from_millis(40),
            lag: Duration::from_millis(40),
        };
        let gw = Gateway::new()
            .client::<Input, _, _>(OpenAiRealtime)?
            .provider::<Input, _, _>(
                "openai",
                OpenAiRealtime,
                WsTransport::new(format!("ws://{at}"))?.connect(),
            )?
            .target("openai")?
            .plugin(Reserve(remote));
        let gw = gw.plugin(respan_gateway_http::relay::Relay::RAW);
        let addr = listen(&gw).await;
        let sampler = sample(&books);
        let mut ws = client(addr).await;
        hear(&mut ws).await;
        let item = json!({ "type": "conversation.item.create", "item": { "type": "message", "role": "user", "content": [{ "type": "input_text", "text": "hi" }] } });
        say(&mut ws, item.clone()).await;
        say(&mut ws, json!({ "type": "response.create" })).await;
        say(&mut ws, item).await;
        tokio::time::sleep(Duration::from_millis(at_ms)).await;
        drop(ws);
        gw.idle().await;
        tokio::time::sleep(Duration::from_millis(200)).await;
        let low = sampler.finish().await;
        let (spent, left) = (books.spent("sk-demo"), books.available("sk-demo"));
        if spent + left != 100_000 || low < 0 {
            bad.push(format!(
                "drop at {at_ms} ms: spent {spent} available {left} lowest {low}"
            ));
        }
    }
    assert!(bad.is_empty(), "{}", bad.join("\n"));
    Ok(())
}

async fn audio_front(
    at: std::net::SocketAddr,
    tokens: Amount,
) -> (Gateway, MemoryLedger, std::net::SocketAddr) {
    let wallets = MemoryLedger::new();
    wallets.open("sk-demo", None, tokens);
    let gw = gateway_with_audio(
        &format!("ws://{at}"),
        &Meter::default(),
        &Outcomes::default(),
        &wallets,
        HOUR,
        true,
    )
    .expect("gateway");
    let gw = gw.plugin(respan_gateway_http::relay::Relay::RAW);
    let addr = listen(&gw).await;
    (gw, wallets, addr)
}

#[tokio::test(flavor = "multi_thread")]
async fn text_redaction_refuses_audio_before_the_provider_receives_it() {
    let s = start().await.expect("session");
    let mut ws = client(s.addr).await;
    heard(&mut ws).await;
    say(
        &mut ws,
        json!({"type":"input_audio_buffer.append","audio":"AAAA"}),
    )
    .await;
    assert_eq!(
        done_or_closed(&mut ws).await,
        Err(Some((1008, "audio_requires_redaction".into())))
    );
    s.gateway.idle().await;
    assert!(read_by_provider(&s.openai).is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn audio_usage_is_metered_per_turn_and_charged() {
    let at = answering(|_| {
        vec![
            json!({"type":"response.created","response":{"id":"audio1"}}),
            json!({"type":"response.output_audio.delta","delta":"AAAA"}),
            json!({"type":"response.done","response":{"id":"audio1","status":"completed","usage":{
            "input_tokens":32,"output_tokens":12,"total_tokens":44,
            "input_token_details":{"audio_tokens":30,"text_tokens":2},
            "output_token_details":{"audio_tokens":10,"text_tokens":2}}}}),
        ]
    })
    .await;
    let meter = Meter::default();
    let wallets = MemoryLedger::new();
    wallets.open("sk-demo", None, 100_000);
    let gw = gateway_with_audio(
        &format!("ws://{at}"),
        &meter,
        &Outcomes::default(),
        &wallets,
        HOUR,
        true,
    )
    .expect("gateway");
    let gw = gw.plugin(respan_gateway_http::relay::Relay::RAW);
    let addr = listen(&gw).await;
    let mut ws = client(addr).await;
    heard(&mut ws).await;
    for _ in 0..2 {
        say(
            &mut ws,
            json!({"type":"input_audio_buffer.append","audio":"AAAA"}),
        )
        .await;
        say(&mut ws, json!({"type":"input_audio_buffer.commit"})).await;
        say(&mut ws, json!({"type":"response.create"})).await;
        assert_eq!(last(&turn(&mut ws).await)["type"], "response.done");
    }
    close(&mut ws).await;
    gw.idle().await;
    assert_eq!(meter.turns().len(), 2);
    assert!(
        meter
            .turns()
            .iter()
            .all(|t| t.input_audio_tokens == 30 && t.output_audio_tokens == 10)
    );
    assert_eq!(wallets.spent("sk-demo"), 88);
}

#[tokio::test(flavor = "multi_thread")]
async fn capped_audio_refuses_unmetered_transcription_and_invalid_audio() {
    for event in [
        json!({"type":"session.update","session":{"audio":{"input":{"transcription":{"model":"x"}}}}}),
        json!({"type":"session.update","session":{"input_audio_format":"mp3"}}),
        json!({"type":"session.update","session":4}),
        json!({"type":"session.update","session":{"audio":{"input":[]}}}),
        json!({"type":"input_audio_buffer.append","audio":"not base64!"}),
    ] {
        let at = answering(|_| script(&["ok"], usage(20, 10))).await;
        let (gw, wallets, addr) = audio_front(at, 5_000).await;
        let mut ws = client(addr).await;
        heard(&mut ws).await;
        say(&mut ws, event).await;
        let Err(Some((code, _))) = done_or_closed(&mut ws).await else {
            panic!("must refuse")
        };
        assert_eq!(code, 1008);
        gw.idle().await;
        assert_eq!(wallets.spent("sk-demo"), 0);
    }
}

#[test]
fn audio_bounds_use_validated_samples_instead_of_base64_text_tokens() {
    assert_eq!(
        audio::tokens(&"A".repeat(60_000)).expect("valid audio"),
        113
    );
    assert!(audio::tokens("not base64").is_err());
    assert!(
        context(&json!({"type":"input_audio_buffer.append","audio":"A".repeat(60_000)}))
            .expect("valid audio")
            < 200
    );
}
