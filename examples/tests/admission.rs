use super::*;

fn models(mock: &MockTransport) -> Vec<String> {
    let model = |b: &[u8]| {
        let v: serde_json::Value = serde_json::from_slice(b).ok()?;
        v["model"].as_str().map(Into::into)
    };
    mock.sent().iter().filter_map(|r| model(r.body())).collect()
}

/// Until `n` requests are parked.
async fn parked(slots: &Slots, n: usize) {
    let count = || -> usize {
        slots
            .queue()
            .waiting
            .values()
            .flatten()
            .map(|(_, l)| l.len())
            .sum()
    };
    while count() < n {
        tokio::task::yield_now().await;
    }
}

#[tokio::test(start_paused = true)]
async fn a_request_past_its_deadline_gets_429_with_retry_after() -> Result<(), Fault> {
    let mock = MockTransport::new();
    mock.reply(OPENAI, mock::json(StatusCode::OK, OK));
    let gw = gateway(&mock, &Slots::new(1), Duration::from_millis(50))?;
    let (held, _) = serve(&gw, request("a", "paid")).await;
    let started = Instant::now();
    let (status, headers, _) = read_whole(serve(&gw, request("b", "free")).await.0).await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(headers[RETRY_AFTER], "1");
    assert_eq!(started.elapsed(), Duration::from_millis(50), "it waited");
    drop(held);
    Ok(())
}

#[tokio::test]
async fn parked_requests_are_admitted_by_priority_when_a_slot_frees() -> Result<(), Fault> {
    let mock = MockTransport::new();
    for _ in 0..3 {
        mock.reply(OPENAI, mock::json(StatusCode::OK, OK));
    }
    let slots = Slots::new(1);
    let gw = gateway(&mock, &slots, Duration::from_secs(5))?;
    let (held, _) = serve(&gw, request("first", "paid")).await;
    let park = |model: &'static str, tier| {
        let gw = gw.clone();
        tokio::spawn(async move { read_whole(serve(&gw, request(model, tier)).await.0).await.0 })
    };
    let free = park("free", "free");
    parked(&slots, 1).await;
    let paid = park("paid", "paid");
    parked(&slots, 2).await;
    drop(held);
    let statuses = (paid.await.ok(), free.await.ok());
    assert_eq!(statuses, (Some(StatusCode::OK), Some(StatusCode::OK)));
    assert_eq!(models(&mock), ["first", "paid", "free"], "paid went first");
    Ok(())
}

#[tokio::test]
async fn a_parked_request_that_leaves_loses_no_slot() -> Result<(), Fault> {
    let mock = MockTransport::new();
    mock.reply(OPENAI, mock::json(StatusCode::OK, OK));
    let slots = Slots::new(1);
    let gw = gateway(&mock, &slots, Duration::from_secs(5))?;
    let (held, _) = serve(&gw, request("first", "paid")).await;
    let gone = tokio::spawn({
        let gw = gw.clone();
        async move { drop(serve(&gw, request("gone", "paid")).await) }
    });
    parked(&slots, 1).await;
    gone.abort();
    let _ = gone.await;
    drop(held);
    gw.idle().await;
    assert_eq!(slots.queue().free, 1, "the slot came back");
    Ok(())
}

/// A request for `model` from `user`, on the free tier.
fn from(model: &str, user: &'static str) -> http::Request<respan_gateway_http::Body> {
    let mut req = request(model, "free");
    req.headers_mut()
        .insert("x-user", HeaderValue::from_static(user));
    req
}

#[tokio::test]
async fn one_heavy_user_cannot_starve_another() -> Result<(), Fault> {
    let mock = MockTransport::new();
    for _ in 0..6 {
        mock.reply(OPENAI, mock::json(StatusCode::OK, OK));
    }
    let slots = Slots::new(1);
    let gw = gateway(&mock, &slots, Duration::from_secs(5))?;
    let (held, _) = serve(&gw, from("first", "heavy")).await;
    let park = |model: &'static str, user| {
        let gw = gw.clone();
        tokio::spawn(async move { read_whole(serve(&gw, from(model, user)).await.0).await.0 })
    };
    // The heavy user parks four requests before the light one parks one.
    let mut all = Vec::new();
    for (i, model) in ["h1", "h2", "h3", "h4"].into_iter().enumerate() {
        all.push(park(model, "heavy"));
        parked(&slots, i + 1).await;
    }
    all.push(park("l1", "light"));
    parked(&slots, 5).await;
    drop(held);
    for done in all {
        assert_eq!(done.await.ok(), Some(StatusCode::OK));
    }
    // Served in turns: the light user's one request is second, not last.
    assert_eq!(models(&mock), ["first", "h1", "l1", "h2", "h3", "h4"]);
    Ok(())
}

#[tokio::test]
async fn priority_still_comes_before_fair_share() -> Result<(), Fault> {
    let mock = MockTransport::new();
    for _ in 0..3 {
        mock.reply(OPENAI, mock::json(StatusCode::OK, OK));
    }
    let slots = Slots::new(1);
    let gw = gateway(&mock, &slots, Duration::from_secs(5))?;
    let (held, _) = serve(&gw, from("first", "a")).await;
    let park = |model: &'static str, user, tier| {
        let gw = gw.clone();
        let mut req = request(model, tier);
        req.headers_mut()
            .insert("x-user", HeaderValue::from_static(user));
        tokio::spawn(async move { read_whole(serve(&gw, req).await.0).await.0 })
    };
    let free = park("free", "b", "free");
    parked(&slots, 1).await;
    let paid = park("paid", "a", "paid");
    parked(&slots, 2).await;
    drop(held);
    assert_eq!(
        (paid.await.ok(), free.await.ok()),
        (Some(StatusCode::OK), Some(StatusCode::OK))
    );
    assert_eq!(models(&mock), ["first", "paid", "free"]);
    Ok(())
}
