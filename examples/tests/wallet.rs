use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn fifty_parallel_requests_never_overspend_a_one_dollar_wallet() -> Result<(), Fault> {
    let (mock, ledger) = (MockTransport::new(), wallets());
    let gw = gateway(&mock, ledger.clone())?;
    let served = fifty(&gw, &mock, "sk-b1").await;
    let spent = ledger.spent("acme");
    assert!(spent <= 1_000_000, "overspent: {spent}");
    assert_eq!(spent, served as Amount * 80_020, "each settled once");
    assert!(served >= 10, "held about $0.1 each: {served}");
    assert_eq!(ledger.spent("team-b"), spent);
    Ok(())
}

#[tokio::test]
async fn a_spent_team_stops_its_keys_and_not_the_others() -> Result<(), Fault> {
    let (mock, ledger) = (MockTransport::new(), wallets());
    let gw = gateway(&mock, ledger.clone())?;
    for _ in 0..4 {
        mock.reply(OPENAI, answer());
    }
    // team-a's $0.30 covers three calls; the fourth is refused, whichever key.
    for _ in 0..3 {
        assert!(ask(&gw, "sk-a1").await?.is_success());
    }
    assert_eq!(ask(&gw, "sk-a2").await?, StatusCode::PAYMENT_REQUIRED);
    assert!(ask(&gw, "sk-b1").await?.is_success());
    assert_eq!(ledger.spent("team-a"), 3 * 80_020);
    assert_eq!(ledger.available("sk-a2"), 1_000_000, "nothing held");
    Ok(())
}

#[tokio::test]
async fn a_request_with_no_cost_bound_is_refused() -> Result<(), Fault> {
    let (mock, ledger) = (MockTransport::new(), wallets());
    let gw = gateway(&mock, ledger)?;
    let unbounded = r#"{"model":"gpt-x","messages":[]}"#;
    assert_eq!(
        send(&gw, "sk-b1", unbounded).await?,
        StatusCode::BAD_REQUEST
    );
    assert!(mock.sent().is_empty());
    Ok(())
}

/// An image's tokens have no byte bound: 600k of them would cost $1.20.
#[tokio::test]
async fn an_image_cannot_overspend_the_wallet() -> Result<(), Fault> {
    let (mock, ledger) = (MockTransport::new(), wallets());
    let gw = gateway(&mock, ledger.clone())?;
    let image = r#"{"model":"gpt-x","max_tokens":100,"messages":[{"role":"user","content":[{"type":"text","text":"what?"},{"type":"image_url","image_url":{"url":"data:image/png;base64,AAAA"}}]}]}"#;
    assert_eq!(send(&gw, "sk-b1", image).await?, StatusCode::BAD_REQUEST);
    assert!(mock.sent().is_empty());
    assert_eq!(ledger.spent("acme"), 0);
    Ok(())
}

/// Five answers of up to 10,000 tokens each: the hold covers all five.
#[tokio::test]
async fn n_answers_cannot_overspend_the_wallet() -> Result<(), Fault> {
    let (mock, ledger) = (MockTransport::new(), wallets());
    let gw = gateway(&mock, ledger.clone())?;
    let five = r#"{"id":"c1","object":"chat.completion","model":"gpt-x","choices":[{"index":0,"finish_reason":"length","message":{"role":"assistant","content":"..."}}],"usage":{"prompt_tokens":10,"completion_tokens":50000}}"#;
    let body =
        r#"{"model":"gpt-x","n":5,"max_tokens":10000,"messages":[{"role":"user","content":"hi"}]}"#;
    let mut statuses = vec![];
    for _ in 0..4 {
        mock.reply(OPENAI, mock::json(StatusCode::OK, five));
        statuses.push(send(&gw, "sk-b1", body).await?.as_u16());
    }
    // Each holds about $0.40 of acme's $1: two are served.
    assert_eq!(statuses, [200, 200, 402, 402]);
    assert_eq!(ledger.spent("acme"), 2 * 400_020);
    Ok(())
}
