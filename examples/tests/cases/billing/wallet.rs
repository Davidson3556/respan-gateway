use super::*;
use respan_gateway_conversation::Input;
use respan_gateway_examples::LIMIT;

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn fifty_parallel_requests_never_overspend_a_one_dollar_wallet() -> Result<(), Fault> {
    let (fake, ledger) = (FakeOpenAi::start().await, wallets());
    let gw = gateway(
        provider_support::openai(&fake.base_url())?.connect(),
        ledger.clone(),
    )?;
    let served = fifty(&gw, &fake, "sk-b1").await;
    let spent = ledger.spent("acme");
    assert!(spent <= 1_000_000, "overspent: {spent}");
    assert_eq!(spent, served as i64 * 80_020, "each settled once");
    assert!(served >= 10, "held about $0.1 each: {served}");
    assert_eq!(ledger.spent("team-b"), spent);
    assert_eq!(fake.requests().len(), served);
    Ok(())
}

#[tokio::test]
async fn a_spent_team_stops_its_keys_and_not_the_others() -> Result<(), Fault> {
    let (fake, ledger) = (FakeOpenAi::start().await, wallets());
    let gw = gateway(
        provider_support::openai(&fake.base_url())?.connect(),
        ledger.clone(),
    )?;
    for _ in 0..4 {
        fake.answer(answer());
    }
    // team-a's $0.30 covers three calls; the fourth is refused, whichever key.
    for _ in 0..3 {
        assert!(ask(&gw, "sk-a1").await?.is_success());
    }
    assert_eq!(ask(&gw, "sk-a2").await?, StatusCode::PAYMENT_REQUIRED);
    assert!(ask(&gw, "sk-b1").await?.is_success());
    assert_eq!(ledger.spent("team-a"), 3 * 80_020);
    assert_eq!(ledger.available("sk-a2"), 1_000_000, "nothing held");
    assert_eq!(fake.requests().len(), 4);
    Ok(())
}

#[tokio::test]
async fn a_request_with_no_cost_bound_is_refused() -> Result<(), Fault> {
    let (fake, ledger) = (FakeOpenAi::start().await, wallets());
    let gw = gateway(
        provider_support::openai(&fake.base_url())?.connect(),
        ledger,
    )?;
    let unbounded = r#"{"model":"gpt-x","messages":[]}"#;
    assert_eq!(
        send(&gw, "sk-b1", unbounded).await?,
        StatusCode::BAD_REQUEST
    );
    assert!(fake.requests().is_empty());
    Ok(())
}

/// An image's tokens have no byte bound: 600k of them would cost $1.20.
#[tokio::test]
async fn an_image_cannot_overspend_the_wallet() -> Result<(), Fault> {
    let (fake, ledger) = (FakeOpenAi::start().await, wallets());
    let gw = gateway(
        provider_support::openai(&fake.base_url())?.connect(),
        ledger.clone(),
    )?;
    let image = r#"{"model":"gpt-x","max_tokens":100,"messages":[{"role":"user","content":[{"type":"text","text":"what?"},{"type":"image_url","image_url":{"url":"data:image/png;base64,AAAA"}}]}]}"#;
    assert_eq!(send(&gw, "sk-b1", image).await?, StatusCode::BAD_REQUEST);
    assert!(fake.requests().is_empty());
    assert_eq!(ledger.spent("acme"), 0);
    Ok(())
}

/// The conversation kind has no form for several choices, so a request for
/// five is refused before it holds anything or reaches a provider.
#[tokio::test]
async fn several_answers_are_refused_unless_relayed_raw() -> Result<(), Fault> {
    let (fake, ledger) = (FakeOpenAi::start().await, wallets());
    let gw = gateway(
        provider_support::openai(&fake.base_url())?.connect(),
        ledger.clone(),
    )?;
    let body =
        r#"{"model":"gpt-x","n":5,"max_tokens":10000,"messages":[{"role":"user","content":"hi"}]}"#;
    assert_eq!(send(&gw, "sk-b1", body).await?, StatusCode::BAD_REQUEST);
    assert_eq!(ledger.spent("acme"), 0);
    assert!(fake.requests().is_empty());
    Ok(())
}

/// Five answers of up to 10,000 tokens each, through an explicit raw relay
/// (the only way a request for several choices reaches a provider): the
/// hold covers all five.
#[tokio::test]
async fn n_answers_cannot_overspend_the_wallet() -> Result<(), Fault> {
    let (fake, ledger) = (FakeOpenAi::start().await, wallets());
    let prices = prices();
    let accounting = Accounting::new(ledger.clone(), move |px, tally| quote(px, tally, &prices));
    // A raw relay keeps the client's path, so the provider sits at the origin.
    let base = fake.base_url();
    let origin = base.trim_end_matches("/v1");
    let gw = Gateway::new()
        .provider::<Input, _, _>(
            "openai",
            OpenAiChat,
            provider_support::openai(origin)?.connect(),
        )?
        .client::<Input, _, _>(OpenAiChat)?
        .plugin(accounting)
        .plugin(respan_gateway_http::relay::Relay::RAW)
        .target("openai")?;
    let five = r#"{"id":"c1","object":"chat.completion","model":"gpt-x","choices":[{"index":0,"finish_reason":"length","message":{"role":"assistant","content":"..."}}],"usage":{"prompt_tokens":10,"completion_tokens":50000}}"#;
    let body =
        r#"{"model":"gpt-x","n":5,"max_tokens":10000,"messages":[{"role":"user","content":"hi"}]}"#;
    let mut statuses = vec![];
    for _ in 0..4 {
        fake.answer(Answer::raw(five));
        statuses.push(send(&gw, "sk-b1", body).await?.as_u16());
    }
    // Each holds about $0.40 of acme's $1: two are served.
    assert_eq!(statuses, [200, 200, 402, 402]);
    assert_eq!(ledger.spent("acme"), 2 * 400_020);
    assert_eq!(fake.requests().len(), 2);
    Ok(())
}

/// The kind reads the larger of two spellings, so the hold covers whichever
/// one a provider reads.
#[tokio::test]
async fn two_spellings_of_max_tokens_cannot_overspend() -> Result<(), Fault> {
    let (fake, ledger) = (FakeOpenAi::start().await, wallets());
    let gw = gateway(
        provider_support::openai(&fake.base_url())?.connect(),
        ledger.clone(),
    )?;
    let body = r#"{"model":"gpt-x","max_tokens":100000,"max_completion_tokens":10,"messages":[{"role":"user","content":"hi"}]}"#;
    // Priced at the larger limit, 100,000 tokens: more than team-a has.
    assert_eq!(
        send(&gw, "sk-a1", body).await?,
        StatusCode::PAYMENT_REQUIRED
    );
    assert!(fake.requests().is_empty());
    assert_eq!(ledger.spent("team-a"), 0);
    Ok(())
}

/// Use case 4 with use case 9: a hedge races two deployments, and the one
/// that loses is charged too. It was abandoned before it reported any usage,
/// and it may have cost all it was allowed, so it pays at least its hold.
#[tokio::test(start_paused = true)]
async fn the_loser_of_a_hedge_is_charged_at_least_its_hold() -> Result<(), Fault> {
    use respan_gateway_core::{ClientContext, Plugin, Response, async_trait};
    use respan_gateway_plugins::routing::{Until, race};
    use std::time::Duration;

    /// Races both deployments, the second starting 200 ms after the first.
    struct Hedge;

    #[async_trait]
    impl Plugin for Hedge {
        async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
            let calls = ["openai", "openai-eu"].map(|p| cx.call(p));
            let stagger = Duration::from_millis(200);
            race(
                calls,
                stagger,
                Until::content(respan_gateway_conversation::Output::is_content),
            )
            .await
            .map(Some)
        }
    }

    let (slow_fake, eu_fake) = tokio::join!(FakeOpenAi::start(), FakeOpenAi::start());
    let ledger = wallets();
    let prices = prices();
    let accounting = Accounting::new(ledger.clone(), move |px, tally| quote(px, tally, &prices));
    let backend = provider_support::openai(&slow_fake.base_url())?;
    let slow = move |request| {
        let backend = backend.clone();
        async move {
            tokio::time::sleep(Duration::from_secs(1)).await;
            backend.send(request).await
        }
    };
    let gw = Gateway::new()
        .provider::<Input, _, _>(
            "openai",
            OpenAiChat,
            respan_gateway_http::connect(slow, LIMIT),
        )?
        .provider::<Input, _, _>(
            "openai-eu",
            OpenAiChat,
            provider_support::openai(&eu_fake.base_url())?.connect(),
        )?
        .client::<Input, _, _>(OpenAiChat)?
        .plugin(Hedge)
        .plugin(accounting);
    eu_fake.answer(answer());

    assert!(ask(&gw, "sk-b1").await?.is_success());

    // The winner settled from its usage (80,020); the loser held up to
    // 12,000 output tokens at $8 a million, 96,000, and pays no less.
    let spent = ledger.spent("sk-b1");
    assert!(
        spent >= 80_020 + 96_000,
        "the loser was not charged its hold: {spent}"
    );
    assert_eq!(
        ledger.available("sk-b1"),
        1_000_000 - spent,
        "nothing left held"
    );
    assert_eq!(
        (slow_fake.requests().len(), eu_fake.requests().len()),
        (0, 1)
    );
    Ok(())
}

/// A client's absurd `max_tokens` prices high, never wraps to a free hold.
#[tokio::test]
async fn a_huge_max_tokens_is_priced_high_not_wrapped() -> Result<(), Fault> {
    let (fake, ledger) = (FakeOpenAi::start().await, wallets());
    let gw = gateway(
        provider_support::openai(&fake.base_url())?.connect(),
        ledger.clone(),
    )?;
    let huge = [
        r#"{"model":"gpt-x","max_tokens":9223372036854775808,"messages":[{"role":"user","content":"hi"}]}"#,
        r#"{"model":"gpt-x","max_tokens":2305843009213693952,"n":8,"messages":[{"role":"user","content":"hi"}]}"#,
    ];
    for body in huge {
        assert_eq!(
            send(&gw, "sk-a1", body).await?,
            StatusCode::PAYMENT_REQUIRED
        );
    }
    assert!(fake.requests().is_empty());
    assert_eq!(ledger.spent("team-a"), 0);
    Ok(())
}
