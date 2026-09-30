use super::*;
use respan_gateway_formats::conversation::Conversation;

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn fifty_parallel_requests_never_overspend_a_one_dollar_wallet() -> Result<(), Fault> {
    let (mock, ledger) = (MockTransport::new(), wallets());
    let gw = gateway(&mock, ledger.clone())?;
    let served = fifty(&gw, &mock, "sk-b1").await;
    let spent = ledger.spent("acme");
    assert!(spent <= 1_000_000, "overspent: {spent}");
    assert_eq!(spent, served as i64 * 80_020, "each settled once");
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

/// The kind reads the larger of two spellings, so the hold covers whichever
/// one a provider reads.
#[tokio::test]
async fn two_spellings_of_max_tokens_cannot_overspend() -> Result<(), Fault> {
    let (mock, ledger) = (MockTransport::new(), wallets());
    let gw = gateway(&mock, ledger.clone())?;
    let body = r#"{"model":"gpt-x","max_tokens":100000,"max_completion_tokens":10,"messages":[{"role":"user","content":"hi"}]}"#;
    // Priced at the larger limit, 100,000 tokens: more than team-a has.
    assert_eq!(
        send(&gw, "sk-a1", body).await?,
        StatusCode::PAYMENT_REQUIRED
    );
    assert!(mock.sent().is_empty());
    assert_eq!(ledger.spent("team-a"), 0);
    Ok(())
}

/// Use case 4 with use case 9: a hedge races two deployments, and the one
/// that loses is charged too. It was abandoned before it reported any usage,
/// and it may have cost all it was allowed, so it pays at least its hold.
#[tokio::test(start_paused = true)]
async fn the_loser_of_a_hedge_is_charged_at_least_its_hold() -> Result<(), Fault> {
    use respan_gateway_core::{ClientContext, Plugin, Response, async_trait};
    use respan_gateway_http::{Exchange, RequestHead, ResponseHead, Transport};
    use respan_gateway_routing::{Until, race};
    use std::time::Duration;

    const EU: &str = "http://openai-eu.test/v1";

    /// Answers after a delay: the deployment that loses.
    struct Slow<T>(T, Duration);

    impl Slow<Transport> {
        pub async fn send(
            &self,
            call: Exchange<RequestHead>,
        ) -> Result<Exchange<ResponseHead>, Fault> {
            tokio::time::sleep(self.1).await;
            self.0.send(call).await
        }
    }
    impl<T: Into<Transport>> From<Slow<T>> for respan_gateway_http::Transport {
        fn from(value: Slow<T>) -> Self {
            let value = std::sync::Arc::new(Slow(value.0.into(), value.1));
            Self::new(move |call| {
                let value = value.clone();
                async move { value.send(call).await }
            })
        }
    }

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
                Until::content(respan_gateway_formats::conversation::Reply::is_content),
            )
            .await
            .map(Some)
        }
    }

    let (mock, ledger) = (MockTransport::new(), wallets());
    let prices = prices();
    let accounting = Accounting::new(ledger.clone(), move |px, tally| quote(px, tally, &prices));
    let slow = Slow(mock.at(OPENAI), Duration::from_secs(1));
    let gw = Gateway::new()
        .provider::<Conversation, _, _>("openai", OpenAiChat, respan_gateway_http::connect(slow))?
        .provider::<Conversation, _, _>(
            "openai-eu",
            OpenAiChat,
            respan_gateway_http::connect(mock.at(EU)),
        )?
        .client::<Conversation, _, _>(OpenAiChat)?
        .plugin("hedge", Hedge)?
        .plugin("accounting", accounting)?;
    mock.reply(EU, answer());

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
    Ok(())
}

/// A client's absurd `max_tokens` prices high, never wraps to a free hold.
#[tokio::test]
async fn a_huge_max_tokens_is_priced_high_not_wrapped() -> Result<(), Fault> {
    let (mock, ledger) = (MockTransport::new(), wallets());
    let gw = gateway(&mock, ledger.clone())?;
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
    assert!(mock.sent().is_empty());
    assert_eq!(ledger.spent("team-a"), 0);
    Ok(())
}
