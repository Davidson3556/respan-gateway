use super::*;
use respan_gateway_sdk::BadRequest;
use respan_gateway_sdk::conversation::Conversation;
use respan_gateway_sdk::conversation::units;

#[tokio::test]
async fn each_call_is_billed_at_its_models_price() -> Result<(), Fault> {
    let (openai, store) = (FakeOpenAi::start().await, Memory::default());
    let gw = gateway(
        provider_support::openai(&openai.base_url())?.connect(),
        store.clone(),
    )?;
    ask(&gw, &openai, "acme", SMALL, Some((1_000, 500))).await;
    ask(&gw, &openai, "acme", LARGE, Some((1_000, 500))).await;
    ask(&gw, &openai, "globex", UNPRICED, Some((10, 10))).await;
    assert_eq!(openai.requests().len(), 3);
    let rows = store.rows();
    let billed: Vec<_> = rows
        .iter()
        .map(|r| (r.account.as_str(), r.micros))
        .collect();
    // 1000 × 0.15 + 500 × 0.6 = 450; 1000 × 2.5 + 500 × 10 = 7500.
    assert_eq!(
        billed,
        [("acme", Some(450)), ("acme", Some(7_500)), ("globex", None)]
    );
    assert_eq!(rows[0].usage.get(units::OUTPUT_TOKENS), Some(500.0));
    Ok(())
}

#[tokio::test]
async fn a_failed_call_with_no_usage_writes_no_row() -> Result<(), Fault> {
    let (openai, store) = (FakeOpenAi::start().await, Memory::default());
    let gw = gateway(
        provider_support::openai(&openai.base_url())?.connect(),
        store.clone(),
    )?;
    ask(&gw, &openai, "acme", SMALL, None).await;
    assert!(store.rows().is_empty());
    assert_eq!(openai.requests().len(), 1);
    Ok(())
}

/// Refuses the event that carries the usage on the provider edge.
struct Refuses;

#[async_trait]
impl Plugin for Refuses {
    async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
        px.filter(
            |reply: &mut respan_gateway_sdk::conversation::Reply| match reply {
                respan_gateway_sdk::conversation::Reply::Usage(_) => {
                    Err(BadRequest::fault("refused"))
                }
                _ => Ok(()),
            },
        )
    }
}

// Registered last, billing's filter sees each event before an earlier
// plugin's filter fails on it, so the usage in the event is still billed.
#[tokio::test]
async fn a_filter_registered_earlier_cannot_hide_usage_from_billing() -> Result<(), Fault> {
    let (openai, store) = (FakeOpenAi::start().await, Memory::default());
    let prices = HashMap::from([(SMALL, Price::new(150_000, 600_000))]);
    let gw = Gateway::new()
        .provider::<Conversation, _, _>(
            "openai",
            OpenAiChat,
            provider_support::openai(&openai.base_url())?.connect(),
        )?
        .client::<Conversation, _, _>(OpenAiChat)?
        .plugin(Refuses)
        .plugin(Billing {
            prices,
            store: store.clone(),
        })
        .target("openai")?;
    ask(&gw, &openai, "acme", SMALL, Some((1_000, 500))).await;
    assert_eq!(openai.requests().len(), 1);
    let billed: Vec<_> = store.rows().iter().map(|r| r.micros).collect();
    assert_eq!(billed, [Some(450)]);
    Ok(())
}
