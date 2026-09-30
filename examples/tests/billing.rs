use super::*;
use respan_gateway_formats::conversation::Conversation;
use respan_gateway_formats::conversation::units;

#[tokio::test]
async fn each_call_is_billed_at_its_models_price() -> Result<(), Fault> {
    let (mock, store) = (MockTransport::new(), Memory::default());
    let gw = gateway(&mock, store.clone())?;
    ask(&gw, &mock, "acme", "small", Some((1_000, 500))).await;
    ask(&gw, &mock, "acme", "large", Some((1_000, 500))).await;
    ask(&gw, &mock, "globex", "unpriced", Some((10, 10))).await;
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
    let (mock, store) = (MockTransport::new(), Memory::default());
    let gw = gateway(&mock, store.clone())?;
    ask(&gw, &mock, "acme", "small", None).await;
    assert!(store.rows().is_empty());
    Ok(())
}

/// Refuses every answer on the provider edge.
struct Refuses;

#[async_trait]
impl Plugin for Refuses {
    async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault> {
        px.filter(|_: &mut respan_gateway_formats::conversation::Reply| {
            Err(Fault::bad_request("refused"))
        })
    }
}

// Registered last, billing's filter sees each reply before an earlier
// plugin's filter fails on it, so the usage in it is still billed.
#[tokio::test]
async fn a_filter_registered_earlier_cannot_hide_usage_from_billing() -> Result<(), Fault> {
    let (mock, store) = (MockTransport::new(), Memory::default());
    let prices = HashMap::from([("small", Price::new(150_000, 600_000))]);
    let gw = Gateway::new()
        .provider::<Conversation, _, _>(
            "openai",
            OpenAiChat,
            respan_gateway_http::connect(mock.at(OPENAI)),
        )?
        .client::<Conversation, _, _>(OpenAiChat)?
        .plugin("refuses", Refuses)?
        .plugin(
            "billing",
            Billing {
                prices,
                store: store.clone(),
            },
        )?
        .target("openai")?;
    ask(&gw, &mock, "acme", "small", Some((1_000, 500))).await;
    let billed: Vec<_> = store.rows().iter().map(|r| r.micros).collect();
    assert_eq!(billed, [Some(450)]);
    Ok(())
}
