use super::*;
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
