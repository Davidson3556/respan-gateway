use super::*;
use respan_gateway_formats::conversation::units;

#[tokio::test]
async fn content_is_logged_except_for_a_zero_retention_tenant() -> Result<(), Fault> {
    let records = run(&MockTransport::new()).await?;
    // Per request: its call first, then the request, sharing its id.
    let ids: Vec<_> = records
        .iter()
        .map(|r| (r.request, r.call.is_some()))
        .collect();
    assert_eq!(ids, [(1, true), (1, false), (2, true), (2, false)]);

    let acme = &records[1];
    assert_eq!(acme.prompt.as_deref(), Some("hi"));
    assert_eq!(acme.completion.as_deref(), Some("Hello world"));
    let zdr = &records[3];
    assert_eq!((&zdr.prompt, &zdr.completion), (&None, &None));
    // A success logs no status.
    assert_eq!((zdr.status, zdr.end.as_str()), (None, "ok"));
    // Billing and audit still flow: the call's usage is on its record.
    assert_eq!(records[2].usage.get(units::OUTPUT_TOKENS), Some(2.0));
    Ok(())
}
