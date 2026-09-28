use super::*;
use respan_gateway_examples::Meter;
use respan_gateway_formats::conversation::units;

#[tokio::test]
async fn the_verdict_picks_the_target_and_the_provider_edge_covers_the_judge() -> Result<(), Fault>
{
    let mock = MockTransport::new();
    mock.reply(JUDGE, answer("HARD"))
        .reply(STRONG, answer("ok"));
    let egress = Egress::default();
    let meter = Meter::default();
    let gw = gateway(&mock, egress.clone())?.plugin("meter", meter.clone())?;
    let (status, _, _) = read_whole(gw.handle(post("/v1/chat/completions", REQUEST)).await.0).await;
    assert_eq!(status, http::StatusCode::OK);
    let sent: Vec<String> = mock.sent().iter().map(|r| r.uri().to_string()).collect();
    assert_eq!(
        sent,
        [
            format!("{JUDGE}/chat/completions"),
            format!("{STRONG}/chat/completions")
        ]
    );
    let judged = String::from_utf8_lossy(mock.sent()[0].body()).into_owned();
    assert!(
        judged.contains("judge-mini") && judged.contains("Riemann"),
        "{judged}"
    );
    assert_eq!(*egress.seen(), ["judge", "strong"]);
    gw.idle().await;
    // The judge's tokens are billed like the answer's.
    assert_eq!(meter.usage().get(units::INPUT_TOKENS), Some(18.0));
    Ok(())
}

#[tokio::test]
async fn an_easy_verdict_keeps_the_cheap_model() -> Result<(), Fault> {
    let mock = MockTransport::new();
    mock.reply(JUDGE, answer("EASY")).reply(CHEAP, answer("ok"));
    let gw = gateway(&mock, Egress::default())?;
    read_whole(gw.handle(post("/v1/chat/completions", REQUEST)).await.0)
        .await
        .2?;
    assert_eq!(
        mock.sent()[1].uri().to_string(),
        format!("{CHEAP}/chat/completions")
    );
    Ok(())
}

#[tokio::test]
async fn a_provider_edge_refusal_stops_the_judge_too() -> Result<(), Fault> {
    let mock = MockTransport::new();
    let gw = gateway(&mock, Egress::default())?;
    let mut req = post("/v1/chat/completions", REQUEST);
    req.headers_mut()
        .insert("x-blocked", http::HeaderValue::from_static("1"));
    let (status, _, _) = read_whole(gw.handle(req).await.0).await;
    assert_eq!(status, http::StatusCode::FORBIDDEN);
    assert!(mock.sent().is_empty(), "the judge's call never left");
    Ok(())
}
