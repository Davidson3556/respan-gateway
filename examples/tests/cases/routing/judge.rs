use super::*;
use respan_gateway_examples::Meter;
use respan_gateway_sdk::conversation::units;

#[tokio::test]
async fn the_verdict_picks_the_target_and_the_provider_edge_covers_the_judge() -> Result<(), Fault>
{
    let (judge, cheap, strong) = tokio::join!(
        FakeOpenAi::start(),
        FakeOpenAi::start(),
        FakeOpenAi::start()
    );
    judge.answer(answer("HARD"));
    strong.answer(answer("ok"));
    let egress = Egress::default();
    let meter = Meter::default();
    let gw = gateway(&judge, &cheap, &strong, egress.clone())?.plugin(meter.clone());
    let (status, _, _) =
        read_whole(serve(&gw, post("/v1/chat/completions", REQUEST)).await.0).await;
    assert_eq!(status, http::StatusCode::OK);
    assert_eq!(
        (
            judge.requests().len(),
            cheap.requests().len(),
            strong.requests().len()
        ),
        (1, 0, 1)
    );
    assert_eq!(judge.requests()[0].path, "/v1/chat/completions");
    assert_eq!(strong.requests()[0].path, "/v1/chat/completions");
    let judged = String::from_utf8_lossy(&judge.requests()[0].raw).into_owned();
    assert!(
        judged.contains("judge-mini") && judged.contains("Riemann"),
        "{judged}"
    );
    assert_eq!(*egress.seen(), ["judge", "strong"]);
    gw.idle().await;
    // The judge's tokens are billed like the answer's.
    assert_eq!(meter.usage().get(units::INPUT_TOKENS), Some(18));
    Ok(())
}

#[tokio::test]
async fn an_easy_verdict_keeps_the_cheap_model() -> Result<(), Fault> {
    let (judge, cheap, strong) = tokio::join!(
        FakeOpenAi::start(),
        FakeOpenAi::start(),
        FakeOpenAi::start()
    );
    judge.answer(answer("EASY"));
    cheap.answer(answer("ok"));
    let gw = gateway(&judge, &cheap, &strong, Egress::default())?;
    read_whole(serve(&gw, post("/v1/chat/completions", REQUEST)).await.0)
        .await
        .2?;
    assert_eq!(
        (
            judge.requests().len(),
            cheap.requests().len(),
            strong.requests().len()
        ),
        (1, 1, 0)
    );
    assert_eq!(cheap.requests()[0].path, "/v1/chat/completions");
    Ok(())
}

#[tokio::test]
async fn a_provider_edge_refusal_stops_the_judge_too() -> Result<(), Fault> {
    let (judge, cheap, strong) = tokio::join!(
        FakeOpenAi::start(),
        FakeOpenAi::start(),
        FakeOpenAi::start()
    );
    let gw = gateway(&judge, &cheap, &strong, Egress::default())?;
    let mut req = post("/v1/chat/completions", REQUEST);
    req.headers_mut()
        .insert("x-blocked", http::HeaderValue::from_static("1"));
    let (status, _, _) = read_whole(serve(&gw, req).await.0).await;
    assert_eq!(status, http::StatusCode::FORBIDDEN);
    assert_eq!(
        (
            judge.requests().len(),
            cheap.requests().len(),
            strong.requests().len()
        ),
        (0, 0, 0),
        "the judge's call never left"
    );
    Ok(())
}
