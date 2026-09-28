use super::*;
use respan_gateway_core::http_body_util::BodyExt;

/// Whether an SSE body holds the whole answer. A replay is written from
/// the stored replies, so its bytes are the gateway's, not the provider's.
fn whole(sse: &str) -> bool {
    sse.contains(r#""content":"Hello""#)
        && sse.contains(r#""content":" world""#)
        && sse.ends_with("data: [DONE]\n\n")
}

#[tokio::test]
async fn a_duplicate_waits_for_the_run_in_flight_and_the_provider_is_called_once()
-> Result<(), Fault> {
    let mock = MockTransport::new();
    mock.reply(OPENAI, mock::sse(STREAM))
        .reply(OPENAI, mock::sse(STREAM));
    let store = Arc::new(Memory::default());
    let gw = gateway(&mock, &store)?;
    // The first is in flight while its body is unread.
    let (first, _) = gw.handle(request(ACME, "op-1")).await;
    let duplicate = tokio::spawn({
        let gw = gw.clone();
        async move { send(&gw, request(ACME, "op-1")).await }
    });
    let waiting = || {
        locked(&store.0)
            .values()
            .map(|w| w.receiver_count())
            .sum::<usize>()
    };
    while waiting() == 0 {
        tokio::task::yield_now().await;
    }
    read_whole(first).await.2?;
    let duplicate = duplicate
        .await
        .map_err(|e| Fault::internal(e.to_string()))??;
    assert!(whole(&duplicate), "{duplicate}");
    let retry = send(&gw, request(ACME, "op-1")).await?;
    assert!(whole(&retry), "a later retry too");
    assert_eq!(mock.sent().len(), 1);
    Ok(())
}

#[tokio::test]
async fn the_key_is_bound_to_the_credential_and_the_body() -> Result<(), Fault> {
    let mock = MockTransport::new();
    for _ in 0..3 {
        mock.reply(OPENAI, mock::sse(STREAM));
    }
    let gw = gateway(&mock, &Arc::default())?;
    send(&gw, request(ACME, "op-1")).await?;
    // Another credential with the same key, even claiming acme in a
    // header, runs on its own.
    let mut spoof = request("Bearer beta-key", "op-1");
    spoof
        .headers_mut()
        .insert("x-tenant", HeaderValue::from_static("acme"));
    send(&gw, spoof).await?;
    assert_eq!(mock.sent().len(), 2);
    // The same key with another body runs too.
    let other = r#"{"model":"gpt-x","stream":true,"messages":[{"role":"user","content":"bye"}]}"#;
    send(&gw, keyed(ACME, "op-1", other)).await?;
    assert_eq!(mock.sent().len(), 3);
    Ok(())
}

#[tokio::test]
async fn a_zero_retention_request_stores_nothing() -> Result<(), Fault> {
    let mock = MockTransport::new();
    mock.reply(OPENAI, mock::sse(STREAM))
        .reply(OPENAI, mock::sse(STREAM));
    let store = Arc::new(Memory::default());
    let gw = gateway(&mock, &store)?;
    for _ in 0..2 {
        let mut req = request(ACME, "op-1");
        req.extensions_mut().insert(NoContent);
        assert!(whole(&send(&gw, req).await?));
    }
    assert!(locked(&store.0).is_empty(), "nothing stored");
    assert_eq!(mock.sent().len(), 2, "so each request runs");
    Ok(())
}

#[tokio::test]
async fn a_run_cut_by_a_disconnect_stores_nothing_and_runs_again() -> Result<(), Fault> {
    let mock = MockTransport::new();
    mock.reply(OPENAI, mock::sse(STREAM))
        .reply(OPENAI, mock::sse(STREAM));
    let gw = gateway(&mock, &Arc::default())?;
    let (resp, outcome) = gw.handle(request(ACME, "op-1")).await;
    let mut body = resp.into_body();
    let frame = body.frame().await.transpose()?;
    assert!(frame.is_some(), "the client got the start");
    drop(body);
    let end = outcome.await.end.map_err(|f| f.code().to_owned());
    assert_eq!(end, Err("abandoned".into()), "core cut the call");
    gw.idle().await;

    let retry = send(&gw, request(ACME, "op-1")).await?;
    assert!(whole(&retry), "not half an answer: {retry}");
    assert_eq!(mock.sent().len(), 2, "the cut run cost a second call");
    assert!(
        whole(&send(&gw, request(ACME, "op-1")).await?),
        "now it is stored"
    );
    assert_eq!(mock.sent().len(), 2);
    Ok(())
}
