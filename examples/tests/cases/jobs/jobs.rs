use super::*;
use respan_gateway_core::Provider;
use respan_gateway_examples::LIMIT;
use respan_gateway_http::http_body_util::BodyExt;

#[tokio::test]
async fn a_completed_result_survives_reopening_the_database() -> Result<(), Fault> {
    let path = std::env::temp_dir().join(format!(
        "respan-idempotency-{}-{}.db",
        std::process::id(),
        hash(std::time::SystemTime::now())
    ));
    let mock = FakeOpenAi::start().await;
    mock.answer(provider_support::sse(STREAM));
    {
        let store = Arc::new(Sqlite::open(&path)?);
        let gateway = gateway(provider_support::openai(&mock.base_url())?, &store)?;
        assert!(whole(&send(&gateway, keyed(ACME, "durable", BODY)).await?));
    }
    {
        let store = Arc::new(Sqlite::open(&path)?);
        let gateway = gateway(provider_support::openai(&mock.base_url())?, &store)?;
        assert!(whole(&send(&gateway, keyed(ACME, "durable", BODY)).await?));
    }
    assert_eq!(
        mock.requests().len(),
        1,
        "reopening replays, without another provider call"
    );
    std::fs::remove_file(path).map_err(|e| Fault::internal(e.to_string()))?;
    Ok(())
}

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
    let mock = FakeOpenAi::start().await;
    mock.answer(provider_support::sse(STREAM));
    mock.answer(provider_support::sse(STREAM));
    let store = Arc::new(Memory::default());
    let gw = gateway(provider_support::openai(&mock.base_url())?, &store)?;
    // The first is in flight while its body is unread.
    let (first, _) = serve(&gw, keyed(ACME, "op-1", BODY)).await;
    let duplicate = tokio::spawn({
        let gw = gw.clone();
        async move { send(&gw, keyed(ACME, "op-1", BODY)).await }
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
    let retry = send(&gw, keyed(ACME, "op-1", BODY)).await?;
    assert!(whole(&retry), "a later retry too");
    assert_eq!(mock.requests().len(), 1);
    Ok(())
}

#[tokio::test]
async fn the_key_is_bound_to_the_credential_and_the_body() -> Result<(), Fault> {
    let mock = FakeOpenAi::start().await;
    for _ in 0..3 {
        mock.answer(provider_support::sse(STREAM));
    }
    let gw = gateway(
        provider_support::openai(&mock.base_url())?,
        &Arc::<Memory>::default(),
    )?;
    send(&gw, keyed(ACME, "op-1", BODY)).await?;
    // Another credential with the same key, even claiming acme in a
    // header, runs on its own.
    let mut spoof = keyed("Bearer beta-key", "op-1", BODY);
    spoof
        .headers_mut()
        .insert("x-tenant", HeaderValue::from_static("acme"));
    send(&gw, spoof).await?;
    assert_eq!(mock.requests().len(), 2);
    // The same key with another body runs too.
    let other =
        r#"{"model":"gpt-4o-mini","stream":true,"messages":[{"role":"user","content":"bye"}]}"#;
    send(&gw, keyed(ACME, "op-1", other)).await?;
    assert_eq!(mock.requests().len(), 3);
    Ok(())
}

#[tokio::test]
async fn every_credential_form_isolates_cached_results() -> Result<(), Fault> {
    for form in ["x-api-key", "api-key", "query", "subprotocol"] {
        let mock = FakeOpenAi::start().await;
        mock.answer(provider_support::sse(STREAM));
        mock.answer(provider_support::sse(STREAM));
        let gw = gateway(
            provider_support::openai(&mock.base_url())?,
            &Arc::<Memory>::default(),
        )?;
        for token in ["acme", "beta", "acme", "beta"] {
            let mut req = keyed(ACME, "op-1", BODY);
            req.headers_mut().remove(AUTHORIZATION);
            match form {
                "query" => {
                    *req.uri_mut() = format!("/v1/chat/completions?api_key={token}")
                        .parse()
                        .map_err(|_| Fault::internal("test query URI"))?;
                }
                "subprotocol" => {
                    req.headers_mut().insert(
                        "sec-websocket-protocol",
                        HeaderValue::from_str(&format!("openai-insecure-api-key.{token}"))
                            .map_err(|_| Fault::internal("test subprotocol credential"))?,
                    );
                }
                header => {
                    req.headers_mut()
                        .insert(header, HeaderValue::from_static(token));
                }
            }
            assert!(whole(&send(&gw, req).await?));
        }
        assert_eq!(mock.requests().len(), 2, "each {form} tenant runs once");
    }
    Ok(())
}

#[tokio::test]
async fn a_zero_retention_request_stores_nothing() -> Result<(), Fault> {
    let mock = FakeOpenAi::start().await;
    mock.answer(provider_support::sse(STREAM));
    mock.answer(provider_support::sse(STREAM));
    let store = Arc::new(Memory::default());
    let gw = gateway(provider_support::openai(&mock.base_url())?, &store)?;
    for _ in 0..2 {
        let mut req = keyed(ACME, "op-1", BODY);
        let mut values = respan_gateway_core::Extensions::new();
        values.insert(NoContent);
        req.extensions_mut().insert(values);
        assert!(whole(&send(&gw, req).await?));
    }
    assert!(locked(&store.0).is_empty(), "nothing stored");
    assert_eq!(mock.requests().len(), 2, "so each request runs");
    Ok(())
}

#[tokio::test]
async fn a_run_cut_by_a_disconnect_stores_nothing_and_runs_again() -> Result<(), Fault> {
    let mock = FakeOpenAi::start().await;
    mock.answer(provider_support::sse(STREAM));
    mock.answer(provider_support::sse(STREAM));
    let gw = gateway(
        provider_support::openai(&mock.base_url())?,
        &Arc::<Memory>::default(),
    )?;
    let (resp, outcome) = serve(&gw, keyed(ACME, "op-1", BODY)).await;
    let mut body = resp.into_body();
    let frame = body.frame().await.transpose()?;
    assert!(frame.is_some(), "the client got the start");
    drop(body);
    let end = outcome
        .await
        .map_err(|f| respan_gateway_http_faults::error_code(&f).to_owned());
    assert_eq!(end, Err("abandoned".into()), "core cut the call");
    gw.idle().await;

    let retry = send(&gw, keyed(ACME, "op-1", BODY)).await?;
    assert!(whole(&retry), "not half an answer: {retry}");
    assert_eq!(mock.requests().len(), 2, "the cut run cost a second call");
    assert!(
        whole(&send(&gw, keyed(ACME, "op-1", BODY)).await?),
        "now it is stored"
    );
    assert_eq!(mock.requests().len(), 2);
    Ok(())
}

#[tokio::test]
async fn a_lost_lease_cancels_provider_opening_and_retries_with_the_same_upstream_key()
-> Result<(), Fault> {
    use respan_gateway_examples::store::Database;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::time::Duration;

    struct Dropped(Arc<AtomicBool>);
    impl Drop for Dropped {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }
    let path = std::env::temp_dir().join(format!(
        "respan-jobs-lease-{}-{}.db",
        std::process::id(),
        hash(std::time::SystemTime::now())
    ));
    let store = Arc::new(Sqlite::with_lease(&path, Duration::from_millis(600))?);
    let mock = FakeOpenAi::start().await;
    mock.answer(provider_support::sse(STREAM));
    let calls = Arc::new(AtomicUsize::new(0));
    let dropped = Arc::new(AtomicBool::new(false));
    let keys = Arc::new(Mutex::new(Vec::new()));
    let transport = {
        let (calls, dropped, keys) = (calls.clone(), dropped.clone(), keys.clone());
        let transport = provider_support::openai(&mock.base_url())?;
        move |call: respan_gateway_http::http::Request<respan_gateway_http::Body>| {
            let (calls, dropped, keys, transport) = (
                calls.clone(),
                dropped.clone(),
                keys.clone(),
                transport.clone(),
            );
            async move {
                locked(&keys).push(call.headers().get("idempotency-key").cloned());
                if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                    let _dropped = Dropped(dropped);
                    let _call = call;
                    std::future::pending().await
                } else {
                    transport.send(call).await
                }
            }
        }
    };
    let gw = Gateway::new()
        .client::<Input, _, _>(OpenAiChat)?
        .provider(Provider::new(
            "openai",
            OpenAiChat,
            respan_gateway_http::connect(supervised(transport), LIMIT),
        ))?
        .plugin(Idempotent(store))
        .target("openai")?;
    let first = tokio::spawn({
        let gw = gw.clone();
        async move { read_whole(serve(&gw, keyed(ACME, "lease", BODY)).await.0).await }
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        while calls.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .map_err(|_| Fault::internal("provider did not start"))?;
    let db = Database::open(&path)?;
    let (locked_db, ready) = tokio::sync::oneshot::channel();
    let blocker = tokio::spawn(async move {
        db.transact(move |_| {
            let _ = locked_db.send(());
            std::thread::sleep(Duration::from_millis(900));
            Ok(())
        })
        .await
    });
    ready
        .await
        .map_err(|_| Fault::internal("database did not lock"))?;
    let response = tokio::time::timeout(Duration::from_millis(550), first)
        .await
        .map_err(|_| Fault::internal("lease loss did not stop provider opening"))?
        .map_err(|error| Fault::internal("request failed").with_source(error))?;
    assert!(response.0.is_server_error());
    assert!(
        dropped.load(Ordering::SeqCst),
        "provider must stop before lease reuse"
    );
    blocker
        .await
        .map_err(|error| Fault::internal("blocker failed").with_source(error))??;
    gw.idle().await;
    assert!(whole(&send(&gw, keyed(ACME, "lease", BODY)).await?));
    let keys = locked(&keys);
    assert_eq!(keys.len(), 2);
    assert!(keys[0].is_some());
    assert_eq!(
        keys[0], keys[1],
        "upstream deduplication survives a lost local lease"
    );
    assert_eq!(
        keys[0].as_ref().map(HeaderValue::as_bytes).map(<[u8]>::len),
        Some(64)
    );
    drop(keys);
    drop(gw);
    let _ = std::fs::remove_file(path);
    Ok(())
}

#[tokio::test]
async fn an_expired_claim_cannot_renew_or_publish_a_result() -> Result<(), Fault> {
    use std::time::Duration;
    let path = std::env::temp_dir().join(format!(
        "respan-jobs-expired-{}-{}.db",
        std::process::id(),
        hash(std::time::SystemTime::now())
    ));
    let store = Sqlite::with_lease(&path, Duration::from_millis(300))?;
    let key = ("tenant".into(), "key".into(), "body".into());
    let Claim::Run(owner, _) = store.claim(&key).await? else {
        panic!("fresh claim must run")
    };
    tokio::time::sleep(Duration::from_millis(350)).await;
    assert!(store.renew(&key, owner).await.is_err());
    let stale = vec![
        Output::block_start(0, respan_gateway_conversation::Block::Text),
        Output::text(0, "stale"),
        Output::block_end(0),
    ];
    store.finish(&key, owner, Some(Arc::new(stale))).await?;
    assert!(
        matches!(store.claim(&key).await?, Claim::Run(_, _)),
        "expired owner must not publish"
    );
    drop(store);
    let _ = std::fs::remove_file(path);
    Ok(())
}
