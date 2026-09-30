use super::*;

fn models(mock: &MockTransport) -> Vec<String> {
    let model = |b: &[u8]| {
        let v: serde_json::Value = serde_json::from_slice(b).ok()?;
        v["model"].as_str().map(Into::into)
    };
    mock.sent().iter().filter_map(|r| model(r.body())).collect()
}

/// Until `n` requests are parked.
async fn parked(slots: &Slots, n: usize) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while slots.counts().await.unwrap_or_else(|e| panic!("{e}")).0 < n {
        assert!(Instant::now() < deadline, "waiters did not reach the queue");
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

#[tokio::test]
async fn a_request_past_its_deadline_gets_429_with_retry_after() -> Result<(), Fault> {
    let mock = MockTransport::new();
    mock.reply(OPENAI, mock::json(StatusCode::OK, OK));
    let gw = gateway(&mock, &Slots::new(1)?, Duration::from_millis(50))?;
    let (held, _) = serve(&gw, request("a", "paid")).await;
    let started = Instant::now();
    let (status, headers, _) = read_whole(serve(&gw, request("b", "free")).await.0).await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(headers[RETRY_AFTER], "1");
    assert!(started.elapsed() >= Duration::from_millis(50), "it waited");
    drop(held);
    Ok(())
}

#[tokio::test]
async fn parked_requests_are_admitted_by_priority_when_a_slot_frees() -> Result<(), Fault> {
    let mock = MockTransport::new();
    for _ in 0..3 {
        mock.reply(OPENAI, mock::json(StatusCode::OK, OK));
    }
    let slots = Slots::new(1)?;
    let gw = gateway(&mock, &slots, Duration::from_secs(5))?;
    let (held, _) = serve(&gw, request("first", "paid")).await;
    let park = |model: &'static str, tier| {
        let gw = gw.clone();
        tokio::spawn(async move { read_whole(serve(&gw, request(model, tier)).await.0).await.0 })
    };
    let free = park("free", "free");
    parked(&slots, 1).await;
    let paid = park("paid", "paid");
    parked(&slots, 2).await;
    drop(held);
    let statuses = (paid.await.ok(), free.await.ok());
    assert_eq!(statuses, (Some(StatusCode::OK), Some(StatusCode::OK)));
    assert_eq!(models(&mock), ["first", "paid", "free"], "paid went first");
    Ok(())
}

#[tokio::test]
async fn a_parked_request_that_leaves_loses_no_slot() -> Result<(), Fault> {
    let mock = MockTransport::new();
    mock.reply(OPENAI, mock::json(StatusCode::OK, OK));
    let slots = Slots::new(1)?;
    let gw = gateway(&mock, &slots, Duration::from_secs(5))?;
    let (held, _) = serve(&gw, request("first", "paid")).await;
    let gone = tokio::spawn({
        let gw = gw.clone();
        async move { drop(serve(&gw, request("gone", "paid")).await) }
    });
    parked(&slots, 1).await;
    gone.abort();
    let _ = gone.await;
    drop(held);
    gw.idle().await;
    assert_eq!(slots.counts().await?.1, 1, "the slot came back");
    Ok(())
}

/// A request for `model` from `user`, on the free tier.
fn from(model: &str, user: &'static str) -> http::Request<respan_gateway_http::Body> {
    let mut req = request(model, "free");
    req.headers_mut()
        .insert("x-user", HeaderValue::from_static(user));
    req
}

#[tokio::test]
async fn one_heavy_user_cannot_starve_another() -> Result<(), Fault> {
    let mock = MockTransport::new();
    for _ in 0..6 {
        mock.reply(OPENAI, mock::json(StatusCode::OK, OK));
    }
    let slots = Slots::new(1)?;
    let gw = gateway(&mock, &slots, Duration::from_secs(5))?;
    let (held, _) = serve(&gw, from("first", "heavy")).await;
    let park = |model: &'static str, user| {
        let gw = gw.clone();
        tokio::spawn(async move { read_whole(serve(&gw, from(model, user)).await.0).await.0 })
    };
    // The heavy user parks four requests before the light one parks one.
    let mut all = Vec::new();
    for (i, model) in ["h1", "h2", "h3", "h4"].into_iter().enumerate() {
        all.push(park(model, "heavy"));
        parked(&slots, i + 1).await;
    }
    all.push(park("l1", "light"));
    parked(&slots, 5).await;
    drop(held);
    for done in all {
        assert_eq!(done.await.ok(), Some(StatusCode::OK));
    }
    // Served in turns: the light user's one request is second, not last.
    assert_eq!(models(&mock), ["first", "h1", "l1", "h2", "h3", "h4"]);
    Ok(())
}

#[tokio::test]
async fn priority_still_comes_before_fair_share() -> Result<(), Fault> {
    let mock = MockTransport::new();
    for _ in 0..3 {
        mock.reply(OPENAI, mock::json(StatusCode::OK, OK));
    }
    let slots = Slots::new(1)?;
    let gw = gateway(&mock, &slots, Duration::from_secs(5))?;
    let (held, _) = serve(&gw, from("first", "a")).await;
    let park = |model: &'static str, user, tier| {
        let gw = gw.clone();
        let mut req = request(model, tier);
        req.headers_mut()
            .insert("x-user", HeaderValue::from_static(user));
        tokio::spawn(async move { read_whole(serve(&gw, req).await.0).await.0 })
    };
    let free = park("free", "b", "free");
    parked(&slots, 1).await;
    let paid = park("paid", "a", "paid");
    parked(&slots, 2).await;
    drop(held);
    assert_eq!(
        (paid.await.ok(), free.await.ok()),
        (Some(StatusCode::OK), Some(StatusCode::OK))
    );
    assert_eq!(models(&mock), ["first", "paid", "free"]);
    Ok(())
}

#[tokio::test]
async fn configured_weights_give_two_turns_then_one() -> Result<(), Fault> {
    let mock = MockTransport::new();
    for _ in 0..7 {
        mock.reply(OPENAI, mock::json(StatusCode::OK, OK));
    }
    let slots = Slots::new(1)?.weight("heavy", 2)?;
    let gw = gateway(&mock, &slots, Duration::from_secs(5))?;
    let (held, _) = serve(&gw, from("first", "other")).await;
    let mut running = Vec::new();
    for (i, (model, user)) in [
        ("h1", "heavy"),
        ("h2", "heavy"),
        ("h3", "heavy"),
        ("h4", "heavy"),
        ("l1", "light"),
        ("l2", "light"),
    ]
    .into_iter()
    .enumerate()
    {
        let gw = gw.clone();
        running.push(tokio::spawn(async move {
            read_whole(serve(&gw, from(model, user)).await.0).await.0
        }));
        parked(&slots, i + 1).await;
    }
    drop(held);
    for task in running {
        assert_eq!(task.await.ok(), Some(StatusCode::OK));
    }
    assert_eq!(models(&mock), ["first", "h1", "h2", "l1", "h3", "h4", "l2"]);
    Ok(())
}

fn database_path() -> std::path::PathBuf {
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "respan-admission-{}-{stamp}.sqlite",
        std::process::id()
    ))
}

fn io(error: std::io::Error) -> Fault {
    Fault::internal("test process I/O failed").with_source(error)
}

/// Called only by the subprocess regression below.
#[tokio::test]
async fn admission_process_worker() -> Result<(), Fault> {
    let Ok(path) = std::env::var("RESPAN_ADMISSION_TEST_DATABASE") else {
        return Ok(());
    };
    let slots = Slots::open(&path, 1, Duration::from_millis(600))?;
    let _slot = slots
        .acquire(0, "child", Instant::now() + Duration::from_secs(2))
        .await?
        .ok_or_else(|| Fault::internal("child was not admitted"))?;
    std::fs::write(format!("{path}.held"), b"held").map_err(io)?;
    tokio::time::sleep(Duration::from_secs(20)).await;
    Ok(())
}

struct Worker(std::process::Child);
impl Drop for Worker {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[tokio::test]
async fn independent_processes_share_capacity_and_recover_a_crashed_lease() -> Result<(), Fault> {
    let path = database_path();
    let mut worker = Worker(
        std::process::Command::new(std::env::current_exe().map_err(io)?)
            .args(["--exact", "tests::admission_process_worker", "--nocapture"])
            .env("RESPAN_ADMISSION_TEST_DATABASE", &path)
            .stdout(std::process::Stdio::null())
            .spawn()
            .map_err(io)?,
    );
    let marker = std::path::PathBuf::from(format!("{}.held", path.display()));
    tokio::time::timeout(Duration::from_secs(5), async {
        while !marker.exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .map_err(|_| Fault::internal("child did not acquire its lease"))?;
    let slots = Slots::open(&path, 1, Duration::from_millis(600))?;
    // Renewals preserve the child's slot across more than one original lease.
    tokio::time::sleep(Duration::from_millis(750)).await;
    assert!(
        slots
            .acquire(1, "parent", Instant::now() + Duration::from_millis(40))
            .await?
            .is_none()
    );
    worker.0.kill().map_err(io)?;
    worker.0.wait().map_err(io)?;
    let replacement = slots
        .acquire(1, "parent", Instant::now() + Duration::from_secs(2))
        .await?
        .ok_or_else(|| Fault::internal("crashed lease was not recovered"))?;
    replacement.release().await?;
    assert_eq!(slots.counts().await?.1, 1);
    drop(slots);
    let _ = std::fs::remove_file(marker);
    let _ = std::fs::remove_file(path);
    Ok(())
}

#[tokio::test]
async fn losing_the_store_cancels_a_call_waiting_for_its_provider_head() -> Result<(), Fault> {
    use respan_gateway_examples::store::{Database, sql};
    use std::sync::atomic::{AtomicBool, Ordering};
    struct Dropped(Arc<AtomicBool>);
    impl Drop for Dropped {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }
    let path = database_path();
    let slots = Slots::open(&path, 1, Duration::from_millis(600))?;
    let started = Arc::new(AtomicBool::new(false));
    let dropped = Arc::new(AtomicBool::new(false));
    let transport = respan_gateway_http::Transport::new({
        let (started, dropped) = (started.clone(), dropped.clone());
        move |call| {
            let (started, dropped) = (started.clone(), dropped.clone());
            async move {
                let _call = call;
                let _dropped = Dropped(dropped);
                started.store(true, Ordering::SeqCst);
                std::future::pending().await
            }
        }
    });
    let gw = Gateway::new()
        .client::<Conversation, _, _>(OpenAiChat)?
        .provider::<Conversation, _, _>(
            "p",
            OpenAiChat,
            respan_gateway_http::connect(supervised(transport)),
        )?
        .plugin(
            "admission",
            Admission {
                slots: slots.clone(),
                paid: Duration::from_secs(2),
                free: Duration::from_secs(2),
            },
        )?
        .target("p")?;
    let request = tokio::spawn({
        let gw = gw.clone();
        async move {
            read_whole(serve(&gw, from("pending", "user")).await.0)
                .await
                .0
        }
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        while !started.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .map_err(|_| Fault::internal("provider was never started"))?;
    let db = Database::open(&path)?;
    // A competing transaction prevents renewal; work must stop before the
    // old database lease can be reassigned, even without a response head.
    let (locked, lock_ready) = tokio::sync::oneshot::channel();
    let blocker = tokio::spawn(async move {
        db.transact(move |tx| {
            tx.execute("UPDATE admission_config SET capacity=capacity", [])
                .map_err(sql)?;
            let _ = locked.send(());
            std::thread::sleep(Duration::from_millis(850));
            Ok(())
        })
        .await
    });
    lock_ready
        .await
        .map_err(|_| Fault::internal("store lock was not acquired"))?;
    let status = tokio::time::timeout(Duration::from_millis(550), request)
        .await
        .map_err(|_| Fault::internal("lease loss did not cancel provider opening"))?
        .map_err(|error| Fault::internal("request task failed").with_source(error))?;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    assert!(
        dropped.load(Ordering::SeqCst),
        "provider opening was dropped"
    );
    blocker
        .await
        .map_err(|error| Fault::internal("store blocker failed").with_source(error))??;
    gw.idle().await;
    let next = slots
        .acquire(0, "next", Instant::now() + Duration::from_secs(1))
        .await?
        .ok_or_else(|| Fault::internal("capacity was not recovered"))?;
    next.release().await?;
    drop(gw);
    drop(slots);
    let _ = std::fs::remove_file(path);
    Ok(())
}

#[tokio::test]
async fn the_admission_deadline_also_bounds_waiting_for_the_database() -> Result<(), Fault> {
    use respan_gateway_examples::store::Database;
    let path = database_path();
    let slots = Slots::open(&path, 1, Duration::from_secs(3))?;
    let first = slots
        .acquire(0, "first", Instant::now() + Duration::from_secs(1))
        .await?
        .ok_or_else(|| Fault::internal("first slot was not admitted"))?;
    first.release().await?;
    let db = Database::open(&path)?;
    let (locked, lock_ready) = tokio::sync::oneshot::channel();
    let blocker = tokio::spawn(async move {
        db.transact(move |_| {
            let _ = locked.send(());
            std::thread::sleep(Duration::from_millis(100));
            Ok(())
        })
        .await
    });
    lock_ready
        .await
        .map_err(|_| Fault::internal("store lock was not acquired"))?;
    let expired = tokio::time::timeout(
        Duration::from_millis(80),
        slots.acquire(0, "expired", Instant::now() + Duration::from_millis(20)),
    )
    .await
    .map_err(|_| Fault::internal("database wait ignored admission deadline"))??;
    assert!(expired.is_none());
    blocker
        .await
        .map_err(|error| Fault::internal("blocker failed").with_source(error))??;
    // The abandoned blocking transaction can finish late, but its expired
    // ticket cannot take capacity from the next request.
    let next = slots
        .acquire(0, "next", Instant::now() + Duration::from_secs(1))
        .await?
        .ok_or_else(|| Fault::internal("late transaction kept the slot"))?;
    assert_eq!(slots.counts().await?, (0, 0));
    next.release().await?;
    drop(slots);
    let _ = std::fs::remove_file(path);
    Ok(())
}

#[tokio::test]
async fn canceling_during_enqueue_removes_a_ticket_inserted_later() -> Result<(), Fault> {
    use respan_gateway_examples::store::Database;
    let path = database_path();
    let slots = Slots::open(&path, 1, Duration::from_secs(3))?;
    let db = Database::open(&path)?;
    let (locked, lock_ready) = tokio::sync::oneshot::channel();
    let blocker = tokio::spawn(async move {
        db.transact(move |_| {
            let _ = locked.send(());
            std::thread::sleep(Duration::from_millis(150));
            Ok(())
        })
        .await
    });
    lock_ready
        .await
        .map_err(|_| Fault::internal("store lock was not acquired"))?;
    let request = tokio::spawn({
        let slots = slots.clone();
        async move {
            slots
                .acquire(0, "canceled", Instant::now() + Duration::from_secs(30))
                .await
        }
    });
    tokio::time::sleep(Duration::from_millis(30)).await;
    request.abort();
    assert!(request.await.is_err());
    blocker
        .await
        .map_err(|error| Fault::internal("blocker failed").with_source(error))??;
    // Let the already-running insert finish after its awaiting task is gone.
    tokio::time::sleep(Duration::from_millis(50)).await;
    let next = slots
        .acquire(0, "next", Instant::now() + Duration::from_millis(300))
        .await?
        .ok_or_else(|| Fault::internal("canceled enqueue consumed capacity"))?;
    assert_eq!(slots.counts().await?, (0, 0));
    next.release().await?;
    drop(slots);
    let _ = std::fs::remove_file(path);
    Ok(())
}
