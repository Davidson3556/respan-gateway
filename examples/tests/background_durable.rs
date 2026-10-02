use super::*;
use respan_gateway_examples::LIMIT;
use respan_gateway_http::http::StatusCode;

fn database() -> std::path::PathBuf {
    let mut random = [0u8; 8];
    getrandom::fill(&mut random).expect("test entropy");
    std::env::temp_dir().join(format!("respan-jobs-{}.db", hex(&random)))
}

fn account() -> [u8; 32] {
    identity(Some(&Credential::of(
        request("POST", "/v1/responses", "{}").headers().clone(),
    )))
}

#[tokio::test]
async fn completed_jobs_and_signed_ids_survive_reopening() -> Result<(), Fault> {
    let path = database();
    let (east, west) = deployments().await?;
    west.answer(
        StatusCode::OK,
        r#"{"id":"native-durable","status":"completed","usage":{"total_tokens":7}}"#,
    );
    let id = {
        let jobs = Jobs::open(&path)?;
        jobs.budget(account(), 10_000).await?;
        let gateway = gateway(
            east.transport()?.connect(),
            west.transport()?.connect(),
            &jobs,
        )?;
        let queued = send(
            &gateway,
            request(
                "POST",
                "/v1/responses",
                r#"{"background":true,"input":"hello"}"#,
            ),
        )
        .await?;
        gateway.idle().await;
        queued["id"].as_str().expect("job id").to_owned()
    };
    let jobs = Jobs::open(&path)?;
    let gateway = gateway(
        east.transport()?.connect(),
        west.transport()?.connect(),
        &jobs,
    )?;
    let done = send(&gateway, request("GET", &format!("/v1/responses/{id}"), "")).await?;
    assert_eq!(done["status"], "completed");
    assert_eq!(done["id"], id);
    assert_eq!(jobs.native(&id).await?.as_deref(), Some("native-durable"));
    let amounts: (i64, i64) = jobs
        .db(|tx| {
            tx.query_row("SELECT held,spent FROM job_accounts", [], |r| {
                Ok((r.get(0)?, r.get(1)?))
            })
            .map_err(sql)
        })
        .await?;
    assert_eq!(amounts, (0, 7));
    assert_eq!(west.requests().len(), 1);
    assert!(
        east.requests().is_empty(),
        "no requests to the east deployment"
    );
    Ok(())
}

#[test]
fn crash_child() -> Result<(), Fault> {
    let Some(path) = std::env::var_os("RESPAN_JOB_CRASH_DB") else {
        return Ok(());
    };
    let runtime = tokio::runtime::Runtime::new().map_err(|e| Fault::internal(e.to_string()))?;
    runtime.block_on(async {
        let jobs = Jobs::open(path)?;
        jobs.budget(account(), 10_000).await?;
        let pending =
            || respan_gateway_http::connect(|_| async { std::future::pending().await }, LIMIT);
        let gateway = gateway(pending(), pending(), &jobs)?;
        let queued = send(
            &gateway,
            request(
                "POST",
                "/v1/responses",
                r#"{"background":true,"input":"survive me"}"#,
            ),
        )
        .await?;
        assert_eq!(queued["status"], "queued");
        // Deliberately bypass Drop and all end hooks, like a killed worker.
        std::process::exit(0);
        #[allow(unreachable_code)]
        Ok::<(), Fault>(())
    })
}

#[tokio::test]
async fn a_process_crash_preserves_its_hold_and_recovers_the_request() -> Result<(), Fault> {
    let path = database();
    let status = std::process::Command::new(
        std::env::current_exe().map_err(|e| Fault::internal(e.to_string()))?,
    )
    .args(["--exact", "durable_tests::crash_child", "--nocapture"])
    .env("RESPAN_JOB_CRASH_DB", &path)
    .status()
    .map_err(|e| Fault::internal(e.to_string()))?;
    assert!(status.success());
    let jobs = Jobs::open(&path)?;
    let id: String = jobs
        .db(|tx| {
            let held: i64 = tx
                .query_row("SELECT held FROM job_accounts", [], |r| r.get(0))
                .map_err(sql)?;
            assert!(held > 0, "a crash does not forgive reserved cost");
            // Advance the persisted lease deadline without waiting 30 real seconds.
            tx.execute("UPDATE background_jobs SET lease=0", [])
                .map_err(sql)?;
            tx.query_row("SELECT id FROM background_jobs", [], |r| r.get(0))
                .map_err(sql)
        })
        .await?;
    let (east, west) = deployments().await?;
    west.answer(
        StatusCode::OK,
        r#"{"id":"native-after-restart","status":"completed","usage":{"total_tokens":9}}"#,
    );
    let gateway = gateway(
        east.transport()?.connect(),
        west.transport()?.connect(),
        &jobs,
    )?;
    jobs.resume(&gateway).await?;
    gateway.idle().await;
    let done = send(&gateway, request("GET", &format!("/v1/responses/{id}"), "")).await?;
    assert_eq!(done["status"], "completed");
    assert_eq!(west.requests()[0].headers["idempotency-key"], id);
    let amounts: (i64, i64) = jobs
        .db(|tx| {
            tx.query_row("SELECT held,spent FROM job_accounts", [], |r| {
                Ok((r.get(0)?, r.get(1)?))
            })
            .map_err(sql)
        })
        .await?;
    assert_eq!(amounts, (0, 9));
    Ok(())
}

#[tokio::test]
async fn durable_budget_refuses_an_unfunded_job_before_it_starts() -> Result<(), Fault> {
    let jobs = Jobs::default();
    jobs.budget(account(), 1).await?;
    let (east, west) = deployments().await?;
    let gateway = gateway(
        east.transport()?.connect(),
        west.transport()?.connect(),
        &jobs,
    )?;
    let req = request(
        "POST",
        "/v1/responses",
        r#"{"background":true,"input":"hello"}"#,
    );
    let (status, ..) = read_whole(serve(&gateway, req).await.0).await;
    assert_eq!(status, StatusCode::PAYMENT_REQUIRED);
    assert!(west.requests().is_empty());
    assert!(
        east.requests().is_empty(),
        "no requests to the east deployment"
    );
    Ok(())
}

#[tokio::test]
async fn finish_webhooks_retry_from_the_durable_outbox() -> Result<(), Fault> {
    let received = Arc::new(Mutex::new(Vec::new()));
    let requests = received.clone();
    let app = axum::Router::new().route(
        "/done",
        axum::routing::post(move |headers: http::HeaderMap, body: Bytes| {
            let requests = requests.clone();
            async move {
                let mut seen = lock(&requests);
                seen.push((
                    headers["idempotency-key"].to_str().expect("key").to_owned(),
                    serde_json::from_slice::<Value>(&body).expect("JSON webhook"),
                ));
                if seen.len() == 1 {
                    StatusCode::SERVICE_UNAVAILABLE
                } else {
                    StatusCode::NO_CONTENT
                }
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|e| Fault::internal(e.to_string()))?;
    let url = format!(
        "http://{}/done",
        listener
            .local_addr()
            .map_err(|e| Fault::internal(e.to_string()))?
    );
    let server = tokio::spawn(async move { axum::serve(listener, app).await });
    let path = database();
    let (east, west) = deployments().await?;
    west.answer(
        StatusCode::OK,
        r#"{"id":"native-hook","status":"completed"}"#,
    );
    {
        let jobs = Jobs::open(&path)?;
        let gateway = gateway(
            east.transport()?.connect(),
            west.transport()?.connect(),
            &jobs,
        )?;
        let body = json!({"background":true,"webhook":url}).to_string();
        send(&gateway, request("POST", "/v1/responses", &body)).await?;
        gateway.idle().await;
    }
    assert_eq!(lock(&received).len(), 1);
    let jobs = Jobs::open(&path)?;
    jobs.deliver_webhooks().await?;
    jobs.deliver_webhooks().await?;
    let seen = lock(&received);
    assert_eq!(
        seen.len(),
        2,
        "successful delivery is marked, failed delivery survives restart"
    );
    assert_eq!(seen[0], seen[1]);
    assert_eq!(seen[1].1["status"], "completed");
    server.abort();
    Ok(())
}
