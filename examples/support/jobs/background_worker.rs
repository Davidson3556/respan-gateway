//! Durable job execution and completion. The database owns claims, budget
//! holds and the webhook outbox; task handles only speed up local cancellation.

use super::*;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

impl Jobs {
    /// Run queued jobs and recover expired worker leases after a restart.
    /// The same signed id is the provider's Idempotency-Key on every retry.
    /// Exactly-once remote execution requires the provider to honor that key.
    pub(super) async fn resume(&self, gateway: &Gateway) -> Result<(), Fault> {
        let ids: Vec<String> = self
            .db(|tx| {
                let mut rows = tx
                    .prepare("SELECT id FROM background_jobs WHERE result IS NULL AND lease<=?1")
                    .map_err(sql)?;
                rows.query_map([now()], |row| row.get(0))
                    .map_err(sql)?
                    .collect::<Result<_, _>>()
                    .map_err(sql)
            })
            .await?;
        for id in ids {
            self.run(gateway, &id).await?;
        }
        self.deliver_webhooks().await
    }

    pub(super) async fn run(&self, gateway: &Gateway, id: &str) -> Result<(), Fault> {
        let mut token = [0u8; 16];
        getrandom::fill(&mut token).map_err(|e| Fault::internal(e.to_string()))?;
        let owner = hex(&token);
        let (saved_id, saved_owner) = (id.to_owned(), owner.clone());
        let request: Option<(String, String, String)> = self.db(move |tx| {
            let claimed = tx.execute("UPDATE background_jobs SET owner=?1,lease=?2 WHERE id=?3 AND result IS NULL AND lease<=?4",
                (&saved_owner, now()+30, &saved_id, now())).map_err(sql)?;
            if claimed == 0 { return Ok(None); }
            tx.query_row("SELECT request,uri,headers FROM background_jobs WHERE id=?1", [&saved_id],
                |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?))).map(Some).map_err(sql)
        }).await?;
        let Some((body, uri, headers)) = request else {
            return Ok(());
        };
        let mut req = http::Request::post(uri)
            .body(respan_gateway_http::Body::full(body))
            .map_err(built_wrong)?;
        let headers: Vec<(String, Vec<u8>)> =
            serde_json::from_str(&headers).map_err(|e| Fault::internal(e.to_string()))?;
        for (name, value) in headers {
            let name =
                http::HeaderName::from_bytes(name.as_bytes()).map_err(|e| built_wrong(e.into()))?;
            let value = http::HeaderValue::from_bytes(&value).map_err(|e| built_wrong(e.into()))?;
            req.headers_mut().append(name, value);
        }
        req.headers_mut().insert(
            "idempotency-key",
            http::HeaderValue::from_str(id).map_err(|e| built_wrong(e.into()))?,
        );
        let mut extensions = Extensions::new();
        extensions.insert(Job {
            id: id.to_owned(),
            owner: owner.clone(),
            output: Arc::default(),
        });
        req.extensions_mut().insert(extensions);
        // Register with gateway.idle before the spawning request can finish.
        let served = respan_gateway_http::serve(gateway, req);
        let (jobs, id) = (self.clone(), id.to_owned());
        let active_id = id.clone();
        let (start, started) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let _ = started.await;
            let work = async {
                let (response, ended) = served.await;
                let _ = response.into_body().collect().await;
                let _ = ended.await;
            };
            tokio::pin!(work);
            let mut deadline = tokio::time::Instant::now() + Duration::from_secs(30);
            loop {
                tokio::select! {
                    biased;
                    _ = tokio::time::sleep_until(deadline) => break,
                    _ = &mut work => break,
                    _ = tokio::time::sleep(Duration::from_secs(10)) => {
                        let (id, owner) = (id.clone(), owner.clone());
                        let renewed = tokio::time::timeout_at(deadline, jobs.db(move |tx| {
                            tx.execute("UPDATE background_jobs SET lease=?1 WHERE id=?2 AND owner=?3 AND result IS NULL AND lease>?4", (now()+30,id,owner,now())).map_err(sql)
                        })).await;
                        if !matches!(renewed, Ok(Ok(1))) { break; }
                        deadline = tokio::time::Instant::now() + Duration::from_secs(30);
                    }
                }
            }
        });
        lock(&self.0.active).insert(active_id, task.abort_handle());
        let _ = start.send(());
        Ok(())
    }

    /// Complete once, atomically releasing the hold, charging reported usage
    /// (the reservation when unknown), and enqueueing the finish webhook.
    pub(super) async fn finish(
        &self,
        id: &str,
        owner: Option<&str>,
        body: Bytes,
    ) -> Result<(), Fault> {
        let (id, owner) = (id.to_owned(), owner.map(str::to_owned));
        self.db(move |tx| {
            let job: Option<(String,i64,String,Option<String>,i64)> = tx.query_row(
                "SELECT tenant,reserve,owner,webhook,lease FROM background_jobs WHERE id=?1 AND result IS NULL", [&id],
                |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?))).optional().map_err(sql)?;
            let Some((tenant,reserve,held_by,webhook,lease)) = job else { return Ok(()); };
            if owner.as_ref().is_some_and(|owner| *owner != held_by || lease <= now()) { return Ok(()); }
            let reply: Value = serde_json::from_slice(&body).unwrap_or_default();
            let usage = &reply["usage"];
            let reported = usage["total_tokens"].as_i64().or_else(|| Some(usage["input_tokens"].as_i64()?.saturating_add(usage["output_tokens"].as_i64()?)));
            let spent = if held_by.is_empty() { 0 } else { reported.unwrap_or(reserve).max(0) };
            let (held, charged): (i64,i64) = tx.query_row("SELECT held,spent FROM job_accounts WHERE tenant=?1", [&tenant], |r| Ok((r.get(0)?,r.get(1)?))).map_err(sql)?;
            tx.execute("UPDATE job_accounts SET held=?1,spent=?2 WHERE tenant=?3", (held.saturating_sub(reserve), charged.saturating_add(spent),tenant)).map_err(sql)?;
            tx.execute("UPDATE background_jobs SET result=?1,lease=0 WHERE id=?2", (body.as_ref(),&id)).map_err(sql)?;
            if let Some(url) = webhook {
                tx.execute("INSERT OR IGNORE INTO job_webhooks(id,url,body) VALUES(?1,?2,?3)", (&id,url,body.as_ref())).map_err(sql)?;
            }
            Ok(())
        }).await
    }

    /// Retry undelivered finish notices, including notices left by a crash.
    /// Delivery is at least once; the job id is the receiver's deduplication key.
    pub(super) async fn deliver_webhooks(&self) -> Result<(), Fault> {
        let notices: Vec<(String, String, Vec<u8>)> = self
            .db(|tx| {
                let mut rows = tx
                    .prepare("SELECT id,url,body FROM job_webhooks WHERE delivered=0")
                    .map_err(sql)?;
                rows.query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
                    .map_err(sql)?
                    .collect::<Result<_, _>>()
                    .map_err(sql)
            })
            .await?;
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(3))
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|e| Fault::upstream(e.to_string()))?;
        for (id, url, body) in notices {
            let sent = client
                .post(url)
                .header("idempotency-key", &id)
                .header("content-type", "application/json")
                .body(body)
                .send()
                .await;
            if sent.is_ok_and(|response| response.status().is_success()) {
                self.db(move |tx| {
                    tx.execute("UPDATE job_webhooks SET delivered=1 WHERE id=?1", [id])
                        .map_err(sql)?;
                    Ok(())
                })
                .await?;
            }
        }
        Ok(())
    }

    /// Set a tenant's durable token budget; existing charges and holds remain.
    pub(super) async fn budget(&self, tenant: [u8; 32], amount: i64) -> Result<(), Fault> {
        self.db(move |tx| {
            tx.execute("INSERT INTO job_accounts(tenant,balance) VALUES(?1,?2) ON CONFLICT(tenant) DO UPDATE SET balance=excluded.balance", (hex(&tenant),amount)).map_err(sql)?;
            Ok(())
        }).await
    }
}
