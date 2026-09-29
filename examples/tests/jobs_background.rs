use super::*;
use std::time::Duration;

use respan_gateway_http::http::StatusCode;
use respan_gateway_http::{Exchange, RequestHead, ResponseHead};

/// Answers after `.1`: a provider that takes its time.
struct Slow<T>(T, Duration);

#[async_trait]
impl<T: Transport> Transport for Slow<T> {
    async fn send(&self, call: Exchange<RequestHead>) -> Result<Exchange<ResponseHead>, Fault> {
        tokio::time::sleep(self.1).await;
        self.0.send(call).await
    }
}

/// Notes, for each request that reaches it, whether it has a credential.
/// Registered after `jobs`, it sees only what `jobs` lets through.
#[derive(Clone, Default)]
struct Seen(Arc<Mutex<Vec<bool>>>);

#[async_trait]
impl Plugin for Seen {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let credential = cx.extensions().get::<Credential>().is_some();
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(credential);
        Ok(None)
    }
}

/// A gateway whose west provider takes a minute, with `Seen` after `jobs`.
fn slow_west(mock: &MockTransport, seen: &Seen) -> Result<Gateway, Fault> {
    let jobs = Jobs::default();
    let west = Slow(mock.at(WEST), Duration::from_secs(60));
    gateway(mock.at(EAST), west, &jobs)?.plugin("seen", seen.clone())
}

fn get(id: &str) -> http::Request<Body> {
    request("GET", &format!("/v1/responses/{id}"), "")
}

#[tokio::test(start_paused = true)]
async fn a_background_job_outlives_its_client_and_is_polled_later() -> Result<(), Fault> {
    let (mock, seen) = (MockTransport::new(), Seen::default());
    let done = r#"{"id":"native-1","output":"done"}"#;
    mock.reply(WEST, mock::json(StatusCode::OK, done));
    let gateway = slow_west(&mock, &seen)?;

    let queued = send(
        &gateway,
        request("POST", "/v1/responses", r#"{"background":true}"#),
    )
    .await?;
    let id = queued["id"].as_str().unwrap_or_default();
    assert_eq!(queued["status"], "queued");
    assert!(id.starts_with("resp_1") && id.len() <= 64, "{id}");

    // The client is gone; the provider has not answered yet.
    assert_eq!(send(&gateway, get(id)).await?["status"], "in_progress");
    gateway.idle().await;
    assert_eq!(send(&gateway, get(id)).await?["output"], "done");

    // The client sees the signed id, never the provider's.
    assert_eq!(send(&gateway, get(id)).await?["id"], id);

    // One call, with the tenant's key, and from the re-entered run only
    // (the job's body says `background`, and is not started as a job again):
    // the later plugin never saw the client's request, `jobs` answered it.
    let sent = mock.sent();
    assert_eq!(sent.len(), 1, "a poll is answered from the store");
    assert_eq!(sent[0].headers()["authorization"], "Bearer tenant-key");
    assert_eq!(
        *seen.0.lock().unwrap_or_else(PoisonError::into_inner),
        [true]
    );
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn a_job_that_fails_is_polled_as_failed() -> Result<(), Fault> {
    let (mock, seen) = (MockTransport::new(), Seen::default());
    mock.reply(
        WEST,
        mock::json(StatusCode::INTERNAL_SERVER_ERROR, r#"{"error":"boom"}"#),
    );
    let gateway = slow_west(&mock, &seen)?;

    let queued = send(
        &gateway,
        request("POST", "/v1/responses", r#"{"background":true}"#),
    )
    .await?;
    gateway.idle().await;

    let polled = send(&gateway, get(queued["id"].as_str().unwrap_or_default())).await?;
    assert_eq!(polled["status"], "failed", "{polled}");
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn a_signed_id_routes_a_later_get_to_its_deployment_with_the_native_id() -> Result<(), Fault>
{
    let (mock, seen) = (MockTransport::new(), Seen::default());
    let reply = |status| format!(r#"{{"id":"native-77","status":"{status}"}}"#);
    mock.reply(WEST, mock::json(StatusCode::OK, &reply("in_progress")));
    mock.reply(WEST, mock::json(StatusCode::OK, &reply("completed")));
    let gateway = slow_west(&mock, &seen)?;

    let relayed = send(&gateway, request("POST", "/v1/responses", "{}")).await?;
    let id = relayed["id"].as_str().unwrap_or_default();
    assert!(
        id.starts_with("resp_1") && id.len() <= 64,
        "the client sees the signed id: {id}"
    );
    assert_eq!(send(&gateway, get(id)).await?["status"], "completed");

    let sent = mock.sent();
    assert_eq!(sent[1].method(), Method::GET);
    assert_eq!(
        sent[1].uri().to_string(),
        "http://west.test/v1/responses/native-77"
    );
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn a_forged_id_is_refused_before_any_call() -> Result<(), Fault> {
    let (mock, seen) = (MockTransport::new(), Seen::default());
    mock.reply(WEST, mock::json(StatusCode::OK, r#"{"id":"native-1"}"#));
    let gateway = slow_west(&mock, &seen)?;
    let relayed = send(&gateway, request("POST", "/v1/responses", "{}")).await?;
    let id = relayed["id"].as_str().unwrap_or_default();

    // The deployment flipped, and an id never issued.
    let flipped = id.replacen("resp_1", "resp_0", 1);
    for forged in [flipped.as_str(), "resp_1deadbeef"] {
        let (status, ..) = read_whole(serve(&gateway, get(forged)).await.0).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{forged}");
    }
    assert_eq!(mock.sent().len(), 1, "no call for a forged id");
    Ok(())
}

/// The same request, from a tenant with another key.
fn as_other(mut req: http::Request<Body>) -> http::Request<Body> {
    let key = "Bearer other-key".parse().expect("a header value");
    req.headers_mut().insert("authorization", key);
    req
}

#[tokio::test(start_paused = true)]
async fn an_id_is_only_good_for_the_tenant_it_was_issued_to() -> Result<(), Fault> {
    let (mock, seen) = (MockTransport::new(), Seen::default());
    let done = r#"{"id":"native-1","output":"secret"}"#;
    mock.reply(WEST, mock::json(StatusCode::OK, done));
    mock.reply(WEST, mock::json(StatusCode::OK, r#"{"id":"native-2"}"#));
    let gateway = slow_west(&mock, &seen)?;
    let job = request("POST", "/v1/responses", r#"{"background":true}"#);
    let job = send(&gateway, job).await?;
    let relayed = send(&gateway, request("POST", "/v1/responses", "{}")).await?;
    gateway.idle().await;

    for id in [&job["id"], &relayed["id"]] {
        let id = id.as_str().unwrap_or_default();
        let (status, ..) = read_whole(serve(&gateway, as_other(get(id))).await.0).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{id}");
    }
    assert_eq!(mock.sent().len(), 2, "no call for another tenant's id");
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn a_zero_retention_tenant_is_refused_background_mode() -> Result<(), Fault> {
    let (mock, seen) = (MockTransport::new(), Seen::default());
    let gateway = slow_west(&mock, &seen)?;
    let mut req = request("POST", "/v1/responses", r#"{"background":true}"#);
    let mut values = respan_gateway_core::Extensions::new();
    values.insert(NoContent);
    req.extensions_mut().insert(values);
    let (status, ..) = read_whole(serve(&gateway, req).await.0).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    gateway.idle().await;
    assert!(mock.sent().is_empty(), "no job ran");
    Ok(())
}
