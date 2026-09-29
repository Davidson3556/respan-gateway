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

// A poll of a response that was relayed, not a job, shows the signed id too.
#[tokio::test(start_paused = true)]
async fn a_poll_of_a_relayed_response_shows_the_signed_id() -> Result<(), Fault> {
    let (mock, seen) = (MockTransport::new(), Seen::default());
    let reply = |status| format!(r#"{{"id":"native-77","status":"{status}"}}"#);
    mock.reply(WEST, mock::json(StatusCode::OK, &reply("in_progress")));
    mock.reply(WEST, mock::json(StatusCode::OK, &reply("completed")));
    let gateway = slow_west(&mock, &seen)?;
    let relayed = send(&gateway, request("POST", "/v1/responses", "{}")).await?;
    let id = relayed["id"].as_str().unwrap_or_default();
    assert_ne!(id, "native-77");
    let polled = send(&gateway, get(id)).await?;
    assert_eq!(polled["id"], id);
    assert_eq!(polled["status"], "completed");
    // The provider was asked for its own id.
    assert_eq!(mock.sent()[1].uri().path(), "/v1/responses/native-77");
    Ok(())
}

// Tenants are told apart by every credential they send, `x-api-key` included.
#[tokio::test(start_paused = true)]
async fn tenants_with_other_credential_headers_do_not_share_ids() -> Result<(), Fault> {
    let (mock, seen) = (MockTransport::new(), Seen::default());
    let body = r#"{"id":"native-1","output":"secret"}"#;
    mock.reply(WEST, mock::json(StatusCode::OK, body));
    let gateway = slow_west(&mock, &seen)?;
    let with_key = |key: &str, mut req: http::Request<Body>| {
        req.headers_mut().remove("authorization");
        req.headers_mut()
            .insert("x-api-key", key.parse().expect("a value"));
        req
    };
    let post = request("POST", "/v1/responses", r#"{"background":true}"#);
    let job = send(&gateway, with_key("alice", post)).await?;
    gateway.idle().await;
    let id = job["id"].as_str().unwrap_or_default();
    let mallory = serve(&gateway, with_key("mallory", get(id))).await.0;
    assert_eq!(read_whole(mallory).await.0, StatusCode::NOT_FOUND);
    let alice = send(&gateway, with_key("alice", get(id))).await?;
    assert_eq!(alice["output"], "secret");
    Ok(())
}

/// The body of the `n`th request that reached a provider.
fn body_of(mock: &MockTransport, n: usize) -> Value {
    serde_json::from_slice(mock.sent()[n].body()).unwrap_or_default()
}

// The job's own run is a plain foreground request: a provider that honours
// `background` would answer `queued`, and the job would never finish.
#[tokio::test(start_paused = true)]
async fn a_jobs_own_run_is_not_a_background_request() -> Result<(), Fault> {
    let (mock, seen) = (MockTransport::new(), Seen::default());
    let done = r#"{"id":"native-1","status":"completed","output":"done"}"#;
    mock.reply(WEST, mock::json(StatusCode::OK, done));
    let gateway = slow_west(&mock, &seen)?;
    let post = r#"{"background":true,"input":"hi"}"#;
    let job = send(&gateway, request("POST", "/v1/responses", post)).await?;
    gateway.idle().await;
    let sent = body_of(&mock, 0);
    assert_eq!(sent, json!({ "input": "hi" }), "sent: {sent}");
    let polled = send(&gateway, get(job["id"].as_str().unwrap_or_default())).await?;
    assert_eq!(polled["status"], "completed");
    Ok(())
}

// A turn chained with `previous_response_id` goes to the deployment that
// response lives at, with the provider's id, for a relayed response and for
// a finished job; the id of another tenant's, or a forged one, is a 404.
#[tokio::test(start_paused = true)]
async fn a_previous_response_id_reaches_the_provider_as_its_own() -> Result<(), Fault> {
    let (mock, seen) = (MockTransport::new(), Seen::default());
    for n in 1..=4 {
        let reply = format!(r#"{{"id":"native-{n}"}}"#);
        mock.reply(WEST, mock::json(StatusCode::OK, &reply));
    }
    let gateway = slow_west(&mock, &seen)?;
    let first = send(&gateway, request("POST", "/v1/responses", "{}")).await?;
    let chain = |id: &Value| json!({ "previous_response_id": id, "input": "more" }).to_string();
    let second = send(
        &gateway,
        request("POST", "/v1/responses", &chain(&first["id"])),
    )
    .await?;
    assert_eq!(body_of(&mock, 1)["previous_response_id"], "native-1");
    assert_eq!(body_of(&mock, 1)["input"], "more");
    assert_ne!(second["id"], "native-2", "the client sees a signed id");

    // A finished job can be chained on too, after its provider id is known.
    let job = request("POST", "/v1/responses", r#"{"background":true}"#);
    let job = send(&gateway, job).await?;
    gateway.idle().await;
    let third = request("POST", "/v1/responses", &chain(&job["id"]));
    send(&gateway, third).await?;
    assert_eq!(body_of(&mock, 3)["previous_response_id"], "native-3");

    for forged in [json!("resp_1deadbeef"), first["id"].clone()] {
        let req = request("POST", "/v1/responses", &chain(&forged));
        let req = if forged == first["id"] {
            as_other(req)
        } else {
            req
        };
        let (status, ..) = read_whole(serve(&gateway, req).await.0).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{forged}");
    }
    assert_eq!(mock.sent().len(), 4, "no call for a refused id");
    Ok(())
}

// `/cancel`, `/input_items` and `DELETE` of a signed id reach the provider
// with its own id; the answer says the signed one.
#[tokio::test(start_paused = true)]
async fn the_paths_under_a_signed_id_reach_the_provider_with_its_own() -> Result<(), Fault> {
    let (mock, seen) = (MockTransport::new(), Seen::default());
    mock.reply(WEST, mock::json(StatusCode::OK, r#"{"id":"native-9"}"#));
    for reply in [
        r#"{"id":"native-9","status":"cancelled"}"#,
        r#"{"object":"list","data":[]}"#,
        r#"{"id":"native-9","deleted":true}"#,
    ] {
        mock.reply(WEST, mock::json(StatusCode::OK, reply));
    }
    let gateway = slow_west(&mock, &seen)?;
    let first = send(&gateway, request("POST", "/v1/responses", "{}")).await?;
    let id = first["id"].as_str().unwrap_or_default();

    let cancel = format!("/v1/responses/{id}/cancel");
    let cancelled = send(&gateway, request("POST", &cancel, "")).await?;
    assert_eq!(cancelled["id"], first["id"]);
    assert_eq!(cancelled["status"], "cancelled");
    let items = format!("/v1/responses/{id}/input_items?limit=5");
    let listed = send(&gateway, request("GET", &items, "")).await?;
    assert_eq!(listed["object"], "list");
    let deleted = send(
        &gateway,
        request("DELETE", &format!("/v1/responses/{id}"), ""),
    )
    .await?;
    assert_eq!(deleted["id"], first["id"]);

    let sent = mock.sent();
    let seen: Vec<(String, String)> = sent[1..]
        .iter()
        .map(|r| (r.method().to_string(), r.uri().to_string()))
        .collect();
    let west = |path: &str| format!("http://west.test{path}");
    assert_eq!(
        seen,
        [
            ("POST".into(), west("/v1/responses/native-9/cancel")),
            (
                "GET".into(),
                west("/v1/responses/native-9/input_items?limit=5")
            ),
            ("DELETE".into(), west("/v1/responses/native-9")),
        ]
    );

    // Another tenant's, and a job still running (no provider id yet), are 404.
    let (status, ..) = read_whole(
        serve(&gateway, as_other(request("POST", &cancel, "")))
            .await
            .0,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let job = request("POST", "/v1/responses", r#"{"background":true}"#);
    let job = send(&gateway, job).await?;
    let cancel = format!(
        "/v1/responses/{}/cancel",
        job["id"].as_str().unwrap_or_default()
    );
    let (status, ..) = read_whole(serve(&gateway, request("POST", &cancel, "")).await.0).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    Ok(())
}

/// `idle` after a background POST: the job's answer must be pollable, as
/// the example's `main` and doc promise. On a multi-threaded runtime the
/// spawned task that calls `serve` may not have run when the POST's
/// request ends.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn l5_idle_waits_for_a_background_job_on_a_multi_threaded_runtime() -> Result<(), Fault> {
    let mut early = 0;
    for _ in 0..200 {
        let mock = MockTransport::new();
        let done = r#"{"id":"native-1","output":"done"}"#;
        mock.reply(WEST, mock::json(StatusCode::OK, done));
        let jobs = Jobs::default();
        let gateway = gateway(mock.at(EAST), mock.at(WEST), &jobs)?;
        let queued = send(
            &gateway,
            request("POST", "/v1/responses", r#"{"background":true}"#),
        )
        .await?;
        gateway.idle().await;
        let polled = send(&gateway, get(queued["id"].as_str().unwrap_or_default())).await?;
        if polled["output"] != "done" {
            early += 1;
        }
    }
    assert_eq!(
        early, 0,
        "idle resolved before the job was done in {early}/200"
    );
    Ok(())
}
