use super::*;
use std::time::Duration;

use respan_gateway_http::http::StatusCode;
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
fn slow_west(
    east: &ResponsesServer,
    west: &ResponsesServer,
    seen: &Seen,
) -> Result<Gateway, Fault> {
    let jobs = Jobs::default();
    let backend = west.transport()?;
    let slow = respan_gateway_http::connect(move |request| {
        let backend = backend.clone();
        async move {
            tokio::time::sleep(Duration::from_secs(60)).await;
            backend.send(request).await
        }
    });
    Ok(gateway(east.transport()?.connect(), slow, &jobs)?.plugin(seen.clone()))
}

fn get(id: &str) -> http::Request<Body> {
    request("GET", &format!("/v1/responses/{id}"), "")
}

#[tokio::test(start_paused = true)]
async fn a_background_job_outlives_its_client_and_is_polled_later() -> Result<(), Fault> {
    let (east, west) = deployments().await?;
    let seen = Seen::default();
    let done = r#"{"id":"native-1","output":"done"}"#;
    west.answer(StatusCode::OK, done);
    let gateway = slow_west(&east, &west, &seen)?;

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
    let sent = west.requests();
    assert_eq!(sent.len(), 1, "a poll is answered from the store");
    assert_eq!(sent[0].headers["authorization"], "Bearer tenant-key");
    assert_eq!(
        *seen.0.lock().unwrap_or_else(PoisonError::into_inner),
        [true]
    );
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn a_job_that_fails_is_polled_as_failed() -> Result<(), Fault> {
    let (east, west) = deployments().await?;
    let seen = Seen::default();
    west.answer(StatusCode::INTERNAL_SERVER_ERROR, r#"{"error":"boom"}"#);
    let gateway = slow_west(&east, &west, &seen)?;

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
    let (east, west) = deployments().await?;
    let seen = Seen::default();
    let reply = |status| format!(r#"{{"id":"native-77","status":"{status}"}}"#);
    west.answer(StatusCode::OK, &reply("in_progress"));
    west.answer(StatusCode::OK, &reply("completed"));
    let gateway = slow_west(&east, &west, &seen)?;

    let relayed = send(&gateway, request("POST", "/v1/responses", "{}")).await?;
    let id = relayed["id"].as_str().unwrap_or_default();
    assert!(
        id.starts_with("resp_1") && id.len() <= 64,
        "the client sees the signed id: {id}"
    );
    assert_eq!(send(&gateway, get(id)).await?["status"], "completed");

    let sent = west.requests();
    assert_eq!(sent[1].method, Method::GET);
    assert_eq!(sent[1].uri.to_string(), "/v1/responses/native-77");
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn a_forged_id_is_refused_before_any_call() -> Result<(), Fault> {
    let (east, west) = deployments().await?;
    let seen = Seen::default();
    west.answer(StatusCode::OK, r#"{"id":"native-1"}"#);
    let gateway = slow_west(&east, &west, &seen)?;
    let relayed = send(&gateway, request("POST", "/v1/responses", "{}")).await?;
    let id = relayed["id"].as_str().unwrap_or_default();

    // The deployment flipped, and an id never issued.
    let flipped = id.replacen("resp_1", "resp_0", 1);
    for forged in [flipped.as_str(), "resp_1deadbeef"] {
        let (status, ..) = read_whole(serve(&gateway, get(forged)).await.0).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{forged}");
    }
    assert_eq!(west.requests().len(), 1, "no call for a forged id");
    assert!(
        east.requests().is_empty(),
        "no requests to the east deployment"
    );
    Ok(())
}

#[tokio::test]
async fn malformed_unicode_ids_are_refused_before_any_call() -> Result<(), Fault> {
    let (east, west) = deployments().await?;
    let gateway = gateway(
        east.transport()?.connect(),
        west.transport()?.connect(),
        &Jobs::default(),
    )?;
    // The signed-id byte length is valid, but either numeric slice would
    // end inside a multibyte character.
    for forged in [
        format!("resp_é{}", "0".repeat(47)),
        format!("resp_1{}é{}", "0".repeat(15), "0".repeat(31)),
    ] {
        let body = json!({ "previous_response_id": forged });
        let req = request("POST", "/v1/responses", &body.to_string());
        let (status, ..) = read_whole(serve(&gateway, req).await.0).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{forged}");
    }
    assert!(west.requests().is_empty());
    assert!(
        east.requests().is_empty(),
        "no requests to the east deployment"
    );
    Ok(())
}

#[tokio::test]
async fn response_json_split_across_frames_is_mapped_whole() -> Result<(), Fault> {
    use respan_gateway_sdk::futures::StreamExt;

    let (east, west) = deployments().await?;
    west.answer(StatusCode::OK, r#"{"id":"native-1","status":"completed"}"#);
    let west_http = west.transport()?;
    let split = respan_gateway_http::connect(move |call| {
        let west = west_http.clone();
        async move {
            let (response, completion) = west.send(call).await?;
            let response = response.map(|body| {
                respan_gateway_http::Body::stream(
                    body.flat_map(|frame| {
                        let pieces = match frame {
                            Ok(bytes) => bytes
                                .iter()
                                .map(|b| Ok(Bytes::copy_from_slice(&[*b])))
                                .collect(),
                            Err(fault) => vec![Err(fault)],
                        };
                        stream::iter(pieces)
                    })
                    .boxed(),
                )
            });
            Ok((response, completion))
        }
    });
    let jobs = Jobs::default();
    let gateway = gateway(east.transport()?.connect(), split, &jobs)?;
    let body = send(&gateway, request("POST", "/v1/responses", "{}")).await?;
    let id = body["id"].as_str().unwrap_or_default();
    assert!(id.starts_with("resp_"), "{body}");
    assert_eq!(body["status"], "completed");
    assert_eq!(jobs.native(id).await?.as_deref(), Some("native-1"));

    // A bodyless response stays bodyless; buffering must not synthesize
    // an empty JSON event for the mapping filter.
    west.answer(StatusCode::NO_CONTENT, "");
    let req = request("DELETE", &format!("/v1/responses/{id}"), "");
    let (status, _, body) = read_whole(serve(&gateway, req).await.0).await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    assert!(body?.is_empty());
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
    let (east, west) = deployments().await?;
    let seen = Seen::default();
    let done = r#"{"id":"native-1","output":"secret"}"#;
    west.answer(StatusCode::OK, done);
    west.answer(StatusCode::OK, r#"{"id":"native-2"}"#);
    let gateway = slow_west(&east, &west, &seen)?;
    let job = request("POST", "/v1/responses", r#"{"background":true}"#);
    let job = send(&gateway, job).await?;
    let relayed = send(&gateway, request("POST", "/v1/responses", "{}")).await?;
    gateway.idle().await;

    for id in [&job["id"], &relayed["id"]] {
        let id = id.as_str().unwrap_or_default();
        let (status, ..) = read_whole(serve(&gateway, as_other(get(id))).await.0).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{id}");
    }
    assert_eq!(west.requests().len(), 2, "no call for another tenant's id");
    assert!(
        east.requests().is_empty(),
        "no requests to the east deployment"
    );
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn a_zero_retention_tenant_is_refused_background_mode() -> Result<(), Fault> {
    let (east, west) = deployments().await?;
    let seen = Seen::default();
    let gateway = slow_west(&east, &west, &seen)?;
    let mut req = request("POST", "/v1/responses", r#"{"background":true}"#);
    let mut values = respan_gateway_sdk::Extensions::new();
    values.insert(NoContent);
    req.extensions_mut().insert(values);
    let (status, ..) = read_whole(serve(&gateway, req).await.0).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    gateway.idle().await;
    assert!(west.requests().is_empty(), "no job ran");
    assert!(
        east.requests().is_empty(),
        "no requests to the east deployment"
    );
    Ok(())
}

// A poll of a response that was relayed, not a job, shows the signed id too.
#[tokio::test(start_paused = true)]
async fn a_poll_of_a_relayed_response_shows_the_signed_id() -> Result<(), Fault> {
    let (east, west) = deployments().await?;
    let seen = Seen::default();
    let reply = |status| format!(r#"{{"id":"native-77","status":"{status}"}}"#);
    west.answer(StatusCode::OK, &reply("in_progress"));
    west.answer(StatusCode::OK, &reply("completed"));
    let gateway = slow_west(&east, &west, &seen)?;
    let relayed = send(&gateway, request("POST", "/v1/responses", "{}")).await?;
    let id = relayed["id"].as_str().unwrap_or_default();
    assert_ne!(id, "native-77");
    let polled = send(&gateway, get(id)).await?;
    assert_eq!(polled["id"], id);
    assert_eq!(polled["status"], "completed");
    // The provider was asked for its own id.
    assert_eq!(west.requests()[1].uri.path(), "/v1/responses/native-77");
    Ok(())
}

// Tenants are told apart by every credential they send, `x-api-key` included.
#[tokio::test(start_paused = true)]
async fn tenants_with_other_credential_headers_do_not_share_ids() -> Result<(), Fault> {
    let (east, west) = deployments().await?;
    let seen = Seen::default();
    let body = r#"{"id":"native-1","output":"secret"}"#;
    west.answer(StatusCode::OK, body);
    let gateway = slow_west(&east, &west, &seen)?;
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
fn body_of(west: &ResponsesServer, n: usize) -> Value {
    serde_json::from_slice(&west.requests()[n].body).unwrap_or_default()
}

// The job's own run is a plain foreground request: a provider that honours
// `background` would answer `queued`, and the job would never finish.
#[tokio::test(start_paused = true)]
async fn a_jobs_own_run_is_not_a_background_request() -> Result<(), Fault> {
    let (east, west) = deployments().await?;
    let seen = Seen::default();
    let done = r#"{"id":"native-1","status":"completed","output":"done"}"#;
    west.answer(StatusCode::OK, done);
    let gateway = slow_west(&east, &west, &seen)?;
    let post = r#"{"background":true,"input":"hi"}"#;
    let job = send(&gateway, request("POST", "/v1/responses", post)).await?;
    gateway.idle().await;
    let sent = body_of(&west, 0);
    assert_eq!(
        sent,
        json!({ "input": "hi", "max_output_tokens": 1024 }),
        "sent: {sent}"
    );
    let polled = send(&gateway, get(job["id"].as_str().unwrap_or_default())).await?;
    assert_eq!(polled["status"], "completed");
    Ok(())
}

// A turn chained with `previous_response_id` goes to the deployment that
// response lives at, with the provider's id, for a relayed response and for
// a finished job; the id of another tenant's, or a forged one, is a 404.
#[tokio::test(start_paused = true)]
async fn a_previous_response_id_reaches_the_provider_as_its_own() -> Result<(), Fault> {
    let (east, west) = deployments().await?;
    let seen = Seen::default();
    for n in 1..=4 {
        let reply = format!(r#"{{"id":"native-{n}"}}"#);
        west.answer(StatusCode::OK, &reply);
    }
    let gateway = slow_west(&east, &west, &seen)?;
    let first = send(&gateway, request("POST", "/v1/responses", "{}")).await?;
    let chain = |id: &Value| json!({ "previous_response_id": id, "input": "more" }).to_string();
    let second = send(
        &gateway,
        request("POST", "/v1/responses", &chain(&first["id"])),
    )
    .await?;
    assert_eq!(body_of(&west, 1)["previous_response_id"], "native-1");
    assert_eq!(body_of(&west, 1)["input"], "more");
    assert_ne!(second["id"], "native-2", "the client sees a signed id");

    // A finished job can be chained on too, after its provider id is known.
    let job = request("POST", "/v1/responses", r#"{"background":true}"#);
    let job = send(&gateway, job).await?;
    gateway.idle().await;
    let third = request("POST", "/v1/responses", &chain(&job["id"]));
    send(&gateway, third).await?;
    assert_eq!(body_of(&west, 3)["previous_response_id"], "native-3");

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
    assert_eq!(west.requests().len(), 4, "no call for a refused id");
    assert!(
        east.requests().is_empty(),
        "no requests to the east deployment"
    );
    Ok(())
}

// `/cancel`, `/input_items` and `DELETE` of a signed id reach the provider
// with its own id; the answer says the signed one.
#[tokio::test(start_paused = true)]
async fn the_paths_under_a_signed_id_reach_the_provider_with_its_own() -> Result<(), Fault> {
    let (east, west) = deployments().await?;
    let seen = Seen::default();
    west.answer(StatusCode::OK, r#"{"id":"native-9"}"#);
    for reply in [
        r#"{"id":"native-9","status":"cancelled"}"#,
        r#"{"object":"list","data":[]}"#,
        r#"{"id":"native-9","deleted":true}"#,
    ] {
        west.answer(StatusCode::OK, reply);
    }
    let gateway = slow_west(&east, &west, &seen)?;
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

    let sent = west.requests();
    let seen: Vec<(String, String)> = sent[1..]
        .iter()
        .map(|r| (r.method.to_string(), r.uri.to_string()))
        .collect();
    let west_path = str::to_owned;
    assert_eq!(
        seen,
        [
            ("POST".into(), west_path("/v1/responses/native-9/cancel")),
            (
                "GET".into(),
                west_path("/v1/responses/native-9/input_items?limit=5")
            ),
            ("DELETE".into(), west_path("/v1/responses/native-9")),
        ]
    );

    // Another tenant's id is refused; a running job can be cancelled before its provider id arrives.
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
    let (status, _, body) = read_whole(serve(&gateway, request("POST", &cancel, "")).await.0).await;
    assert_eq!(status, StatusCode::OK);
    let body: Value = serde_json::from_slice(&body?).map_err(|e| Fault::internal(e.to_string()))?;
    assert_eq!(body["status"], "cancelled");
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
        let (east, west) = deployments().await?;
        let done = r#"{"id":"native-1","output":"done"}"#;
        west.answer(StatusCode::OK, done);
        let jobs = Jobs::default();
        let gateway = gateway(
            east.transport()?.connect(),
            west.transport()?.connect(),
            &jobs,
        )?;
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

/// A streamed request is refused, clearly, before any call: on a post, on a
/// background post, and on a poll.
#[tokio::test(start_paused = true)]
async fn a_streamed_request_is_refused_before_any_call() -> Result<(), Fault> {
    let (east, west) = deployments().await?;
    let gateway = gateway(
        east.transport()?.connect(),
        west.transport()?.connect(),
        &Jobs::default(),
    )?;
    let requests = [
        request("POST", "/v1/responses", r#"{"stream":true,"input":"hi"}"#),
        request(
            "POST",
            "/v1/responses",
            r#"{"stream":true,"background":true}"#,
        ),
        request("GET", "/v1/responses/resp_0?stream=true", ""),
    ];
    for req in requests {
        let (status, _, body) = read_whole(serve(&gateway, req).await.0).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body:?}");
        let body = String::from_utf8_lossy(&body.unwrap_or_default()).into_owned();
        assert!(body.contains("streaming is not supported"), "{body}");
    }
    assert!(west.requests().is_empty());
    assert!(
        east.requests().is_empty(),
        "no requests to the east deployment"
    );
    Ok(())
}

/// A chained response echoes its `previous_response_id`: the client sees
/// the signed id it sent, never the provider's.
#[tokio::test(start_paused = true)]
async fn an_echoed_previous_response_id_is_signed_too() -> Result<(), Fault> {
    let (east, west) = deployments().await?;
    west.answer(StatusCode::OK, r#"{"id":"native-1"}"#);
    west.answer(
        StatusCode::OK,
        r#"{"id":"native-2","previous_response_id":"native-1"}"#,
    );
    let gateway = gateway(
        east.transport()?.connect(),
        west.transport()?.connect(),
        &Jobs::default(),
    )?;
    let first = send(&gateway, request("POST", "/v1/responses", "{}")).await?;
    let chained = json!({ "previous_response_id": first["id"], "input": "more" }).to_string();
    let second = send(&gateway, request("POST", "/v1/responses", &chained)).await?;
    assert_ne!(second["id"], "native-2");
    assert_eq!(second["previous_response_id"], first["id"]);
    Ok(())
}

/// An endpoint under the prefix that is not an id passes through to the
/// default deployment.
#[tokio::test(start_paused = true)]
async fn an_endpoint_under_the_prefix_that_is_not_an_id_passes_through() -> Result<(), Fault> {
    let (east, west) = deployments().await?;
    let counted = r#"{"object":"response.input_tokens","input_tokens":3}"#;
    west.answer(StatusCode::OK, counted);
    let gateway = gateway(
        east.transport()?.connect(),
        west.transport()?.connect(),
        &Jobs::default(),
    )?;
    let req = request("POST", "/v1/responses/input_tokens", r#"{"input":"hi"}"#);
    let answer = send(&gateway, req).await?;
    assert_eq!(answer["input_tokens"], 3);
    assert_eq!(west.requests().len(), 1);
    assert!(
        east.requests().is_empty(),
        "no requests to the east deployment"
    );
    Ok(())
}

/// Whatever else lies under the prefix (`compact`) is the provider's too.
#[tokio::test(start_paused = true)]
async fn any_path_under_the_prefix_that_is_not_ours_passes_through() -> Result<(), Fault> {
    let (east, west) = deployments().await?;
    west.answer(StatusCode::OK, r#"{"object":"response.compaction"}"#);
    let gateway = gateway(
        east.transport()?.connect(),
        west.transport()?.connect(),
        &Jobs::default(),
    )?;
    let req = request("POST", "/v1/responses/compact", r#"{"input":"hi"}"#);
    let answer = send(&gateway, req).await?;
    assert_eq!(answer["object"], "response.compaction");
    assert_eq!(west.requests()[0].uri.path(), "/v1/responses/compact");
    Ok(())
}

/// Two tenants whose answers carry the same provider id: the id a chained
/// answer echoes is mapped back to its own tenant's signed id, never the
/// other's.
#[tokio::test(start_paused = true)]
async fn an_echoed_id_is_mapped_back_within_its_own_tenant() -> Result<(), Fault> {
    let as_tenant = |key: &str, body: &str| {
        http::Request::builder()
            .method("POST")
            .uri("/v1/responses")
            .header("authorization", format!("Bearer {key}"))
            .body(respan_gateway_http::Body::full(body.to_owned()))
            .expect("a request")
    };
    let (east, west) = deployments().await?;
    let same = r#"{"id":"native-1"}"#;
    let echo = r#"{"id":"native-2","previous_response_id":"native-1"}"#;
    for body in [same, same, echo] {
        west.answer(StatusCode::OK, body);
    }
    let gateway = gateway(
        east.transport()?.connect(),
        west.transport()?.connect(),
        &Jobs::default(),
    )?;
    let _a = send(&gateway, as_tenant("tenant-a", "{}")).await?;
    let b = send(&gateway, as_tenant("tenant-b", "{}")).await?;
    let chained = json!({ "previous_response_id": b["id"] }).to_string();
    let next = send(&gateway, as_tenant("tenant-b", &chained)).await?;
    assert_eq!(next["previous_response_id"], b["id"]);
    Ok(())
}
