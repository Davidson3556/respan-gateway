use super::*;

fn auth(req: &mockllm::Request) -> Option<&str> {
    req.header("authorization")
}

async fn providers() -> Result<(FakeOpenAi, FakeOpenAi, HttpClient, HttpClient), Fault> {
    let (openai, azure) = (FakeOpenAi::start().await, FakeOpenAi::start().await);
    let first = HttpClient::new(openai.base_url())?;
    let second = HttpClient::new(azure.base_url())?;
    Ok((openai, azure, first, second))
}

#[tokio::test]
async fn each_tenant_gets_its_target_and_key_and_edits_apply_live() -> Result<(), Fault> {
    let (openai, azure, first, second) = providers().await?;
    for _ in 0..2 {
        openai.answer(Answer::raw(OK));
    }
    azure.answer(Answer::raw(OK));
    let tenants = Tenants::default();
    let gw = gateway(first.connect(), second.connect(), tenants.clone())?;
    assert!(tenants.upsert("gw-acme", tenant(1, "openai", "sk-acme")?));
    assert!(tenants.upsert("gw-beta", tenant(1, "openai", "sk-beta")?));
    assert_eq!(send(&gw, "gw-acme").await?, StatusCode::OK);
    assert_eq!(send(&gw, "gw-beta").await?, StatusCode::OK);

    assert!(tenants.upsert("gw-acme", tenant(3, "azure", "az-acme")?));
    // A stale edit arriving late changes nothing.
    assert!(!tenants.upsert("gw-acme", tenant(2, "openai", "sk-old")?));
    assert_eq!(send(&gw, "gw-acme").await?, StatusCode::OK);

    let (openai, azure) = (openai.requests(), azure.requests());
    assert_eq!((openai.len(), azure.len()), (2, 1));
    assert_eq!(auth(&openai[0]), Some("Bearer sk-acme"));
    assert_eq!(auth(&openai[1]), Some("Bearer sk-beta"));
    assert_eq!(auth(&azure[0]), Some("Bearer az-acme"));
    assert!(
        openai
            .iter()
            .chain(azure.iter())
            .all(|r| r.method == "POST" && r.path == "/v1/chat/completions")
    );
    Ok(())
}

/// Edits the tenant mid-request, as the control plane may at any time.
struct EditsMidway(Tenants);

#[async_trait]
impl Plugin for EditsMidway {
    async fn on_client(&self, _cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        self.0.upsert("gw-acme", tenant(2, "azure", "az-acme")?);
        Ok(None)
    }
}

#[tokio::test]
async fn a_request_keeps_the_version_it_started_with() -> Result<(), Fault> {
    let (openai, azure, first, second) = providers().await?;
    openai.answer(Answer::raw(OK));
    let tenants = Tenants::default();
    tenants.upsert("gw-acme", tenant(1, "openai", "sk-acme")?);
    let gw = gateway(first.connect(), second.connect(), tenants.clone())?
        .plugin("edits", EditsMidway(tenants.clone()))?;
    assert_eq!(send(&gw, "gw-acme").await?, StatusCode::OK);
    assert_eq!(openai.requests().len(), 1);
    assert_eq!(auth(&openai.requests()[0]), Some("Bearer sk-acme"));
    assert!(azure.requests().is_empty());
    assert_eq!(tenants.get("gw-acme").map(|t| t.version), Some(2));
    Ok(())
}

#[tokio::test]
async fn unknown_keys_and_providers_without_a_key_are_refused() -> Result<(), Fault> {
    let (openai, azure, first, second) = providers().await?;
    let tenants = Tenants::default();
    let gw = gateway(first.connect(), second.connect(), tenants.clone())?;
    assert_eq!(send(&gw, "gw-nobody").await?, StatusCode::UNAUTHORIZED);
    let mut keyless = tenant(1, "openai", "sk")?;
    keyless.keys.clear();
    tenants.upsert("gw-keyless", keyless);
    assert_eq!(send(&gw, "gw-keyless").await?, StatusCode::FORBIDDEN);
    assert!(openai.requests().is_empty());
    assert!(azure.requests().is_empty());
    Ok(())
}
