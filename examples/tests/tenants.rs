use super::*;

fn auth(req: &http::Request<respan_gateway_core::bytes::Bytes>) -> Option<&str> {
    req.headers()
        .get(AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
}

#[tokio::test]
async fn each_tenant_gets_its_target_and_key_and_edits_apply_live() -> Result<(), Fault> {
    let mock = MockTransport::new();
    for base in [OPENAI, OPENAI, AZURE] {
        mock.reply(base, mock::json(StatusCode::OK, OK));
    }
    let tenants = Tenants::default();
    let gw = gateway(&mock, tenants.clone())?;
    assert!(tenants.upsert("gw-acme", tenant(1, "openai", "sk-acme")?));
    assert!(tenants.upsert("gw-beta", tenant(1, "openai", "sk-beta")?));
    assert_eq!(send(&gw, "gw-acme").await?, StatusCode::OK);
    assert_eq!(send(&gw, "gw-beta").await?, StatusCode::OK);

    assert!(tenants.upsert("gw-acme", tenant(3, "azure", "az-acme")?));
    // A stale edit arriving late changes nothing.
    assert!(!tenants.upsert("gw-acme", tenant(2, "openai", "sk-old")?));
    assert_eq!(send(&gw, "gw-acme").await?, StatusCode::OK);

    let sent = mock.sent();
    let seen: Vec<_> = sent.iter().map(|r| (r.uri().host(), auth(r))).collect();
    assert_eq!(
        seen,
        [
            (Some("openai.test"), Some("Bearer sk-acme")),
            (Some("openai.test"), Some("Bearer sk-beta")),
            (Some("azure.test"), Some("Bearer az-acme")),
        ]
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
    let mock = MockTransport::new();
    mock.reply(OPENAI, mock::json(StatusCode::OK, OK));
    let tenants = Tenants::default();
    tenants.upsert("gw-acme", tenant(1, "openai", "sk-acme")?);
    let gw = gateway(&mock, tenants.clone())?.plugin("edits", EditsMidway(tenants.clone()))?;
    assert_eq!(send(&gw, "gw-acme").await?, StatusCode::OK);
    assert_eq!(auth(&mock.sent()[0]), Some("Bearer sk-acme"));
    assert_eq!(tenants.get("gw-acme").map(|t| t.version), Some(2));
    Ok(())
}

#[tokio::test]
async fn unknown_keys_and_providers_without_a_key_are_refused() -> Result<(), Fault> {
    let mock = MockTransport::new();
    let tenants = Tenants::default();
    let gw = gateway(&mock, tenants.clone())?;
    assert_eq!(send(&gw, "gw-nobody").await?, StatusCode::UNAUTHORIZED);
    let mut keyless = tenant(1, "openai", "sk")?;
    keyless.keys.clear();
    tenants.upsert("gw-keyless", keyless);
    assert_eq!(send(&gw, "gw-keyless").await?, StatusCode::FORBIDDEN);
    assert!(mock.sent().is_empty());
    Ok(())
}
