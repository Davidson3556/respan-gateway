//! A provider's own plugins: their provider-edge hooks run on that
//! provider's calls alone, after the gateway's plugins, and never on the
//! client edge.
//! On the toy edge.
#![allow(clippy::unwrap_used)]

mod edge;
mod faults;

use std::sync::{Arc, Mutex};

use edge::*;
use respan_gateway_core::futures::FutureExt;
use respan_gateway_core::*;

/// Records each hook it runs as `<name> <hook> <provider>`.
#[derive(Clone)]
struct Named(&'static str, Arc<Mutex<Vec<String>>>);

#[async_trait]
impl Plugin for Named {
    async fn on_client(&self, _: &mut ClientContext) -> Result<Option<Response>> {
        lock(&self.1).push(format!("{} client", self.0));
        Ok(None)
    }

    async fn on_provider(&self, px: &mut ProviderContext) -> Result<()> {
        lock(&self.1).push(format!("{} provider {}", self.0, px.provider()));
        Ok(())
    }

    async fn on_provider_end(&self, px: &ProviderContext, _: &Result<()>) {
        lock(&self.1).push(format!("{} end {}", self.0, px.provider()));
    }
}

fn gateway(seen: &Arc<Mutex<Vec<String>>>, target: &str) -> Gateway {
    let ok = || connect(fnt(|_| async { Ok(ok_resp(vec!["a", "DONE"])) }.boxed()));
    Gateway::new()
        .client::<In, _, _>(Toy)
        .unwrap()
        .provider(Provider::new::<In, _, _>("a", Toy, ok()).plugin(Named("own", seen.clone())))
        .unwrap()
        .provider(Provider::new::<In, _, _>("b", Toy, ok()))
        .unwrap()
        .plugin(Named("plugin", seen.clone()))
        .target(target)
        .unwrap()
}

#[tokio::test(flavor = "current_thread")]
async fn a_providers_plugin_runs_after_the_gateways_on_its_calls_alone() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let gw = gateway(&seen, "a");
    let (res, end) = handle(&gw, req(vec!["hi"])).await;
    assert_eq!(read(res).await.map(|(status, _)| status), Ok(200));
    assert_eq!(code(&end.await), "ok");
    gw.idle().await;
    assert_eq!(
        *lock(&seen),
        [
            "plugin client",
            "plugin provider a",
            "own provider a",
            "plugin end a",
            "own end a",
        ]
    );

    let seen = Arc::new(Mutex::new(Vec::new()));
    let gw = gateway(&seen, "b");
    let (res, end) = handle(&gw, req(vec!["hi"])).await;
    assert_eq!(read(res).await.map(|(status, _)| status), Ok(200));
    assert_eq!(code(&end.await), "ok");
    gw.idle().await;
    assert_eq!(
        *lock(&seen),
        ["plugin client", "plugin provider b", "plugin end b"]
    );
}
