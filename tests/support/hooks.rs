//! End-hook records for public-API lifecycle tests.

use respan_gateway_sdk::{ClientContext, Fault, Plugin, ProviderContext, async_trait};
use std::sync::{Arc, Mutex, PoisonError};

#[derive(Debug, Clone, Default)]
pub struct Outcomes(Arc<Mutex<Vec<Result<(), Fault>>>>);

impl Outcomes {
    pub fn all(&self) -> Vec<Result<(), Fault>> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    pub fn only(&self) -> Result<(), Fault> {
        let mut all = self.all();
        assert_eq!(all.len(), 1, "expected exactly one end: {all:?}");
        all.remove(0)
    }
}

#[async_trait]
impl Plugin for Outcomes {
    async fn on_client_end(&self, _cx: &ClientContext, end: &Result<(), Fault>) {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(end.clone());
    }
}

#[derive(Debug, Clone, Default)]
pub struct Calls(Arc<Mutex<Vec<Result<(), Fault>>>>);

impl Calls {
    pub fn all(&self) -> Vec<Result<(), Fault>> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

#[async_trait]
impl Plugin for Calls {
    async fn on_provider_end(&self, _px: &ProviderContext, end: &Result<(), Fault>) {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(end.clone());
    }
}
