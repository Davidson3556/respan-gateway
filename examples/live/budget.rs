//! Shared example bookkeeping: one reservation per in-flight turn, no protocol knowledge.
use respan_gateway_core::Fault;
use respan_gateway_ledger::{Amount, HoldId, Ledger, MemoryLedger};
use respan_gateway_ws::CloseCode;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

pub const OUTPUT: Amount = 1_024;
// Deployment quote: choose a model whose entire billed input fits this bound.
// Provider-side caches and instructions are covered too, unlike a byte estimate.
#[cfg(test)]
pub const QUOTE: Amount = 128_000 + OUTPUT;
pub const SESSION_MAX: Duration = Duration::from_secs(1_800);

pub fn lock<T>(value: &Mutex<T>) -> MutexGuard<'_, T> {
    value.lock().unwrap_or_else(PoisonError::into_inner)
}

pub struct Budget {
    pub ledger: MemoryLedger,
    active: Mutex<HashMap<String, (HoldId, bool)>>,
    quote: Amount,
    pub turns: Arc<Mutex<Vec<Value>>>,
}

impl Budget {
    pub fn new(
        ledger: MemoryLedger,
        turns: Arc<Mutex<Vec<Value>>>,
        input_limit: Amount,
    ) -> Result<Self, Fault> {
        let quote = input_limit
            .checked_add(OUTPUT)
            .filter(|_| input_limit > 0)
            .ok_or_else(|| Fault::config("model input-token bound must be positive and finite"))?;
        Ok(Self {
            ledger,
            active: Mutex::default(),
            quote,
            turns,
        })
    }
    pub fn active(&self, lane: &str) -> bool {
        lock(&self.active).contains_key(lane)
    }
    pub async fn begin(&self, lane: &str) -> Result<(), Fault> {
        let hold = HoldId::new();
        {
            let mut active = lock(&self.active);
            if active.contains_key(lane) {
                return Err(
                    Fault::new("turn_busy", "this lane already has a turn").with(CloseCode(1008))
                );
            }
            active.insert(lane.to_owned(), (hold, false));
        }
        self.ledger
            .reserve(
                &hold,
                &["demo".to_owned()],
                self.quote,
                Duration::from_secs(3600),
            )
            .await
            .map_err(|f| f.with(CloseCode(1008)))?;
        if let Some((_, sent)) = lock(&self.active).get_mut(lane) {
            *sent = true;
        }
        Ok(())
    }
    pub async fn finish(
        &self,
        lane: &str,
        usage: Option<Amount>,
        record: Value,
    ) -> Result<(), Fault> {
        let hold = lock(&self.active).get(lane).copied();
        let Some((hold, _)) = hold else { return Ok(()) };
        self.ledger
            .settle(&hold, usage.unwrap_or(self.quote))
            .await?;
        lock(&self.active).remove(lane);
        lock(&self.turns).push(record);
        Ok(())
    }
    pub async fn end(&self) {
        let active = std::mem::take(&mut *lock(&self.active));
        for (lane, (hold, sent)) in active {
            let amount = if sent { self.quote } else { 0 };
            let _ = self.ledger.settle(&hold, amount).await;
            lock(&self.turns)
                .push(serde_json::json!({"lane":lane,"status":"cut","charged":amount}));
        }
    }
}
