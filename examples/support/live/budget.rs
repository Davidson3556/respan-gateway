//! Shared example bookkeeping: one reservation per in-flight turn, no protocol knowledge.
use crate::ledger::{Amount, HoldId, Ledger, MemoryLedger};
use parking_lot::Mutex;
use respan_gateway_core::ErrorCode;
use respan_gateway_core::Fault;
use respan_gateway_ws::CloseCode;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

pub const OUTPUT: Amount = 1_024;
pub const SESSION_MAX: Duration = Duration::from_secs(1_800);

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
        self.active.lock().contains_key(lane)
    }
    pub async fn begin(&self, lane: &str) -> Result<(), Fault> {
        let hold = HoldId::new();
        {
            let mut active = self.active.lock();
            if active.contains_key(lane) {
                return Err(Fault::new("this lane already has a turn")
                    .with(ErrorCode("turn_busy".into()))
                    .with(CloseCode(1008)));
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
        if let Some((_, sent)) = self.active.lock().get_mut(lane) {
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
        let hold = self.active.lock().get(lane).copied();
        let Some((hold, _)) = hold else { return Ok(()) };
        self.ledger
            .settle(&hold, usage.unwrap_or(self.quote))
            .await?;
        self.active.lock().remove(lane);
        self.turns.lock().push(record);
        Ok(())
    }
    pub async fn end(&self) {
        let active = std::mem::take(&mut *self.active.lock());
        for (lane, (hold, sent)) in active {
            let amount = if sent { self.quote } else { 0 };
            let _ = self.ledger.settle(&hold, amount).await;
            self.turns
                .lock()
                .push(serde_json::json!({"lane":lane,"status":"cut","charged":amount}));
        }
    }
}
