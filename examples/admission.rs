//! Use case 10, waiting instead of failing: an admission stage before any
//! call. `on_client` may await, so a request parks in a priority queue
//! until a slot frees or its deadline passes, then gets 429 with
//! `Retry-After`. Paid requests go first and wait longer.
//!
//! The slot is held until the request ends, its response included, and
//! released in `on_client_end`: core keeps a request's context until its
//! end hooks run. A request that leaves while parked just drops out. Fair
//! share per end user is not built here.
//!
//! `cargo run -p respan-gateway-examples --example admission`

use std::cmp::Reverse;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use respan_gateway_core::{ClientContext, Fault, Gateway, Outcome, Plugin, Response, async_trait};
use respan_gateway_formats::OpenAiChat;
use respan_gateway_http::Asked;
use respan_gateway_http::http::{self, HeaderValue, StatusCode, header::RETRY_AFTER};
use respan_gateway_testkit::{MockTransport, mock, post, read_whole, serve};
use tokio::sync::oneshot;
use tokio::time::Instant;

/// A counting semaphore whose waiters are served by priority, then in
/// order. Clones share it.
#[derive(Clone)]
struct Slots(Arc<Mutex<Queue>>);

struct Queue {
    free: usize,
    /// Highest priority first, then earliest.
    waiting: BTreeMap<(Reverse<u8>, u64), oneshot::Sender<Slot>>,
    seq: u64,
}

/// A slot held; dropped, it goes to the next waiter. One sent to a waiter
/// that just gave up is dropped with the channel, so it is never lost.
struct Slot(Slots);

impl Drop for Slot {
    fn drop(&mut self) {
        self.0.release();
    }
}

impl Slots {
    fn new(free: usize) -> Self {
        let waiting = BTreeMap::new();
        Self(Arc::new(Mutex::new(Queue {
            free,
            waiting,
            seq: 0,
        })))
    }

    fn queue(&self) -> MutexGuard<'_, Queue> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// A slot, waiting until `deadline` at most.
    async fn acquire(&self, priority: u8, deadline: Instant) -> Option<Slot> {
        let woken = {
            let mut q = self.queue();
            if q.free > 0 && q.waiting.is_empty() {
                q.free -= 1;
                return Some(Slot(self.clone()));
            }
            let (wake, woken) = oneshot::channel();
            q.seq += 1;
            let seq = q.seq;
            q.waiting.insert((Reverse(priority), seq), wake);
            woken
        };
        tokio::time::timeout_at(deadline, woken).await.ok()?.ok()
    }

    fn release(&self) {
        let mut q = self.queue();
        // Waiters that gave up are skipped.
        while let Some((_, wake)) = q.waiting.pop_first() {
            if !wake.is_closed() {
                drop(q);
                // Refused, the slot drops and is released again.
                let _ = wake.send(Slot(self.clone()));
                return;
            }
        }
        q.free += 1;
    }
}

/// The request's slot, taken back at its end.
#[derive(Clone)]
struct Held(Arc<Mutex<Option<Slot>>>);

/// Admits each request into `slots`: `x-tier: paid` first, waiting up to
/// `paid`; others up to `free`.
struct Admission {
    slots: Slots,
    paid: Duration,
    free: Duration,
}

#[async_trait]
impl Plugin for Admission {
    async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault> {
        let asked = cx.extensions().get::<Asked>();
        let paid = asked
            .and_then(|a| a.headers.get("x-tier"))
            .is_some_and(|t| t == "paid");
        let (priority, wait) = if paid { (1, self.paid) } else { (0, self.free) };
        let Some(slot) = self.slots.acquire(priority, Instant::now() + wait).await else {
            let busy = "every slot is busy; retry later";
            let fault = Fault::new(StatusCode::TOO_MANY_REQUESTS, "busy", busy);
            return Err(fault.with_retry_after(Duration::from_secs(1)));
        };
        let held = Held(Arc::new(Mutex::new(Some(slot))));
        cx.extensions_mut().insert(held);
        Ok(None)
    }

    async fn on_client_end(&self, cx: &ClientContext, _outcome: &Outcome) {
        if let Some(Held(slot)) = cx.extensions().get::<Held>() {
            slot.lock().unwrap_or_else(PoisonError::into_inner).take();
        }
    }
}

const OPENAI: &str = "http://openai.test/v1";
const OK: &str =
    r#"{"id":"c","choices":[{"index":0,"message":{"role":"assistant","content":"hi"}}]}"#;

fn gateway(mock: &MockTransport, slots: &Slots, free: Duration) -> Result<Gateway, Fault> {
    let admission = Admission {
        slots: slots.clone(),
        paid: Duration::from_secs(5),
        free,
    };
    Gateway::new()
        .provider("openai", OpenAiChat, mock.at(OPENAI))?
        .serve(OpenAiChat)?
        .plugin("admission", admission)?
        .target("openai")
}

fn request(model: &str, tier: &'static str) -> http::Request<respan_gateway_http::Body> {
    let body = format!(r#"{{"model":"{model}","messages":[{{"role":"user","content":"hi"}}]}}"#);
    let mut req = post("/v1/chat/completions", body);
    req.headers_mut()
        .insert("x-tier", HeaderValue::from_static(tier));
    req
}

#[tokio::main]
async fn main() -> Result<(), Fault> {
    let mock = MockTransport::new();
    mock.reply(OPENAI, mock::json(StatusCode::OK, OK));
    let gw = gateway(&mock, &Slots::new(1), Duration::from_millis(200))?;
    // The only slot is held while this response is unread.
    let (held, _) = serve(&gw, request("a", "paid")).await;
    let (status, headers, _) = read_whole(serve(&gw, request("b", "free")).await.0).await;
    let retry = headers.get(RETRY_AFTER);
    println!("a free request, after 200 ms parked: {status}, retry-after {retry:?}");
    drop(held);
    Ok(())
}

#[cfg(test)]
#[path = "tests/admission.rs"]
mod tests;
