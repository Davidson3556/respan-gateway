//! Use case 10, waiting instead of failing: an admission stage before any
//! call. `on_client` may await, so a request parks in a priority queue
//! until a slot frees or its deadline passes, then gets 429 with
//! `Retry-After`. Paid requests go first and wait longer.
//!
//! Within a priority, end users share fairly: the queue is round-robin over
//! users (`x-user` here; your auth plugin in practice), each with its own
//! first-come queue. A user with fifty requests parked gets one slot, then
//! the next user gets one, so a heavy user cannot starve a light one.
//!
//! The slot is held until the request ends, its response included, and
//! released in `on_client_end`: core keeps a request's context until its
//! end hooks run. A request that leaves while parked just drops out.
//!
//! Not covered: weighted shares (a user's share is one turn in the ring); a
//! limit per user on how many requests may park; a queue shared by several
//! gateway processes.
//!
//! `cargo run -p respan-gateway-examples --example admission`

use respan_gateway_formats::conversation::Conversation;
use std::cmp::Reverse;
use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use respan_gateway_core::{ClientContext, Fault, Gateway, Plugin, Response, async_trait};
use respan_gateway_formats::OpenAiChat;
use respan_gateway_http::Asked;
use respan_gateway_http::Status;
use respan_gateway_http::http::{self, HeaderValue, StatusCode, header::RETRY_AFTER};
use respan_gateway_testkit::{MockTransport, mock, post, read_whole, serve};
use tokio::sync::oneshot;
use tokio::time::Instant;

/// A counting semaphore whose waiters are served by priority, then by user
/// in turn, then in order. Clones share it.
#[derive(Clone)]
struct Slots(Arc<Mutex<Queue>>);

/// One user's waiters, earliest first.
type Line = VecDeque<oneshot::Sender<Slot>>;

struct Queue {
    free: usize,
    /// Highest priority first; in each, the users in the order of their turns.
    waiting: BTreeMap<Reverse<u8>, VecDeque<(String, Line)>>,
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
        Self(Arc::new(Mutex::new(Queue { free, waiting })))
    }

    fn queue(&self) -> MutexGuard<'_, Queue> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// A slot for `user`, waiting until `deadline` at most.
    async fn acquire(&self, priority: u8, user: &str, deadline: Instant) -> Option<Slot> {
        let woken = {
            let mut q = self.queue();
            if q.free > 0 && q.waiting.is_empty() {
                q.free -= 1;
                return Some(Slot(self.clone()));
            }
            let (wake, woken) = oneshot::channel();
            let ring = q.waiting.entry(Reverse(priority)).or_default();
            match ring.iter_mut().find(|(name, _)| name == user) {
                Some((_, line)) => line.push_back(wake),
                None => ring.push_back((user.to_owned(), VecDeque::from([wake]))),
            }
            woken
        };
        tokio::time::timeout_at(deadline, woken).await.ok()?.ok()
    }

    fn release(&self) {
        let mut q = self.queue();
        // The first user of the best priority has the turn; waiters that
        // gave up are skipped. A user with more waiting goes to the back.
        while let Some(mut tier) = q.waiting.first_entry() {
            let Some((user, mut line)) = tier.get_mut().pop_front() else {
                tier.remove();
                continue;
            };
            while let Some(wake) = line.pop_front() {
                if wake.is_closed() {
                    continue;
                }
                if !line.is_empty() {
                    tier.get_mut().push_back((user, line));
                }
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
        let user = asked.and_then(|a| a.headers.get("x-user")?.to_str().ok());
        let user = user.unwrap_or_default().to_owned();
        let until = Instant::now() + wait;
        let Some(slot) = self.slots.acquire(priority, &user, until).await else {
            let busy = "every slot is busy; retry later";
            let fault = Fault::new("busy", busy).with(Status(429));
            return Err(fault.with_retry_after(Duration::from_secs(1)));
        };
        let held = Held(Arc::new(Mutex::new(Some(slot))));
        cx.extensions_mut().insert(held);
        Ok(None)
    }

    async fn on_client_end(&self, cx: &ClientContext, _end: &Result<(), Fault>) {
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
        .provider::<Conversation, _, _>(
            "openai",
            OpenAiChat,
            respan_gateway_http::connect(mock.at(OPENAI)),
        )?
        .client::<Conversation, _, _>(OpenAiChat)?
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
