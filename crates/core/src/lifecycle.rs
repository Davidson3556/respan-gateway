use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use futures::FutureExt;
use futures::future::BoxFuture;
use tokio::sync::Notify;

use crate::{ClientContext, Fault, Plugin, Result};

/// The context an edge's hooks get: [`ClientContext`] on the client edge,
/// [`ProviderContext`](crate::ProviderContext) on the provider edge.
pub(crate) trait Edge: Send + Sync + 'static {
    /// The request, whose runtime runs the end hooks.
    fn client(&self) -> &ClientContext;

    /// `plugin`'s end hook on this edge.
    fn hook<'a>(&'a self, plugin: &'a dyn Plugin, end: &'a Result<()>) -> BoxFuture<'a, ()>;

    /// The edge ends `end`, just before its end hooks.
    fn close(&mut self, end: &Result<()>);
}

/// Runs one edge's end hooks exactly once, for every plugin whose start
/// hook started: when told how it ended, or abandoned if dropped first.
pub(crate) struct EndHooks<E: Edge> {
    /// Kept until its end hooks finish, for `Gateway::idle`.
    live: Option<InFlightGuard>,
    /// How many plugins' `on_client` or `on_provider` started.
    ran: usize,
    /// Taken when the hooks start.
    cx: Option<E>,
    /// Work that must finish before these hooks start.
    after: Option<Arc<InFlight>>,
}

impl<E: Edge> EndHooks<E> {
    /// The end hooks of `cx`, counted in `in_flight`, which start once
    /// `after`, if any, is idle.
    pub(crate) fn new(in_flight: &Arc<InFlight>, cx: E, after: Option<Arc<InFlight>>) -> Self {
        Self {
            live: Some(InFlight::enter(in_flight)),
            ran: 0,
            cx: Some(cx),
            after,
        }
    }

    /// The context for the next plugin's start hook, whose end hook then
    /// runs too.
    pub(crate) fn begin(&mut self) -> &mut E {
        self.ran += 1;
        self.cx()
    }

    pub(crate) fn cx(&mut self) -> &mut E {
        self.cx
            .as_mut()
            .expect("the context lives until the end hooks start")
    }

    pub(crate) fn is_finished(&self) -> bool {
        self.cx.is_none()
    }

    /// End the edge `end`, once, and start its end hooks, detached.
    pub(crate) fn finish(&mut self, end: Result<()>) {
        let Some(mut cx) = self.cx.take() else {
            return;
        };
        // The end is known, so a panic neither changes it nor stops the hooks.
        let _ = std::panic::catch_unwind(AssertUnwindSafe(|| cx.close(&end)));
        let (ran, live, after) = (self.ran, self.live.take(), self.after.take());
        // The context goes before `live`, so `Gateway::idle` finds the
        // request's values, the gateway's own included, dropped.
        if ran == 0 {
            drop(cx);
            return;
        }
        let runtime = cx.client().runtime.clone();
        // Detached, so a slow hook never delays the response or request.
        drop(runtime.spawn(async move {
            if let Some(first) = after {
                first.idle().await;
            }
            for plugin in cx.client().gateway.plugins.iter().take(ran) {
                // One plugin's panic must not cost the others their hook.
                let hook = AssertUnwindSafe(cx.hook(&**plugin, &end));
                let _ = hook.catch_unwind().await;
            }
            drop(cx);
            drop(live);
        }));
    }
}

/// Dropped unfinished, it ends abandoned, even during a panic's unwinding.
impl<E: Edge> Drop for EndHooks<E> {
    fn drop(&mut self) {
        self.finish(Err(Fault::abandoned()));
    }
}

/// Wait on `notify` until `done`, registering before each check so a
/// notification in between wakes it.
async fn wait_until(notify: &Notify, done: impl Fn() -> bool) {
    loop {
        let notified = notify.notified();
        if done() {
            return;
        }
        notified.await;
    }
}

/// Closed once, and never reopened: whoever waits on it then wakes.
#[derive(Clone, Default)]
pub(crate) struct Latch(Arc<LatchState>);

#[derive(Default)]
struct LatchState {
    closed: AtomicBool,
    notify: Notify,
}

impl Latch {
    pub(crate) fn close(&self) {
        if !self.0.closed.swap(true, Ordering::SeqCst) {
            self.0.notify.notify_waiters();
        }
    }

    /// Resolves once it is closed.
    pub(crate) fn closed(&self) -> impl Future<Output = ()> + Send + 'static {
        let state = self.0.clone();
        async move { wait_until(&state.notify, || state.closed.load(Ordering::SeqCst)).await }
    }
}

/// A count of running work, such as requests and end hooks, to wait on.
#[derive(Default)]
pub(crate) struct InFlight {
    count: AtomicUsize,
    idle: Notify,
}

/// One entry in [`InFlight`], left when dropped.
pub(crate) struct InFlightGuard(Arc<InFlight>);

impl InFlight {
    pub(crate) fn enter(in_flight: &Arc<InFlight>) -> InFlightGuard {
        in_flight.count.fetch_add(1, Ordering::SeqCst);
        InFlightGuard(in_flight.clone())
    }

    /// Resolves once none is left.
    pub(crate) fn idle(self: &Arc<Self>) -> impl Future<Output = ()> + Send + 'static {
        let this = self.clone();
        async move { wait_until(&this.idle, || this.count.load(Ordering::SeqCst) == 0).await }
    }
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        if self.0.count.fetch_sub(1, Ordering::SeqCst) == 1 {
            self.0.idle.notify_waiters();
        }
    }
}
