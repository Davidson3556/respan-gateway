use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use futures::FutureExt;
use futures::future::BoxFuture;
use tokio::sync::Notify;

use crate::gateway::Registered;
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
    pub(crate) ran: usize,
    /// Taken when the hooks start.
    cx: Option<E>,
    /// End hooks that must finish before these start.
    pub(crate) after: Option<Arc<InFlight>>,
}

impl<E: Edge> EndHooks<E> {
    pub(crate) fn new(in_flight: &Arc<InFlight>, cx: E) -> Self {
        Self {
            live: Some(InFlight::enter(in_flight)),
            ran: 0,
            cx: Some(cx),
            after: None,
        }
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
            for Registered { plugin, .. } in cx.client().gateway.plugins.iter().take(ran) {
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
        async move {
            loop {
                let idle = this.idle.notified();
                if this.count.load(Ordering::SeqCst) == 0 {
                    return;
                }
                idle.await;
            }
        }
    }
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        if self.0.count.fetch_sub(1, Ordering::SeqCst) == 1 {
            self.0.idle.notify_waiters();
        }
    }
}
