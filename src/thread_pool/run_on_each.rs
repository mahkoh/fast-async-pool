use crate::thread_pool::{localp, LocalQueueEntry, Scope, TpFuture};
use std::{
    future::Future,
    mem,
    pin::Pin,
    sync::{
        atomic::{
            AtomicUsize,
            Ordering::{Acquire, Relaxed},
        },
        Arc,
    },
    task::{Context, Poll},
};

enum RunOnEachState<'a> {
    Pending(Option<Box<dyn Fn() + Send + Sync + 'a>>),
    Submitted(Arc<LocalQueueEntry>),
}

struct RunOnEach<'a> {
    state: RunOnEachState<'a>,
}

pub(super) fn run_on_each<'a>(
    scope: Scope<'a>,
    f: impl Fn() + Send + Sync + 'a,
) -> impl Future<Output = ()> + Send + Unpin + Into<TpFuture<'a, ()>> + 'a {
    scope.spawn(RunOnEach {
        state: RunOnEachState::Pending(Some(Box::new(f))),
    })
}

impl<'a> Future for RunOnEach<'a> {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let f = match &mut self.state {
            RunOnEachState::Pending(p) => p.take().unwrap(),
            RunOnEachState::Submitted(l) => {
                if l.pending.load(Acquire) == 0 {
                    return Poll::Ready(());
                } else {
                    return Poll::Pending;
                }
            }
        };
        let f = unsafe {
            // SAFETY: We're running in a thread pool with scope 'a. The waker we're storing
            // below holds a reference to our `Task`. Therefore, having a reference to `f`
            // implies having a reference to `Task`. If `f` is not destroyed before `'a` ends,
            // then `Task` is also not destroyed and the process will be aborted.
            mem::transmute::<Box<dyn Fn() + Send + Sync + 'a>, Box<dyn Fn() + Send + Sync>>(
                f,
            )
        };
        localp::with(|local| {
            let entry = Arc::new(LocalQueueEntry {
                pending: AtomicUsize::new(local.shared.num_workers),
                waker: cx.waker().clone(),
                f,
            });
            for lq in &local.shared.local_queues {
                let mut entries = lq.entries.lock();
                entries.push(entry.clone());
                lq.num_entries.fetch_add(1, Relaxed);
            }
            self.state = RunOnEachState::Submitted(entry);
            Poll::Pending
        })
    }
}
