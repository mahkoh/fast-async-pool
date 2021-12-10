mod allocator;
mod future;
mod localp;
mod run_on_each;
mod thread_id;
mod thread_local;

pub use crate::thread_pool::future::TpFuture;
use crate::{
    abort::AbortOnDrop,
    thread_pool::{
        future::Runnable,
        localp::{InjectionPoint, Local},
        thread_id::ThreadId,
    },
};
use bumpalo::Bump;
use crossbeam_deque::{Injector, Steal, Stealer, Worker};
use parking_lot::{lock_api::RawMutex, Mutex};
use rand::Rng;
use std::{
    fmt::{Debug, Formatter},
    future::Future,
    marker::PhantomData,
    num::NonZeroU64,
    panic,
    sync::{
        atomic::{
            AtomicBool, AtomicIsize, AtomicUsize,
            Ordering::{Acquire, Relaxed, Release},
        },
        Arc,
    },
    task::Waker,
    thread,
    thread::{JoinHandle, Thread},
};
pub use thread_local::ThreadLocal;

static NEXT_EPOCH: Mutex<u64> = Mutex::const_new(parking_lot::RawMutex::INIT, 1);

#[derive(Copy, Clone, Eq, PartialEq)]
struct Epoch(NonZeroU64);

impl Default for Epoch {
    fn default() -> Self {
        let mut lock = NEXT_EPOCH.lock();
        let slf = Self(NonZeroU64::new(*lock).unwrap());
        *lock += 1;
        slf
    }
}

pub struct ThreadPool {
    next_epoch: Epoch,
    shared: Arc<SharedData>,
    workers: Vec<WorkerData>,
    sync: Arc<SyncData>,
    bump: Bump,
}

impl Debug for ThreadPool {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ThreadPool")
            .field("num_threads", &self.shared.num_workers)
            .finish_non_exhaustive()
    }
}

struct WorkerData {
    join_handle: JoinHandle<()>,
}

struct SharedDataLocked {
    main_thread: Thread,
}

impl Default for SharedDataLocked {
    fn default() -> Self {
        Self {
            main_thread: thread::current(),
        }
    }
}

struct LocalQueueEntry {
    pending: AtomicUsize,
    waker: Waker,
    f: Box<dyn Fn() + Send + Sync>,
}

struct LocalQueue {
    num_entries: AtomicUsize,
    entries: Mutex<Vec<Arc<LocalQueueEntry>>>,
}

struct SharedData {
    initial_epoch: Epoch,
    locked: Mutex<SharedDataLocked>,
    num_workers: usize,
    offset: u64,
    stealers: Vec<Stealer<Msg>>,
    local_queues: Vec<Arc<LocalQueue>>,
    injector: Injector<Msg>,
    stopped: AtomicBool,
}

#[derive(Default)]
struct SyncData {
    new_epoch: Epoch,
    park: AtomicBool,
    counter: AtomicUsize,
    live_futures: AtomicIsize,
}

enum Msg {
    Runnable(Runnable),
    Exit,
    Park(Arc<SyncData>),
}

impl ThreadPool {
    pub fn new(name: &str) -> Self {
        // Experimentation has shown `num_cpus::get() - 2` to produce superior results.
        let num_workers = (num_cpus::get() - 2).max(1);
        // let num_workers = 1;
        let mut qworkers = vec![];
        let mut stealers = vec![];
        for _ in 0..num_workers {
            let worker = Worker::new_fifo();
            stealers.push(worker.stealer());
            qworkers.push(worker);
        }
        let (offset, thread_ids) = thread_id::reserve(num_workers);
        let local_queues = std::iter::repeat_with(|| {
            Arc::new(LocalQueue {
                num_entries: Default::default(),
                entries: Default::default(),
            })
        })
        .take(num_workers)
        .collect();
        let shared = Arc::new(SharedData {
            initial_epoch: Epoch::default(),
            locked: Default::default(),
            num_workers,
            offset,
            stealers,
            local_queues,
            injector: Default::default(),
            stopped: AtomicBool::new(true),
        });
        let mut workers = vec![];
        for (i, (thread_id, qworker)) in thread_ids.zip(qworkers.into_iter()).enumerate()
        {
            let name = format!("{}-{}", name, i);
            let shared = shared.clone();
            let local_queue = shared.local_queues[i].clone();
            let join_handle = thread::Builder::new()
                .name(name)
                .spawn(move || unsafe {
                    // SAFETY: `scope` verifies that all futures have been dropped before returning.
                    thread(shared, thread_id, qworker, local_queue)
                })
                .unwrap();
            workers.push(WorkerData { join_handle });
        }
        Self {
            next_epoch: shared.initial_epoch,
            shared,
            workers,
            sync: Default::default(),
            bump: Default::default(),
        }
    }

    pub fn thread_local<T>(&self, f: impl FnMut() -> T) -> ThreadLocal<T> {
        ThreadLocal::new(self.shared.offset, self.shared.num_workers, f)
    }

    pub fn scope<'a, T, F>(&mut self, f: F) -> T
    where
        F: FnOnce(Scope<'a>) -> T,
    {
        let abort = AbortOnDrop;
        let shared = &self.shared;
        let bump = &mut self.bump;
        let next_epoch = &mut self.next_epoch;
        let sync = &mut self.sync;
        shared.stopped.store(false, Relaxed);
        sync.park.store(false, Relaxed);
        for worker in &self.workers {
            worker.join_handle.thread().unpark();
        }
        let mut local = Local {
            epoch: *next_epoch,
            shared,
            injection: InjectionPoint::Injector,
        };
        localp::set_and_run(&mut local, || unsafe {
            // SAFETY: We call `reset` before the closure returns.
            allocator::set_and_run(bump, || {
                let scope = Scope {
                    epoch: *next_epoch,
                    _invariant: Default::default(),
                };
                *next_epoch = Epoch::default();
                let res = f(scope);
                shared.locked.lock().main_thread = thread::current();
                *sync = Arc::new(SyncData {
                    new_epoch: *next_epoch,
                    park: AtomicBool::new(true),
                    counter: AtomicUsize::new(shared.num_workers),
                    // SAFETY: We're using the value to perform accounting.
                    live_futures: AtomicIsize::new(future::get_and_clear_live_futures()),
                });
                for _ in 0..shared.num_workers {
                    shared.injector.push(Msg::Park(sync.clone()));
                }
                while sync.counter.load(Acquire) > 0 {
                    thread::park();
                }
                if sync.live_futures.load(Relaxed) != 0 {
                    abort!("There are still live futures.");
                }
                // SAFETY: Since `sync.live_futures` is 0, all futures created on this thread
                // have been destroyed.
                allocator::reset();
                abort.forget();
                res
            })
        })
    }
}

impl Drop for ThreadPool {
    fn drop(&mut self) {
        self.shared.stopped.store(false, Relaxed);
        self.sync.park.store(false, Relaxed);
        for _ in 0..self.shared.num_workers {
            self.shared.injector.push(Msg::Exit);
        }
        for worker in &self.workers {
            worker.join_handle.thread().unpark();
        }
        for worker in self.workers.drain(..) {
            worker.join_handle.join().unwrap();
        }
        match self.shared.injector.steal() {
            Steal::Empty => {}
            _ => abort!("ThreadPool queue is not empty at drop"),
        }
    }
}

#[derive(Copy, Clone)]
pub struct Scope<'a> {
    epoch: Epoch,

    _invariant: PhantomData<fn(&'a u8) -> &'a u8>,
}

impl<'a> Debug for Scope<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Scope").field(&self.epoch.0).finish()
    }
}

impl<'a> Scope<'a> {
    pub fn spawn<T: Send + 'a>(
        self,
        future: impl Future<Output = T> + Send + 'a,
    ) -> impl Future<Output = T> + Send + Unpin + Into<TpFuture<'a, T>> + 'a {
        unsafe {
            localp::with(move |local| {
                if local.epoch != self.epoch {
                    panic!("Trying to spawn a future from an unrelated thread pool.");
                }
            });
            // SAFETY:
            // - We've just checked that `self.epoch` is this thread's epoch.
            // - `'a` outlives the `scope` function call of the thread pool.
            // - At the end of `scope`, it is verified that all futures spawned by `self`
            //   have been dropped.
            future::spawn(future, self.epoch)
        }
    }

    pub fn run_on_each(
        self,
        f: impl Fn() + Send + Sync + 'a,
    ) -> impl Future<Output = ()> + Send + Unpin + Into<TpFuture<'a, ()>> + 'a {
        run_on_each::run_on_each(self, f)
    }
}

fn get_msg(shared: &SharedData, worker: &Worker<Msg>, local_queue: &LocalQueue) -> Msg {
    loop {
        if let Some(msg) = worker.pop() {
            return msg;
        }
        loop {
            if local_queue.num_entries.load(Relaxed) > 0 {
                let abort = AbortOnDrop;
                let entries: Vec<_> = local_queue.entries.lock().drain(..).collect();
                local_queue.num_entries.fetch_sub(entries.len(), Relaxed);
                for entry in entries {
                    (entry.f)();
                    if entry.pending.fetch_sub(1, Release) == 1 {
                        entry.waker.wake_by_ref();
                    }
                }
                abort.forget();
                break;
            }
            loop {
                match shared.injector.steal() {
                    Steal::Success(msg) => return msg,
                    Steal::Empty => break,
                    Steal::Retry => {}
                }
            }
            let mut rand = rand::thread_rng();
            // NOTE: idx is slightly biased towards small indices but since `usize::MAX` should
            // be much larger than `stealers.len()`, this bias should be irrelevant.
            let idx = rand.gen::<usize>() % shared.stealers.len();
            loop {
                match shared.stealers[idx].steal_batch_and_pop(worker) {
                    Steal::Success(msg) => return msg,
                    Steal::Empty => break,
                    Steal::Retry => {}
                }
            }
        }
    }
}

/// After parking this thread, it will add call `get_and_clear_live_futures` and add the value
/// to `sync.live_futures` before reducing `sync.counter` by 1.
///
/// Before parking, all thread-local queues will be emptied.
///
/// # Safety
///
/// - After parking this thread, it must only be unparked after all futures allocated on this
///   thread have been destroyed.
unsafe fn thread(
    shared: Arc<SharedData>,
    thread_id: ThreadId,
    worker: Worker<Msg>,
    local_queue: Arc<LocalQueue>,
) {
    thread_id::set(thread_id);
    let mut bump = Bump::new();
    let mut local = Local {
        epoch: shared.initial_epoch,
        shared: &shared,
        injection: InjectionPoint::Worker(&worker),
    };
    while shared.stopped.load(Relaxed) {
        thread::park();
    }
    localp::set_and_run(&mut local, || {
        allocator::set_and_run(&mut bump, || loop {
            // NOTE: there is a loop here
            let msg = get_msg(&shared, &worker, &local_queue);
            match msg {
                Msg::Exit => break,
                Msg::Runnable(runnable) => {
                    runnable.run();
                }
                Msg::Park(sync) => {
                    // NOTE: `Park` messages are only ever injected via the injector.
                    // Since the accessed the injector, our local queue is empty at this
                    // point.

                    // SAFETY: We're at the top of the thread stack.
                    localp::set_epoch(sync.new_epoch);
                    // SAFETY: We're passing the value along to the main thread.
                    let live_futures = future::get_and_clear_live_futures();
                    sync.live_futures.fetch_add(live_futures, Relaxed);
                    // NOTE: It is not necessary to perform an `AcqRel` here because of
                    // C++17 4.7.1 paragraph 5 and 32.4 paragraph 2.
                    if sync.counter.fetch_sub(1, Release) == 1 {
                        let locked = shared.locked.lock();
                        locked.main_thread.unpark();
                    }
                    while sync.park.load(Relaxed) {
                        thread::park();
                    }
                    // SAFETY: We've been woken up so the main thread has validated that
                    // all futures have been dropped. There are no other objects using the
                    // allocator.
                    allocator::reset();
                }
            }
        });
    });
}

fn run(runnable: Runnable) {
    localp::with(move |local| {
        let msg = Msg::Runnable(runnable);
        match &local.injection {
            InjectionPoint::Injector => local.shared.injector.push(msg),
            InjectionPoint::Worker(worker) => worker.push(msg),
        }
    });
}
