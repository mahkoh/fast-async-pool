use crate::thread_pool::{future::proxy::TpFutureTyped, Epoch};
pub use proxy::TpFuture;
pub use runnable::Runnable;
use std::future::Future;
pub use task::get_and_clear_live_futures;
use task::Task;

mod task {
    use super::runnable::Runnable;
    use crate::{
        abort::AbortOnDrop,
        thread_pool::{allocator, localp, Epoch},
    };
    pub use accounting::get_and_clear_live_futures;
    use parking_lot::Mutex;
    use std::{
        cell::UnsafeCell,
        future::Future,
        mem::ManuallyDrop,
        pin::Pin,
        ptr,
        sync::{
            atomic,
            atomic::{
                AtomicUsize,
                Ordering::{Acquire, Relaxed, Release},
            },
        },
        task::{Context, Poll, RawWaker, Waker},
    };
    use v_table::VTable;

    union TaskData<T, F> {
        future: ManuallyDrop<F>,
        result: ManuallyDrop<T>,
    }

    /// The components of a task with interior mutability.
    struct TaskInterior<T, F> {
        /// The waker that should be woken after this task has completed, if any.
        /// This field is only accessed while the state mutex is held.
        waker: Option<Waker>,
        /// The future OR the result of the future.
        /// Iff `COMPLETED` is not set, then this contains the future. The future field is
        ///     only accessed in `run_owned` and by the contract of `run_owned`, no concurrent
        ///     access is possible.
        /// Iff `COMPLETED` is set, then this contains the result and the field is only
        ///     accessed while the state mutex is held.
        data: TaskData<T, F>,
    }

    /// The task is currently running and we want it to re-run immediately afterwards.
    const RUN_AGAIN: u8 = 1 << 0;
    /// The task is currently running.
    const RUNNING: u8 = 1 << 1;
    /// The task has been completed, `interior.data.future` has been dropped and the result has been
    /// written to `interior.data.result`.
    const COMPLETED: u8 = 1 << 2;
    /// The task has been completed and the result has been read from `interior.data.result`.
    const EMPTIED: u8 = 1 << 3;

    pub(super) struct Task<T, F> {
        /// The epoch of this task.
        epoch: Epoch,
        /// The number of references to this task.
        ref_count: AtomicUsize,
        /// The state of the task.
        state: Mutex<u8>,
        /// The components of the task with interior mutability.
        interior: UnsafeCell<TaskInterior<T, F>>,
    }

    impl<T, F> Task<T, F>
    where
        F: Future<Output = T>,
    {
        /// Returns an owned reference to a pinned `Self`.
        ///
        /// # Safety
        ///
        /// - You must not move out of the pointer.
        /// - epoch must be the epoch of the current thread.
        /// - You must not reset this thread's allocator until the task is destroyed.
        /// - You must not access the task after the lifetimes of `T` or `F` end.
        pub unsafe fn new(future: F, epoch: Epoch) -> *const Self {
            // Abort if anything panics here to simplify reference counting
            let abort = AbortOnDrop;
            let task = Self {
                epoch,
                state: Mutex::new(RUNNING),
                ref_count: AtomicUsize::new(2), // we pass one reference to `push_owning_runnable`
                // and one is returned
                interior: UnsafeCell::new(TaskInterior {
                    waker: None,
                    data: TaskData {
                        future: ManuallyDrop::new(future),
                    },
                }),
            };
            // SAFETY: We've just created a future and by the contract of this function the epoch
            // is this thread's epoch.
            accounting::inc_live_futures();
            let task = allocator::allocate(task);
            // SAFETY: task is a valid pointer.
            let task = &*task;
            // SAFETY: - We're passing one owned reference of the two we created above.
            //         - RUNNING was set above.
            //         - We've just created `task` so this function has never been called.
            task.push_owning_runnable();
            abort.forget();
            task
        }

        fn inc_ref_count(&self) {
            let ref_count = self.ref_count.fetch_add(1, Relaxed);
            if ref_count > isize::MAX as usize {
                abort!("ref-count overflow: {}", ref_count);
            }
        }

        /// # Safety
        ///
        /// The caller must own a reference. This function consumes the reference.
        pub unsafe fn dec_ref_count(&self) {
            let ref_count = self.ref_count.fetch_sub(1, Release);
            if ref_count <= 0 {
                abort!("ref-count-underflow");
            }
            if ref_count == 1 {
                atomic::fence(Acquire);
                // SAFETY: This was the last reference to `self`. The fence above
                // synchronizes with the fetch_sub(Release) of all other references so our
                // view of `self` is up-to-date.
                ptr::drop_in_place(self as *const _ as *mut Task<T, F>);
            }
        }

        /// # Safety
        ///
        /// If `transfer_ownership` is true then the caller must own a reference and this
        /// function consumes the reference.
        unsafe fn run_again(&self, transfer_ownership: bool) {
            let mut state = self.state.lock();
            if *state & (RUN_AGAIN | COMPLETED) != 0 {
                // Drop the lock to prevent deadlocking in `drop`.
                drop(state);
                if transfer_ownership {
                    // SAFETY: By the contract of this function.
                    self.dec_ref_count();
                }
                return;
            }
            if *state & RUNNING != 0 {
                *state |= RUN_AGAIN;
                // Drop the lock to prevent deadlocking in `drop`.
                drop(state);
                if transfer_ownership {
                    // SAFETY: By the contract of this function.
                    self.dec_ref_count();
                }
            } else {
                *state |= RUNNING;
                if !transfer_ownership {
                    self.inc_ref_count();
                }
                // SAFETY: - If our caller didn't pass us ownership, then the inc_ref_count created one.
                //         - We just set `RUNNING`.
                //         - `RUNNING` wasn't set when we locked `state`.
                self.push_owning_runnable();
            }
        }

        /// # Safety
        ///
        /// - `self` must be an owned reference to `Self` and this function consumes the reference.
        /// - `state` must contain `RUNNING`.
        /// - This function must not be called again until `RUNNING` is unset.
        unsafe fn push_owning_runnable(&self) {
            // SAFETY: It's safe to call `run_owned_proxy` once with `self` as argument
            // by the contract of this function.
            let runnabel =
                Runnable::new(self as *const _ as *const _, Self::run_owned_proxy);
            super::super::run(runnabel);
        }

        /// # Safety
        ///
        /// - `task` must be an owned reference to `Self` and this function consumes the reference.
        /// - `state` must contain `RUNNING`.
        /// - This function must not be called again until `RUNNING` is unset.
        unsafe fn run_owned_proxy(task: *const (), poll: bool) {
            // SAFETY: By the contract of this function.
            let task = &*(task as *const Self);
            // SAFETY: By the contract of this function.
            task.run_owned(poll);
        }

        /// # Safety
        ///
        /// - `self` must be an owned reference to `Self` and this function consumes the reference.
        /// - `state` must contain `RUNNING`.
        /// - This function must not be called again until `RUNNING` is unset.
        unsafe fn run_owned(&self, poll: bool) {
            // Abort to simplify panic handling.
            let abort = AbortOnDrop;

            if poll {
                loop {
                    // We create a waker below which owns one reference.
                    self.inc_ref_count();

                    let raw_waker = RawWaker::new(
                        self as *const _ as *const _,
                        &VTable::<T, F>::V_TABLE,
                    );
                    // SAFETY: - We've just created a reference for this waker to consume.
                    //         - See `V_TABLE` for why it satisfies the contract of Waker.
                    let waker = Waker::from_raw(raw_waker);
                    let mut context = Context::from_waker(&waker);

                    let state = *self.state.lock();
                    if state & COMPLETED != 0 {
                        break;
                    }

                    loop {
                        // SAFETY: - Since `COMPLETED` is not set, `future` contains a future
                        //           and `poll` will not try to read from `result`.
                        //         - By the contract of this function, this function is not
                        //           called again until we unset `RUNNING` below, therefore
                        //           there is never more than one mutable reference to it.
                        //         - Since we own a reference, `drop` will not be run while
                        //           we're referencing `future`.
                        let future = &mut *(*self.interior.get()).data.future;
                        // SAFETY: - `self` is pinned and we don't move `future` until it is dropped.
                        //         - If `poll` panics, the process will be aborted.
                        if let Poll::Ready(t) =
                            Pin::new_unchecked(future).poll(&mut context)
                        {
                            let mut state = self.state.lock();
                            // SAFETY:
                            // - We just called `future` by reference so we can drop it now.
                            // - If `drop` panics, the process will be aborted.
                            ptr::drop_in_place(&mut *(*self.interior.get()).data.future);
                            // SAFETY: `result` is a valid place to store a `T`. Since
                            // `COMPLETED` is not set, there is no concurrent access to
                            // this field.
                            ptr::write(&mut *(*self.interior.get()).data.result, t);
                            // SAFETY: `waker` is protected by the `state` mutex.
                            if let Some(waker) = (*self.interior.get()).waker.take() {
                                waker.wake();
                            }
                            *state = COMPLETED;
                        } else {
                            let mut state = self.state.lock();
                            if *state & RUN_AGAIN != 0 {
                                *state &= !RUN_AGAIN;
                                continue;
                            }
                            *state &= !RUNNING;
                        }
                        break;
                    }
                    break;
                }
            }

            // SAFETY: By the contract of this function we own a reference.
            self.dec_ref_count();
            abort.forget();
        }

        pub fn poll(&self, cx: &mut Context<'_>) -> Poll<T> {
            let mut state = self.state.lock();
            if *state & COMPLETED != 0 {
                if *state & EMPTIED != 0 {
                    abort!("Future polled after it already returned its result");
                }
                *state |= EMPTIED;
                // SAFETY: Since `COMPLETED` is set and `EMPTIED` is not set, we can
                // move a `T` out of `result`. Since `COMPLETED` is set, `future` will no longer
                // be accessed. Access to `result` is protected by the `state` mutex.
                unsafe {
                    Poll::Ready(ptr::read(&mut *(*self.interior.get()).data.result))
                }
            } else {
                let abort = AbortOnDrop;
                unsafe {
                    // Access to `waker` is protected by the `state` mutex.
                    (*self.interior.get()).waker = Some(cx.waker().clone());
                }
                abort.forget();
                Poll::Pending
            }
        }
    }

    impl<T, F> Drop for Task<T, F> {
        fn drop(&mut self) {
            localp::with(|local| {
                if local.epoch != self.epoch {
                    abort!("Cannot drop a spawned future in an unrelated thread pool.");
                }
            });
            let state = *self.state.get_mut();
            unsafe {
                if state & COMPLETED != 0 {
                    if state & EMPTIED == 0 {
                        // SAFETY: There are no live references to `self` so this reference cannot alias.
                        // Since `COMPLETED` is set and `EMPTIED` is not set, `result` contains a
                        // `T`.
                        ptr::drop_in_place(&mut self.interior.get_mut().data.result);
                    }
                } else {
                    // SAFETY: There are no live references to `self` so this reference cannot alias.
                    // Since `COMPLETED` is not set, `future` contains a `F`.
                    ptr::drop_in_place(&mut self.interior.get_mut().data.future);
                }
                // SAFETY: We're destroying this future and we've checked above that the future
                // belongs to the epoch of the current thread.
                accounting::dec_live_futures();
            }
        }
    }

    mod accounting {
        use std::cell::Cell;

        thread_local! {
            static DATA: Cell<isize> = Default::default();
        }

        /// Returns the number of futures created in this thread in this epoch minus the number
        /// of futures dropped in this thread in this epoch.
        ///
        /// # Safety
        ///
        /// This resets the value to 0.
        pub unsafe fn get_and_clear_live_futures() -> isize {
            DATA.with(|rcs| rcs.replace(0))
        }

        /// Increments this thread's accounting number.
        ///
        /// # Safety
        ///
        /// This must only be called after a future has been created in this thread's epoch.
        pub(super) unsafe fn inc_live_futures() {
            DATA.with(|rcs| {
                rcs.set(rcs.get().checked_add(1).unwrap());
            });
        }

        /// Decrements this thread's accounting number.
        ///
        /// # Safety
        ///
        /// This must only be called after a future has been destroyed in this thread's epoch.
        pub(super) unsafe fn dec_live_futures() {
            DATA.with(|rcs| {
                rcs.set(rcs.get().checked_sub(1).unwrap());
            });
        }
    }

    mod v_table {
        use crate::thread_pool::future::task::Task;
        use std::{
            future::Future,
            marker::PhantomData,
            task::{RawWaker, RawWakerVTable},
        };

        pub struct VTable<T, F>(PhantomData<Task<T, F>>);

        impl<T, F> VTable<T, F>
        where
            F: Future<Output = T>,
        {
            pub const V_TABLE: RawWakerVTable = RawWakerVTable::new(
                Self::clone,
                Self::wake,
                Self::wake_by_ref,
                Self::drop,
            );

            /// Increments the reference count of `data`.
            ///
            /// # Safety
            ///
            /// `data` must be a reference to `Task<T, F>`.
            unsafe fn clone(data: *const ()) -> RawWaker {
                // SAFETY: By the contract of this function.
                let task = &*(data as *const Task<T, F>);
                task.inc_ref_count();
                RawWaker::new(data, &Self::V_TABLE)
            }

            /// # Safety
            ///
            /// `data` must be an owned reference to `Task<T, F>`.
            /// This function consumes the reference.
            unsafe fn wake(data: *const ()) {
                // SAFETY: By the contract of this function.
                let task = &*(data as *const Task<T, F>);
                // SAFETY: By the contract of this function, the caller transfer ownership to us.
                task.run_again(true);
            }

            /// # Safety
            ///
            /// `data` must be a reference to `Task<T, F>`.
            unsafe fn wake_by_ref(data: *const ()) {
                // SAFETY: By the contract of this function.
                let task = &*(data as *const Task<T, F>);
                // SAFETY: `transfer_ownership` is false.
                task.run_again(false);
            }

            /// # Safety
            ///
            /// `data` must be an owned reference to `Task<T, F>`.
            /// This function consumes the reference.
            unsafe fn drop(data: *const ()) {
                // SAFETY: By the contract of this function.
                let task = &*(data as *const Task<T, F>);
                // SAFETY: By the contract of this function, the caller transfer ownership to us.
                task.dec_ref_count();
            }
        }
    }
}

mod runnable {
    use std::mem::ManuallyDrop;

    pub struct Runnable {
        data: *const (),
        run_owned: unsafe fn(*const (), bool),
    }

    // SAFETY: By the contract of `new`.
    unsafe impl Send for Runnable {
    }

    // SAFETY: Runnable has no `&self` functions
    unsafe impl Sync for Runnable {
    }

    impl Runnable {
        /// # Safety
        ///
        /// - It must be safe to call `run_owned` once with the data pointer.
        /// - It must be safe to call `run_owned` from any thread.
        pub(super) unsafe fn new(
            data: *const (),
            run_owned: unsafe fn(*const (), bool),
        ) -> Self {
            Self { data, run_owned }
        }

        pub fn run(self) {
            let slf = ManuallyDrop::new(self);
            unsafe {
                // SAFETY: By the contract of `new`. Note that `drop` will not be called.
                (slf.run_owned)(slf.data, true);
            }
        }
    }

    impl Drop for Runnable {
        fn drop(&mut self) {
            unsafe {
                // SAFETY: By the contract of `new`.
                (self.run_owned)(self.data, false);
            }
        }
    }
}

mod proxy {
    use crate::thread_pool::future::task::Task;
    use std::{
        future::Future,
        marker::PhantomData,
        mem::ManuallyDrop,
        pin::Pin,
        task::{Context, Poll},
    };

    pub(super) struct TpFutureTyped<'a, T, F>
    where
        F: Future<Output = T> + Send + 'a,
        T: Send + 'a,
    {
        task: *const Task<T, F>,

        _invariant: PhantomData<fn(&'a u8) -> &'a u8>,
    }

    // SAFETY: `poll` and `dec_ref_count` of `Task` are `Sync`.
    unsafe impl<'a, T, F> Send for TpFutureTyped<'a, T, F>
    where
        F: Future<Output = T> + Send + 'a,
        T: Send + 'a,
    {
    }

    impl<'a, T, F> TpFutureTyped<'a, T, F>
    where
        F: Future<Output = T> + Send + 'a,
        T: Send + 'a,
    {
        /// # Safety
        ///
        /// `task` must be an owned reference to a Task. This function takes ownership.
        pub(super) unsafe fn new(task: *const Task<T, F>) -> Self {
            Self {
                task,
                _invariant: PhantomData,
            }
        }
    }

    impl<'a, T, F> Future for TpFutureTyped<'a, T, F>
    where
        F: Future<Output = T> + Send + 'a,
        T: Send + 'a,
    {
        type Output = T;

        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            let task = unsafe {
                // SAFETY: By the contract of `new` we own a reference to the task so the pointer
                // is still valid.
                &*self.task
            };
            task.poll(cx)
        }
    }

    impl<'a, T, F> Drop for TpFutureTyped<'a, T, F>
    where
        F: Future<Output = T> + Send + 'a,
        T: Send + 'a,
    {
        fn drop(&mut self) {
            unsafe {
                // SAFETY: By the contract of `new` we own a reference to the task so the pointer
                // is still valid.
                let task = &*self.task;
                // SAFETY: By the contract of `new` we own a reference.
                task.dec_ref_count();
            }
        }
    }

    impl<'a, T, F> Into<TpFuture<'a, T>> for TpFutureTyped<'a, T, F>
    where
        F: Future<Output = T> + Send + 'a,
        T: Send + 'a,
    {
        fn into(self) -> TpFuture<'a, T> {
            let slf = ManuallyDrop::new(self);
            TpFuture {
                task: slf.task as *const u8,
                vtable: &TpVTableImpl::<T, F>::V_TABLE,
                _invariant: Default::default(),
            }
        }
    }

    struct TpVTable<T> {
        poll: unsafe fn(*const u8, &mut Context<'_>) -> Poll<T>,
        drop: unsafe fn(*const u8),
    }

    struct TpVTableImpl<T, F>(T, F);

    impl<T, F> TpVTableImpl<T, F>
    where
        F: Future<Output = T> + Send,
        T: Send,
    {
        const V_TABLE: TpVTable<T> = TpVTable {
            poll: Self::poll,
            drop: Self::drop,
        };

        /// # Safety
        ///
        /// `task` must be a reference to `Task<T, F>`.
        unsafe fn poll(task: *const u8, cx: &mut Context<'_>) -> Poll<T> {
            // SAFETY: By the condition of this function.
            let task = &*(task as *const Task<T, F>);
            task.poll(cx)
        }

        /// # Safety
        ///
        /// `task` must be an owned reference to `Task<T, F>`. This function consumes the
        /// reference.
        unsafe fn drop(task: *const u8) {
            // SAFETY: By the condition of this function.
            let task = &*(task as *const Task<T, F>);
            // SAFETY: We own the reference.
            task.dec_ref_count();
        }
    }

    /// A future spawned into the thread pool.
    pub struct TpFuture<'a, T: Send + 'a> {
        task: *const u8,
        vtable: &'a TpVTable<T>,

        _invariant: PhantomData<fn(&'a u8) -> &'a u8>,
    }

    // SAFETY: TpFutureTyped is Send and TpFuture is a type-erased version of that.
    unsafe impl<'a, T: Send + 'a> Send for TpFuture<'a, T> {
    }

    impl<'a, T: Send + 'a> Future for TpFuture<'a, T> {
        type Output = T;

        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            unsafe {
                // SAFETY: For some `F`, task is a `Task<T, F>` and `poll` is
                // `TpFutureVTable::<T, F>::poll`. Furthermore, `self` was constructed
                // from a `TpFutureTyped<T, F>` which passed ownership of task to us.
                (self.vtable.poll)(self.task, cx)
            }
        }
    }

    impl<'a, T: Send + 'a> Drop for TpFuture<'a, T> {
        fn drop(&mut self) {
            unsafe {
                // SAFETY: For some `F`, task is a `Task<T, F>` and `drop` is
                // `TpFutureVTable::<T, F>::drop`. Furthermore, `self` was constructed
                // from a `TpFutureTyped<T, F>` which passed ownership of task to us.
                (self.vtable.drop)(self.task)
            }
        }
    }
}

/// This function keeps track of how many futures have been spawned in the epoch minus
/// how many of them have been dropped. This number can be retrieved via
/// `get_and_clear_live_futures`. This number can be negative if a future created an a different
/// thread has been dropped on the current thread. Sum the values across all threads to get
/// the total value.
///
/// # Safety
///
/// - The spawned future must not be accessed after
///     - `'a`, or
///     - the thread's allocator has been reset.
/// - `epoch` must be the epoch of the current thread.
pub(super) unsafe fn spawn<'a, T, F>(
    future: F,
    epoch: Epoch,
) -> impl Future<Output = T> + Send + Unpin + Into<TpFuture<'a, T>> + 'a
where
    F: Future<Output = T> + Send + 'a,
    T: Send + 'a,
{
    // SAFETY:
    // - We're never exposing `task` to our callers and nowhere in this module do
    //   we move out of the pointer.
    // - By the contract of this function epoch is this thread's epoch.
    let task = Task::new(future, epoch);
    // SAFETY: task is an owned reference.
    TpFutureTyped::new(task)
}
