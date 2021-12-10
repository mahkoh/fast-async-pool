use crate::thread_pool::{Epoch, Msg, SharedData};
use crossbeam_deque::Worker;
use isnt::std_1::primitive::IsntConstPtrExt;
use std::{cell::Cell, mem, ptr};

thread_local! {
    // todo: https://github.com/rust-lang/rust/issues/84223
    static LOCAL: Cell<*const Local<'static>> = Cell::new(ptr::null());
}

pub(super) enum InjectionPoint<'a> {
    Worker(&'a Worker<Msg>),
    Injector,
}

pub(super) struct Local<'a> {
    pub epoch: Epoch,
    pub shared: &'a SharedData,
    pub injection: InjectionPoint<'a>,
}

pub struct Reset;

impl Drop for Reset {
    fn drop(&mut self) {
        LOCAL.with(|shared| {
            shared.set(ptr::null());
        })
    }
}

/// # Safety
///
/// This function must be called from outside all `with` calls.
pub(super) unsafe fn set_epoch(epoch: Epoch) {
    LOCAL.with(|local| {
        let local = local.get();
        if local.is_null() {
            abort!("Cannot access thread pool data outside of thread pool.");
        }
        unsafe {
            // SAFETY: By the contract of the function, there are no other references to local.
            (*(local as *mut Local)).epoch = epoch;
        }
    })
}

pub(super) fn set_and_run<T>(s: &mut Local, f: impl FnOnce() -> T) -> T {
    LOCAL.with(|local| {
        if local.get().is_not_null() {
            panic!("Thread pools cannot be nested.");
        }
        unsafe {
            // SAFETY: We're lengthening the lifetime to `'static` but we'll unset the pointer
            // before this function returns and we never grant access to the pointed to value
            // for a lifetime longer than this function call.
            local.set(mem::transmute::<&Local, *const Local<'static>>(s));
        }
        let _reset = Reset;
        f()
    })
}

/// Panics if called from outside a thread pool.
#[inline]
pub(super) fn with<T>(f: impl FnOnce(&Local) -> T) -> T {
    LOCAL.with(|local| {
        let local = local.get();
        if local.is_null() {
            panic!("Cannot access thread pool data outside of thread pool.");
        }
        // SAFETY: Since the pointer is not null we're being called from within the callback passed
        // to `set_and_run`. Hence the pointer is valid and points to a `Local` that outlives this
        // function call. Since `Local` is covariant, we can cast the pointer to `&Local` with a
        // shorter lifetime.
        f(unsafe { &*local })
    })
}
