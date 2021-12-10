use bumpalo::Bump;
use isnt::std_1::primitive::IsntMutPtrExt;
use std::{cell::Cell, ptr};

thread_local! {
    // todo: https://github.com/rust-lang/rust/issues/84223
    static ALLOCATOR: Cell<*mut Bump> = Cell::new(ptr::null_mut());
}

pub struct Reset;

impl Drop for Reset {
    fn drop(&mut self) {
        ALLOCATOR.with(|a| {
            a.set(ptr::null_mut());
        });
    }
}

/// # Safety
///
/// All allocated values must have been dropped before this function returns.
pub unsafe fn set_and_run<T>(bump: &mut Bump, f: impl FnOnce() -> T) -> T {
    ALLOCATOR.with(|a| {
        if a.get().is_not_null() {
            abort!("Thread pools cannot be nested");
        }
        a.set(bump);
        let _reset = Reset;
        f()
    })
}

fn with_bump<T>(f: impl FnOnce(&mut Bump) -> T) -> T {
    ALLOCATOR.with(|a| {
        let a = a.get();
        if a.is_null() {
            abort!("Allocator has not been set");
        }
        let a = unsafe { &mut *a };
        f(a)
    })
}

/// # Safety
///
/// This function must only be called after all allocated values have been dropped.
pub unsafe fn reset() {
    with_bump(|b| b.reset());
}

/// Returns a pointer to `t` which was moved to this thread's allocator.
pub fn allocate<T>(t: T) -> *mut T {
    with_bump(|b| b.alloc(t) as *mut T)
}
