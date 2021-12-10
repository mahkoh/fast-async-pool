use parking_lot::{lock_api::RawMutex, Mutex};
use std::cell::Cell;

static NEXT_THREAD_ID: Mutex<u64> = Mutex::const_new(parking_lot::RawMutex::INIT, 1);

thread_local! {
    // todo: https://github.com/rust-lang/rust/issues/84223
    static THREAD_ID: Cell<u64> = Cell::new(u64::MAX);
}

pub struct ThreadId(u64);

pub fn reserve(n: usize) -> (u64, impl Iterator<Item = ThreadId>) {
    let n = n as u64;
    let mut locked = NEXT_THREAD_ID.lock();
    let res = *locked;
    *locked = locked.checked_add(n).unwrap();
    (res, (res..res + n).into_iter().map(ThreadId))
}

pub fn set(id: ThreadId) {
    THREAD_ID.with(|idp| {
        idp.set(id.0);
    })
}

#[inline]
pub fn get() -> u64 {
    THREAD_ID.with(|id| id.get())
}
