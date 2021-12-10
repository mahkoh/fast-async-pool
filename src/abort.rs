use std::mem;

pub struct AbortOnDrop;

impl AbortOnDrop {
    #[inline(always)]
    pub fn forget(self) {
        mem::forget(self);
    }
}

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        std::process::abort();
    }
}

macro_rules! abort {
    ($($tt:tt)*) => {{
        let _abort = $crate::abort::AbortOnDrop;
        panic!($($tt)*);
    }}
}
