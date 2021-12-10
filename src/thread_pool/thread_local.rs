use crate::thread_pool::thread_id;
use std::iter;

pub struct ThreadLocal<T> {
    offset: u64,
    data: Vec<T>,
}

unsafe impl<T: Send> Send for ThreadLocal<T> {
}

// SAFETY: `get` ensures that each `T` can only be accessed by one thread at a time.
unsafe impl<T: Send> Sync for ThreadLocal<T> {
}

impl<T> ThreadLocal<T> {
    pub(crate) fn new(offset: u64, num_workers: usize, f: impl FnMut() -> T) -> Self {
        ThreadLocal {
            offset,
            data: iter::repeat_with(f).take(num_workers).collect(),
        }
    }

    pub unsafe fn get_at(&self, idx: usize) -> &T {
        &self.data[idx]
    }

    pub fn get(&self) -> &T {
        let pos = thread_id::get().wrapping_sub(self.offset);
        if pos >= self.data.len() as u64 {
            panic!("Thread local data cannot be accesses from outside the thread pool");
        }
        &self.data[pos as usize]
    }

    pub fn into_inner(self) -> Vec<T> {
        self.data
    }

    pub fn slice(&mut self) -> &mut [T] {
        &mut self.data
    }
}
