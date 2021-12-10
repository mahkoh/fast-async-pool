#![feature(thread_local_const_init, thread_local)]

pub use thread_pool::*;

#[macro_use] mod abort;
mod thread_pool;
