#![cfg_attr(sgx, no_std)]
#![feature(get_mut_unchecked)]
#![feature(option_unwrap_none)]
#![feature(drain_filter)]
#![feature(slice_fill)]

#[cfg(sgx)]
extern crate sgx_types;
#[cfg(sgx)]
#[macro_use]
extern crate sgx_tstd as std;
#[cfg(sgx)]
extern crate sgx_trts;
#[cfg(sgx)]
extern crate untrusted_allocator;

#[cfg(sgx)]
use sgx_trts::libc;
#[cfg(sgx)]
use std::prelude::v1::*;

mod event;
mod file;
mod page_cache;
mod util;

pub use crate::event::waiter::{Waiter, WaiterQueue};
pub use crate::file::{AsyncFile, AsyncFileRt, Flusher};
pub use crate::page_cache::{AsFd, Page, PageCache, PageHandle, PageState};

#[cfg(test)]
mod tests {
    use self::runtime::Runtime;
    use super::*;
    use crate::event::waiter::{Waiter, WaiterQueue};
    use lazy_static::lazy_static;
    use io_uring_callback::{IoUring, Builder};

    // TODO: enable this test after integrating with io_uring 
    #[test]
    fn hello_world() {
        async_rt::task::block_on(async {
            let path = "tmp.data";
            let file = {
                let path = path.to_string();
                let flags = libc::O_WRONLY | libc::O_CREAT | libc::O_TRUNC;
                let mode = libc::S_IRUSR | libc::S_IWUSR;
                AsyncFile::<Runtime>::open(path.clone(), flags, mode).unwrap()
            };
            let input_buf = "hello world\n".to_string().into_bytes().into_boxed_slice();
            let retval = file.write_at(0, &input_buf).await;
            assert!(retval as usize == input_buf.len());
            file.flush().await;
            drop(file);

            let file = {
                let path = path.to_string();
                let flags = libc::O_RDONLY;
                let mode = 0;
                AsyncFile::<Runtime>::open(path.clone(), flags, mode).unwrap()
            };
            let mut output_vec = Vec::with_capacity(input_buf.len());
            output_vec.resize(input_buf.len(), 0);
            let mut output_buf = output_vec.into_boxed_slice();
            let retval = file.read_at(0, &mut output_buf[..]).await;
            assert!(retval as usize == input_buf.len());
            assert!(output_buf.len() == input_buf.len());
            assert!(output_buf == input_buf);
        });
    }

    mod runtime {
        use super::*;
        use std::sync::Once;

        pub struct Runtime;

        pub const PAGE_CACHE_SIZE: usize = 16;
        pub const DIRTY_LOW_MARK: usize = 4;
        pub const DIRTY_HIGH_MARK: usize = 8;
        pub const MAX_DIRTY_PAGES_PER_FLUSH: usize = 12;

        lazy_static! {
            static ref PAGE_CACHE: PageCache = PageCache::with_capacity(PAGE_CACHE_SIZE);
            static ref FLUSHER: Flusher<Runtime> = Flusher::new();
            static ref WAITER_QUEUE: WaiterQueue = WaiterQueue::new();
            pub static ref RING: IoUring = Builder::new().build(1024).unwrap();
        }

        impl AsyncFileRt for Runtime {
            fn io_uring() -> &'static IoUring {
                &RING
            }
            fn page_cache() -> &'static PageCache {
                &PAGE_CACHE
            }

            fn flusher() -> &'static Flusher<Self> {
                &FLUSHER
            }

            fn auto_flush() {
                static INIT: Once = Once::new();
                INIT.call_once(|| {
                    async_rt::task::spawn(async {
                        let page_cache = &PAGE_CACHE;
                        let flusher = &FLUSHER;
                        let waiter_queue = &WAITER_QUEUE;
                        let waiter = Waiter::new();
                        waiter_queue.enqueue(&waiter);
                        loop {
                            // Start flushing when the # of dirty pages rises above the high watermark
                            while page_cache.num_dirty_pages() < DIRTY_HIGH_MARK {
                                waiter.wait().await;
                            }

                            // Stop flushing until the # of dirty pages falls below the low watermark
                            while page_cache.num_dirty_pages() > DIRTY_LOW_MARK {
                                flusher.flush(MAX_DIRTY_PAGES_PER_FLUSH).await;
                            }
                        }
                    });
                });

                if PAGE_CACHE.num_dirty_pages() >= DIRTY_HIGH_MARK {
                    WAITER_QUEUE.wake_all();
                }
            }
        }
    }

    #[ctor::ctor]
    fn auto_init_async_rt() {
        async_rt::executor::set_parallelism(4);

        let ring = &runtime::RING;
        let actor = move || {
            ring.trigger_callbacks();
        };
        async_rt::executor::register_actor(actor);
    }
}
