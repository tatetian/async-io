#![feature(get_mut_unchecked)]
#![feature(option_unwrap_none)]
#![feature(drain_filter)]
#![feature(slice_fill)]

mod event;
mod file;
mod page_cache;
mod util;

pub use crate::file::{AsyncFile, AsyncFileRt, Flusher};
pub use crate::page_cache::{Page, PageCache, PageHandle};

#[cfg(test)]
mod tests {
    use self::runtime::Runtime;
    use super::*;
    use crate::event::waiter::{Waiter, WaiterQueue};
    use lazy_static::lazy_static;

    #[test]
    fn it_works() {
        async fn hello_world() {
            let path = "tmp.data";
            let file = {
                let path = path.to_string();
                let flags = libc::O_WRONLY;
                let mode = libc::O_CREAT | libc::O_TRUNC;
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
            let mut output_buf = Vec::with_capacity(input_buf.len()).into_boxed_slice();
            let retval = file.read_at(0, &mut output_buf[..]).await;
            assert!(retval as usize == input_buf.len());
            assert!(output_buf.len() == input_buf.len());
            assert!(output_buf == input_buf);
        }
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
            static ref FLUSHER: Flusher = Flusher::new(&PAGE_CACHE);
            static ref WAITER_QUEUE: WaiterQueue = WaiterQueue::new();
        }

        impl AsyncFileRt for Runtime {
            fn page_cache() -> &'static PageCache {
                &PAGE_CACHE
            }

            fn flusher() -> &'static Flusher {
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
                            while page_cache.num_dirty_pages() > DIRTY_HIGH_MARK {
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
    }
}
