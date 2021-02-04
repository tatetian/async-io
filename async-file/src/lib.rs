#![cfg_attr(feature = "sgx", no_std)]
#![feature(get_mut_unchecked)]
#![feature(option_unwrap_none)]
#![feature(drain_filter)]
#![feature(slice_fill)]
#![feature(test)]

#[cfg(feature = "sgx")]
extern crate sgx_types;
#[cfg(feature = "sgx")]
#[macro_use]
extern crate sgx_tstd as std;
#[cfg(feature = "sgx")]
extern crate sgx_libc as libc;
#[cfg(feature = "sgx")]
extern crate sgx_trts;
#[cfg(feature = "sgx")]
extern crate sgx_untrusted_alloc;

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
    use io_uring_callback::{Builder, IoUring};
    use lazy_static::lazy_static;

    #[test]
    fn hello_world() {
        async_rt::task::block_on(async {
            let path = "tmp.data.hello_world";
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

    #[test]
    fn test_seq_write_read() {
        async_rt::task::block_on(async {
            let path = "tmp.data.test_seq_write_read";
            let file = {
                let path = path.to_string();
                let flags = libc::O_WRONLY | libc::O_CREAT | libc::O_TRUNC;
                let mode = libc::S_IRUSR | libc::S_IWUSR;
                AsyncFile::<Runtime>::open(path.clone(), flags, mode).unwrap()
            };

            let data_len = 4096 * 1024;
            let mut data: Vec<u8> = Vec::with_capacity(data_len);
            for i in 0..data_len {
                let ch = (i % 128) as u8;
                data.push(ch);
            }

            let input_buf = data.into_boxed_slice();
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

    // #[test]
    // fn bench_random() {
    //     use std::time::{Duration, Instant};
    //     use async_rt::task::JoinHandle;
    //     use rand::Rng;
    //     use std::sync::Arc;

    //     static file_num: usize = 5;
    //     static block_size: usize = 4096 * 2;

    //     let rng = Arc::new(rand::thread_rng());

    //     let mut randoms = Vec::with_capacity(FILE_LEN / block_size * 4);
    //     while randoms.len() < FILE_LEN / block_size * 4 {
    //         randoms.push(rng.gen_range(0..FILE_LEN / block_size));
    //     }
    //     drop(rng);

    //     async_rt::task::block_on(async {
    //         prepare_file(file_num).await;

    //         let mut join_handles: Vec<JoinHandle<i32>> = (0..file_num)
    //             .map(|i| {
    //                 async_rt::task::spawn(async move {
    //                     let start = Instant::now();

    //                     let file = {
    //                         let path = format!("tmp.data.{}", i).to_string();
    //                         let flags = libc::O_RDWR;
    //                         let mode = 0;
    //                         AsyncFile::<Runtime>::open(path.clone(), flags, mode).unwrap()
    //                     };

    //                     let mut vec = vec![0; block_size];
    //                     let mut buf = vec.into_boxed_slice();

    //                     let mut bytes = 0;
    //                     while bytes < FILE_LEN {
    //                         let offset = randoms.pop().unwrap() * block_size;
    //                         let retval = file.read_at(offset, &mut buf[..]).await;
    //                         // assert!(retval as usize == buf.len());
    //                         assert!(retval >= 0);
    //                         bytes += retval as usize;
    //                     }

    //                     while bytes < FILE_LEN {
    //                         let offset = randoms.pop().unwrap() * block_size;
    //                         buf[0] = buf[0] % 128 + 1;
    //                         let retval = file.write_at(offset, &buf[..]).await;
    //                         // assert!(retval as usize == buf.len());
    //                         assert!(retval >= 0);
    //                         bytes += retval as usize;
    //                     }

    //                     file.flush().await;

    //                     let duration = start.elapsed();
    //                     println!("Time elapsed in random task {} [file_size: {}, block_size: {}] is: {:?}", i, FILE_LEN, block_size, duration);
    //                     i as i32
    //                 })
    //             })
    //             .collect();

    //         for (i, join_handle) in join_handles.iter_mut().enumerate() {
    //             assert!(join_handle.await == (i as i32));
    //         }
    //     });
    // }

    mod runtime {
        use super::*;
        use std::sync::Once;

        pub struct Runtime;

        pub const PAGE_CACHE_SIZE: usize = 102400; // 400 MB
        pub const DIRTY_LOW_MARK: usize = PAGE_CACHE_SIZE / 10 * 3;
        pub const DIRTY_HIGH_MARK: usize = PAGE_CACHE_SIZE / 10 * 7;
        pub const MAX_DIRTY_PAGES_PER_FLUSH: usize = PAGE_CACHE_SIZE / 10;

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
        async_rt::executor::set_parallelism(1).unwrap();

        let ring = &runtime::RING;
        unsafe { ring.start_enter_syscall_thread(); }
        let actor = move || {
            ring.trigger_callbacks();
        };
        async_rt::executor::register_actor(actor);
    }
}
