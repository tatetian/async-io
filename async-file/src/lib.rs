#![cfg_attr(sgx, no_std)]
#![feature(get_mut_unchecked)]
#![feature(option_unwrap_none)]
#![feature(drain_filter)]
#![feature(slice_fill)]
#![feature(test)]

#[cfg(sgx)]
extern crate sgx_types;
#[cfg(sgx)]
#[macro_use]
extern crate sgx_tstd as std;
#[cfg(sgx)]
extern crate sgx_trts;
#[cfg(sgx)]
extern crate untrusted_allocator;
#[cfg(not(sgx))]
extern crate test;
#[cfg(not(sgx))]
extern crate rand;

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
    use test::Bencher;
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

    #[test]
    fn test_seq_write_read() {
        async_rt::task::block_on(async {
            let path = "tmp.data";
            let file = {
                let path = path.to_string();
                let flags = libc::O_WRONLY | libc::O_CREAT | libc::O_TRUNC;
                let mode = libc::S_IRUSR | libc::S_IWUSR;
                AsyncFile::<Runtime>::open(path.clone(), flags, mode).unwrap()
            };

            let data_len = 4096 * 1024;
            let mut data : Vec<u8> = Vec::with_capacity(data_len);
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

    const FILE_LEN: usize = 4096 * 10240; // 40 MB
    async fn prepare_file(file_num: usize) {
        for idx in 0..file_num {
            let file = {
                let path = format!("tmp.data.{}", idx).to_string();
                let flags = libc::O_WRONLY | libc::O_CREAT | libc::O_TRUNC;
                let mode = libc::S_IRUSR | libc::S_IWUSR;
                AsyncFile::<Runtime>::open(path.clone(), flags, mode).unwrap()
            };

            let mut data : Vec<u8> = vec![0; FILE_LEN];
            let input_buf = data.into_boxed_slice();
            let retval = file.write_at(0, &input_buf).await;
            assert!(retval as usize == input_buf.len());
            file.flush().await;
        }
    }

    #[test]
    fn bench_seq() {
        use std::time::{Duration, Instant};
        use async_rt::task::JoinHandle;

        async_rt::task::block_on(async {
            let file_num: usize = 5;
            let block_size: usize = 4096 * 1;
            prepare_file(file_num).await;
            
            let mut join_handles: Vec<JoinHandle<i32>> = (0..file_num)
                .map(|i| {
                    async_rt::task::spawn(async move {
                        let start = Instant::now();
                        
                        let file = {
                            let path = format!("tmp.data.{}", i).to_string();
                            let flags = libc::O_RDWR;
                            let mode = 0;
                            AsyncFile::<Runtime>::open(path.clone(), flags, mode).unwrap()
                        };

                        let mut vec = vec![0; block_size];
                        let mut buf = vec.into_boxed_slice();
                        let mut offset = 0;
                        while offset < FILE_LEN {
                            let retval = file.read_at(offset, &mut buf[..]).await;
                            // assert!(retval as usize == buf.len());
                            assert!(retval >= 0);
                            offset += retval as usize;
                        }

                        offset = 0;
                        while offset < FILE_LEN {
                            buf[0] = buf[0] % 128 + 1;
                            let retval = file.write_at(offset, &buf[..]).await;
                            // assert!(retval as usize == buf.len());
                            assert!(retval >= 0);
                            offset += retval as usize;
                        }

                        file.flush().await;
                        
                        let duration = start.elapsed();
                        println!("Time elapsed in sequential task {} [file_size: {}, block_size: {}] is: {:?}", i, FILE_LEN, block_size, duration);
                        i as i32
                    })
                })
                .collect();

            for (i, join_handle) in join_handles.iter_mut().enumerate() {
                assert!(join_handle.await == (i as i32));
            }

            // let start = Instant::now();

            // let mut files = Vec::new();
            // for idx in 0..file_num {
            //     files.push({
            //         let path = format!("tmp.data.{}", idx).to_string();
            //         let flags = libc::O_RDWR;
            //         let mode = 0;
            //         AsyncFile::<Runtime>::open(path.clone(), flags, mode).unwrap()
            //     });
            // }
            
            // let mut bufs = vec![vec![0; block_size].into_boxed_slice(); file_num];
            // for i in 0..(FILE_LEN / block_size) {
            //     for (cnt, file) in files.iter().enumerate() {
            //         let mut read_buf = &mut bufs[cnt];
            //         let offset = block_size * i;
            //         let retval = file.read_at(offset, &mut read_buf[..]).await;
            //         if retval as usize != read_buf.len() {
            //             println!("retval as usize != read_buf.len(), {}, {}", retval, read_buf.len())
            //         }
            //         assert!(retval as usize == read_buf.len());
            //     }
            // }

            // for i in 0..(FILE_LEN / block_size) {
            //     for (cnt, file) in files.iter().enumerate() {
            //         let mut write_buf = &mut bufs[cnt];
            //         write_buf[0] += 1;
            //         let offset = block_size * i;
            //         let retval = file.write_at(offset, &write_buf[..]).await;
            //         if retval as usize != write_buf.len() {
            //             println!("retval as usize != write_buf.len(), {}, {}", retval, write_buf.len())
            //         }
            //         assert!(retval as usize == write_buf.len());
            //     }
            // }

            // for file in &files {
            //     file.flush().await
            // }

            // let duration = start.elapsed();
            // println!("Time elapsed in expensive_function() is: {:?}", duration);

        });
    }

    #[test]
    fn bench_seq2() {
        use std::time::{Duration, Instant};
        use async_rt::task::JoinHandle;

        static file_num: usize = 1;
        static block_size: usize = 4096 * 8;

        async_rt::task::block_on(async {
            prepare_file(file_num).await;
        });

        async_rt::task::block_on(async {
            let start = Instant::now();
            let mut join_handles: Vec<JoinHandle<i32>> = (0..file_num)
                .map(|i| {
                    async_rt::task::spawn(async move {
                        
                        let file = {
                            let path = format!("tmp.data.{}", i).to_string();
                            let flags = libc::O_RDWR;
                            let mode = 0;
                            AsyncFile::<Runtime>::open(path.clone(), flags, mode).unwrap()
                        };

                        let mut vec = vec![0; block_size];
                        let mut buf = vec.into_boxed_slice();
                        let mut offset = 0;
                        while offset < FILE_LEN {
                            let retval = file.read_at(offset, &mut buf[..]).await;
                            assert!(retval >= 0);
                            offset += retval as usize;
                        }
                        
                        i as i32
                    })
                })
                .collect();

            for (i, join_handle) in join_handles.iter_mut().enumerate() {
                assert!(join_handle.await == (i as i32));
            }

            let duration = start.elapsed();
            println!("sequential read [file_size: {}, file_num: {}, block_size: {}] is: {:?}, throughput: {} Mb/s", 
                FILE_LEN, file_num, block_size, duration, ((FILE_LEN * file_num) as f64 / 1024.0 / 1024.0) / (duration.as_millis() as f64 / 1000.0));
            
        });

        async_rt::task::block_on(async {
            let start = Instant::now();
            let mut join_handles: Vec<JoinHandle<i32>> = (0..file_num)
                .map(|i| {
                    async_rt::task::spawn(async move {
                        
                        let file = {
                            let path = format!("tmp.data.{}", i).to_string();
                            let flags = libc::O_RDWR;
                            let mode = 0;
                            AsyncFile::<Runtime>::open(path.clone(), flags, mode).unwrap()
                        };

                        let mut vec = vec![0; block_size];
                        let mut buf = vec.into_boxed_slice();
                        let mut offset = 0;
                        while offset < FILE_LEN {
                            buf[0] = buf[0] % 128 + 1;
                            let retval = file.write_at(offset, &buf[..]).await;
                            assert!(retval >= 0);
                            offset += retval as usize;
                        }

                        file.flush().await;
                        
                        i as i32
                    })
                })
                .collect();

            for (i, join_handle) in join_handles.iter_mut().enumerate() {
                assert!(join_handle.await == (i as i32));
            }

            let duration = start.elapsed();
            println!("sequential write [file_size: {}, file_num: {}, block_size: {}] is: {:?}, throughput: {} Mb/s", 
                FILE_LEN, file_num, block_size, duration, ((FILE_LEN * file_num) as f64 / 1024.0 / 1024.0) / (duration.as_millis() as f64 / 1000.0));
            
        });
    }

    #[test]
    fn bench_seq3() {
        use std::time::{Duration, Instant};
        use async_rt::task::JoinHandle;

        static file_num: usize = 1;
        static block_size: usize = 4096 * 8;

        async_rt::task::block_on(async {
            prepare_file(file_num).await;
        });

        async_rt::task::block_on(async {
            let start = Instant::now();
            let mut join_handles: Vec<JoinHandle<i32>> = (0..file_num)
                .map(|i| {
                    async_rt::task::spawn(async move {
                        
                        let file = {
                            let path = format!("tmp.data.{}", i).to_string();
                            let flags = libc::O_RDWR;
                            let mode = 0;
                            let c_path = std::ffi::CString::new(path).unwrap();
                            let c_path_ptr = c_path.as_bytes_with_nul().as_ptr() as _;
                            unsafe{ libc::open(c_path_ptr, flags, mode) }
                        };
                        unsafe { libc::lseek(file, 0, libc::SEEK_SET) };

                        let mut vec = vec![0; block_size];
                        let mut bytes = 0;
                        while bytes < FILE_LEN {
                            let retval = unsafe{ libc::read(file, vec.as_mut_ptr() as *mut libc::c_void, block_size)};
                            assert!(retval >= 0);
                            bytes += retval as usize;
                        }
                        
                        unsafe{ libc::close(file); }
                        i as i32
                    })
                })
                .collect();

            for (i, join_handle) in join_handles.iter_mut().enumerate() {
                assert!(join_handle.await == (i as i32));
            }

            let duration = start.elapsed();
            println!("sequential read [file_size: {}, file_num: {}, block_size: {}] is: {:?}, throughput: {} Mb/s", 
                FILE_LEN, file_num, block_size, duration, ((FILE_LEN * file_num) as f64 / 1024.0 / 1024.0) / (duration.as_millis() as f64 / 1000.0));
            
        });

        async_rt::task::block_on(async {
            let start = Instant::now();
            let mut join_handles: Vec<JoinHandle<i32>> = (0..file_num)
                .map(|i| {
                    async_rt::task::spawn(async move {
                        
                        let file = {
                            let path = format!("tmp.data.{}", i).to_string();
                            let flags = libc::O_RDWR;
                            let mode = 0;
                            let c_path = std::ffi::CString::new(path).unwrap();
                            let c_path_ptr = c_path.as_bytes_with_nul().as_ptr() as _;
                            unsafe{ libc::open(c_path_ptr, flags, mode) }
                        };
                        unsafe { libc::lseek(file, 0, libc::SEEK_SET) };

                        let mut vec = vec![1; block_size];
                        let mut bytes = 0;
                        while bytes < FILE_LEN {
                            let retval = unsafe{ libc::write(file, vec.as_ptr() as *const libc::c_void, block_size) };
                            assert!(retval >= 0);
                            bytes += retval as usize;
                        }
                        
                        unsafe{ 
                            libc::fsync(file); 
                            libc::close(file); 
                        }
                        i as i32
                    })
                })
                .collect();

            for (i, join_handle) in join_handles.iter_mut().enumerate() {
                assert!(join_handle.await == (i as i32));
            }

            let duration = start.elapsed();
            println!("sequential write [file_size: {}, file_num: {}, block_size: {}] is: {:?}, throughput: {} Mb/s", 
                FILE_LEN, file_num, block_size, duration, ((FILE_LEN * file_num) as f64 / 1024.0 / 1024.0) / (duration.as_millis() as f64 / 1000.0));
            
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

    //#[bench]
    fn bench_seq_read_write(b: &mut Bencher) {
        static file_num: usize = 1;
        async_rt::task::block_on(prepare_file(file_num));

        b.iter(|| async_rt::task::block_on(async {
            let mut files = Vec::new();
            for idx in 0..file_num {
                files.push({
                    let path = format!("tmp.data.{}", idx).to_string();
                    let flags = libc::O_RDWR;
                    let mode = 0;
                    AsyncFile::<Runtime>::open(path.clone(), flags, mode).unwrap()
                });
            }
            
            let block_size: usize = 4096;
            let mut bufs = vec![vec![0; block_size].into_boxed_slice(); file_num];
            for i in 0..(FILE_LEN / block_size) {
                for (cnt, file) in files.iter().enumerate() {
                    let mut read_buf = &mut bufs[cnt];
                    let offset = block_size * i;
                    let retval = file.read_at(offset, &mut read_buf[..]).await;
                    assert!(retval as usize == read_buf.len());
                }
            }

            for i in 0..(FILE_LEN / block_size) {
                for (cnt, file) in files.iter().enumerate() {
                    let mut write_buf = &mut bufs[cnt];
                    write_buf[0] += 1;
                    let offset = block_size * i;
                    let retval = file.write_at(offset, &write_buf[..]).await;
                    assert!(retval as usize == write_buf.len());
                }
            }

            for file in &files {
                file.flush().await
            }
        }));
    }

    mod runtime {
        use super::*;
        use std::sync::Once;

        pub struct Runtime;

        pub const PAGE_CACHE_SIZE: usize = 102400 * 2; // 800 MB
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
        async_rt::executor::set_parallelism(1);

        let ring = &runtime::RING;
        let actor = move || {
            ring.trigger_callbacks();
        };
        async_rt::executor::register_actor(actor);
    }
}
