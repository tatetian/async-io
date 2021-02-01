use super::*;
use std::prelude::v1::*;
use std::sync::Once;
use async_rt::prelude::*;
use async_file::*;
use lazy_static::lazy_static;
use io_uring_callback::{IoUring, Builder};

pub fn init_runtime() {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        let ring = &runtime::RING;
        let actor = move || {
            ring.trigger_callbacks();
        };
        TEST_RT.register_actor(actor);
    });
}

pub fn run_blocking<T: Send + 'static>(
    future: impl Future<Output = T> + 'static + Send,
) -> T {
    TEST_RT.run_blocking(future)
}

const TEST_PARALLELISM: u32 = 1;

pub const PAGE_CACHE_SIZE: usize = 102400 * 2; // 800 MB
pub const DIRTY_LOW_MARK: usize = PAGE_CACHE_SIZE / 10 * 3;
pub const DIRTY_HIGH_MARK: usize = PAGE_CACHE_SIZE / 10 * 7;
pub const MAX_DIRTY_PAGES_PER_FLUSH: usize = PAGE_CACHE_SIZE / 10;

lazy_static! {
    static ref TEST_RT: TestRt = TestRt::new(TEST_PARALLELISM);

    static ref PAGE_CACHE: PageCache = PageCache::with_capacity(PAGE_CACHE_SIZE);
    static ref FLUSHER: Flusher<Runtime> = Flusher::new();
    static ref WAITER_QUEUE: WaiterQueue = WaiterQueue::new();
    static ref RING: IoUring = Builder::new().build(1024).unwrap();
}

pub struct Runtime;

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

struct TestRt {
    threads: Vec<std::thread::JoinHandle<()>>,
}

impl TestRt {
    pub fn new(parallelism: u32) -> Self {
        async_rt::executor::set_parallelism(parallelism).unwrap();

        let threads = (0..parallelism)
            .map(|_| std::thread::spawn(|| async_rt::executor::run_tasks()))
            .collect::<Vec<_>>();
        Self { threads }
    }

    pub fn register_actor(&self, actor: impl Fn() + Send + 'static) {
        async_rt::executor::register_actor(actor);
    }

    pub fn run_blocking<T: Send + 'static>(
        &self,
        future: impl Future<Output = T> + 'static + Send,
    ) -> T {
        async_rt::task::block_on(future)
    }
}

impl Drop for TestRt {
    fn drop(&mut self) {
        // Shutdown the executor and free the threads
        async_rt::executor::shutdown();

        for th in self.threads.drain(0..self.threads.len()) {
            th.join().unwrap();
        }
    }
}