#[cfg(feature = "sgx")]
use std::prelude::v1::*;
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
#[cfg(not(feature = "sgx"))]
use std::sync::Mutex;
#[cfg(feature = "sgx")]
use std::sync::SgxMutex as Mutex;
use std::task::{Context, Poll, Waker};

/// A counter for wait and wakeup.
///
/// The APIs of EventCounter are similar to that of Liunx's eventfd.
pub struct Counter {
    counter: AtomicU64,
    wakers: Mutex<VecDeque<Waker>>,
}

impl Counter {
    pub fn new(init_counter: u64) -> Self {
        Self {
            counter: AtomicU64::new(init_counter),
            wakers: Mutex::new(VecDeque::new()),
        }
    }

    /// Write to the counter.
    ///
    /// Write does two things: 1) increase the counter by one; 2) unblock any blocking read.
    pub fn write(&self) {
        let mut wakers = self.wakers.lock().unwrap();

        self.counter.fetch_add(1, Ordering::Relaxed);

        if let Some(waker) = wakers.pop_front() {
            waker.wake();
        }
    }

    /// Read from the counter.
    ///
    /// Read always returns an non-zero value of the counter and resets the counter to zero.
    /// If the current value of the counter is zero, it blocks until somebody else writes
    /// to the counter.
    pub fn read(&self) -> impl Future<Output = u64> + '_ {
        Read::new(self)
    }
}

impl Default for Counter {
    fn default() -> Self {
        Self::new(0)
    }
}

struct Read<'a> {
    inner: &'a Counter,
}

impl<'a> Read<'a> {
    fn new(inner: &'a Counter) -> Self {
        Self { inner }
    }
}

impl<'a> Future for Read<'a> {
    type Output = u64;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut wakers = self.inner.wakers.lock().unwrap();

        let old_counter = self.inner.counter.swap(0, Ordering::Relaxed);
        if old_counter > 0 {
            return Poll::Ready(old_counter);
        }

        let waker = cx.waker().clone();
        wakers.push_back(waker);
        Poll::Pending
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use super::*;

    #[test]
    fn read_and_then_write() {
        async_rt::task::block_on(async {
            let counter = Arc::new(Counter::new(0));

            let join_handle0 = {
                let counter = counter.clone();
                async_rt::task::spawn(async move {
                    counter.read().await;
                })
            };

            let join_handle1 = {
                let counter = counter.clone();
                async_rt::task::spawn(async move {
                    std::thread::sleep(std::time::Duration::from_millis(250));
                    counter.write();
                })
            };

            join_handle0.await;
            join_handle1.await;
        });
    }
}
