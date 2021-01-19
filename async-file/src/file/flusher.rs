use std::future::Future;

use futures::future::{BoxFuture, FutureExt};
use itertools::Itertools;

use crate::page_cache::{Page, PageCache, PageHandle, PageState};

/// Flush dirty pages in a page cache.
pub struct Flusher {
    page_cache: &'static PageCache,
}

impl Flusher {
    pub fn new(page_cache: &'static PageCache) -> Self {
        Self { page_cache }
    }

    pub async fn flush_by_fd(&self, fd: i32, max_pages: usize) -> usize {
        let mut dirty_pages = self.page_cache.evict_dirty_pages_by_fd(fd, max_pages);
        self.do_flush(dirty_pages).await
    }
    /*
        pub fn flush_by_watermarks(&self) -> usize {
            let num_dirty_pages = page_cache.num_dirty_pages();
            if nun_dirty_pages < self.high_watermark {
                return 0;
            }

            let mut num_flushed_pages = 0;
            while num_dirty_pages > self.low_watermark {
                let num_dirty_pages = page_cache.num_dirty_pages();

            }
            num_flushed_pages
        }
    */
    pub async fn flush(&self, max_pages: usize) -> usize {
        let mut dirty_pages = self.page_cache.evict_dirty_pages(max_pages);
        self.do_flush(dirty_pages).await
    }

    async fn do_flush(&self, mut dirty_pages: Vec<PageHandle>) -> usize {
        let page_cache = self.page_cache;
        // Remove all false positives in the supposed-to-be-dirty pages
        dirty_pages
            .drain_filter(|page| {
                let should_remove = {
                    let mut state = page.state();
                    if *state == PageState::Dirty {
                        *state = PageState::Flushing;
                        false
                    } else {
                        true
                    }
                };
                should_remove
            })
            .for_each(|non_dirty_page| {
                page_cache.release(non_dirty_page);
            });

        let num_dirty_pages = dirty_pages.len();
        if num_dirty_pages == 0 {
            return 0;
        }

        // Sort the pages so that we can easily merge small writes into larger ones
        dirty_pages.sort_by_key(|page| page.key());

        // Flush the dirty pages one fd at a time
        let mut futures: Vec<BoxFuture<'static, i32>> = Vec::new();
        dirty_pages
            .into_iter()
            .group_by(|page| page.fd())
            .into_iter()
            .for_each(|(fd, dirty_pages_of_a_fd)| {
                self.flush_dirty_pages_of_a_fd(fd, dirty_pages_of_a_fd, &mut futures);
            });
        for future in futures {
            future.await;
        }
        num_dirty_pages
    }

    fn flush_dirty_pages_of_a_fd(
        &self,
        fd: i32,
        mut iter: impl Iterator<Item = PageHandle>,
        futures: &mut Vec<BoxFuture<'static, i32>>,
    ) {
        let mut first_page_opt = iter.next();
        // Scan the dirty pages to group them into consecutive pages
        loop {
            let first_page = match first_page_opt {
                Some(first_page) => first_page,
                None => {
                    break;
                }
            };
            let first_offset = first_page.offset();

            let mut consecutive_pages = vec![first_page];
            let mut next_offset = first_offset + Page::size();
            loop {
                let next_page = match iter.next() {
                    Some(next_page) => next_page,
                    None => {
                        first_page_opt = None;
                        break;
                    }
                };
                if next_page.offset() != next_offset {
                    first_page_opt = Some(next_page);
                    break;
                }

                consecutive_pages.push(next_page);
                next_offset += Page::size();
            }

            let future =
                self.flush_consecutive_dirty_pages_of_a_fd(fd, first_offset, consecutive_pages);
            futures.push(future);
        }
    }

    fn flush_consecutive_dirty_pages_of_a_fd(
        &self,
        fd: i32,
        offset: usize,
        mut consecutive_pages: Vec<PageHandle>,
    ) -> BoxFuture<'static, i32> {
        let iovec: Vec<libc::iovec> = consecutive_pages
            .iter()
            .map(|page| libc::iovec {
                iov_base: page.page().as_mut_ptr() as _,
                iov_len: Page::size(),
            })
            .collect();
        let page_cache = self.page_cache;
        let complete_fn = move |retval: i32| {
            // TODO: handle the case when retval < consecutive_pages.len() * Page::size()
            assert!(retval as usize == consecutive_pages.len() * Page::size());

            for page in consecutive_pages {
                let mut state = page.state();
                match *state {
                    PageState::Flushing => {
                        *state = PageState::UpToDate;
                    }
                    _ => unreachable!(),
                };
                drop(state);
                page_cache.release(page);
            }
        };
        //let io_uring = ...;
        //let handle = io_uring.writev(fd, iovec.as_ptr(), iovec.len(), offset, complete_fn);
        todo!("import io_uring_callback")
    }
}
